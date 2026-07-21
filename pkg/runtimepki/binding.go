package runtimepki

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

var (
	errRuntimeCredentialNotEnrolled = errors.New("runtime credential is not enrolled")
	errRuntimeCredentialInvalid     = errors.New("runtime credential binding is invalid")
)

type BindingVerifier struct {
	pool runtimeBindingQuerier
}

type runtimeBindingQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func NewBindingVerifier(pool *pgxpool.Pool) *BindingVerifier {
	return &BindingVerifier{pool: pool}
}

func (v *BindingVerifier) ResolveRuntimeDeviceIdentity(ctx context.Context, credentialID uuid.UUID) (coreruntime.RuntimeDeviceIdentity, error) {
	if v == nil || v.pool == nil || credentialID == uuid.Nil {
		return coreruntime.RuntimeDeviceIdentity{}, errors.New("runtime credential binding is unavailable")
	}
	var identity coreruntime.RuntimeDeviceIdentity
	var status string
	var bindingMode string
	err := v.pool.QueryRow(ctx, `
SELECT node.node_id, node.device_certificate_serial,
       COALESCE(certificate.certificate_fingerprint, ''),
       binding.public_key_thumbprint, node.status, binding.binding_mode
FROM runtime_node_bindings binding
JOIN runtime_nodes node ON node.node_id = binding.node_id
LEFT JOIN LATERAL (
    SELECT issued.certificate_fingerprint
    FROM runtime_node_certificates issued
    WHERE issued.node_id = node.node_id
      AND issued.public_key_thumbprint = binding.public_key_thumbprint
    ORDER BY issued.not_after DESC, issued.issued_at DESC
    LIMIT 1
) certificate ON TRUE
WHERE binding.credential_id = $1`, credentialID).Scan(
		&identity.NodeID,
		&identity.CertificateSerial,
		&identity.CertificateFingerprintSHA256,
		&identity.PublicKeyThumbprintSHA256,
		&status,
		&bindingMode,
	)
	if err != nil || bindingMode != "mtls" ||
		(status != "active" && status != "draining") ||
		identity.CertificateFingerprintSHA256 == "" {
		if errors.Is(err, pgx.ErrNoRows) {
			return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialNotEnrolled
		}
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}
	identity.AuthenticationMode = coreruntime.RuntimeAuthenticationMTLS
	return identity, nil
}

func (v *BindingVerifier) VerifyRuntimePrincipalBinding(
	ctx context.Context,
	credentialID uuid.UUID,
	device coreruntime.RuntimeDeviceIdentity,
) error {
	bound, err := v.ResolveRuntimeDeviceIdentity(ctx, credentialID)
	if err != nil {
		if errors.Is(err, errRuntimeCredentialNotEnrolled) {
			return v.verifyLegacyRuntimePrincipalBinding(ctx, credentialID, device)
		}
		return err
	}
	if bound.NodeID != device.NodeID ||
		bound.CertificateSerial != device.CertificateSerial ||
		bound.PublicKeyThumbprintSHA256 != device.PublicKeyThumbprintSHA256 {
		return errors.New("Agent Token is not bound to the presented Runtime Node key")
	}
	return nil
}

func (v *BindingVerifier) ResolveTokenOnlyRuntimeDeviceIdentity(
	ctx context.Context,
	credentialID uuid.UUID,
	nodeID uuid.UUID,
) (coreruntime.RuntimeDeviceIdentity, error) {
	if v == nil || v.pool == nil || credentialID == uuid.Nil || nodeID == uuid.Nil {
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}
	var identity coreruntime.RuntimeDeviceIdentity
	var status string
	var bindingMode string
	err := v.pool.QueryRow(ctx, `
SELECT node.node_id, node.device_certificate_serial,
       node.device_public_key_thumbprint, node.status, binding.binding_mode
FROM runtime_node_bindings binding
JOIN runtime_nodes node ON node.node_id = binding.node_id
WHERE binding.credential_id = $1`, credentialID).Scan(
		&identity.NodeID,
		&identity.CertificateSerial,
		&identity.PublicKeyThumbprintSHA256,
		&status,
		&bindingMode,
	)
	if err == nil {
		if identity.NodeID != nodeID ||
			(status != "active" && status != "draining") ||
			(bindingMode != "token_only" && bindingMode != "mtls") {
			return coreruntime.RuntimeDeviceIdentity{}, errors.New("Agent Token is not bound to the selected Runtime Node")
		}
		identity.AuthenticationMode = coreruntime.RuntimeAuthenticationTokenOnly
		identity.CertificateFingerprintSHA256 = identity.PublicKeyThumbprintSHA256
		return identity, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}

	err = v.pool.QueryRow(ctx, `
SELECT node.node_id, node.device_certificate_serial,
       node.device_public_key_thumbprint, node.status
FROM runtime_nodes node
WHERE node.node_id = $2
  AND EXISTS (
      SELECT 1
      FROM runtime_sessions session
      WHERE session.credential_id = $1
        AND session.node_id = node.node_id
        AND session.device_certificate_serial = node.device_certificate_serial
  )`, credentialID, nodeID).Scan(
		&identity.NodeID,
		&identity.CertificateSerial,
		&identity.PublicKeyThumbprintSHA256,
		&status,
	)
	if err == nil {
		if status != "active" && status != "draining" {
			return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
		}
		identity.AuthenticationMode = coreruntime.RuntimeAuthenticationTokenOnly
		identity.CertificateFingerprintSHA256 = identity.PublicKeyThumbprintSHA256
		return identity, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}

	var occupied bool
	if err = v.pool.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM runtime_nodes WHERE node_id = $1)`, nodeID).Scan(&occupied); err != nil || occupied {
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}
	return pendingTokenOnlyRuntimeDeviceIdentity(credentialID, nodeID), nil
}

func pendingTokenOnlyRuntimeDeviceIdentity(credentialID, nodeID uuid.UUID) coreruntime.RuntimeDeviceIdentity {
	thumbprint := tokenOnlyIdentityDigest("identity", credentialID, nodeID)
	return coreruntime.RuntimeDeviceIdentity{
		NodeID:                       nodeID,
		AuthenticationMode:           coreruntime.RuntimeAuthenticationTokenOnly,
		CertificateSerial:            tokenOnlyIdentityDigest("serial", credentialID, nodeID),
		CertificateFingerprintSHA256: thumbprint,
		PublicKeyThumbprintSHA256:    thumbprint,
	}
}

func tokenOnlyIdentityDigest(purpose string, credentialID, nodeID uuid.UUID) string {
	digest := sha256.Sum256([]byte(
		"openlinker/runtime/token-only/" + purpose + "/v1\x00" + credentialID.String() + "\x00" + nodeID.String(),
	))
	return hex.EncodeToString(digest[:])
}

func (v *BindingVerifier) verifyLegacyRuntimePrincipalBinding(
	ctx context.Context,
	credentialID uuid.UUID,
	device coreruntime.RuntimeDeviceIdentity,
) error {
	if v == nil || v.pool == nil || credentialID == uuid.Nil || device.NodeID == uuid.Nil {
		return errRuntimeCredentialInvalid
	}
	var related bool
	err := v.pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM runtime_sessions session
    JOIN runtime_nodes node ON node.node_id = session.node_id
    WHERE session.credential_id = $1
      AND session.node_id = $2
      AND session.device_certificate_serial = $3
      AND node.device_certificate_serial = $3
      AND node.device_public_key_thumbprint = $4
      AND node.status IN ('active', 'draining')
)`, credentialID, device.NodeID, device.CertificateSerial, device.PublicKeyThumbprintSHA256).Scan(&related)
	if err != nil || !related {
		return errors.New("Agent Token has no historical Session with the presented Runtime Node key")
	}
	return nil
}
