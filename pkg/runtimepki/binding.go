package runtimepki

import (
	"context"
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
	err := v.pool.QueryRow(ctx, `
SELECT node.node_id, node.device_certificate_serial,
       certificate.certificate_fingerprint,
       binding.public_key_thumbprint, node.status
FROM runtime_node_bindings binding
JOIN runtime_nodes node ON node.node_id = binding.node_id
JOIN LATERAL (
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
	)
	if err != nil || (status != "active" && status != "draining") {
		if errors.Is(err, pgx.ErrNoRows) {
			return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialNotEnrolled
		}
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}
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
	if nodeID == uuid.Nil {
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}
	bound, err := v.ResolveRuntimeDeviceIdentity(ctx, credentialID)
	if err == nil {
		if bound.NodeID != nodeID {
			return coreruntime.RuntimeDeviceIdentity{}, errors.New("Agent Token is not bound to the selected Runtime Node")
		}
		return bound, nil
	}
	if !errors.Is(err, errRuntimeCredentialNotEnrolled) {
		return coreruntime.RuntimeDeviceIdentity{}, err
	}
	if v == nil || v.pool == nil || credentialID == uuid.Nil {
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}
	var identity coreruntime.RuntimeDeviceIdentity
	var status string
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
	if err != nil || (status != "active" && status != "draining") {
		return coreruntime.RuntimeDeviceIdentity{}, errRuntimeCredentialInvalid
	}
	// Token-only transport has no presented leaf. This field remains populated
	// with a stable database-backed digest so the shared Runtime principal shape
	// stays valid; it is not treated as a certificate authentication factor.
	identity.CertificateFingerprintSHA256 = identity.PublicKeyThumbprintSHA256
	return identity, nil
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
