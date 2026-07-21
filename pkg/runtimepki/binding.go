package runtimepki

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

type BindingVerifier struct {
	pool *pgxpool.Pool
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
			return coreruntime.RuntimeDeviceIdentity{}, errors.New("runtime credential is not enrolled")
		}
		return coreruntime.RuntimeDeviceIdentity{}, errors.New("runtime credential binding is invalid")
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
		// Compatibility window for Nodes enrolled by the former offline CLI:
		// their mTLS leaf is still independently authenticated, but they have no
		// binding row to migrate until their next explicit credential rotation.
		if device.NodeID != uuid.Nil && strings.Contains(err.Error(), "not enrolled") {
			return nil
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
