BEGIN;

-- Core-owned Runtime certificate authorities. Private keys are encrypted by
-- the application before they enter PostgreSQL; every Core replica shares the
-- same trust hierarchy without a manually distributed CA key file.
CREATE TABLE runtime_pki_authorities (
    authority_id TEXT PRIMARY KEY,
    certificate_pem TEXT NOT NULL,
    encrypted_private_key BYTEA NOT NULL,
    not_before TIMESTAMPTZ NOT NULL,
    not_after TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT runtime_pki_authorities_id_valid
        CHECK (authority_id IN ('root', 'client-intermediate', 'server-intermediate')),
    CONSTRAINT runtime_pki_authorities_validity
        CHECK (not_before < not_after)
);

-- One Agent Token Credential owns exactly one Runtime Node public key. The
-- Node may serve several sessions for the Agent, but the Credential cannot be
-- rebound to another Node or another key.
CREATE TABLE runtime_node_bindings (
    credential_id UUID PRIMARY KEY REFERENCES agent_tokens(id) ON DELETE RESTRICT,
    node_id UUID NOT NULL UNIQUE REFERENCES runtime_nodes(node_id) ON DELETE RESTRICT,
    agent_id UUID NOT NULL,
    public_key_thumbprint TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT runtime_node_bindings_token_agent_fk
        FOREIGN KEY (credential_id, agent_id)
        REFERENCES agent_tokens(id, agent_id)
        ON DELETE RESTRICT,
    CONSTRAINT runtime_node_bindings_thumbprint_format
        CHECK (public_key_thumbprint ~ '^[a-f0-9]{64}$')
);

-- Certificate identity/validity rows are immutable after issuance; only the
-- explicit revocation marker may be added. Renewal creates a new leaf and
-- leaves the prior leaf usable until its 24-hour validity window ends, which
-- makes credential replacement race-free for in-flight connections.
CREATE TABLE runtime_node_certificates (
    certificate_serial TEXT PRIMARY KEY,
    node_id UUID NOT NULL REFERENCES runtime_nodes(node_id) ON DELETE RESTRICT,
    public_key_thumbprint TEXT NOT NULL,
    certificate_fingerprint TEXT NOT NULL UNIQUE,
    not_before TIMESTAMPTZ NOT NULL,
    not_after TIMESTAMPTZ NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    revoked_at TIMESTAMPTZ,
    CONSTRAINT runtime_node_certificates_serial_format
        CHECK (certificate_serial ~ '^[a-f0-9]+$'),
    CONSTRAINT runtime_node_certificates_thumbprint_format
        CHECK (public_key_thumbprint ~ '^[a-f0-9]{64}$'),
    CONSTRAINT runtime_node_certificates_fingerprint_format
        CHECK (certificate_fingerprint ~ '^[a-f0-9]{64}$'),
    CONSTRAINT runtime_node_certificates_validity
        CHECK (not_before < not_after),
    CONSTRAINT runtime_node_certificates_revocation_time
        CHECK (revoked_at IS NULL OR revoked_at >= not_before)
);

CREATE INDEX idx_runtime_node_certificates_active_node
    ON runtime_node_certificates(node_id, not_after DESC)
    WHERE revoked_at IS NULL;

-- Preserve manually enrolled Nodes during the compatibility window. Their
-- original leaf remains resolvable through runtime_nodes; newly issued leaves
-- are resolved through runtime_node_certificates.

COMMIT;
