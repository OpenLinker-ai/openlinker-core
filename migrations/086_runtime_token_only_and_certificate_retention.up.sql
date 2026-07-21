BEGIN;

ALTER TABLE runtime_node_bindings
    ADD COLUMN binding_mode TEXT NOT NULL DEFAULT 'mtls',
    ADD CONSTRAINT runtime_node_bindings_mode_valid
        CHECK (binding_mode IN ('mtls', 'token_only'));

COMMENT ON COLUMN runtime_node_bindings.binding_mode IS
    'Authentication mode for this immutable credential-to-Node binding. token_only identity digests are selectors, never certificate factors.';

ALTER TABLE runtime_node_certificates
    ADD COLUMN certificate_pem TEXT,
    ADD COLUMN certificate_chain_pem TEXT,
    ADD COLUMN trust_bundle_pem TEXT,
    ADD COLUMN renew_after TIMESTAMPTZ,
    ADD CONSTRAINT runtime_node_certificates_replay_material_shape
        CHECK (
            (certificate_pem IS NULL
             AND certificate_chain_pem IS NULL
             AND trust_bundle_pem IS NULL
             AND renew_after IS NULL)
            OR
            (certificate_pem IS NOT NULL
             AND certificate_chain_pem IS NOT NULL
             AND trust_bundle_pem IS NOT NULL
             AND renew_after IS NOT NULL
             AND not_before < renew_after
             AND renew_after < not_after)
        );

CREATE INDEX idx_runtime_node_certificates_retention
    ON runtime_node_certificates(not_after, certificate_serial);

COMMIT;
