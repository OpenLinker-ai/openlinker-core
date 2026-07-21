BEGIN;

DROP INDEX IF EXISTS idx_runtime_node_certificates_retention;

ALTER TABLE runtime_node_certificates
    DROP CONSTRAINT IF EXISTS runtime_node_certificates_replay_material_shape,
    DROP COLUMN IF EXISTS renew_after,
    DROP COLUMN IF EXISTS trust_bundle_pem,
    DROP COLUMN IF EXISTS certificate_chain_pem,
    DROP COLUMN IF EXISTS certificate_pem;

ALTER TABLE runtime_node_bindings
    DROP CONSTRAINT IF EXISTS runtime_node_bindings_mode_valid,
    DROP COLUMN IF EXISTS binding_mode;

COMMIT;
