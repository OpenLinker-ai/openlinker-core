BEGIN;

ALTER TABLE run_artifact_chunks
    DROP CONSTRAINT IF EXISTS run_artifact_chunks_checksum_status_valid,
    DROP CONSTRAINT IF EXISTS run_artifact_chunks_declared_sha256_len,
    DROP CONSTRAINT IF EXISTS run_artifact_chunks_payload_sha256_len,
    DROP CONSTRAINT IF EXISTS run_artifact_chunks_parts_sha256_len;

ALTER TABLE run_artifact_chunks
    DROP COLUMN IF EXISTS checksum_status,
    DROP COLUMN IF EXISTS declared_sha256,
    DROP COLUMN IF EXISTS payload_sha256,
    DROP COLUMN IF EXISTS parts_sha256;

COMMIT;
