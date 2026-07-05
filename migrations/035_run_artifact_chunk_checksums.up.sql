BEGIN;

ALTER TABLE run_artifact_chunks
    ADD COLUMN parts_sha256 TEXT,
    ADD COLUMN payload_sha256 TEXT,
    ADD COLUMN declared_sha256 TEXT,
    ADD COLUMN checksum_status TEXT NOT NULL DEFAULT 'not_provided';

ALTER TABLE run_artifact_chunks
    ADD CONSTRAINT run_artifact_chunks_parts_sha256_len
        CHECK (parts_sha256 IS NULL OR parts_sha256 ~ '^[a-f0-9]{64}$'),
    ADD CONSTRAINT run_artifact_chunks_payload_sha256_len
        CHECK (payload_sha256 IS NULL OR payload_sha256 ~ '^[a-f0-9]{64}$'),
    ADD CONSTRAINT run_artifact_chunks_declared_sha256_len
        CHECK (declared_sha256 IS NULL OR declared_sha256 ~ '^[a-f0-9]{64}$'),
    ADD CONSTRAINT run_artifact_chunks_checksum_status_valid
        CHECK (checksum_status IN ('not_provided', 'verified', 'mismatch', 'invalid'));

COMMIT;
