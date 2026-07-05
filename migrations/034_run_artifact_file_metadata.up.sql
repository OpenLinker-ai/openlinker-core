BEGIN;

ALTER TABLE run_artifacts
    ADD COLUMN mime_type TEXT,
    ADD COLUMN file_uri TEXT,
    ADD COLUMN file_name TEXT,
    ADD COLUMN file_sha256 TEXT,
    ADD COLUMN file_size_bytes BIGINT;

ALTER TABLE run_artifacts
    ADD CONSTRAINT run_artifacts_mime_type_len
        CHECK (mime_type IS NULL OR char_length(mime_type) BETWEEN 1 AND 200),
    ADD CONSTRAINT run_artifacts_file_uri_len
        CHECK (file_uri IS NULL OR char_length(file_uri) BETWEEN 1 AND 2000),
    ADD CONSTRAINT run_artifacts_file_name_len
        CHECK (file_name IS NULL OR char_length(file_name) BETWEEN 1 AND 500),
    ADD CONSTRAINT run_artifacts_file_sha256_len
        CHECK (file_sha256 IS NULL OR file_sha256 ~ '^[A-Fa-f0-9]{64}$'),
    ADD CONSTRAINT run_artifacts_file_size_nonnegative
        CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0);

COMMIT;
