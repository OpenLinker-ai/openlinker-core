BEGIN;

ALTER TABLE run_artifacts
    DROP CONSTRAINT IF EXISTS run_artifacts_file_size_nonnegative,
    DROP CONSTRAINT IF EXISTS run_artifacts_file_sha256_len,
    DROP CONSTRAINT IF EXISTS run_artifacts_file_name_len,
    DROP CONSTRAINT IF EXISTS run_artifacts_file_uri_len,
    DROP CONSTRAINT IF EXISTS run_artifacts_mime_type_len;

ALTER TABLE run_artifacts
    DROP COLUMN IF EXISTS file_size_bytes,
    DROP COLUMN IF EXISTS file_sha256,
    DROP COLUMN IF EXISTS file_name,
    DROP COLUMN IF EXISTS file_uri,
    DROP COLUMN IF EXISTS mime_type;

COMMIT;
