BEGIN;

CREATE TABLE proxy_run_artifacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proxy_run_id UUID NOT NULL REFERENCES proxy_runs(id) ON DELETE CASCADE,
    cloud_run_id UUID NOT NULL,
    source_artifact_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL DEFAULT 'data',
    title TEXT NOT NULL,
    content JSONB NOT NULL DEFAULT '{}'::jsonb,
    mime_type TEXT,
    file_uri TEXT,
    file_name TEXT,
    file_sha256 TEXT,
    file_size_bytes BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT proxy_run_artifacts_source_len CHECK (char_length(source_artifact_id) BETWEEN 1 AND 160),
    CONSTRAINT proxy_run_artifacts_title_len CHECK (char_length(title) BETWEEN 1 AND 300),
    CONSTRAINT proxy_run_artifacts_type_len CHECK (char_length(artifact_type) BETWEEN 1 AND 80),
    CONSTRAINT proxy_run_artifacts_mime_len CHECK (mime_type IS NULL OR char_length(mime_type) <= 200),
    CONSTRAINT proxy_run_artifacts_uri_len CHECK (file_uri IS NULL OR char_length(file_uri) <= 2000),
    CONSTRAINT proxy_run_artifacts_name_len CHECK (file_name IS NULL OR char_length(file_name) <= 500),
    CONSTRAINT proxy_run_artifacts_sha256_format CHECK (file_sha256 IS NULL OR file_sha256 ~ '^[a-f0-9]{64}$'),
    CONSTRAINT proxy_run_artifacts_size_nonnegative CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0),
    CONSTRAINT proxy_run_artifacts_unique_source UNIQUE (proxy_run_id, source_artifact_id)
);

CREATE INDEX idx_proxy_run_artifacts_proxy_run
    ON proxy_run_artifacts (proxy_run_id, created_at);

COMMIT;
