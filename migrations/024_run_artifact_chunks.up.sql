BEGIN;

ALTER TABLE run_artifacts
    ADD COLUMN source_artifact_id TEXT;

CREATE UNIQUE INDEX idx_run_artifacts_run_source
    ON run_artifacts (run_id, source_artifact_id)
    WHERE source_artifact_id IS NOT NULL;

CREATE TABLE run_artifact_chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    run_artifact_id UUID NOT NULL REFERENCES run_artifacts(id) ON DELETE CASCADE,
    source_artifact_id TEXT NOT NULL,
    event_sequence INTEGER,
    chunk_index INTEGER NOT NULL,
    append BOOLEAN NOT NULL DEFAULT TRUE,
    last_chunk BOOLEAN NOT NULL DEFAULT FALSE,
    parts JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT run_artifact_chunks_source_len
        CHECK (char_length(source_artifact_id) BETWEEN 1 AND 200),
    CONSTRAINT run_artifact_chunks_index_nonnegative
        CHECK (chunk_index >= 0),
    CONSTRAINT run_artifact_chunks_parts_array
        CHECK (jsonb_typeof(parts) = 'array')
);

CREATE UNIQUE INDEX idx_run_artifact_chunks_run_source_index
    ON run_artifact_chunks (run_id, source_artifact_id, chunk_index);

CREATE INDEX idx_run_artifact_chunks_artifact
    ON run_artifact_chunks (run_artifact_id, chunk_index ASC);

COMMIT;
