BEGIN;

CREATE TABLE run_artifacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL DEFAULT 'json',
    title TEXT NOT NULL,
    content JSONB NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT run_artifacts_type_valid
        CHECK (artifact_type IN ('json', 'text', 'file', 'data')),
    CONSTRAINT run_artifacts_visibility_valid
        CHECK (visibility IN ('private', 'shared', 'public_example')),
    CONSTRAINT run_artifacts_title_len
        CHECK (char_length(title) BETWEEN 1 AND 200)
);

CREATE INDEX idx_run_artifacts_run
    ON run_artifacts (run_id, created_at ASC);

COMMIT;
