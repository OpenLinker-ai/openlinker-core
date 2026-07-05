BEGIN;

CREATE TABLE run_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    event_sequence INTEGER,
    role TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT run_messages_role_valid
        CHECK (role IN ('user', 'agent', 'tool', 'platform')),
    CONSTRAINT run_messages_content_len
        CHECK (char_length(content) <= 10000)
);

CREATE INDEX idx_run_messages_run
    ON run_messages (run_id, created_at ASC, id ASC);

CREATE INDEX idx_run_messages_event_sequence
    ON run_messages (run_id, event_sequence)
    WHERE event_sequence IS NOT NULL;

COMMIT;
