-- 004_run_events.up.sql
-- Run event stream foundation for SSE / Push / audit.
-- 关联 docs/27-registry-bridge-availability-a2a-priority.md 6.7 / 6.8

BEGIN;

CREATE TABLE run_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    parent_run_id UUID REFERENCES runs(id) ON DELETE SET NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT run_events_sequence_positive CHECK (sequence > 0),
    CONSTRAINT run_events_type_format CHECK (
        event_type ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$'
    ),
    CONSTRAINT run_events_run_sequence_unique UNIQUE (run_id, sequence)
);

CREATE INDEX idx_run_events_run_sequence ON run_events (run_id, sequence);
CREATE INDEX idx_run_events_parent_run ON run_events (parent_run_id) WHERE parent_run_id IS NOT NULL;
CREATE INDEX idx_run_events_type_time ON run_events (event_type, created_at DESC);

COMMIT;
