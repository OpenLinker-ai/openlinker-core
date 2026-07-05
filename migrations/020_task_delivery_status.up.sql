BEGIN;

ALTER TABLE task_queries
    ADD COLUMN delivery_status TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN delivery_visibility TEXT NOT NULL DEFAULT 'private',
    ADD COLUMN delivery_artifact JSONB,
    ADD COLUMN accepted_at TIMESTAMPTZ,
    ADD COLUMN revision_requested_at TIMESTAMPTZ,
    ADD COLUMN revision_note TEXT,
    ADD CONSTRAINT task_queries_delivery_status_valid CHECK (
        delivery_status IN ('pending', 'submitted', 'revision_requested', 'accepted', 'failed')
    ),
    ADD CONSTRAINT task_queries_delivery_visibility_valid CHECK (
        delivery_visibility IN ('private', 'shared', 'public_example')
    ),
    ADD CONSTRAINT task_queries_revision_note_len CHECK (
        revision_note IS NULL OR char_length(revision_note) <= 2000
    ),
    ADD CONSTRAINT task_queries_delivery_acceptance_consistency CHECK (
        accepted_at IS NULL OR delivery_status = 'accepted'
    );

UPDATE task_queries
SET delivery_status = 'submitted',
    delivery_visibility = 'private'
WHERE completed_at IS NOT NULL
  AND delivery_status = 'pending';

CREATE INDEX idx_task_queries_delivery_status
    ON task_queries (delivery_status, created_at DESC);

COMMIT;
