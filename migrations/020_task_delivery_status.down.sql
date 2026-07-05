BEGIN;

DROP INDEX IF EXISTS idx_task_queries_delivery_status;

ALTER TABLE task_queries
    DROP CONSTRAINT IF EXISTS task_queries_delivery_acceptance_consistency,
    DROP CONSTRAINT IF EXISTS task_queries_revision_note_len,
    DROP CONSTRAINT IF EXISTS task_queries_delivery_visibility_valid,
    DROP CONSTRAINT IF EXISTS task_queries_delivery_status_valid,
    DROP COLUMN IF EXISTS revision_note,
    DROP COLUMN IF EXISTS revision_requested_at,
    DROP COLUMN IF EXISTS accepted_at,
    DROP COLUMN IF EXISTS delivery_artifact,
    DROP COLUMN IF EXISTS delivery_visibility,
    DROP COLUMN IF EXISTS delivery_status;

COMMIT;
