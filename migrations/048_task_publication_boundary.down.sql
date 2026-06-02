DROP INDEX IF EXISTS idx_task_queries_public_board;

ALTER TABLE task_queries
    DROP CONSTRAINT IF EXISTS task_queries_publication_consistency,
    DROP CONSTRAINT IF EXISTS task_queries_public_summary_len,
    DROP CONSTRAINT IF EXISTS task_queries_visibility_valid,
    DROP COLUMN IF EXISTS published_at,
    DROP COLUMN IF EXISTS public_summary,
    DROP COLUMN IF EXISTS visibility;
