BEGIN;

ALTER TABLE task_queries
    DROP COLUMN IF EXISTS mcp_tools;

COMMIT;
