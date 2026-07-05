BEGIN;

ALTER TABLE task_queries
    ADD COLUMN mcp_tools TEXT[] NOT NULL DEFAULT '{}';

COMMIT;
