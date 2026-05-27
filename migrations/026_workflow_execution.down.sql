BEGIN;

DROP TRIGGER IF EXISTS workflow_run_steps_set_updated_at ON workflow_run_steps;
DROP INDEX IF EXISTS idx_workflow_run_steps_run;
DROP TABLE IF EXISTS workflow_run_steps;

DROP TRIGGER IF EXISTS workflow_runs_set_updated_at ON workflow_runs;
DROP INDEX IF EXISTS idx_workflow_runs_user;
DROP INDEX IF EXISTS idx_workflow_runs_workflow;
DROP TABLE IF EXISTS workflow_runs;

DROP INDEX IF EXISTS idx_workflow_nodes_order;
DROP INDEX IF EXISTS idx_workflow_nodes_key;
DROP TABLE IF EXISTS workflow_nodes;

DROP TRIGGER IF EXISTS workflows_set_updated_at ON workflows;
DROP INDEX IF EXISTS idx_workflows_user;
DROP TABLE IF EXISTS workflows;

COMMIT;
