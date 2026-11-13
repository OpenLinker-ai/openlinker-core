BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.workflow_runs DROP CONSTRAINT workflow_runs_revision_nonnegative;
ALTER TABLE public.workflow_runs DROP COLUMN revision;

COMMIT;
