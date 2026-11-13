BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Phase D of the workflow pipeline completion: a monotonic revision per
-- workflow run. Every write to the run, its logical nodes, attempts and stop
-- records increments it in the same transaction, which also sends
-- pg_notify('workflow_run.changed', run id) from application code; run event
-- streams are a snapshot plus this revision. Adding the column needs no
-- drain: a run written by an older Core simply keeps its revision.
ALTER TABLE public.workflow_runs
    ADD COLUMN revision bigint DEFAULT 0 NOT NULL;

ALTER TABLE public.workflow_runs
    ADD CONSTRAINT workflow_runs_revision_nonnegative CHECK (revision >= 0);

COMMIT;
