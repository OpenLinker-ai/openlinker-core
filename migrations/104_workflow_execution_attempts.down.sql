BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- The rollback exists for disposable databases. It refuses once the new
-- engine has produced anything the old schema cannot hold: a second attempt,
-- a stop record, a lease, a recovery, a pending/timeout/canceled attempt, or a
-- workflow run that is not finished. The backfilled logical node rows are
-- derived data and are dropped.
LOCK TABLE public.workflow_runs, public.workflow_run_steps, public.workflow_step_launches,
    public.workflow_run_nodes, public.workflow_step_stops
    IN ACCESS EXCLUSIVE MODE;

DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM public.workflow_runs WHERE status IN ('pending', 'running', 'paused')) THEN
  RAISE EXCEPTION '104_workflow_execution_attempts rollback refused: workflow runs are not drained';
 END IF;
 IF EXISTS (SELECT 1 FROM public.workflow_step_stops) THEN
  RAISE EXCEPTION '104_workflow_execution_attempts rollback refused: workflow step stop records exist';
 END IF;
 IF EXISTS (
  SELECT 1 FROM public.workflow_runs
  WHERE lease_owner IS NOT NULL OR lease_epoch <> 0 OR recovery_count <> 0
 ) THEN
  RAISE EXCEPTION '104_workflow_execution_attempts rollback refused: workflow run leases were used';
 END IF;
 IF EXISTS (
  SELECT 1 FROM public.workflow_run_steps
  WHERE status IN ('pending', 'timeout', 'canceled') OR deadline_at IS NOT NULL OR not_before IS NOT NULL
 ) THEN
  RAISE EXCEPTION '104_workflow_execution_attempts rollback refused: new-engine attempts exist';
 END IF;
 IF EXISTS (
  SELECT 1 FROM public.workflow_step_launches l
  WHERE l.attempt <> 1
    AND NOT EXISTS (
        SELECT 1 FROM public.workflow_run_steps s
        WHERE s.id = l.workflow_run_step_id AND s.attempt = l.attempt
    )
 ) THEN
  RAISE EXCEPTION '104_workflow_execution_attempts rollback refused: launches for later attempts exist';
 END IF;
END $$;

DROP TABLE public.workflow_step_stops;
DROP TABLE public.workflow_run_nodes;
DROP INDEX public.idx_workflow_run_steps_open_deadline;
DROP INDEX public.idx_workflow_runs_lease_expiry;

ALTER TABLE public.workflow_step_launches
    DROP CONSTRAINT workflow_step_launches_legacy_orphan_unlinked,
    DROP CONSTRAINT workflow_step_launches_attempt_positive,
    DROP CONSTRAINT workflow_step_launches_pkey;
ALTER TABLE public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_pkey PRIMARY KEY (workflow_run_id, workflow_node_id);
ALTER TABLE public.workflow_step_launches
    DROP COLUMN legacy_orphan,
    DROP COLUMN attempt;

ALTER TABLE public.workflow_run_steps
    DROP CONSTRAINT workflow_run_steps_attempt_key,
    DROP CONSTRAINT workflow_run_steps_legacy_unfinished_running,
    DROP CONSTRAINT workflow_run_steps_source_valid,
    DROP CONSTRAINT workflow_run_steps_attempt_positive,
    DROP CONSTRAINT workflow_run_steps_status_valid;
ALTER TABLE public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_status_valid CHECK ((status = ANY (ARRAY['running'::text, 'success'::text, 'failed'::text])));
ALTER TABLE public.workflow_run_steps
    DROP COLUMN source,
    DROP COLUMN legacy_unfinished,
    DROP COLUMN not_before,
    DROP COLUMN deadline_at,
    DROP COLUMN attempt;

ALTER TABLE public.workflow_runs
    DROP CONSTRAINT workflow_runs_lease_complete,
    DROP CONSTRAINT workflow_runs_lease_mode_valid,
    DROP CONSTRAINT workflow_runs_recovery_count_nonnegative,
    DROP CONSTRAINT workflow_runs_lease_epoch_nonnegative;
ALTER TABLE public.workflow_runs
    DROP COLUMN recovery_count,
    DROP COLUMN lease_mode,
    DROP COLUMN lease_epoch,
    DROP COLUMN lease_expires_at,
    DROP COLUMN lease_owner,
    DROP COLUMN unconfirmed_children_acknowledged_by;

COMMIT;
