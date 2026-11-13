BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Rolling back while draining would silently reopen admission, and dropping
-- the request columns would erase which cancellations an administrator made.
-- Refuse both instead. Locks are taken before the checks so no concurrent
-- write can change the answer between the check and the drop.
LOCK TABLE public.workflow_admission IN ACCESS EXCLUSIVE MODE;
LOCK TABLE public.workflow_run_cancellations IN ACCESS EXCLUSIVE MODE;

DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM public.workflow_admission WHERE state <> 'open') THEN
  RAISE EXCEPTION '103_workflow_admission_drain rollback refused: workflow admission is draining';
 END IF;
 IF EXISTS (SELECT 1 FROM public.workflow_run_cancellations WHERE requested_via = 'admin') THEN
  RAISE EXCEPTION '103_workflow_admission_drain rollback refused: administrator cancellations exist';
 END IF;
END $$;

ALTER TABLE public.workflow_run_cancellations
    DROP CONSTRAINT workflow_run_cancellations_requested_by_fkey,
    DROP CONSTRAINT workflow_run_cancellations_requested_via_valid,
    DROP COLUMN requested_via,
    DROP COLUMN requested_by;

DROP TABLE public.workflow_admission;

COMMIT;
