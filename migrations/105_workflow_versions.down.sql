BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Rollback only works while every workflow is still at its backfilled
-- version 1 and every run references a node that still exists in the
-- workflow_nodes projection. Later versions are refused, never truncated.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM public.workflow_versions WHERE version > 1 OR semantics <> 'v1') THEN
  RAISE EXCEPTION '105_workflow_versions rollback refused: workflows have versions beyond the backfill';
 END IF;
END $$;

DROP INDEX public.idx_workflow_runs_version;
ALTER TABLE public.workflow_runs DROP CONSTRAINT workflow_runs_workflow_version_fkey;
ALTER TABLE public.workflow_runs DROP COLUMN workflow_version_id;
ALTER TABLE public.workflows DROP CONSTRAINT workflows_current_version_fkey;
ALTER TABLE public.workflows DROP COLUMN current_version_id;

ALTER TABLE public.workflow_run_nodes
    ADD CONSTRAINT workflow_run_nodes_workflow_node_id_fkey FOREIGN KEY (workflow_node_id)
        REFERENCES public.workflow_nodes(id) ON DELETE RESTRICT;
ALTER TABLE public.workflow_run_nodes DROP CONSTRAINT workflow_run_nodes_workflow_version_node_id_fkey;
ALTER TABLE public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_workflow_node_id_fkey FOREIGN KEY (workflow_node_id)
        REFERENCES public.workflow_nodes(id) ON DELETE RESTRICT;
ALTER TABLE public.workflow_step_launches DROP CONSTRAINT workflow_step_launches_workflow_version_node_id_fkey;
ALTER TABLE public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_workflow_node_id_fkey FOREIGN KEY (workflow_node_id)
        REFERENCES public.workflow_nodes(id) ON DELETE RESTRICT;
ALTER TABLE public.workflow_run_steps DROP CONSTRAINT workflow_run_steps_workflow_version_node_id_fkey;

DROP TABLE public.workflow_version_nodes;
DROP TABLE public.workflow_versions;

COMMIT;
