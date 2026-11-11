BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Phase A of the workflow pipeline completion: the data the new execution
-- engine needs (attempts, logical node state, per-attempt stop records and
-- run leases). The backfill only handles history. A release drains every
-- workflow run before migrating (migration 103's admission switch), so no
-- in-flight state is translated; this migration refuses to run otherwise.
LOCK TABLE public.workflow_runs, public.workflow_run_steps, public.workflow_step_launches
    IN SHARE ROW EXCLUSIVE MODE;

DO $$ BEGIN
 IF EXISTS (
  SELECT 1 FROM public.workflow_runs WHERE status IN ('pending', 'running', 'paused')
 ) THEN
  RAISE EXCEPTION '104_workflow_execution_attempts refused: drain workflow runs first (pending, running or paused runs exist)';
 END IF;
END $$;

-- Run leases. lease_epoch increases on every claim; lease_mode is 'execute'
-- for a normal executor and 'finalize' for the cleanup lease granted once
-- the recovery budget is spent. The three lease columns are set together.
ALTER TABLE public.workflow_runs
    ADD COLUMN lease_owner uuid,
    ADD COLUMN lease_expires_at timestamp with time zone,
    ADD COLUMN lease_epoch bigint DEFAULT 0 NOT NULL,
    ADD COLUMN lease_mode text,
    ADD COLUMN recovery_count integer DEFAULT 0 NOT NULL;

ALTER TABLE public.workflow_runs
    ADD CONSTRAINT workflow_runs_lease_epoch_nonnegative CHECK (lease_epoch >= 0),
    ADD CONSTRAINT workflow_runs_recovery_count_nonnegative CHECK (recovery_count >= 0),
    ADD CONSTRAINT workflow_runs_lease_mode_valid CHECK (lease_mode IS NULL OR lease_mode = ANY (ARRAY['execute'::text, 'finalize'::text])),
    ADD CONSTRAINT workflow_runs_lease_complete CHECK (
        ((lease_owner IS NULL) = (lease_expires_at IS NULL)) AND ((lease_owner IS NULL) = (lease_mode IS NULL))
    );

-- Attempts. Each (run, node) step row becomes one attempt. Historical rows
-- keep their status and output; they are numbered in the order Core has
-- listed them since migration 103 (sequence, created_at, id), so the
-- effective attempt (the highest) matches what readers already show.
ALTER TABLE public.workflow_run_steps
    ADD COLUMN attempt integer DEFAULT 1 NOT NULL,
    ADD COLUMN deadline_at timestamp with time zone,
    ADD COLUMN not_before timestamp with time zone,
    ADD COLUMN legacy_unfinished boolean DEFAULT false NOT NULL;

-- Backfilling history must not rewrite its timestamps.
ALTER TABLE public.workflow_run_steps DISABLE TRIGGER workflow_run_steps_set_updated_at;

WITH numbered AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY workflow_run_id, workflow_node_id
               ORDER BY sequence, created_at, id
           ) AS attempt
    FROM public.workflow_run_steps
)
UPDATE public.workflow_run_steps s
SET attempt = numbered.attempt
FROM numbered
WHERE s.id = numbered.id AND numbered.attempt <> 1;

-- A step still 'running' under a finished parent was interrupted by the old
-- engine (stale recovery or cancellation). Keep it as evidence; nothing
-- schedules, settles or cleans it.
UPDATE public.workflow_run_steps s
SET legacy_unfinished = true
FROM public.workflow_runs r
WHERE r.id = s.workflow_run_id
  AND s.status = 'running';

ALTER TABLE public.workflow_run_steps ENABLE TRIGGER workflow_run_steps_set_updated_at;

ALTER TABLE public.workflow_run_steps
    DROP CONSTRAINT workflow_run_steps_status_valid;

ALTER TABLE public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_status_valid CHECK (
        status = ANY (ARRAY['pending'::text, 'running'::text, 'success'::text, 'failed'::text, 'timeout'::text, 'canceled'::text])
    ),
    ADD CONSTRAINT workflow_run_steps_attempt_positive CHECK (attempt >= 1),
    ADD CONSTRAINT workflow_run_steps_legacy_unfinished_running CHECK (NOT legacy_unfinished OR status = 'running'),
    ADD CONSTRAINT workflow_run_steps_attempt_key UNIQUE (workflow_run_id, workflow_node_id, attempt);

-- Launches carry their attempt. A launch whose step link was cleared by the
-- old recovery points at that node's effective step; a launch whose node has
-- no step at all is kept as a legacy orphan with an empty link rather than a
-- fabricated attempt.
ALTER TABLE public.workflow_step_launches
    ADD COLUMN attempt integer DEFAULT 1 NOT NULL,
    ADD COLUMN legacy_orphan boolean DEFAULT false NOT NULL;

UPDATE public.workflow_step_launches l
SET attempt = s.attempt
FROM public.workflow_run_steps s
WHERE l.workflow_run_step_id = s.id
  AND s.attempt <> 1;

WITH effective AS (
    SELECT DISTINCT ON (workflow_run_id, workflow_node_id)
           id, workflow_run_id, workflow_node_id, attempt
    FROM public.workflow_run_steps
    ORDER BY workflow_run_id, workflow_node_id, attempt DESC
)
UPDATE public.workflow_step_launches l
SET workflow_run_step_id = effective.id,
    attempt = effective.attempt
FROM effective
WHERE l.workflow_run_step_id IS NULL
  AND effective.workflow_run_id = l.workflow_run_id
  AND effective.workflow_node_id = l.workflow_node_id
  AND NOT EXISTS (
      SELECT 1 FROM public.workflow_step_launches other
      WHERE other.workflow_run_step_id = effective.id
  );

UPDATE public.workflow_step_launches l
SET legacy_orphan = true
WHERE l.workflow_run_step_id IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM public.workflow_run_steps s
      WHERE s.workflow_run_id = l.workflow_run_id
        AND s.workflow_node_id = l.workflow_node_id
  );

ALTER TABLE public.workflow_step_launches
    DROP CONSTRAINT workflow_step_launches_pkey;

ALTER TABLE public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_pkey PRIMARY KEY (workflow_run_id, workflow_node_id, attempt),
    ADD CONSTRAINT workflow_step_launches_attempt_positive CHECK (attempt >= 1),
    ADD CONSTRAINT workflow_step_launches_legacy_orphan_unlinked CHECK (NOT legacy_orphan OR workflow_run_step_id IS NULL);

-- Logical node state, separate from attempt state. Edges and the scheduler
-- read only this. Terminal states record terminal_at and never change again.
CREATE TABLE public.workflow_run_nodes (
    workflow_run_id uuid NOT NULL,
    workflow_node_id uuid NOT NULL,
    node_key text NOT NULL,
    state text NOT NULL,
    effective_attempt integer,
    retry_exhausted boolean DEFAULT false NOT NULL,
    skip_reason text,
    terminal_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT workflow_run_nodes_pkey PRIMARY KEY (workflow_run_id, workflow_node_id),
    CONSTRAINT workflow_run_nodes_state_valid CHECK (state = ANY (ARRAY[
        'waiting'::text, 'running'::text, 'retry_wait'::text, 'stopping'::text,
        'success'::text, 'failed'::text, 'timeout'::text, 'skipped'::text, 'canceled'::text
    ])),
    CONSTRAINT workflow_run_nodes_terminal_at CHECK (
        (state = ANY (ARRAY['success'::text, 'failed'::text, 'timeout'::text, 'skipped'::text, 'canceled'::text]))
        = (terminal_at IS NOT NULL)
    ),
    CONSTRAINT workflow_run_nodes_effective_attempt_positive CHECK (effective_attempt IS NULL OR effective_attempt >= 1),
    CONSTRAINT workflow_run_nodes_skip_reason_length CHECK (skip_reason IS NULL OR char_length(skip_reason) <= 500),
    CONSTRAINT workflow_run_nodes_workflow_run_id_fkey FOREIGN KEY (workflow_run_id) REFERENCES public.workflow_runs(id) ON DELETE CASCADE,
    CONSTRAINT workflow_run_nodes_workflow_node_id_fkey FOREIGN KEY (workflow_node_id) REFERENCES public.workflow_nodes(id) ON DELETE RESTRICT
);

-- History: one logical row per node whose effective attempt finished. A node
-- whose effective attempt was interrupted (legacy_unfinished) or that has no
-- step at all gets no row; readers show it as having no execution record.
INSERT INTO public.workflow_run_nodes (
    workflow_run_id, workflow_node_id, node_key, state, effective_attempt,
    terminal_at, created_at, updated_at
)
SELECT effective.workflow_run_id, effective.workflow_node_id, effective.node_key,
       effective.status, effective.attempt,
       COALESCE(effective.finished_at, effective.updated_at),
       effective.created_at, effective.updated_at
FROM (
    SELECT DISTINCT ON (workflow_run_id, workflow_node_id)
           workflow_run_id, workflow_node_id, node_key, status, attempt,
           finished_at, created_at, updated_at
    FROM public.workflow_run_steps
    ORDER BY workflow_run_id, workflow_node_id, attempt DESC
) effective
WHERE effective.status IN ('success', 'failed');

-- Per-attempt stop records, separate from the public whole-run cancellation.
-- A stop is driven by its own claim (claim_owner, claim_epoch,
-- claim_expires_at), never by the run's execution lease.
CREATE TABLE public.workflow_step_stops (
    workflow_run_id uuid NOT NULL,
    workflow_node_id uuid NOT NULL,
    attempt integer NOT NULL,
    child_run_id uuid,
    reason text NOT NULL,
    state text DEFAULT 'requested'::text NOT NULL,
    claim_owner uuid,
    claim_epoch bigint DEFAULT 0 NOT NULL,
    claim_expires_at timestamp with time zone,
    requested_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    applied_at timestamp with time zone,
    finished_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT workflow_step_stops_pkey PRIMARY KEY (workflow_run_id, workflow_node_id, attempt),
    CONSTRAINT workflow_step_stops_reason_valid CHECK (reason = ANY (ARRAY['node_timeout'::text, 'workflow_failure'::text, 'workflow_cancel'::text])),
    CONSTRAINT workflow_step_stops_state_valid CHECK (state = ANY (ARRAY['requested'::text, 'stopping'::text, 'stopped'::text, 'unconfirmed'::text])),
    CONSTRAINT workflow_step_stops_claim_complete CHECK ((claim_owner IS NULL) = (claim_expires_at IS NULL)),
    CONSTRAINT workflow_step_stops_claim_epoch_nonnegative CHECK (claim_epoch >= 0),
    CONSTRAINT workflow_step_stops_finished CHECK (
        (state = ANY (ARRAY['stopped'::text, 'unconfirmed'::text])) = (finished_at IS NOT NULL)
    ),
    CONSTRAINT workflow_step_stops_attempt_fkey FOREIGN KEY (workflow_run_id, workflow_node_id, attempt)
        REFERENCES public.workflow_run_steps(workflow_run_id, workflow_node_id, attempt) ON DELETE RESTRICT,
    CONSTRAINT workflow_step_stops_child_run_id_fkey FOREIGN KEY (child_run_id) REFERENCES public.runs(id) ON DELETE RESTRICT
);

CREATE INDEX idx_workflow_step_stops_pending ON public.workflow_step_stops
    USING btree (state, updated_at)
    WHERE (state = ANY (ARRAY['requested'::text, 'stopping'::text]));

CREATE INDEX idx_workflow_runs_lease_expiry ON public.workflow_runs
    USING btree (lease_expires_at)
    WHERE (lease_owner IS NOT NULL);

COMMIT;
