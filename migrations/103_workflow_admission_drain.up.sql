BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- One durable admission row for workflow runs. While it is 'draining', no new
-- workflow run is created and the worker claims no pending run; running runs
-- finish. Run creation reads the row FOR SHARE and the switch updates it, so a
-- committed switch is a barrier: nothing created or claimed after it can have
-- read 'open'. The state survives restarts and the next Core version, so a
-- release can drain, migrate and verify before reopening.
CREATE TABLE public.workflow_admission (
    singleton boolean DEFAULT true NOT NULL,
    state text DEFAULT 'open'::text NOT NULL,
    reason text,
    changed_by uuid,
    changed_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT workflow_admission_pkey PRIMARY KEY (singleton),
    CONSTRAINT workflow_admission_singleton CHECK (singleton),
    CONSTRAINT workflow_admission_state_valid CHECK (state = ANY (ARRAY['open'::text, 'draining'::text])),
    CONSTRAINT workflow_admission_reason_length CHECK (reason IS NULL OR char_length(reason) <= 500)
);
-- changed_by is an audit value, not a foreign key: a cascading TRUNCATE of
-- users must never remove the admission row, because run creation fails
-- closed without it.

INSERT INTO public.workflow_admission (singleton, state) VALUES (true, 'open');

-- actor_user_id keeps its meaning: the workflow run owner, the subject Runtime
-- authorizes child cancellation and evidence reads against. requested_by and
-- requested_via record who actually asked, so an administrator draining other
-- users' runs is audited without changing that authorization subject.
ALTER TABLE public.workflow_run_cancellations
    ADD COLUMN requested_by uuid,
    ADD COLUMN requested_via text DEFAULT 'owner'::text NOT NULL;

UPDATE public.workflow_run_cancellations
SET requested_by = actor_user_id,
    requested_via = CASE reason_code
        WHEN 'OWNER_CANCEL_REQUESTED' THEN 'owner'
        ELSE 'external'
    END;

ALTER TABLE public.workflow_run_cancellations
    ALTER COLUMN requested_by SET NOT NULL;

ALTER TABLE public.workflow_run_cancellations
    ADD CONSTRAINT workflow_run_cancellations_requested_via_valid
    CHECK (requested_via = ANY (ARRAY['owner'::text, 'external'::text, 'admin'::text])) NOT VALID;

ALTER TABLE public.workflow_run_cancellations
    VALIDATE CONSTRAINT workflow_run_cancellations_requested_via_valid;

ALTER TABLE public.workflow_run_cancellations
    ADD CONSTRAINT workflow_run_cancellations_requested_by_fkey
    FOREIGN KEY (requested_by) REFERENCES public.users(id) ON DELETE RESTRICT NOT VALID;

ALTER TABLE public.workflow_run_cancellations
    VALIDATE CONSTRAINT workflow_run_cancellations_requested_by_fkey;

COMMIT;
