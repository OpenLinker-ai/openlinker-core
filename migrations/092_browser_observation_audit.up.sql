BEGIN;

-- Observation needs the live Browser identity of a running Run, which
-- browser_run_controls cannot provide: that row exists only while a challenge
-- has paused the Run for takeover. This projection is written from the ready
-- lifecycle event, so a normally executing Run is observable.
CREATE TABLE public.browser_observable_attempts (
    run_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    runtime_session_id uuid NOT NULL,
    -- Hashed, because the ready lifecycle event only ever publishes hashes.
    browser_session_sha256 text NOT NULL,
    session_epoch bigint NOT NULL,
    browser_attachment_sha256 text NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT browser_observable_attempts_pkey PRIMARY KEY (run_id),
    CONSTRAINT browser_observable_attempts_run_id_fkey
        FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE,
    CONSTRAINT browser_observable_attempts_epoch_positive
        CHECK (session_epoch > 0),
    CONSTRAINT browser_observable_attempts_digests_hex
        CHECK (
            browser_session_sha256 ~ '^[0-9a-f]{64}$'
            AND browser_attachment_sha256 ~ '^[0-9a-f]{64}$'
        )
);

-- Authenticated read-only observation is a separate capability from human
-- takeover: separate lease, separate authorization, separate audit. Sharing the
-- takeover audit table would make "was allowed to watch" indistinguishable from
-- "was allowed to drive" after the fact.
CREATE TABLE public.browser_observation_audits (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    observer_user_id uuid NOT NULL,
    -- Cross-user observation is an operator action and must carry a reason;
    -- an owner watching their own Run does not.
    observer_is_admin boolean DEFAULT false NOT NULL,
    reason text,
    session_epoch bigint NOT NULL,
    attachment_sha256 text NOT NULL,
    lease_id uuid NOT NULL,
    -- Persisted so a Core that exits mid-observation can still be reconciled:
    -- without it a crashed observation stays active forever.
    lease_expires_at timestamp with time zone NOT NULL,
    status text NOT NULL,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    end_reason text,
    frame_count bigint DEFAULT 0 NOT NULL,
    -- False whenever the count is a persisted lower bound rather than the exact
    -- total, so a consumer can never read a reconciled count as precise.
    frame_count_complete boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT browser_observation_audits_pkey PRIMARY KEY (id),
    CONSTRAINT browser_observation_audits_run_id_fkey
        FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE,
    CONSTRAINT browser_observation_audits_observer_fkey
        FOREIGN KEY (observer_user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT browser_observation_audits_status_known
        CHECK (status IN ('active', 'closed')),
    CONSTRAINT browser_observation_audits_epoch_positive
        CHECK (session_epoch > 0),
    CONSTRAINT browser_observation_audits_reason_bounded
        CHECK (reason IS NULL OR char_length(reason) BETWEEN 1 AND 500),
    CONSTRAINT browser_observation_audits_end_reason_bounded
        CHECK (end_reason IS NULL OR char_length(end_reason) BETWEEN 1 AND 100),
    -- An admin observation without a reason must not be storable at all.
    CONSTRAINT browser_observation_audits_admin_reason
        CHECK (NOT observer_is_admin OR reason IS NOT NULL),
    -- A closed record always has both an end time and why it ended, so a
    -- dangling record cannot masquerade as a normal finish.
    CONSTRAINT browser_observation_audits_closed_complete
        CHECK (
            status <> 'closed'
            OR (ended_at IS NOT NULL AND end_reason IS NOT NULL)
        ),
    CONSTRAINT browser_observation_audits_frame_count_non_negative
        CHECK (frame_count >= 0)
);

-- One live observation per Run: the Runtime lease is singular, so a second
-- active record would describe a state the Runtime cannot be in.
CREATE UNIQUE INDEX browser_observation_audits_active_run_idx
    ON public.browser_observation_audits (run_id)
    WHERE status = 'active';

CREATE INDEX browser_observation_audits_reconcile_idx
    ON public.browser_observation_audits (lease_expires_at)
    WHERE status = 'active';

CREATE INDEX browser_observation_audits_run_started_idx
    ON public.browser_observation_audits (run_id, started_at DESC);

COMMIT;
