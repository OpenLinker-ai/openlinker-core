BEGIN;

CREATE TABLE public.browser_run_controls (
    run_id uuid NOT NULL,
    user_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    lease_id uuid NOT NULL,
    fencing_token bigint NOT NULL,
    node_id uuid NOT NULL,
    worker_id text NOT NULL,
    runtime_session_id uuid NOT NULL,
    browser_session_id uuid NOT NULL,
    session_epoch bigint NOT NULL,
    attachment_id uuid NOT NULL,
    control_epoch bigint NOT NULL,
    controller text NOT NULL,
    state text NOT NULL,
    pause_reason text NOT NULL,
    run_deadline_at timestamp with time zone NOT NULL,
    pause_expires_at timestamp with time zone NOT NULL,
    human_expires_at timestamp with time zone,
    claimed_by_user_id uuid,
    claimed_at timestamp with time zone,
    released_at timestamp with time zone,
    resumed_at timestamp with time zone,
    input_count bigint DEFAULT 0 NOT NULL,
    frame_count bigint DEFAULT 0 NOT NULL,
    human_duration_ms bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT browser_run_controls_pkey PRIMARY KEY (run_id),
    CONSTRAINT browser_run_controls_run_id_fkey
        FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE,
    CONSTRAINT browser_run_controls_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT browser_run_controls_agent_id_fkey
        FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE CASCADE,
    CONSTRAINT browser_run_controls_claimed_by_user_id_fkey
        FOREIGN KEY (claimed_by_user_id) REFERENCES public.users(id) ON DELETE SET NULL,
    CONSTRAINT browser_run_controls_epoch_positive
        CHECK (session_epoch > 0 AND control_epoch > 0 AND fencing_token > 0),
    CONSTRAINT browser_run_controls_worker_bounded
        CHECK (char_length(btrim(worker_id)) BETWEEN 1 AND 200),
    CONSTRAINT browser_run_controls_controller_valid
        CHECK (controller = ANY (ARRAY['agent'::text, 'none'::text, 'human'::text])),
    CONSTRAINT browser_run_controls_state_valid
        CHECK (state = ANY (ARRAY['paused'::text, 'human'::text, 'released'::text, 'resumed'::text, 'closed'::text])),
    CONSTRAINT browser_run_controls_reason_bounded
        CHECK (char_length(btrim(pause_reason)) BETWEEN 1 AND 120),
    CONSTRAINT browser_run_controls_duration_nonnegative
        CHECK (human_duration_ms >= 0),
    CONSTRAINT browser_run_controls_human_consistent
        CHECK (
            (state = 'human'::text AND controller = 'human'::text
                AND claimed_by_user_id IS NOT NULL
                AND claimed_at IS NOT NULL
                AND human_expires_at IS NOT NULL)
            OR
            (state <> 'human'::text AND controller <> 'human'::text)
        )
);

CREATE INDEX browser_run_controls_expiry_idx
    ON public.browser_run_controls (state, pause_expires_at, human_expires_at)
    WHERE state = ANY (ARRAY['paused'::text, 'human'::text, 'released'::text]);

CREATE TABLE public.browser_human_control_audits (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    user_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    browser_session_id uuid NOT NULL,
    session_epoch bigint NOT NULL,
    attachment_id uuid NOT NULL,
    control_epoch bigint NOT NULL,
    controller text NOT NULL,
    pause_reason text NOT NULL,
    claimed_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone NOT NULL,
    duration_ms bigint NOT NULL,
    end_reason text NOT NULL,
    input_count bigint DEFAULT 0 NOT NULL,
    frame_count bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT browser_human_control_audits_pkey PRIMARY KEY (id),
    CONSTRAINT browser_human_control_audits_run_id_fkey
        FOREIGN KEY (run_id) REFERENCES public.runs(id) ON DELETE CASCADE,
    CONSTRAINT browser_human_control_audits_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT browser_human_control_audits_epoch_positive
        CHECK (session_epoch > 0 AND control_epoch > 0),
    CONSTRAINT browser_human_control_audits_controller_human
        CHECK (controller = 'human'::text),
    CONSTRAINT browser_human_control_audits_pause_reason_bounded
        CHECK (char_length(btrim(pause_reason)) BETWEEN 1 AND 120),
    CONSTRAINT browser_human_control_audits_window_valid
        CHECK (ended_at >= claimed_at AND duration_ms >= 0),
    CONSTRAINT browser_human_control_audits_reason_bounded
        CHECK (char_length(btrim(end_reason)) BETWEEN 1 AND 120),
    CONSTRAINT browser_human_control_audits_counts_nonnegative
        CHECK (input_count >= 0 AND frame_count >= 0)
);

CREATE INDEX browser_human_control_audits_run_created_idx
    ON public.browser_human_control_audits (run_id, created_at DESC);

COMMIT;
