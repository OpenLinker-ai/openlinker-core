BEGIN;

LOCK TABLE public.agents IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE public.agents
    ADD COLUMN browser_execution_profile boolean
        DEFAULT false NOT NULL;

ALTER TABLE public.agents
    ADD CONSTRAINT agents_browser_execution_profile_private
    CHECK (
        NOT browser_execution_profile
        OR visibility = 'private'::text
    );

CREATE TABLE public.runtime_agent_execution_profiles (
    agent_id uuid NOT NULL,
    execution_profile text DEFAULT 'browser'::text NOT NULL,
    classified_at timestamp with time zone
        DEFAULT clock_timestamp() NOT NULL,
    classified_by_credential_id uuid NOT NULL,
    source_runtime_session_id uuid NOT NULL,
    last_confirmed_at timestamp with time zone
        DEFAULT clock_timestamp() NOT NULL,
    reset_at timestamp with time zone,
    reset_by_user_id uuid,
    reset_reason text,
    profile_purge_attested boolean,
    CONSTRAINT runtime_agent_execution_profiles_profile_valid
        CHECK (
            execution_profile = ANY (
                ARRAY['standard'::text, 'browser'::text]
            )
        ),
    CONSTRAINT runtime_agent_execution_profiles_reset_consistent
        CHECK (
            (
                execution_profile = 'browser'::text
                AND reset_at IS NULL
                AND reset_by_user_id IS NULL
                AND reset_reason IS NULL
                AND profile_purge_attested IS NULL
            )
            OR
            (
                execution_profile = 'standard'::text
                AND reset_at IS NOT NULL
                AND reset_by_user_id IS NOT NULL
                AND char_length(btrim(reset_reason)) BETWEEN 1 AND 500
                AND profile_purge_attested IS TRUE
            )
        )
);

ALTER TABLE ONLY public.runtime_agent_execution_profiles
    ADD CONSTRAINT runtime_agent_execution_profiles_pkey
    PRIMARY KEY (agent_id);

ALTER TABLE ONLY public.runtime_agent_execution_profiles
    ADD CONSTRAINT runtime_agent_execution_profiles_agent_id_fkey
    FOREIGN KEY (agent_id)
    REFERENCES public.agents(id)
    ON DELETE CASCADE;

ALTER TABLE ONLY public.runtime_agent_execution_profiles
    ADD CONSTRAINT runtime_agent_execution_profiles_credential_id_fkey
    FOREIGN KEY (classified_by_credential_id)
    REFERENCES public.agent_tokens(id)
    ON DELETE RESTRICT;

ALTER TABLE ONLY public.runtime_agent_execution_profiles
    ADD CONSTRAINT runtime_agent_execution_profiles_reset_by_user_id_fkey
    FOREIGN KEY (reset_by_user_id)
    REFERENCES public.users(id)
    ON DELETE RESTRICT;

CREATE INDEX runtime_agent_execution_profiles_browser_idx
    ON public.runtime_agent_execution_profiles USING btree (agent_id)
    WHERE execution_profile = 'browser'::text;

COMMIT;
