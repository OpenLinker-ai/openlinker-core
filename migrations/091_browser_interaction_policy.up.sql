BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.runtime_agent_execution_profiles
    ADD COLUMN interaction_policy text DEFAULT 'restricted'::text NOT NULL,
    ADD COLUMN browser_mutation_origins text[] DEFAULT ARRAY[]::text[] NOT NULL,
    ADD COLUMN interaction_policy_generation bigint DEFAULT 1 NOT NULL,
    ADD COLUMN interaction_policy_changed_at timestamp with time zone
        DEFAULT clock_timestamp() NOT NULL,
    ADD COLUMN interaction_policy_changed_by_user_id uuid;

UPDATE public.runtime_agent_execution_profiles profile
SET interaction_policy_changed_by_user_id = agent.creator_id
FROM public.agents agent
WHERE agent.id = profile.agent_id;

ALTER TABLE public.runtime_agent_execution_profiles
    ALTER COLUMN interaction_policy_changed_by_user_id SET NOT NULL,
    ADD CONSTRAINT runtime_agent_execution_profiles_interaction_policy_valid
        CHECK (
            interaction_policy = ANY (
                ARRAY['restricted'::text, 'full'::text]
            )
        ) NOT VALID,
    ADD CONSTRAINT runtime_agent_profiles_policy_generation_positive
        CHECK (interaction_policy_generation > 0) NOT VALID,
    ADD CONSTRAINT runtime_agent_execution_profiles_mutation_origins_consistent
        CHECK (
            (
                interaction_policy = 'restricted'::text
                AND cardinality(browser_mutation_origins) = 0
            )
            OR
            (
                interaction_policy = 'full'::text
                AND cardinality(browser_mutation_origins) BETWEEN 1 AND 32
            )
        ) NOT VALID,
    ADD CONSTRAINT runtime_agent_execution_profiles_policy_changed_by_user_id_fkey
        FOREIGN KEY (interaction_policy_changed_by_user_id)
        REFERENCES public.users(id)
        ON DELETE RESTRICT;

ALTER TABLE public.runtime_agent_execution_profiles
    VALIDATE CONSTRAINT runtime_agent_execution_profiles_interaction_policy_valid,
    VALIDATE CONSTRAINT runtime_agent_profiles_policy_generation_positive,
    VALIDATE CONSTRAINT runtime_agent_execution_profiles_mutation_origins_consistent;

ALTER TABLE public.agents
    ADD CONSTRAINT agents_browser_execution_profile_runtime
        CHECK (
            NOT browser_execution_profile
            OR connection_mode = 'runtime'::text
        ) NOT VALID;

ALTER TABLE public.agents
    VALIDATE CONSTRAINT agents_browser_execution_profile_runtime;

-- Owner-staged authority closes the first-Session bootstrap gap without
-- trusting Worker metadata.  The row is consumed transactionally by the first
-- compatible Browser Runtime Session; the execution-profile row remains the
-- sole authority after classification.
CREATE TABLE public.runtime_agent_browser_policy_intents (
    agent_id uuid NOT NULL,
    interaction_policy text NOT NULL,
    browser_mutation_origins text[] NOT NULL,
    configured_at timestamp with time zone
        DEFAULT clock_timestamp() NOT NULL,
    configured_by_user_id uuid NOT NULL,
    CONSTRAINT runtime_agent_browser_policy_intents_pkey
        PRIMARY KEY (agent_id),
    CONSTRAINT runtime_agent_browser_policy_intents_agent_id_fkey
        FOREIGN KEY (agent_id)
        REFERENCES public.agents(id)
        ON DELETE CASCADE,
    CONSTRAINT runtime_agent_browser_policy_intents_configured_by_user_id_fkey
        FOREIGN KEY (configured_by_user_id)
        REFERENCES public.users(id)
        ON DELETE RESTRICT,
    CONSTRAINT runtime_agent_browser_policy_intents_policy_valid
        CHECK (
            interaction_policy = ANY (
                ARRAY['restricted'::text, 'full'::text]
            )
        ),
    CONSTRAINT runtime_agent_browser_policy_intents_origins_consistent
        CHECK (
            (
                interaction_policy = 'restricted'::text
                AND cardinality(browser_mutation_origins) = 0
            )
            OR
            (
                interaction_policy = 'full'::text
                AND cardinality(browser_mutation_origins) BETWEEN 1 AND 32
            )
        )
);

COMMIT;
