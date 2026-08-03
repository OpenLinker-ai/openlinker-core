-- name: GetRuntimeAgentExecutionProfile :one
SELECT agent_id, execution_profile, classified_at,
       classified_by_credential_id, source_runtime_session_id,
       last_confirmed_at, reset_at, reset_by_user_id,
       reset_reason, profile_purge_attested,
       interaction_policy, browser_mutation_origins,
       interaction_policy_generation, interaction_policy_changed_at,
       interaction_policy_changed_by_user_id
FROM runtime_agent_execution_profiles
WHERE agent_id = $1;

-- name: GetRuntimeAgentExecutionProfileForUpdate :one
SELECT agent_id, execution_profile, classified_at,
       classified_by_credential_id, source_runtime_session_id,
       last_confirmed_at, reset_at, reset_by_user_id,
       reset_reason, profile_purge_attested,
       interaction_policy, browser_mutation_origins,
       interaction_policy_generation, interaction_policy_changed_at,
       interaction_policy_changed_by_user_id
FROM runtime_agent_execution_profiles
WHERE agent_id = $1
FOR UPDATE;

-- name: LockRuntimeAgentBrowserDeclaration :one
-- Every Session kind shares the per-Agent profile lock with initial policy
-- staging. A standard Session must not race past an Owner's Browser
-- declaration before the first Browser execution-profile row exists.
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended(
            'runtime-agent-profile:' || (sqlc.arg(agent_id)::uuid)::text,
            0
        )
    )
)
SELECT agent.browser_execution_profile
FROM agents agent
CROSS JOIN profile_lock
WHERE agent.id = sqlc.arg(agent_id)
FOR SHARE OF agent;

-- name: GetRuntimeAgentBrowserPolicyIntent :one
SELECT agent_id, interaction_policy, browser_mutation_origins,
       configured_at, configured_by_user_id
FROM runtime_agent_browser_policy_intents
WHERE agent_id = $1;

-- name: StageRuntimeAgentBrowserPolicyIntentForOwner :one
-- This is the only pre-classification write path. It authenticates the Owner,
-- requires a private Runtime Agent, shares the profile advisory lock and
-- refuses active work. The first compatible Browser Session consumes the row.
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended(
            'runtime-agent-profile:' || (sqlc.arg(agent_id)::uuid)::text,
            0
        )
    )
),
eligible AS MATERIALIZED (
    SELECT agent.id
    FROM agents agent
    CROSS JOIN profile_lock
    WHERE agent.id = sqlc.arg(agent_id)
      AND agent.creator_id = sqlc.arg(configured_by_user_id)
      AND agent.visibility = 'private'
      AND agent.connection_mode = 'runtime'
      AND NOT EXISTS (
          SELECT 1
          FROM runtime_agent_execution_profiles profile
          WHERE profile.agent_id = agent.id
      )
      AND NOT EXISTS (
          SELECT 1
          FROM runtime_sessions session
          WHERE session.agent_id = agent.id
            AND session.status IN ('active', 'draining', 'offline')
      )
      AND NOT EXISTS (
          SELECT 1
          FROM runs run
          WHERE run.agent_id = agent.id
            AND run.status = 'running'
      )
      AND NOT EXISTS (
          SELECT 1
          FROM browser_run_controls control
          WHERE control.agent_id = agent.id
            AND control.state IN ('paused', 'human')
      )
    FOR UPDATE OF agent
),
marked_agent AS (
    UPDATE agents agent
    SET browser_execution_profile = TRUE
    FROM eligible
    WHERE agent.id = eligible.id
      AND NOT agent.browser_execution_profile
    RETURNING agent.id
)
INSERT INTO runtime_agent_browser_policy_intents (
    agent_id, interaction_policy, browser_mutation_origins,
    configured_at, configured_by_user_id
)
SELECT eligible.id, sqlc.arg(interaction_policy),
       sqlc.arg(browser_mutation_origins)::text[],
       clock_timestamp(), sqlc.arg(configured_by_user_id)
FROM eligible
ON CONFLICT (agent_id) DO UPDATE
SET interaction_policy = EXCLUDED.interaction_policy,
    browser_mutation_origins = EXCLUDED.browser_mutation_origins,
    configured_at = clock_timestamp(),
    configured_by_user_id = EXCLUDED.configured_by_user_id
RETURNING agent_id, interaction_policy, browser_mutation_origins,
          configured_at, configured_by_user_id;

-- name: LockRuntimeAgentBrowserProfileForRunCreate :one
-- Browser Run creation shares the same per-Agent advisory lock as policy
-- mutation/classification/reset. Holding it through the Run INSERT makes the
-- authority snapshot and the policy-generation transition serializable
-- without adding a per-action read.
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended(
            'runtime-agent-profile:' || (sqlc.arg(agent_id)::uuid)::text,
            0
        )
    )
)
SELECT profile.agent_id, profile.execution_profile, profile.classified_at,
       profile.classified_by_credential_id, profile.source_runtime_session_id,
       profile.last_confirmed_at, profile.reset_at, profile.reset_by_user_id,
       profile.reset_reason, profile.profile_purge_attested,
       profile.interaction_policy, profile.browser_mutation_origins,
       profile.interaction_policy_generation, profile.interaction_policy_changed_at,
       profile.interaction_policy_changed_by_user_id
FROM runtime_agent_execution_profiles profile
CROSS JOIN profile_lock
WHERE profile.agent_id = sqlc.arg(agent_id)
  AND profile.execution_profile = 'browser'
FOR SHARE OF profile;

-- name: ClassifyRuntimeAgentBrowserExecutionProfile :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended(
            'runtime-agent-profile:' || (sqlc.arg(agent_id)::uuid)::text,
            0
        )
    )
),
eligible AS MATERIALIZED (
    SELECT a.id AS agent_id, a.creator_id,
           intent.interaction_policy AS staged_interaction_policy,
           intent.browser_mutation_origins AS staged_browser_mutation_origins,
           intent.configured_at AS staged_configured_at,
           intent.configured_by_user_id AS staged_configured_by_user_id
    FROM agents a
    CROSS JOIN profile_lock
    JOIN agent_tokens t
      ON t.id = sqlc.arg(credential_id)
     AND t.agent_id = a.id
     AND t.creator_user_id = a.creator_id
     AND t.status = 'active_runtime'
     AND t.revoked_at IS NULL
     AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
    LEFT JOIN runtime_agent_execution_profiles existing
      ON existing.agent_id = a.id
    LEFT JOIN runtime_agent_browser_policy_intents intent
      ON intent.agent_id = a.id
    WHERE a.visibility = 'private'
      AND a.connection_mode = 'runtime'
      AND (
          existing.agent_id IS NOT NULL
          OR (
              intent.agent_id IS NULL
              AND NOT sqlc.arg(full_browser_interaction)::boolean
          )
          OR (
              intent.interaction_policy = CASE
                  WHEN sqlc.arg(full_browser_interaction)::boolean
                      THEN 'full'::text
                  ELSE 'restricted'::text
              END
          )
      )
    FOR UPDATE OF a, t
),
marked_agent AS (
    UPDATE agents a
    SET browser_execution_profile = TRUE
    FROM eligible
    WHERE a.id = eligible.agent_id
      AND NOT a.browser_execution_profile
    RETURNING a.id
),
classified AS (
INSERT INTO runtime_agent_execution_profiles (
    agent_id,
    execution_profile,
    classified_by_credential_id,
    source_runtime_session_id,
    interaction_policy,
    browser_mutation_origins,
    interaction_policy_changed_at,
    interaction_policy_changed_by_user_id
)
SELECT
    eligible.agent_id,
    'browser',
    sqlc.arg(credential_id),
    sqlc.arg(runtime_session_id),
    COALESCE(eligible.staged_interaction_policy, 'restricted'::text),
    COALESCE(eligible.staged_browser_mutation_origins, ARRAY[]::text[]),
    COALESCE(eligible.staged_configured_at, clock_timestamp()),
    COALESCE(eligible.staged_configured_by_user_id, eligible.creator_id)
FROM eligible
ON CONFLICT (agent_id) DO UPDATE
SET execution_profile = 'browser',
    classified_at = CASE
        WHEN runtime_agent_execution_profiles.execution_profile = 'browser'
            THEN runtime_agent_execution_profiles.classified_at
        ELSE clock_timestamp()
    END,
    classified_by_credential_id = EXCLUDED.classified_by_credential_id,
    source_runtime_session_id = EXCLUDED.source_runtime_session_id,
    last_confirmed_at = clock_timestamp(),
    reset_at = NULL,
    reset_by_user_id = NULL,
    reset_reason = NULL,
    profile_purge_attested = NULL
RETURNING agent_id, execution_profile, classified_at,
          classified_by_credential_id, source_runtime_session_id,
          last_confirmed_at, reset_at, reset_by_user_id,
          reset_reason, profile_purge_attested,
          interaction_policy, browser_mutation_origins,
          interaction_policy_generation, interaction_policy_changed_at,
          interaction_policy_changed_by_user_id
),
cleared_intent AS (
    DELETE FROM runtime_agent_browser_policy_intents intent
    USING classified
    WHERE intent.agent_id = classified.agent_id
    RETURNING intent.agent_id
)
SELECT classified.agent_id, classified.execution_profile,
       classified.classified_at, classified.classified_by_credential_id,
       classified.source_runtime_session_id, classified.last_confirmed_at,
       classified.reset_at, classified.reset_by_user_id,
       classified.reset_reason, classified.profile_purge_attested,
       classified.interaction_policy, classified.browser_mutation_origins,
       classified.interaction_policy_generation,
       classified.interaction_policy_changed_at,
       classified.interaction_policy_changed_by_user_id
FROM classified;

-- name: ResetRuntimeAgentExecutionProfile :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended(
            'runtime-agent-profile:' || (sqlc.arg(agent_id)::uuid)::text,
            0
        )
    )
),
reset_profile AS (
UPDATE runtime_agent_execution_profiles profile
SET execution_profile = 'standard',
    interaction_policy = 'restricted',
    browser_mutation_origins = ARRAY[]::text[],
    interaction_policy_generation = interaction_policy_generation + 1,
    interaction_policy_changed_at = clock_timestamp(),
    interaction_policy_changed_by_user_id = sqlc.arg(reset_by_user_id),
    reset_at = clock_timestamp(),
    reset_by_user_id = sqlc.arg(reset_by_user_id),
    reset_reason = btrim(sqlc.arg(reset_reason)),
    profile_purge_attested = TRUE,
    last_confirmed_at = clock_timestamp()
FROM profile_lock
WHERE profile.agent_id = sqlc.arg(agent_id)
  AND profile.execution_profile = 'browser'
  AND sqlc.arg(profile_purge_attested)::boolean
  AND char_length(btrim(sqlc.arg(reset_reason))) BETWEEN 1 AND 500
  AND NOT EXISTS (
      SELECT 1
      FROM runtime_sessions session
      WHERE session.agent_id = profile.agent_id
        AND session.status IN ('active', 'draining', 'offline')
  )
  AND NOT EXISTS (
      SELECT 1
      FROM runtime_resume_grants grant_row
      WHERE grant_row.agent_id = profile.agent_id
        AND grant_row.revoked_at IS NULL
        AND grant_row.expires_at > clock_timestamp()
  )
RETURNING profile.agent_id, profile.execution_profile, profile.classified_at,
          profile.classified_by_credential_id, profile.source_runtime_session_id,
          profile.last_confirmed_at, profile.reset_at, profile.reset_by_user_id,
          profile.reset_reason, profile.profile_purge_attested,
          profile.interaction_policy, profile.browser_mutation_origins,
          profile.interaction_policy_generation, profile.interaction_policy_changed_at,
          profile.interaction_policy_changed_by_user_id
),
reset_agent AS (
    UPDATE agents agent
    SET browser_execution_profile = FALSE
    FROM reset_profile
    WHERE agent.id = reset_profile.agent_id
    RETURNING agent.id
)
SELECT reset_profile.agent_id, reset_profile.execution_profile,
       reset_profile.classified_at, reset_profile.classified_by_credential_id,
       reset_profile.source_runtime_session_id, reset_profile.last_confirmed_at,
       reset_profile.reset_at, reset_profile.reset_by_user_id,
       reset_profile.reset_reason, reset_profile.profile_purge_attested,
       reset_profile.interaction_policy, reset_profile.browser_mutation_origins,
       reset_profile.interaction_policy_generation,
       reset_profile.interaction_policy_changed_at,
       reset_profile.interaction_policy_changed_by_user_id
FROM reset_profile
JOIN reset_agent ON reset_agent.id = reset_profile.agent_id;

-- name: UpdateRuntimeAgentBrowserInteractionPolicyForOwner :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended(
            'runtime-agent-profile:' || (sqlc.arg(agent_id)::uuid)::text,
            0
        )
    )
)
UPDATE runtime_agent_execution_profiles profile
SET interaction_policy = sqlc.arg(interaction_policy),
    browser_mutation_origins = sqlc.arg(browser_mutation_origins)::text[],
    interaction_policy_generation = profile.interaction_policy_generation + 1,
    interaction_policy_changed_at = clock_timestamp(),
    interaction_policy_changed_by_user_id = sqlc.arg(changed_by_user_id),
    last_confirmed_at = clock_timestamp()
FROM agents agent, profile_lock
WHERE profile.agent_id = sqlc.arg(agent_id)
  AND profile.execution_profile = 'browser'
  AND agent.id = profile.agent_id
  AND agent.creator_id = sqlc.arg(changed_by_user_id)
  AND agent.visibility = 'private'
  AND (
      profile.interaction_policy,
      profile.browser_mutation_origins
  ) IS DISTINCT FROM (
      sqlc.arg(interaction_policy),
      sqlc.arg(browser_mutation_origins)::text[]
  )
  AND NOT EXISTS (
      SELECT 1
      FROM runtime_sessions session
      WHERE session.agent_id = profile.agent_id
        AND session.status IN ('active', 'draining', 'offline')
  )
  AND NOT EXISTS (
      SELECT 1
      FROM runs run
      WHERE run.agent_id = profile.agent_id
        AND run.status = 'running'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM browser_run_controls control
      WHERE control.agent_id = profile.agent_id
        AND control.state IN ('paused', 'human')
  )
RETURNING profile.agent_id, profile.execution_profile, profile.classified_at,
          profile.classified_by_credential_id, profile.source_runtime_session_id,
          profile.last_confirmed_at, profile.reset_at, profile.reset_by_user_id,
          profile.reset_reason, profile.profile_purge_attested,
          profile.interaction_policy, profile.browser_mutation_origins,
          profile.interaction_policy_generation, profile.interaction_policy_changed_at,
          profile.interaction_policy_changed_by_user_id;
