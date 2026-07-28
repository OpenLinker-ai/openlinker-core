-- name: GetRuntimeAgentExecutionProfile :one
SELECT agent_id, execution_profile, classified_at,
       classified_by_credential_id, source_runtime_session_id,
       last_confirmed_at, reset_at, reset_by_user_id,
       reset_reason, profile_purge_attested
FROM runtime_agent_execution_profiles
WHERE agent_id = $1;

-- name: GetRuntimeAgentExecutionProfileForUpdate :one
SELECT agent_id, execution_profile, classified_at,
       classified_by_credential_id, source_runtime_session_id,
       last_confirmed_at, reset_at, reset_by_user_id,
       reset_reason, profile_purge_attested
FROM runtime_agent_execution_profiles
WHERE agent_id = $1
FOR UPDATE;

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
    SELECT a.id AS agent_id
    FROM agents a
    CROSS JOIN profile_lock
    JOIN agent_tokens t
      ON t.id = sqlc.arg(credential_id)
     AND t.agent_id = a.id
     AND t.creator_user_id = a.creator_id
     AND t.status = 'active_runtime'
     AND t.revoked_at IS NULL
     AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
    WHERE a.visibility = 'private'
    FOR UPDATE OF a, t
),
marked_agent AS (
    UPDATE agents a
    SET browser_execution_profile = TRUE
    FROM eligible
    WHERE a.id = eligible.agent_id
      AND NOT a.browser_execution_profile
    RETURNING a.id
)
INSERT INTO runtime_agent_execution_profiles (
    agent_id,
    execution_profile,
    classified_by_credential_id,
    source_runtime_session_id
)
SELECT
    eligible.agent_id,
    'browser',
    sqlc.arg(credential_id),
    sqlc.arg(runtime_session_id)
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
          reset_reason, profile_purge_attested;

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
          profile.reset_reason, profile.profile_purge_attested
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
       reset_profile.reset_reason, reset_profile.profile_purge_attested
FROM reset_profile
JOIN reset_agent ON reset_agent.id = reset_profile.agent_id;
