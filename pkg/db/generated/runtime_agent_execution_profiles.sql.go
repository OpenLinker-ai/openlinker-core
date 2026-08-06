// Hand-maintained SQL query implementation.
// sqlc comparison output is isolated under .sqlc/generated; review it manually before any migration of this runtime package.

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const getRuntimeAgentExecutionProfile = `-- name: GetRuntimeAgentExecutionProfile :one
SELECT agent_id, execution_profile, classified_at,
       classified_by_credential_id, source_runtime_session_id,
       last_confirmed_at, reset_at, reset_by_user_id,
       reset_reason, profile_purge_attested,
       interaction_policy, browser_mutation_origins,
       interaction_policy_generation, interaction_policy_changed_at,
       interaction_policy_changed_by_user_id
FROM runtime_agent_execution_profiles
WHERE agent_id = $1`

func (q *Queries) GetRuntimeAgentExecutionProfile(
	ctx context.Context,
	agentID uuid.UUID,
) (RuntimeAgentExecutionProfile, error) {
	row := q.db.QueryRow(ctx, getRuntimeAgentExecutionProfile, agentID)
	return scanRuntimeAgentExecutionProfile(row)
}

const getRuntimeAgentExecutionProfileForUpdate = `-- name: GetRuntimeAgentExecutionProfileForUpdate :one
SELECT agent_id, execution_profile, classified_at,
       classified_by_credential_id, source_runtime_session_id,
       last_confirmed_at, reset_at, reset_by_user_id,
       reset_reason, profile_purge_attested,
       interaction_policy, browser_mutation_origins,
       interaction_policy_generation, interaction_policy_changed_at,
       interaction_policy_changed_by_user_id
FROM runtime_agent_execution_profiles
WHERE agent_id = $1
FOR UPDATE`

func (q *Queries) GetRuntimeAgentExecutionProfileForUpdate(
	ctx context.Context,
	agentID uuid.UUID,
) (RuntimeAgentExecutionProfile, error) {
	row := q.db.QueryRow(ctx, getRuntimeAgentExecutionProfileForUpdate, agentID)
	return scanRuntimeAgentExecutionProfile(row)
}

const lockRuntimeAgentBrowserDeclaration = `-- name: LockRuntimeAgentBrowserDeclaration :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended('runtime-agent-profile:' || ($1::uuid)::text, 0)
    )
)
SELECT agent.browser_execution_profile
FROM agents agent
CROSS JOIN profile_lock
WHERE agent.id = $1
FOR SHARE OF agent`

func (q *Queries) LockRuntimeAgentBrowserDeclaration(
	ctx context.Context,
	agentID uuid.UUID,
) (bool, error) {
	row := q.db.QueryRow(ctx, lockRuntimeAgentBrowserDeclaration, agentID)
	var browserExecutionProfile bool
	err := row.Scan(&browserExecutionProfile)
	return browserExecutionProfile, err
}

type RuntimeAgentBrowserPolicyIntent struct {
	AgentID                uuid.UUID `db:"agent_id" json:"agent_id"`
	InteractionPolicy      string    `db:"interaction_policy" json:"interaction_policy"`
	BrowserMutationOrigins []string  `db:"browser_mutation_origins" json:"browser_mutation_origins"`
	ConfiguredAt           time.Time `db:"configured_at" json:"configured_at"`
	ConfiguredByUserID     uuid.UUID `db:"configured_by_user_id" json:"configured_by_user_id"`
}

const getRuntimeAgentBrowserPolicyIntent = `-- name: GetRuntimeAgentBrowserPolicyIntent :one
SELECT agent_id, interaction_policy, browser_mutation_origins,
       configured_at, configured_by_user_id
FROM runtime_agent_browser_policy_intents
WHERE agent_id = $1`

func (q *Queries) GetRuntimeAgentBrowserPolicyIntent(
	ctx context.Context,
	agentID uuid.UUID,
) (RuntimeAgentBrowserPolicyIntent, error) {
	row := q.db.QueryRow(ctx, getRuntimeAgentBrowserPolicyIntent, agentID)
	var intent RuntimeAgentBrowserPolicyIntent
	err := row.Scan(
		&intent.AgentID,
		&intent.InteractionPolicy,
		&intent.BrowserMutationOrigins,
		&intent.ConfiguredAt,
		&intent.ConfiguredByUserID,
	)
	return intent, err
}

const stageRuntimeAgentBrowserPolicyIntentForOwner = `-- name: StageRuntimeAgentBrowserPolicyIntentForOwner :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended('runtime-agent-profile:' || ($1::uuid)::text, 0)
    )
),
eligible AS MATERIALIZED (
    SELECT agent.id
    FROM agents agent
    CROSS JOIN profile_lock
    WHERE agent.id = $1
      AND agent.creator_id = $2
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
SELECT eligible.id, $3, $4::text[], clock_timestamp(), $2
FROM eligible
ON CONFLICT (agent_id) DO UPDATE
SET interaction_policy = EXCLUDED.interaction_policy,
    browser_mutation_origins = EXCLUDED.browser_mutation_origins,
    configured_at = clock_timestamp(),
    configured_by_user_id = EXCLUDED.configured_by_user_id
RETURNING agent_id, interaction_policy, browser_mutation_origins,
          configured_at, configured_by_user_id`

type StageRuntimeAgentBrowserPolicyIntentForOwnerParams struct {
	AgentID                uuid.UUID `db:"agent_id" json:"agent_id"`
	ConfiguredByUserID     uuid.UUID `db:"configured_by_user_id" json:"configured_by_user_id"`
	InteractionPolicy      string    `db:"interaction_policy" json:"interaction_policy"`
	BrowserMutationOrigins []string  `db:"browser_mutation_origins" json:"browser_mutation_origins"`
}

func (q *Queries) StageRuntimeAgentBrowserPolicyIntentForOwner(
	ctx context.Context,
	arg StageRuntimeAgentBrowserPolicyIntentForOwnerParams,
) (RuntimeAgentBrowserPolicyIntent, error) {
	row := q.db.QueryRow(
		ctx,
		stageRuntimeAgentBrowserPolicyIntentForOwner,
		arg.AgentID,
		arg.ConfiguredByUserID,
		arg.InteractionPolicy,
		arg.BrowserMutationOrigins,
	)
	var intent RuntimeAgentBrowserPolicyIntent
	err := row.Scan(
		&intent.AgentID,
		&intent.InteractionPolicy,
		&intent.BrowserMutationOrigins,
		&intent.ConfiguredAt,
		&intent.ConfiguredByUserID,
	)
	return intent, err
}

const lockRuntimeAgentBrowserProfileForRunCreate = `-- name: LockRuntimeAgentBrowserProfileForRunCreate :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended('runtime-agent-profile:' || ($1::uuid)::text, 0)
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
WHERE profile.agent_id = $1
  AND profile.execution_profile = 'browser'
FOR SHARE OF profile`

func (q *Queries) LockRuntimeAgentBrowserProfileForRunCreate(
	ctx context.Context,
	agentID uuid.UUID,
) (RuntimeAgentExecutionProfile, error) {
	row := q.db.QueryRow(ctx, lockRuntimeAgentBrowserProfileForRunCreate, agentID)
	return scanRuntimeAgentExecutionProfile(row)
}

const classifyRuntimeAgentBrowserExecutionProfile = `-- name: ClassifyRuntimeAgentBrowserExecutionProfile :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended('runtime-agent-profile:' || ($1::uuid)::text, 0)
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
      ON t.id = $2
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
          OR (intent.agent_id IS NULL AND NOT $3::boolean)
          OR (
              intent.interaction_policy = CASE
                  WHEN $3::boolean THEN 'full'::text
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
SELECT eligible.agent_id, 'browser', $2, $4,
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
FROM classified`

type ClassifyRuntimeAgentBrowserExecutionProfileParams struct {
	AgentID                uuid.UUID `db:"agent_id" json:"agent_id"`
	CredentialID           uuid.UUID `db:"credential_id" json:"credential_id"`
	FullBrowserInteraction bool      `db:"full_browser_interaction" json:"full_browser_interaction"`
	RuntimeSessionID       uuid.UUID `db:"runtime_session_id" json:"runtime_session_id"`
}

func (q *Queries) ClassifyRuntimeAgentBrowserExecutionProfile(
	ctx context.Context,
	arg ClassifyRuntimeAgentBrowserExecutionProfileParams,
) (RuntimeAgentExecutionProfile, error) {
	row := q.db.QueryRow(
		ctx,
		classifyRuntimeAgentBrowserExecutionProfile,
		arg.AgentID,
		arg.CredentialID,
		arg.FullBrowserInteraction,
		arg.RuntimeSessionID,
	)
	return scanRuntimeAgentExecutionProfile(row)
}

const resetRuntimeAgentExecutionProfile = `-- name: ResetRuntimeAgentExecutionProfile :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended('runtime-agent-profile:' || ($1::uuid)::text, 0)
    )
),
reset_profile AS (
UPDATE runtime_agent_execution_profiles profile
SET execution_profile = 'standard',
    interaction_policy = 'restricted',
    browser_mutation_origins = ARRAY[]::text[],
    interaction_policy_generation = interaction_policy_generation + 1,
    interaction_policy_changed_at = clock_timestamp(),
    interaction_policy_changed_by_user_id = $2,
    reset_at = clock_timestamp(),
    reset_by_user_id = $2,
    reset_reason = btrim($3),
    profile_purge_attested = TRUE,
    last_confirmed_at = clock_timestamp()
FROM profile_lock
WHERE profile.agent_id = $1
  AND profile.execution_profile = 'browser'
  AND $4::boolean
  AND char_length(btrim($3)) BETWEEN 1 AND 500
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
JOIN reset_agent ON reset_agent.id = reset_profile.agent_id`

type ResetRuntimeAgentExecutionProfileParams struct {
	AgentID              uuid.UUID `db:"agent_id" json:"agent_id"`
	ResetByUserID        uuid.UUID `db:"reset_by_user_id" json:"reset_by_user_id"`
	ResetReason          string    `db:"reset_reason" json:"reset_reason"`
	ProfilePurgeAttested bool      `db:"profile_purge_attested" json:"profile_purge_attested"`
}

func (q *Queries) ResetRuntimeAgentExecutionProfile(
	ctx context.Context,
	arg ResetRuntimeAgentExecutionProfileParams,
) (RuntimeAgentExecutionProfile, error) {
	row := q.db.QueryRow(
		ctx,
		resetRuntimeAgentExecutionProfile,
		arg.AgentID,
		arg.ResetByUserID,
		arg.ResetReason,
		arg.ProfilePurgeAttested,
	)
	return scanRuntimeAgentExecutionProfile(row)
}

func scanRuntimeAgentExecutionProfile(row interface {
	Scan(...any) error
}) (RuntimeAgentExecutionProfile, error) {
	var profile RuntimeAgentExecutionProfile
	err := row.Scan(
		&profile.AgentID,
		&profile.ExecutionProfile,
		&profile.ClassifiedAt,
		&profile.ClassifiedByCredentialID,
		&profile.SourceRuntimeSessionID,
		&profile.LastConfirmedAt,
		&profile.ResetAt,
		&profile.ResetByUserID,
		&profile.ResetReason,
		&profile.ProfilePurgeAttested,
		&profile.InteractionPolicy,
		&profile.BrowserMutationOrigins,
		&profile.InteractionPolicyGeneration,
		&profile.InteractionPolicyChangedAt,
		&profile.InteractionPolicyChangedByUserID,
	)
	return profile, err
}

const updateRuntimeAgentBrowserInteractionPolicyForOwner = `-- name: UpdateRuntimeAgentBrowserInteractionPolicyForOwner :one
WITH profile_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended('runtime-agent-profile:' || ($1::uuid)::text, 0)
    )
)
UPDATE runtime_agent_execution_profiles profile
SET interaction_policy = $2,
    browser_mutation_origins = $3::text[],
    interaction_policy_generation = profile.interaction_policy_generation + 1,
    interaction_policy_changed_at = clock_timestamp(),
    interaction_policy_changed_by_user_id = $4,
    last_confirmed_at = clock_timestamp()
FROM agents agent, profile_lock
WHERE profile.agent_id = $1
  AND profile.execution_profile = 'browser'
  AND agent.id = profile.agent_id
  AND agent.creator_id = $4
  AND agent.visibility = 'private'
  AND (profile.interaction_policy, profile.browser_mutation_origins)
      IS DISTINCT FROM ($2, $3::text[])
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
          profile.interaction_policy_changed_by_user_id`

type UpdateRuntimeAgentBrowserInteractionPolicyForOwnerParams struct {
	AgentID                uuid.UUID `db:"agent_id" json:"agent_id"`
	InteractionPolicy      string    `db:"interaction_policy" json:"interaction_policy"`
	BrowserMutationOrigins []string  `db:"browser_mutation_origins" json:"browser_mutation_origins"`
	ChangedByUserID        uuid.UUID `db:"changed_by_user_id" json:"changed_by_user_id"`
}

func (q *Queries) UpdateRuntimeAgentBrowserInteractionPolicyForOwner(
	ctx context.Context,
	arg UpdateRuntimeAgentBrowserInteractionPolicyForOwnerParams,
) (RuntimeAgentExecutionProfile, error) {
	row := q.db.QueryRow(
		ctx,
		updateRuntimeAgentBrowserInteractionPolicyForOwner,
		arg.AgentID,
		arg.InteractionPolicy,
		arg.BrowserMutationOrigins,
		arg.ChangedByUserID,
	)
	return scanRuntimeAgentExecutionProfile(row)
}
