-- agent_availability.sql
-- docs/27：注册不代表可用。真实运行终态更新 Agent availability。

-- name: GetAgentAvailabilitySnapshot :one
SELECT agent_id, availability_status, last_successful_run_at, last_failed_run_at,
       last_checked_at, consecutive_failures, updated_at
FROM agent_availability_snapshots
WHERE agent_id = $1;

-- name: MarkAgentAvailabilitySuccess :one
INSERT INTO agent_availability_snapshots (
    agent_id, availability_status, last_successful_run_at, last_checked_at,
    consecutive_failures, updated_at
) VALUES (
    $1, 'healthy', NOW(), NOW(), 0, NOW()
)
ON CONFLICT (agent_id) DO UPDATE
SET availability_status = 'healthy',
    last_successful_run_at = NOW(),
    last_checked_at = NOW(),
    consecutive_failures = 0,
    updated_at = NOW()
RETURNING agent_id, availability_status, last_successful_run_at, last_failed_run_at,
          last_checked_at, consecutive_failures, updated_at;

-- name: MarkAgentAvailabilityHeartbeat :one
INSERT INTO agent_availability_snapshots (
    agent_id, availability_status, last_checked_at,
    consecutive_failures, updated_at
) VALUES (
    $1, 'healthy', NOW(), 0, NOW()
)
ON CONFLICT (agent_id) DO UPDATE
SET availability_status = 'healthy',
    last_checked_at = NOW(),
    consecutive_failures = 0,
    updated_at = NOW()
RETURNING agent_id, availability_status, last_successful_run_at, last_failed_run_at,
          last_checked_at, consecutive_failures, updated_at;

-- name: MarkAgentAvailabilityFailure :one
INSERT INTO agent_availability_snapshots (
    agent_id, availability_status, last_failed_run_at, last_checked_at,
    consecutive_failures, updated_at
) VALUES (
    $1, 'degraded', NOW(), NOW(), 1, NOW()
)
ON CONFLICT (agent_id) DO UPDATE
SET consecutive_failures = agent_availability_snapshots.consecutive_failures + 1,
    availability_status = CASE
        WHEN agent_availability_snapshots.consecutive_failures + 1 >= 3 THEN 'unreachable'
        ELSE 'degraded'
    END,
    last_failed_run_at = NOW(),
    last_checked_at = NOW(),
    updated_at = NOW()
RETURNING agent_id, availability_status, last_successful_run_at, last_failed_run_at,
          last_checked_at, consecutive_failures, updated_at;

-- name: ListAgentsDueAvailabilityCheck :many
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at,
       a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.connection_mode, a.mcp_tool_name, a.created_at, a.updated_at
FROM agents a
LEFT JOIN agent_availability_snapshots s ON s.agent_id = a.id
WHERE a.lifecycle_status = 'active'
  AND a.connection_mode IN ('direct_http', 'mcp_server')
  AND EXISTS (SELECT 1 FROM agent_capabilities c WHERE c.agent_id = a.id)
  AND EXISTS (SELECT 1 FROM agent_examples e WHERE e.agent_id = a.id)
  AND (
    s.last_checked_at IS NULL
    OR s.last_checked_at < NOW() - ($1::int * INTERVAL '1 second')
  )
ORDER BY COALESCE(s.last_checked_at, TIMESTAMPTZ 'epoch') ASC, a.created_at ASC
LIMIT $2;

-- name: UpsertAgentAvailabilityAlert :one
INSERT INTO agent_availability_alerts (
    agent_id, creator_id, alert_type, severity, availability_status,
    consecutive_failures, title, message, last_error, repair_hints
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
ON CONFLICT (agent_id, alert_type) WHERE read_at IS NULL DO UPDATE
SET severity = EXCLUDED.severity,
    availability_status = EXCLUDED.availability_status,
    consecutive_failures = EXCLUDED.consecutive_failures,
    title = EXCLUDED.title,
    message = EXCLUDED.message,
    last_error = EXCLUDED.last_error,
    repair_hints = EXCLUDED.repair_hints,
    updated_at = NOW()
RETURNING id, agent_id, creator_id, alert_type, severity, availability_status,
          consecutive_failures, title, message, last_error, repair_hints,
          read_at, created_at, updated_at;

-- name: ListAgentAvailabilityAlertsByCreator :many
SELECT aa.id, aa.agent_id, a.slug AS agent_slug, a.name AS agent_name,
       aa.creator_id, aa.alert_type, aa.severity, aa.availability_status,
       aa.consecutive_failures, aa.title, aa.message, aa.last_error,
       aa.repair_hints, aa.read_at, aa.created_at, aa.updated_at
FROM agent_availability_alerts aa
JOIN agents a ON a.id = aa.agent_id
WHERE aa.creator_id = $1
ORDER BY (aa.read_at IS NULL) DESC, aa.created_at DESC
LIMIT $2;

-- name: CountAgentAvailabilityAlertsByCreator :one
SELECT COUNT(*)::int
FROM agent_availability_alerts
WHERE creator_id = $1;

-- name: CountUnreadAgentAvailabilityAlertsByCreator :one
SELECT COUNT(*)::int
FROM agent_availability_alerts
WHERE creator_id = $1 AND read_at IS NULL;

-- name: MarkAgentAvailabilityAlertRead :one
UPDATE agent_availability_alerts
SET read_at = COALESCE(read_at, NOW()),
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2
RETURNING id, agent_id, creator_id, alert_type, severity, availability_status,
          consecutive_failures, title, message, last_error, repair_hints,
          read_at, created_at, updated_at;
