// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/agent_availability.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanAgentAvailabilitySnapshot(row interface {
	Scan(dest ...any) error
}, s *AgentAvailabilitySnapshot) error {
	return row.Scan(
		&s.AgentID,
		&s.AvailabilityStatus,
		&s.LastSuccessfulRunAt,
		&s.LastFailedRunAt,
		&s.LastCheckedAt,
		&s.ConsecutiveFailures,
		&s.UpdatedAt,
	)
}

const getAgentAvailabilitySnapshot = `-- name: GetAgentAvailabilitySnapshot :one
SELECT agent_id, availability_status, last_successful_run_at, last_failed_run_at,
       last_checked_at, consecutive_failures, updated_at
FROM agent_availability_snapshots
WHERE agent_id = $1`

func (q *Queries) GetAgentAvailabilitySnapshot(ctx context.Context, agentID uuid.UUID) (AgentAvailabilitySnapshot, error) {
	row := q.db.QueryRow(ctx, getAgentAvailabilitySnapshot, agentID)
	var s AgentAvailabilitySnapshot
	err := scanAgentAvailabilitySnapshot(row, &s)
	return s, err
}

const markAgentAvailabilitySuccess = `-- name: MarkAgentAvailabilitySuccess :one
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
          last_checked_at, consecutive_failures, updated_at`

func (q *Queries) MarkAgentAvailabilitySuccess(ctx context.Context, agentID uuid.UUID) (AgentAvailabilitySnapshot, error) {
	row := q.db.QueryRow(ctx, markAgentAvailabilitySuccess, agentID)
	var s AgentAvailabilitySnapshot
	err := scanAgentAvailabilitySnapshot(row, &s)
	return s, err
}

const markAgentAvailabilityHeartbeat = `-- name: MarkAgentAvailabilityHeartbeat :one
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
          last_checked_at, consecutive_failures, updated_at`

func (q *Queries) MarkAgentAvailabilityHeartbeat(ctx context.Context, agentID uuid.UUID) (AgentAvailabilitySnapshot, error) {
	row := q.db.QueryRow(ctx, markAgentAvailabilityHeartbeat, agentID)
	var s AgentAvailabilitySnapshot
	err := scanAgentAvailabilitySnapshot(row, &s)
	return s, err
}

const markAgentAvailabilityFailure = `-- name: MarkAgentAvailabilityFailure :one
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
          last_checked_at, consecutive_failures, updated_at`

func (q *Queries) MarkAgentAvailabilityFailure(ctx context.Context, agentID uuid.UUID) (AgentAvailabilitySnapshot, error) {
	row := q.db.QueryRow(ctx, markAgentAvailabilityFailure, agentID)
	var s AgentAvailabilitySnapshot
	err := scanAgentAvailabilitySnapshot(row, &s)
	return s, err
}

const listAgentsDueAvailabilityCheck = `-- name: ListAgentsDueAvailabilityCheck :many
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
LIMIT $2`

type ListAgentsDueAvailabilityCheckParams struct {
	StaleSeconds int32 `db:"stale_seconds" json:"stale_seconds"`
	Limit        int32 `db:"limit" json:"limit"`
}

func (q *Queries) ListAgentsDueAvailabilityCheck(ctx context.Context, arg ListAgentsDueAvailabilityCheckParams) ([]Agent, error) {
	rows, err := q.db.Query(ctx, listAgentsDueAvailabilityCheck, arg.StaleSeconds, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Agent
	for rows.Next() {
		var a Agent
		if err := scanAgent(rows, &a); err != nil {
			return nil, err
		}
		items = append(items, a)
	}
	return items, rows.Err()
}

func scanAgentAvailabilityAlert(row interface {
	Scan(dest ...any) error
}, a *AgentAvailabilityAlert) error {
	return row.Scan(
		&a.ID,
		&a.AgentID,
		&a.CreatorID,
		&a.AlertType,
		&a.Severity,
		&a.AvailabilityStatus,
		&a.ConsecutiveFailures,
		&a.Title,
		&a.Message,
		&a.LastError,
		&a.RepairHints,
		&a.ReadAt,
		&a.CreatedAt,
		&a.UpdatedAt,
	)
}

const upsertAgentAvailabilityAlert = `-- name: UpsertAgentAvailabilityAlert :one
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
          read_at, created_at, updated_at`

type UpsertAgentAvailabilityAlertParams struct {
	AgentID             uuid.UUID `db:"agent_id" json:"agent_id"`
	CreatorID           uuid.UUID `db:"creator_id" json:"creator_id"`
	AlertType           string    `db:"alert_type" json:"alert_type"`
	Severity            string    `db:"severity" json:"severity"`
	AvailabilityStatus  string    `db:"availability_status" json:"availability_status"`
	ConsecutiveFailures int32     `db:"consecutive_failures" json:"consecutive_failures"`
	Title               string    `db:"title" json:"title"`
	Message             string    `db:"message" json:"message"`
	LastError           *string   `db:"last_error" json:"last_error"`
	RepairHints         []string  `db:"repair_hints" json:"repair_hints"`
}

func (q *Queries) UpsertAgentAvailabilityAlert(ctx context.Context, arg UpsertAgentAvailabilityAlertParams) (AgentAvailabilityAlert, error) {
	row := q.db.QueryRow(ctx, upsertAgentAvailabilityAlert,
		arg.AgentID,
		arg.CreatorID,
		arg.AlertType,
		arg.Severity,
		arg.AvailabilityStatus,
		arg.ConsecutiveFailures,
		arg.Title,
		arg.Message,
		arg.LastError,
		arg.RepairHints,
	)
	var a AgentAvailabilityAlert
	err := scanAgentAvailabilityAlert(row, &a)
	return a, err
}

const listAgentAvailabilityAlertsByCreator = `-- name: ListAgentAvailabilityAlertsByCreator :many
SELECT aa.id, aa.agent_id, a.slug AS agent_slug, a.name AS agent_name,
       aa.creator_id, aa.alert_type, aa.severity, aa.availability_status,
       aa.consecutive_failures, aa.title, aa.message, aa.last_error,
       aa.repair_hints, aa.read_at, aa.created_at, aa.updated_at
FROM agent_availability_alerts aa
JOIN agents a ON a.id = aa.agent_id
WHERE aa.creator_id = $1
ORDER BY (aa.read_at IS NULL) DESC, aa.created_at DESC
LIMIT $2`

type ListAgentAvailabilityAlertsByCreatorParams struct {
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
	Limit     int32     `db:"limit" json:"limit"`
}

type ListAgentAvailabilityAlertsByCreatorRow struct {
	AgentAvailabilityAlert
	AgentSlug string `db:"agent_slug" json:"agent_slug"`
	AgentName string `db:"agent_name" json:"agent_name"`
}

func (q *Queries) ListAgentAvailabilityAlertsByCreator(ctx context.Context, arg ListAgentAvailabilityAlertsByCreatorParams) ([]ListAgentAvailabilityAlertsByCreatorRow, error) {
	rows, err := q.db.Query(ctx, listAgentAvailabilityAlertsByCreator, arg.CreatorID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListAgentAvailabilityAlertsByCreatorRow
	for rows.Next() {
		var item ListAgentAvailabilityAlertsByCreatorRow
		if err := rows.Scan(
			&item.ID,
			&item.AgentID,
			&item.AgentSlug,
			&item.AgentName,
			&item.CreatorID,
			&item.AlertType,
			&item.Severity,
			&item.AvailabilityStatus,
			&item.ConsecutiveFailures,
			&item.Title,
			&item.Message,
			&item.LastError,
			&item.RepairHints,
			&item.ReadAt,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const countAgentAvailabilityAlertsByCreator = `-- name: CountAgentAvailabilityAlertsByCreator :one
SELECT COUNT(*)::int
FROM agent_availability_alerts
WHERE creator_id = $1`

func (q *Queries) CountAgentAvailabilityAlertsByCreator(ctx context.Context, creatorID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countAgentAvailabilityAlertsByCreator, creatorID)
	var count int32
	err := row.Scan(&count)
	return count, err
}

const countUnreadAgentAvailabilityAlertsByCreator = `-- name: CountUnreadAgentAvailabilityAlertsByCreator :one
SELECT COUNT(*)::int
FROM agent_availability_alerts
WHERE creator_id = $1 AND read_at IS NULL`

func (q *Queries) CountUnreadAgentAvailabilityAlertsByCreator(ctx context.Context, creatorID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countUnreadAgentAvailabilityAlertsByCreator, creatorID)
	var count int32
	err := row.Scan(&count)
	return count, err
}

const markAgentAvailabilityAlertRead = `-- name: MarkAgentAvailabilityAlertRead :one
UPDATE agent_availability_alerts
SET read_at = COALESCE(read_at, NOW()),
    updated_at = NOW()
WHERE id = $1 AND creator_id = $2
RETURNING id, agent_id, creator_id, alert_type, severity, availability_status,
          consecutive_failures, title, message, last_error, repair_hints,
          read_at, created_at, updated_at`

type MarkAgentAvailabilityAlertReadParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

func (q *Queries) MarkAgentAvailabilityAlertRead(ctx context.Context, arg MarkAgentAvailabilityAlertReadParams) (AgentAvailabilityAlert, error) {
	row := q.db.QueryRow(ctx, markAgentAvailabilityAlertRead, arg.ID, arg.CreatorID)
	var a AgentAvailabilityAlert
	err := scanAgentAvailabilityAlert(row, &a)
	return a, err
}
