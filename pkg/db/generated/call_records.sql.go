package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const listCallRecordsForUser = `-- name: ListCallRecordsForUser :many
SELECT r.id,
       r.user_id,
       r.agent_id,
       r.status,
       CASE WHEN r.user_id = $1 THEN r.cost_cents ELSE 0 END::int AS cost_cents,
       CASE WHEN a.creator_id = $1 THEN r.creator_revenue_cents ELSE 0 END::int AS creator_revenue_cents,
       r.duration_ms,
       r.started_at,
       r.finished_at,
       r.source,
       r.runtime_contract_id,
       COALESCE(r.connection_mode_snapshot, '')::text AS agent_connection_mode,
       COALESCE(runtime_evidence.transport, '')::text AS runtime_transport,
       COALESCE(runtime_evidence.transport_reason, '')::text AS runtime_transport_reason,
       runtime_evidence.transport_changed_at AS runtime_transport_changed_at,
       r.dispatch_state,
       r.attempt_count,
       r.max_attempts,
       r.next_attempt_at,
       r.latest_attempt_id,
       r.active_attempt_id,
       r.cancel_state,
       r.cancel_requested_at,
       r.cancel_acknowledged_at,
       r.cancel_reason,
       r.dead_lettered_at,
       r.replay_of_run_id,
       a.slug AS agent_slug,
       a.name AS agent_name,
       CASE
           WHEN r.user_id = $1 AND a.creator_id = $1 THEN 'both'
           WHEN r.user_id = $1 THEN 'made'
           ELSE 'received'
       END::text AS direction,
       COALESCE(d.parent_run_id::text, '')::text AS parent_run_id,
       COALESCE(d.caller_agent_id::text, '')::text AS caller_agent_id,
       COALESCE(caller.slug, '')::text AS caller_agent_slug,
       COALESCE(caller.name, '')::text AS caller_agent_name,
       COALESCE(ctx.protocol_context_id, '')::text AS protocol_context_id,
       COALESCE(ctx.protocol_task_id, '')::text AS protocol_task_id,
       COALESCE(ctx.root_context_id, '')::text AS root_context_id,
       COALESCE(ctx.parent_context_id, '')::text AS parent_context_id,
       COALESCE(ctx.parent_task_id, '')::text AS parent_task_id,
       COALESCE(ctx.trace_id, '')::text AS trace_id,
       COALESCE(ctx.reference_task_ids, ARRAY[]::text[]) AS reference_task_ids,
       COALESCE(ctx.source, '')::text AS context_source,
       COALESCE(NULLIF(ctx.protocol_task_id, ''), r.id::text)::text AS call_id,
       COALESCE(children.child_count, 0)::int AS child_count
FROM runs r
JOIN agents a ON a.id = r.agent_id
LEFT JOIN run_delegations d ON d.child_run_id = r.id
LEFT JOIN agents caller ON caller.id = d.caller_agent_id
LEFT JOIN a2a_context_mappings ctx ON ctx.run_id = r.id
LEFT JOIN LATERAL (
    SELECT attachment.transport,
           attachment.transport_reason,
           attachment.transport_changed_at
    FROM run_attempts attempt
    JOIN runtime_session_attachments attachment
      ON attachment.id = attempt.runtime_attachment_id
     AND attachment.runtime_session_id = attempt.runtime_session_id
    WHERE attempt.run_id = r.id
      AND attempt.id = COALESCE(r.active_attempt_id, r.latest_attempt_id)
      AND attempt.executor_type = 'runtime'
      AND attempt.accepted_at IS NOT NULL
      AND attachment.transport IN ('websocket', 'long_poll')
) runtime_evidence ON TRUE
LEFT JOIN LATERAL (
    SELECT COUNT(*)::int AS child_count
    FROM run_delegations cd
    WHERE cd.parent_run_id = r.id
) children ON TRUE
WHERE (
    ($2 = 'made' AND r.user_id = $1)
    OR ($2 = 'received' AND a.creator_id = $1)
    OR ($2 = 'all' AND (r.user_id = $1 OR a.creator_id = $1))
)
AND (
    $3 = ''
    OR r.id::text ILIKE '%' || $3 || '%'
    OR r.agent_id::text ILIKE '%' || $3 || '%'
    OR r.status ILIKE '%' || $3 || '%'
    OR r.source ILIKE '%' || $3 || '%'
    OR a.slug ILIKE '%' || $3 || '%'
    OR a.name ILIKE '%' || $3 || '%'
    OR COALESCE(d.parent_run_id::text, '') ILIKE '%' || $3 || '%'
    OR COALESCE(d.caller_agent_id::text, '') ILIKE '%' || $3 || '%'
    OR COALESCE(caller.slug, '') ILIKE '%' || $3 || '%'
    OR COALESCE(caller.name, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.protocol_context_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.protocol_task_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.root_context_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.parent_context_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.parent_task_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.trace_id, '') ILIKE '%' || $3 || '%'
    OR COALESCE(ctx.source, '') ILIKE '%' || $3 || '%'
    OR COALESCE(NULLIF(ctx.protocol_task_id, ''), r.id::text) ILIKE '%' || $3 || '%'
    OR array_to_string(COALESCE(ctx.reference_task_ids, ARRAY[]::text[]), ' ') ILIKE '%' || $3 || '%'
)
AND ($4 = '' OR r.status = $4)
AND ($5 = '' OR r.source = $5)
AND (
    $6 = ''
    OR ($6 = 'direct' AND d.parent_run_id IS NULL AND COALESCE(children.child_count, 0) = 0)
    OR ($6 = 'a2a_child' AND d.parent_run_id IS NOT NULL)
    OR ($6 = 'a2a_parent' AND d.parent_run_id IS NULL AND COALESCE(children.child_count, 0) > 0)
)
ORDER BY
    CASE WHEN $7 = 'started_asc' THEN r.started_at END ASC,
    CASE WHEN $7 = 'started_desc' THEN r.started_at END DESC,
    CASE WHEN $7 = 'amount_asc' THEN
        CASE WHEN a.creator_id = $1 AND r.user_id <> $1 THEN r.creator_revenue_cents ELSE r.cost_cents END
    END ASC,
    CASE WHEN $7 = 'amount_desc' THEN
        CASE WHEN a.creator_id = $1 AND r.user_id <> $1 THEN r.creator_revenue_cents ELSE r.cost_cents END
    END DESC,
    CASE WHEN $7 = 'duration_asc' THEN COALESCE(r.duration_ms, 2147483647) END ASC,
    CASE WHEN $7 = 'duration_desc' THEN COALESCE(r.duration_ms, -1) END DESC,
    r.started_at DESC,
    r.id DESC
LIMIT $8 OFFSET $9`

type ListCallRecordsForUserParams struct {
	UserID   uuid.UUID `db:"user_id" json:"user_id"`
	View     string    `db:"view" json:"view"`
	Query    string    `db:"query" json:"query"`
	Status   string    `db:"status" json:"status"`
	Source   string    `db:"source" json:"source"`
	Relation string    `db:"relation" json:"relation"`
	Sort     string    `db:"sort" json:"sort"`
	Limit    int32     `db:"limit" json:"limit"`
	Offset   int32     `db:"offset" json:"offset"`
}

type ListCallRecordsForUserRow struct {
	ID                        uuid.UUID  `db:"id" json:"id"`
	UserID                    uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID                   uuid.UUID  `db:"agent_id" json:"agent_id"`
	Status                    string     `db:"status" json:"status"`
	CostCents                 int32      `db:"cost_cents" json:"cost_cents"`
	CreatorRevenueCents       int32      `db:"creator_revenue_cents" json:"creator_revenue_cents"`
	DurationMs                *int32     `db:"duration_ms" json:"duration_ms"`
	StartedAt                 time.Time  `db:"started_at" json:"started_at"`
	FinishedAt                *time.Time `db:"finished_at" json:"finished_at"`
	Source                    string     `db:"source" json:"source"`
	RuntimeContractID         string     `db:"runtime_contract_id" json:"runtime_contract_id"`
	AgentConnectionMode       string     `db:"agent_connection_mode" json:"agent_connection_mode"`
	RuntimeTransport          string     `db:"runtime_transport" json:"runtime_transport"`
	RuntimeTransportReason    string     `db:"runtime_transport_reason" json:"runtime_transport_reason"`
	RuntimeTransportChangedAt *time.Time `db:"runtime_transport_changed_at" json:"runtime_transport_changed_at"`
	DispatchState             string     `db:"dispatch_state" json:"dispatch_state"`
	AttemptCount              int32      `db:"attempt_count" json:"attempt_count"`
	MaxAttempts               int32      `db:"max_attempts" json:"max_attempts"`
	NextAttemptAt             *time.Time `db:"next_attempt_at" json:"next_attempt_at"`
	LatestAttemptID           *uuid.UUID `db:"latest_attempt_id" json:"latest_attempt_id"`
	ActiveAttemptID           *uuid.UUID `db:"active_attempt_id" json:"active_attempt_id"`
	CancelState               *string    `db:"cancel_state" json:"cancel_state"`
	CancelRequestedAt         *time.Time `db:"cancel_requested_at" json:"cancel_requested_at"`
	CancelAcknowledgedAt      *time.Time `db:"cancel_acknowledged_at" json:"cancel_acknowledged_at"`
	CancelReason              *string    `db:"cancel_reason" json:"cancel_reason"`
	DeadLetteredAt            *time.Time `db:"dead_lettered_at" json:"dead_lettered_at"`
	ReplayOfRunID             *uuid.UUID `db:"replay_of_run_id" json:"replay_of_run_id"`
	AgentSlug                 string     `db:"agent_slug" json:"agent_slug"`
	AgentName                 string     `db:"agent_name" json:"agent_name"`
	Direction                 string     `db:"direction" json:"direction"`
	ParentRunID               string     `db:"parent_run_id" json:"parent_run_id"`
	CallerAgentID             string     `db:"caller_agent_id" json:"caller_agent_id"`
	CallerAgentSlug           string     `db:"caller_agent_slug" json:"caller_agent_slug"`
	CallerAgentName           string     `db:"caller_agent_name" json:"caller_agent_name"`
	ProtocolContextID         string     `db:"protocol_context_id" json:"protocol_context_id"`
	ProtocolTaskID            string     `db:"protocol_task_id" json:"protocol_task_id"`
	RootContextID             string     `db:"root_context_id" json:"root_context_id"`
	ParentContextID           string     `db:"parent_context_id" json:"parent_context_id"`
	ParentTaskID              string     `db:"parent_task_id" json:"parent_task_id"`
	TraceID                   string     `db:"trace_id" json:"trace_id"`
	ReferenceTaskIDs          []string   `db:"reference_task_ids" json:"reference_task_ids"`
	ContextSource             string     `db:"context_source" json:"context_source"`
	CallID                    string     `db:"call_id" json:"call_id"`
	ChildCount                int32      `db:"child_count" json:"child_count"`
}

func (q *Queries) ListCallRecordsForUser(ctx context.Context, arg ListCallRecordsForUserParams) ([]ListCallRecordsForUserRow, error) {
	rows, err := q.db.Query(ctx, listCallRecordsForUser, arg.UserID, arg.View, arg.Query, arg.Status, arg.Source, arg.Relation, arg.Sort, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ListCallRecordsForUserRow{}
	for rows.Next() {
		var item ListCallRecordsForUserRow
		if err := rows.Scan(
			&item.ID,
			&item.UserID,
			&item.AgentID,
			&item.Status,
			&item.CostCents,
			&item.CreatorRevenueCents,
			&item.DurationMs,
			&item.StartedAt,
			&item.FinishedAt,
			&item.Source,
			&item.RuntimeContractID,
			&item.AgentConnectionMode,
			&item.RuntimeTransport,
			&item.RuntimeTransportReason,
			&item.RuntimeTransportChangedAt,
			&item.DispatchState,
			&item.AttemptCount,
			&item.MaxAttempts,
			&item.NextAttemptAt,
			&item.LatestAttemptID,
			&item.ActiveAttemptID,
			&item.CancelState,
			&item.CancelRequestedAt,
			&item.CancelAcknowledgedAt,
			&item.CancelReason,
			&item.DeadLetteredAt,
			&item.ReplayOfRunID,
			&item.AgentSlug,
			&item.AgentName,
			&item.Direction,
			&item.ParentRunID,
			&item.CallerAgentID,
			&item.CallerAgentSlug,
			&item.CallerAgentName,
			&item.ProtocolContextID,
			&item.ProtocolTaskID,
			&item.RootContextID,
			&item.ParentContextID,
			&item.ParentTaskID,
			&item.TraceID,
			&item.ReferenceTaskIDs,
			&item.ContextSource,
			&item.CallID,
			&item.ChildCount,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const countCallRecordsForUser = `-- name: CountCallRecordsForUser :one
SELECT COUNT(*)::int AS total
FROM runs r
JOIN agents a ON a.id = r.agent_id
LEFT JOIN run_delegations d ON d.child_run_id = r.id
LEFT JOIN LATERAL (
    SELECT COUNT(*)::int AS child_count
    FROM run_delegations cd
    WHERE cd.parent_run_id = r.id
) children ON TRUE
WHERE (
    ($2 = 'made' AND r.user_id = $1)
    OR ($2 = 'received' AND a.creator_id = $1)
    OR ($2 = 'all' AND (r.user_id = $1 OR a.creator_id = $1))
)
AND (
    $3 = ''
    OR r.id::text ILIKE '%' || $3 || '%'
    OR r.agent_id::text ILIKE '%' || $3 || '%'
    OR r.status ILIKE '%' || $3 || '%'
    OR r.source ILIKE '%' || $3 || '%'
    OR a.slug ILIKE '%' || $3 || '%'
    OR a.name ILIKE '%' || $3 || '%'
    OR EXISTS (
        SELECT 1
        FROM run_delegations d
        LEFT JOIN agents caller ON caller.id = d.caller_agent_id
        WHERE d.child_run_id = r.id
          AND (
              d.parent_run_id::text ILIKE '%' || $3 || '%'
              OR d.caller_agent_id::text ILIKE '%' || $3 || '%'
              OR COALESCE(caller.slug, '') ILIKE '%' || $3 || '%'
              OR COALESCE(caller.name, '') ILIKE '%' || $3 || '%'
          )
    )
    OR EXISTS (
        SELECT 1
        FROM a2a_context_mappings ctx
        WHERE ctx.run_id = r.id
          AND (
              ctx.protocol_context_id ILIKE '%' || $3 || '%'
              OR ctx.protocol_task_id ILIKE '%' || $3 || '%'
              OR ctx.root_context_id ILIKE '%' || $3 || '%'
              OR ctx.parent_context_id ILIKE '%' || $3 || '%'
              OR ctx.parent_task_id ILIKE '%' || $3 || '%'
              OR ctx.trace_id ILIKE '%' || $3 || '%'
              OR ctx.source ILIKE '%' || $3 || '%'
              OR COALESCE(NULLIF(ctx.protocol_task_id, ''), r.id::text) ILIKE '%' || $3 || '%'
              OR array_to_string(ctx.reference_task_ids, ' ') ILIKE '%' || $3 || '%'
          )
    )
)
AND ($4 = '' OR r.status = $4)
AND ($5 = '' OR r.source = $5)
AND (
    $6 = ''
    OR ($6 = 'direct' AND d.parent_run_id IS NULL AND COALESCE(children.child_count, 0) = 0)
    OR ($6 = 'a2a_child' AND d.parent_run_id IS NOT NULL)
    OR ($6 = 'a2a_parent' AND d.parent_run_id IS NULL AND COALESCE(children.child_count, 0) > 0)
)`

type CountCallRecordsForUserParams struct {
	UserID   uuid.UUID `db:"user_id" json:"user_id"`
	View     string    `db:"view" json:"view"`
	Query    string    `db:"query" json:"query"`
	Status   string    `db:"status" json:"status"`
	Source   string    `db:"source" json:"source"`
	Relation string    `db:"relation" json:"relation"`
}

func (q *Queries) CountCallRecordsForUser(ctx context.Context, arg CountCallRecordsForUserParams) (int32, error) {
	row := q.db.QueryRow(ctx, countCallRecordsForUser, arg.UserID, arg.View, arg.Query, arg.Status, arg.Source, arg.Relation)
	var total int32
	err := row.Scan(&total)
	return total, err
}
