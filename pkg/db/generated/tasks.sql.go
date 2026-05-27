// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/tasks.sql）。
//
// 子轮 2.4 任务驱动 A 形态：task_queries 表 CRUD + 批量取 Agent 详情。

package db

import (
	"context"

	"github.com/google/uuid"
)

// scanTaskQuery 把一行扫描成 TaskQuery（按声明列顺序，给 RETURNING / SELECT 共用）。
//
// parsed_skills / mcp_tools 是 TEXT[]、recommended_agent_ids 是 UUID[]，pgx/v5 都能直接
// scan 到 []string / []uuid.UUID。
func scanTaskQuery(row interface {
	Scan(dest ...any) error
}, t *TaskQuery) error {
	return row.Scan(
		&t.ID,
		&t.UserID,
		&t.Query,
		&t.ParsedSkills,
		&t.MCPTools,
		&t.RecommendedAgentIDs,
		&t.ChosenAgentID,
		&t.ChosenAt,
		&t.ClaimedAgentID,
		&t.ClaimedByUserID,
		&t.ClaimedAt,
		&t.ClaimRunID,
		&t.CompletedAt,
		&t.CompletionSummary,
		&t.CompletionRunID,
		&t.DeliveryStatus,
		&t.DeliveryVisibility,
		&t.DeliveryArtifact,
		&t.AcceptedAt,
		&t.RevisionRequestedAt,
		&t.RevisionNote,
		&t.CreatedAt,
	)
}

const createTaskQuery = `-- name: CreateTaskQuery :one
INSERT INTO task_queries (user_id, query, parsed_skills, mcp_tools, recommended_agent_ids)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          created_at`

// CreateTaskQueryParams 入参。
type CreateTaskQueryParams struct {
	UserID              uuid.UUID   `db:"user_id" json:"user_id"`
	Query               string      `db:"query" json:"query"`
	ParsedSkills        []string    `db:"parsed_skills" json:"parsed_skills"`
	MCPTools            []string    `db:"mcp_tools" json:"mcp_tools"`
	RecommendedAgentIDs []uuid.UUID `db:"recommended_agent_ids" json:"recommended_agent_ids"`
}

// CreateTaskQuery 写入一条任务查询。
func (q *Queries) CreateTaskQuery(ctx context.Context, arg CreateTaskQueryParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, createTaskQuery,
		arg.UserID,
		arg.Query,
		arg.ParsedSkills,
		arg.MCPTools,
		arg.RecommendedAgentIDs,
	)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const getTaskQuery = `-- name: GetTaskQuery :one
SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
       chosen_agent_id, chosen_at,
       claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
       completed_at, completion_summary, completion_run_id,
       delivery_status, delivery_visibility, delivery_artifact,
       accepted_at, revision_requested_at, revision_note,
       created_at
FROM task_queries
WHERE id = $1`

// GetTaskQuery 按 id 查单条；调用方需自行校验 user_id 归属。
func (q *Queries) GetTaskQuery(ctx context.Context, id uuid.UUID) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, getTaskQuery, id)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const markTaskQueryChosen = `-- name: MarkTaskQueryChosen :one
UPDATE task_queries
SET chosen_agent_id = $3,
    chosen_at = NOW()
WHERE id = $1 AND user_id = $2
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          created_at`

// MarkTaskQueryChosenParams 入参。
type MarkTaskQueryChosenParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	UserID        uuid.UUID `db:"user_id" json:"user_id"`
	ChosenAgentID uuid.UUID `db:"chosen_agent_id" json:"chosen_agent_id"`
}

// MarkTaskQueryChosen 用户选定推荐里某个 agent；返回 pgx.ErrNoRows 表示不存在或越权。
func (q *Queries) MarkTaskQueryChosen(ctx context.Context, arg MarkTaskQueryChosenParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, markTaskQueryChosen, arg.ID, arg.UserID, arg.ChosenAgentID)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const claimTaskQuery = `-- name: ClaimTaskQuery :one
UPDATE task_queries
SET claimed_agent_id = $3,
    claimed_by_user_id = $2,
    claimed_at = NOW()
WHERE id = $1
  AND claimed_agent_id IS NULL
  AND completed_at IS NULL
  AND chosen_agent_id IS NULL
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          created_at`

type ClaimTaskQueryParams struct {
	ID      uuid.UUID `db:"id" json:"id"`
	UserID  uuid.UUID `db:"user_id" json:"user_id"`
	AgentID uuid.UUID `db:"agent_id" json:"agent_id"`
}

func (q *Queries) ClaimTaskQuery(ctx context.Context, arg ClaimTaskQueryParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, claimTaskQuery, arg.ID, arg.UserID, arg.AgentID)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const completeTaskQuery = `-- name: CompleteTaskQuery :one
UPDATE task_queries
SET claimed_agent_id = COALESCE(claimed_agent_id, $3),
    claimed_by_user_id = COALESCE(claimed_by_user_id, $2),
    claimed_at = COALESCE(claimed_at, NOW()),
    claim_run_id = COALESCE(claim_run_id, $4),
    completed_at = NOW(),
    completion_summary = $5,
    completion_run_id = $4,
    delivery_status = 'submitted',
    delivery_artifact = $6,
    delivery_visibility = $7,
    accepted_at = NULL,
    revision_requested_at = NULL,
    revision_note = NULL
WHERE id = $1
  AND (completed_at IS NULL OR delivery_status = 'revision_requested')
  AND (user_id = $2 OR claimed_by_user_id = $2)
  AND (
      claimed_agent_id = $3
      OR (claimed_agent_id IS NULL AND chosen_agent_id = $3)
  )
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          created_at`

type CompleteTaskQueryParams struct {
	ID                 uuid.UUID `db:"id" json:"id"`
	UserID             uuid.UUID `db:"user_id" json:"user_id"`
	AgentID            uuid.UUID `db:"agent_id" json:"agent_id"`
	CompletionRunID    uuid.UUID `db:"completion_run_id" json:"completion_run_id"`
	CompletionSummary  string    `db:"completion_summary" json:"completion_summary"`
	DeliveryArtifact   []byte    `db:"delivery_artifact" json:"delivery_artifact"`
	DeliveryVisibility string    `db:"delivery_visibility" json:"delivery_visibility"`
}

func (q *Queries) CompleteTaskQuery(ctx context.Context, arg CompleteTaskQueryParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, completeTaskQuery,
		arg.ID,
		arg.UserID,
		arg.AgentID,
		arg.CompletionRunID,
		arg.CompletionSummary,
		arg.DeliveryArtifact,
		arg.DeliveryVisibility,
	)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const acceptTaskDelivery = `-- name: AcceptTaskDelivery :one
UPDATE task_queries
SET delivery_status = 'accepted',
    accepted_at = NOW()
WHERE id = $1
  AND user_id = $2
  AND delivery_status = 'submitted'
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          created_at`

type AcceptTaskDeliveryParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) AcceptTaskDelivery(ctx context.Context, arg AcceptTaskDeliveryParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, acceptTaskDelivery, arg.ID, arg.UserID)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const requestTaskRevision = `-- name: RequestTaskRevision :one
UPDATE task_queries
SET delivery_status = 'revision_requested',
    revision_requested_at = NOW(),
    revision_note = $3,
    accepted_at = NULL
WHERE id = $1
  AND user_id = $2
  AND delivery_status = 'submitted'
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          created_at`

type RequestTaskRevisionParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	UserID       uuid.UUID `db:"user_id" json:"user_id"`
	RevisionNote string    `db:"revision_note" json:"revision_note"`
}

func (q *Queries) RequestTaskRevision(ctx context.Context, arg RequestTaskRevisionParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, requestTaskRevision, arg.ID, arg.UserID, arg.RevisionNote)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const listTaskQueriesByUser = `-- name: ListTaskQueriesByUser :many
SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
       chosen_agent_id, chosen_at,
       claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
       completed_at, completion_summary, completion_run_id,
       delivery_status, delivery_visibility, delivery_artifact,
       accepted_at, revision_requested_at, revision_note,
       created_at
FROM task_queries
WHERE user_id = $1
ORDER BY created_at DESC
LIMIT $2`

// ListTaskQueriesByUserParams 入参。
type ListTaskQueriesByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Limit  int32     `db:"limit" json:"limit"`
}

// ListTaskQueriesByUser 用户最近 N 条任务历史（倒序）。
func (q *Queries) ListTaskQueriesByUser(ctx context.Context, arg ListTaskQueriesByUserParams) ([]TaskQuery, error) {
	rows, err := q.db.Query(ctx, listTaskQueriesByUser, arg.UserID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TaskQuery
	for rows.Next() {
		var t TaskQuery
		if err := scanTaskQuery(rows, &t); err != nil {
			return nil, err
		}
		items = append(items, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listPublicTaskQueries = `-- name: ListPublicTaskQueries :many
SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
       chosen_agent_id, chosen_at,
       claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
       completed_at, completion_summary, completion_run_id,
       delivery_status, delivery_visibility, delivery_artifact,
       accepted_at, revision_requested_at, revision_note,
       created_at
FROM task_queries
ORDER BY created_at DESC
LIMIT $1`

// ListPublicTaskQueries 最近公开任务流（任务广场用）。
//
// 当前 task_queries 暂无 visibility 字段；本查询不返回用户邮箱/姓名，仅用于展示
// 需求文本、Skill 和匹配状态。
func (q *Queries) ListPublicTaskQueries(ctx context.Context, limit int32) ([]TaskQuery, error) {
	rows, err := q.db.Query(ctx, listPublicTaskQueries, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TaskQuery
	for rows.Next() {
		var t TaskQuery
		if err := scanTaskQuery(rows, &t); err != nil {
			return nil, err
		}
		items = append(items, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getAgentsByIDs = `-- name: GetAgentsByIDs :many
SELECT a.id, a.creator_id, a.slug, a.name, a.description, a.endpoint_url,
       a.endpoint_auth_header, a.price_per_call_cents, a.tags,
       a.lifecycle_status, a.visibility, a.certification_status,
       a.rejection_reason, a.certified_at, a.total_calls, a.total_revenue_cents,
       a.webhook_url, a.created_at, a.updated_at, u.display_name AS creator_name
FROM agents a
JOIN users u ON u.id = a.creator_id
WHERE a.id = ANY($1::uuid[])
  AND a.visibility = 'public'
  AND a.lifecycle_status = 'active'`

// GetAgentsByIDsRow Agent 全字段 + creator 显示名。
type GetAgentsByIDsRow struct {
	Agent
	CreatorName string `db:"creator_name" json:"creator_name"`
}

// GetAgentsByIDs 批量按 id 取当前公开运行的 Agent 详情（任务推荐回填用）。
// 返回顺序由 Postgres 决定（无序），调用方需按入参顺序自行重排。
func (q *Queries) GetAgentsByIDs(ctx context.Context, ids []uuid.UUID) ([]GetAgentsByIDsRow, error) {
	rows, err := q.db.Query(ctx, getAgentsByIDs, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetAgentsByIDsRow
	for rows.Next() {
		var r GetAgentsByIDsRow
		if err := rows.Scan(
			&r.ID,
			&r.CreatorID,
			&r.Slug,
			&r.Name,
			&r.Description,
			&r.EndpointURL,
			&r.EndpointAuthHeader,
			&r.PricePerCallCents,
			&r.Tags,
			&r.LifecycleStatus,
			&r.Visibility,
			&r.CertificationStatus,
			&r.RejectionReason,
			&r.CertifiedAt,
			&r.TotalCalls,
			&r.TotalRevenueCents,
			&r.WebhookURL,
			&r.CreatedAt,
			&r.UpdatedAt,
			&r.CreatorName,
		); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
