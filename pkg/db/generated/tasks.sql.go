// Hand-maintained SQL query implementation.
// Do not run sqlc generate against pkg/db/generated; use make sqlc CONFIRM_OVERWRITE=1 only for an intentional migration.
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
		&t.CompletedAt,
		&t.CompletionSummary,
		&t.CompletionRunID,
		&t.CreatedAt,
	)
}

const createTaskQuery = `-- name: CreateTaskQuery :one
INSERT INTO task_queries (user_id, query, parsed_skills, mcp_tools, recommended_agent_ids)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          completed_at, completion_summary, completion_run_id,
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
       completed_at, completion_summary, completion_run_id,
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
          completed_at, completion_summary, completion_run_id,
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

const listTaskQueriesByUser = `-- name: ListTaskQueriesByUser :many
SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
       chosen_agent_id, chosen_at,
       completed_at, completion_summary, completion_run_id,
       created_at
FROM task_queries
WHERE user_id = $1
  AND (
      $2::text = ''
      OR id::text ILIKE '%' || $2 || '%'
      OR query ILIKE '%' || $2 || '%'
      OR COALESCE(completion_summary, '') ILIKE '%' || $2 || '%'
      OR COALESCE(completion_run_id::text, '') ILIKE '%' || $2 || '%'
      OR array_to_string(COALESCE(parsed_skills, ARRAY[]::text[]), ' ') ILIKE '%' || $2 || '%'
      OR array_to_string(COALESCE(mcp_tools, ARRAY[]::text[]), ' ') ILIKE '%' || $2 || '%'
      OR COALESCE(recommended_agent_ids::text, '') ILIKE '%' || $2 || '%'
  )
  AND (
      $3::text = ''
      OR ($3 = 'completed' AND completed_at IS NOT NULL)
      OR ($3 = 'matched' AND completed_at IS NULL AND chosen_agent_id IS NOT NULL)
      OR ($3 = 'needs_agent' AND completed_at IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) = 0)
      OR ($3 = 'open' AND completed_at IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) > 0)
  )
ORDER BY
  CASE WHEN $4 = 'created_asc' THEN created_at END ASC,
  CASE WHEN $4 = 'created_desc' THEN created_at END DESC,
  created_at DESC,
  id DESC
LIMIT $5 OFFSET $6`

// ListTaskQueriesByUserParams 入参。
type ListTaskQueriesByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Query  string    `db:"query" json:"query"`
	Status string    `db:"status" json:"status"`
	Sort   string    `db:"sort" json:"sort"`
	Limit  int32     `db:"limit" json:"limit"`
	Offset int32     `db:"offset" json:"offset"`
}

// ListTaskQueriesByUser 用户最近 N 条任务历史（倒序）。
func (q *Queries) ListTaskQueriesByUser(ctx context.Context, arg ListTaskQueriesByUserParams) ([]TaskQuery, error) {
	rows, err := q.db.Query(ctx, listTaskQueriesByUser, arg.UserID, arg.Query, arg.Status, arg.Sort, arg.Limit, arg.Offset)
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

const countTaskQueriesByUser = `-- name: CountTaskQueriesByUser :one
SELECT COUNT(*)::int
FROM task_queries
WHERE user_id = $1
  AND (
      $2::text = ''
      OR id::text ILIKE '%' || $2 || '%'
      OR query ILIKE '%' || $2 || '%'
      OR COALESCE(completion_summary, '') ILIKE '%' || $2 || '%'
      OR COALESCE(completion_run_id::text, '') ILIKE '%' || $2 || '%'
      OR array_to_string(COALESCE(parsed_skills, ARRAY[]::text[]), ' ') ILIKE '%' || $2 || '%'
      OR array_to_string(COALESCE(mcp_tools, ARRAY[]::text[]), ' ') ILIKE '%' || $2 || '%'
      OR COALESCE(recommended_agent_ids::text, '') ILIKE '%' || $2 || '%'
  )
  AND (
      $3::text = ''
      OR ($3 = 'completed' AND completed_at IS NOT NULL)
      OR ($3 = 'matched' AND completed_at IS NULL AND chosen_agent_id IS NOT NULL)
      OR ($3 = 'needs_agent' AND completed_at IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) = 0)
      OR ($3 = 'open' AND completed_at IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) > 0)
  )`

type CountTaskQueriesByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Query  string    `db:"query" json:"query"`
	Status string    `db:"status" json:"status"`
}

func (q *Queries) CountTaskQueriesByUser(ctx context.Context, arg CountTaskQueriesByUserParams) (int32, error) {
	row := q.db.QueryRow(ctx, countTaskQueriesByUser, arg.UserID, arg.Query, arg.Status)
	var count int32
	err := row.Scan(&count)
	return count, err
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

const listAdminTasks = `-- name: ListAdminTasks :many
SELECT t.id, t.user_id, t.query, t.parsed_skills, t.mcp_tools, t.recommended_agent_ids,
       t.chosen_agent_id, t.chosen_at,
       t.completed_at, t.completion_summary, t.completion_run_id,
       t.created_at,
       u.email AS user_email,
       u.display_name AS user_display_name,
       chosen.slug AS chosen_agent_slug,
       chosen.name AS chosen_agent_name
FROM task_queries t
JOIN users u ON u.id = t.user_id
LEFT JOIN agents chosen ON chosen.id = t.chosen_agent_id
WHERE (
    $1::text = ''
    OR t.query ILIKE '%' || $1 || '%'
    OR COALESCE(t.completion_summary, '') ILIKE '%' || $1 || '%'
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
    OR chosen.slug ILIKE '%' || $1 || '%'
    OR chosen.name ILIKE '%' || $1 || '%'
  )
  AND (
    $2::text = ''
    OR ($2 = 'completed' AND t.completed_at IS NOT NULL)
    OR ($2 = 'matched' AND t.completed_at IS NULL AND t.chosen_agent_id IS NOT NULL)
    OR ($2 = 'needs_agent' AND t.completed_at IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) = 0)
    OR ($2 = 'open' AND t.completed_at IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) > 0)
  )
ORDER BY t.created_at DESC
LIMIT $3 OFFSET $4`

type ListAdminTasksParams struct {
	Query  string `db:"query" json:"query"`
	Status string `db:"status" json:"status"`
	Limit  int32  `db:"limit" json:"limit"`
	Offset int32  `db:"offset" json:"offset"`
}

type ListAdminTasksRow struct {
	TaskQuery
	UserEmail       string  `db:"user_email" json:"user_email"`
	UserDisplayName string  `db:"user_display_name" json:"user_display_name"`
	ChosenAgentSlug *string `db:"chosen_agent_slug" json:"chosen_agent_slug"`
	ChosenAgentName *string `db:"chosen_agent_name" json:"chosen_agent_name"`
}

func scanAdminTaskRow(row interface {
	Scan(dest ...any) error
}, r *ListAdminTasksRow) error {
	return row.Scan(
		&r.ID,
		&r.UserID,
		&r.Query,
		&r.ParsedSkills,
		&r.MCPTools,
		&r.RecommendedAgentIDs,
		&r.ChosenAgentID,
		&r.ChosenAt,
		&r.CompletedAt,
		&r.CompletionSummary,
		&r.CompletionRunID,
		&r.CreatedAt,
		&r.UserEmail,
		&r.UserDisplayName,
		&r.ChosenAgentSlug,
		&r.ChosenAgentName,
	)
}

func (q *Queries) ListAdminTasks(ctx context.Context, arg ListAdminTasksParams) ([]ListAdminTasksRow, error) {
	rows, err := q.db.Query(ctx, listAdminTasks, arg.Query, arg.Status, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListAdminTasksRow
	for rows.Next() {
		var r ListAdminTasksRow
		if err := scanAdminTaskRow(rows, &r); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const countAdminTasks = `-- name: CountAdminTasks :one
SELECT COUNT(*)::int AS total
FROM task_queries t
JOIN users u ON u.id = t.user_id
LEFT JOIN agents chosen ON chosen.id = t.chosen_agent_id
WHERE (
    $1::text = ''
    OR t.query ILIKE '%' || $1 || '%'
    OR COALESCE(t.completion_summary, '') ILIKE '%' || $1 || '%'
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
    OR chosen.slug ILIKE '%' || $1 || '%'
    OR chosen.name ILIKE '%' || $1 || '%'
  )
  AND (
    $2::text = ''
    OR ($2 = 'completed' AND t.completed_at IS NOT NULL)
    OR ($2 = 'matched' AND t.completed_at IS NULL AND t.chosen_agent_id IS NOT NULL)
    OR ($2 = 'needs_agent' AND t.completed_at IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) = 0)
    OR ($2 = 'open' AND t.completed_at IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) > 0)
  )`

type CountAdminTasksParams struct {
	Query  string `db:"query" json:"query"`
	Status string `db:"status" json:"status"`
}

func (q *Queries) CountAdminTasks(ctx context.Context, arg CountAdminTasksParams) (int32, error) {
	row := q.db.QueryRow(ctx, countAdminTasks, arg.Query, arg.Status)
	var total int32
	err := row.Scan(&total)
	return total, err
}
