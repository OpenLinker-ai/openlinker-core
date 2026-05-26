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
// parsed_skills 是 TEXT[]、recommended_agent_ids 是 UUID[]，pgx/v5 都能直接
// scan 到 []string / []uuid.UUID。
func scanTaskQuery(row interface {
	Scan(dest ...any) error
}, t *TaskQuery) error {
	return row.Scan(
		&t.ID,
		&t.UserID,
		&t.Query,
		&t.ParsedSkills,
		&t.RecommendedAgentIDs,
		&t.ChosenAgentID,
		&t.ChosenAt,
		&t.CreatedAt,
	)
}

const createTaskQuery = `-- name: CreateTaskQuery :one
INSERT INTO task_queries (user_id, query, parsed_skills, recommended_agent_ids)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, query, parsed_skills, recommended_agent_ids,
          chosen_agent_id, chosen_at, created_at`

// CreateTaskQueryParams 入参。
type CreateTaskQueryParams struct {
	UserID              uuid.UUID   `db:"user_id" json:"user_id"`
	Query               string      `db:"query" json:"query"`
	ParsedSkills        []string    `db:"parsed_skills" json:"parsed_skills"`
	RecommendedAgentIDs []uuid.UUID `db:"recommended_agent_ids" json:"recommended_agent_ids"`
}

// CreateTaskQuery 写入一条任务查询。
func (q *Queries) CreateTaskQuery(ctx context.Context, arg CreateTaskQueryParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, createTaskQuery,
		arg.UserID,
		arg.Query,
		arg.ParsedSkills,
		arg.RecommendedAgentIDs,
	)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const getTaskQuery = `-- name: GetTaskQuery :one
SELECT id, user_id, query, parsed_skills, recommended_agent_ids,
       chosen_agent_id, chosen_at, created_at
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
RETURNING id, user_id, query, parsed_skills, recommended_agent_ids,
          chosen_agent_id, chosen_at, created_at`

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
SELECT id, user_id, query, parsed_skills, recommended_agent_ids,
       chosen_agent_id, chosen_at, created_at
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
SELECT id, user_id, query, parsed_skills, recommended_agent_ids,
       chosen_agent_id, chosen_at, created_at
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
