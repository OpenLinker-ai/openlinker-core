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
		&t.Visibility,
		&t.PublicSummary,
		&t.PublishedAt,
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
          visibility, public_summary, published_at,
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
       visibility, public_summary, published_at,
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
          visibility, public_summary, published_at,
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

const publishTaskQuery = `-- name: PublishTaskQuery :one
UPDATE task_queries
SET visibility = 'public',
    public_summary = $3,
    published_at = COALESCE(published_at, NOW())
WHERE id = $1
  AND user_id = $2
  AND visibility = 'private'
  AND completed_at IS NULL
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          visibility, public_summary, published_at,
          created_at`

type PublishTaskQueryParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	UserID        uuid.UUID `db:"user_id" json:"user_id"`
	PublicSummary string    `db:"public_summary" json:"public_summary"`
}

func (q *Queries) PublishTaskQuery(ctx context.Context, arg PublishTaskQueryParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, publishTaskQuery, arg.ID, arg.UserID, arg.PublicSummary)
	var t TaskQuery
	err := scanTaskQuery(row, &t)
	return t, err
}

const unpublishTaskQuery = `-- name: UnpublishTaskQuery :one
UPDATE task_queries
SET visibility = 'private',
    public_summary = NULL,
    published_at = NULL
WHERE id = $1
  AND user_id = $2
  AND visibility = 'public'
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          visibility, public_summary, published_at,
          created_at`

type UnpublishTaskQueryParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	UserID uuid.UUID `db:"user_id" json:"user_id"`
}

func (q *Queries) UnpublishTaskQuery(ctx context.Context, arg UnpublishTaskQueryParams) (TaskQuery, error) {
	row := q.db.QueryRow(ctx, unpublishTaskQuery, arg.ID, arg.UserID)
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
  AND visibility = 'public'
  AND claimed_agent_id IS NULL
  AND completed_at IS NULL
  AND chosen_agent_id IS NULL
RETURNING id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
          chosen_agent_id, chosen_at,
          claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
          completed_at, completion_summary, completion_run_id,
          delivery_status, delivery_visibility, delivery_artifact,
          accepted_at, revision_requested_at, revision_note,
          visibility, public_summary, published_at,
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
          visibility, public_summary, published_at,
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
          visibility, public_summary, published_at,
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
          visibility, public_summary, published_at,
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
       visibility, public_summary, published_at,
       created_at
FROM task_queries
WHERE user_id = $1
  AND (
      $2::text = ''
      OR id::text ILIKE '%' || $2 || '%'
      OR query ILIKE '%' || $2 || '%'
      OR COALESCE(public_summary, '') ILIKE '%' || $2 || '%'
      OR COALESCE(completion_summary, '') ILIKE '%' || $2 || '%'
      OR COALESCE(revision_note, '') ILIKE '%' || $2 || '%'
      OR COALESCE(claim_run_id::text, '') ILIKE '%' || $2 || '%'
      OR COALESCE(completion_run_id::text, '') ILIKE '%' || $2 || '%'
      OR array_to_string(COALESCE(parsed_skills, ARRAY[]::text[]), ' ') ILIKE '%' || $2 || '%'
      OR array_to_string(COALESCE(mcp_tools, ARRAY[]::text[]), ' ') ILIKE '%' || $2 || '%'
      OR COALESCE(recommended_agent_ids::text, '') ILIKE '%' || $2 || '%'
  )
  AND ($3::text = '' OR visibility = $3)
  AND (
      $4::text = ''
      OR ($4 = 'accepted' AND delivery_status = 'accepted')
      OR ($4 = 'revision_requested' AND delivery_status = 'revision_requested')
      OR ($4 = 'completed' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NOT NULL)
      OR ($4 = 'in_progress' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NOT NULL)
      OR ($4 = 'matched' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NOT NULL)
      OR ($4 = 'needs_agent' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) = 0)
      OR ($4 = 'open' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) > 0)
  )
ORDER BY
  CASE WHEN $5 = 'created_asc' THEN created_at END ASC,
  CASE WHEN $5 = 'created_desc' THEN created_at END DESC,
  created_at DESC,
  id DESC
LIMIT $6 OFFSET $7`

// ListTaskQueriesByUserParams 入参。
type ListTaskQueriesByUserParams struct {
	UserID     uuid.UUID `db:"user_id" json:"user_id"`
	Query      string    `db:"query" json:"query"`
	Visibility string    `db:"visibility" json:"visibility"`
	Status     string    `db:"status" json:"status"`
	Sort       string    `db:"sort" json:"sort"`
	Limit      int32     `db:"limit" json:"limit"`
	Offset     int32     `db:"offset" json:"offset"`
}

// ListTaskQueriesByUser 用户最近 N 条任务历史（倒序）。
func (q *Queries) ListTaskQueriesByUser(ctx context.Context, arg ListTaskQueriesByUserParams) ([]TaskQuery, error) {
	rows, err := q.db.Query(ctx, listTaskQueriesByUser, arg.UserID, arg.Query, arg.Visibility, arg.Status, arg.Sort, arg.Limit, arg.Offset)
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
      OR COALESCE(public_summary, '') ILIKE '%' || $2 || '%'
      OR COALESCE(completion_summary, '') ILIKE '%' || $2 || '%'
      OR COALESCE(revision_note, '') ILIKE '%' || $2 || '%'
      OR COALESCE(claim_run_id::text, '') ILIKE '%' || $2 || '%'
      OR COALESCE(completion_run_id::text, '') ILIKE '%' || $2 || '%'
      OR array_to_string(COALESCE(parsed_skills, ARRAY[]::text[]), ' ') ILIKE '%' || $2 || '%'
      OR array_to_string(COALESCE(mcp_tools, ARRAY[]::text[]), ' ') ILIKE '%' || $2 || '%'
      OR COALESCE(recommended_agent_ids::text, '') ILIKE '%' || $2 || '%'
  )
  AND ($3::text = '' OR visibility = $3)
  AND (
      $4::text = ''
      OR ($4 = 'accepted' AND delivery_status = 'accepted')
      OR ($4 = 'revision_requested' AND delivery_status = 'revision_requested')
      OR ($4 = 'completed' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NOT NULL)
      OR ($4 = 'in_progress' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NOT NULL)
      OR ($4 = 'matched' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NOT NULL)
      OR ($4 = 'needs_agent' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) = 0)
      OR ($4 = 'open' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) > 0)
  )`

type CountTaskQueriesByUserParams struct {
	UserID     uuid.UUID `db:"user_id" json:"user_id"`
	Query      string    `db:"query" json:"query"`
	Visibility string    `db:"visibility" json:"visibility"`
	Status     string    `db:"status" json:"status"`
}

func (q *Queries) CountTaskQueriesByUser(ctx context.Context, arg CountTaskQueriesByUserParams) (int32, error) {
	row := q.db.QueryRow(ctx, countTaskQueriesByUser, arg.UserID, arg.Query, arg.Visibility, arg.Status)
	var count int32
	err := row.Scan(&count)
	return count, err
}

const listPublicTaskQueries = `-- name: ListPublicTaskQueries :many
SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
       chosen_agent_id, chosen_at,
       claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
       completed_at, completion_summary, completion_run_id,
       delivery_status, delivery_visibility, delivery_artifact,
       accepted_at, revision_requested_at, revision_note,
       visibility, public_summary, published_at,
       created_at
FROM task_queries
WHERE visibility = 'public'
ORDER BY published_at DESC, created_at DESC
LIMIT $1`

// ListPublicTaskQueries 最近公开任务流（任务广场用）。
//
// 只返回 visibility=public 的任务；本查询不返回用户邮箱/姓名，仅用于展示
// 公开摘要、Skill 和匹配状态。
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

const listPublicTaskQueriesPage = `-- name: ListPublicTaskQueriesPage :many
SELECT id, user_id, query, parsed_skills, mcp_tools, recommended_agent_ids,
       chosen_agent_id, chosen_at,
       claimed_agent_id, claimed_by_user_id, claimed_at, claim_run_id,
       completed_at, completion_summary, completion_run_id,
       delivery_status, delivery_visibility, delivery_artifact,
       accepted_at, revision_requested_at, revision_note,
       visibility, public_summary, published_at,
       created_at
FROM task_queries
WHERE visibility = 'public'
  AND (
      $1::text = ''
      OR id::text ILIKE '%' || $1 || '%'
      OR COALESCE(public_summary, '') ILIKE '%' || $1 || '%'
      OR array_to_string(COALESCE(parsed_skills, ARRAY[]::text[]), ' ') ILIKE '%' || $1 || '%'
      OR array_to_string(COALESCE(mcp_tools, ARRAY[]::text[]), ' ') ILIKE '%' || $1 || '%'
  )
  AND (
      $2::text = ''
      OR ($2 = 'accepted' AND delivery_status = 'accepted')
      OR ($2 = 'revision_requested' AND delivery_status = 'revision_requested')
      OR ($2 = 'completed' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NOT NULL)
      OR ($2 = 'in_progress' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NOT NULL)
      OR ($2 = 'matched' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NOT NULL)
      OR ($2 = 'needs_agent' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) = 0)
      OR ($2 = 'open' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) > 0)
  )
  AND (
      cardinality(COALESCE($3::text[], ARRAY[]::text[])) = 0
      OR COALESCE(parsed_skills, ARRAY[]::text[]) && COALESCE($3::text[], ARRAY[]::text[])
  )
  AND ($4::text = '' OR $4 = ANY(COALESCE(mcp_tools, ARRAY[]::text[])))
ORDER BY
  CASE WHEN $5 = 'published_asc' THEN COALESCE(published_at, created_at) END ASC,
  CASE WHEN $5 = 'created_desc' THEN created_at END DESC,
  CASE WHEN $5 = 'recommended_desc' THEN cardinality(recommended_agent_ids) END DESC,
  COALESCE(published_at, created_at) DESC,
  created_at DESC,
  id DESC
LIMIT $6 OFFSET $7`

type ListPublicTaskQueriesPageParams struct {
	Query    string   `db:"query" json:"query"`
	Status   string   `db:"status" json:"status"`
	SkillIDs []string `db:"skill_ids" json:"skill_ids"`
	MCP      string   `db:"mcp" json:"mcp"`
	Sort     string   `db:"sort" json:"sort"`
	Limit    int32    `db:"limit" json:"limit"`
	Offset   int32    `db:"offset" json:"offset"`
}

// ListPublicTaskQueriesPage returns public task board rows with search,
// filters, sorting, and pagination.
func (q *Queries) ListPublicTaskQueriesPage(ctx context.Context, arg ListPublicTaskQueriesPageParams) ([]TaskQuery, error) {
	rows, err := q.db.Query(ctx, listPublicTaskQueriesPage, arg.Query, arg.Status, arg.SkillIDs, arg.MCP, arg.Sort, arg.Limit, arg.Offset)
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

const countPublicTaskQueriesPage = `-- name: CountPublicTaskQueriesPage :one
SELECT COUNT(*)::int
FROM task_queries
WHERE visibility = 'public'
  AND (
      $1::text = ''
      OR id::text ILIKE '%' || $1 || '%'
      OR COALESCE(public_summary, '') ILIKE '%' || $1 || '%'
      OR array_to_string(COALESCE(parsed_skills, ARRAY[]::text[]), ' ') ILIKE '%' || $1 || '%'
      OR array_to_string(COALESCE(mcp_tools, ARRAY[]::text[]), ' ') ILIKE '%' || $1 || '%'
  )
  AND (
      $2::text = ''
      OR ($2 = 'accepted' AND delivery_status = 'accepted')
      OR ($2 = 'revision_requested' AND delivery_status = 'revision_requested')
      OR ($2 = 'completed' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NOT NULL)
      OR ($2 = 'in_progress' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NOT NULL)
      OR ($2 = 'matched' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NOT NULL)
      OR ($2 = 'needs_agent' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) = 0)
      OR ($2 = 'open' AND delivery_status NOT IN ('accepted', 'revision_requested') AND completed_at IS NULL AND claimed_agent_id IS NULL AND chosen_agent_id IS NULL AND cardinality(recommended_agent_ids) > 0)
  )
  AND (
      cardinality(COALESCE($3::text[], ARRAY[]::text[])) = 0
      OR COALESCE(parsed_skills, ARRAY[]::text[]) && COALESCE($3::text[], ARRAY[]::text[])
  )
  AND ($4::text = '' OR $4 = ANY(COALESCE(mcp_tools, ARRAY[]::text[])))`

type CountPublicTaskQueriesPageParams struct {
	Query    string   `db:"query" json:"query"`
	Status   string   `db:"status" json:"status"`
	SkillIDs []string `db:"skill_ids" json:"skill_ids"`
	MCP      string   `db:"mcp" json:"mcp"`
}

func (q *Queries) CountPublicTaskQueriesPage(ctx context.Context, arg CountPublicTaskQueriesPageParams) (int32, error) {
	row := q.db.QueryRow(ctx, countPublicTaskQueriesPage, arg.Query, arg.Status, arg.SkillIDs, arg.MCP)
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
       t.claimed_agent_id, t.claimed_by_user_id, t.claimed_at, t.claim_run_id,
       t.completed_at, t.completion_summary, t.completion_run_id,
       t.delivery_status, t.delivery_visibility, t.delivery_artifact,
       t.accepted_at, t.revision_requested_at, t.revision_note,
       t.visibility, t.public_summary, t.published_at,
       t.created_at,
       u.email AS user_email,
       u.display_name AS user_display_name,
       chosen.slug AS chosen_agent_slug,
       chosen.name AS chosen_agent_name,
       claimed.slug AS claimed_agent_slug,
       claimed.name AS claimed_agent_name,
       claimed_user.email AS claimed_by_email,
       claimed_user.display_name AS claimed_by_display_name
FROM task_queries t
JOIN users u ON u.id = t.user_id
LEFT JOIN agents chosen ON chosen.id = t.chosen_agent_id
LEFT JOIN agents claimed ON claimed.id = t.claimed_agent_id
LEFT JOIN users claimed_user ON claimed_user.id = t.claimed_by_user_id
WHERE (
    $1::text = ''
    OR t.query ILIKE '%' || $1 || '%'
    OR COALESCE(t.public_summary, '') ILIKE '%' || $1 || '%'
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
    OR chosen.slug ILIKE '%' || $1 || '%'
    OR chosen.name ILIKE '%' || $1 || '%'
    OR claimed.slug ILIKE '%' || $1 || '%'
    OR claimed.name ILIKE '%' || $1 || '%'
  )
  AND ($2::text = '' OR t.visibility = $2)
  AND ($3::text = '' OR t.delivery_status = $3)
  AND (
    $4::text = ''
    OR ($4 = 'accepted' AND t.delivery_status = 'accepted')
    OR ($4 = 'revision_requested' AND t.delivery_status = 'revision_requested')
    OR ($4 = 'completed' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NOT NULL)
    OR ($4 = 'in_progress' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NOT NULL)
    OR ($4 = 'matched' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NOT NULL)
    OR ($4 = 'needs_agent' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) = 0)
    OR ($4 = 'open' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) > 0)
  )
ORDER BY t.created_at DESC
LIMIT $5 OFFSET $6`

type ListAdminTasksParams struct {
	Query          string `db:"query" json:"query"`
	Visibility     string `db:"visibility" json:"visibility"`
	DeliveryStatus string `db:"delivery_status" json:"delivery_status"`
	Status         string `db:"status" json:"status"`
	Limit          int32  `db:"limit" json:"limit"`
	Offset         int32  `db:"offset" json:"offset"`
}

type ListAdminTasksRow struct {
	TaskQuery
	UserEmail            string  `db:"user_email" json:"user_email"`
	UserDisplayName      string  `db:"user_display_name" json:"user_display_name"`
	ChosenAgentSlug      *string `db:"chosen_agent_slug" json:"chosen_agent_slug"`
	ChosenAgentName      *string `db:"chosen_agent_name" json:"chosen_agent_name"`
	ClaimedAgentSlug     *string `db:"claimed_agent_slug" json:"claimed_agent_slug"`
	ClaimedAgentName     *string `db:"claimed_agent_name" json:"claimed_agent_name"`
	ClaimedByEmail       *string `db:"claimed_by_email" json:"claimed_by_email"`
	ClaimedByDisplayName *string `db:"claimed_by_display_name" json:"claimed_by_display_name"`
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
		&r.ClaimedAgentID,
		&r.ClaimedByUserID,
		&r.ClaimedAt,
		&r.ClaimRunID,
		&r.CompletedAt,
		&r.CompletionSummary,
		&r.CompletionRunID,
		&r.DeliveryStatus,
		&r.DeliveryVisibility,
		&r.DeliveryArtifact,
		&r.AcceptedAt,
		&r.RevisionRequestedAt,
		&r.RevisionNote,
		&r.Visibility,
		&r.PublicSummary,
		&r.PublishedAt,
		&r.CreatedAt,
		&r.UserEmail,
		&r.UserDisplayName,
		&r.ChosenAgentSlug,
		&r.ChosenAgentName,
		&r.ClaimedAgentSlug,
		&r.ClaimedAgentName,
		&r.ClaimedByEmail,
		&r.ClaimedByDisplayName,
	)
}

func (q *Queries) ListAdminTasks(ctx context.Context, arg ListAdminTasksParams) ([]ListAdminTasksRow, error) {
	rows, err := q.db.Query(ctx, listAdminTasks, arg.Query, arg.Visibility, arg.DeliveryStatus, arg.Status, arg.Limit, arg.Offset)
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
LEFT JOIN agents claimed ON claimed.id = t.claimed_agent_id
WHERE (
    $1::text = ''
    OR t.query ILIKE '%' || $1 || '%'
    OR COALESCE(t.public_summary, '') ILIKE '%' || $1 || '%'
    OR u.email ILIKE '%' || $1 || '%'
    OR u.display_name ILIKE '%' || $1 || '%'
    OR chosen.slug ILIKE '%' || $1 || '%'
    OR chosen.name ILIKE '%' || $1 || '%'
    OR claimed.slug ILIKE '%' || $1 || '%'
    OR claimed.name ILIKE '%' || $1 || '%'
  )
  AND ($2::text = '' OR t.visibility = $2)
  AND ($3::text = '' OR t.delivery_status = $3)
  AND (
    $4::text = ''
    OR ($4 = 'accepted' AND t.delivery_status = 'accepted')
    OR ($4 = 'revision_requested' AND t.delivery_status = 'revision_requested')
    OR ($4 = 'completed' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NOT NULL)
    OR ($4 = 'in_progress' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NOT NULL)
    OR ($4 = 'matched' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NOT NULL)
    OR ($4 = 'needs_agent' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) = 0)
    OR ($4 = 'open' AND t.delivery_status NOT IN ('accepted', 'revision_requested') AND t.completed_at IS NULL AND t.claimed_agent_id IS NULL AND t.chosen_agent_id IS NULL AND cardinality(t.recommended_agent_ids) > 0)
  )`

type CountAdminTasksParams struct {
	Query          string `db:"query" json:"query"`
	Visibility     string `db:"visibility" json:"visibility"`
	DeliveryStatus string `db:"delivery_status" json:"delivery_status"`
	Status         string `db:"status" json:"status"`
}

func (q *Queries) CountAdminTasks(ctx context.Context, arg CountAdminTasksParams) (int32, error) {
	row := q.db.QueryRow(ctx, countAdminTasks, arg.Query, arg.Visibility, arg.DeliveryStatus, arg.Status)
	var total int32
	err := row.Scan(&total)
	return total, err
}
