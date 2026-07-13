// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/benchmark.sql）。
//
// 模块 B（Phase 2 §5）：Skill Benchmark。

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ──────────────────────────────────────────────────────
// skill_test_cases
// ──────────────────────────────────────────────────────

func scanSkillTestCase(row interface {
	Scan(dest ...any) error
}, c *SkillTestCase) error {
	return row.Scan(
		&c.ID,
		&c.SkillID,
		&c.Title,
		&c.InputJSON,
		&c.JudgePrompt,
		&c.SortOrder,
		&c.CreatedAt,
	)
}

const listTestCasesBySkill = `-- name: ListTestCasesBySkill :many
SELECT id, skill_id, title, input_json, judge_prompt, sort_order, created_at
FROM skill_test_cases
WHERE skill_id = $1
ORDER BY sort_order, created_at`

func (q *Queries) ListTestCasesBySkill(ctx context.Context, skillID string) ([]SkillTestCase, error) {
	rows, err := q.db.Query(ctx, listTestCasesBySkill, skillID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []SkillTestCase
	for rows.Next() {
		var c SkillTestCase
		if err := scanSkillTestCase(rows, &c); err != nil {
			return nil, err
		}
		items = append(items, c)
	}
	return items, rows.Err()
}

const countTestCasesBySkill = `-- name: CountTestCasesBySkill :one
SELECT COUNT(*)::int FROM skill_test_cases WHERE skill_id = $1`

func (q *Queries) CountTestCasesBySkill(ctx context.Context, skillID string) (int32, error) {
	row := q.db.QueryRow(ctx, countTestCasesBySkill, skillID)
	var count int32
	err := row.Scan(&count)
	return count, err
}

// ──────────────────────────────────────────────────────
// agent_skill_benchmark_runs
// ──────────────────────────────────────────────────────

func scanBenchmarkRun(row interface {
	Scan(dest ...any) error
}, r *AgentSkillBenchmarkRun) error {
	return row.Scan(
		&r.ID,
		&r.BatchID,
		&r.AgentID,
		&r.SkillID,
		&r.TestCaseID,
		&r.Status,
		&r.Score,
		&r.RawOutput,
		&r.JudgeReasoning,
		&r.ErrorMessage,
		&r.StartedAt,
		&r.FinishedAt,
	)
}

const createBenchmarkRun = `-- name: CreateBenchmarkRun :one
INSERT INTO agent_skill_benchmark_runs (
    batch_id, agent_id, skill_id, test_case_id, status, started_at
) VALUES ($1, $2, $3, $4, 'pending', NOW())
RETURNING id, batch_id, agent_id, skill_id, test_case_id, status,
          score, raw_output, judge_reasoning, error_message,
          started_at, finished_at`

type CreateBenchmarkRunParams struct {
	BatchID    uuid.UUID `db:"batch_id" json:"batch_id"`
	AgentID    uuid.UUID `db:"agent_id" json:"agent_id"`
	SkillID    string    `db:"skill_id" json:"skill_id"`
	TestCaseID uuid.UUID `db:"test_case_id" json:"test_case_id"`
}

func (q *Queries) CreateBenchmarkRun(ctx context.Context, arg CreateBenchmarkRunParams) (AgentSkillBenchmarkRun, error) {
	row := q.db.QueryRow(ctx, createBenchmarkRun,
		arg.BatchID, arg.AgentID, arg.SkillID, arg.TestCaseID,
	)
	var r AgentSkillBenchmarkRun
	err := scanBenchmarkRun(row, &r)
	return r, err
}

const markBenchmarkRunSuccess = `-- name: MarkBenchmarkRunSuccess :execrows
UPDATE agent_skill_benchmark_runs
SET status = 'success',
    score = $2,
    raw_output = $3,
    judge_reasoning = $4,
    finished_at = NOW()
WHERE id = $1`

type MarkBenchmarkRunSuccessParams struct {
	ID             uuid.UUID `db:"id" json:"id"`
	Score          int32     `db:"score" json:"score"`
	RawOutput      []byte    `db:"raw_output" json:"raw_output"`
	JudgeReasoning string    `db:"judge_reasoning" json:"judge_reasoning"`
}

func (q *Queries) MarkBenchmarkRunSuccess(ctx context.Context, arg MarkBenchmarkRunSuccessParams) (int64, error) {
	tag, err := q.db.Exec(ctx, markBenchmarkRunSuccess, arg.ID, arg.Score, arg.RawOutput, arg.JudgeReasoning)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const markBenchmarkRunFailed = `-- name: MarkBenchmarkRunFailed :execrows
UPDATE agent_skill_benchmark_runs
SET status = 'failed',
    error_message = $2,
    finished_at = NOW()
WHERE id = $1`

type MarkBenchmarkRunFailedParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	ErrorMessage string    `db:"error_message" json:"error_message"`
}

func (q *Queries) MarkBenchmarkRunFailed(ctx context.Context, arg MarkBenchmarkRunFailedParams) (int64, error) {
	tag, err := q.db.Exec(ctx, markBenchmarkRunFailed, arg.ID, arg.ErrorMessage)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const listBenchmarkRunsByBatch = `-- name: ListBenchmarkRunsByBatch :many
SELECT r.id, r.batch_id, r.agent_id, r.skill_id, r.test_case_id, r.status,
       r.score, r.raw_output, r.judge_reasoning, r.error_message,
       r.started_at, r.finished_at,
       c.title AS test_case_title
FROM agent_skill_benchmark_runs r
JOIN skill_test_cases c ON c.id = r.test_case_id
WHERE r.batch_id = $1
ORDER BY c.sort_order, c.created_at`

// ListBenchmarkRunsByBatchRow 行类型：benchmark run + test case 标题。
type ListBenchmarkRunsByBatchRow struct {
	AgentSkillBenchmarkRun
	TestCaseTitle string `db:"test_case_title" json:"test_case_title"`
}

func (q *Queries) ListBenchmarkRunsByBatch(ctx context.Context, batchID uuid.UUID) ([]ListBenchmarkRunsByBatchRow, error) {
	rows, err := q.db.Query(ctx, listBenchmarkRunsByBatch, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListBenchmarkRunsByBatchRow
	for rows.Next() {
		var item ListBenchmarkRunsByBatchRow
		if err := rows.Scan(
			&item.ID, &item.BatchID, &item.AgentID, &item.SkillID, &item.TestCaseID,
			&item.Status, &item.Score, &item.RawOutput, &item.JudgeReasoning,
			&item.ErrorMessage, &item.StartedAt, &item.FinishedAt,
			&item.TestCaseTitle,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ──────────────────────────────────────────────────────
// agent_skill_scores
// ──────────────────────────────────────────────────────

func scanAgentSkillScore(row interface {
	Scan(dest ...any) error
}, s *AgentSkillScore) error {
	return row.Scan(
		&s.AgentID,
		&s.SkillID,
		&s.Status,
		&s.AverageScore,
		&s.PassCount,
		&s.TotalCount,
		&s.LastBatchID,
		&s.VerifiedAt,
		&s.UpdatedAt,
	)
}

const upsertAgentSkillScore = `-- name: UpsertAgentSkillScore :one
INSERT INTO agent_skill_scores (
    agent_id, skill_id, status, average_score,
    pass_count, total_count, last_batch_id, verified_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT ON CONSTRAINT agent_skill_scores_pkey DO UPDATE
SET status = EXCLUDED.status,
    average_score = EXCLUDED.average_score,
    pass_count = EXCLUDED.pass_count,
    total_count = EXCLUDED.total_count,
    last_batch_id = EXCLUDED.last_batch_id,
    verified_at = COALESCE(EXCLUDED.verified_at, agent_skill_scores.verified_at),
    updated_at = NOW()
RETURNING agent_skill_scores.agent_id, agent_skill_scores.skill_id,
          agent_skill_scores.status, agent_skill_scores.average_score,
          agent_skill_scores.pass_count, agent_skill_scores.total_count,
          agent_skill_scores.last_batch_id, agent_skill_scores.verified_at,
          agent_skill_scores.updated_at`

type UpsertAgentSkillScoreParams struct {
	AgentID      uuid.UUID  `db:"agent_id" json:"agent_id"`
	SkillID      string     `db:"skill_id" json:"skill_id"`
	Status       string     `db:"status" json:"status"`
	AverageScore *int32     `db:"average_score" json:"average_score"`
	PassCount    int32      `db:"pass_count" json:"pass_count"`
	TotalCount   int32      `db:"total_count" json:"total_count"`
	LastBatchID  *uuid.UUID `db:"last_batch_id" json:"last_batch_id"`
	VerifiedAt   *time.Time `db:"verified_at" json:"verified_at"`
}

func (q *Queries) UpsertAgentSkillScore(ctx context.Context, arg UpsertAgentSkillScoreParams) (AgentSkillScore, error) {
	row := q.db.QueryRow(ctx, upsertAgentSkillScore,
		arg.AgentID, arg.SkillID, arg.Status, arg.AverageScore,
		arg.PassCount, arg.TotalCount, arg.LastBatchID, arg.VerifiedAt,
	)
	var s AgentSkillScore
	err := scanAgentSkillScore(row, &s)
	return s, err
}

const getAgentSkillScore = `-- name: GetAgentSkillScore :one
SELECT agent_id, skill_id, status, average_score,
       pass_count, total_count, last_batch_id, verified_at, updated_at
FROM agent_skill_scores
WHERE agent_id = $1 AND skill_id = $2`

type GetAgentSkillScoreParams struct {
	AgentID uuid.UUID `db:"agent_id" json:"agent_id"`
	SkillID string    `db:"skill_id" json:"skill_id"`
}

func (q *Queries) GetAgentSkillScore(ctx context.Context, arg GetAgentSkillScoreParams) (AgentSkillScore, error) {
	row := q.db.QueryRow(ctx, getAgentSkillScore, arg.AgentID, arg.SkillID)
	var s AgentSkillScore
	err := scanAgentSkillScore(row, &s)
	return s, err
}

const listAgentSkillScoresByAgent = `-- name: ListAgentSkillScoresByAgent :many
SELECT s.agent_id, s.skill_id, s.status, s.average_score,
       s.pass_count, s.total_count, s.last_batch_id, s.verified_at, s.updated_at
FROM agent_skill_scores s
WHERE s.agent_id = $1
ORDER BY s.status DESC, s.average_score DESC NULLS LAST, s.skill_id`

func (q *Queries) ListAgentSkillScoresByAgent(ctx context.Context, agentID uuid.UUID) ([]AgentSkillScore, error) {
	rows, err := q.db.Query(ctx, listAgentSkillScoresByAgent, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentSkillScore
	for rows.Next() {
		var s AgentSkillScore
		if err := scanAgentSkillScore(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const listAgentSkillScoresBySlug = `-- name: ListAgentSkillScoresBySlug :many
SELECT s.agent_id, s.skill_id, s.status, s.average_score,
       s.pass_count, s.total_count, s.last_batch_id, s.verified_at, s.updated_at
FROM agent_skill_scores s
JOIN agents a ON a.id = s.agent_id
WHERE a.slug = $1 AND a.visibility IN ('public', 'unlisted') AND a.lifecycle_status = 'active'
ORDER BY s.status DESC, s.average_score DESC NULLS LAST, s.skill_id`

func (q *Queries) ListAgentSkillScoresBySlug(ctx context.Context, slug string) ([]AgentSkillScore, error) {
	rows, err := q.db.Query(ctx, listAgentSkillScoresBySlug, slug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentSkillScore
	for rows.Next() {
		var s AgentSkillScore
		if err := scanAgentSkillScore(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

const listTopAgentsBySkill = `-- name: ListTopAgentsBySkill :many
SELECT s.agent_id, s.average_score, s.verified_at,
       a.slug, a.name, a.description, a.tags, a.price_per_call_cents, a.total_calls
FROM agent_skill_scores s
JOIN agents a ON a.id = s.agent_id
WHERE s.skill_id = $1
  AND s.status = 'verified'
  AND a.visibility = 'public' AND a.lifecycle_status = 'active'
ORDER BY s.average_score DESC NULLS LAST, a.total_calls DESC, a.id
LIMIT $2`

// ListTopAgentsBySkillRow 行类型：verified 评分 + agent 公开字段。
type ListTopAgentsBySkillRow struct {
	AgentID           uuid.UUID  `db:"agent_id" json:"agent_id"`
	AverageScore      *int32     `db:"average_score" json:"average_score"`
	VerifiedAt        *time.Time `db:"verified_at" json:"verified_at"`
	Slug              string     `db:"slug" json:"slug"`
	Name              string     `db:"name" json:"name"`
	Description       string     `db:"description" json:"description"`
	Tags              []string   `db:"tags" json:"tags"`
	PricePerCallCents int32      `db:"price_per_call_cents" json:"price_per_call_cents"`
	TotalCalls        int32      `db:"total_calls" json:"total_calls"`
}

type ListTopAgentsBySkillParams struct {
	SkillID string `db:"skill_id" json:"skill_id"`
	Limit   int32  `db:"limit" json:"limit"`
}

func (q *Queries) ListTopAgentsBySkill(ctx context.Context, arg ListTopAgentsBySkillParams) ([]ListTopAgentsBySkillRow, error) {
	rows, err := q.db.Query(ctx, listTopAgentsBySkill, arg.SkillID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListTopAgentsBySkillRow
	for rows.Next() {
		var r ListTopAgentsBySkillRow
		if err := rows.Scan(
			&r.AgentID, &r.AverageScore, &r.VerifiedAt,
			&r.Slug, &r.Name, &r.Description, &r.Tags,
			&r.PricePerCallCents, &r.TotalCalls,
		); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	return items, rows.Err()
}

const listAgentsBySkillsWithVerified = `-- name: ListAgentsBySkillsWithVerified :many
-- RecommendAgentsBySkills 加 verified/availability 加权。Agent Node 必须有
-- PostgreSQL 证明的 current-contract ready Session；Agent Token 的使用时间
-- 不是在线性证据。Direct/MCP Agent 仍需 healthy 或成功运行证据。
-- 排序：命中 skill 数 desc → 可用性 → 最近 Session/成功证据 → verified 数 desc → total_calls desc。
-- 同时返回 verified_count 让上层决定排序权重。
SELECT a.id AS agent_id,
       COUNT(DISTINCT ag.skill_id)::int AS match_count,
       COUNT(DISTINCT s.skill_id) FILTER (WHERE s.status = 'verified')::int AS verified_count,
       a.total_calls
FROM agent_skills ag
JOIN agents a ON a.id = ag.agent_id
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
LEFT JOIN LATERAL (
    SELECT MAX(session.heartbeat_at) AS last_runtime_session_at
    FROM runtime_sessions session
    JOIN runtime_nodes node ON node.node_id = session.node_id
    JOIN agent_tokens credential
      ON credential.id = session.credential_id
     AND credential.agent_id = session.agent_id
    JOIN runtime_schema_contracts contract
      ON contract.runtime_contract_id = session.runtime_contract_id
     AND contract.runtime_contract_digest = session.runtime_contract_digest
     AND contract.is_current
    WHERE session.agent_id = a.id
      AND session.status = 'active'
      AND session.attached_core_instance_id IS NOT NULL
      AND session.disconnected_at IS NULL
      AND session.heartbeat_at >= clock_timestamp() - INTERVAL '45 seconds'
      AND session.protocol_version = 2
      AND session.runtime_contract_id = 'openlinker.runtime.v2'
      AND session.runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53'
      AND session.features @> ARRAY[
          'lease_fence', 'assignment_confirm', 'renew', 'resume',
          'event_ack', 'result_ack', 'cancel', 'persistent_spool'
      ]::text[]
      AND node.status = 'active'
      AND node.revoked_at IS NULL
      AND node.protocol_version = session.protocol_version
      AND node.runtime_contract_id = session.runtime_contract_id
      AND node.runtime_contract_digest = session.runtime_contract_digest
      AND node.device_certificate_serial = session.device_certificate_serial
      AND node.node_version = session.node_version
      AND node.features @> session.features
      AND session.features @> node.features
      AND node.last_seen_at IS NOT NULL
      AND node.last_seen_at >= clock_timestamp() - INTERVAL '45 seconds'
      AND credential.status = 'active_runtime'
      AND credential.revoked_at IS NULL
      AND credential.scopes @> ARRAY['agent:pull']::text[]
      AND (credential.expires_at IS NULL OR credential.expires_at > clock_timestamp())
      AND EXISTS (
          SELECT 1
          FROM runtime_session_attachments attachment
          WHERE attachment.runtime_session_id = session.runtime_session_id
            AND attachment.core_instance_id = session.attached_core_instance_id
            AND attachment.detached_at IS NULL
      )
) runtime_ready ON TRUE
LEFT JOIN agent_skill_scores s
       ON s.agent_id = ag.agent_id
      AND s.skill_id = ag.skill_id
      AND s.skill_id = ANY($1::text[])
WHERE ag.skill_id = ANY($1::text[])
  AND a.visibility = 'public' AND a.lifecycle_status = 'active'
  AND NOT EXISTS (
      SELECT 1
      FROM unnest(a.tags) AS tag
      WHERE lower(tag) IN ('internal', 'test', 'testing', 'validation')
         OR tag IN ('内部', '测试', '验收')
  )
  AND COALESCE(av.availability_status, 'unknown') <> 'unreachable'
  AND (
      (
          a.connection_mode = 'agent_node'
          AND runtime_ready.last_runtime_session_at IS NOT NULL
      )
      OR (
          a.connection_mode <> 'agent_node'
          AND (
              COALESCE(av.availability_status, 'unknown') = 'healthy'
              OR av.last_successful_run_at IS NOT NULL
          )
      )
  )
GROUP BY a.id, a.total_calls, av.availability_status, av.last_successful_run_at, runtime_ready.last_runtime_session_at
ORDER BY match_count DESC,
    CASE COALESCE(av.availability_status, 'unknown')
    WHEN 'healthy' THEN 0
    WHEN 'unknown' THEN 1
    WHEN 'degraded' THEN 2
    ELSE 3
END ASC,
    GREATEST(
        COALESCE(av.last_successful_run_at, TIMESTAMPTZ 'epoch'),
        COALESCE(runtime_ready.last_runtime_session_at, TIMESTAMPTZ 'epoch')
    ) DESC,
    verified_count DESC, a.total_calls DESC, a.id`

// AgentSkillMatchVerified 推荐结果行（带 verified 加权）。
type AgentSkillMatchVerified struct {
	AgentID       uuid.UUID `db:"agent_id" json:"agent_id"`
	MatchCount    int32     `db:"match_count" json:"match_count"`
	VerifiedCount int32     `db:"verified_count" json:"verified_count"`
	TotalCalls    int32     `db:"total_calls" json:"total_calls"`
}

func (q *Queries) ListAgentsBySkillsWithVerified(ctx context.Context, skillIDs []string) ([]AgentSkillMatchVerified, error) {
	rows, err := q.db.Query(ctx, listAgentsBySkillsWithVerified, skillIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentSkillMatchVerified
	for rows.Next() {
		var m AgentSkillMatchVerified
		if err := rows.Scan(&m.AgentID, &m.MatchCount, &m.VerifiedCount, &m.TotalCalls); err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	return items, rows.Err()
}

const listBenchmarkBatchSummariesByAgent = `-- name: ListBenchmarkBatchSummariesByAgent :many
SELECT b.batch_id, b.skill_id,
       MIN(b.started_at) AS started_at,
       MAX(b.finished_at) AS finished_at,
       COUNT(*)::int AS total_count,
       COUNT(*) FILTER (WHERE b.status = 'success')::int AS success_count,
       (AVG(b.score) FILTER (WHERE b.status = 'success'))::int AS average_score
FROM agent_skill_benchmark_runs b
WHERE b.agent_id = $1
GROUP BY b.batch_id, b.skill_id
ORDER BY MIN(b.started_at) DESC
LIMIT $2`

type ListBenchmarkBatchSummariesByAgentParams struct {
	AgentID uuid.UUID `db:"agent_id" json:"agent_id"`
	Limit   int32     `db:"limit" json:"limit"`
}

type ListBenchmarkBatchSummariesByAgentRow struct {
	BatchID      uuid.UUID  `db:"batch_id" json:"batch_id"`
	SkillID      string     `db:"skill_id" json:"skill_id"`
	StartedAt    time.Time  `db:"started_at" json:"started_at"`
	FinishedAt   *time.Time `db:"finished_at" json:"finished_at"`
	TotalCount   int32      `db:"total_count" json:"total_count"`
	SuccessCount int32      `db:"success_count" json:"success_count"`
	AverageScore *int32     `db:"average_score" json:"average_score"`
}

func (q *Queries) ListBenchmarkBatchSummariesByAgent(ctx context.Context, arg ListBenchmarkBatchSummariesByAgentParams) ([]ListBenchmarkBatchSummariesByAgentRow, error) {
	rows, err := q.db.Query(ctx, listBenchmarkBatchSummariesByAgent, arg.AgentID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListBenchmarkBatchSummariesByAgentRow
	for rows.Next() {
		var r ListBenchmarkBatchSummariesByAgentRow
		if err := rows.Scan(&r.BatchID, &r.SkillID, &r.StartedAt, &r.FinishedAt,
			&r.TotalCount, &r.SuccessCount, &r.AverageScore); err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	return items, rows.Err()
}

const getAgentVerifiedSkillStats = `-- name: GetAgentVerifiedSkillStats :one
SELECT
    COUNT(*) FILTER (WHERE s.status = 'verified')::int AS verified_count,
    (SELECT latest.last_batch_id FROM agent_skill_scores latest
     WHERE latest.agent_id = $1 AND latest.last_batch_id IS NOT NULL
     ORDER BY latest.updated_at DESC LIMIT 1) AS latest_batch_id
FROM agent_skill_scores s
WHERE s.agent_id = $1`

type GetAgentVerifiedSkillStatsRow struct {
	VerifiedCount int32      `db:"verified_count" json:"verified_count"`
	LatestBatchID *uuid.UUID `db:"latest_batch_id" json:"latest_batch_id"`
}

func (q *Queries) GetAgentVerifiedSkillStats(ctx context.Context, agentID uuid.UUID) (GetAgentVerifiedSkillStatsRow, error) {
	row := q.db.QueryRow(ctx, getAgentVerifiedSkillStats, agentID)
	var r GetAgentVerifiedSkillStatsRow
	err := row.Scan(&r.VerifiedCount, &r.LatestBatchID)
	return r, err
}
