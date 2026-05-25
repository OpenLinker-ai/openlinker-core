// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/capabilities.sql）。
//
// 模块 A（Phase 2 §4）：Agent capabilities + examples + onboarding status。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanAgentCapability(row interface {
	Scan(dest ...any) error
}, c *AgentCapability) error {
	return row.Scan(
		&c.ID,
		&c.AgentID,
		&c.InputSchema,
		&c.OutputSchema,
		&c.Summary,
		&c.Version,
		&c.PublishedAt,
		&c.UpdatedAt,
	)
}

const upsertAgentCapability = `-- name: UpsertAgentCapability :one
INSERT INTO agent_capabilities (
    agent_id, input_schema, output_schema, summary, version, published_at, updated_at
)
SELECT a.id, $3, $4, $5, 1, NOW(), NOW()
FROM agents a
WHERE a.id = $1 AND a.creator_id = $2
ON CONFLICT (agent_id) DO UPDATE
SET input_schema = EXCLUDED.input_schema,
    output_schema = EXCLUDED.output_schema,
    summary = EXCLUDED.summary,
    version = agent_capabilities.version + 1,
    published_at = NOW(),
    updated_at = NOW()
RETURNING id, agent_id, input_schema, output_schema, summary,
          version, published_at, updated_at`

type UpsertAgentCapabilityParams struct {
	AgentID      uuid.UUID `db:"agent_id" json:"agent_id"`
	CreatorID    uuid.UUID `db:"creator_id" json:"creator_id"`
	InputSchema  []byte    `db:"input_schema" json:"input_schema"`
	OutputSchema []byte    `db:"output_schema" json:"output_schema"`
	Summary      string    `db:"summary" json:"summary"`
}

func (q *Queries) UpsertAgentCapability(ctx context.Context, arg UpsertAgentCapabilityParams) (AgentCapability, error) {
	row := q.db.QueryRow(ctx, upsertAgentCapability,
		arg.AgentID,
		arg.CreatorID,
		arg.InputSchema,
		arg.OutputSchema,
		arg.Summary,
	)
	var c AgentCapability
	err := scanAgentCapability(row, &c)
	return c, err
}

const getAgentCapabilityByAgentID = `-- name: GetAgentCapabilityByAgentID :one
SELECT id, agent_id, input_schema, output_schema, summary,
       version, published_at, updated_at
FROM agent_capabilities
WHERE agent_id = $1`

func (q *Queries) GetAgentCapabilityByAgentID(ctx context.Context, agentID uuid.UUID) (AgentCapability, error) {
	row := q.db.QueryRow(ctx, getAgentCapabilityByAgentID, agentID)
	var c AgentCapability
	err := scanAgentCapability(row, &c)
	return c, err
}

const getAgentCapabilityBySlug = `-- name: GetAgentCapabilityBySlug :one
SELECT c.id, c.agent_id, c.input_schema, c.output_schema, c.summary,
       c.version, c.published_at, c.updated_at
FROM agent_capabilities c
JOIN agents a ON a.id = c.agent_id
WHERE a.slug = $1 AND a.visibility IN ('public', 'unlisted') AND a.lifecycle_status = 'active'`

func (q *Queries) GetAgentCapabilityBySlug(ctx context.Context, slug string) (AgentCapability, error) {
	row := q.db.QueryRow(ctx, getAgentCapabilityBySlug, slug)
	var c AgentCapability
	err := scanAgentCapability(row, &c)
	return c, err
}

func scanAgentExample(row interface {
	Scan(dest ...any) error
}, e *AgentExample) error {
	return row.Scan(
		&e.ID,
		&e.AgentID,
		&e.Title,
		&e.InputJSON,
		&e.ExpectedOutputJSON,
		&e.SortOrder,
		&e.CreatedAt,
		&e.UpdatedAt,
	)
}

const createAgentExample = `-- name: CreateAgentExample :one
INSERT INTO agent_examples (
    agent_id, title, input_json, expected_output_json, sort_order
)
SELECT a.id, $3, $4, $5, $6
FROM agents a
WHERE a.id = $1 AND a.creator_id = $2
RETURNING id, agent_id, title, input_json, expected_output_json,
          sort_order, created_at, updated_at`

type CreateAgentExampleParams struct {
	AgentID            uuid.UUID `db:"agent_id" json:"agent_id"`
	CreatorID          uuid.UUID `db:"creator_id" json:"creator_id"`
	Title              string    `db:"title" json:"title"`
	InputJSON          []byte    `db:"input_json" json:"input_json"`
	ExpectedOutputJSON []byte    `db:"expected_output_json" json:"expected_output_json"`
	SortOrder          int32     `db:"sort_order" json:"sort_order"`
}

func (q *Queries) CreateAgentExample(ctx context.Context, arg CreateAgentExampleParams) (AgentExample, error) {
	row := q.db.QueryRow(ctx, createAgentExample,
		arg.AgentID,
		arg.CreatorID,
		arg.Title,
		arg.InputJSON,
		arg.ExpectedOutputJSON,
		arg.SortOrder,
	)
	var e AgentExample
	err := scanAgentExample(row, &e)
	return e, err
}

const listAgentExamplesByAgentID = `-- name: ListAgentExamplesByAgentID :many
SELECT id, agent_id, title, input_json, expected_output_json,
       sort_order, created_at, updated_at
FROM agent_examples
WHERE agent_id = $1
ORDER BY sort_order, created_at`

func (q *Queries) ListAgentExamplesByAgentID(ctx context.Context, agentID uuid.UUID) ([]AgentExample, error) {
	rows, err := q.db.Query(ctx, listAgentExamplesByAgentID, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentExample
	for rows.Next() {
		var e AgentExample
		if err := scanAgentExample(rows, &e); err != nil {
			return nil, err
		}
		items = append(items, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listAgentExamplesBySlug = `-- name: ListAgentExamplesBySlug :many
SELECT e.id, e.agent_id, e.title, e.input_json, e.expected_output_json,
       e.sort_order, e.created_at, e.updated_at
FROM agent_examples e
JOIN agents a ON a.id = e.agent_id
WHERE a.slug = $1 AND a.visibility IN ('public', 'unlisted') AND a.lifecycle_status = 'active'
ORDER BY e.sort_order, e.created_at`

func (q *Queries) ListAgentExamplesBySlug(ctx context.Context, slug string) ([]AgentExample, error) {
	rows, err := q.db.Query(ctx, listAgentExamplesBySlug, slug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentExample
	for rows.Next() {
		var e AgentExample
		if err := scanAgentExample(rows, &e); err != nil {
			return nil, err
		}
		items = append(items, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const deleteAgentExampleForOwner = `-- name: DeleteAgentExampleForOwner :execrows
DELETE FROM agent_examples e
USING agents a
WHERE e.id = $1
  AND e.agent_id = $2
  AND e.agent_id = a.id
  AND a.creator_id = $3`

type DeleteAgentExampleForOwnerParams struct {
	ID        uuid.UUID `db:"id" json:"id"`
	AgentID   uuid.UUID `db:"agent_id" json:"agent_id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

func (q *Queries) DeleteAgentExampleForOwner(ctx context.Context, arg DeleteAgentExampleForOwnerParams) (int64, error) {
	result, err := q.db.Exec(ctx, deleteAgentExampleForOwner, arg.ID, arg.AgentID, arg.CreatorID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const countAgentExamplesByAgentID = `-- name: CountAgentExamplesByAgentID :one
SELECT COUNT(*)::int FROM agent_examples WHERE agent_id = $1`

func (q *Queries) CountAgentExamplesByAgentID(ctx context.Context, agentID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countAgentExamplesByAgentID, agentID)
	var count int32
	err := row.Scan(&count)
	return count, err
}

const getFirstExampleInputByAgentID = `-- name: GetFirstExampleInputByAgentID :one
SELECT input_json
FROM agent_examples
WHERE agent_id = $1
ORDER BY sort_order, created_at
LIMIT 1`

func (q *Queries) GetFirstExampleInputByAgentID(ctx context.Context, agentID uuid.UUID) ([]byte, error) {
	row := q.db.QueryRow(ctx, getFirstExampleInputByAgentID, agentID)
	var input []byte
	err := row.Scan(&input)
	return input, err
}

const ensureOnboardingStatus = `-- name: EnsureOnboardingStatus :exec
INSERT INTO agent_onboarding_status (agent_id, endpoint_set)
VALUES ($1, TRUE)
ON CONFLICT (agent_id) DO NOTHING`

func (q *Queries) EnsureOnboardingStatus(ctx context.Context, agentID uuid.UUID) error {
	_, err := q.db.Exec(ctx, ensureOnboardingStatus, agentID)
	return err
}

func scanAgentOnboardingStatus(row interface {
	Scan(dest ...any) error
}, s *AgentOnboardingStatus) error {
	return row.Scan(
		&s.AgentID,
		&s.EndpointSet,
		&s.CapabilitiesSet,
		&s.ExamplesSet,
		&s.DryRunPassed,
		&s.DryRunLastResult,
		&s.DryRunError,
		&s.DryRunAt,
		&s.UpdatedAt,
	)
}

const getOnboardingStatusForOwner = `-- name: GetOnboardingStatusForOwner :one
SELECT s.agent_id, s.endpoint_set, s.capabilities_set, s.examples_set,
       s.dry_run_passed, s.dry_run_last_result, s.dry_run_error, s.dry_run_at,
       s.updated_at
FROM agent_onboarding_status s
JOIN agents a ON a.id = s.agent_id
WHERE s.agent_id = $1 AND a.creator_id = $2`

type GetOnboardingStatusForOwnerParams struct {
	AgentID   uuid.UUID `db:"agent_id" json:"agent_id"`
	CreatorID uuid.UUID `db:"creator_id" json:"creator_id"`
}

func (q *Queries) GetOnboardingStatusForOwner(ctx context.Context, arg GetOnboardingStatusForOwnerParams) (AgentOnboardingStatus, error) {
	row := q.db.QueryRow(ctx, getOnboardingStatusForOwner, arg.AgentID, arg.CreatorID)
	var s AgentOnboardingStatus
	err := scanAgentOnboardingStatus(row, &s)
	return s, err
}

const markCapabilitiesSet = `-- name: MarkCapabilitiesSet :execrows
UPDATE agent_onboarding_status
SET capabilities_set = TRUE, updated_at = NOW()
WHERE agent_id = $1`

func (q *Queries) MarkCapabilitiesSet(ctx context.Context, agentID uuid.UUID) (int64, error) {
	result, err := q.db.Exec(ctx, markCapabilitiesSet, agentID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const markExamplesSet = `-- name: MarkExamplesSet :execrows
UPDATE agent_onboarding_status
SET examples_set = $2, updated_at = NOW()
WHERE agent_id = $1`

type MarkExamplesSetParams struct {
	AgentID     uuid.UUID `db:"agent_id" json:"agent_id"`
	ExamplesSet bool      `db:"examples_set" json:"examples_set"`
}

func (q *Queries) MarkExamplesSet(ctx context.Context, arg MarkExamplesSetParams) (int64, error) {
	result, err := q.db.Exec(ctx, markExamplesSet, arg.AgentID, arg.ExamplesSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const updateDryRunResult = `-- name: UpdateDryRunResult :execrows
UPDATE agent_onboarding_status
SET dry_run_passed = ($2 = 'pass'),
    dry_run_last_result = $2,
    dry_run_error = $3,
    dry_run_at = NOW(),
    updated_at = NOW()
WHERE agent_id = $1`

type UpdateDryRunResultParams struct {
	AgentID uuid.UUID `db:"agent_id" json:"agent_id"`
	Result  string    `db:"result" json:"result"`
	Error   *string   `db:"error" json:"error"`
}

func (q *Queries) UpdateDryRunResult(ctx context.Context, arg UpdateDryRunResultParams) (int64, error) {
	result, err := q.db.Exec(ctx, updateDryRunResult, arg.AgentID, arg.Result, arg.Error)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
