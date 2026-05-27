// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/workflows.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanWorkflow(row interface {
	Scan(dest ...any) error
}, w *Workflow) error {
	return row.Scan(&w.ID, &w.UserID, &w.Name, &w.Description, &w.Status, &w.Edges, &w.CreatedAt, &w.UpdatedAt)
}

const createWorkflow = `-- name: CreateWorkflow :one
INSERT INTO workflows (user_id, name, description, edges)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, name, description, status, edges, created_at, updated_at`

type CreateWorkflowParams struct {
	UserID      uuid.UUID `db:"user_id" json:"user_id"`
	Name        string    `db:"name" json:"name"`
	Description string    `db:"description" json:"description"`
	Edges       []byte    `db:"edges" json:"edges"`
}

func (q *Queries) CreateWorkflow(ctx context.Context, arg CreateWorkflowParams) (Workflow, error) {
	row := q.db.QueryRow(ctx, createWorkflow, arg.UserID, arg.Name, arg.Description, arg.Edges)
	var w Workflow
	err := scanWorkflow(row, &w)
	return w, err
}

func scanWorkflowNode(row interface {
	Scan(dest ...any) error
}, n *WorkflowNode) error {
	return row.Scan(&n.ID, &n.WorkflowID, &n.NodeKey, &n.NodeType, &n.AgentID, &n.Title, &n.Config, &n.Position, &n.CreatedAt)
}

const createWorkflowNode = `-- name: CreateWorkflowNode :one
INSERT INTO workflow_nodes (
    workflow_id, node_key, node_type, agent_id, title, config, position
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING id, workflow_id, node_key, node_type, agent_id, title, config, position, created_at`

type CreateWorkflowNodeParams struct {
	WorkflowID uuid.UUID `db:"workflow_id" json:"workflow_id"`
	NodeKey    string    `db:"node_key" json:"node_key"`
	NodeType   string    `db:"node_type" json:"node_type"`
	AgentID    uuid.UUID `db:"agent_id" json:"agent_id"`
	Title      string    `db:"title" json:"title"`
	Config     []byte    `db:"config" json:"config"`
	Position   int32     `db:"position" json:"position"`
}

func (q *Queries) CreateWorkflowNode(ctx context.Context, arg CreateWorkflowNodeParams) (WorkflowNode, error) {
	row := q.db.QueryRow(ctx, createWorkflowNode,
		arg.WorkflowID, arg.NodeKey, arg.NodeType, arg.AgentID, arg.Title, arg.Config, arg.Position)
	var n WorkflowNode
	err := scanWorkflowNode(row, &n)
	return n, err
}

const getWorkflowByID = `-- name: GetWorkflowByID :one
SELECT id, user_id, name, description, status, edges, created_at, updated_at
FROM workflows
WHERE id = $1`

func (q *Queries) GetWorkflowByID(ctx context.Context, id uuid.UUID) (Workflow, error) {
	row := q.db.QueryRow(ctx, getWorkflowByID, id)
	var w Workflow
	err := scanWorkflow(row, &w)
	return w, err
}

const listWorkflowNodes = `-- name: ListWorkflowNodes :many
SELECT id, workflow_id, node_key, node_type, agent_id, title, config, position, created_at
FROM workflow_nodes
WHERE workflow_id = $1
ORDER BY position ASC, created_at ASC`

func (q *Queries) ListWorkflowNodes(ctx context.Context, workflowID uuid.UUID) ([]WorkflowNode, error) {
	rows, err := q.db.Query(ctx, listWorkflowNodes, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []WorkflowNode
	for rows.Next() {
		var n WorkflowNode
		if err := scanWorkflowNode(rows, &n); err != nil {
			return nil, err
		}
		items = append(items, n)
	}
	return items, rows.Err()
}

const listWorkflowsByUser = `-- name: ListWorkflowsByUser :many
SELECT id, user_id, name, description, status, edges, created_at, updated_at
FROM workflows
WHERE user_id = $1
ORDER BY updated_at DESC, created_at DESC
LIMIT $2`

type ListWorkflowsByUserParams struct {
	UserID uuid.UUID `db:"user_id" json:"user_id"`
	Limit  int32     `db:"limit" json:"limit"`
}

func (q *Queries) ListWorkflowsByUser(ctx context.Context, arg ListWorkflowsByUserParams) ([]Workflow, error) {
	rows, err := q.db.Query(ctx, listWorkflowsByUser, arg.UserID, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Workflow
	for rows.Next() {
		var w Workflow
		if err := scanWorkflow(rows, &w); err != nil {
			return nil, err
		}
		items = append(items, w)
	}
	return items, rows.Err()
}

const countWorkflowsByUser = `-- name: CountWorkflowsByUser :one
SELECT COUNT(*)::int
FROM workflows
WHERE user_id = $1`

func (q *Queries) CountWorkflowsByUser(ctx context.Context, userID uuid.UUID) (int32, error) {
	row := q.db.QueryRow(ctx, countWorkflowsByUser, userID)
	var count int32
	err := row.Scan(&count)
	return count, err
}

func scanWorkflowRun(row interface {
	Scan(dest ...any) error
}, r *WorkflowRun) error {
	return row.Scan(
		&r.ID, &r.WorkflowID, &r.UserID, &r.Status, &r.Input, &r.Output, &r.ErrorMessage,
		&r.StartedAt, &r.FinishedAt, &r.CreatedAt, &r.UpdatedAt,
	)
}

const createWorkflowRun = `-- name: CreateWorkflowRun :one
INSERT INTO workflow_runs (workflow_id, user_id, status, input)
VALUES ($1, $2, 'running', $3)
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at`

type CreateWorkflowRunParams struct {
	WorkflowID uuid.UUID `db:"workflow_id" json:"workflow_id"`
	UserID     uuid.UUID `db:"user_id" json:"user_id"`
	Input      []byte    `db:"input" json:"input"`
}

func (q *Queries) CreateWorkflowRun(ctx context.Context, arg CreateWorkflowRunParams) (WorkflowRun, error) {
	row := q.db.QueryRow(ctx, createWorkflowRun, arg.WorkflowID, arg.UserID, arg.Input)
	var r WorkflowRun
	err := scanWorkflowRun(row, &r)
	return r, err
}

const markWorkflowRunSuccess = `-- name: MarkWorkflowRunSuccess :one
UPDATE workflow_runs
SET status = 'success',
    output = $2,
    error_message = NULL,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at`

type MarkWorkflowRunSuccessParams struct {
	ID     uuid.UUID `db:"id" json:"id"`
	Output []byte    `db:"output" json:"output"`
}

func (q *Queries) MarkWorkflowRunSuccess(ctx context.Context, arg MarkWorkflowRunSuccessParams) (WorkflowRun, error) {
	row := q.db.QueryRow(ctx, markWorkflowRunSuccess, arg.ID, arg.Output)
	var r WorkflowRun
	err := scanWorkflowRun(row, &r)
	return r, err
}

const markWorkflowRunFailed = `-- name: MarkWorkflowRunFailed :one
UPDATE workflow_runs
SET status = 'failed',
    error_message = $2,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at`

type MarkWorkflowRunFailedParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	ErrorMessage *string   `db:"error_message" json:"error_message"`
}

func (q *Queries) MarkWorkflowRunFailed(ctx context.Context, arg MarkWorkflowRunFailedParams) (WorkflowRun, error) {
	row := q.db.QueryRow(ctx, markWorkflowRunFailed, arg.ID, arg.ErrorMessage)
	var r WorkflowRun
	err := scanWorkflowRun(row, &r)
	return r, err
}

const getWorkflowRunByID = `-- name: GetWorkflowRunByID :one
SELECT id, workflow_id, user_id, status, input, output, error_message,
       started_at, finished_at, created_at, updated_at
FROM workflow_runs
WHERE id = $1`

func (q *Queries) GetWorkflowRunByID(ctx context.Context, id uuid.UUID) (WorkflowRun, error) {
	row := q.db.QueryRow(ctx, getWorkflowRunByID, id)
	var r WorkflowRun
	err := scanWorkflowRun(row, &r)
	return r, err
}

func scanWorkflowRunStep(row interface {
	Scan(dest ...any) error
}, s *WorkflowRunStep) error {
	return row.Scan(
		&s.ID, &s.WorkflowRunID, &s.WorkflowNodeID, &s.NodeKey, &s.AgentID, &s.RunID,
		&s.Status, &s.Input, &s.Output, &s.ErrorMessage, &s.Sequence, &s.StartedAt, &s.FinishedAt,
		&s.CreatedAt, &s.UpdatedAt,
	)
}

const createWorkflowRunStep = `-- name: CreateWorkflowRunStep :one
INSERT INTO workflow_run_steps (
    workflow_run_id, workflow_node_id, node_key, agent_id, status, input, sequence
) VALUES (
    $1, $2, $3, $4, 'running', $5, $6
)
RETURNING id, workflow_run_id, workflow_node_id, node_key, agent_id, run_id,
          status, input, output, error_message, sequence, started_at, finished_at,
          created_at, updated_at`

type CreateWorkflowRunStepParams struct {
	WorkflowRunID  uuid.UUID `db:"workflow_run_id" json:"workflow_run_id"`
	WorkflowNodeID uuid.UUID `db:"workflow_node_id" json:"workflow_node_id"`
	NodeKey        string    `db:"node_key" json:"node_key"`
	AgentID        uuid.UUID `db:"agent_id" json:"agent_id"`
	Input          []byte    `db:"input" json:"input"`
	Sequence       int32     `db:"sequence" json:"sequence"`
}

func (q *Queries) CreateWorkflowRunStep(ctx context.Context, arg CreateWorkflowRunStepParams) (WorkflowRunStep, error) {
	row := q.db.QueryRow(ctx, createWorkflowRunStep,
		arg.WorkflowRunID, arg.WorkflowNodeID, arg.NodeKey, arg.AgentID, arg.Input, arg.Sequence)
	var s WorkflowRunStep
	err := scanWorkflowRunStep(row, &s)
	return s, err
}

const markWorkflowRunStepSuccess = `-- name: MarkWorkflowRunStepSuccess :one
UPDATE workflow_run_steps
SET status = 'success',
    run_id = $2,
    output = $3,
    error_message = NULL,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
RETURNING id, workflow_run_id, workflow_node_id, node_key, agent_id, run_id,
          status, input, output, error_message, sequence, started_at, finished_at,
          created_at, updated_at`

type MarkWorkflowRunStepSuccessParams struct {
	ID     uuid.UUID  `db:"id" json:"id"`
	RunID  *uuid.UUID `db:"run_id" json:"run_id"`
	Output []byte     `db:"output" json:"output"`
}

func (q *Queries) MarkWorkflowRunStepSuccess(ctx context.Context, arg MarkWorkflowRunStepSuccessParams) (WorkflowRunStep, error) {
	row := q.db.QueryRow(ctx, markWorkflowRunStepSuccess, arg.ID, arg.RunID, arg.Output)
	var s WorkflowRunStep
	err := scanWorkflowRunStep(row, &s)
	return s, err
}

const markWorkflowRunStepFailed = `-- name: MarkWorkflowRunStepFailed :one
UPDATE workflow_run_steps
SET status = 'failed',
    run_id = $2,
    error_message = $3,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
RETURNING id, workflow_run_id, workflow_node_id, node_key, agent_id, run_id,
          status, input, output, error_message, sequence, started_at, finished_at,
          created_at, updated_at`

type MarkWorkflowRunStepFailedParams struct {
	ID           uuid.UUID  `db:"id" json:"id"`
	RunID        *uuid.UUID `db:"run_id" json:"run_id"`
	ErrorMessage *string    `db:"error_message" json:"error_message"`
}

func (q *Queries) MarkWorkflowRunStepFailed(ctx context.Context, arg MarkWorkflowRunStepFailedParams) (WorkflowRunStep, error) {
	row := q.db.QueryRow(ctx, markWorkflowRunStepFailed, arg.ID, arg.RunID, arg.ErrorMessage)
	var s WorkflowRunStep
	err := scanWorkflowRunStep(row, &s)
	return s, err
}

const listWorkflowRunSteps = `-- name: ListWorkflowRunSteps :many
SELECT id, workflow_run_id, workflow_node_id, node_key, agent_id, run_id,
       status, input, output, error_message, sequence, started_at, finished_at,
       created_at, updated_at
FROM workflow_run_steps
WHERE workflow_run_id = $1
ORDER BY sequence ASC`

func (q *Queries) ListWorkflowRunSteps(ctx context.Context, workflowRunID uuid.UUID) ([]WorkflowRunStep, error) {
	rows, err := q.db.Query(ctx, listWorkflowRunSteps, workflowRunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []WorkflowRunStep
	for rows.Next() {
		var s WorkflowRunStep
		if err := scanWorkflowRunStep(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	return items, rows.Err()
}
