-- workflows.sql
-- Minimal production workflow execution engine.

-- name: CreateWorkflow :one
INSERT INTO workflows (user_id, name, description, edges)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, name, description, status, edges, created_at, updated_at;

-- name: CreateWorkflowNode :one
INSERT INTO workflow_nodes (
    workflow_id, node_key, node_type, agent_id, title, config, position
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING id, workflow_id, node_key, node_type, agent_id, title, config, position, created_at;

-- name: GetWorkflowByID :one
SELECT id, user_id, name, description, status, edges, created_at, updated_at
FROM workflows
WHERE id = $1;

-- name: ListWorkflowNodes :many
SELECT id, workflow_id, node_key, node_type, agent_id, title, config, position, created_at
FROM workflow_nodes
WHERE workflow_id = $1
ORDER BY position ASC, created_at ASC;

-- name: ListWorkflowsByUser :many
SELECT id, user_id, name, description, status, edges, created_at, updated_at
FROM workflows
WHERE user_id = $1
ORDER BY updated_at DESC, created_at DESC
LIMIT $2;

-- name: CountWorkflowsByUser :one
SELECT COUNT(*)::int
FROM workflows
WHERE user_id = $1;

-- name: CreateWorkflowRun :one
INSERT INTO workflow_runs (workflow_id, user_id, status, input)
VALUES ($1, $2, 'running', $3)
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at;

-- name: MarkWorkflowRunSuccess :one
UPDATE workflow_runs
SET status = 'success',
    output = $2,
    error_message = NULL,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at;

-- name: MarkWorkflowRunFailed :one
UPDATE workflow_runs
SET status = 'failed',
    error_message = $2,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at;

-- name: GetWorkflowRunByID :one
SELECT id, workflow_id, user_id, status, input, output, error_message,
       started_at, finished_at, created_at, updated_at
FROM workflow_runs
WHERE id = $1;

-- name: CreateWorkflowRunStep :one
INSERT INTO workflow_run_steps (
    workflow_run_id, workflow_node_id, node_key, agent_id, status, input, sequence
) VALUES (
    $1, $2, $3, $4, 'running', $5, $6
)
RETURNING id, workflow_run_id, workflow_node_id, node_key, agent_id, run_id,
          status, input, output, error_message, sequence, started_at, finished_at,
          created_at, updated_at;

-- name: MarkWorkflowRunStepSuccess :one
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
          created_at, updated_at;

-- name: MarkWorkflowRunStepFailed :one
UPDATE workflow_run_steps
SET status = 'failed',
    run_id = $2,
    error_message = $3,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
RETURNING id, workflow_run_id, workflow_node_id, node_key, agent_id, run_id,
          status, input, output, error_message, sequence, started_at, finished_at,
          created_at, updated_at;

-- name: ListWorkflowRunSteps :many
SELECT id, workflow_run_id, workflow_node_id, node_key, agent_id, run_id,
       status, input, output, error_message, sequence, started_at, finished_at,
       created_at, updated_at
FROM workflow_run_steps
WHERE workflow_run_id = $1
ORDER BY sequence ASC;
