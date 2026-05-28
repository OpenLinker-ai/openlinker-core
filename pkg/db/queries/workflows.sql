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
          started_at, finished_at, created_at, updated_at,
          attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error;

-- name: CreatePendingWorkflowRun :one
INSERT INTO workflow_runs (workflow_id, user_id, status, input, max_attempts)
VALUES ($1, $2, 'pending', $3, $4)
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at,
          attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error;

-- name: MarkWorkflowRunSuccess :one
UPDATE workflow_runs
SET status = 'success',
    output = $2,
    error_message = NULL,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
  AND status = 'running'
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at,
          attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error;

-- name: MarkWorkflowRunFailed :one
UPDATE workflow_runs
SET status = 'failed',
    error_message = $2,
    finished_at = NOW(),
    updated_at = NOW()
WHERE id = $1
  AND status = 'running'
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at,
          attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error;

-- name: PauseWorkflowRun :one
UPDATE workflow_runs
SET status = 'paused',
    claimed_at = NULL,
    next_retry_at = NULL,
    last_worker_error = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status IN ('pending', 'running')
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at,
          attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error;

-- name: ResumeWorkflowRun :one
UPDATE workflow_runs
SET status = 'pending',
    claimed_at = NULL,
    next_retry_at = NOW(),
    finished_at = NULL,
    error_message = NULL,
    last_worker_error = NULL,
    updated_at = NOW()
WHERE id = $1
  AND status = 'paused'
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at,
          attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error;

-- name: CancelWorkflowRun :one
UPDATE workflow_runs
SET status = 'canceled',
    claimed_at = NULL,
    next_retry_at = NULL,
    error_message = COALESCE(error_message, 'workflow run canceled by user'),
    finished_at = COALESCE(finished_at, NOW()),
    updated_at = NOW()
WHERE id = $1
  AND status IN ('pending', 'running', 'paused')
RETURNING id, workflow_id, user_id, status, input, output, error_message,
          started_at, finished_at, created_at, updated_at,
          attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error;

-- name: GetWorkflowRunByID :one
SELECT id, workflow_id, user_id, status, input, output, error_message,
       started_at, finished_at, created_at, updated_at,
       attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error
FROM workflow_runs
WHERE id = $1;

-- name: ListWorkflowRunsByWorkflow :many
SELECT id, workflow_id, user_id, status, input, output, error_message,
       started_at, finished_at, created_at, updated_at,
       attempt_count, max_attempts, next_retry_at, claimed_at, last_worker_error
FROM workflow_runs
WHERE workflow_id = $1
ORDER BY created_at DESC
LIMIT $2;

-- name: CountWorkflowRunsByWorkflow :one
SELECT COUNT(*)::int
FROM workflow_runs
WHERE workflow_id = $1;

-- name: ClaimPendingWorkflowRun :one
WITH candidate AS (
    SELECT id
    FROM workflow_runs
    WHERE status = 'pending'
      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
    ORDER BY created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE workflow_runs r
SET status = 'running',
    claimed_at = NOW(),
    next_retry_at = NULL,
    attempt_count = r.attempt_count + 1,
    last_worker_error = NULL,
    updated_at = NOW()
FROM candidate
WHERE r.id = candidate.id
RETURNING r.id, r.workflow_id, r.user_id, r.status, r.input, r.output, r.error_message,
          r.started_at, r.finished_at, r.created_at, r.updated_at,
          r.attempt_count, r.max_attempts, r.next_retry_at, r.claimed_at, r.last_worker_error;

-- name: RequeueStaleWorkflowRuns :execrows
UPDATE workflow_runs
SET status = CASE
        WHEN attempt_count < max_attempts THEN 'pending'
        ELSE 'failed'
    END,
    next_retry_at = CASE
        WHEN attempt_count < max_attempts THEN NOW()
        ELSE NULL
    END,
    claimed_at = NULL,
    finished_at = CASE
        WHEN attempt_count < max_attempts THEN finished_at
        ELSE NOW()
    END,
    error_message = CASE
        WHEN attempt_count < max_attempts THEN error_message
        ELSE COALESCE(error_message, 'workflow run timed out before worker completed')
    END,
    last_worker_error = 'workflow worker stale claim timed out',
    updated_at = NOW()
WHERE status = 'running'
  AND claimed_at IS NOT NULL
  AND claimed_at < $1;

-- name: DeleteWorkflowRunSteps :exec
DELETE FROM workflow_run_steps
WHERE workflow_run_id = $1;

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
