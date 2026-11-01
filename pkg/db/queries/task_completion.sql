-- name: CompletePrivateTaskFromSuccessfulRun :one
-- A successful task-owned Run becomes the private Task's latest result in the
-- same transaction that finalizes the Run. Requirement evidence is the
-- durable Run -> Task association created at Run submission.
UPDATE task_queries task
SET completed_at = run.finished_at,
    completion_run_id = run.id,
    completion_summary = $2
FROM runs run
JOIN run_requirement_evidence evidence ON evidence.run_id = run.id
WHERE run.id = $1
  AND run.status = 'success'
  AND task.id = evidence.task_id
  AND task.user_id = run.user_id
  AND evidence.user_id = run.user_id
  AND task.chosen_agent_id = run.agent_id
  AND evidence.agent_id = run.agent_id
RETURNING task.id;

-- name: ListTaskCompletionSources :many
-- The stored summary can cut a secret in half, so reads re-derive it from the
-- Run output. The Agent may be gone; such reads count as non-creator reads.
SELECT run.id AS run_id, run.output, agent.creator_id, agent.endpoint_auth_header
FROM runs run
LEFT JOIN agents agent ON agent.id = run.agent_id
WHERE run.id = ANY($1::uuid[]);
