-- run_messages.sql
-- Stable message replay model for A2A Message / Part mapping.

-- name: CreateRunMessage :one
INSERT INTO run_messages (
    run_id, event_sequence, role, content, payload
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING id, run_id, event_sequence, role, content, payload, created_at;

-- name: ListRunMessagesByRun :many
SELECT id, run_id, event_sequence, role, content, payload, created_at
FROM run_messages
WHERE run_id = $1
ORDER BY created_at ASC, id ASC;
