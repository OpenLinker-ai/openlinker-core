-- Reliable runtime v2 dead-letter inventory.

-- name: CreateRunDeadLetter :one
INSERT INTO run_dead_letters (
    run_id, final_attempt_no, reason_code, reason_redacted
) VALUES ($1, $2, $3, $4)
ON CONFLICT (run_id) DO NOTHING
RETURNING *;

-- name: GetRunDeadLetterByRun :one
SELECT * FROM run_dead_letters WHERE run_id = $1;

-- name: ListRunDeadLetters :many
SELECT *
FROM run_dead_letters
ORDER BY created_at DESC, id DESC
LIMIT $1 OFFSET $2;

-- name: CountRunDeadLetters :one
SELECT COUNT(*)::int FROM run_dead_letters;
