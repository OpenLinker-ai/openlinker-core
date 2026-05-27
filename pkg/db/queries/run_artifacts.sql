-- run_artifacts.sql
-- A2A Artifact / Output Delivery 基础模型：成功 run 的结构化产物。

-- name: CreateRunArtifact :one
INSERT INTO run_artifacts (
    run_id, artifact_type, title, content, visibility, source_artifact_id
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING id, run_id, artifact_type, title, content, visibility, source_artifact_id, created_at;

-- name: ListRunArtifactsByRun :many
SELECT id, run_id, artifact_type, title, content, visibility, source_artifact_id, created_at
FROM run_artifacts
WHERE run_id = $1
ORDER BY created_at ASC, id ASC;

-- name: GetRunArtifactBySourceID :one
SELECT id, run_id, artifact_type, title, content, visibility, source_artifact_id, created_at
FROM run_artifacts
WHERE run_id = $1
  AND source_artifact_id = $2;

-- name: UpdateRunArtifactContent :one
UPDATE run_artifacts
SET artifact_type = $3,
    title = $4,
    content = $5,
    visibility = $6
WHERE id = $1
  AND run_id = $2
RETURNING id, run_id, artifact_type, title, content, visibility, source_artifact_id, created_at;

-- name: CreateRunArtifactChunk :one
WITH next_chunk AS (
    SELECT COALESCE(MAX(chunk_index) + 1, 0)::INTEGER AS chunk_index
    FROM run_artifact_chunks
    WHERE run_id = $1
      AND source_artifact_id = $3
)
INSERT INTO run_artifact_chunks (
    run_id, run_artifact_id, source_artifact_id, event_sequence, chunk_index,
    append, last_chunk, parts, payload
)
SELECT
    $1, $2, $3, $4, next_chunk.chunk_index,
    $5, $6, $7, $8
FROM next_chunk
RETURNING id, run_id, run_artifact_id, source_artifact_id, event_sequence, chunk_index,
          append, last_chunk, parts, payload, created_at;

-- name: ListRunArtifactChunksByRun :many
SELECT id, run_id, run_artifact_id, source_artifact_id, event_sequence, chunk_index,
       append, last_chunk, parts, payload, created_at
FROM run_artifact_chunks
WHERE run_id = $1
ORDER BY source_artifact_id ASC, chunk_index ASC, id ASC;
