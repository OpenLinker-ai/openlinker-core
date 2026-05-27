// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/run_artifacts.sql）。

package db

import (
	"context"

	"github.com/google/uuid"
)

func scanRunArtifact(row interface {
	Scan(dest ...any) error
}, a *RunArtifact) error {
	return row.Scan(
		&a.ID,
		&a.RunID,
		&a.ArtifactType,
		&a.Title,
		&a.Content,
		&a.Visibility,
		&a.SourceArtifactID,
		&a.CreatedAt,
	)
}

const createRunArtifact = `-- name: CreateRunArtifact :one
INSERT INTO run_artifacts (
    run_id, artifact_type, title, content, visibility, source_artifact_id
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING id, run_id, artifact_type, title, content, visibility, source_artifact_id, created_at`

type CreateRunArtifactParams struct {
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	ArtifactType     string    `db:"artifact_type" json:"artifact_type"`
	Title            string    `db:"title" json:"title"`
	Content          []byte    `db:"content" json:"content"`
	Visibility       string    `db:"visibility" json:"visibility"`
	SourceArtifactID *string   `db:"source_artifact_id" json:"source_artifact_id"`
}

func (q *Queries) CreateRunArtifact(ctx context.Context, arg CreateRunArtifactParams) (RunArtifact, error) {
	row := q.db.QueryRow(ctx, createRunArtifact,
		arg.RunID,
		arg.ArtifactType,
		arg.Title,
		arg.Content,
		arg.Visibility,
		arg.SourceArtifactID,
	)
	var a RunArtifact
	err := scanRunArtifact(row, &a)
	return a, err
}

const listRunArtifactsByRun = `-- name: ListRunArtifactsByRun :many
SELECT id, run_id, artifact_type, title, content, visibility, source_artifact_id, created_at
FROM run_artifacts
WHERE run_id = $1
ORDER BY created_at ASC, id ASC`

func (q *Queries) ListRunArtifactsByRun(ctx context.Context, runID uuid.UUID) ([]RunArtifact, error) {
	rows, err := q.db.Query(ctx, listRunArtifactsByRun, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunArtifact
	for rows.Next() {
		var a RunArtifact
		if err := scanRunArtifact(rows, &a); err != nil {
			return nil, err
		}
		items = append(items, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getRunArtifactBySourceID = `-- name: GetRunArtifactBySourceID :one
SELECT id, run_id, artifact_type, title, content, visibility, source_artifact_id, created_at
FROM run_artifacts
WHERE run_id = $1
  AND source_artifact_id = $2`

type GetRunArtifactBySourceIDParams struct {
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	SourceArtifactID string    `db:"source_artifact_id" json:"source_artifact_id"`
}

func (q *Queries) GetRunArtifactBySourceID(ctx context.Context, arg GetRunArtifactBySourceIDParams) (RunArtifact, error) {
	row := q.db.QueryRow(ctx, getRunArtifactBySourceID, arg.RunID, arg.SourceArtifactID)
	var a RunArtifact
	err := scanRunArtifact(row, &a)
	return a, err
}

const updateRunArtifactContent = `-- name: UpdateRunArtifactContent :one
UPDATE run_artifacts
SET artifact_type = $3,
    title = $4,
    content = $5,
    visibility = $6
WHERE id = $1
  AND run_id = $2
RETURNING id, run_id, artifact_type, title, content, visibility, source_artifact_id, created_at`

type UpdateRunArtifactContentParams struct {
	ID           uuid.UUID `db:"id" json:"id"`
	RunID        uuid.UUID `db:"run_id" json:"run_id"`
	ArtifactType string    `db:"artifact_type" json:"artifact_type"`
	Title        string    `db:"title" json:"title"`
	Content      []byte    `db:"content" json:"content"`
	Visibility   string    `db:"visibility" json:"visibility"`
}

func (q *Queries) UpdateRunArtifactContent(ctx context.Context, arg UpdateRunArtifactContentParams) (RunArtifact, error) {
	row := q.db.QueryRow(ctx, updateRunArtifactContent,
		arg.ID,
		arg.RunID,
		arg.ArtifactType,
		arg.Title,
		arg.Content,
		arg.Visibility,
	)
	var a RunArtifact
	err := scanRunArtifact(row, &a)
	return a, err
}

func scanRunArtifactChunk(row interface {
	Scan(dest ...any) error
}, c *RunArtifactChunk) error {
	return row.Scan(
		&c.ID,
		&c.RunID,
		&c.RunArtifactID,
		&c.SourceArtifactID,
		&c.EventSequence,
		&c.ChunkIndex,
		&c.Append,
		&c.LastChunk,
		&c.Parts,
		&c.Payload,
		&c.CreatedAt,
	)
}

const createRunArtifactChunk = `-- name: CreateRunArtifactChunk :one
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
          append, last_chunk, parts, payload, created_at`

type CreateRunArtifactChunkParams struct {
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	RunArtifactID    uuid.UUID `db:"run_artifact_id" json:"run_artifact_id"`
	SourceArtifactID string    `db:"source_artifact_id" json:"source_artifact_id"`
	EventSequence    *int32    `db:"event_sequence" json:"event_sequence"`
	Append           bool      `db:"append" json:"append"`
	LastChunk        bool      `db:"last_chunk" json:"last_chunk"`
	Parts            []byte    `db:"parts" json:"parts"`
	Payload          []byte    `db:"payload" json:"payload"`
}

func (q *Queries) CreateRunArtifactChunk(ctx context.Context, arg CreateRunArtifactChunkParams) (RunArtifactChunk, error) {
	row := q.db.QueryRow(ctx, createRunArtifactChunk,
		arg.RunID,
		arg.RunArtifactID,
		arg.SourceArtifactID,
		arg.EventSequence,
		arg.Append,
		arg.LastChunk,
		arg.Parts,
		arg.Payload,
	)
	var c RunArtifactChunk
	err := scanRunArtifactChunk(row, &c)
	return c, err
}

const listRunArtifactChunksByRun = `-- name: ListRunArtifactChunksByRun :many
SELECT id, run_id, run_artifact_id, source_artifact_id, event_sequence, chunk_index,
       append, last_chunk, parts, payload, created_at
FROM run_artifact_chunks
WHERE run_id = $1
ORDER BY source_artifact_id ASC, chunk_index ASC, id ASC`

func (q *Queries) ListRunArtifactChunksByRun(ctx context.Context, runID uuid.UUID) ([]RunArtifactChunk, error) {
	rows, err := q.db.Query(ctx, listRunArtifactChunksByRun, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RunArtifactChunk
	for rows.Next() {
		var c RunArtifactChunk
		if err := scanRunArtifactChunk(rows, &c); err != nil {
			return nil, err
		}
		items = append(items, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
