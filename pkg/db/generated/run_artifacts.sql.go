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
		&a.MimeType,
		&a.FileUri,
		&a.FileName,
		&a.FileSha256,
		&a.FileSizeBytes,
		&a.CreatedAt,
	)
}

const createRunArtifact = `-- name: CreateRunArtifact :one
INSERT INTO run_artifacts (
    run_id, artifact_type, title, content, visibility, source_artifact_id,
    mime_type, file_uri, file_name, file_sha256, file_size_bytes
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
RETURNING id, run_id, artifact_type, title, content, visibility, source_artifact_id,
          mime_type, file_uri, file_name, file_sha256, file_size_bytes, created_at`

type CreateRunArtifactParams struct {
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	ArtifactType     string    `db:"artifact_type" json:"artifact_type"`
	Title            string    `db:"title" json:"title"`
	Content          []byte    `db:"content" json:"content"`
	Visibility       string    `db:"visibility" json:"visibility"`
	SourceArtifactID *string   `db:"source_artifact_id" json:"source_artifact_id"`
	MimeType         *string   `db:"mime_type" json:"mime_type"`
	FileUri          *string   `db:"file_uri" json:"file_uri"`
	FileName         *string   `db:"file_name" json:"file_name"`
	FileSha256       *string   `db:"file_sha256" json:"file_sha256"`
	FileSizeBytes    *int64    `db:"file_size_bytes" json:"file_size_bytes"`
}

func (q *Queries) CreateRunArtifact(ctx context.Context, arg CreateRunArtifactParams) (RunArtifact, error) {
	row := q.db.QueryRow(ctx, createRunArtifact,
		arg.RunID,
		arg.ArtifactType,
		arg.Title,
		arg.Content,
		arg.Visibility,
		arg.SourceArtifactID,
		arg.MimeType,
		arg.FileUri,
		arg.FileName,
		arg.FileSha256,
		arg.FileSizeBytes,
	)
	var a RunArtifact
	err := scanRunArtifact(row, &a)
	return a, err
}

const listRunArtifactsByRun = `-- name: ListRunArtifactsByRun :many
SELECT id, run_id, artifact_type, title, content, visibility, source_artifact_id,
       mime_type, file_uri, file_name, file_sha256, file_size_bytes, created_at
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
SELECT id, run_id, artifact_type, title, content, visibility, source_artifact_id,
       mime_type, file_uri, file_name, file_sha256, file_size_bytes, created_at
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
    visibility = $6,
    mime_type = $7,
    file_uri = $8,
    file_name = $9,
    file_sha256 = $10,
    file_size_bytes = $11
WHERE id = $1
  AND run_id = $2
RETURNING id, run_id, artifact_type, title, content, visibility, source_artifact_id,
          mime_type, file_uri, file_name, file_sha256, file_size_bytes, created_at`

type UpdateRunArtifactContentParams struct {
	ID            uuid.UUID `db:"id" json:"id"`
	RunID         uuid.UUID `db:"run_id" json:"run_id"`
	ArtifactType  string    `db:"artifact_type" json:"artifact_type"`
	Title         string    `db:"title" json:"title"`
	Content       []byte    `db:"content" json:"content"`
	Visibility    string    `db:"visibility" json:"visibility"`
	MimeType      *string   `db:"mime_type" json:"mime_type"`
	FileUri       *string   `db:"file_uri" json:"file_uri"`
	FileName      *string   `db:"file_name" json:"file_name"`
	FileSha256    *string   `db:"file_sha256" json:"file_sha256"`
	FileSizeBytes *int64    `db:"file_size_bytes" json:"file_size_bytes"`
}

func (q *Queries) UpdateRunArtifactContent(ctx context.Context, arg UpdateRunArtifactContentParams) (RunArtifact, error) {
	row := q.db.QueryRow(ctx, updateRunArtifactContent,
		arg.ID,
		arg.RunID,
		arg.ArtifactType,
		arg.Title,
		arg.Content,
		arg.Visibility,
		arg.MimeType,
		arg.FileUri,
		arg.FileName,
		arg.FileSha256,
		arg.FileSizeBytes,
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
		&c.PartsSha256,
		&c.PayloadSha256,
		&c.DeclaredSha256,
		&c.ChecksumStatus,
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
    append, last_chunk, parts, payload, parts_sha256, payload_sha256,
    declared_sha256, checksum_status
)
SELECT
    $1, $2, $3, $4, next_chunk.chunk_index,
    $5, $6, $7, $8, $9, $10, $11, $12
FROM next_chunk
RETURNING id, run_id, run_artifact_id, source_artifact_id, event_sequence, chunk_index,
          append, last_chunk, parts, payload, parts_sha256, payload_sha256,
          declared_sha256, checksum_status, created_at`

type CreateRunArtifactChunkParams struct {
	RunID            uuid.UUID `db:"run_id" json:"run_id"`
	RunArtifactID    uuid.UUID `db:"run_artifact_id" json:"run_artifact_id"`
	SourceArtifactID string    `db:"source_artifact_id" json:"source_artifact_id"`
	EventSequence    *int32    `db:"event_sequence" json:"event_sequence"`
	Append           bool      `db:"append" json:"append"`
	LastChunk        bool      `db:"last_chunk" json:"last_chunk"`
	Parts            []byte    `db:"parts" json:"parts"`
	Payload          []byte    `db:"payload" json:"payload"`
	PartsSha256      *string   `db:"parts_sha256" json:"parts_sha256"`
	PayloadSha256    *string   `db:"payload_sha256" json:"payload_sha256"`
	DeclaredSha256   *string   `db:"declared_sha256" json:"declared_sha256"`
	ChecksumStatus   string    `db:"checksum_status" json:"checksum_status"`
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
		arg.PartsSha256,
		arg.PayloadSha256,
		arg.DeclaredSha256,
		arg.ChecksumStatus,
	)
	var c RunArtifactChunk
	err := scanRunArtifactChunk(row, &c)
	return c, err
}

const listRunArtifactChunksByRun = `-- name: ListRunArtifactChunksByRun :many
SELECT id, run_id, run_artifact_id, source_artifact_id, event_sequence, chunk_index,
       append, last_chunk, parts, payload, parts_sha256, payload_sha256,
       declared_sha256, checksum_status, created_at
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
