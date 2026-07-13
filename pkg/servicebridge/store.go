package servicebridge

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ExecutionRecord struct {
	ExternalOrderID  uuid.UUID
	BuyerUserID      uuid.UUID
	SellerUserID     uuid.UUID
	TargetType       string
	TargetID         uuid.UUID
	InputFingerprint []byte
	TraceID          string
	ExecutionKind    *string
	ExecutionID      *uuid.UUID
}

type Store interface {
	Reserve(context.Context, ExecutionRecord) (ExecutionRecord, error)
	Get(context.Context, uuid.UUID) (ExecutionRecord, error)
	Attach(context.Context, uuid.UUID, string, uuid.UUID) (ExecutionRecord, error)
}

type SQLStore struct {
	pool *pgxpool.Pool
}

func NewSQLStore(pool *pgxpool.Pool) *SQLStore {
	return &SQLStore{pool: pool}
}

func (s *SQLStore) Reserve(ctx context.Context, record ExecutionRecord) (ExecutionRecord, error) {
	if s.pool == nil {
		return ExecutionRecord{}, errors.New("service bridge database is not configured")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO hosted_service_executions (
			external_order_id, buyer_user_id, seller_user_id, target_type,
			target_id, input_fingerprint, trace_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (external_order_id) DO NOTHING
	`, record.ExternalOrderID, record.BuyerUserID, record.SellerUserID, record.TargetType,
		record.TargetID, record.InputFingerprint, record.TraceID)
	if err != nil {
		return ExecutionRecord{}, err
	}
	return s.Get(ctx, record.ExternalOrderID)
}

func (s *SQLStore) Get(ctx context.Context, externalOrderID uuid.UUID) (ExecutionRecord, error) {
	if s.pool == nil {
		return ExecutionRecord{}, errors.New("service bridge database is not configured")
	}
	row := s.pool.QueryRow(ctx, `
		SELECT external_order_id, buyer_user_id, seller_user_id, target_type,
		       target_id, input_fingerprint, trace_id, execution_kind, execution_id
		FROM hosted_service_executions
		WHERE external_order_id = $1
	`, externalOrderID)
	return scanExecutionRecord(row)
}

func (s *SQLStore) Attach(ctx context.Context, externalOrderID uuid.UUID, executionKind string, executionID uuid.UUID) (ExecutionRecord, error) {
	if s.pool == nil {
		return ExecutionRecord{}, errors.New("service bridge database is not configured")
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE hosted_service_executions
		SET execution_kind = $2, execution_id = $3
		WHERE external_order_id = $1
		  AND execution_id IS NULL
		RETURNING external_order_id, buyer_user_id, seller_user_id, target_type,
		          target_id, input_fingerprint, trace_id, execution_kind, execution_id
	`, externalOrderID, executionKind, executionID)
	record, err := scanExecutionRecord(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.Get(ctx, externalOrderID)
	}
	return record, err
}

func scanExecutionRecord(row interface{ Scan(...any) error }) (ExecutionRecord, error) {
	var record ExecutionRecord
	err := row.Scan(
		&record.ExternalOrderID,
		&record.BuyerUserID,
		&record.SellerUserID,
		&record.TargetType,
		&record.TargetID,
		&record.InputFingerprint,
		&record.TraceID,
		&record.ExecutionKind,
		&record.ExecutionID,
	)
	return record, err
}
