package externalexecution

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const cancellationRecordColumns = `id, caller_service_id, external_request_id, actor_user_id,
	reason_code, state, execution_kind_snapshot, execution_id_snapshot, error_code,
	requested_at, applied_at, finished_at, updated_at`

var externalCancellationIDNamespace = uuid.MustParse("bfcc4752-6b61-5c94-8cf3-103b95d72472")

func deterministicExternalCancellationID(callerServiceID string, externalRequestID uuid.UUID) uuid.UUID {
	return uuid.NewHash(
		sha256.New(), externalCancellationIDNamespace,
		[]byte(callerServiceID+"\x00"+externalRequestID.String()), 5,
	)
}

func (s *SQLStore) GetKey(ctx context.Context, callerServiceID string, externalRequestID uuid.UUID) (ExecutionKey, error) {
	if s == nil || s.pool == nil {
		return ExecutionKey{}, errors.New("external execution database is not configured")
	}
	var key ExecutionKey
	err := s.pool.QueryRow(ctx, `
		SELECT caller_service_id, external_request_id, actor_user_id
		FROM external_execution_keys
		WHERE caller_service_id = $1 AND external_request_id = $2
	`, callerServiceID, externalRequestID).Scan(
		&key.CallerServiceID, &key.ExternalRequestID, &key.ActorUserID,
	)
	return key, err
}

func (s *SQLStore) GetCancellation(ctx context.Context, callerServiceID string, externalRequestID uuid.UUID) (CancellationRecord, error) {
	if s == nil || s.pool == nil {
		return CancellationRecord{}, errors.New("external execution database is not configured")
	}
	return scanCancellationRecord(s.pool.QueryRow(ctx, `
		SELECT `+cancellationRecordColumns+`
		FROM external_execution_cancellations
		WHERE caller_service_id = $1 AND external_request_id = $2
	`, callerServiceID, externalRequestID))
}

func (s *SQLStore) ClaimLaunch(
	ctx context.Context,
	callerServiceID string,
	externalRequestID, token uuid.UUID,
	lease time.Duration,
) (ExecutionRecord, bool, error) {
	if s == nil || s.pool == nil {
		return ExecutionRecord{}, false, errors.New("external execution database is not configured")
	}
	if token == uuid.Nil || lease.Milliseconds() < 1 {
		return ExecutionRecord{}, false, errors.New("external execution launch claim is invalid")
	}
	var record ExecutionRecord
	claimed := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := lockExternalExecutionKey(ctx, tx, callerServiceID, externalRequestID); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE external_executions e
			SET start_state = 'launching',
			    start_token = $3,
			    start_lease_until = clock_timestamp() + ($4::bigint * interval '1 millisecond'),
			    downstream_idempotency_key_hash = NULL,
			    downstream_creation_fingerprint = NULL,
			    updated_at = clock_timestamp()
			WHERE e.caller_service_id = $1
			  AND e.external_request_id = $2
			  AND e.execution_id IS NULL
			  AND NOT EXISTS (
			      SELECT 1 FROM external_execution_cancellations c
			      WHERE c.caller_service_id = e.caller_service_id
			        AND c.external_request_id = e.external_request_id
			  )
			  AND (
			      e.start_state = 'authorized'
			      OR (
			          e.start_state = 'launching'
			          AND e.start_lease_until <= clock_timestamp()
			          AND e.downstream_idempotency_key_hash IS NULL
			          AND e.downstream_creation_fingerprint IS NULL
			      )
			  )
			RETURNING `+executionRecordColumns+`
		`, callerServiceID, externalRequestID, token, lease.Milliseconds())
		var err error
		record, err = scanExecutionRecord(row)
		if errors.Is(err, pgx.ErrNoRows) {
			record, err = getExecutionTx(ctx, tx, callerServiceID, externalRequestID, true)
			return err
		}
		claimed = err == nil
		return err
	})
	return record, claimed, err
}

func (s *SQLStore) AttachLaunched(
	ctx context.Context,
	callerServiceID string,
	externalRequestID, token uuid.UUID,
	executionKind string,
	executionID uuid.UUID,
) (ExecutionRecord, bool, error) {
	return s.attachWithFence(ctx, callerServiceID, externalRequestID, &token, executionKind, executionID, false)
}

func (s *SQLStore) AttachCanceledRecovery(
	ctx context.Context,
	callerServiceID string,
	externalRequestID uuid.UUID,
	executionKind string,
	executionID uuid.UUID,
) (ExecutionRecord, bool, error) {
	return s.attachWithFence(ctx, callerServiceID, externalRequestID, nil, executionKind, executionID, true)
}

func (s *SQLStore) attachWithFence(
	ctx context.Context,
	callerServiceID string,
	externalRequestID uuid.UUID,
	token *uuid.UUID,
	executionKind string,
	executionID uuid.UUID,
	canceledRecovery bool,
) (ExecutionRecord, bool, error) {
	if s == nil || s.pool == nil {
		return ExecutionRecord{}, false, errors.New("external execution database is not configured")
	}
	if executionID == uuid.Nil || (executionKind != "run" && executionKind != "workflow_run") {
		return ExecutionRecord{}, false, errors.New("external execution attachment is invalid")
	}
	var record ExecutionRecord
	attached := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := lockExternalExecutionKey(ctx, tx, callerServiceID, externalRequestID); err != nil {
			return err
		}
		if _, err := getExecutionTx(ctx, tx, callerServiceID, externalRequestID, true); err != nil {
			return err
		}
		if canceledRecovery {
			if _, err := lockCancellationTx(ctx, tx, callerServiceID, externalRequestID); err != nil {
				return err
			}
			row := tx.QueryRow(ctx, `
				UPDATE external_executions
				SET execution_kind = $3, execution_id = $4, updated_at = clock_timestamp()
				WHERE caller_service_id = $1 AND external_request_id = $2
				  AND start_state = 'canceled'
				  AND execution_id IS NULL
				  AND downstream_idempotency_key_hash IS NOT NULL
				RETURNING `+executionRecordColumns+`
			`, callerServiceID, externalRequestID, executionKind, executionID)
			var err error
			record, err = scanExecutionRecord(row)
			if errors.Is(err, pgx.ErrNoRows) {
				record, err = getExecutionTx(ctx, tx, callerServiceID, externalRequestID, false)
				return err
			}
			if err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `
				UPDATE external_execution_cancellations
				SET execution_kind_snapshot = COALESCE(execution_kind_snapshot, $3),
				    execution_id_snapshot = COALESCE(execution_id_snapshot, $4),
				    updated_at = clock_timestamp()
				WHERE caller_service_id = $1 AND external_request_id = $2
			`, callerServiceID, externalRequestID, executionKind, executionID); err != nil {
				return err
			}
			attached = true
			return nil
		}
		if token == nil || *token == uuid.Nil {
			return errors.New("external execution launch token is invalid")
		}
		row := tx.QueryRow(ctx, `
			UPDATE external_executions
			SET execution_kind = $4,
			    execution_id = $5,
			    start_state = 'attached',
			    start_token = NULL,
			    start_lease_until = NULL,
			    updated_at = clock_timestamp()
			WHERE caller_service_id = $1 AND external_request_id = $2
			  AND execution_id IS NULL
			  AND start_state = 'launching'
			  AND start_token = $3
			RETURNING `+executionRecordColumns+`
		`, callerServiceID, externalRequestID, *token, executionKind, executionID)
		var err error
		record, err = scanExecutionRecord(row)
		if errors.Is(err, pgx.ErrNoRows) {
			record, err = getExecutionTx(ctx, tx, callerServiceID, externalRequestID, false)
			return err
		}
		attached = err == nil
		return err
	})
	return record, attached, err
}

func (s *SQLStore) RequestCancel(
	ctx context.Context,
	callerServiceID string,
	externalRequestID, actorUserID uuid.UUID,
	reasonCode string,
) (*ExecutionRecord, CancellationRecord, error) {
	if s == nil || s.pool == nil {
		return nil, CancellationRecord{}, errors.New("external execution database is not configured")
	}
	if actorUserID == uuid.Nil || (reasonCode != "CALLER_REQUESTED" && reasonCode != "DEADLINE_EXCEEDED") {
		return nil, CancellationRecord{}, errors.New("external cancellation request is invalid")
	}
	var execution *ExecutionRecord
	var cancellation CancellationRecord
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := insertExternalExecutionKey(ctx, tx, callerServiceID, externalRequestID, actorUserID); err != nil {
			return err
		}
		key, err := lockExternalExecutionKey(ctx, tx, callerServiceID, externalRequestID)
		if err != nil {
			return err
		}
		if key.ActorUserID != actorUserID {
			return ErrExecutionIdentityConflict
		}
		record, err := getExecutionTx(ctx, tx, callerServiceID, externalRequestID, true)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil {
			execution = &record
		}
		if existing, err := lockCancellationTx(ctx, tx, callerServiceID, externalRequestID); err == nil {
			cancellation = existing
			return nil
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		state := "stopped"
		var kind *string
		var id *uuid.UUID
		if execution != nil {
			kind, id = execution.ExecutionKind, execution.ExecutionID
			if id != nil || (execution.StartState == startStateLaunching &&
				(execution.TargetType == TargetTypeWorkflow || len(execution.DownstreamKeyHash) > 0)) {
				state = "requested"
			}
			if _, err := tx.Exec(ctx, `
				UPDATE external_executions
				SET start_state = 'canceled', start_token = NULL,
				    start_lease_until = NULL, rejection_code = NULL,
				    updated_at = clock_timestamp()
				WHERE caller_service_id = $1 AND external_request_id = $2
			`, callerServiceID, externalRequestID); err != nil {
				return err
			}
			execution.StartState = startStateCanceled
			execution.StartToken = nil
			execution.StartLeaseUntil = nil
		}
		idValue := deterministicExternalCancellationID(callerServiceID, externalRequestID)
		row := tx.QueryRow(ctx, `
			INSERT INTO external_execution_cancellations (
			    id, caller_service_id, external_request_id, actor_user_id,
			    reason_code, state, execution_kind_snapshot, execution_id_snapshot,
			    applied_at, finished_at
			) VALUES (
			    $1, $2, $3, $4, $5, $6, $7, $8,
			    CASE WHEN $6 = 'stopped' THEN clock_timestamp() ELSE NULL END,
			    CASE WHEN $6 = 'stopped' THEN clock_timestamp() ELSE NULL END
			)
			RETURNING `+cancellationRecordColumns+`
		`, idValue, callerServiceID, externalRequestID, actorUserID, reasonCode, state, kind, id)
		cancellation, err = scanCancellationRecord(row)
		return err
	})
	return execution, cancellation, err
}

func (s *SQLStore) AdvanceCancellation(
	ctx context.Context,
	callerServiceID string,
	externalRequestID uuid.UUID,
	nextState string,
) (*ExecutionRecord, CancellationRecord, error) {
	if s == nil || s.pool == nil {
		return nil, CancellationRecord{}, errors.New("external execution database is not configured")
	}
	if nextState != "stopping" && nextState != "stopped" && nextState != "unconfirmed" && nextState != "not_applied" {
		return nil, CancellationRecord{}, errors.New("external cancellation state is invalid")
	}
	var execution *ExecutionRecord
	var cancellation CancellationRecord
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := lockExternalExecutionKey(ctx, tx, callerServiceID, externalRequestID); err != nil {
			return err
		}
		record, err := getExecutionTx(ctx, tx, callerServiceID, externalRequestID, true)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil {
			execution = &record
		}
		current, err := lockCancellationTx(ctx, tx, callerServiceID, externalRequestID)
		if err != nil {
			return err
		}
		if current.State == nextState || cancellationTerminal(current.State) {
			cancellation = current
			return nil
		}
		if !cancellationTransitionAllowed(current.State, nextState) {
			return fmt.Errorf("external cancellation transition %s -> %s is invalid", current.State, nextState)
		}
		var kind *string
		var id *uuid.UUID
		if execution != nil {
			kind, id = execution.ExecutionKind, execution.ExecutionID
		}
		row := tx.QueryRow(ctx, `
			UPDATE external_execution_cancellations
			SET state = $3,
			    execution_kind_snapshot = COALESCE(execution_kind_snapshot, $4),
			    execution_id_snapshot = COALESCE(execution_id_snapshot, $5),
			    applied_at = COALESCE(applied_at, clock_timestamp()),
			    finished_at = CASE
			        WHEN $3 IN ('stopped', 'unconfirmed', 'not_applied')
			        THEN COALESCE(finished_at, clock_timestamp())
			        ELSE NULL
			    END,
			    error_code = CASE WHEN $3 = 'unconfirmed' THEN 'CANCEL_UNCONFIRMED' ELSE NULL END,
			    updated_at = clock_timestamp()
			WHERE caller_service_id = $1 AND external_request_id = $2
			RETURNING `+cancellationRecordColumns+`
		`, callerServiceID, externalRequestID, nextState, kind, id)
		cancellation, err = scanCancellationRecord(row)
		return err
	})
	return execution, cancellation, err
}

func (s *SQLStore) ListPendingCancellations(ctx context.Context, limit int) ([]CancellationRecord, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("external execution database is not configured")
	}
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+cancellationRecordColumns+`
		FROM external_execution_cancellations
		WHERE state IN ('requested', 'stopping')
		ORDER BY updated_at ASC, id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]CancellationRecord, 0)
	for rows.Next() {
		record, err := scanCancellationRecord(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func cancellationTransitionAllowed(current, next string) bool {
	switch current {
	case "requested":
		return next == "stopping" || next == "stopped" || next == "unconfirmed" || next == "not_applied"
	case "stopping":
		return next == "stopped" || next == "unconfirmed"
	default:
		return false
	}
}

func cancellationTerminal(state string) bool {
	return state == "stopped" || state == "unconfirmed" || state == "not_applied"
}

func insertExternalExecutionKey(
	ctx context.Context,
	tx pgx.Tx,
	callerServiceID string,
	externalRequestID, actorUserID uuid.UUID,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO external_execution_keys (
		    caller_service_id, external_request_id, actor_user_id
		) VALUES ($1, $2, $3)
		ON CONFLICT (caller_service_id, external_request_id) DO NOTHING
	`, callerServiceID, externalRequestID, actorUserID)
	return err
}

func lockExternalExecutionKey(
	ctx context.Context,
	tx pgx.Tx,
	callerServiceID string,
	externalRequestID uuid.UUID,
) (ExecutionKey, error) {
	var key ExecutionKey
	err := tx.QueryRow(ctx, `
		SELECT caller_service_id, external_request_id, actor_user_id
		FROM external_execution_keys
		WHERE caller_service_id = $1 AND external_request_id = $2
		FOR UPDATE
	`, callerServiceID, externalRequestID).Scan(
		&key.CallerServiceID, &key.ExternalRequestID, &key.ActorUserID,
	)
	return key, err
}

func getExecutionTx(
	ctx context.Context,
	tx pgx.Tx,
	callerServiceID string,
	externalRequestID uuid.UUID,
	forUpdate bool,
) (ExecutionRecord, error) {
	query := `SELECT ` + executionRecordColumns + `
		FROM external_executions
		WHERE caller_service_id = $1 AND external_request_id = $2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	return scanExecutionRecord(tx.QueryRow(ctx, query, callerServiceID, externalRequestID))
}

func lockCancellationTx(
	ctx context.Context,
	tx pgx.Tx,
	callerServiceID string,
	externalRequestID uuid.UUID,
) (CancellationRecord, error) {
	return scanCancellationRecord(tx.QueryRow(ctx, `
		SELECT `+cancellationRecordColumns+`
		FROM external_execution_cancellations
		WHERE caller_service_id = $1 AND external_request_id = $2
		FOR UPDATE
	`, callerServiceID, externalRequestID))
}

func scanCancellationRecord(row interface{ Scan(...any) error }) (CancellationRecord, error) {
	var record CancellationRecord
	err := row.Scan(
		&record.ID, &record.CallerServiceID, &record.ExternalRequestID,
		&record.ActorUserID, &record.ReasonCode, &record.State,
		&record.ExecutionKindSnapshot, &record.ExecutionIDSnapshot,
		&record.ErrorCode, &record.RequestedAt, &record.AppliedAt,
		&record.FinishedAt, &record.UpdatedAt,
	)
	return record, err
}
