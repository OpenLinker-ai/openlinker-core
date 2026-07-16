package externalexecution

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type memoryCancellationStoreState struct {
	keys          map[string]ExecutionKey
	cancellations map[string]CancellationRecord
}

var memoryCancellationStates sync.Map

func memoryCancellationState(store *memoryStore) *memoryCancellationStoreState {
	state, _ := memoryCancellationStates.LoadOrStore(store, &memoryCancellationStoreState{
		keys: map[string]ExecutionKey{}, cancellations: map[string]CancellationRecord{},
	})
	return state.(*memoryCancellationStoreState)
}

func (m *memoryStore) ClaimLaunch(
	_ context.Context, caller string, requestID, token uuid.UUID, lease time.Duration,
) (ExecutionRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(caller, requestID)
	record, ok := m.records[key]
	if !ok {
		return ExecutionRecord{}, false, pgx.ErrNoRows
	}
	record = memoryExecutionRecordDefaults(record)
	claimable := record.StartState == startStateAuthorized ||
		(record.StartState == startStateLaunching && record.StartLeaseUntil != nil &&
			!record.StartLeaseUntil.After(m.now()) && len(record.DownstreamKeyHash) == 0)
	if !claimable {
		return record, false, nil
	}
	until := m.now().Add(lease)
	record.StartState = startStateLaunching
	record.StartToken = &token
	record.StartLeaseUntil = &until
	record.DownstreamKeyHash = nil
	record.DownstreamFingerprint = nil
	m.records[key] = record
	return record, true, nil
}

func (m *memoryStore) AttachLaunched(
	_ context.Context, caller string, requestID, token uuid.UUID, kind string, executionID uuid.UUID,
) (ExecutionRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(caller, requestID)
	record, ok := m.records[key]
	if !ok {
		return ExecutionRecord{}, false, pgx.ErrNoRows
	}
	if record.StartState == startStateLaunching && record.StartToken != nil && *record.StartToken == token {
		record.StartState = startStateAttached
		record.StartToken = nil
		record.StartLeaseUntil = nil
		record.ExecutionKind = &kind
		record.ExecutionID = &executionID
		m.records[key] = record
		return record, true, nil
	}
	return record, false, nil
}

func (m *memoryStore) AttachCanceledRecovery(
	_ context.Context, caller string, requestID uuid.UUID, kind string, executionID uuid.UUID,
) (ExecutionRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(caller, requestID)
	record, ok := m.records[key]
	if !ok {
		return ExecutionRecord{}, false, pgx.ErrNoRows
	}
	if record.StartState == startStateCanceled && record.ExecutionID == nil && len(record.DownstreamKeyHash) > 0 {
		record.ExecutionKind = &kind
		record.ExecutionID = &executionID
		m.records[key] = record
		return record, true, nil
	}
	return record, false, nil
}

func (m *memoryStore) GetKey(_ context.Context, caller string, requestID uuid.UUID) (ExecutionKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(caller, requestID)
	if record, ok := m.records[key]; ok {
		return ExecutionKey{CallerServiceID: caller, ExternalRequestID: requestID, ActorUserID: record.ActorUserID}, nil
	}
	state := memoryCancellationState(m)
	if stored, ok := state.keys[key]; ok {
		return stored, nil
	}
	return ExecutionKey{}, pgx.ErrNoRows
}

func (m *memoryStore) GetCancellation(_ context.Context, caller string, requestID uuid.UUID) (CancellationRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := memoryCancellationState(m).cancellations[executionStoreKey(caller, requestID)]
	if !ok {
		return CancellationRecord{}, pgx.ErrNoRows
	}
	return record, nil
}

func (m *memoryStore) RequestCancel(
	_ context.Context, caller string, requestID, actorID uuid.UUID, reason string,
) (*ExecutionRecord, CancellationRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(caller, requestID)
	state := memoryCancellationState(m)
	if existing, ok := state.cancellations[key]; ok {
		var execution *ExecutionRecord
		if record, found := m.records[key]; found {
			copyRecord := record
			execution = &copyRecord
		}
		return execution, existing, nil
	}
	if existing, ok := m.records[key]; ok && existing.ActorUserID != actorID {
		return nil, CancellationRecord{}, ErrExecutionIdentityConflict
	}
	state.keys[key] = ExecutionKey{CallerServiceID: caller, ExternalRequestID: requestID, ActorUserID: actorID}
	now := m.now()
	cancellation := CancellationRecord{
		ID: deterministicExternalCancellationID(caller, requestID), CallerServiceID: caller,
		ExternalRequestID: requestID, ActorUserID: actorID, ReasonCode: reason,
		State: "stopped", RequestedAt: now, UpdatedAt: now,
	}
	var execution *ExecutionRecord
	if record, ok := m.records[key]; ok {
		if record.ExecutionID != nil || (record.StartState == startStateLaunching && len(record.DownstreamKeyHash) > 0) {
			cancellation.State = "requested"
		} else {
			applied, finished := now, now
			cancellation.AppliedAt, cancellation.FinishedAt = &applied, &finished
		}
		record.StartState = startStateCanceled
		record.StartToken, record.StartLeaseUntil = nil, nil
		record.RejectionCode = nil
		m.records[key] = record
		copyRecord := record
		execution = &copyRecord
		cancellation.ExecutionKindSnapshot = record.ExecutionKind
		cancellation.ExecutionIDSnapshot = record.ExecutionID
	} else {
		applied, finished := now, now
		cancellation.AppliedAt, cancellation.FinishedAt = &applied, &finished
	}
	state.cancellations[key] = cancellation
	return execution, cancellation, nil
}

func (m *memoryStore) AdvanceCancellation(
	_ context.Context, caller string, requestID uuid.UUID, next string,
) (*ExecutionRecord, CancellationRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := executionStoreKey(caller, requestID)
	state := memoryCancellationState(m)
	cancellation, ok := state.cancellations[key]
	if !ok {
		return nil, CancellationRecord{}, pgx.ErrNoRows
	}
	if !cancellationTerminal(cancellation.State) && cancellationTransitionAllowed(cancellation.State, next) {
		now := m.now()
		cancellation.State = next
		if cancellation.AppliedAt == nil {
			cancellation.AppliedAt = &now
		}
		if cancellationTerminal(next) {
			cancellation.FinishedAt = &now
		}
		if next == "unconfirmed" {
			code := "CANCEL_UNCONFIRMED"
			cancellation.ErrorCode = &code
		}
		cancellation.UpdatedAt = now
		state.cancellations[key] = cancellation
	}
	var execution *ExecutionRecord
	if record, found := m.records[key]; found {
		copyRecord := record
		execution = &copyRecord
	}
	return execution, cancellation, nil
}

func (m *memoryStore) ListPendingCancellations(context.Context, int) ([]CancellationRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := []CancellationRecord{}
	for _, record := range memoryCancellationState(m).cancellations {
		if record.State == "requested" || record.State == "stopping" {
			result = append(result, record)
		}
	}
	return result, nil
}
