package runtime

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

// Runtime event errors are transport-neutral. HTTP, WebSocket, gRPC, and MCP
// adapters map these codes to their own error envelopes without parsing text.
type RuntimeEventErrorCode string

const (
	RuntimeEventErrorIDConflict            RuntimeEventErrorCode = "EVENT_ID_CONFLICT"
	RuntimeEventErrorStaleLease            RuntimeEventErrorCode = "STALE_LEASE"
	RuntimeEventErrorLeaseExpired          RuntimeEventErrorCode = "LEASE_EXPIRED"
	RuntimeEventErrorLeaseIdentityMismatch RuntimeEventErrorCode = "LEASE_IDENTITY_MISMATCH"
	RuntimeEventErrorRunAlreadyTerminal    RuntimeEventErrorCode = "RUN_ALREADY_TERMINAL"
	RuntimeEventErrorEventsMissing         RuntimeEventErrorCode = "EVENTS_MISSING"
)

var (
	// ErrInvalidRuntimeEvent is returned before touching PostgreSQL when the
	// request cannot satisfy the runtime v2 event contract.
	ErrInvalidRuntimeEvent = errors.New("invalid runtime event")
	errNilEventStore       = errors.New("runtime event store is not configured")
)

// RuntimeEventError carries a stable reason code and, for EVENTS_MISSING, the
// exact client sequence ranges that must be uploaded before retrying Result.
type RuntimeEventError struct {
	Code          RuntimeEventErrorCode `json:"code"`
	MissingRanges []EventRange          `json:"missing_ranges,omitempty"`
	cause         error
}

func (e *RuntimeEventError) Error() string {
	if e == nil {
		return ""
	}
	switch e.Code {
	case RuntimeEventErrorIDConflict:
		return "runtime event identity conflicts with a stored event"
	case RuntimeEventErrorStaleLease:
		return "runtime lease is stale"
	case RuntimeEventErrorLeaseExpired:
		return "runtime lease or execution deadline has expired"
	case RuntimeEventErrorLeaseIdentityMismatch:
		return "authenticated runtime identity does not own the lease"
	case RuntimeEventErrorRunAlreadyTerminal:
		return "run is already terminal"
	case RuntimeEventErrorEventsMissing:
		return "runtime events are missing"
	default:
		return "runtime event rejected"
	}
}

func (e *RuntimeEventError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// IsRuntimeEventError reports whether err has the requested stable code.
func IsRuntimeEventError(err error, code RuntimeEventErrorCode) bool {
	var eventErr *RuntimeEventError
	return errors.As(err, &eventErr) && eventErr.Code == code
}

func newRuntimeEventError(code RuntimeEventErrorCode, cause error) error {
	return &RuntimeEventError{Code: code, cause: cause}
}

// RuntimeAttemptIdentity is the immutable identity from the runtime v2 wire
// contract. AttemptNo is intentionally absent: Core derives it from the
// locked run_attempts row after assignment confirmation.
//
// NodeID, RuntimeSessionID, and WorkerID are required for agent_node attempts
// and absent for Core-owned HTTP/MCP attempts.
type RuntimeAttemptIdentity struct {
	RunID            uuid.UUID  `json:"run_id"`
	AttemptID        uuid.UUID  `json:"attempt_id"`
	LeaseID          uuid.UUID  `json:"lease_id"`
	FencingToken     int64      `json:"fencing_token"`
	NodeID           *uuid.UUID `json:"node_id,omitempty"`
	AgentID          uuid.UUID  `json:"agent_id"`
	WorkerID         *string    `json:"worker_id,omitempty"`
	RuntimeSessionID *uuid.UUID `json:"runtime_session_id,omitempty"`
}

// RuntimeEventPrincipal is the already-authenticated execution principal.
// EventStore never accepts a token or transport assertion in place of it.
type RuntimeEventPrincipal struct {
	AgentID          uuid.UUID
	NodeID           *uuid.UUID
	WorkerID         *string
	RuntimeSessionID *uuid.UUID
}

// RuntimeEventRequest is the transport-neutral RunEventPayload body after its
// AttemptIdentity has been separated by the caller.
type RuntimeEventRequest struct {
	ClientEventID  uuid.UUID      `json:"client_event_id"`
	ClientEventSeq int64          `json:"client_event_seq"`
	EventType      string         `json:"event_type"`
	Payload        map[string]any `json:"payload"`
}

// RuntimeEventAck matches RunEventAckPayload. Inserted is internal-only: an
// upper layer may publish side effects exclusively when it is true.
type RuntimeEventAck struct {
	ClientEventID  uuid.UUID `json:"client_event_id"`
	ClientEventSeq int64     `json:"client_event_seq"`
	Sequence       int32     `json:"sequence"`
	Replayed       bool      `json:"replayed"`
	Inserted       bool      `json:"-"`
	EventID        uuid.UUID `json:"-"`
	CreatedAt      time.Time `json:"-"`
}

// EventRange is an inclusive range of missing client event sequences.
type EventRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// EventStore appends runtime execution events using PostgreSQL as the sole
// linearization point.
type EventStore struct {
	pool *pgxpool.Pool
}

func NewEventStore(pool *pgxpool.Pool) *EventStore {
	return &EventStore{pool: pool}
}

// NewRuntimeEventStore is an explicit alias for call sites where EventStore
// would otherwise be ambiguous with domain or delivery event stores.
func NewRuntimeEventStore(pool *pgxpool.Pool) *EventStore {
	return NewEventStore(pool)
}

const runtimeEventFingerprintVersion = "openlinker.runtime-event.v1"

// MaxRuntimeEventPayloadBytes is the runtime v2 max_non_artifact_message_bytes
// limit. It is applied to RFC 8785 canonical payload bytes, not to a
// transport-dependent encoded envelope.
const MaxRuntimeEventPayloadBytes = 4 * 1024 * 1024

var runtimeEventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

// Runtime workers may report execution progress, but Core alone owns public
// terminal events and the synthetic retention-gap marker. Accepting either
// from a worker would let an untrusted executor forge lifecycle evidence (or
// trip the terminal-event database invariant before Result finalization).
var coreOwnedRuntimeEventTypes = map[string]struct{}{
	"run.completed":  {},
	"run.failed":     {},
	"run.canceled":   {},
	"run.stream.gap": {},
}

// RuntimeEventFingerprint computes the Core-owned payload fingerprint. The
// stable client event ID and transport envelope are deliberately excluded so
// callers cannot choose or spoof the digest domain.
func RuntimeEventFingerprint(request RuntimeEventRequest) ([]byte, error) {
	if _, err := canonicalRuntimeEventPayload(request); err != nil {
		return nil, err
	}
	canonical, err := CanonicalizeRFC8785(map[string]any{
		"version":          runtimeEventFingerprintVersion,
		"client_event_seq": request.ClientEventSeq,
		"event_type":       request.EventType,
		"payload":          request.Payload,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: canonical payload: %v", ErrInvalidRuntimeEvent, err)
	}
	digest := sha256.Sum256(canonical)
	return digest[:], nil
}

// Append is a READ COMMITTED transaction. The Run row is the per-Run
// linearization lock. A stored client_event_id is resolved before any active
// lease/deadline validation so a legal replay remains ACK-able after expiry,
// cancellation, completion, failover, or fence rotation.
func (s *EventStore) Append(
	ctx context.Context,
	principal RuntimeEventPrincipal,
	identity RuntimeAttemptIdentity,
	request RuntimeEventRequest,
) (RuntimeEventAck, error) {
	if s == nil || s.pool == nil {
		return RuntimeEventAck{}, errNilEventStore
	}
	if err := validateRuntimeAttemptIdentity(identity); err != nil {
		return RuntimeEventAck{}, err
	}
	if err := validateRuntimeEventPrincipal(principal); err != nil {
		return RuntimeEventAck{}, err
	}
	fingerprint, err := RuntimeEventFingerprint(request)
	if err != nil {
		return RuntimeEventAck{}, err
	}
	payload, err := canonicalRuntimeEventPayload(request)
	if err != nil {
		return RuntimeEventAck{}, err
	}

	var ack RuntimeEventAck
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		queries := db.New(tx)
		run, err := lockRuntimeEventRun(ctx, queries, identity.RunID)
		if err != nil {
			return err
		}
		// Ownership is checked before client_event_id lookup. Otherwise an
		// authenticated Agent from another Run could use conflict/replay
		// behavior as an event-ID existence oracle.
		if principal.AgentID != run.agentID || identity.AgentID != run.agentID {
			return newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, nil)
		}
		if run.runtimeContractID != "openlinker.runtime.v2" {
			return newRuntimeEventError(RuntimeEventErrorStaleLease, nil)
		}

		stored, found, err := findStoredRuntimeEvent(ctx, queries, identity.RunID, request.ClientEventID)
		if err != nil {
			return err
		}
		if found {
			if !storedRuntimeEventMatches(stored, identity, request.ClientEventSeq, fingerprint) {
				return newRuntimeEventError(RuntimeEventErrorIDConflict, nil)
			}
			if !runtimePrincipalMatchesAttempt(principal, stored.attempt) {
				return newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, nil)
			}
			ack = RuntimeEventAck{
				ClientEventID:  request.ClientEventID,
				ClientEventSeq: request.ClientEventSeq,
				Sequence:       stored.sequence,
				Replayed:       true,
				Inserted:       false,
				EventID:        stored.id,
				CreatedAt:      stored.createdAt,
			}
			return nil
		}

		if run.status != "running" || run.dispatchState == "terminal" || run.dispatchState == "dead_letter" {
			return newRuntimeEventError(RuntimeEventErrorRunAlreadyTerminal, nil)
		}
		if run.dispatchState != "executing" || run.activeAttemptID == nil || *run.activeAttemptID != identity.AttemptID {
			return newRuntimeEventError(RuntimeEventErrorStaleLease, nil)
		}

		attempt, err := getRuntimeEventAttempt(ctx, queries, identity.RunID, identity.AttemptID)
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeEventError(RuntimeEventErrorStaleLease, err)
		}
		if err != nil {
			return err
		}
		if attempt.attemptNo == nil || attempt.acceptedAt == nil || attempt.finishedAt != nil || attempt.resultID != nil {
			return newRuntimeEventError(RuntimeEventErrorStaleLease, nil)
		}
		if attempt.leaseID != identity.LeaseID || attempt.fencingToken != identity.FencingToken ||
			run.leaseID == nil || *run.leaseID != identity.LeaseID || run.fencingToken != identity.FencingToken {
			return newRuntimeEventError(RuntimeEventErrorStaleLease, nil)
		}
		if !runtimeIdentityMatchesAttempt(identity, attempt) || !runtimePrincipalMatchesAttempt(principal, attempt) {
			return newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, nil)
		}
		if !runtimeEventDeadlinesValid(run.databaseNow, run.runDeadlineAt, attempt.leaseExpiresAt, attempt.attemptDeadlineAt) {
			return newRuntimeEventError(RuntimeEventErrorLeaseExpired, nil)
		}

		_, err = queries.GetRunEventByAttemptSequence(ctx, db.GetRunEventByAttemptSequenceParams{
			RunID:          identity.RunID,
			AttemptID:      identity.AttemptID,
			AttemptNo:      *attempt.attemptNo,
			ClientEventSeq: request.ClientEventSeq,
		})
		if err == nil {
			return newRuntimeEventError(RuntimeEventErrorIDConflict, nil)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		// Every writer uses the same Run-row -> advisory-lock order. The
		// advisory lock keeps global sequence allocation compatible with system
		// event writers while they are migrated to this shared store.
		if err := queries.LockRunEventSequence(ctx, identity.RunID); err != nil {
			return err
		}

		var parentRunID *uuid.UUID
		delegation, delegationErr := queries.GetRunDelegationByChild(ctx, identity.RunID)
		if delegationErr == nil {
			parentRunID = &delegation.ParentRunID
		} else if !errors.Is(delegationErr, pgx.ErrNoRows) {
			return delegationErr
		}

		inserted, err := queries.CreateRuntimeRunEvent(ctx, db.CreateRuntimeRunEventParams{
			RunID:              identity.RunID,
			ParentRunID:        parentRunID,
			EventType:          request.EventType,
			Payload:            payload,
			ClientEventID:      request.ClientEventID,
			ClientEventSeq:     request.ClientEventSeq,
			PayloadFingerprint: fingerprint,
			AttemptID:          identity.AttemptID,
			AttemptNo:          *attempt.attemptNo,
			FencingToken:       identity.FencingToken,
		})
		if err != nil {
			return err
		}

		_, err = queries.AdvanceRunAttemptEventSequence(ctx, db.AdvanceRunAttemptEventSequenceParams{
			RunID:          identity.RunID,
			ID:             identity.AttemptID,
			LeaseID:        identity.LeaseID,
			FencingToken:   identity.FencingToken,
			ClientEventSeq: request.ClientEventSeq,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeEventError(RuntimeEventErrorStaleLease, err)
			}
			return err
		}

		ack = RuntimeEventAck{
			ClientEventID:  request.ClientEventID,
			ClientEventSeq: request.ClientEventSeq,
			Sequence:       inserted.Sequence,
			Replayed:       false,
			Inserted:       true,
			EventID:        inserted.ID,
			CreatedAt:      inserted.CreatedAt,
		}
		return nil
	})
	if err != nil {
		return RuntimeEventAck{}, err
	}
	return ack, nil
}

// AppendEvent is a naming-compatible wrapper for callers that make the event
// nature explicit at the call site.
func (s *EventStore) AppendEvent(
	ctx context.Context,
	principal RuntimeEventPrincipal,
	identity RuntimeAttemptIdentity,
	request RuntimeEventRequest,
) (RuntimeEventAck, error) {
	return s.Append(ctx, principal, identity, request)
}

// MissingClientEventRanges returns inclusive gaps in 1..finalClientEventSeq.
// It scans only persisted sequences with implicit 0/N+1 sentinels and lag; it
// never expands the range with generate_series, so work is proportional to
// events actually retained rather than the claimed final sequence.
func (s *EventStore) MissingClientEventRanges(
	ctx context.Context,
	runID uuid.UUID,
	attemptID uuid.UUID,
	finalClientEventSeq int64,
) ([]EventRange, error) {
	if s == nil || s.pool == nil {
		return nil, errNilEventStore
	}
	if runID == uuid.Nil || attemptID == uuid.Nil || finalClientEventSeq < 0 || finalClientEventSeq == math.MaxInt64 {
		return nil, ErrInvalidRuntimeEvent
	}
	if finalClientEventSeq == 0 {
		return []EventRange{}, nil
	}

	queries := db.New(s.pool)
	attempt, err := queries.GetRunAttemptByID(ctx, attemptID)
	if err != nil {
		return nil, err
	}
	if attempt.RunID != runID || attempt.AttemptNo == nil {
		return nil, ErrInvalidRuntimeEvent
	}
	sequences, err := queries.ListClientEventSequencesThrough(ctx, db.ListClientEventSequencesThroughParams{
		RunID:           runID,
		AttemptID:       attemptID,
		AttemptNo:       *attempt.AttemptNo,
		ThroughSequence: finalClientEventSeq,
	})
	if err != nil {
		return nil, err
	}
	return missingRuntimeEventRanges(sequences, finalClientEventSeq), nil
}

// RequireCompleteClientEvents turns exact missing ranges into the stable
// EVENTS_MISSING error consumed by the Result finalizer.
func (s *EventStore) RequireCompleteClientEvents(
	ctx context.Context,
	runID uuid.UUID,
	attemptID uuid.UUID,
	finalClientEventSeq int64,
) error {
	ranges, err := s.MissingClientEventRanges(ctx, runID, attemptID, finalClientEventSeq)
	if err != nil {
		return err
	}
	if len(ranges) == 0 {
		return nil
	}
	return &RuntimeEventError{Code: RuntimeEventErrorEventsMissing, MissingRanges: ranges}
}

type runtimeEventRun struct {
	agentID           uuid.UUID
	runtimeContractID string
	status            string
	dispatchState     string
	activeAttemptID   *uuid.UUID
	leaseID           *uuid.UUID
	fencingToken      int64
	runDeadlineAt     time.Time
	databaseNow       time.Time
}

type runtimeEventAttempt struct {
	id                uuid.UUID
	runID             uuid.UUID
	agentID           uuid.UUID
	attemptNo         *int32
	executorType      string
	leaseID           uuid.UUID
	fencingToken      int64
	workerID          *string
	runtimeSessionID  *uuid.UUID
	nodeID            *uuid.UUID
	acceptedAt        *time.Time
	leaseExpiresAt    time.Time
	attemptDeadlineAt time.Time
	finishedAt        *time.Time
	resultID          *uuid.UUID
}

type runtimeStoredEvent struct {
	id                 uuid.UUID
	sequence           int32
	clientEventSeq     int64
	payloadFingerprint []byte
	attemptID          uuid.UUID
	attemptNo          int32
	fencingToken       int64
	createdAt          time.Time
	attempt            runtimeEventAttempt
}

func missingRuntimeEventRanges(sequences []int64, finalClientEventSeq int64) []EventRange {
	ranges := make([]EventRange, 0)
	previous := int64(0)
	for _, sequence := range sequences {
		if sequence <= previous || sequence > finalClientEventSeq {
			continue
		}
		if sequence > previous+1 {
			ranges = append(ranges, EventRange{Start: previous + 1, End: sequence - 1})
		}
		previous = sequence
	}
	sentinel := finalClientEventSeq + 1
	if sentinel > previous+1 {
		ranges = append(ranges, EventRange{Start: previous + 1, End: sentinel - 1})
	}
	return ranges
}

func lockRuntimeEventRun(ctx context.Context, queries *db.Queries, runID uuid.UUID) (runtimeEventRun, error) {
	locked, err := queries.LockRunForEventAppend(ctx, runID)
	if err != nil {
		return runtimeEventRun{}, err
	}
	if locked.RunDeadlineAt == nil {
		return runtimeEventRun{}, errors.New("runtime v2 Run is missing run deadline")
	}
	return runtimeEventRun{
		agentID:           locked.AgentID,
		runtimeContractID: locked.RuntimeContractID,
		status:            locked.Status,
		dispatchState:     locked.DispatchState,
		activeAttemptID:   locked.ActiveAttemptID,
		leaseID:           locked.LeaseID,
		fencingToken:      locked.FencingToken,
		runDeadlineAt:     *locked.RunDeadlineAt,
		databaseNow:       locked.DatabaseNow,
	}, nil
}

func getRuntimeEventAttempt(ctx context.Context, queries *db.Queries, runID, attemptID uuid.UUID) (runtimeEventAttempt, error) {
	attempt, err := queries.GetRunAttemptByID(ctx, attemptID)
	if err != nil {
		return runtimeEventAttempt{}, err
	}
	if attempt.RunID != runID {
		return runtimeEventAttempt{}, pgx.ErrNoRows
	}
	return runtimeEventAttemptFromDB(attempt), nil
}

func findStoredRuntimeEvent(
	ctx context.Context,
	queries *db.Queries,
	runID uuid.UUID,
	clientEventID uuid.UUID,
) (runtimeStoredEvent, bool, error) {
	event, err := queries.GetRunEventByClientID(ctx, db.GetRunEventByClientIDParams{
		RunID:         runID,
		ClientEventID: clientEventID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return runtimeStoredEvent{}, false, nil
	}
	if err != nil {
		return runtimeStoredEvent{}, false, err
	}
	if event.ClientEventSeq == nil || event.AttemptID == nil || event.AttemptNo == nil || event.FencingToken == nil {
		return runtimeStoredEvent{}, false, errors.New("stored runtime event is missing client identity")
	}
	attempt, err := queries.GetRunAttemptByID(ctx, *event.AttemptID)
	if err != nil {
		return runtimeStoredEvent{}, false, err
	}
	if attempt.RunID != runID {
		return runtimeStoredEvent{}, false, errors.New("stored runtime event attempt belongs to another Run")
	}
	return runtimeStoredEvent{
		id:                 event.ID,
		sequence:           event.Sequence,
		clientEventSeq:     *event.ClientEventSeq,
		payloadFingerprint: event.PayloadFingerprint,
		attemptID:          *event.AttemptID,
		attemptNo:          *event.AttemptNo,
		fencingToken:       *event.FencingToken,
		createdAt:          event.CreatedAt,
		attempt:            runtimeEventAttemptFromDB(attempt),
	}, true, nil
}

func runtimeEventAttemptFromDB(attempt db.RunAttempt) runtimeEventAttempt {
	return runtimeEventAttempt{
		id:                attempt.ID,
		runID:             attempt.RunID,
		agentID:           attempt.AgentID,
		attemptNo:         attempt.AttemptNo,
		executorType:      attempt.ExecutorType,
		leaseID:           attempt.LeaseID,
		fencingToken:      attempt.FencingToken,
		workerID:          attempt.RuntimeWorkerID,
		runtimeSessionID:  attempt.RuntimeSessionID,
		nodeID:            attempt.NodeID,
		acceptedAt:        attempt.AcceptedAt,
		leaseExpiresAt:    attempt.LeaseExpiresAt,
		attemptDeadlineAt: attempt.AttemptDeadlineAt,
		finishedAt:        attempt.FinishedAt,
		resultID:          attempt.ResultID,
	}
}

func storedRuntimeEventMatches(
	stored runtimeStoredEvent,
	identity RuntimeAttemptIdentity,
	clientEventSeq int64,
	fingerprint []byte,
) bool {
	if stored.clientEventSeq != clientEventSeq ||
		stored.attemptID != identity.AttemptID ||
		stored.fencingToken != identity.FencingToken ||
		stored.attempt.attemptNo == nil ||
		stored.attemptNo != *stored.attempt.attemptNo {
		return false
	}
	if subtle.ConstantTimeCompare(stored.payloadFingerprint, fingerprint) != 1 {
		return false
	}
	wantIdentity := runtimeAttemptIdentityDigest(identity)
	storedIdentity := runtimeAttemptIdentityDigest(RuntimeAttemptIdentity{
		RunID:            stored.attempt.runID,
		AttemptID:        stored.attempt.id,
		LeaseID:          stored.attempt.leaseID,
		FencingToken:     stored.attempt.fencingToken,
		NodeID:           stored.attempt.nodeID,
		AgentID:          stored.attempt.agentID,
		WorkerID:         stored.attempt.workerID,
		RuntimeSessionID: stored.attempt.runtimeSessionID,
	})
	return subtle.ConstantTimeCompare(wantIdentity[:], storedIdentity[:]) == 1
}

func runtimeAttemptIdentityDigest(identity RuntimeAttemptIdentity) [32]byte {
	value := map[string]any{
		"run_id":        identity.RunID.String(),
		"attempt_id":    identity.AttemptID.String(),
		"lease_id":      identity.LeaseID.String(),
		"fencing_token": identity.FencingToken,
		"agent_id":      identity.AgentID.String(),
	}
	if identity.NodeID != nil {
		value["node_id"] = identity.NodeID.String()
	}
	if identity.WorkerID != nil {
		value["worker_id"] = *identity.WorkerID
	}
	if identity.RuntimeSessionID != nil {
		value["runtime_session_id"] = identity.RuntimeSessionID.String()
	}
	canonical, err := CanonicalizeRFC8785(value)
	if err != nil {
		// All values were validated scalars; retaining an all-zero digest makes
		// an impossible internal encoding failure reject rather than authorize.
		return [32]byte{}
	}
	return sha256.Sum256(canonical)
}

func runtimeIdentityMatchesAttempt(identity RuntimeAttemptIdentity, attempt runtimeEventAttempt) bool {
	if identity.RunID != attempt.runID || identity.AttemptID != attempt.id ||
		identity.LeaseID != attempt.leaseID || identity.FencingToken != attempt.fencingToken ||
		identity.AgentID != attempt.agentID {
		return false
	}
	return optionalUUIDEqual(identity.NodeID, attempt.nodeID) &&
		optionalUUIDEqual(identity.RuntimeSessionID, attempt.runtimeSessionID) &&
		optionalStringEqual(identity.WorkerID, attempt.workerID) &&
		runtimeExecutorIdentityShapeValid(attempt.executorType, identity.NodeID, identity.RuntimeSessionID, identity.WorkerID)
}

func runtimePrincipalMatchesAttempt(principal RuntimeEventPrincipal, attempt runtimeEventAttempt) bool {
	if principal.AgentID != attempt.agentID {
		return false
	}
	return optionalUUIDEqual(principal.NodeID, attempt.nodeID) &&
		optionalUUIDEqual(principal.RuntimeSessionID, attempt.runtimeSessionID) &&
		optionalStringEqual(principal.WorkerID, attempt.workerID) &&
		runtimeExecutorIdentityShapeValid(attempt.executorType, principal.NodeID, principal.RuntimeSessionID, principal.WorkerID)
}

func runtimeExecutorIdentityShapeValid(executorType string, nodeID, sessionID *uuid.UUID, workerID *string) bool {
	switch executorType {
	case "agent_node":
		return nodeID != nil && *nodeID != uuid.Nil && sessionID != nil && *sessionID != uuid.Nil &&
			workerID != nil && *workerID != ""
	case "core_http", "core_mcp":
		return nodeID == nil && sessionID == nil && workerID == nil
	default:
		return false
	}
}

func runtimeEventDeadlinesValid(databaseNow, runDeadlineAt, leaseExpiresAt, attemptDeadlineAt time.Time) bool {
	return databaseNow.Before(leaseExpiresAt) &&
		databaseNow.Before(attemptDeadlineAt) &&
		databaseNow.Before(runDeadlineAt)
}

func validateRuntimeAttemptIdentity(identity RuntimeAttemptIdentity) error {
	if identity.RunID == uuid.Nil || identity.AttemptID == uuid.Nil || identity.LeaseID == uuid.Nil ||
		identity.AgentID == uuid.Nil || identity.FencingToken < 1 {
		return ErrInvalidRuntimeEvent
	}
	if !optionalRuntimeExecutorIdentityValid(identity.NodeID, identity.RuntimeSessionID, identity.WorkerID) {
		return ErrInvalidRuntimeEvent
	}
	return nil
}

func validateRuntimeEventPrincipal(principal RuntimeEventPrincipal) error {
	if principal.AgentID == uuid.Nil ||
		!optionalRuntimeExecutorIdentityValid(principal.NodeID, principal.RuntimeSessionID, principal.WorkerID) {
		return ErrInvalidRuntimeEvent
	}
	return nil
}

func optionalRuntimeExecutorIdentityValid(nodeID, sessionID *uuid.UUID, workerID *string) bool {
	none := nodeID == nil && sessionID == nil && workerID == nil
	all := nodeID != nil && *nodeID != uuid.Nil && sessionID != nil && *sessionID != uuid.Nil &&
		workerID != nil && *workerID != "" && utf8.ValidString(*workerID) && utf8.RuneCountInString(*workerID) <= 200
	return none || all
}

func validateRuntimeEventRequest(request RuntimeEventRequest) error {
	if request.ClientEventID == uuid.Nil || request.ClientEventSeq < 1 ||
		!runtimeEventTypePattern.MatchString(request.EventType) || request.Payload == nil {
		return ErrInvalidRuntimeEvent
	}
	if _, coreOwned := coreOwnedRuntimeEventTypes[request.EventType]; coreOwned {
		return ErrInvalidRuntimeEvent
	}
	return nil
}

func canonicalRuntimeEventPayload(request RuntimeEventRequest) ([]byte, error) {
	if err := validateRuntimeEventRequest(request); err != nil {
		return nil, err
	}
	payload, err := CanonicalizeRFC8785(request.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: canonical payload: %v", ErrInvalidRuntimeEvent, err)
	}
	if len(payload) > MaxRuntimeEventPayloadBytes {
		return nil, fmt.Errorf("%w: canonical payload exceeds %d bytes", ErrInvalidRuntimeEvent, MaxRuntimeEventPayloadBytes)
	}
	return payload, nil
}

func optionalUUIDEqual(left, right *uuid.UUID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func optionalStringEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
