package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	// RuntimeResultFingerprintVersion makes the immutable Result fingerprint
	// domain explicit. Changing it is a breaking runtime protocol change.
	RuntimeResultFingerprintVersion = "openlinker.runtime-result.v1"

	maxRuntimeResultPayloadBytes = 4 * 1024 * 1024
	maxRuntimeResultErrorCodeLen = 120
	maxRuntimeResultErrorTextLen = 500
	externalRunEffectMaxAttempts = 3
	internalRunEffectMaxAttempts = 12
)

// RuntimeResultClassification is Core's persisted interpretation of a Result.
// Timeout and dead_letter are ACK classifications derived from the final Run
// state; only the first three values are written to run_attempts.
type RuntimeResultClassification string

const (
	RuntimeResultClassificationSuccess      RuntimeResultClassification = "success"
	RuntimeResultClassificationRetryable    RuntimeResultClassification = "retryable_failure"
	RuntimeResultClassificationNonRetryable RuntimeResultClassification = "non_retryable_failure"
	RuntimeResultClassificationTimeout      RuntimeResultClassification = "timeout"
	RuntimeResultClassificationCanceled     RuntimeResultClassification = "canceled"
	RuntimeResultClassificationDeadLetter   RuntimeResultClassification = "dead_letter"
)

// RuntimeResultFailure is the normalized failed Result body. RetryableHint is
// advisory input; the server-side ResultClassifier remains authoritative.
type RuntimeResultFailure struct {
	ErrorCode     string `json:"error_code"`
	Message       string `json:"message"`
	RetryableHint bool   `json:"retryable_hint,omitempty"`
}

// RuntimeResultRequest is the transport-neutral runtime v2 Result payload.
// The envelope message ID and transport correlation fields intentionally live
// outside this type and therefore outside the semantic fingerprint.
type RuntimeResultRequest struct {
	AttemptIdentity     RuntimeAttemptIdentity `json:"attempt_identity"`
	ResultID            uuid.UUID              `json:"result_id"`
	Status              string                 `json:"status"`
	Output              map[string]any         `json:"output,omitempty"`
	Error               *RuntimeResultFailure  `json:"error,omitempty"`
	DurationMS          int32                  `json:"duration_ms"`
	FinalClientEventSeq int64                  `json:"final_client_event_seq"`
}

// RuntimeResultPrincipal is an already-authenticated execution principal.
// Authentication and revocation precede envelope decoding in the Task 6
// transport. It aliases the event principal so Event and Result use identical
// Node/Agent/worker/session ownership semantics.
type RuntimeResultPrincipal = RuntimeEventPrincipal

// RuntimeResultAck is durable business acknowledgement. A caller may delete
// its Result spool only after receiving this ACK.
type RuntimeResultAck struct {
	ResultID       uuid.UUID                   `json:"result_id"`
	Classification RuntimeResultClassification `json:"classification"`
	RunStatus      string                      `json:"run_status"`
	DispatchState  string                      `json:"dispatch_state"`
	Replayed       bool                        `json:"replayed"`
	NextAttemptAt  *time.Time                  `json:"next_attempt_at,omitempty"`
}

// RuntimeResultErrorCode is stable across HTTP, WebSocket, gRPC, and MCP
// adapters. Human-readable transport text must never be parsed for behavior.
type RuntimeResultErrorCode string

const (
	RuntimeResultErrorValidationFailed      RuntimeResultErrorCode = "VALIDATION_FAILED"
	RuntimeResultErrorResultIDConflict      RuntimeResultErrorCode = "RESULT_ID_CONFLICT"
	RuntimeResultErrorRunAlreadyTerminal    RuntimeResultErrorCode = "RUN_ALREADY_TERMINAL"
	RuntimeResultErrorStaleLease            RuntimeResultErrorCode = "STALE_LEASE"
	RuntimeResultErrorLeaseExpired          RuntimeResultErrorCode = "LEASE_EXPIRED"
	RuntimeResultErrorLeaseIdentityMismatch RuntimeResultErrorCode = "LEASE_IDENTITY_MISMATCH"
	RuntimeResultErrorEventsMissing         RuntimeResultErrorCode = "EVENTS_MISSING"
	RuntimeResultErrorRunCancelRequested    RuntimeResultErrorCode = "RUN_CANCEL_REQUESTED"
)

// RuntimeResultError carries a stable code and exact inclusive Event gaps.
type RuntimeResultError struct {
	Code          RuntimeResultErrorCode `json:"code"`
	MissingRanges []EventRange           `json:"missing_ranges,omitempty"`
	cause         error
}

func (e *RuntimeResultError) Error() string {
	if e == nil {
		return ""
	}
	switch e.Code {
	case RuntimeResultErrorValidationFailed:
		return "runtime result validation failed"
	case RuntimeResultErrorResultIDConflict:
		return "runtime result identity conflicts with a stored result"
	case RuntimeResultErrorRunAlreadyTerminal:
		return "run is already terminal"
	case RuntimeResultErrorStaleLease:
		return "runtime lease is stale"
	case RuntimeResultErrorLeaseExpired:
		return "runtime lease or attempt deadline has expired"
	case RuntimeResultErrorLeaseIdentityMismatch:
		return "authenticated runtime identity does not own the lease"
	case RuntimeResultErrorEventsMissing:
		return "runtime events are missing"
	case RuntimeResultErrorRunCancelRequested:
		return "run cancellation has been requested"
	default:
		return "runtime result rejected"
	}
}

func (e *RuntimeResultError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// IsRuntimeResultError reports whether err carries the requested stable code.
func IsRuntimeResultError(err error, code RuntimeResultErrorCode) bool {
	var resultErr *RuntimeResultError
	return errors.As(err, &resultErr) && resultErr.Code == code
}

func newRuntimeResultError(code RuntimeResultErrorCode, cause error) error {
	return &RuntimeResultError{Code: code, cause: cause}
}

func missingRuntimeResultEvents(ranges []EventRange) error {
	return &RuntimeResultError{
		Code:          RuntimeResultErrorEventsMissing,
		MissingRanges: append([]EventRange(nil), ranges...),
	}
}

// RuntimeResultFingerprint computes the Core-owned RFC 8785 SHA-256 digest.
// ResultID, Attempt identity, and the transport envelope are excluded; those
// identities are compared separately under the Run lock.
func RuntimeResultFingerprint(request RuntimeResultRequest) ([]byte, error) {
	value, err := canonicalRuntimeResultValue(request)
	if err != nil {
		return nil, err
	}
	canonical, err := CanonicalizeRFC8785(value)
	if err != nil {
		return nil, newRuntimeResultError(RuntimeResultErrorValidationFailed, err)
	}
	if len(canonical) > maxRuntimeResultPayloadBytes {
		return nil, newRuntimeResultError(RuntimeResultErrorValidationFailed, errors.New("result exceeds runtime message limit"))
	}
	digest := sha256.Sum256(canonical)
	return digest[:], nil
}

func canonicalRuntimeResultValue(request RuntimeResultRequest) (map[string]any, error) {
	if err := validateRuntimeResultRequest(request); err != nil {
		return nil, err
	}
	value := map[string]any{
		"duration_ms":            request.DurationMS,
		"final_client_event_seq": request.FinalClientEventSeq,
		"schema":                 RuntimeResultFingerprintVersion,
		"status":                 request.Status,
	}
	if request.Status == "success" {
		value["output"] = request.Output
	} else {
		value["error"] = map[string]any{
			"error_code":     request.Error.ErrorCode,
			"message":        request.Error.Message,
			"retryable_hint": request.Error.RetryableHint,
		}
	}
	return value, nil
}

func validateRuntimeResultRequest(request RuntimeResultRequest) error {
	if request.ResultID == uuid.Nil || request.DurationMS < 0 ||
		request.FinalClientEventSeq < 0 || request.FinalClientEventSeq == math.MaxInt64 {
		return newRuntimeResultError(RuntimeResultErrorValidationFailed, nil)
	}
	if err := validateRuntimeAttemptIdentity(request.AttemptIdentity); err != nil {
		return newRuntimeResultError(RuntimeResultErrorValidationFailed, err)
	}
	switch request.Status {
	case "success":
		if request.Output == nil || request.Error != nil {
			return newRuntimeResultError(RuntimeResultErrorValidationFailed, nil)
		}
	case "failed":
		if request.Output != nil || request.Error == nil || !validRuntimeResultFailure(*request.Error) {
			return newRuntimeResultError(RuntimeResultErrorValidationFailed, nil)
		}
	default:
		return newRuntimeResultError(RuntimeResultErrorValidationFailed, nil)
	}
	return nil
}

func validRuntimeResultFailure(failure RuntimeResultFailure) bool {
	if !utf8.ValidString(failure.ErrorCode) || !utf8.ValidString(failure.Message) ||
		strings.TrimSpace(failure.ErrorCode) == "" || strings.TrimSpace(failure.Message) == "" {
		return false
	}
	return utf8.RuneCountInString(failure.ErrorCode) <= maxRuntimeResultErrorCodeLen &&
		utf8.RuneCountInString(failure.Message) <= maxRuntimeResultErrorTextLen
}

// ResultClassificationInput is the immutable server policy snapshot supplied
// to ResultClassifier. The caller-provided retryable hint is never sufficient
// for Core-owned HTTP/MCP attempts without endpoint idempotency evidence.
type ResultClassificationInput struct {
	ExecutorType        string
	EndpointIdempotency bool
	Request             RuntimeResultRequest
}

// ResultClassifier owns retry policy. Implementations must return one of the
// three persisted Attempt classifications.
type ResultClassifier interface {
	ClassifyResult(ResultClassificationInput) RuntimeResultClassification
}

// ResultClassifierFunc adapts a function for focused policy tests.
type ResultClassifierFunc func(ResultClassificationInput) RuntimeResultClassification

func (fn ResultClassifierFunc) ClassifyResult(input ResultClassificationInput) RuntimeResultClassification {
	return fn(input)
}

type defaultResultClassifier struct{}

func (defaultResultClassifier) ClassifyResult(input ResultClassificationInput) RuntimeResultClassification {
	if input.Request.Status == "success" {
		return RuntimeResultClassificationSuccess
	}
	if input.Request.Error == nil || !input.Request.Error.RetryableHint {
		return RuntimeResultClassificationNonRetryable
	}
	switch input.ExecutorType {
	case "agent_node":
		return RuntimeResultClassificationRetryable
	case "core_http", "core_mcp":
		if input.EndpointIdempotency {
			return RuntimeResultClassificationRetryable
		}
	}
	return RuntimeResultClassificationNonRetryable
}

// ResultRetryPlanner persists a single delay for an accepted retryable Result.
// Task 7 replaces the deterministic placeholder with the approved jittered
// policy without changing Finalizer's atomic boundary.
type ResultRetryPlanner interface {
	NextRetryDelay(attemptNo int32) time.Duration
}

// ResultRetryPlannerFunc adapts a function for focused tests.
type ResultRetryPlannerFunc func(int32) time.Duration

func (fn ResultRetryPlannerFunc) NextRetryDelay(attemptNo int32) time.Duration {
	return fn(attemptNo)
}

type fixedResultRetryPlanner struct{}

func (fixedResultRetryPlanner) NextRetryDelay(attemptNo int32) time.Duration {
	if attemptNo < 1 {
		attemptNo = 1
	}
	shift := attemptNo - 1
	if shift > 6 {
		shift = 6
	}
	delay := time.Second * time.Duration(1<<shift)
	if delay > 60*time.Second {
		return 60 * time.Second
	}
	return delay
}

// ResultFinalizer is the sole atomic write path for an accepted runtime v2
// Result. Public transports are intentionally wired in Task 6.
type ResultFinalizer struct {
	pool         *pgxpool.Pool
	classifier   ResultClassifier
	retryPlanner ResultRetryPlanner
}

// NewResultFinalizer builds an isolated Result finalizer. Nil policies select
// conservative server defaults.
func NewResultFinalizer(pool *pgxpool.Pool, classifier ResultClassifier, retryPlanner ResultRetryPlanner) *ResultFinalizer {
	if classifier == nil {
		classifier = defaultResultClassifier{}
	}
	if retryPlanner == nil {
		retryPlanner = fixedResultRetryPlanner{}
	}
	return &ResultFinalizer{pool: pool, classifier: classifier, retryPlanner: retryPlanner}
}

var errNilResultFinalizer = errors.New("runtime result finalizer is not configured")

// Finalize validates, fingerprints, and atomically persists a Result. The
// implementation uses PostgreSQL's Run row as the per-Run linearization point.
func (f *ResultFinalizer) Finalize(
	ctx context.Context,
	principal RuntimeResultPrincipal,
	request RuntimeResultRequest,
) (RuntimeResultAck, error) {
	if f == nil || f.pool == nil {
		return RuntimeResultAck{}, errNilResultFinalizer
	}
	if err := validateRuntimeEventPrincipal(principal); err != nil {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorValidationFailed, err)
	}
	fingerprint, err := RuntimeResultFingerprint(request)
	if err != nil {
		return RuntimeResultAck{}, err
	}

	var ack RuntimeResultAck
	err = pgx.BeginTxFunc(ctx, f.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		var finalizeErr error
		ack, finalizeErr = f.finalizeTx(ctx, db.New(tx), principal, request, fingerprint)
		return finalizeErr
	})
	if err != nil {
		return RuntimeResultAck{}, err
	}
	return ack, nil
}

// finalizeTx uses the generated Task 5 query layer. Keeping it separate makes
// the transaction boundary explicit and directly testable.
func (f *ResultFinalizer) finalizeTx(
	ctx context.Context,
	queries *db.Queries,
	principal RuntimeResultPrincipal,
	request RuntimeResultRequest,
	fingerprint []byte,
) (RuntimeResultAck, error) {
	lockedPrincipal, err := lockRuntimePrincipal(ctx, queries, principal)
	if err != nil {
		return RuntimeResultAck{}, mapRuntimePrincipalErrorToResult(err)
	}
	locked, err := queries.LockRunForResultFinalization(ctx, request.AttemptIdentity.RunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorStaleLease, err)
	}
	if err != nil {
		return RuntimeResultAck{}, err
	}
	run, err := runtimeResultRunFromLocked(locked)
	if err != nil {
		return RuntimeResultAck{}, err
	}
	if principal.AgentID != run.agentID || request.AttemptIdentity.AgentID != run.agentID {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorLeaseIdentityMismatch, nil)
	}
	if !lockedPrincipal.validAt(run.databaseNow) {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorLeaseIdentityMismatch, nil)
	}

	attempt, err := queries.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
		RunID: run.id,
		ID:    request.AttemptIdentity.AttemptID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorStaleLease, err)
	}
	if err != nil {
		return RuntimeResultAck{}, err
	}
	if !runtimeResultIdentityMatchesAttempt(request.AttemptIdentity, attempt) {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorLeaseIdentityMismatch, nil)
	}

	// Stored Result replay deliberately precedes terminal, lease, and deadline
	// checks. Ownership and the full immutable identity were already verified.
	if attempt.ResultID != nil {
		if *attempt.ResultID != request.ResultID ||
			!runtimeResultFingerprintsEqual(attempt.ResultFingerprint, fingerprint) {
			return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorResultIDConflict, nil)
		}
		if _, err := authorizeRuntimeAttemptPrincipal(
			ctx, queries, principal, request.AttemptIdentity, runtimeEventAttemptFromDB(attempt), true,
		); err != nil {
			return RuntimeResultAck{}, mapRuntimePrincipalErrorToResult(err)
		}
		return runtimeResultAckFromStored(run, attempt, true)
	}

	otherAttempt, otherErr := queries.GetRunAttemptByResultID(ctx, db.GetRunAttemptByResultIDParams{
		RunID:    run.id,
		ResultID: request.ResultID,
	})
	if otherErr == nil {
		_ = otherAttempt
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorResultIDConflict, nil)
	}
	if !errors.Is(otherErr, pgx.ErrNoRows) {
		return RuntimeResultAck{}, otherErr
	}

	if run.status != "running" || run.dispatchState == "terminal" || run.dispatchState == "dead_letter" {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorRunAlreadyTerminal, nil)
	}
	if run.cancelRequestID != nil {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorRunCancelRequested, nil)
	}
	if !runtimeResultActiveAttemptMatches(run, request.AttemptIdentity, attempt) {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorStaleLease, nil)
	}
	authorization, err := authorizeRuntimeAttemptPrincipal(
		ctx, queries, principal, request.AttemptIdentity, runtimeEventAttemptFromDB(attempt), false,
	)
	if err != nil {
		return RuntimeResultAck{}, mapRuntimePrincipalErrorToResult(err)
	}

	deadlineExceeded := !run.databaseNow.Before(run.runDeadlineAt)
	if request.FinalClientEventSeq < attempt.LastClientEventSeq {
		return RuntimeResultAck{}, newRuntimeResultError(
			RuntimeResultErrorValidationFailed,
			fmt.Errorf("final_client_event_seq %d precedes persisted sequence %d", request.FinalClientEventSeq, attempt.LastClientEventSeq),
		)
	}
	if !deadlineExceeded {
		if (!authorization.resumed && !run.databaseNow.Before(attempt.LeaseExpiresAt)) ||
			!run.databaseNow.Before(attempt.AttemptDeadlineAt) {
			return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorLeaseExpired, nil)
		}
		sequences, sequenceErr := queries.ListClientEventSequencesThrough(ctx, db.ListClientEventSequencesThroughParams{
			RunID:           run.id,
			AttemptID:       attempt.ID,
			AttemptNo:       *attempt.AttemptNo,
			ThroughSequence: request.FinalClientEventSeq,
		})
		if sequenceErr != nil {
			return RuntimeResultAck{}, sequenceErr
		}
		if ranges := missingRuntimeEventRanges(sequences, request.FinalClientEventSeq); len(ranges) > 0 {
			return RuntimeResultAck{}, missingRuntimeResultEvents(ranges)
		}
	}

	endpointIdempotency := run.endpointIdempotency != nil && *run.endpointIdempotency
	classification := f.classifier.ClassifyResult(ResultClassificationInput{
		ExecutorType:        attempt.ExecutorType,
		EndpointIdempotency: endpointIdempotency,
		Request:             request,
	})
	if err := validateRuntimeResultClassification(request, classification); err != nil {
		return RuntimeResultAck{}, err
	}
	retryExhausted := classification == RuntimeResultClassificationRetryable && run.attemptCount >= run.maxAttempts
	runStatus, dispatchState, attemptOutcome, eventType, publicErrorCode, ackClassification :=
		runtimeResultTerminalSemantics(request, classification, deadlineExceeded, retryExhausted)

	var attemptErrorCode, attemptErrorDetail *string
	if request.Error != nil {
		code := request.Error.ErrorCode
		detail := request.Error.Message
		attemptErrorCode = &code
		attemptErrorDetail = &detail
	} else if deadlineExceeded {
		code := "RUN_DEADLINE_EXCEEDED"
		attemptErrorCode = &code
	}
	finishedAttempt, err := queries.FinishRunAttempt(ctx, db.FinishRunAttemptParams{
		RunID:                run.id,
		ID:                   attempt.ID,
		LeaseID:              attempt.LeaseID,
		FencingToken:         attempt.FencingToken,
		Outcome:              attemptOutcome,
		ResultID:             request.ResultID,
		ResultFingerprint:    fingerprint,
		ResultClassification: string(classification),
		FinalClientEventSeq:  request.FinalClientEventSeq,
		ErrorCode:            attemptErrorCode,
		ErrorDetailRedacted:  attemptErrorDetail,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorStaleLease, err)
	}
	if err != nil {
		return RuntimeResultAck{}, err
	}
	if err := releaseRuntimeAttemptCapacity(ctx, queries, attempt); err != nil {
		return RuntimeResultAck{}, err
	}

	if classification == RuntimeResultClassificationRetryable && !deadlineExceeded && !retryExhausted {
		delay := f.retryPlanner.NextRetryDelay(run.attemptCount)
		if delay <= 0 || delay > 60*time.Second {
			return RuntimeResultAck{}, fmt.Errorf("result retry planner returned invalid delay %s", delay)
		}
		transitioned, transitionErr := queries.TransitionRunToRetryWait(ctx, db.TransitionRunToRetryWaitParams{
			RunID:        run.id,
			AttemptID:    attempt.ID,
			RetryAfterMs: delay.Milliseconds(),
		})
		if errors.Is(transitionErr, pgx.ErrNoRows) {
			return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorStaleLease, transitionErr)
		}
		if transitionErr != nil {
			return RuntimeResultAck{}, transitionErr
		}
		return RuntimeResultAck{
			ResultID:       request.ResultID,
			Classification: RuntimeResultClassificationRetryable,
			RunStatus:      transitioned.Status,
			DispatchState:  transitioned.DispatchState,
			Replayed:       false,
			NextAttemptAt:  transitioned.NextAttemptAt,
		}, nil
	}

	terminalEventID := deterministicTerminalEventID(run.id, runStatus)
	effectPlan, err := discoverRuntimeResultEffects(ctx, queries, run, terminalEventID, eventType)
	if err != nil {
		return RuntimeResultAck{}, err
	}
	terminalPayload, err := marshalRuntimeResultJSON(terminalRuntimeResultPayload(
		request, runStatus, ackClassification, publicErrorCode,
	))
	if err != nil {
		return RuntimeResultAck{}, err
	}
	if err := queries.LockRunEventSequence(ctx, run.id); err != nil {
		return RuntimeResultAck{}, err
	}
	terminalEvent, err := queries.InsertTerminalRunEvent(ctx, db.InsertTerminalRunEventParams{
		ID:          terminalEventID,
		RunID:       run.id,
		ParentRunID: effectPlan.parentRunID,
		EventType:   eventType,
		Payload:     terminalPayload,
	})
	if err != nil {
		return RuntimeResultAck{}, err
	}

	if retryExhausted && !deadlineExceeded {
		if finishedAttempt.AttemptNo == nil {
			return RuntimeResultAck{}, errors.New("finished retryable attempt has no attempt number")
		}
		if err := ensureRuntimeResultDeadLetter(ctx, queries, run.id, *finishedAttempt.AttemptNo, attemptErrorCode); err != nil {
			return RuntimeResultAck{}, err
		}
	}

	var output []byte
	var publicErrorMessage *string
	var resultID *uuid.UUID
	var resultFingerprint []byte
	if runStatus == "success" {
		output, err = marshalRuntimeResultJSON(request.Output)
		if err != nil {
			return RuntimeResultAck{}, err
		}
		resultID = &request.ResultID
		resultFingerprint = fingerprint
	} else if runStatus == "timeout" {
		message := "Run deadline exceeded"
		publicErrorMessage = &message
	} else {
		if retryExhausted {
			message := "Runtime retry budget exhausted"
			publicErrorMessage = &message
		} else {
			message := request.Error.Message
			publicErrorMessage = &message
		}
		resultID = &request.ResultID
		resultFingerprint = fingerprint
	}
	var publicErrorCodePtr *string
	if publicErrorCode != "" {
		code := publicErrorCode
		publicErrorCodePtr = &code
	}
	finalized, err := queries.FinalizeRunFromResult(ctx, db.FinalizeRunFromResultParams{
		RunID:             run.id,
		AttemptID:         attempt.ID,
		Status:            runStatus,
		DispatchState:     dispatchState,
		Output:            output,
		ErrorCode:         publicErrorCodePtr,
		ErrorMessage:      publicErrorMessage,
		ResultID:          resultID,
		ResultFingerprint: resultFingerprint,
		DurationMs:        request.DurationMS,
		TerminalEventID:   terminalEvent.ID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return RuntimeResultAck{}, newRuntimeResultError(RuntimeResultErrorStaleLease, err)
	}
	if err != nil {
		return RuntimeResultAck{}, err
	}

	successDelta := int32(0)
	revenueDelta := int64(0)
	if runStatus == "success" {
		successDelta = 1
		revenueDelta = int64(run.creatorRevenueCents)
	}
	if err := insertRuntimeResultLedgerAndStats(
		ctx, queries, run, terminalEvent.ID, successDelta, revenueDelta,
	); err != nil {
		return RuntimeResultAck{}, err
	}
	for _, effect := range effectPlan.effects {
		if err := ensureRuntimeResultEffect(ctx, queries, run.id, terminalEvent.ID, effect); err != nil {
			return RuntimeResultAck{}, err
		}
	}

	return RuntimeResultAck{
		ResultID:       request.ResultID,
		Classification: ackClassification,
		RunStatus:      finalized.Status,
		DispatchState:  finalized.DispatchState,
		Replayed:       false,
		NextAttemptAt:  finalized.NextAttemptAt,
	}, nil
}

var terminalEventNamespace = uuid.MustParse("b61ef958-9798-5f8f-998b-9d9f5de7c2e8")

var runEffectNamespace = uuid.MustParse("27a4451d-c6f6-5636-87eb-a020f9d44a63")

const (
	runtimeResultEffectAgentWebhook     = "run.agent_webhook"
	runtimeResultEffectTaskCallback     = "run.task_callback"
	runtimeResultEffectDefaultDelivery  = "run.default_delivery"
	runtimeResultEffectParentCompletion = "run.parent_completion"
)

func deterministicTerminalEventID(runID uuid.UUID, status string) uuid.UUID {
	return uuid.NewSHA1(terminalEventNamespace, []byte(runID.String()+"/"+status))
}

func deterministicRunEffectID(runID uuid.UUID, effectType, targetKey string) uuid.UUID {
	name := runID.String() + "/" + effectType + "/" + targetKey
	return uuid.NewSHA1(runEffectNamespace, []byte(name))
}

type runtimeResultRun struct {
	id                  uuid.UUID
	userID              uuid.UUID
	agentID             uuid.UUID
	status              string
	dispatchState       string
	connectionMode      *string
	endpointIdempotency *bool
	attemptCount        int32
	maxAttempts         int32
	latestAttemptID     *uuid.UUID
	activeAttemptID     *uuid.UUID
	leaseID             *uuid.UUID
	fencingToken        int64
	runtimeNodeID       *uuid.UUID
	runtimeWorkerID     *string
	runtimeSessionID    *uuid.UUID
	runDeadlineAt       time.Time
	nextAttemptAt       *time.Time
	cancelRequestID     *uuid.UUID
	creatorRevenueCents int32
	databaseNow         time.Time
}

type runtimeResultEffect struct {
	id          uuid.UUID
	effectType  string
	targetKey   string
	metadata    []byte
	maxAttempts int32
}

func validateRuntimeResultClassification(
	request RuntimeResultRequest,
	classification RuntimeResultClassification,
) error {
	if request.Status == "success" && classification == RuntimeResultClassificationSuccess {
		return nil
	}
	if request.Status == "failed" &&
		(classification == RuntimeResultClassificationRetryable ||
			classification == RuntimeResultClassificationNonRetryable) {
		return nil
	}
	return fmt.Errorf("result classifier returned invalid classification %q for status %q", classification, request.Status)
}

func runtimeResultIdentityMatchesAttempt(identity RuntimeAttemptIdentity, attempt db.RunAttempt) bool {
	storedIdentity := RuntimeAttemptIdentity{
		RunID:            attempt.RunID,
		AttemptID:        attempt.ID,
		LeaseID:          attempt.LeaseID,
		FencingToken:     attempt.FencingToken,
		NodeID:           attempt.NodeID,
		AgentID:          attempt.AgentID,
		WorkerID:         attempt.RuntimeWorkerID,
		RuntimeSessionID: attempt.RuntimeSessionID,
	}
	wantDigest := runtimeAttemptIdentityDigest(identity)
	storedDigest := runtimeAttemptIdentityDigest(storedIdentity)
	return subtle.ConstantTimeCompare(wantDigest[:], storedDigest[:]) == 1
}

func mapRuntimePrincipalErrorToResult(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrInvalidRuntimeEvent) {
		return newRuntimeResultError(RuntimeResultErrorValidationFailed, err)
	}
	var eventErr *RuntimeEventError
	if errors.As(err, &eventErr) {
		switch eventErr.Code {
		case RuntimeEventErrorLeaseIdentityMismatch:
			return newRuntimeResultError(RuntimeResultErrorLeaseIdentityMismatch, err)
		case RuntimeEventErrorLeaseExpired:
			return newRuntimeResultError(RuntimeResultErrorLeaseExpired, err)
		case RuntimeEventErrorStaleLease:
			return newRuntimeResultError(RuntimeResultErrorStaleLease, err)
		}
	}
	return err
}

func releaseRuntimeAttemptCapacity(ctx context.Context, queries *db.Queries, attempt db.RunAttempt) error {
	if attempt.ExecutorType != "agent_node" {
		return nil
	}
	if attempt.SlotAcquiredAt == nil || attempt.ActiveRuntimeSessionID == nil || attempt.NodeID == nil {
		return errors.New("agent_node Attempt is missing capacity identity")
	}

	// The Attempt row owns the capacity slot. Marking it released is the only
	// operation that authorizes counter decrements, so retries and competing
	// terminal paths cannot release the same slot twice.
	capacity, err := queries.MarkRunAttemptCapacityReleased(ctx, db.MarkRunAttemptCapacityReleasedParams{
		RunID:        attempt.RunID,
		AttemptID:    attempt.ID,
		LeaseID:      attempt.LeaseID,
		FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("agent_node Attempt capacity was already released or changed")
	}
	if err != nil {
		return err
	}
	if capacity.RuntimeSessionID != *attempt.ActiveRuntimeSessionID || capacity.NodeID != *attempt.NodeID {
		return errors.New("agent_node Attempt capacity owner changed")
	}
	if _, err := queries.ReleaseRuntimeSessionSlot(ctx, capacity.RuntimeSessionID); err != nil {
		return err
	}
	if _, err := queries.ReleaseRuntimeNodeSlot(ctx, capacity.NodeID); err != nil {
		return err
	}
	return nil
}

func runtimeResultActiveAttemptMatches(run runtimeResultRun, identity RuntimeAttemptIdentity, attempt db.RunAttempt) bool {
	return run.status == "running" && run.dispatchState == "executing" &&
		run.activeAttemptID != nil && *run.activeAttemptID == attempt.ID &&
		run.latestAttemptID != nil && *run.latestAttemptID == attempt.ID &&
		run.leaseID != nil && *run.leaseID == identity.LeaseID &&
		run.fencingToken == identity.FencingToken &&
		attempt.RunID == run.id && attempt.AcceptedAt != nil && attempt.AttemptNo != nil &&
		attempt.FinishedAt == nil && attempt.ResultID == nil &&
		attempt.LeaseID == identity.LeaseID && attempt.FencingToken == identity.FencingToken &&
		optionalUUIDEqual(run.runtimeNodeID, identity.NodeID) &&
		optionalStringEqual(run.runtimeWorkerID, identity.WorkerID) &&
		optionalUUIDEqual(run.runtimeSessionID, identity.RuntimeSessionID) &&
		runtimeResultConnectionMatchesExecutor(run.connectionMode, attempt.ExecutorType)
}

func runtimeResultConnectionMatchesExecutor(connectionMode *string, executorType string) bool {
	if connectionMode == nil {
		return false
	}
	switch executorType {
	case "agent_node":
		return *connectionMode == connectionModeRuntimePull || *connectionMode == connectionModeRuntimeWS
	case "core_http":
		return *connectionMode == connectionModeDirectHTTP
	case "core_mcp":
		return *connectionMode == connectionModeMCPServer
	default:
		return false
	}
}

func runtimeResultAckFromStored(run runtimeResultRun, attempt db.RunAttempt, replayed bool) (RuntimeResultAck, error) {
	if attempt.ResultID == nil || attempt.ResultClassification == nil {
		return RuntimeResultAck{}, errors.New("stored runtime result is incomplete")
	}
	classification := RuntimeResultClassification(*attempt.ResultClassification)
	if run.dispatchState == "dead_letter" && run.latestAttemptID != nil && *run.latestAttemptID == attempt.ID {
		classification = RuntimeResultClassificationDeadLetter
	} else if run.status == "success" {
		classification = RuntimeResultClassificationSuccess
	} else if run.status == "failed" {
		classification = RuntimeResultClassificationNonRetryable
	} else if run.status == "timeout" {
		classification = RuntimeResultClassificationTimeout
	} else if run.status == "canceled" {
		classification = RuntimeResultClassificationCanceled
	} else if attempt.Outcome != nil && *attempt.Outcome == "timeout" {
		classification = RuntimeResultClassificationTimeout
	}
	return RuntimeResultAck{
		ResultID:       *attempt.ResultID,
		Classification: classification,
		RunStatus:      run.status,
		DispatchState:  run.dispatchState,
		Replayed:       replayed,
		NextAttemptAt:  run.nextAttemptAt,
	}, nil
}

func runtimeResultTerminalSemantics(
	request RuntimeResultRequest,
	classification RuntimeResultClassification,
	deadlineExceeded bool,
	retryExhausted bool,
) (runStatus, dispatchState, attemptOutcome, eventType, publicErrorCode string, ackClassification RuntimeResultClassification) {
	if deadlineExceeded {
		return "timeout", "terminal", "timeout", "run.failed", "RUN_DEADLINE_EXCEEDED", RuntimeResultClassificationTimeout
	}
	if retryExhausted {
		return "failed", "dead_letter", "retryable_failure", "run.failed", "RUNTIME_RETRY_EXHAUSTED", RuntimeResultClassificationDeadLetter
	}
	if classification == RuntimeResultClassificationSuccess {
		return "success", "terminal", "success", "run.completed", "", RuntimeResultClassificationSuccess
	}
	if classification == RuntimeResultClassificationRetryable {
		return "running", "retry_wait", "retryable_failure", "", "", RuntimeResultClassificationRetryable
	}
	return "failed", "terminal", "non_retryable_failure", "run.failed", request.Error.ErrorCode, RuntimeResultClassificationNonRetryable
}

func terminalRuntimeResultPayload(
	request RuntimeResultRequest,
	runStatus string,
	classification RuntimeResultClassification,
	publicErrorCode string,
) map[string]any {
	payload := map[string]any{
		"classification": classification,
		"duration_ms":    request.DurationMS,
		"status":         runStatus,
		"terminal":       true,
	}
	if classification != RuntimeResultClassificationTimeout {
		payload["result_id"] = request.ResultID.String()
	}
	if runStatus == "success" {
		payload["output"] = request.Output
	} else if runStatus == "timeout" {
		payload["error_message"] = "Run deadline exceeded"
	} else if request.Error != nil {
		payload["error_message"] = request.Error.Message
	}
	if publicErrorCode != "" {
		payload["error_code"] = publicErrorCode
	}
	return payload
}

func runtimeResultRunFromLocked(row db.LockRunForResultFinalizationRow) (runtimeResultRun, error) {
	if row.RunDeadlineAt == nil {
		return runtimeResultRun{}, errors.New("runtime v2 Run is missing run deadline")
	}
	return runtimeResultRun{
		id:                  row.ID,
		userID:              row.UserID,
		agentID:             row.AgentID,
		status:              row.Status,
		dispatchState:       row.DispatchState,
		connectionMode:      row.ConnectionModeSnapshot,
		endpointIdempotency: row.EndpointIdempotencySnapshot,
		attemptCount:        row.AttemptCount,
		maxAttempts:         row.MaxAttempts,
		latestAttemptID:     row.LatestAttemptID,
		activeAttemptID:     row.ActiveAttemptID,
		leaseID:             row.LeaseID,
		fencingToken:        row.FencingToken,
		runtimeNodeID:       row.RuntimeNodeID,
		runtimeWorkerID:     row.RuntimeWorkerID,
		runtimeSessionID:    row.RuntimeSessionID,
		runDeadlineAt:       *row.RunDeadlineAt,
		nextAttemptAt:       row.NextAttemptAt,
		cancelRequestID:     row.CancelRequestID,
		creatorRevenueCents: row.CreatorRevenueCents,
		databaseNow:         row.DatabaseNow,
	}, nil
}

func ensureRuntimeResultDeadLetter(
	ctx context.Context,
	queries *db.Queries,
	runID uuid.UUID,
	finalAttemptNo int32,
	attemptErrorCode *string,
) error {
	reasonCode := "RUNTIME_RETRY_EXHAUSTED"
	var reasonRedacted *string
	if attemptErrorCode != nil {
		redacted := *attemptErrorCode
		reasonRedacted = &redacted
	}
	created, err := queries.CreateRunDeadLetter(ctx, db.CreateRunDeadLetterParams{
		RunID:          runID,
		FinalAttemptNo: finalAttemptNo,
		ReasonCode:     reasonCode,
		ReasonRedacted: reasonRedacted,
	})
	if err == nil {
		if created.RunID != runID || created.FinalAttemptNo != finalAttemptNo || created.ReasonCode != reasonCode {
			return errors.New("created runtime dead letter is inconsistent")
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	existing, err := queries.GetRunDeadLetterByRun(ctx, runID)
	if err != nil {
		return err
	}
	if existing.FinalAttemptNo != finalAttemptNo || existing.ReasonCode != reasonCode ||
		!optionalStringEqual(existing.ReasonRedacted, reasonRedacted) {
		return errors.New("runtime dead letter identity conflict")
	}
	return nil
}

func insertRuntimeResultLedgerAndStats(
	ctx context.Context,
	queries *db.Queries,
	run runtimeResultRun,
	terminalEventID uuid.UUID,
	successDelta int32,
	revenueDelta int64,
) error {
	created, err := queries.InsertRunAccountingLedger(ctx, db.InsertRunAccountingLedgerParams{
		RunID:             run.id,
		TerminalEventID:   terminalEventID,
		AgentID:           run.agentID,
		SuccessDelta:      successDelta,
		RevenueDeltaCents: revenueDelta,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := queries.GetRunAccountingLedger(ctx, run.id)
		if getErr != nil {
			return getErr
		}
		if existing.TerminalEventID != terminalEventID || existing.AgentID != run.agentID ||
			existing.SuccessDelta != successDelta || existing.RevenueDeltaCents != revenueDelta {
			return errors.New("runtime accounting ledger identity conflict")
		}
		// The aggregate is advanced only by the transaction that inserted the
		// ledger row. A replay that observes it must not increment again.
		return nil
	}
	if err != nil {
		return err
	}
	if created.RunID != run.id || created.TerminalEventID != terminalEventID {
		return errors.New("created runtime accounting ledger is inconsistent")
	}
	if successDelta == 1 {
		if err := queries.IncrementAgentStats(ctx, db.IncrementAgentStatsParams{
			ID:           run.agentID,
			RevenueCents: revenueDelta,
		}); err != nil {
			return err
		}
		_, err = queries.MarkAgentAvailabilitySuccess(ctx, run.agentID)
		return err
	}
	_, err = queries.MarkAgentAvailabilityFailure(ctx, run.agentID)
	return err
}

func ensureRuntimeResultEffect(
	ctx context.Context,
	queries *db.Queries,
	runID, terminalEventID uuid.UUID,
	effect runtimeResultEffect,
) error {
	created, err := queries.CreateRunEffect(ctx, db.CreateRunEffectParams{
		ID:              effect.id,
		RunID:           runID,
		TerminalEventID: terminalEventID,
		EffectType:      effect.effectType,
		TargetKey:       effect.targetKey,
		Metadata:        effect.metadata,
		MaxAttempts:     effect.maxAttempts,
	})
	if err == nil {
		if !runtimeResultEffectMatches(created, runID, terminalEventID, effect) {
			return errors.New("created runtime effect is inconsistent")
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	existing, err := queries.GetRunEffectByBusinessKey(ctx, db.GetRunEffectByBusinessKeyParams{
		RunID:      runID,
		EffectType: effect.effectType,
		TargetKey:  effect.targetKey,
	})
	if err != nil {
		return err
	}
	if !runtimeResultEffectMatches(existing, runID, terminalEventID, effect) {
		return errors.New("runtime effect identity conflict")
	}
	return nil
}

func runtimeResultEffectMatches(
	stored db.RunEffectOutbox,
	runID, terminalEventID uuid.UUID,
	effect runtimeResultEffect,
) bool {
	return stored.ID == effect.id && stored.RunID == runID && stored.TerminalEventID == terminalEventID &&
		stored.EffectType == effect.effectType && stored.TargetKey == effect.targetKey &&
		stored.MaxAttempts == effect.maxAttempts && runtimeResultJSONEqual(stored.Metadata, effect.metadata)
}

func runtimeResultJSONEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	leftCanonical, leftErr := CanonicalizeRFC8785(leftValue)
	rightCanonical, rightErr := CanonicalizeRFC8785(rightValue)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftCanonical, rightCanonical)
}

type runtimeResultEffectPlan struct {
	parentRunID *uuid.UUID
	effects     []runtimeResultEffect
}

func discoverRuntimeResultEffects(
	ctx context.Context,
	queries *db.Queries,
	run runtimeResultRun,
	terminalEventID uuid.UUID,
	eventType string,
) (runtimeResultEffectPlan, error) {
	plan := runtimeResultEffectPlan{effects: make([]runtimeResultEffect, 0, 4)}
	delegation, err := queries.GetRunDelegationByChild(ctx, run.id)
	delegated := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return runtimeResultEffectPlan{}, err
	}
	if delegated {
		parentRunID := delegation.ParentRunID
		plan.parentRunID = &parentRunID
		metadata, marshalErr := marshalRuntimeResultJSON(map[string]any{
			"child_run_id":  run.id.String(),
			"parent_run_id": parentRunID.String(),
		})
		if marshalErr != nil {
			return runtimeResultEffectPlan{}, marshalErr
		}
		targetKey := "parent_run:" + parentRunID.String()
		plan.effects = append(plan.effects, newRuntimeResultEffect(
			run.id, runtimeResultEffectParentCompletion, targetKey, metadata,
		))
	} else {
		webhook, webhookErr := queries.GetAgentWebhookConfig(ctx, run.agentID)
		if webhookErr != nil {
			return runtimeResultEffectPlan{}, webhookErr
		}
		if webhook.WebhookURL != nil && strings.TrimSpace(*webhook.WebhookURL) != "" {
			metadata, marshalErr := marshalRuntimeResultJSON(map[string]any{
				"agent_id": run.agentID.String(),
			})
			if marshalErr != nil {
				return runtimeResultEffectPlan{}, marshalErr
			}
			targetKey := "agent:" + run.agentID.String()
			plan.effects = append(plan.effects, newRuntimeResultEffect(
				run.id, runtimeResultEffectAgentWebhook, targetKey, metadata,
			))
		}

		target, targetErr := queries.GetDefaultDeliveryTarget(ctx, run.userID)
		if targetErr != nil && !errors.Is(targetErr, pgx.ErrNoRows) {
			return runtimeResultEffectPlan{}, targetErr
		}
		if targetErr == nil && runtimeDeliveryTargetAllowsEvent(target.Config, eventType) {
			metadata, marshalErr := marshalRuntimeResultJSON(map[string]any{
				"delivery_target_id": target.ID.String(),
			})
			if marshalErr != nil {
				return runtimeResultEffectPlan{}, marshalErr
			}
			targetKey := "delivery_target:" + target.ID.String()
			plan.effects = append(plan.effects, newRuntimeResultEffect(
				run.id, runtimeResultEffectDefaultDelivery, targetKey, metadata,
			))
		}
	}

	subscriptions, err := queries.ListActiveTaskCallbackSubscriptionsForEvent(
		ctx,
		db.ListActiveTaskCallbackSubscriptionsForEventParams{
			RunID:     run.id,
			EventType: eventType,
		},
	)
	if err != nil {
		return runtimeResultEffectPlan{}, err
	}
	for _, subscription := range subscriptions {
		metadata, marshalErr := marshalRuntimeResultJSON(map[string]any{
			"run_event_id":    terminalEventID.String(),
			"subscription_id": subscription.ID.String(),
		})
		if marshalErr != nil {
			return runtimeResultEffectPlan{}, marshalErr
		}
		targetKey := "subscription:" + subscription.ID.String()
		plan.effects = append(plan.effects, newRuntimeResultEffect(
			run.id, runtimeResultEffectTaskCallback, targetKey, metadata,
		))
	}

	sort.Slice(plan.effects, func(i, j int) bool {
		if plan.effects[i].effectType != plan.effects[j].effectType {
			return plan.effects[i].effectType < plan.effects[j].effectType
		}
		return plan.effects[i].targetKey < plan.effects[j].targetKey
	})
	return plan, nil
}

func newRuntimeResultEffect(runID uuid.UUID, effectType, targetKey string, metadata []byte) runtimeResultEffect {
	maxAttempts := int32(externalRunEffectMaxAttempts)
	if effectType == runtimeResultEffectParentCompletion {
		maxAttempts = internalRunEffectMaxAttempts
	}
	return runtimeResultEffect{
		id:          deterministicRunEffectID(runID, effectType, targetKey),
		effectType:  effectType,
		targetKey:   targetKey,
		metadata:    metadata,
		maxAttempts: maxAttempts,
	}
}

func runtimeDeliveryTargetAllowsEvent(raw []byte, eventType string) bool {
	var config struct {
		EventTypes []string `json:"event_types"`
	}
	if len(raw) > 0 && json.Unmarshal(raw, &config) != nil {
		return false
	}
	if len(config.EventTypes) == 0 {
		return true
	}
	for _, allowed := range config.EventTypes {
		if allowed == eventType {
			return true
		}
	}
	return false
}

func runtimeResultFingerprintsEqual(left, right []byte) bool {
	return len(left) == sha256.Size && len(right) == sha256.Size &&
		subtle.ConstantTimeCompare(left, right) == 1
}

func marshalRuntimeResultJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal runtime result data: %w", err)
	}
	return raw, nil
}
