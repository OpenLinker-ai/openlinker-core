package runtime

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	defaultCoreCancellationPollInterval = 2 * time.Second
	defaultCoreCancellationBatchSize    = 512
	coreFinalizationTimeout             = 15 * time.Second
	corePersistenceAttempts             = 3
	corePersistenceRetryDelay           = 100 * time.Millisecond
)

var (
	errCoreAttemptOwnerCanceled = errors.New("core runtime attempt canceled by owner")
	errCoreAttemptDeadline      = errors.New("core runtime attempt deadline exceeded")
	coreRuntimeEventNamespace   = uuid.MustParse("7d8e1a7c-9095-5578-b3ea-897ce63e5819")
)

type coreAttemptExecution struct {
	identity            RuntimeAttemptIdentity
	attemptNo           int32
	deadlineAt          time.Time
	endpointIdempotency bool
}

type coreAttemptCancellationQueries interface {
	ListRequestedCoreAttemptCancellations(context.Context, []uuid.UUID) ([]db.ListRequestedCoreAttemptCancellationsRow, error)
}

type activeCoreAttempt struct {
	identity RuntimeAttemptIdentity
	cancel   context.CancelCauseFunc
}

// coreAttemptRegistry is process-local acceleration only. One shared fallback
// coordinator always returns to PostgreSQL, so Redis loss or a dropped signal
// cannot make owner cancellation depend on this map.
type coreAttemptRegistry struct {
	queries   coreAttemptCancellationQueries
	pollEvery time.Duration

	mu      sync.Mutex
	entries map[uuid.UUID]map[uuid.UUID]activeCoreAttempt
	start   sync.Once
	changed chan struct{}
}

func newCoreAttemptRegistry(queries coreAttemptCancellationQueries, pollEvery time.Duration) *coreAttemptRegistry {
	if pollEvery <= 0 || pollEvery > defaultCoreCancellationPollInterval {
		pollEvery = defaultCoreCancellationPollInterval
	}
	return &coreAttemptRegistry{
		queries: queries, pollEvery: pollEvery,
		entries: make(map[uuid.UUID]map[uuid.UUID]activeCoreAttempt),
		changed: make(chan struct{}, 1),
	}
}

func (r *coreAttemptRegistry) register(
	parent context.Context,
	execution coreAttemptExecution,
) (context.Context, func()) {
	if parent == nil {
		parent = context.Background()
	}
	deadlineCtx, deadlineCancel := context.WithDeadlineCause(parent, execution.deadlineAt, errCoreAttemptDeadline)
	callCtx, cancel := context.WithCancelCause(deadlineCtx)
	entry := activeCoreAttempt{identity: execution.identity, cancel: cancel}

	r.mu.Lock()
	byAttempt := r.entries[execution.identity.RunID]
	if byAttempt == nil {
		byAttempt = make(map[uuid.UUID]activeCoreAttempt)
		r.entries[execution.identity.RunID] = byAttempt
	}
	byAttempt[execution.identity.AttemptID] = entry
	r.mu.Unlock()
	select {
	case r.changed <- struct{}{}:
	default:
	}

	var once sync.Once
	unregister := func() {
		once.Do(func() {
			r.mu.Lock()
			if attempts := r.entries[execution.identity.RunID]; attempts != nil {
				delete(attempts, execution.identity.AttemptID)
				if len(attempts) == 0 {
					delete(r.entries, execution.identity.RunID)
				}
			}
			r.mu.Unlock()
			cancel(nil)
			deadlineCancel()
		})
	}
	return callCtx, unregister
}

func (r *coreAttemptRegistry) cancelRun(runID uuid.UUID) {
	if r == nil || runID == uuid.Nil {
		return
	}
	r.mu.Lock()
	entries := make([]activeCoreAttempt, 0, len(r.entries[runID]))
	for _, entry := range r.entries[runID] {
		entries = append(entries, entry)
	}
	r.mu.Unlock()
	for _, entry := range entries {
		entry.cancel(errCoreAttemptOwnerCanceled)
	}
}

func (r *coreAttemptRegistry) startCancellationCoordinator(ctx context.Context) {
	if r == nil || r.queries == nil {
		return
	}
	r.start.Do(func() {
		go func() {
			ticker := time.NewTicker(r.pollEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-r.changed:
					r.reconcileCancellations(ctx)
				case <-ticker.C:
					r.reconcileCancellations(ctx)
				}
			}
		}()
	})
}

func (r *coreAttemptRegistry) reconcileCancellations(ctx context.Context) {
	if r == nil || r.queries == nil || ctx.Err() != nil {
		return
	}
	r.mu.Lock()
	runIDs := make([]uuid.UUID, 0, len(r.entries))
	for runID := range r.entries {
		runIDs = append(runIDs, runID)
	}
	r.mu.Unlock()
	if len(runIDs) == 0 {
		return
	}
	for start := 0; start < len(runIDs); start += defaultCoreCancellationBatchSize {
		end := start + defaultCoreCancellationBatchSize
		if end > len(runIDs) {
			end = len(runIDs)
		}
		queryCtx, cancel := context.WithTimeout(ctx, r.pollEvery)
		requested, err := r.queries.ListRequestedCoreAttemptCancellations(queryCtx, runIDs[start:end])
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				log.Warn().Err(err).Int("active_runs", len(runIDs)).
					Msg("runtime core cancellation batch reconciliation failed")
			}
			return
		}
		for _, item := range requested {
			r.mu.Lock()
			entry, ok := r.entries[item.RunID][item.AttemptID]
			r.mu.Unlock()
			if !ok || entry.identity.LeaseID != item.LeaseID ||
				entry.identity.FencingToken != item.FencingToken {
				continue
			}
			entry.cancel(errCoreAttemptOwnerCanceled)
		}
	}
}

func (s *Service) beginCoreAttempt(ctx context.Context, invocation *runInvocation) (coreAttemptExecution, error) {
	if s == nil || s.pool == nil || invocation == nil || s.coreInstanceID == uuid.Nil {
		return coreAttemptExecution{}, errors.New("core runtime executor is not configured")
	}
	executorType := "core_http"
	if invocation.agent.ConnectionMode == connectionModeMCPServer {
		executorType = "core_mcp"
	} else if invocation.agent.ConnectionMode != "" && invocation.agent.ConnectionMode != connectionModeDirectHTTP {
		return coreAttemptExecution{}, errors.New("queued runtime Run cannot use the Core executor")
	}

	attemptID, leaseID := uuid.New(), uuid.New()
	var execution coreAttemptExecution
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite,
	}, func(tx pgx.Tx) error {
		if err := RequireRuntimeClusterOperation(ctx, tx, RuntimeClusterClaim); err != nil {
			return err
		}
		queries := s.queries.WithTx(tx)
		locked, err := queries.LockCoreRunForExecution(ctx, db.LockCoreRunForExecutionParams{
			RunID: invocation.runID, AgentID: invocation.agent.ID,
		})
		if err != nil {
			return err
		}
		if locked.ConnectionModeSnapshot == nil ||
			(*locked.ConnectionModeSnapshot == connectionModeDirectHTTP && executorType != "core_http") ||
			(*locked.ConnectionModeSnapshot == connectionModeMCPServer && executorType != "core_mcp") {
			return errors.New("Core executor does not match the immutable connection snapshot")
		}
		if locked.EndpointIdempotencySnapshot == nil {
			return errors.New("Core executor is missing endpoint idempotency evidence")
		}
		deadlineAt := locked.RunDeadlineAt
		attempt, err := queries.CreateRunAttempt(ctx, db.CreateRunAttemptParams{
			ID: attemptID, RunID: locked.ID, AgentID: locked.AgentID,
			OfferNo: locked.OfferCount + 1, ExecutorType: executorType,
			LeaseID: leaseID, FencingToken: locked.FencingToken + 1,
			OfferedByCoreInstanceID: s.coreInstanceID,
			AttachedCoreInstanceID:  s.coreInstanceID,
			OfferExpiresAt:          deadlineAt, LeaseExpiresAt: deadlineAt,
			AttemptDeadlineAt: deadlineAt,
		})
		if err != nil {
			return err
		}
		accepted, err := queries.AcceptRunAttempt(ctx, db.AcceptRunAttemptParams{
			RunID: attempt.RunID, ID: attempt.ID, LeaseID: attempt.LeaseID,
			FencingToken: attempt.FencingToken, AttemptNo: locked.AttemptCount + 1,
			LeaseExpiresAt: deadlineAt, AttachedCoreInstanceID: s.coreInstanceID,
		})
		if err != nil {
			return err
		}
		mirrored, err := queries.MirrorCoreRunAcceptedAttempt(ctx, db.MirrorCoreRunAcceptedAttemptParams{
			RunID: accepted.RunID, AttemptID: accepted.ID, LeaseID: accepted.LeaseID,
			FencingToken: accepted.FencingToken, ExecutorType: executorType,
		})
		if err != nil {
			return err
		}
		if accepted.AttemptNo == nil || mirrored.ActiveAttemptID == nil ||
			*mirrored.ActiveAttemptID != accepted.ID || mirrored.AttemptCount != *accepted.AttemptNo ||
			mirrored.ExecutorType == nil || *mirrored.ExecutorType != executorType {
			return errors.New("Core Attempt confirmation did not mirror its immutable Run summary")
		}
		execution = coreAttemptExecution{
			identity: RuntimeAttemptIdentity{
				RunID: accepted.RunID, AttemptID: accepted.ID, LeaseID: accepted.LeaseID,
				FencingToken: accepted.FencingToken, AgentID: accepted.AgentID,
			},
			attemptNo: *accepted.AttemptNo, deadlineAt: accepted.AttemptDeadlineAt,
			endpointIdempotency: *locked.EndpointIdempotencySnapshot,
		}
		return nil
	})
	return execution, err
}

func (s *Service) executeCoreAttempt(ctx context.Context, invocation *runInvocation) *RunResponse {
	execution, err := s.beginCoreAttempt(ctx, invocation)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) && !isContextErr(err) {
			log.Error().Err(err).Str("run_id", invocation.runID.String()).
				Msg("runtime: begin Core Attempt")
		}
		return s.readRunResponseAfterExecution(invocation)
	}

	principal := RuntimeEventPrincipal{AgentID: invocation.agent.ID}
	lastEventSeq := int64(0)
	startedRequest := RuntimeEventRequest{
		ClientEventID:  coreRuntimeEventID(execution.identity.AttemptID, 1, "run.started"),
		ClientEventSeq: 1,
		EventType:      "run.started",
		Payload:        runStartedEventPayload(invocation.agent, invocation.userID),
	}
	if err = s.appendCoreRuntimeEvent(principal, execution.identity, startedRequest); err != nil {
		return s.finalizeCoreSetupFailure(invocation, execution, principal, err)
	}
	lastEventSeq = 1

	callCtx, unregister := s.coreExecutions.register(ctx, execution)
	startedAt := time.Now()
	output, events, agentErr, callErr := s.callAgent(
		callCtx, &invocation.agent, invocation.runID, invocation.userID,
		invocation.req, invocation.delegation,
	)
	durationMS := clampDurationMillisToInt32(time.Since(startedAt))
	cause := context.Cause(callCtx)
	unregister()

	if errors.Is(cause, errCoreAttemptOwnerCanceled) {
		s.acknowledgeCoreStopped(execution.identity)
		return s.readRunResponseAfterExecution(invocation)
	}

	if len(events) > maxAgentResponseEvents {
		events = events[:maxAgentResponseEvents]
	}
	for _, event := range events {
		if _, allowed := allowedAgentResponseEventTypes[event.EventType]; !allowed || event.Payload == nil {
			continue
		}
		next := lastEventSeq + 1
		request := RuntimeEventRequest{
			ClientEventID:  coreRuntimeEventID(execution.identity.AttemptID, next, event.EventType),
			ClientEventSeq: next,
			EventType:      event.EventType,
			Payload:        event.Payload,
		}
		if appendErr := s.appendCoreRuntimeEvent(principal, execution.identity, request); appendErr != nil {
			log.Error().Err(appendErr).Str("run_id", invocation.runID.String()).
				Int64("client_event_seq", next).Msg("runtime: persist Core endpoint event")
			// The endpoint outcome is known in memory, but publishing it while a
			// preceding Event may or may not have committed would create an
			// unverifiable gap. Leave the fenced Attempt to the reconciler, which
			// fails closed as ENDPOINT_RESULT_UNKNOWN after the database deadline.
			return s.readRunResponseAfterExecution(invocation)
		}
		lastEventSeq = next
	}

	request := RuntimeResultRequest{
		AttemptIdentity:     execution.identity,
		ResultID:            uuid.New(),
		DurationMS:          durationMS,
		FinalClientEventSeq: lastEventSeq,
	}
	if callErr == nil && agentErr == nil {
		if output == nil {
			output = map[string]any{}
		}
		request.Status = "success"
		request.Output = output
	} else {
		request.Status = "failed"
		request.Error = coreRuntimeFailure(agentErr, callErr, execution.endpointIdempotency)
	}

	finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), coreFinalizationTimeout)
	_, finalizeErr := s.finalizeCoreResult(finalizeCtx, principal, request)
	finalizeCancel()
	if finalizeErr != nil {
		if IsRuntimeResultError(finalizeErr, RuntimeResultErrorRunAlreadyTerminal) ||
			IsRuntimeResultError(finalizeErr, RuntimeResultErrorRunCancelRequested) {
			s.acknowledgeCoreStopped(execution.identity)
		} else {
			log.Error().Err(finalizeErr).Str("run_id", invocation.runID.String()).
				Msg("runtime: finalize Core Result")
		}
	}
	return s.readRunResponseAfterExecution(invocation)
}

func (s *Service) finalizeCoreSetupFailure(
	invocation *runInvocation,
	execution coreAttemptExecution,
	principal RuntimeEventPrincipal,
	cause error,
) *RunResponse {
	message := "Core could not persist execution progress before endpoint invocation"
	request := RuntimeResultRequest{
		AttemptIdentity: execution.identity, ResultID: uuid.New(), Status: "failed",
		Error:      &RuntimeResultFailure{ErrorCode: "CORE_EVENT_PERSIST_FAILED", Message: message},
		DurationMS: 0, FinalClientEventSeq: 0,
	}
	finalizeCtx, cancel := context.WithTimeout(context.Background(), coreFinalizationTimeout)
	_, err := s.finalizeCoreResult(finalizeCtx, principal, request)
	cancel()
	if err != nil {
		log.Error().Err(errors.Join(cause, err)).Str("run_id", invocation.runID.String()).
			Msg("runtime: finalize Core setup failure")
	}
	return s.readRunResponseAfterExecution(invocation)
}

func (s *Service) acknowledgeCoreStopped(identity RuntimeAttemptIdentity) {
	if s == nil || s.cancellation == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), coreFinalizationTimeout)
	_, err := s.cancellation.AcknowledgeCoreStopped(ctx, s.coreInstanceID, identity)
	cancel()
	if err != nil && !IsRuntimeLeaseError(err, RuntimeLeaseErrorStaleLease) {
		log.Error().Err(err).Str("run_id", identity.RunID.String()).
			Msg("runtime: acknowledge Core cancellation")
	}
}

func (s *Service) appendCoreRuntimeEvent(
	principal RuntimeEventPrincipal,
	identity RuntimeAttemptIdentity,
	request RuntimeEventRequest,
) error {
	var lastErr error
	for attempt := 0; attempt < corePersistenceAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), coreFinalizationTimeout)
		_, err := s.AppendRuntimeEvent(ctx, principal, identity, request)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if IsRuntimeEventError(err, RuntimeEventErrorStaleLease) ||
			IsRuntimeEventError(err, RuntimeEventErrorLeaseExpired) ||
			IsRuntimeEventError(err, RuntimeEventErrorRunAlreadyTerminal) ||
			IsRuntimeEventError(err, RuntimeEventErrorLeaseIdentityMismatch) ||
			IsRuntimeEventError(err, RuntimeEventErrorIDConflict) {
			break
		}
		time.Sleep(corePersistenceRetryDelay)
	}
	return lastErr
}

func (s *Service) finalizeCoreResult(
	ctx context.Context,
	principal RuntimeResultPrincipal,
	request RuntimeResultRequest,
) (RuntimeResultAck, error) {
	var lastErr error
	for attempt := 0; attempt < corePersistenceAttempts; attempt++ {
		ack, err := s.resultFinalizer.Finalize(ctx, principal, request)
		if err == nil {
			return ack, nil
		}
		lastErr = err
		var resultErr *RuntimeResultError
		if errors.As(err, &resultErr) {
			break
		}
		select {
		case <-ctx.Done():
			return RuntimeResultAck{}, ctx.Err()
		case <-time.After(corePersistenceRetryDelay):
		}
	}
	return RuntimeResultAck{}, lastErr
}

func (s *Service) readRunResponseAfterExecution(invocation *runInvocation) *RunResponse {
	ctx, cancel := context.WithTimeout(context.Background(), coreFinalizationTimeout)
	defer cancel()
	resp, err := s.GetRun(ctx, invocation.userID, invocation.runID)
	if err == nil && resp != nil {
		return resp
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", invocation.runID.String()).
			Msg("runtime: read Run after Core execution")
	}
	return &RunResponse{
		RunID: invocation.runID.String(), AgentID: invocation.agent.ID.String(),
		AgentConnectionMode: invocation.agent.ConnectionMode,
		Status:              "running", RuntimeContractID: RuntimeContractID,
		DispatchState: string(RuntimeDispatchPending),
	}
}

func coreRuntimeFailure(agentErr *AgentError, callErr error, endpointIdempotency bool) *RuntimeResultFailure {
	if callErr != nil {
		return &RuntimeResultFailure{
			ErrorCode:     "ENDPOINT_RESULT_UNKNOWN",
			Message:       "Endpoint execution outcome could not be confirmed",
			RetryableHint: endpointIdempotency,
		}
	}
	code, message := "AGENT_ERROR", "Agent returned an execution error"
	if agentErr != nil {
		code = truncateRuntimeText(strings.TrimSpace(agentErr.Code), maxRuntimeResultErrorCodeLen)
		message = truncateRuntimeText(strings.TrimSpace(agentErr.Message), maxRuntimeResultErrorTextLen)
	}
	if code == "" {
		code = "AGENT_ERROR"
	}
	if message == "" {
		message = "Agent returned an execution error"
	}
	return &RuntimeResultFailure{ErrorCode: code, Message: message}
}

func truncateRuntimeText(value string, maximum int) string {
	if maximum < 1 || !utf8.ValidString(value) {
		return ""
	}
	runes := []rune(value)
	if len(runes) > maximum {
		runes = runes[:maximum]
	}
	return string(runes)
}

func coreRuntimeEventID(attemptID uuid.UUID, sequence int64, eventType string) uuid.UUID {
	name := attemptID.String() + "/" + eventType + "/" + strconv.FormatInt(sequence, 10)
	return uuid.NewSHA1(coreRuntimeEventNamespace, []byte(name))
}
