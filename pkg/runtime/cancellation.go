package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	defaultRuntimeCancellationDeadline = 30 * time.Second
	runtimeCancellationReasonCode      = "OWNER_CANCEL_REQUESTED"
	runtimeCancellationPublicCode      = "CANCELED"
	runtimeCancellationUnconfirmedCode = "CANCEL_UNCONFIRMED"
	maxRuntimeCancellationReapBatch    = 1000
)

var (
	ErrRuntimeCancellationNotFound = errors.New("Runtime Run is not owned by the requester")
	ErrRuntimeCancellationRunEnded = errors.New("Runtime Run is already terminal")
	ErrRuntimeCancellationInvalid  = errors.New("invalid runtime cancellation request")
	errRuntimeCancellationNotReady = errors.New("runtime cancellation coordinator is not configured")
	runtimeCancellationIDNamespace = uuid.MustParse("f6fcac0b-d253-5ad0-9290-07bf4ec2ac12")
)

// RuntimeCancellationResult is the durable owner-facing result. Replayed is
// true when the Run was already canceled by the same immutable cancellation
// evidence; no terminal artifact or command signal is emitted twice.
type RuntimeCancellationResult struct {
	Run          db.Run
	Cancellation db.RunCancellation
	Replayed     bool
}

// RuntimeCancellationCoordinator linearizes owner cancellation, durable
// command delivery, and Node stop acknowledgement through PostgreSQL.
type RuntimeCancellationCoordinator struct {
	repository      runtimeCancellationRepository
	commandDeadline time.Duration
}

func NewRuntimeCancellationCoordinator(pool *pgxpool.Pool) *RuntimeCancellationCoordinator {
	coordinator := &RuntimeCancellationCoordinator{commandDeadline: defaultRuntimeCancellationDeadline}
	if pool != nil {
		coordinator.repository = &postgresRuntimeCancellationRepository{pool: pool}
	}
	return coordinator
}

func newRuntimeCancellationCoordinator(
	repository runtimeCancellationRepository,
	commandDeadline time.Duration,
) *RuntimeCancellationCoordinator {
	if commandDeadline == 0 {
		commandDeadline = defaultRuntimeCancellationDeadline
	}
	return &RuntimeCancellationCoordinator{repository: repository, commandDeadline: commandDeadline}
}

func (c *RuntimeCancellationCoordinator) nextReapDue(
	ctx context.Context,
) (*time.Time, time.Time, error) {
	if c == nil || c.repository == nil || c.commandDeadline < time.Millisecond || c.commandDeadline > time.Hour {
		return nil, time.Time{}, errRuntimeCancellationNotReady
	}
	next, err := c.repository.nextReapDue(ctx, c.commandDeadline.Milliseconds())
	return next.NextDueAt, next.DatabaseNow, err
}

// CancelOwnedRun atomically creates cancellation evidence and the complete
// public canceled terminal fact. Any active Attempt remains unfinished until
// its executor has actually stopped (or the deadline reaper records an
// unconfirmed stop after a Core/Node crash).
func (c *RuntimeCancellationCoordinator) CancelOwnedRun(
	ctx context.Context,
	requesterID, runID uuid.UUID,
	reason string,
) (RuntimeCancellationResult, error) {
	reason, err := normalizeRuntimeCancellationReason(reason)
	if err != nil {
		return RuntimeCancellationResult{}, err
	}
	if requesterID == uuid.Nil || runID == uuid.Nil {
		return RuntimeCancellationResult{}, ErrRuntimeCancellationInvalid
	}
	if c == nil || c.repository == nil {
		return RuntimeCancellationResult{}, errRuntimeCancellationNotReady
	}

	var result RuntimeCancellationResult
	err = c.repository.WithTransaction(ctx, func(tx runtimeCancellationTransaction) error {
		locked, lockErr := tx.LockRunForResultFinalization(ctx, runID)
		if errors.Is(lockErr, pgx.ErrNoRows) {
			return ErrRuntimeCancellationNotFound
		}
		if lockErr != nil {
			return lockErr
		}
		if locked.UserID != requesterID {
			return ErrRuntimeCancellationNotFound
		}

		if locked.Status != string(RuntimeRunRunning) ||
			locked.DispatchState == string(RuntimeDispatchTerminal) ||
			locked.DispatchState == string(RuntimeDispatchDeadLetter) {
			if locked.Status != string(RuntimeRunCanceled) || locked.CancelRequestID == nil {
				return ErrRuntimeCancellationRunEnded
			}
			cancellation, target, replayErr := lockStoredRuntimeCancellation(ctx, tx, locked)
			if replayErr != nil {
				return replayErr
			}
			run, getErr := tx.GetRunByID(ctx, runID)
			if getErr != nil {
				return getErr
			}
			_ = target
			result = RuntimeCancellationResult{Run: run, Cancellation: cancellation, Replayed: true}
			return nil
		}
		if locked.CancelRequestID != nil || locked.TerminalEventID != nil {
			return ErrRuntimeCancellationRunEnded
		}

		var target *db.RunAttempt
		if locked.ActiveAttemptID != nil {
			attempt, attemptErr := tx.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
				RunID: locked.ID,
				ID:    *locked.ActiveAttemptID,
			})
			if errors.Is(attemptErr, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, attemptErr)
			}
			if attemptErr != nil {
				return attemptErr
			}
			if !runtimeCancellationActiveAttemptMatches(locked, attempt) {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
			}
			target = &attempt
		} else if locked.DispatchState != string(RuntimeDispatchPending) &&
			locked.DispatchState != string(RuntimeDispatchRetryWait) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		cancellationID := deterministicRuntimeCancellationID(runID)
		var targetID *uuid.UUID
		if target != nil {
			id := target.ID
			targetID = &id
		}
		cancellation, createErr := tx.CreateRunCancellation(ctx, db.CreateRunCancellationParams{
			ID:              cancellationID,
			RunID:           runID,
			TargetAttemptID: targetID,
			RequestedByType: "user",
			RequestedByID:   requesterID,
			Reason:          &reason,
		})
		if createErr != nil {
			return createErr
		}
		if cancellation.ID != cancellationID || cancellation.RunID != runID ||
			!optionalUUIDEqual(cancellation.TargetAttemptID, targetID) {
			return errors.New("created runtime cancellation identity is inconsistent")
		}
		if target == nil {
			cancellation, createErr = tx.AdvanceRuntimeRunCancellation(ctx, db.AdvanceRuntimeRunCancellationParams{
				NextState:      string(RuntimeCancelStopped),
				RunID:          runID,
				CancellationID: cancellationID,
				ExpectedState:  string(RuntimeCancelRequested),
			})
			if createErr != nil {
				return createErr
			}
		} else if target.ExecutorType != "runtime" &&
			target.ExecutorType != "core_http" && target.ExecutorType != "core_mcp" {
			return errors.New("runtime cancellation target has unknown executor type")
		}

		run, persistErr := tx.PersistCancellationTerminal(ctx, locked, cancellation, target)
		if persistErr != nil {
			return persistErr
		}
		result = RuntimeCancellationResult{Run: run, Cancellation: cancellation}
		return nil
	})
	if err != nil {
		return RuntimeCancellationResult{}, err
	}
	return result, nil
}

// NextCommand returns one at-least-once durable cancellation command for the
// authenticated Session. A nil command is a normal empty poll.
func (c *RuntimeCancellationCoordinator) NextCommand(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
) (*PendingCommand, time.Time, error) {
	if c == nil || c.repository == nil || c.commandDeadline < time.Millisecond || c.commandDeadline > time.Hour {
		return nil, time.Time{}, errRuntimeCancellationNotReady
	}
	eventPrincipal := principal.EventPrincipal()
	if err := validateRuntimeEventPrincipal(eventPrincipal); err != nil {
		return nil, time.Time{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}

	var command *PendingCommand
	var databaseNow time.Time
	err := c.repository.WithTransaction(ctx, func(tx runtimeCancellationTransaction) error {
		lockedPrincipal, lockErr := lockRuntimePrincipal(ctx, tx, eventPrincipal)
		if lockErr != nil {
			return mapRuntimeCancellationPrincipalError(lockErr)
		}
		databaseNow = lockedPrincipal.session.DatabaseNow

		candidate, candidateErr := tx.LockNextRuntimeCancellationCommandRun(ctx, db.LockNextRuntimeCancellationCommandRunParams{
			AgentID:           principal.AgentID,
			NodeID:            principal.NodeID,
			CredentialID:      principal.CredentialID,
			WorkerID:          principal.WorkerID,
			RuntimeSessionID:  principal.RuntimeSessionID,
			CommandDeadlineMs: c.commandDeadline.Milliseconds(),
		})
		if errors.Is(candidateErr, pgx.ErrNoRows) {
			return nil
		}
		if candidateErr != nil {
			return candidateErr
		}
		databaseNow = candidate.DatabaseNow
		if candidate.AgentID != principal.AgentID || !lockedPrincipal.validAt(candidate.DatabaseNow) {
			return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
		}

		attempt, attemptErr := tx.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
			RunID: candidate.RunID,
			ID:    candidate.TargetAttemptID,
		})
		if errors.Is(attemptErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, attemptErr)
		}
		if attemptErr != nil {
			return attemptErr
		}
		cancellation, cancellationErr := tx.LockRunCancellationForMutation(ctx, db.LockRunCancellationForMutationParams{
			RunID:          candidate.RunID,
			CancellationID: candidate.CancellationID,
		})
		if errors.Is(cancellationErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, cancellationErr)
		}
		if cancellationErr != nil {
			return cancellationErr
		}
		if !runtimeCancellationCommandTargetMatches(principal, candidate, cancellation, attempt) {
			return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
		}

		if cancellation.State == string(RuntimeCancelRequested) ||
			cancellation.State == string(RuntimeCancelDelivered) ||
			cancellation.State == string(RuntimeCancelStopping) {
			nextState := cancellation.State
			if nextState == string(RuntimeCancelRequested) {
				nextState = string(RuntimeCancelDelivered)
			}
			cancellation, cancellationErr = tx.AdvanceRuntimeRunCancellation(ctx, db.AdvanceRuntimeRunCancellationParams{
				NextState:      nextState,
				ErrorCode:      cancellation.ErrorCode,
				RunID:          cancellation.RunID,
				CancellationID: cancellation.ID,
				ExpectedState:  cancellation.State,
			})
			if cancellationErr != nil {
				return cancellationErr
			}
			if _, cancellationErr = tx.MirrorRuntimeRunCancellationState(ctx, db.MirrorRuntimeRunCancellationStateParams{
				RunID: cancellation.RunID, CancellationID: cancellation.ID,
			}); cancellationErr != nil {
				return cancellationErr
			}
		}

		pending, buildErr := runtimeCancellationPendingCommand(cancellation, attempt, c.commandDeadline)
		if buildErr != nil {
			return buildErr
		}
		command = &pending
		return nil
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	return command, databaseNow, nil
}

func (c *RuntimeCancellationCoordinator) PollCommands(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
) (RuntimeCommandsResponse, error) {
	command, databaseNow, err := c.NextCommand(ctx, principal)
	if err != nil {
		return RuntimeCommandsResponse{}, err
	}
	commands := make([]PendingCommand, 0, 1)
	if command != nil {
		commands = append(commands, *command)
	}
	return RuntimeCommandsResponse{Commands: commands, DatabaseTime: databaseNow}, nil
}

// AckCancel advances stop evidence. Only a terminal stop ACK ends the target
// Attempt and releases its capacity, guarded by the Attempt-owned CAS.
func (c *RuntimeCancellationCoordinator) AckCancel(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
	request RunCancelAckPayload,
) (RunCancellationState, error) {
	if c == nil || c.repository == nil {
		return RunCancellationState{}, errRuntimeCancellationNotReady
	}
	if err := ValidateRuntimePayload(request); err != nil || !validRuntimeCancellationAck(request) {
		return RunCancellationState{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	if err := validatePrincipalAttemptIdentity(principal, request.AttemptIdentity); err != nil {
		return RunCancellationState{}, err
	}
	eventPrincipal := principal.EventPrincipal()

	var state RunCancellationState
	err := c.repository.WithTransaction(ctx, func(tx runtimeCancellationTransaction) error {
		lockedPrincipal, lockErr := lockRuntimePrincipal(ctx, tx, eventPrincipal)
		if lockErr != nil {
			return mapRuntimeCancellationPrincipalError(lockErr)
		}
		lockedRun, runErr := tx.LockRunForResultFinalization(ctx, request.AttemptIdentity.RunID)
		if errors.Is(runErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, runErr)
		}
		if runErr != nil {
			return runErr
		}
		if !lockedPrincipal.validAt(lockedRun.DatabaseNow) {
			return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
		}

		attempt, attemptErr := tx.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
			RunID: request.AttemptIdentity.RunID,
			ID:    request.AttemptIdentity.AttemptID,
		})
		if errors.Is(attemptErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, attemptErr)
		}
		if attemptErr != nil {
			return attemptErr
		}
		cancellation, cancellationErr := tx.LockRunCancellationForMutation(ctx, db.LockRunCancellationForMutationParams{
			RunID:          request.AttemptIdentity.RunID,
			CancellationID: request.CancellationID,
		})
		if errors.Is(cancellationErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, cancellationErr)
		}
		if cancellationErr != nil {
			return cancellationErr
		}
		if !runtimeCancellationAckIdentityMatches(lockedRun, eventPrincipal, request, cancellation, attempt) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		if cancellation.State == string(request.CancelState) {
			state = runtimeCancellationStateFromDB(cancellation)
			return nil
		}
		if cancellation.State == string(RuntimeCancelUnconfirmed) {
			state = runtimeCancellationStateFromDB(cancellation)
			return nil
		}
		if cancellation.State == string(RuntimeCancelStopped) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		if runtimeCancellationAckAlreadyCovered(RuntimeCancelState(cancellation.State), request.CancelState) {
			state = runtimeCancellationStateFromDB(cancellation)
			return nil
		}
		if !runtimeCancellationTransitionAllowed(RuntimeCancelState(cancellation.State), request.CancelState) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		var errorCode *string
		if request.ErrorCode != "" {
			code := request.ErrorCode
			errorCode = &code
		}
		advanced, advanceErr := tx.AdvanceRuntimeRunCancellation(ctx, db.AdvanceRuntimeRunCancellationParams{
			NextState:      string(request.CancelState),
			ErrorCode:      errorCode,
			RunID:          cancellation.RunID,
			CancellationID: cancellation.ID,
			ExpectedState:  cancellation.State,
		})
		if errors.Is(advanceErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, advanceErr)
		}
		if advanceErr != nil {
			return advanceErr
		}

		if request.CancelState == RuntimeCancelStopped {
			if finishErr := finishRuntimeCancellationAttempt(
				ctx, tx, attempt, errorCode, c.commandDeadline,
			); finishErr != nil {
				return finishErr
			}
		}
		mirrored, mirrorErr := tx.MirrorRuntimeRunCancellationState(ctx, db.MirrorRuntimeRunCancellationStateParams{
			RunID: advanced.RunID, CancellationID: advanced.ID,
		})
		if errors.Is(mirrorErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, mirrorErr)
		}
		if mirrorErr != nil {
			return mirrorErr
		}
		if mirrored.CancelState == nil || *mirrored.CancelState != advanced.State {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		state = runtimeCancellationStateFromDB(advanced)
		return nil
	})
	if err != nil {
		return RunCancellationState{}, err
	}
	return state, nil
}

// AcknowledgeCoreStopped is called only after the Core-owned HTTP/MCP call has
// returned. It records stopped evidence and ends the immutable Attempt in one
// Run -> Attempt -> Cancellation transaction; the owner request path never
// claims that an in-flight goroutine has already stopped.
func (c *RuntimeCancellationCoordinator) AcknowledgeCoreStopped(
	ctx context.Context,
	coreInstanceID uuid.UUID,
	identity RuntimeAttemptIdentity,
) (RunCancellationState, error) {
	if c == nil || c.repository == nil {
		return RunCancellationState{}, errRuntimeCancellationNotReady
	}
	if coreInstanceID == uuid.Nil {
		return RunCancellationState{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}
	if err := validateRuntimeAttemptIdentity(identity); err != nil ||
		identity.NodeID != nil || identity.RuntimeSessionID != nil || identity.WorkerID != nil {
		return RunCancellationState{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}

	var state RunCancellationState
	err := c.repository.WithTransaction(ctx, func(tx runtimeCancellationTransaction) error {
		locked, err := tx.LockRunForResultFinalization(ctx, identity.RunID)
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		if err != nil {
			return err
		}
		if locked.Status != string(RuntimeRunCanceled) ||
			locked.DispatchState != string(RuntimeDispatchTerminal) ||
			locked.CancelRequestID == nil || locked.LatestAttemptID == nil ||
			*locked.LatestAttemptID != identity.AttemptID || locked.AgentID != identity.AgentID {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		attempt, err := tx.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
			RunID: identity.RunID, ID: identity.AttemptID,
		})
		if err != nil {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		if attempt.RunID != identity.RunID || attempt.AgentID != identity.AgentID ||
			attempt.LeaseID != identity.LeaseID || attempt.FencingToken != identity.FencingToken ||
			attempt.AttachedCoreInstanceID != coreInstanceID ||
			(attempt.ExecutorType != "core_http" && attempt.ExecutorType != "core_mcp") {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		cancellation, err := tx.LockRunCancellationForMutation(ctx, db.LockRunCancellationForMutationParams{
			RunID: identity.RunID, CancellationID: *locked.CancelRequestID,
		})
		if err != nil || cancellation.TargetAttemptID == nil ||
			*cancellation.TargetAttemptID != attempt.ID {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		if cancellation.State == string(RuntimeCancelStopped) {
			if attempt.FinishedAt == nil || attempt.Outcome == nil || *attempt.Outcome != "canceled" {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
			}
			state = runtimeCancellationStateFromDB(cancellation)
			return nil
		}
		if cancellation.State != string(RuntimeCancelRequested) &&
			cancellation.State != string(RuntimeCancelDelivered) &&
			cancellation.State != string(RuntimeCancelStopping) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		advanced, err := tx.AdvanceRuntimeRunCancellation(ctx, db.AdvanceRuntimeRunCancellationParams{
			NextState: string(RuntimeCancelStopped), RunID: cancellation.RunID,
			CancellationID: cancellation.ID, ExpectedState: cancellation.State,
		})
		if err != nil {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		finished, err := tx.FinishRuntimeCoreCanceledAttempt(ctx, db.FinishRuntimeCoreCanceledAttemptParams{
			RunID: attempt.RunID, AttemptID: attempt.ID,
			LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
		})
		if err != nil || finished.FinishedAt == nil || finished.Outcome == nil || *finished.Outcome != "canceled" {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		mirrored, err := tx.MirrorRuntimeRunCancellationState(ctx, db.MirrorRuntimeRunCancellationStateParams{
			RunID: advanced.RunID, CancellationID: advanced.ID,
		})
		if err != nil || mirrored.CancelState == nil || *mirrored.CancelState != advanced.State {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		state = runtimeCancellationStateFromDB(advanced)
		return nil
	})
	return state, err
}

// ReapExpiredCancellation converts one overdue cancellation into durable
// unconfirmed stop evidence, ends its fenced Attempt, and releases capacity in
// the same transaction. A nil result means no cancellation is currently due.
func (c *RuntimeCancellationCoordinator) ReapExpiredCancellation(
	ctx context.Context,
) (*RunCancellationState, error) {
	if c == nil || c.repository == nil || c.commandDeadline < time.Millisecond || c.commandDeadline > time.Hour {
		return nil, errRuntimeCancellationNotReady
	}

	if state, handled, err := c.reapExpiredCoreCancellation(ctx); err != nil || handled {
		return state, err
	}

	var state *RunCancellationState
	err := c.repository.WithTransaction(ctx, func(tx runtimeCancellationTransaction) error {
		candidate, candidateErr := tx.FindNextDueRuntimeCancellation(
			ctx, c.commandDeadline.Milliseconds(),
		)
		if errors.Is(candidateErr, pgx.ErrNoRows) {
			return nil
		}
		if candidateErr != nil {
			return candidateErr
		}
		lockedSessionID, sessionErr := tx.LockRuntimeSessionForCancellationReap(ctx, candidate.RuntimeSessionID)
		if sessionErr != nil {
			return sessionErr
		}
		if lockedSessionID != candidate.RuntimeSessionID {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		lockedNodeID, nodeErr := tx.LockRuntimeNodeForCancellationReap(ctx, candidate.NodeID)
		if nodeErr != nil {
			return nodeErr
		}
		if lockedNodeID != candidate.NodeID {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		lockedRun, runErr := tx.LockDueRuntimeCancellationRun(ctx, db.LockDueRuntimeCancellationRunParams{
			RunID: candidate.RunID, CancellationID: candidate.CancellationID,
			TargetAttemptID: candidate.TargetAttemptID, RuntimeSessionID: candidate.RuntimeSessionID,
			NodeID: candidate.NodeID, CommandDeadlineMs: c.commandDeadline.Milliseconds(),
		})
		if errors.Is(runErr, pgx.ErrNoRows) {
			return nil
		}
		if runErr != nil {
			return runErr
		}
		attempt, attemptErr := tx.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
			RunID: lockedRun.RunID, ID: lockedRun.TargetAttemptID,
		})
		if errors.Is(attemptErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, attemptErr)
		}
		if attemptErr != nil {
			return attemptErr
		}
		cancellation, cancellationErr := tx.LockRunCancellationForMutation(ctx, db.LockRunCancellationForMutationParams{
			RunID: lockedRun.RunID, CancellationID: lockedRun.CancellationID,
		})
		if errors.Is(cancellationErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, cancellationErr)
		}
		if cancellationErr != nil {
			return cancellationErr
		}
		if !runtimeCancellationReapTargetMatches(lockedRun, cancellation, attempt, c.commandDeadline) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		errorCode := runtimeCancellationUnconfirmedCode
		reapedCancellation := cancellation
		switch RuntimeCancelState(cancellation.State) {
		case RuntimeCancelRequested, RuntimeCancelDelivered, RuntimeCancelStopping:
			advanced, advanceErr := tx.AdvanceRuntimeRunCancellation(ctx, db.AdvanceRuntimeRunCancellationParams{
				NextState:      string(RuntimeCancelUnconfirmed),
				ErrorCode:      &errorCode,
				RunID:          cancellation.RunID,
				CancellationID: cancellation.ID,
				ExpectedState:  cancellation.State,
			})
			if errors.Is(advanceErr, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, advanceErr)
			}
			if advanceErr != nil {
				return advanceErr
			}
			reapedCancellation = advanced
		case RuntimeCancelFailed, RuntimeCancelUnsupported:
			// Negative acknowledgements are terminal evidence. The deadline only
			// authorizes ending the still-fenced Attempt and releasing capacity;
			// it must not rewrite the cancellation state or its original error.
		default:
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		if finishErr := finishRuntimeCancellationAttempt(
			ctx, tx, attempt, &errorCode, c.commandDeadline,
		); finishErr != nil {
			return finishErr
		}
		mirrored, mirrorErr := tx.MirrorRuntimeRunCancellationState(ctx, db.MirrorRuntimeRunCancellationStateParams{
			RunID: reapedCancellation.RunID, CancellationID: reapedCancellation.ID,
		})
		if errors.Is(mirrorErr, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, mirrorErr)
		}
		if mirrorErr != nil {
			return mirrorErr
		}
		if mirrored.CancelState == nil || *mirrored.CancelState != reapedCancellation.State {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		value := runtimeCancellationStateFromDB(reapedCancellation)
		state = &value
		return nil
	})
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (c *RuntimeCancellationCoordinator) reapExpiredCoreCancellation(
	ctx context.Context,
) (*RunCancellationState, bool, error) {
	var state *RunCancellationState
	handled := false
	err := c.repository.WithTransaction(ctx, func(tx runtimeCancellationTransaction) error {
		candidate, err := tx.FindNextDueRuntimeCoreCancellation(ctx, c.commandDeadline.Milliseconds())
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		locked, err := tx.LockDueRuntimeCoreCancellationRun(ctx, db.LockDueRuntimeCoreCancellationRunParams{
			RunID: candidate.RunID, CancellationID: candidate.CancellationID,
			TargetAttemptID:   candidate.TargetAttemptID,
			CommandDeadlineMs: c.commandDeadline.Milliseconds(),
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		attempt, err := tx.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
			RunID: locked.RunID, ID: locked.TargetAttemptID,
		})
		if err != nil {
			return err
		}
		cancellation, err := tx.LockRunCancellationForMutation(ctx, db.LockRunCancellationForMutationParams{
			RunID: locked.RunID, CancellationID: locked.CancellationID,
		})
		if err != nil {
			return err
		}
		if attempt.ID != candidate.TargetAttemptID || attempt.RunID != candidate.RunID ||
			attempt.AgentID != candidate.AgentID ||
			(attempt.ExecutorType != "core_http" && attempt.ExecutorType != "core_mcp") ||
			attempt.FinishedAt != nil || attempt.Outcome != nil || attempt.ResultID != nil ||
			cancellation.TargetAttemptID == nil || *cancellation.TargetAttemptID != attempt.ID {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		errorCode := runtimeCancellationUnconfirmedCode
		advanced, err := tx.AdvanceRuntimeRunCancellation(ctx, db.AdvanceRuntimeRunCancellationParams{
			NextState: string(RuntimeCancelUnconfirmed), ErrorCode: &errorCode,
			RunID: cancellation.RunID, CancellationID: cancellation.ID,
			ExpectedState: cancellation.State,
		})
		if err != nil {
			return err
		}
		// The query requires stopped to end a Core Attempt. The reaper records
		// unconfirmed, so use the generic fenced mutation through a dedicated
		// query shape rather than misrepresenting a confirmed stop.
		finished, err := tx.FinishRuntimeCoreUnconfirmedAttempt(ctx, db.FinishRuntimeCoreUnconfirmedAttemptParams{
			RunID: attempt.RunID, AttemptID: attempt.ID,
			LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
		})
		if err != nil || finished.FinishedAt == nil || finished.Outcome == nil || *finished.Outcome != "canceled" {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		mirrored, err := tx.MirrorRuntimeRunCancellationState(ctx, db.MirrorRuntimeRunCancellationStateParams{
			RunID: advanced.RunID, CancellationID: advanced.ID,
		})
		if err != nil || mirrored.CancelState == nil || *mirrored.CancelState != advanced.State {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		value := runtimeCancellationStateFromDB(advanced)
		state = &value
		handled = true
		return nil
	})
	return state, handled, err
}

// ReapExpiredCancellations drains at most limit overdue cancellations. Each
// winner commits independently so one long batch never holds unrelated locks.
func (c *RuntimeCancellationCoordinator) ReapExpiredCancellations(
	ctx context.Context,
	limit int,
) (int, error) {
	if limit < 1 || limit > maxRuntimeCancellationReapBatch {
		return 0, ErrRuntimeCancellationInvalid
	}
	for reaped := 0; reaped < limit; reaped++ {
		state, err := c.ReapExpiredCancellation(ctx)
		if err != nil {
			return reaped, err
		}
		if state == nil {
			return reaped, nil
		}
	}
	return limit, nil
}

func lockStoredRuntimeCancellation(
	ctx context.Context,
	tx runtimeCancellationTransaction,
	locked db.LockRunForResultFinalizationRow,
) (db.RunCancellation, *db.RunAttempt, error) {
	stored, err := tx.GetRunCancellationByRun(ctx, locked.ID)
	if err != nil {
		return db.RunCancellation{}, nil, err
	}
	var target *db.RunAttempt
	if stored.TargetAttemptID != nil {
		attempt, attemptErr := tx.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
			RunID: locked.ID, ID: *stored.TargetAttemptID,
		})
		if attemptErr != nil {
			return db.RunCancellation{}, nil, attemptErr
		}
		target = &attempt
	}
	stored, err = tx.LockRunCancellationForMutation(ctx, db.LockRunCancellationForMutationParams{
		RunID: locked.ID, CancellationID: stored.ID,
	})
	if err != nil {
		return db.RunCancellation{}, nil, err
	}
	if locked.CancelRequestID == nil || stored.ID != *locked.CancelRequestID ||
		locked.CancelState == nil || stored.State != *locked.CancelState {
		return db.RunCancellation{}, nil, errors.New("stored runtime cancellation summary is inconsistent")
	}
	return stored, target, nil
}

func finishRuntimeCancellationAttempt(
	ctx context.Context,
	tx runtimeCancellationTransaction,
	attempt db.RunAttempt,
	errorCode *string,
	commandDeadline time.Duration,
) error {
	if commandDeadline < time.Millisecond || commandDeadline > time.Hour {
		return errRuntimeCancellationNotReady
	}
	finished, err := tx.FinishRuntimeCanceledAttempt(ctx, db.FinishRuntimeCanceledAttemptParams{
		ErrorCode: errorCode, RunID: attempt.RunID, AttemptID: attempt.ID,
		LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
		CommandDeadlineMs: commandDeadline.Milliseconds(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
	}
	if err != nil {
		return err
	}
	if finished.FinishedAt == nil || finished.Outcome == nil || *finished.Outcome != "canceled" ||
		finished.SlotAcquiredAt == nil || finished.ActiveRuntimeSessionID == nil || finished.NodeID == nil {
		return errors.New("finished canceled Attempt is missing capacity evidence")
	}
	capacity, err := tx.MarkRunAttemptCapacityReleased(ctx, db.MarkRunAttemptCapacityReleasedParams{
		RunID: attempt.RunID, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
	}
	if err != nil {
		return err
	}
	if capacity.RuntimeSessionID != *finished.ActiveRuntimeSessionID || capacity.NodeID != *finished.NodeID ||
		capacity.SlotAcquiredAt.IsZero() || capacity.SlotReleasedAt.IsZero() ||
		capacity.SlotReleasedAt.Before(capacity.SlotAcquiredAt) {
		return errors.New("canceled Attempt capacity owner changed")
	}
	if _, err = tx.ReleaseRuntimeSessionSlot(ctx, capacity.RuntimeSessionID); err != nil {
		return err
	}
	if _, err = tx.ReleaseRuntimeNodeSlot(ctx, capacity.NodeID); err != nil {
		return err
	}
	return createRuntimeNodeCapacityAvailableSignal(
		ctx, tx, attempt.AgentID, capacity.NodeID, attempt.RunID,
	)
}

func (t *postgresRuntimeCancellationTransaction) PersistCancellationTerminal(
	ctx context.Context,
	locked db.LockRunForResultFinalizationRow,
	cancellation db.RunCancellation,
	target *db.RunAttempt,
) (db.Run, error) {
	run, err := runtimeResultRunFromLocked(locked)
	if err != nil {
		return db.Run{}, err
	}
	durationMS, err := runtimeCancellationDurationMS(locked.DatabaseNow, locked.StartedAt)
	if err != nil {
		return db.Run{}, err
	}
	terminalEventID := deterministicTerminalEventID(run.id, string(RuntimeRunCanceled))
	effectPlan, err := discoverRuntimeResultEffects(ctx, t.queries, run, terminalEventID, "run.canceled")
	if err != nil {
		return db.Run{}, err
	}
	payload, err := marshalRuntimeResultJSON(map[string]any{
		"cancellation_id": cancellation.ID.String(),
		"classification":  RuntimeResultClassificationCanceled,
		"duration_ms":     durationMS,
		"error_code":      runtimeCancellationPublicCode,
		"error_message":   cancellation.Reason,
		"status":          RuntimeRunCanceled,
		"terminal":        true,
	})
	if err != nil {
		return db.Run{}, err
	}
	if err = t.queries.LockRunEventSequence(ctx, run.id); err != nil {
		return db.Run{}, err
	}
	event, err := t.queries.InsertTerminalRunEvent(ctx, db.InsertTerminalRunEventParams{
		ID: terminalEventID, RunID: run.id, ParentRunID: effectPlan.parentRunID,
		EventType: "run.canceled", Payload: payload,
	})
	if err != nil {
		return db.Run{}, err
	}
	finalized, err := t.queries.FinalizeRuntimeRunCancellation(ctx, db.FinalizeRuntimeRunCancellationParams{
		DurationMs: durationMS, TerminalEventID: event.ID, RunID: run.id, CancellationID: cancellation.ID,
	})
	if err != nil {
		return db.Run{}, err
	}
	if finalized.Status != string(RuntimeRunCanceled) || finalized.DispatchState != string(RuntimeDispatchTerminal) ||
		finalized.TerminalEventID == nil || *finalized.TerminalEventID != event.ID ||
		finalized.CancelRequestID == nil || *finalized.CancelRequestID != cancellation.ID {
		return db.Run{}, errors.New("finalized runtime cancellation is inconsistent")
	}
	if err = insertRuntimeCancellationLedger(ctx, t.queries, run, event.ID); err != nil {
		return db.Run{}, err
	}
	for _, effect := range effectPlan.effects {
		if err = ensureRuntimeResultEffect(ctx, t.queries, run.id, event.ID, effect); err != nil {
			return db.Run{}, err
		}
	}
	if target != nil {
		signalPayload, signalErr := CanonicalizeRFC8785(map[string]any{
			"attempt_id":      target.ID.String(),
			"cancellation_id": cancellation.ID.String(),
			"run_id":          run.id.String(),
		})
		if signalErr != nil {
			return db.Run{}, signalErr
		}
		if target.ExecutorType == "core_http" || target.ExecutorType == "core_mcp" {
			targetInstanceID := target.AttachedCoreInstanceID.String()
			var fields map[string]any
			if unmarshalErr := json.Unmarshal(signalPayload, &fields); unmarshalErr != nil {
				return db.Run{}, unmarshalErr
			}
			fields["target_instance_id"] = targetInstanceID
			signalPayload, signalErr = CanonicalizeRFC8785(fields)
			if signalErr != nil {
				return db.Run{}, signalErr
			}
		}
		if _, signalErr = t.queries.CreateRuntimeSignal(ctx, db.CreateRuntimeSignalParams{
			EventType: "run.cancel", AgentID: run.agentID, RunID: &run.id, Payload: signalPayload,
		}); signalErr != nil {
			return db.Run{}, signalErr
		}
	}
	return t.queries.GetRunByID(ctx, run.id)
}

// Owner cancellation is neutral availability evidence: it must close the
// accounting ledger exactly once without counting as an Agent failure.
func insertRuntimeCancellationLedger(
	ctx context.Context,
	queries *db.Queries,
	run runtimeResultRun,
	terminalEventID uuid.UUID,
) error {
	created, err := queries.InsertRunAccountingLedger(ctx, db.InsertRunAccountingLedgerParams{
		RunID: run.id, TerminalEventID: terminalEventID, AgentID: run.agentID,
		SuccessDelta: 0, RevenueDeltaCents: 0,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := queries.GetRunAccountingLedger(ctx, run.id)
		if getErr != nil {
			return getErr
		}
		if existing.TerminalEventID != terminalEventID || existing.AgentID != run.agentID ||
			existing.SuccessDelta != 0 || existing.RevenueDeltaCents != 0 {
			return errors.New("runtime cancellation accounting ledger identity conflict")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if created.RunID != run.id || created.TerminalEventID != terminalEventID ||
		created.AgentID != run.agentID || created.SuccessDelta != 0 || created.RevenueDeltaCents != 0 {
		return errors.New("created runtime cancellation accounting ledger is inconsistent")
	}
	return nil
}

func runtimeCancellationActiveAttemptMatches(locked db.LockRunForResultFinalizationRow, attempt db.RunAttempt) bool {
	return locked.ActiveAttemptID != nil && *locked.ActiveAttemptID == attempt.ID &&
		locked.LatestAttemptID != nil && *locked.LatestAttemptID == attempt.ID &&
		locked.LeaseID != nil && *locked.LeaseID == attempt.LeaseID &&
		locked.FencingToken == attempt.FencingToken && locked.AgentID == attempt.AgentID &&
		attempt.RunID == locked.ID && attempt.FinishedAt == nil && attempt.Outcome == nil
}

func runtimeCancellationCommandTargetMatches(
	principal RuntimeSessionPrincipal,
	candidate db.LockNextRuntimeCancellationCommandRunRow,
	cancellation db.RunCancellation,
	attempt db.RunAttempt,
) bool {
	return cancellation.ID == candidate.CancellationID && cancellation.RunID == candidate.RunID &&
		cancellation.TargetAttemptID != nil && *cancellation.TargetAttemptID == attempt.ID &&
		attempt.RunID == candidate.RunID && attempt.AgentID == principal.AgentID &&
		attempt.ExecutorType == "runtime" && attempt.FinishedAt == nil &&
		attempt.NodeID != nil && *attempt.NodeID == principal.NodeID &&
		attempt.RuntimeTokenID != nil && *attempt.RuntimeTokenID == principal.CredentialID &&
		attempt.RuntimeWorkerID != nil && *attempt.RuntimeWorkerID == principal.WorkerID &&
		attempt.RuntimeSessionID != nil && *attempt.RuntimeSessionID == principal.RuntimeSessionID
}

func runtimeCancellationReapTargetMatches(
	candidate db.LockDueRuntimeCancellationRunRow,
	cancellation db.RunCancellation,
	attempt db.RunAttempt,
	deadline time.Duration,
) bool {
	if candidate.DatabaseNow.IsZero() || deadline < time.Millisecond || deadline > time.Hour ||
		cancellation.RequestedAt.IsZero() || candidate.DatabaseNow.Before(cancellation.RequestedAt.Add(deadline)) {
		return false
	}
	switch RuntimeCancelState(cancellation.State) {
	case RuntimeCancelRequested, RuntimeCancelDelivered, RuntimeCancelStopping,
		RuntimeCancelUnsupported, RuntimeCancelFailed:
	default:
		return false
	}
	return cancellation.ID == candidate.CancellationID && cancellation.RunID == candidate.RunID &&
		cancellation.TargetAttemptID != nil && *cancellation.TargetAttemptID == candidate.TargetAttemptID &&
		attempt.ID == candidate.TargetAttemptID && attempt.RunID == candidate.RunID &&
		attempt.AgentID == candidate.AgentID && attempt.ExecutorType == "runtime" &&
		attempt.FinishedAt == nil && attempt.Outcome == nil && attempt.ResultID == nil &&
		attempt.SlotAcquiredAt != nil && attempt.SlotReleasedAt == nil &&
		attempt.ActiveRuntimeSessionID != nil && attempt.NodeID != nil
}

func runtimeCancellationAckIdentityMatches(
	locked db.LockRunForResultFinalizationRow,
	principal RuntimeEventPrincipal,
	request RunCancelAckPayload,
	cancellation db.RunCancellation,
	attempt db.RunAttempt,
) bool {
	return locked.Status == string(RuntimeRunCanceled) && locked.DispatchState == string(RuntimeDispatchTerminal) &&
		locked.CancelRequestID != nil && *locked.CancelRequestID == request.CancellationID &&
		cancellation.ID == request.CancellationID && cancellation.RunID == locked.ID &&
		cancellation.TargetAttemptID != nil && *cancellation.TargetAttemptID == attempt.ID &&
		runtimeIdentityMatchesAttempt(request.AttemptIdentity.RuntimeIdentity(), runtimeEventAttemptFromDB(attempt)) &&
		runtimePrincipalMatchesAttempt(principal, runtimeEventAttemptFromDB(attempt))
}

func runtimeCancellationPendingCommand(
	cancellation db.RunCancellation,
	attempt db.RunAttempt,
	deadline time.Duration,
) (PendingCommand, error) {
	identity, err := attemptIdentityFromRow(attempt)
	if err != nil {
		return PendingCommand{}, err
	}
	payload := RunCancelPayload{
		CancellationID:  cancellation.ID,
		AttemptIdentity: identity,
		ReasonCode:      runtimeCancellationReasonCode,
		DeadlineAt:      cancellation.RequestedAt.Add(deadline),
	}
	if err = ValidateRuntimePayload(payload); err != nil {
		return PendingCommand{}, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return PendingCommand{}, err
	}
	command := PendingCommand{Type: RuntimeMessageRunCancel, Payload: raw}
	if _, err = DecodePendingCommand(command); err != nil {
		return PendingCommand{}, err
	}
	return command, nil
}

func normalizeRuntimeCancellationReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "Run canceled by owner"
	}
	if !utf8.ValidString(reason) || utf8.RuneCountInString(reason) > 500 {
		return "", ErrRuntimeCancellationInvalid
	}
	return reason, nil
}

func deterministicRuntimeCancellationID(runID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(runtimeCancellationIDNamespace, []byte(runID.String()+"/owner-cancel"))
}

func runtimeCancellationDurationMS(databaseNow, startedAt time.Time) (int32, error) {
	if databaseNow.IsZero() || startedAt.IsZero() || databaseNow.Before(startedAt) {
		return 0, errors.New("runtime cancellation database clock is invalid")
	}
	milliseconds := databaseNow.Sub(startedAt).Milliseconds()
	if milliseconds > math.MaxInt32 {
		milliseconds = math.MaxInt32
	}
	return int32(milliseconds), nil
}

func validRuntimeCancellationAck(request RunCancelAckPayload) bool {
	switch request.CancelState {
	case RuntimeCancelDelivered, RuntimeCancelStopping, RuntimeCancelStopped:
		return request.ErrorCode == ""
	case RuntimeCancelUnsupported, RuntimeCancelFailed:
		return strings.TrimSpace(request.ErrorCode) != ""
	default:
		return false
	}
}

func runtimeCancellationAckFinalState(state RuntimeCancelState) bool {
	return state == RuntimeCancelStopped || state == RuntimeCancelUnsupported || state == RuntimeCancelFailed
}

func runtimeCancellationTransitionAllowed(current, next RuntimeCancelState) bool {
	switch current {
	case RuntimeCancelRequested:
		return next == RuntimeCancelDelivered || next == RuntimeCancelStopping || runtimeCancellationAckFinalState(next)
	case RuntimeCancelDelivered:
		return next == RuntimeCancelStopping || runtimeCancellationAckFinalState(next)
	case RuntimeCancelStopping:
		return runtimeCancellationAckFinalState(next)
	default:
		return false
	}
}

func runtimeCancellationAckAlreadyCovered(current, incoming RuntimeCancelState) bool {
	return (current == RuntimeCancelStopping && incoming == RuntimeCancelDelivered) ||
		(current == RuntimeCancelUnconfirmed && incoming != RuntimeCancelUnconfirmed)
}

func runtimeCancellationStateFromDB(cancellation db.RunCancellation) RunCancellationState {
	state := RunCancellationState{
		CancellationID: cancellation.ID,
		CancelState:    RuntimeCancelState(cancellation.State),
		UpdatedAt:      cancellation.UpdatedAt,
	}
	if cancellation.ErrorCode != nil {
		state.ErrorCode = *cancellation.ErrorCode
	}
	return state
}

func mapRuntimeCancellationPrincipalError(err error) error {
	if errors.Is(err, ErrInvalidRuntimeEvent) {
		return newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	var eventErr *RuntimeEventError
	if errors.As(err, &eventErr) && eventErr.Code == RuntimeEventErrorLeaseIdentityMismatch {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, err)
	}
	return err
}

type runtimeCancellationRepository interface {
	WithTransaction(context.Context, func(runtimeCancellationTransaction) error) error
	nextReapDue(context.Context, int64) (db.NextRuntimeCancellationReapDueRow, error)
}

type runtimeCancellationTransaction interface {
	runtimePrincipalLockQueries
	LockRunForResultFinalization(context.Context, uuid.UUID) (db.LockRunForResultFinalizationRow, error)
	LockRunAttemptForResult(context.Context, db.LockRunAttemptForResultParams) (db.RunAttempt, error)
	GetRunCancellationByRun(context.Context, uuid.UUID) (db.RunCancellation, error)
	CreateRunCancellation(context.Context, db.CreateRunCancellationParams) (db.RunCancellation, error)
	LockNextRuntimeCancellationCommandRun(context.Context, db.LockNextRuntimeCancellationCommandRunParams) (db.LockNextRuntimeCancellationCommandRunRow, error)
	FindNextDueRuntimeCancellation(context.Context, int64) (db.FindNextDueRuntimeCancellationRow, error)
	FindNextDueRuntimeCoreCancellation(context.Context, int64) (db.FindNextDueRuntimeCoreCancellationRow, error)
	LockRuntimeSessionForCancellationReap(context.Context, uuid.UUID) (uuid.UUID, error)
	LockRuntimeNodeForCancellationReap(context.Context, uuid.UUID) (uuid.UUID, error)
	LockDueRuntimeCancellationRun(context.Context, db.LockDueRuntimeCancellationRunParams) (db.LockDueRuntimeCancellationRunRow, error)
	LockDueRuntimeCoreCancellationRun(context.Context, db.LockDueRuntimeCoreCancellationRunParams) (db.LockDueRuntimeCoreCancellationRunRow, error)
	LockRunCancellationForMutation(context.Context, db.LockRunCancellationForMutationParams) (db.RunCancellation, error)
	AdvanceRuntimeRunCancellation(context.Context, db.AdvanceRuntimeRunCancellationParams) (db.RunCancellation, error)
	MirrorRuntimeRunCancellationState(context.Context, db.MirrorRuntimeRunCancellationStateParams) (db.MirrorRuntimeRunCancellationStateRow, error)
	FinishRuntimeCanceledAttempt(context.Context, db.FinishRuntimeCanceledAttemptParams) (db.RunAttempt, error)
	FinishRuntimeCoreCanceledAttempt(context.Context, db.FinishRuntimeCoreCanceledAttemptParams) (db.RunAttempt, error)
	FinishRuntimeCoreUnconfirmedAttempt(context.Context, db.FinishRuntimeCoreUnconfirmedAttemptParams) (db.RunAttempt, error)
	MarkRunAttemptCapacityReleased(context.Context, db.MarkRunAttemptCapacityReleasedParams) (db.MarkRunAttemptCapacityReleasedRow, error)
	ReleaseRuntimeSessionSlot(context.Context, uuid.UUID) (db.RuntimeSession, error)
	ReleaseRuntimeNodeSlot(context.Context, uuid.UUID) (db.RuntimeNode, error)
	CreateRuntimeSignal(context.Context, db.CreateRuntimeSignalParams) (db.RuntimeSignalOutbox, error)
	GetRunByID(context.Context, uuid.UUID) (db.Run, error)
	PersistCancellationTerminal(context.Context, db.LockRunForResultFinalizationRow, db.RunCancellation, *db.RunAttempt) (db.Run, error)
}

type postgresRuntimeCancellationRepository struct {
	pool *pgxpool.Pool
}

func (r *postgresRuntimeCancellationRepository) WithTransaction(
	ctx context.Context,
	fn func(runtimeCancellationTransaction) error,
) error {
	if r == nil || r.pool == nil {
		return errRuntimeCancellationNotReady
	}
	return pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite,
	}, func(tx pgx.Tx) error {
		queries := db.New(tx)
		return fn(&postgresRuntimeCancellationTransaction{queries: queries})
	})
}

func (r *postgresRuntimeCancellationRepository) nextReapDue(
	ctx context.Context,
	commandDeadlineMS int64,
) (db.NextRuntimeCancellationReapDueRow, error) {
	if r == nil || r.pool == nil {
		return db.NextRuntimeCancellationReapDueRow{}, errRuntimeCancellationNotReady
	}
	return db.New(r.pool).NextRuntimeCancellationReapDue(ctx, commandDeadlineMS)
}

type postgresRuntimeCancellationTransaction struct {
	queries *db.Queries
}

func (t *postgresRuntimeCancellationTransaction) LockRuntimeSessionForPrincipalValidation(ctx context.Context, params db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeSessionForPrincipalValidation(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) LockRuntimeNodeForPrincipalValidation(ctx context.Context, params db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeNodeForPrincipalValidation(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) LockRuntimeCredentialForPrincipalValidation(ctx context.Context, params db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeCredentialForPrincipalValidation(ctx, params)
}

func (t *postgresRuntimeCancellationTransaction) LockRuntimeSessionAttachmentForPrincipalValidation(ctx context.Context, params db.LockRuntimeSessionAttachmentForPrincipalValidationParams) (db.RuntimeSessionAttachment, error) {
	return t.queries.LockRuntimeSessionAttachmentForPrincipalValidation(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) LockRunForResultFinalization(ctx context.Context, id uuid.UUID) (db.LockRunForResultFinalizationRow, error) {
	return t.queries.LockRunForResultFinalization(ctx, id)
}
func (t *postgresRuntimeCancellationTransaction) LockRunAttemptForResult(ctx context.Context, params db.LockRunAttemptForResultParams) (db.RunAttempt, error) {
	return t.queries.LockRunAttemptForResult(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) GetRunCancellationByRun(ctx context.Context, id uuid.UUID) (db.RunCancellation, error) {
	return t.queries.GetRunCancellationByRun(ctx, id)
}
func (t *postgresRuntimeCancellationTransaction) CreateRunCancellation(ctx context.Context, params db.CreateRunCancellationParams) (db.RunCancellation, error) {
	return t.queries.CreateRunCancellation(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) LockNextRuntimeCancellationCommandRun(ctx context.Context, params db.LockNextRuntimeCancellationCommandRunParams) (db.LockNextRuntimeCancellationCommandRunRow, error) {
	return t.queries.LockNextRuntimeCancellationCommandRun(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) FindNextDueRuntimeCancellation(ctx context.Context, commandDeadlineMS int64) (db.FindNextDueRuntimeCancellationRow, error) {
	return t.queries.FindNextDueRuntimeCancellation(ctx, commandDeadlineMS)
}
func (t *postgresRuntimeCancellationTransaction) FindNextDueRuntimeCoreCancellation(ctx context.Context, commandDeadlineMS int64) (db.FindNextDueRuntimeCoreCancellationRow, error) {
	return t.queries.FindNextDueRuntimeCoreCancellation(ctx, commandDeadlineMS)
}
func (t *postgresRuntimeCancellationTransaction) LockRuntimeSessionForCancellationReap(ctx context.Context, sessionID uuid.UUID) (uuid.UUID, error) {
	return t.queries.LockRuntimeSessionForCancellationReap(ctx, sessionID)
}
func (t *postgresRuntimeCancellationTransaction) LockRuntimeNodeForCancellationReap(ctx context.Context, nodeID uuid.UUID) (uuid.UUID, error) {
	return t.queries.LockRuntimeNodeForCancellationReap(ctx, nodeID)
}
func (t *postgresRuntimeCancellationTransaction) LockDueRuntimeCancellationRun(ctx context.Context, params db.LockDueRuntimeCancellationRunParams) (db.LockDueRuntimeCancellationRunRow, error) {
	return t.queries.LockDueRuntimeCancellationRun(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) LockDueRuntimeCoreCancellationRun(ctx context.Context, params db.LockDueRuntimeCoreCancellationRunParams) (db.LockDueRuntimeCoreCancellationRunRow, error) {
	return t.queries.LockDueRuntimeCoreCancellationRun(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) LockRunCancellationForMutation(ctx context.Context, params db.LockRunCancellationForMutationParams) (db.RunCancellation, error) {
	return t.queries.LockRunCancellationForMutation(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) AdvanceRuntimeRunCancellation(ctx context.Context, params db.AdvanceRuntimeRunCancellationParams) (db.RunCancellation, error) {
	return t.queries.AdvanceRuntimeRunCancellation(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) MirrorRuntimeRunCancellationState(ctx context.Context, params db.MirrorRuntimeRunCancellationStateParams) (db.MirrorRuntimeRunCancellationStateRow, error) {
	return t.queries.MirrorRuntimeRunCancellationState(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) FinishRuntimeCanceledAttempt(ctx context.Context, params db.FinishRuntimeCanceledAttemptParams) (db.RunAttempt, error) {
	return t.queries.FinishRuntimeCanceledAttempt(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) FinishRuntimeCoreCanceledAttempt(ctx context.Context, params db.FinishRuntimeCoreCanceledAttemptParams) (db.RunAttempt, error) {
	return t.queries.FinishRuntimeCoreCanceledAttempt(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) FinishRuntimeCoreUnconfirmedAttempt(ctx context.Context, params db.FinishRuntimeCoreUnconfirmedAttemptParams) (db.RunAttempt, error) {
	return t.queries.FinishRuntimeCoreUnconfirmedAttempt(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) MarkRunAttemptCapacityReleased(ctx context.Context, params db.MarkRunAttemptCapacityReleasedParams) (db.MarkRunAttemptCapacityReleasedRow, error) {
	return t.queries.MarkRunAttemptCapacityReleased(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) ReleaseRuntimeSessionSlot(ctx context.Context, id uuid.UUID) (db.RuntimeSession, error) {
	return t.queries.ReleaseRuntimeSessionSlot(ctx, id)
}
func (t *postgresRuntimeCancellationTransaction) ReleaseRuntimeNodeSlot(ctx context.Context, id uuid.UUID) (db.RuntimeNode, error) {
	return t.queries.ReleaseRuntimeNodeSlot(ctx, id)
}

func (t *postgresRuntimeCancellationTransaction) CreateRuntimeSignal(ctx context.Context, params db.CreateRuntimeSignalParams) (db.RuntimeSignalOutbox, error) {
	return t.queries.CreateRuntimeSignal(ctx, params)
}
func (t *postgresRuntimeCancellationTransaction) GetRunByID(ctx context.Context, id uuid.UUID) (db.Run, error) {
	return t.queries.GetRunByID(ctx, id)
}

var _ runtimeCancellationRepository = (*postgresRuntimeCancellationRepository)(nil)
var _ runtimeCancellationTransaction = (*postgresRuntimeCancellationTransaction)(nil)
