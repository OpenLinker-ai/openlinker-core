package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	defaultRuntimeResumeGrantTTL = 15 * time.Minute
	maximumRuntimeResumeGrantTTL = 24 * time.Hour
)

// RuntimeResumeService decides what a reconnecting Runtime Worker may do with
// each immutable Attempt identity. It never moves execution or capacity
// ownership to a replacement Session: cross-Session recovery is restricted
// to uploading an already durable local spool.
type RuntimeResumeService struct {
	repository     runtimeResumeRepository
	coreInstanceID uuid.UUID
	grantTTL       time.Duration
}

// NewRuntimeResumeService creates a PostgreSQL-backed Resume service. A zero
// grant TTL selects the protocol default; all authorization deadlines are
// written and compared by PostgreSQL.
func NewRuntimeResumeService(
	pool *pgxpool.Pool,
	coreInstanceID uuid.UUID,
	grantTTL time.Duration,
) *RuntimeResumeService {
	service := &RuntimeResumeService{
		coreInstanceID: coreInstanceID,
		grantTTL:       normalizeRuntimeResumeGrantTTL(grantTTL),
	}
	if pool != nil {
		service.repository = &postgresRuntimeResumeRepository{pool: pool}
	}
	return service
}

func newRuntimeResumeService(
	repository runtimeResumeRepository,
	coreInstanceID uuid.UUID,
	grantTTL time.Duration,
) *RuntimeResumeService {
	return &RuntimeResumeService{
		repository:     repository,
		coreInstanceID: coreInstanceID,
		grantTTL:       normalizeRuntimeResumeGrantTTL(grantTTL),
	}
}

// Resume returns one decision per requested Attempt, preserving the caller's
// order. Locks are nevertheless acquired by Run UUID and Attempt UUID so two
// reconnecting processes cannot deadlock by presenting a different order.
func (s *RuntimeResumeService) Resume(
	ctx context.Context,
	target RuntimeSessionPrincipal,
	payload RuntimeResumePayload,
) (RuntimeResumeResponse, error) {
	if err := s.validateResume(target, payload); err != nil {
		return RuntimeResumeResponse{}, err
	}

	ordered := sortRuntimeResumeAttempts(payload.Attempts)
	candidate := RuntimeResumeResponse{
		Decisions: make([]RunResumeAcceptedPayload, len(payload.Attempts)),
	}
	err := s.repository.WithTransaction(ctx, func(tx runtimeResumeTransaction) error {
		principal, err := s.lockTargetPrincipal(ctx, tx, target)
		if err != nil {
			return err
		}

		var (
			currentRunID uuid.UUID
			currentRun   db.LockRunForLeaseMutationRow
			runFound     bool
		)
		for _, item := range ordered {
			identity := item.attempt.AttemptIdentity
			if identity.RunID != currentRunID {
				currentRunID = identity.RunID
				currentRun, err = tx.LockRunForLeaseMutation(ctx, identity.RunID)
				switch {
				case err == nil:
					runFound = true
				case errors.Is(err, pgx.ErrNoRows):
					runFound = false
					err = nil
				default:
					return err
				}
			}

			if !runFound {
				candidate.Decisions[item.originalIndex] = revokedResumeDecision(identity)
				continue
			}
			if !principal.validAt(currentRun.DatabaseNow) {
				return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
			}

			attempt, lockErr := tx.LockRunAttemptForResult(ctx, db.LockRunAttemptForResultParams{
				RunID: identity.RunID,
				ID:    identity.AttemptID,
			})
			switch {
			case lockErr == nil:
			case errors.Is(lockErr, pgx.ErrNoRows):
				candidate.Decisions[item.originalIndex] = revokedResumeDecision(identity)
				continue
			default:
				return lockErr
			}

			decision, decisionErr := s.resumeAttempt(
				ctx, tx, target, currentRun, attempt, item.attempt,
			)
			if decisionErr != nil {
				return decisionErr
			}
			candidate.Decisions[item.originalIndex] = decision
		}

		if validationErr := ValidateRuntimePayload(candidate); validationErr != nil {
			return newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, validationErr)
		}
		return nil
	})
	if err != nil {
		return RuntimeResumeResponse{}, err
	}
	return candidate, nil
}

func (s *RuntimeResumeService) resumeAttempt(
	ctx context.Context,
	tx runtimeResumeTransaction,
	target RuntimeSessionPrincipal,
	run db.LockRunForLeaseMutationRow,
	attempt db.RunAttempt,
	request ResumeAttempt,
) (RunResumeAcceptedPayload, error) {
	identity := request.AttemptIdentity
	if !runtimeResumeIdentityMatches(run, attempt, identity) {
		return revokedResumeDecision(identity), nil
	}

	sameSession := attempt.RuntimeSessionID != nil && attempt.RuntimeTokenID != nil &&
		*attempt.RuntimeSessionID == target.RuntimeSessionID &&
		*attempt.RuntimeTokenID == target.CredentialID
	if sameSession {
		resultState, err := runtimeResumeResultState(ctx, tx, attempt, request.PendingResultID)
		if err != nil {
			return RunResumeAcceptedPayload{}, err
		}
		if resultState == runtimeResumeResultStored {
			return ackedResumeDecision(identity), nil
		}
		if runtimeResumeAttemptCanContinue(run, attempt, target) {
			leaseExpiresAt := attempt.LeaseExpiresAt
			return RunResumeAcceptedPayload{
				AttemptIdentity: identity,
				Decision:        RuntimeResumeContinueExecution,
				LeaseExpiresAt:  &leaseExpiresAt,
				AllowedActions: []RuntimeResumeAction{
					RuntimeActionContinueExecution,
					RuntimeActionUploadEvents,
					RuntimeActionUploadResult,
				},
			}, nil
		}
		return revokedResumeDecision(identity), nil
	}

	if len(request.PendingClientEventRanges) == 0 && request.PendingResultID == nil {
		return revokedResumeDecision(identity), nil
	}
	grant, authorized, err := s.lockOrCreateSpoolGrant(ctx, tx, target, attempt, identity)
	if err != nil {
		return RunResumeAcceptedPayload{}, err
	}
	if !authorized {
		return revokedResumeDecision(identity), nil
	}

	// Result knowledge is disclosed to a replacement Session only after the
	// durable grant proves its relationship to the immutable source Session.
	resultState, err := runtimeResumeResultState(ctx, tx, attempt, request.PendingResultID)
	if err != nil {
		return RunResumeAcceptedPayload{}, err
	}
	if resultState == runtimeResumeResultStored {
		consumed, consumeErr := tx.ConsumeRuntimeResumeGrant(ctx, db.ConsumeRuntimeResumeGrantParams{
			GrantID:            grant.ID,
			RunID:              identity.RunID,
			AttemptID:          identity.AttemptID,
			LeaseID:            identity.LeaseID,
			FencingToken:       identity.FencingToken,
			TargetSessionID:    target.RuntimeSessionID,
			TargetCredentialID: target.CredentialID,
			Permission:         runtimeResumeUploadPermission,
		})
		if errors.Is(consumeErr, pgx.ErrNoRows) {
			return revokedResumeDecision(identity), nil
		}
		if consumeErr != nil {
			return RunResumeAcceptedPayload{}, consumeErr
		}
		if !runtimeResumeConsumedGrantMatches(consumed, grant, target, attempt, identity) {
			return revokedResumeDecision(identity), nil
		}
		return ackedResumeDecision(identity), nil
	}
	if !runtimeResumeSpoolCanWrite(run, attempt, identity) {
		if revokeErr := s.revokeSpoolGrant(ctx, tx, grant, identity); revokeErr != nil {
			return RunResumeAcceptedPayload{}, revokeErr
		}
		return revokedResumeDecision(identity), nil
	}

	actions := make([]RuntimeResumeAction, 0, 2)
	if len(request.PendingClientEventRanges) > 0 {
		actions = append(actions, RuntimeActionUploadEvents)
	}
	if request.PendingResultID != nil {
		actions = append(actions, RuntimeActionUploadResult)
	}
	return RunResumeAcceptedPayload{
		AttemptIdentity: identity,
		Decision:        RuntimeResumeUploadSpoolOnly,
		AllowedActions:  actions,
	}, nil
}

func (s *RuntimeResumeService) revokeSpoolGrant(
	ctx context.Context,
	tx runtimeResumeTransaction,
	grant db.RuntimeResumeGrant,
	identity AttemptIdentity,
) error {
	revokedByType := "core_instance"
	revokedByID := s.coreInstanceID
	reason := "spool_upload_no_longer_writable"
	revoked, err := tx.RevokeRuntimeResumeGrant(ctx, db.RevokeRuntimeResumeGrantParams{
		RevokedByType: &revokedByType,
		RevokedByID:   &revokedByID,
		RevokeReason:  &reason,
		GrantID:       grant.ID,
		RunID:         identity.RunID,
		AttemptID:     identity.AttemptID,
		LeaseID:       identity.LeaseID,
		FencingToken:  identity.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if revoked.ID != grant.ID || revoked.RunID != identity.RunID ||
		revoked.AttemptID != identity.AttemptID || revoked.LeaseID != identity.LeaseID ||
		revoked.FencingToken != identity.FencingToken || revoked.RevokedAt == nil ||
		revoked.RevokedByType == nil || *revoked.RevokedByType != revokedByType ||
		revoked.RevokedByID == nil || *revoked.RevokedByID != revokedByID ||
		revoked.RevokeReason == nil || *revoked.RevokeReason != reason {
		return fmt.Errorf("runtime resume grant revocation evidence mismatch")
	}
	return nil
}

func (s *RuntimeResumeService) lockOrCreateSpoolGrant(
	ctx context.Context,
	tx runtimeResumeTransaction,
	target RuntimeSessionPrincipal,
	attempt db.RunAttempt,
	identity AttemptIdentity,
) (db.RuntimeResumeGrant, bool, error) {
	coreInstanceID := s.coreInstanceID
	active, err := tx.LockActiveRuntimeResumeGrantForAttemptTarget(ctx, db.LockActiveRuntimeResumeGrantForAttemptTargetParams{
		RunID:              identity.RunID,
		AttemptID:          identity.AttemptID,
		LeaseID:            identity.LeaseID,
		FencingToken:       identity.FencingToken,
		AgentID:            identity.AgentID,
		NodeID:             identity.NodeID,
		WorkerID:           identity.WorkerID,
		TargetSessionID:    target.RuntimeSessionID,
		TargetCredentialID: target.CredentialID,
		AllowedPermission:  runtimeResumeUploadPermission,
		CoreInstanceID:     &coreInstanceID,
	})
	if err == nil {
		if !runtimeResumeActiveGrantMatches(active, target, attempt, identity) {
			return db.RuntimeResumeGrant{}, false, nil
		}
		return runtimeResumeGrantFromActive(active), true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return db.RuntimeResumeGrant{}, false, err
	}

	grantID, err := uuid.NewRandomFromReader(rand.Reader)
	if err != nil {
		return db.RuntimeResumeGrant{}, false, fmt.Errorf("generate runtime resume grant ID: %w", err)
	}
	created, err := tx.CreateRuntimeResumeGrant(ctx, db.CreateRuntimeResumeGrantParams{
		GrantID:         grantID,
		Permission:      runtimeResumeUploadPermission,
		CoreInstanceID:  s.coreInstanceID,
		GrantTtlMs:      s.grantTTL.Milliseconds(),
		TargetSessionID: target.RuntimeSessionID,
		RunID:           identity.RunID,
		AttemptID:       identity.AttemptID,
		LeaseID:         identity.LeaseID,
		FencingToken:    identity.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.RuntimeResumeGrant{}, false, nil
	}
	if err != nil {
		return db.RuntimeResumeGrant{}, false, err
	}
	if !runtimeResumeGrantMatches(created, target, attempt, identity, s.coreInstanceID) {
		return db.RuntimeResumeGrant{}, false, nil
	}
	return created, true, nil
}

type runtimeResumeResultStatus uint8

const (
	runtimeResumeResultPending runtimeResumeResultStatus = iota
	runtimeResumeResultStored
)

func runtimeResumeResultState(
	ctx context.Context,
	tx runtimeResumeTransaction,
	attempt db.RunAttempt,
	pendingResultID *uuid.UUID,
) (runtimeResumeResultStatus, error) {
	if pendingResultID == nil {
		return runtimeResumeResultPending, nil
	}
	if attempt.ResultID != nil {
		if *attempt.ResultID != *pendingResultID {
			return runtimeResumeResultPending, newRuntimeResultError(RuntimeResultErrorResultIDConflict, nil)
		}
		return runtimeResumeResultStored, nil
	}

	otherAttempt, err := tx.GetRunAttemptByResultID(ctx, db.GetRunAttemptByResultIDParams{
		RunID:    attempt.RunID,
		ResultID: *pendingResultID,
	})
	if err == nil {
		_ = otherAttempt
		return runtimeResumeResultPending, newRuntimeResultError(RuntimeResultErrorResultIDConflict, nil)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return runtimeResumeResultPending, err
	}
	return runtimeResumeResultPending, nil
}

func runtimeResumeIdentityMatches(
	run db.LockRunForLeaseMutationRow,
	attempt db.RunAttempt,
	identity AttemptIdentity,
) bool {
	return run.ID == identity.RunID && run.AgentID == identity.AgentID &&
		attempt.ID == identity.AttemptID && attempt.RunID == identity.RunID &&
		attempt.AgentID == identity.AgentID && attempt.ExecutorType == "runtime" &&
		attempt.LeaseID == identity.LeaseID && attempt.FencingToken == identity.FencingToken &&
		uuidPointerEqual(attempt.NodeID, identity.NodeID) &&
		uuidPointerEqual(attempt.RuntimeSessionID, identity.RuntimeSessionID) &&
		stringPointerEqual(attempt.RuntimeWorkerID, identity.WorkerID) &&
		attempt.RuntimeTokenID != nil
}

func runtimeResumeAttemptCanContinue(
	run db.LockRunForLeaseMutationRow,
	attempt db.RunAttempt,
	target RuntimeSessionPrincipal,
) bool {
	databaseNow := run.DatabaseNow
	return run.Status == string(RuntimeRunRunning) &&
		run.DispatchState == string(RuntimeDispatchExecuting) &&
		run.TerminalEventID == nil && run.CancelRequestID == nil &&
		uuidPointerEqual(run.LatestAttemptID, attempt.ID) &&
		uuidPointerEqual(run.ActiveAttemptID, attempt.ID) &&
		uuidPointerEqual(run.LeaseID, attempt.LeaseID) &&
		run.FencingToken == attempt.FencingToken &&
		uuidPointerEqual(run.RuntimeNodeID, target.NodeID) &&
		stringPointerEqual(run.RuntimeWorkerID, target.WorkerID) &&
		uuidPointerEqual(run.RuntimeSessionID, target.RuntimeSessionID) &&
		uuidPointerEqual(run.LeaseTokenID, target.CredentialID) &&
		run.LeaseAcceptedAt != nil && attempt.AcceptedAt != nil &&
		run.LeaseAcceptedAt.Equal(*attempt.AcceptedAt) &&
		run.LeaseExpiresAt != nil && run.LeaseExpiresAt.Equal(attempt.LeaseExpiresAt) &&
		run.AttemptDeadlineAt != nil && run.AttemptDeadlineAt.Equal(attempt.AttemptDeadlineAt) &&
		run.RunDeadlineAt != nil &&
		attempt.AttemptNo != nil && attempt.FinishedAt == nil && attempt.ResultID == nil &&
		uuidPointerEqual(attempt.RuntimeTokenID, target.CredentialID) &&
		uuidPointerEqual(attempt.ActiveRuntimeSessionID, target.RuntimeSessionID) &&
		databaseNow.Before(attempt.LeaseExpiresAt) &&
		databaseNow.Before(attempt.AttemptDeadlineAt) &&
		databaseNow.Before(*run.LeaseExpiresAt) &&
		databaseNow.Before(*run.RunDeadlineAt)
}

// runtimeResumeSpoolCanWrite mirrors the common preconditions enforced by
// EventStore and ResultFinalizer for a first persisted cross-Session write.
// The replacement grant may outlive the source execution lease, but it never
// bypasses the active fence, cancellation, or Run/Attempt deadlines.
func runtimeResumeSpoolCanWrite(
	run db.LockRunForLeaseMutationRow,
	attempt db.RunAttempt,
	identity AttemptIdentity,
) bool {
	databaseNow := run.DatabaseNow
	return run.Status == string(RuntimeRunRunning) &&
		run.DispatchState == string(RuntimeDispatchExecuting) &&
		run.TerminalEventID == nil && run.CancelRequestID == nil &&
		uuidPointerEqual(run.LatestAttemptID, attempt.ID) &&
		uuidPointerEqual(run.ActiveAttemptID, attempt.ID) &&
		uuidPointerEqual(run.LeaseID, identity.LeaseID) &&
		run.FencingToken == identity.FencingToken &&
		uuidPointerEqual(run.RuntimeNodeID, identity.NodeID) &&
		stringPointerEqual(run.RuntimeWorkerID, identity.WorkerID) &&
		uuidPointerEqual(run.RuntimeSessionID, identity.RuntimeSessionID) &&
		attempt.RuntimeTokenID != nil && uuidPointerEqual(run.LeaseTokenID, *attempt.RuntimeTokenID) &&
		run.LeaseAcceptedAt != nil && attempt.AcceptedAt != nil &&
		run.LeaseAcceptedAt.Equal(*attempt.AcceptedAt) &&
		run.AttemptDeadlineAt != nil && run.AttemptDeadlineAt.Equal(attempt.AttemptDeadlineAt) &&
		run.RunDeadlineAt != nil &&
		attempt.AttemptNo != nil && attempt.FinishedAt == nil && attempt.ResultID == nil &&
		databaseNow.Before(attempt.AttemptDeadlineAt) &&
		databaseNow.Before(*run.RunDeadlineAt)
}

func runtimeResumeActiveGrantMatches(
	grant db.LockActiveRuntimeResumeGrantForAttemptTargetRow,
	target RuntimeSessionPrincipal,
	attempt db.RunAttempt,
	identity AttemptIdentity,
) bool {
	return grant.ID != uuid.Nil && grant.Permission == runtimeResumeUploadPermission &&
		grant.GrantedByCoreInstanceID != uuid.Nil &&
		!grant.GrantedAt.IsZero() && grant.ExpiresAt.After(grant.GrantedAt) &&
		grant.DatabaseNow.Before(grant.ExpiresAt) &&
		grant.RunID == identity.RunID && grant.AttemptID == identity.AttemptID &&
		grant.LeaseID == identity.LeaseID && grant.FencingToken == identity.FencingToken &&
		grant.AgentID == identity.AgentID && grant.NodeID == identity.NodeID &&
		grant.WorkerID == identity.WorkerID &&
		attempt.RuntimeSessionID != nil && grant.SourceSessionID == *attempt.RuntimeSessionID &&
		attempt.RuntimeTokenID != nil && grant.SourceCredentialID == *attempt.RuntimeTokenID &&
		grant.TargetSessionID == target.RuntimeSessionID &&
		grant.TargetCredentialID == target.CredentialID && grant.RevokedAt == nil
}

func runtimeResumeGrantMatches(
	grant db.RuntimeResumeGrant,
	target RuntimeSessionPrincipal,
	attempt db.RunAttempt,
	identity AttemptIdentity,
	coreInstanceID uuid.UUID,
) bool {
	return grant.ID != uuid.Nil && grant.Permission == runtimeResumeUploadPermission &&
		grant.GrantedByCoreInstanceID == coreInstanceID && grant.ExpiresAt.After(grant.GrantedAt) &&
		grant.RunID == identity.RunID && grant.AttemptID == identity.AttemptID &&
		grant.LeaseID == identity.LeaseID && grant.FencingToken == identity.FencingToken &&
		grant.AgentID == identity.AgentID && grant.NodeID == identity.NodeID &&
		grant.WorkerID == identity.WorkerID &&
		attempt.RuntimeSessionID != nil && grant.SourceSessionID == *attempt.RuntimeSessionID &&
		attempt.RuntimeTokenID != nil && grant.SourceCredentialID == *attempt.RuntimeTokenID &&
		grant.TargetSessionID == target.RuntimeSessionID &&
		grant.TargetCredentialID == target.CredentialID && grant.RevokedAt == nil
}

func runtimeResumeConsumedGrantMatches(
	consumed db.RuntimeResumeGrant,
	locked db.RuntimeResumeGrant,
	target RuntimeSessionPrincipal,
	attempt db.RunAttempt,
	identity AttemptIdentity,
) bool {
	return consumed.ID == locked.ID && consumed.FirstUsedAt != nil &&
		runtimeResumeGrantMatches(consumed, target, attempt, identity, locked.GrantedByCoreInstanceID)
}

func runtimeResumeGrantFromActive(row db.LockActiveRuntimeResumeGrantForAttemptTargetRow) db.RuntimeResumeGrant {
	return db.RuntimeResumeGrant{
		ID: row.ID, RunID: row.RunID, AttemptID: row.AttemptID,
		LeaseID: row.LeaseID, FencingToken: row.FencingToken,
		AgentID: row.AgentID, NodeID: row.NodeID, WorkerID: row.WorkerID,
		SourceSessionID: row.SourceSessionID, SourceCredentialID: row.SourceCredentialID,
		TargetSessionID: row.TargetSessionID, TargetCredentialID: row.TargetCredentialID,
		Permission: row.Permission, GrantedByCoreInstanceID: row.GrantedByCoreInstanceID,
		GrantedAt: row.GrantedAt, ExpiresAt: row.ExpiresAt, FirstUsedAt: row.FirstUsedAt,
		RevokedAt: row.RevokedAt, RevokedByType: row.RevokedByType,
		RevokedByID: row.RevokedByID, RevokeReason: row.RevokeReason,
	}
}

func revokedResumeDecision(identity AttemptIdentity) RunResumeAcceptedPayload {
	return RunResumeAcceptedPayload{
		AttemptIdentity: identity,
		Decision:        RuntimeResumeLeaseRevoked,
		AllowedActions: []RuntimeResumeAction{
			RuntimeActionStopExecution,
			RuntimeActionClearSpool,
		},
	}
}

func ackedResumeDecision(identity AttemptIdentity) RunResumeAcceptedPayload {
	return RunResumeAcceptedPayload{
		AttemptIdentity: identity,
		Decision:        RuntimeResumeResultAcked,
		AllowedActions:  []RuntimeResumeAction{RuntimeActionClearSpool},
	}
}

type orderedRuntimeResumeAttempt struct {
	originalIndex int
	attempt       ResumeAttempt
}

func sortRuntimeResumeAttempts(attempts []ResumeAttempt) []orderedRuntimeResumeAttempt {
	ordered := make([]orderedRuntimeResumeAttempt, len(attempts))
	for index, attempt := range attempts {
		ordered[index] = orderedRuntimeResumeAttempt{originalIndex: index, attempt: attempt}
	}
	sort.Slice(ordered, func(i, j int) bool {
		left := ordered[i].attempt.AttemptIdentity
		right := ordered[j].attempt.AttemptIdentity
		if comparison := bytes.Compare(left.RunID[:], right.RunID[:]); comparison != 0 {
			return comparison < 0
		}
		return bytes.Compare(left.AttemptID[:], right.AttemptID[:]) < 0
	})
	return ordered
}

type lockedRuntimeResumePrincipal struct {
	credential db.LockRuntimeCredentialForPrincipalValidationRow
}

func (p lockedRuntimeResumePrincipal) validAt(databaseNow time.Time) bool {
	return !databaseNow.IsZero() &&
		(p.credential.ExpiresAt == nil || databaseNow.Before(*p.credential.ExpiresAt))
}

func (s *RuntimeResumeService) lockTargetPrincipal(
	ctx context.Context,
	tx runtimeResumeTransaction,
	target RuntimeSessionPrincipal,
) (lockedRuntimeResumePrincipal, error) {
	coreInstanceID := s.coreInstanceID
	session, err := tx.LockRuntimeSessionForPrincipalValidation(ctx, db.LockRuntimeSessionForPrincipalValidationParams{
		RuntimeSessionID:       target.RuntimeSessionID,
		NodeID:                 target.NodeID,
		AgentID:                target.AgentID,
		CredentialID:           target.CredentialID,
		WorkerID:               target.WorkerID,
		AttachedCoreInstanceID: &coreInstanceID,
	})
	if err != nil {
		return lockedRuntimeResumePrincipal{}, runtimeResumePrincipalLockError(err)
	}
	if session.RuntimeSessionID != target.RuntimeSessionID || session.NodeID != target.NodeID ||
		session.AgentID != target.AgentID || session.CredentialID != target.CredentialID ||
		session.WorkerID != target.WorkerID || session.SessionEpoch != target.SessionEpoch ||
		session.DeviceCertificateSerial != target.DeviceCertificateSerial ||
		session.AttachedCoreInstanceID == nil || *session.AttachedCoreInstanceID != coreInstanceID {
		return lockedRuntimeResumePrincipal{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}

	node, err := tx.LockRuntimeNodeForPrincipalValidation(ctx, db.LockRuntimeNodeForPrincipalValidationParams{
		NodeID:                    target.NodeID,
		DeviceCertificateSerial:   target.DeviceCertificateSerial,
		DevicePublicKeyThumbprint: target.DevicePublicKeyThumbprintSHA256,
	})
	if err != nil {
		return lockedRuntimeResumePrincipal{}, runtimeResumePrincipalLockError(err)
	}
	agentID := target.AgentID
	credential, err := tx.LockRuntimeCredentialForPrincipalValidation(ctx, db.LockRuntimeCredentialForPrincipalValidationParams{
		CredentialID: target.CredentialID,
		AgentID:      &agentID,
	})
	if err != nil {
		return lockedRuntimeResumePrincipal{}, runtimeResumePrincipalLockError(err)
	}
	if node.NodeID != target.NodeID || node.DeviceCertificateSerial != target.DeviceCertificateSerial ||
		node.DevicePublicKeyThumbprint != target.DevicePublicKeyThumbprintSHA256 ||
		credential.ID != target.CredentialID || credential.AgentID == nil ||
		*credential.AgentID != target.AgentID {
		return lockedRuntimeResumePrincipal{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	if _, err = tx.LockRuntimeSessionAttachmentForPrincipalValidation(ctx, db.LockRuntimeSessionAttachmentForPrincipalValidationParams{
		AttachmentID:     target.AttachmentID,
		RuntimeSessionID: target.RuntimeSessionID,
		CoreInstanceID:   coreInstanceID,
	}); err != nil {
		return lockedRuntimeResumePrincipal{}, runtimeResumePrincipalLockError(err)
	}
	locked := lockedRuntimeResumePrincipal{credential: credential}
	if !locked.validAt(credential.DatabaseNow) {
		return lockedRuntimeResumePrincipal{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	return locked, nil
}

func runtimeResumePrincipalLockError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, err)
	}
	return err
}

func (s *RuntimeResumeService) validateResume(
	target RuntimeSessionPrincipal,
	payload RuntimeResumePayload,
) error {
	if s == nil || s.repository == nil || s.coreInstanceID == uuid.Nil ||
		target.RuntimeSessionID == uuid.Nil || target.NodeID == uuid.Nil ||
		target.AgentID == uuid.Nil || target.CredentialID == uuid.Nil ||
		target.AttachmentID == uuid.Nil || target.SessionEpoch < 1 || target.CoreInstanceID != s.coreInstanceID ||
		!validRuntimeIdentityText(target.WorkerID, 1, maxRuntimeSessionWorkerIDRunes) ||
		!validCertificateSerial(target.DeviceCertificateSerial) ||
		!validSHA256Hex(target.DevicePublicKeyThumbprintSHA256) ||
		(target.Status != "active" && target.Status != "draining") ||
		!validRuntimeResumeGrantTTL(s.grantTTL) {
		return newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}
	if err := ValidateRuntimePayload(payload); err != nil {
		return newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	if payload.RuntimeSessionID != target.RuntimeSessionID || payload.NodeID != target.NodeID ||
		payload.AgentID != target.AgentID || payload.WorkerID != target.WorkerID {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	return nil
}

func normalizeRuntimeResumeGrantTTL(value time.Duration) time.Duration {
	if value == 0 {
		return defaultRuntimeResumeGrantTTL
	}
	return value
}

func validRuntimeResumeGrantTTL(value time.Duration) bool {
	return value >= time.Millisecond && value <= maximumRuntimeResumeGrantTTL &&
		value%time.Millisecond == 0
}

type runtimeResumeRepository interface {
	WithTransaction(context.Context, func(runtimeResumeTransaction) error) error
}

type runtimeResumeTransaction interface {
	LockRuntimeSessionForPrincipalValidation(context.Context, db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error)
	LockRuntimeNodeForPrincipalValidation(context.Context, db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error)
	LockRuntimeCredentialForPrincipalValidation(context.Context, db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error)
	LockRuntimeSessionAttachmentForPrincipalValidation(context.Context, db.LockRuntimeSessionAttachmentForPrincipalValidationParams) (db.RuntimeSessionAttachment, error)
	LockRunForLeaseMutation(context.Context, uuid.UUID) (db.LockRunForLeaseMutationRow, error)
	LockRunAttemptForResult(context.Context, db.LockRunAttemptForResultParams) (db.RunAttempt, error)
	GetRunAttemptByResultID(context.Context, db.GetRunAttemptByResultIDParams) (db.RunAttempt, error)
	LockActiveRuntimeResumeGrantForAttemptTarget(context.Context, db.LockActiveRuntimeResumeGrantForAttemptTargetParams) (db.LockActiveRuntimeResumeGrantForAttemptTargetRow, error)
	CreateRuntimeResumeGrant(context.Context, db.CreateRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error)
	ConsumeRuntimeResumeGrant(context.Context, db.ConsumeRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error)
	RevokeRuntimeResumeGrant(context.Context, db.RevokeRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error)
}

type postgresRuntimeResumeRepository struct {
	pool *pgxpool.Pool
}

func (r *postgresRuntimeResumeRepository) WithTransaction(
	ctx context.Context,
	fn func(runtimeResumeTransaction) error,
) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("runtime resume repository is not configured")
	}
	return pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite,
	}, func(tx pgx.Tx) error {
		return fn(&postgresRuntimeResumeTransaction{queries: db.New(tx)})
	})
}

type postgresRuntimeResumeTransaction struct {
	queries *db.Queries
}

func (t *postgresRuntimeResumeTransaction) LockRuntimeSessionForPrincipalValidation(ctx context.Context, params db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeSessionForPrincipalValidation(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) LockRuntimeSessionAttachmentForPrincipalValidation(ctx context.Context, params db.LockRuntimeSessionAttachmentForPrincipalValidationParams) (db.RuntimeSessionAttachment, error) {
	return t.queries.LockRuntimeSessionAttachmentForPrincipalValidation(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) LockRuntimeNodeForPrincipalValidation(ctx context.Context, params db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeNodeForPrincipalValidation(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) LockRuntimeCredentialForPrincipalValidation(ctx context.Context, params db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeCredentialForPrincipalValidation(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) LockRunForLeaseMutation(ctx context.Context, id uuid.UUID) (db.LockRunForLeaseMutationRow, error) {
	return t.queries.LockRunForLeaseMutation(ctx, id)
}

func (t *postgresRuntimeResumeTransaction) LockRunAttemptForResult(ctx context.Context, params db.LockRunAttemptForResultParams) (db.RunAttempt, error) {
	return t.queries.LockRunAttemptForResult(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) GetRunAttemptByResultID(ctx context.Context, params db.GetRunAttemptByResultIDParams) (db.RunAttempt, error) {
	return t.queries.GetRunAttemptByResultID(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) LockActiveRuntimeResumeGrantForAttemptTarget(ctx context.Context, params db.LockActiveRuntimeResumeGrantForAttemptTargetParams) (db.LockActiveRuntimeResumeGrantForAttemptTargetRow, error) {
	return t.queries.LockActiveRuntimeResumeGrantForAttemptTarget(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) CreateRuntimeResumeGrant(ctx context.Context, params db.CreateRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error) {
	return t.queries.CreateRuntimeResumeGrant(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) ConsumeRuntimeResumeGrant(ctx context.Context, params db.ConsumeRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error) {
	return t.queries.ConsumeRuntimeResumeGrant(ctx, params)
}

func (t *postgresRuntimeResumeTransaction) RevokeRuntimeResumeGrant(ctx context.Context, params db.RevokeRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error) {
	return t.queries.RevokeRuntimeResumeGrant(ctx, params)
}
