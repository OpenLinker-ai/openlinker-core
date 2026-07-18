package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	defaultRuntimeAttemptTTL   = 30 * time.Minute
	defaultRuntimeHeartbeatTTL = 2 * time.Minute
)

// RuntimeLeaseConfig contains the database-enforced durations used by the
// reliable Runtime offer and execution state machine. Zero values select
// the protocol defaults; negative or sub-millisecond values fail closed.
type RuntimeLeaseConfig struct {
	OfferTTL     time.Duration
	LeaseTTL     time.Duration
	AttemptTTL   time.Duration
	HeartbeatTTL time.Duration
}

func DefaultRuntimeLeaseConfig() RuntimeLeaseConfig {
	return RuntimeLeaseConfig{
		OfferTTL:     RuntimeOfferTTLSeconds * time.Second,
		LeaseTTL:     RuntimeLeaseTTLSeconds * time.Second,
		AttemptTTL:   defaultRuntimeAttemptTTL,
		HeartbeatTTL: defaultRuntimeHeartbeatTTL,
	}
}

// RuntimeInvocationCapabilityIssuer issues the two assignment-scoped
// capabilities. Implementations must be deterministic for the same immutable
// Attempt evidence so a repeated claim returns byte-identical authority.
type RuntimeInvocationCapabilityIssuer interface {
	Issue(RuntimeInvocationCapability) (nodeEnvelope, invocationToken string, err error)
}

type RuntimeLeaseErrorCode string

const (
	RuntimeLeaseErrorValidationFailed RuntimeLeaseErrorCode = "VALIDATION_FAILED"
	RuntimeLeaseErrorIdentityMismatch RuntimeLeaseErrorCode = "LEASE_IDENTITY_MISMATCH"
	RuntimeLeaseErrorStaleLease       RuntimeLeaseErrorCode = "STALE_LEASE"
	RuntimeLeaseErrorLeaseExpired     RuntimeLeaseErrorCode = "LEASE_EXPIRED"
	RuntimeLeaseErrorNodeAtCapacity   RuntimeLeaseErrorCode = "NODE_AT_CAPACITY"
	RuntimeLeaseErrorRunTerminal      RuntimeLeaseErrorCode = "RUN_ALREADY_TERMINAL"
	RuntimeLeaseErrorCancelRequested  RuntimeLeaseErrorCode = "RUN_CANCEL_REQUESTED"
)

// RuntimeLeaseError is transport-neutral. HTTP and WebSocket adapters map the
// stable code without parsing its human-readable text.
type RuntimeLeaseError struct {
	Code  RuntimeLeaseErrorCode `json:"code"`
	cause error
}

func (e *RuntimeLeaseError) Error() string {
	if e == nil {
		return ""
	}
	switch e.Code {
	case RuntimeLeaseErrorValidationFailed:
		return "runtime lease validation failed"
	case RuntimeLeaseErrorIdentityMismatch:
		return "authenticated runtime identity does not own the lease"
	case RuntimeLeaseErrorStaleLease:
		return "runtime lease is stale"
	case RuntimeLeaseErrorLeaseExpired:
		return "runtime lease, offer, or execution deadline has expired"
	case RuntimeLeaseErrorNodeAtCapacity:
		return "runtime node is at capacity or unavailable for new work"
	case RuntimeLeaseErrorRunTerminal:
		return "run is already terminal"
	case RuntimeLeaseErrorCancelRequested:
		return "run cancellation has been requested"
	default:
		return "runtime lease operation rejected"
	}
}

func (e *RuntimeLeaseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func IsRuntimeLeaseError(err error, code RuntimeLeaseErrorCode) bool {
	var leaseErr *RuntimeLeaseError
	return errors.As(err, &leaseErr) && leaseErr.Code == code
}

func newRuntimeLeaseError(code RuntimeLeaseErrorCode, cause error) error {
	return &RuntimeLeaseError{Code: code, cause: cause}
}

// RuntimeLeaseService owns offer reservation, assignment confirmation,
// rejection, renewal, and disconnect cleanup. PostgreSQL is the only clock and
// linearization point; no state decision in this service uses process time.
type RuntimeLeaseService struct {
	repository     runtimeLeaseRepository
	coreInstanceID uuid.UUID
	issuer         RuntimeInvocationCapabilityIssuer
	config         RuntimeLeaseConfig
}

func NewRuntimeLeaseService(
	pool *pgxpool.Pool,
	coreInstanceID uuid.UUID,
	issuer RuntimeInvocationCapabilityIssuer,
	config RuntimeLeaseConfig,
) *RuntimeLeaseService {
	service := &RuntimeLeaseService{
		coreInstanceID: coreInstanceID,
		issuer:         issuer,
		config:         normalizeRuntimeLeaseConfig(config),
	}
	if pool != nil {
		service.repository = &postgresRuntimeLeaseRepository{pool: pool}
	}
	return service
}

func newRuntimeLeaseService(
	repository runtimeLeaseRepository,
	coreInstanceID uuid.UUID,
	issuer RuntimeInvocationCapabilityIssuer,
	config RuntimeLeaseConfig,
) *RuntimeLeaseService {
	return &RuntimeLeaseService{
		repository:     repository,
		coreInstanceID: coreInstanceID,
		issuer:         issuer,
		config:         normalizeRuntimeLeaseConfig(config),
	}
}

// ClaimOffer returns nil when no Run is currently claimable. An outstanding,
// unacknowledged offer for this Session is returned first and never reserves a
// second capacity slot. An expired offer is released in the same transaction
// before the next candidate is considered.
func (s *RuntimeLeaseService) ClaimOffer(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
) (*RunAssignedPayload, error) {
	if err := s.validateOperation(principal); err != nil {
		return nil, err
	}
	if s.issuer == nil {
		return nil, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}

	var assigned *RunAssignedPayload
	err := s.repository.WithTransaction(ctx, func(tx runtimeLeaseTransaction) error {
		// hard_maintenance is a persistent dispatch fence. Draining still
		// allows queued work to converge, while renew/Event/Result/cancel do
		// not pass through this new-claim gate.
		if gateErr := tx.RequireRuntimeClusterOperation(ctx, RuntimeClusterClaim); gateErr != nil {
			return gateErr
		}
		locked, err := s.lockPrincipal(ctx, tx, principal)
		if err != nil {
			return err
		}

		existing, err := tx.GetExistingUnacceptedRunOfferForSession(ctx, existingOfferParams(principal))
		switch {
		case err == nil:
			if existing.DatabaseNow.Before(existing.OfferExpiresAt) {
				payload, buildErr := s.assignmentFromExisting(principal, existing)
				if buildErr != nil {
					return buildErr
				}
				assigned = &payload
				return nil
			}
			if err = s.finishAndReleaseOffer(ctx, tx, principal, offerEvidenceFromExisting(existing), "offer_expired", "RUNTIME_OFFER_EXPIRED"); err != nil {
				return err
			}
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}

		candidate, err := tx.LockNextClaimableRuntimeRunForAgent(ctx, principal.AgentID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if candidate.AgentID != principal.AgentID || candidate.RunDeadlineAt == nil {
			return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
		}

		heartbeatAfter := locked.session.DatabaseNow.Add(-s.config.HeartbeatTTL)
		if _, err = tx.ClaimRuntimeSessionSlot(ctx, db.ClaimRuntimeSessionSlotParams{
			RuntimeSessionID: principal.RuntimeSessionID,
			AgentID:          principal.AgentID,
			CoreInstanceID:   s.coreInstanceID,
			HeartbeatAfter:   heartbeatAfter,
		}); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorNodeAtCapacity, err)
			}
			return err
		}
		if _, err = tx.ClaimRuntimeNodeSlot(ctx, db.ClaimRuntimeNodeSlotParams{
			NodeID:        principal.NodeID,
			LastSeenAfter: heartbeatAfter,
		}); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorNodeAtCapacity, err)
			}
			return err
		}

		attemptID, leaseID := uuid.New(), uuid.New()
		attempt, err := tx.CreateRuntimeRunOffer(ctx, db.CreateRuntimeRunOfferParams{
			AttemptID:        attemptID,
			LeaseID:          leaseID,
			CoreInstanceID:   s.coreInstanceID,
			OfferTtlMs:       s.config.OfferTTL.Milliseconds(),
			LeaseTtlMs:       s.config.LeaseTTL.Milliseconds(),
			AttemptTtlMs:     s.config.AttemptTTL.Milliseconds(),
			RuntimeSessionID: principal.RuntimeSessionID,
			RunID:            candidate.ID,
			NodeID:           principal.NodeID,
			CredentialID:     principal.CredentialID,
			WorkerID:         principal.WorkerID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
			}
			return err
		}
		if attempt.ID != attemptID || attempt.LeaseID != leaseID || attempt.RunID != candidate.ID {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		mirrored, err := tx.MirrorRuntimeRunOffer(ctx, mirrorOfferParams(principal, attempt))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
			}
			return err
		}
		if mirrored.ID != attempt.RunID || mirrored.DispatchState != string(RuntimeDispatchOffered) ||
			!uuidPointerEqual(mirrored.ActiveAttemptID, attempt.ID) || !uuidPointerEqual(mirrored.LeaseID, attempt.LeaseID) ||
			mirrored.FencingToken != attempt.FencingToken {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		payload, err := s.assignmentFromAttempt(principal, attempt, candidate.Input, candidate.RequestMetadata, *candidate.RunDeadlineAt)
		if err != nil {
			return err
		}
		assigned = &payload
		return nil
	})
	if err != nil {
		return nil, err
	}
	return assigned, nil
}

func (s *RuntimeLeaseService) AckAssignment(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
	request RunAssignmentAckPayload,
) (RunAssignmentConfirmedPayload, error) {
	if err := s.validateOperation(principal); err != nil {
		return RunAssignmentConfirmedPayload{}, err
	}
	if err := ValidateRuntimePayload(request); err != nil {
		return RunAssignmentConfirmedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	if err := validatePrincipalAttemptIdentity(principal, request.AttemptIdentity); err != nil {
		return RunAssignmentConfirmedPayload{}, err
	}

	var confirmed RunAssignmentConfirmedPayload
	err := s.repository.WithTransaction(ctx, func(tx runtimeLeaseTransaction) error {
		locked, err := s.lockPrincipal(ctx, tx, principal)
		if err != nil {
			return err
		}
		run, attempt, err := lockLeaseMutation(ctx, tx, principal, request.AttemptIdentity)
		if err != nil {
			return err
		}
		if err = rejectCanceledOrTerminalRun(run); err != nil {
			return err
		}

		if attempt.AcceptedAt != nil {
			if attempt.FinishedAt != nil || attempt.AttemptNo == nil || *attempt.AttemptNo < 1 || attempt.LeaseExpiresAt.IsZero() ||
				run.DispatchState != string(RuntimeDispatchExecuting) ||
				!runOwnsAttempt(run, request.AttemptIdentity, principal.CredentialID) {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
			}
			confirmed = confirmedPayload(request.AttemptIdentity, attempt)
			return nil
		}
		if attempt.FinishedAt != nil || run.DispatchState != string(RuntimeDispatchOffered) ||
			!runOwnsAttempt(run, request.AttemptIdentity, principal.CredentialID) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		if leaseOfferExpired(run.DatabaseNow, run, attempt) {
			return newRuntimeLeaseError(RuntimeLeaseErrorLeaseExpired, nil)
		}

		accepted, err := tx.ConfirmRunAssignment(ctx, confirmAssignmentParams(principal, locked.attachment.ID, s.coreInstanceID, s.config.LeaseTTL, request.AttemptIdentity))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// All non-time guards were checked under the Session, Run, and
				// Attempt locks. A boundary crossing at clock_timestamp() is the
				// only legitimate remaining no-row result.
				return newRuntimeLeaseError(RuntimeLeaseErrorLeaseExpired, err)
			}
			return err
		}
		if !attemptMatchesLeaseIdentity(accepted, principal, request.AttemptIdentity) || accepted.AttemptNo == nil ||
			*accepted.AttemptNo < 1 || accepted.AcceptedAt == nil || accepted.LeaseExpiresAt.IsZero() ||
			accepted.RuntimeAttachmentID == nil || *accepted.RuntimeAttachmentID != locked.attachment.ID {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		mirrored, err := tx.MirrorRunConfirmedAssignment(ctx, mirrorConfirmedParams(principal, s.coreInstanceID, request.AttemptIdentity))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
			}
			return err
		}
		if mirrored.ID != request.AttemptIdentity.RunID || mirrored.DispatchState != string(RuntimeDispatchExecuting) ||
			mirrored.AttemptCount != *accepted.AttemptNo {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		confirmed = confirmedPayload(request.AttemptIdentity, accepted)
		return nil
	})
	return confirmed, err
}

func (s *RuntimeLeaseService) RejectAssignment(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
	request RunAssignmentRejectPayload,
) (RunAssignmentRejectedPayload, error) {
	if err := s.validateOperation(principal); err != nil {
		return RunAssignmentRejectedPayload{}, err
	}
	if err := ValidateRuntimePayload(request); err != nil {
		return RunAssignmentRejectedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	if !validRuntimeCapacityReport(request.Capacity, request.Inflight) {
		return RunAssignmentRejectedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}
	if err := validatePrincipalAttemptIdentity(principal, request.AttemptIdentity); err != nil {
		return RunAssignmentRejectedPayload{}, err
	}

	var rejected RunAssignmentRejectedPayload
	err := s.repository.WithTransaction(ctx, func(tx runtimeLeaseTransaction) error {
		if _, err := s.lockPrincipal(ctx, tx, principal); err != nil {
			return err
		}
		run, attempt, err := lockLeaseMutation(ctx, tx, principal, request.AttemptIdentity)
		if err != nil {
			return err
		}
		if isFinishedUnacceptedOffer(attempt) {
			state, stateErr := runtimeLeaseDispatchState(run.DispatchState)
			if stateErr != nil {
				return stateErr
			}
			rejected = RunAssignmentRejectedPayload{
				AttemptIdentity: request.AttemptIdentity,
				Outcome:         RuntimeOfferRejected,
				DispatchState:   state,
			}
			return nil
		}
		if err = rejectCanceledOrTerminalRun(run); err != nil {
			return err
		}
		if attempt.AcceptedAt != nil || attempt.FinishedAt != nil || run.DispatchState != string(RuntimeDispatchOffered) ||
			!runOwnsAttempt(run, request.AttemptIdentity, principal.CredentialID) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}

		evidence := offerEvidenceFromAttempt(attempt)
		if err = s.finishAndReleaseOffer(ctx, tx, principal, evidence, "offer_rejected", string(request.ReasonCode)); err != nil {
			return err
		}
		rejected = RunAssignmentRejectedPayload{
			AttemptIdentity: request.AttemptIdentity,
			Outcome:         RuntimeOfferRejected,
			DispatchState:   RuntimeDispatchPending,
		}
		return nil
	})
	return rejected, err
}

func (s *RuntimeLeaseService) RenewLease(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
	request RunLeaseRenewPayload,
) (RunLeaseRenewedPayload, error) {
	if err := s.validateOperation(principal); err != nil {
		return RunLeaseRenewedPayload{}, err
	}
	if err := ValidateRuntimePayload(request); err != nil {
		return RunLeaseRenewedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	if !validRuntimeCapacityReport(request.Capacity, request.Inflight) {
		return RunLeaseRenewedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}
	if err := validatePrincipalAttemptIdentity(principal, request.AttemptIdentity); err != nil {
		return RunLeaseRenewedPayload{}, err
	}

	var renewed RunLeaseRenewedPayload
	err := s.repository.WithTransaction(ctx, func(tx runtimeLeaseTransaction) error {
		if _, err := s.lockPrincipal(ctx, tx, principal); err != nil {
			return err
		}
		run, attempt, err := lockLeaseMutation(ctx, tx, principal, request.AttemptIdentity)
		if err != nil {
			return err
		}
		if err = rejectCanceledOrTerminalRun(run); err != nil {
			return err
		}
		if attempt.AcceptedAt == nil || attempt.AttemptNo == nil || attempt.FinishedAt != nil ||
			run.DispatchState != string(RuntimeDispatchExecuting) || !runOwnsAttempt(run, request.AttemptIdentity, principal.CredentialID) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		if executionLeaseExpired(run.DatabaseNow, run, attempt) {
			return newRuntimeLeaseError(RuntimeLeaseErrorLeaseExpired, nil)
		}

		updated, err := tx.RenewRuntimeRunAttempt(ctx, renewAttemptParams(principal, s.coreInstanceID, s.config.LeaseTTL, request.AttemptIdentity))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorLeaseExpired, err)
			}
			return err
		}
		if !attemptMatchesLeaseIdentity(updated, principal, request.AttemptIdentity) || updated.AttemptNo == nil ||
			updated.AcceptedAt == nil || updated.FinishedAt != nil || updated.LeaseExpiresAt.IsZero() {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		mirrored, err := tx.MirrorRunLeaseRenewal(ctx, db.MirrorRunLeaseRenewalParams{
			RunID:          request.AttemptIdentity.RunID,
			AttemptID:      request.AttemptIdentity.AttemptID,
			LeaseID:        request.AttemptIdentity.LeaseID,
			FencingToken:   request.AttemptIdentity.FencingToken,
			CoreInstanceID: s.coreInstanceID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
			}
			return err
		}
		if mirrored.ID != request.AttemptIdentity.RunID || mirrored.DispatchState != string(RuntimeDispatchExecuting) ||
			mirrored.LeaseExpiresAt == nil || !mirrored.LeaseExpiresAt.Equal(updated.LeaseExpiresAt) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
		}
		renewed = RunLeaseRenewedPayload{
			AttemptIdentity: request.AttemptIdentity,
			LeaseExpiresAt:  updated.LeaseExpiresAt,
			PendingCommand:  nil,
		}
		return nil
	})
	return renewed, err
}

// ReleaseUnackedOffer is used by a failed send, ACK timeout, or Session close.
// It deliberately searches only for the Session's current unaccepted offer;
// an accepted executing Attempt is left untouched for resume or expiry.
func (s *RuntimeLeaseService) ReleaseUnackedOffer(
	ctx context.Context,
	principal RuntimeSessionPrincipal,
	reason ...string,
) error {
	if err := s.validateOperation(principal); err != nil {
		return err
	}
	if len(reason) > 1 {
		return newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}
	reasonCode := "RUNTIME_OFFER_RELEASED"
	if len(reason) > 0 && strings.TrimSpace(reason[0]) != "" {
		reasonCode = strings.TrimSpace(reason[0])
	}
	if len(reasonCode) > 120 {
		return newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}

	return s.repository.WithTransaction(ctx, func(tx runtimeLeaseTransaction) error {
		if err := s.lockOfferReleasePrincipal(ctx, tx, principal); err != nil {
			return err
		}
		existing, err := tx.GetExistingUnacceptedRunOfferForSession(ctx, existingOfferParams(principal))
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		return s.finishAndReleaseOffer(ctx, tx, principal, offerEvidenceFromExisting(existing), "offer_rejected", reasonCode)
	})
}

func (s *RuntimeLeaseService) lockOfferReleasePrincipal(
	ctx context.Context,
	tx runtimeLeaseTransaction,
	principal RuntimeSessionPrincipal,
) error {
	session, err := tx.LockRuntimeSessionForOfferRelease(ctx, db.LockRuntimeSessionForOfferReleaseParams{
		RuntimeSessionID: principal.RuntimeSessionID,
		NodeID:           principal.NodeID,
		AgentID:          principal.AgentID,
		CredentialID:     principal.CredentialID,
		WorkerID:         principal.WorkerID,
		CoreInstanceID:   s.coreInstanceID,
	})
	if err != nil {
		return principalLockError(err)
	}
	if session.SessionEpoch != principal.SessionEpoch ||
		session.DeviceCertificateSerial != principal.DeviceCertificateSerial ||
		(session.Status != "active" && session.Status != "draining" &&
			session.Status != "offline" && session.Status != "closed") {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}

	node, err := tx.LockRuntimeNodeForPrincipalValidation(ctx, db.LockRuntimeNodeForPrincipalValidationParams{
		NodeID:                    principal.NodeID,
		DeviceCertificateSerial:   principal.DeviceCertificateSerial,
		DevicePublicKeyThumbprint: principal.DevicePublicKeyThumbprintSHA256,
	})
	if err != nil {
		return principalLockError(err)
	}
	credential, err := tx.LockRuntimeCredentialForPrincipalValidation(ctx, db.LockRuntimeCredentialForPrincipalValidationParams{
		CredentialID: principal.CredentialID,
		AgentID:      &principal.AgentID,
	})
	if err != nil {
		return principalLockError(err)
	}
	if node.NodeID != principal.NodeID || credential.AgentID == nil || *credential.AgentID != principal.AgentID {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	_, err = tx.LockRuntimeSessionAttachmentForOfferRelease(ctx, db.LockRuntimeSessionAttachmentForOfferReleaseParams{
		AttachmentID:     principal.AttachmentID,
		RuntimeSessionID: principal.RuntimeSessionID,
		CoreInstanceID:   s.coreInstanceID,
		Detached:         session.Status == "offline" || session.Status == "closed",
	})
	if err != nil {
		return principalLockError(err)
	}
	return nil
}

type lockedRuntimeLeasePrincipal struct {
	session    db.LockRuntimeSessionForPrincipalValidationRow
	node       db.LockRuntimeNodeForPrincipalValidationRow
	credential db.LockRuntimeCredentialForPrincipalValidationRow
	attachment db.RuntimeSessionAttachment
}

func (s *RuntimeLeaseService) lockPrincipal(
	ctx context.Context,
	tx runtimeLeaseTransaction,
	principal RuntimeSessionPrincipal,
) (lockedRuntimeLeasePrincipal, error) {
	coreInstanceID := s.coreInstanceID
	session, err := tx.LockRuntimeSessionForPrincipalValidation(ctx, db.LockRuntimeSessionForPrincipalValidationParams{
		RuntimeSessionID:       principal.RuntimeSessionID,
		NodeID:                 principal.NodeID,
		AgentID:                principal.AgentID,
		CredentialID:           principal.CredentialID,
		WorkerID:               principal.WorkerID,
		AttachedCoreInstanceID: &coreInstanceID,
	})
	if err != nil {
		return lockedRuntimeLeasePrincipal{}, principalLockError(err)
	}
	if session.SessionEpoch != principal.SessionEpoch || session.DeviceCertificateSerial != principal.DeviceCertificateSerial ||
		session.RuntimeContractDigest != principal.RuntimeContractDigest {
		return lockedRuntimeLeasePrincipal{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}

	node, err := tx.LockRuntimeNodeForPrincipalValidation(ctx, db.LockRuntimeNodeForPrincipalValidationParams{
		NodeID:                    principal.NodeID,
		DeviceCertificateSerial:   principal.DeviceCertificateSerial,
		DevicePublicKeyThumbprint: principal.DevicePublicKeyThumbprintSHA256,
	})
	if err != nil {
		return lockedRuntimeLeasePrincipal{}, principalLockError(err)
	}
	credential, err := tx.LockRuntimeCredentialForPrincipalValidation(ctx, db.LockRuntimeCredentialForPrincipalValidationParams{
		CredentialID: principal.CredentialID,
		AgentID:      &principal.AgentID,
	})
	if err != nil {
		return lockedRuntimeLeasePrincipal{}, principalLockError(err)
	}
	if credential.AgentID == nil || *credential.AgentID != principal.AgentID || node.NodeID != principal.NodeID ||
		node.RuntimeContractDigest != principal.RuntimeContractDigest {
		return lockedRuntimeLeasePrincipal{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	attachment, err := tx.LockRuntimeSessionAttachmentForPrincipalValidation(ctx, db.LockRuntimeSessionAttachmentForPrincipalValidationParams{
		AttachmentID:     principal.AttachmentID,
		RuntimeSessionID: principal.RuntimeSessionID,
		CoreInstanceID:   coreInstanceID,
	})
	if err != nil {
		return lockedRuntimeLeasePrincipal{}, principalLockError(err)
	}
	return lockedRuntimeLeasePrincipal{session: session, node: node, credential: credential, attachment: attachment}, nil
}

func principalLockError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, err)
	}
	return err
}

func (s *RuntimeLeaseService) validateOperation(principal RuntimeSessionPrincipal) error {
	if s == nil || s.repository == nil || s.coreInstanceID == uuid.Nil ||
		principal.RuntimeSessionID == uuid.Nil || principal.NodeID == uuid.Nil || principal.AgentID == uuid.Nil ||
		principal.CredentialID == uuid.Nil || principal.AttachmentID == uuid.Nil ||
		!runtimeWireContractSupported(principal.RuntimeContractDigest) ||
		principal.SessionEpoch < 1 || principal.CoreInstanceID != s.coreInstanceID ||
		!validRuntimeIdentityText(principal.WorkerID, 1, maxRuntimeSessionWorkerIDRunes) ||
		!validCertificateSerial(principal.DeviceCertificateSerial) ||
		!validSHA256Hex(principal.DevicePublicKeyThumbprintSHA256) || !validRuntimeLeaseConfig(s.config) {
		return newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}
	return nil
}

func normalizeRuntimeLeaseConfig(config RuntimeLeaseConfig) RuntimeLeaseConfig {
	defaults := DefaultRuntimeLeaseConfig()
	if config.OfferTTL == 0 {
		config.OfferTTL = defaults.OfferTTL
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = defaults.LeaseTTL
	}
	if config.AttemptTTL == 0 {
		config.AttemptTTL = defaults.AttemptTTL
	}
	if config.HeartbeatTTL == 0 {
		config.HeartbeatTTL = defaults.HeartbeatTTL
	}
	return config
}

func validRuntimeLeaseConfig(config RuntimeLeaseConfig) bool {
	return validRuntimeLeaseDuration(config.OfferTTL, 5*time.Minute) &&
		validRuntimeLeaseDuration(config.LeaseTTL, time.Hour) &&
		validRuntimeLeaseDuration(config.AttemptTTL, 24*time.Hour) &&
		validRuntimeLeaseDuration(config.HeartbeatTTL, time.Hour)
}

func validRuntimeLeaseDuration(value, maximum time.Duration) bool {
	return value >= time.Millisecond && value <= maximum && value%time.Millisecond == 0
}

func validRuntimeCapacityReport(capacity, inflight int64) bool {
	return capacity >= 0 && capacity <= RuntimeMaximumNodeCapacity &&
		inflight >= 0 && inflight <= RuntimeMaximumNodeCapacity
}

func validatePrincipalAttemptIdentity(principal RuntimeSessionPrincipal, identity AttemptIdentity) error {
	if identity.AgentID != principal.AgentID || identity.NodeID != principal.NodeID ||
		identity.RuntimeSessionID != principal.RuntimeSessionID || identity.WorkerID != principal.WorkerID {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	return nil
}

func lockLeaseMutation(
	ctx context.Context,
	tx runtimeLeaseTransaction,
	principal RuntimeSessionPrincipal,
	identity AttemptIdentity,
) (db.LockRunForLeaseMutationRow, db.RunAttempt, error) {
	run, err := tx.LockRunForLeaseMutation(ctx, identity.RunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.LockRunForLeaseMutationRow{}, db.RunAttempt{}, newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return db.LockRunForLeaseMutationRow{}, db.RunAttempt{}, err
	}
	if run.AgentID != principal.AgentID || identity.AgentID != run.AgentID {
		return db.LockRunForLeaseMutationRow{}, db.RunAttempt{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	attempt, err := tx.LockRuntimeRunAttemptForLeaseMutation(ctx, attemptLockParams(principal, identity))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.LockRunForLeaseMutationRow{}, db.RunAttempt{}, newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return db.LockRunForLeaseMutationRow{}, db.RunAttempt{}, err
	}
	if attempt.AgentID != principal.AgentID {
		return db.LockRunForLeaseMutationRow{}, db.RunAttempt{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	if !attemptMatchesLeaseIdentity(attempt, principal, identity) {
		return db.LockRunForLeaseMutationRow{}, db.RunAttempt{}, newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
	}
	return run, attempt, nil
}

func rejectCanceledOrTerminalRun(run db.LockRunForLeaseMutationRow) error {
	if run.CancelRequestID != nil {
		return newRuntimeLeaseError(RuntimeLeaseErrorCancelRequested, nil)
	}
	if run.Status != string(RuntimeRunRunning) || run.TerminalEventID != nil ||
		run.DispatchState == string(RuntimeDispatchTerminal) || run.DispatchState == string(RuntimeDispatchDeadLetter) {
		return newRuntimeLeaseError(RuntimeLeaseErrorRunTerminal, nil)
	}
	return nil
}

func runOwnsAttempt(run db.LockRunForLeaseMutationRow, identity AttemptIdentity, credentialID uuid.UUID) bool {
	return uuidPointerEqual(run.ActiveAttemptID, identity.AttemptID) &&
		uuidPointerEqual(run.LeaseID, identity.LeaseID) && run.FencingToken == identity.FencingToken &&
		uuidPointerEqual(run.RuntimeNodeID, identity.NodeID) &&
		uuidPointerEqual(run.RuntimeSessionID, identity.RuntimeSessionID) &&
		uuidPointerEqual(run.LeaseTokenID, credentialID) &&
		stringPointerEqual(run.RuntimeWorkerID, identity.WorkerID)
}

func uuidPointerEqual(value *uuid.UUID, want uuid.UUID) bool {
	return value != nil && *value == want
}

func stringPointerEqual(value *string, want string) bool {
	return value != nil && *value == want
}

func attemptMatchesLeaseIdentity(attempt db.RunAttempt, principal RuntimeSessionPrincipal, identity AttemptIdentity) bool {
	return attempt.ID == identity.AttemptID && attempt.RunID == identity.RunID && attempt.AgentID == identity.AgentID &&
		attempt.ExecutorType == "runtime" && attempt.LeaseID == identity.LeaseID && attempt.FencingToken == identity.FencingToken &&
		uuidPointerEqual(attempt.RuntimeTokenID, principal.CredentialID) && uuidPointerEqual(attempt.NodeID, identity.NodeID) &&
		uuidPointerEqual(attempt.RuntimeSessionID, identity.RuntimeSessionID) && stringPointerEqual(attempt.RuntimeWorkerID, identity.WorkerID)
}

func leaseOfferExpired(databaseNow time.Time, run db.LockRunForLeaseMutationRow, attempt db.RunAttempt) bool {
	return !databaseNow.Before(attempt.OfferExpiresAt) || !databaseNow.Before(attempt.AttemptDeadlineAt) ||
		run.RunDeadlineAt == nil || !databaseNow.Before(*run.RunDeadlineAt)
}

func executionLeaseExpired(databaseNow time.Time, run db.LockRunForLeaseMutationRow, attempt db.RunAttempt) bool {
	return !databaseNow.Before(attempt.LeaseExpiresAt) || !databaseNow.Before(attempt.AttemptDeadlineAt) ||
		run.LeaseExpiresAt == nil || !databaseNow.Before(*run.LeaseExpiresAt) ||
		run.RunDeadlineAt == nil || !databaseNow.Before(*run.RunDeadlineAt)
}

func confirmedPayload(identity AttemptIdentity, attempt db.RunAttempt) RunAssignmentConfirmedPayload {
	attemptNo := int64(0)
	if attempt.AttemptNo != nil {
		attemptNo = int64(*attempt.AttemptNo)
	}
	return RunAssignmentConfirmedPayload{
		AttemptIdentity: identity,
		AttemptNo:       attemptNo,
		LeaseExpiresAt:  attempt.LeaseExpiresAt,
	}
}

func isFinishedUnacceptedOffer(attempt db.RunAttempt) bool {
	if attempt.FinishedAt == nil || attempt.AcceptedAt != nil || attempt.AttemptNo != nil || attempt.Outcome == nil {
		return false
	}
	return *attempt.Outcome == "offer_rejected" || *attempt.Outcome == "offer_expired"
}

func runtimeLeaseDispatchState(value string) (RuntimeDispatchState, error) {
	state := RuntimeDispatchState(value)
	if !validRuntimeDispatchState(state) {
		return "", newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
	}
	return state, nil
}

type runtimeOfferEvidence struct {
	RunID        uuid.UUID
	AttemptID    uuid.UUID
	LeaseID      uuid.UUID
	FencingToken int64
}

func offerEvidenceFromAttempt(attempt db.RunAttempt) runtimeOfferEvidence {
	return runtimeOfferEvidence{
		RunID: attempt.RunID, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
	}
}

func offerEvidenceFromExisting(existing db.GetExistingUnacceptedRunOfferForSessionRow) runtimeOfferEvidence {
	return runtimeOfferEvidence{
		RunID: existing.RunID, AttemptID: existing.AttemptID, LeaseID: existing.LeaseID, FencingToken: existing.FencingToken,
	}
}

func (s *RuntimeLeaseService) finishAndReleaseOffer(
	ctx context.Context,
	tx runtimeLeaseTransaction,
	principal RuntimeSessionPrincipal,
	evidence runtimeOfferEvidence,
	outcome string,
	reasonCode string,
) error {
	workerID := principal.WorkerID
	sessionID, nodeID, credentialID := principal.RuntimeSessionID, principal.NodeID, principal.CredentialID
	coreInstanceID := s.coreInstanceID
	if _, err := tx.FinishUnacceptedRunOffer(ctx, db.FinishUnacceptedRunOfferParams{
		Outcome:          &outcome,
		ErrorCode:        &reasonCode,
		RunID:            evidence.RunID,
		AttemptID:        evidence.AttemptID,
		LeaseID:          evidence.LeaseID,
		FencingToken:     evidence.FencingToken,
		RuntimeSessionID: &sessionID,
		NodeID:           &nodeID,
		CredentialID:     &credentialID,
		WorkerID:         &workerID,
		CoreInstanceID:   &coreInstanceID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return err
	}
	capacity, err := tx.MarkRunAttemptCapacityReleased(ctx, db.MarkRunAttemptCapacityReleasedParams{
		RunID: evidence.RunID, AttemptID: evidence.AttemptID, LeaseID: evidence.LeaseID, FencingToken: evidence.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// Another idempotent path already consumed this Attempt's capacity
		// evidence. Only the CAS winner may decrement the shared counters.
		return nil
	}
	if err != nil {
		return err
	}
	if capacity.RuntimeSessionID != principal.RuntimeSessionID || capacity.NodeID != principal.NodeID || capacity.SlotAcquiredAt.IsZero() ||
		capacity.SlotReleasedAt.IsZero() || capacity.SlotReleasedAt.Before(capacity.SlotAcquiredAt) {
		return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
	}
	if _, err := tx.ReleaseRuntimeSessionSlot(ctx, capacity.RuntimeSessionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return err
	}
	if _, err := tx.ReleaseRuntimeNodeSlot(ctx, capacity.NodeID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return err
	}
	if _, err := tx.ResetRunAfterUnacceptedOffer(ctx, db.ResetRunAfterUnacceptedOfferParams{
		RunID: evidence.RunID, AttemptID: evidence.AttemptID, LeaseID: evidence.LeaseID, FencingToken: evidence.FencingToken,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return err
	}
	payload, err := CanonicalizeRFC8785(map[string]any{"run_id": evidence.RunID.String()})
	if err != nil {
		return err
	}
	if _, err = tx.CreateRuntimeSignal(ctx, db.CreateRuntimeSignalParams{
		EventType: "run.available", AgentID: principal.AgentID, RunID: &evidence.RunID, Payload: payload,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return err
	}
	return nil
}

func (s *RuntimeLeaseService) assignmentFromExisting(
	principal RuntimeSessionPrincipal,
	existing db.GetExistingUnacceptedRunOfferForSessionRow,
) (RunAssignedPayload, error) {
	if existing.RuntimeTokenID == nil || *existing.RuntimeTokenID != principal.CredentialID ||
		existing.RuntimeWorkerID == nil || existing.RuntimeSessionID == nil || existing.NodeID == nil ||
		existing.RunDeadlineAt == nil {
		return RunAssignedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
	}
	attempt := db.RunAttempt{
		ID: existing.AttemptID, RunID: existing.RunID, AgentID: existing.AgentID, OfferNo: existing.OfferNo,
		ExecutorType: "runtime", LeaseID: existing.LeaseID, FencingToken: existing.FencingToken, RuntimeTokenID: existing.RuntimeTokenID,
		RuntimeWorkerID: existing.RuntimeWorkerID, RuntimeSessionID: existing.RuntimeSessionID, NodeID: existing.NodeID,
		OfferedByCoreInstanceID: existing.OfferedByCoreInstanceID, AttachedCoreInstanceID: existing.AttachedCoreInstanceID,
		OfferedAt: existing.OfferedAt, OfferExpiresAt: existing.OfferExpiresAt, LeaseExpiresAt: existing.LeaseExpiresAt,
		AttemptDeadlineAt: existing.AttemptDeadlineAt,
	}
	return s.assignmentFromAttempt(principal, attempt, existing.Input, existing.RequestMetadata, *existing.RunDeadlineAt)
}

func (s *RuntimeLeaseService) assignmentFromAttempt(
	principal RuntimeSessionPrincipal,
	attempt db.RunAttempt,
	rawInput []byte,
	rawMetadata []byte,
	runDeadlineAt time.Time,
) (RunAssignedPayload, error) {
	identity, err := attemptIdentityFromRow(attempt)
	if err != nil {
		return RunAssignedPayload{}, err
	}
	if err = validatePrincipalAttemptIdentity(principal, identity); err != nil {
		return RunAssignedPayload{}, err
	}
	if !attemptMatchesLeaseIdentity(attempt, principal, identity) {
		return RunAssignedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	input, canonicalInput, err := decodeRuntimeJSONObject(rawInput)
	if err != nil {
		return RunAssignedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	metadata, _, err := decodeRuntimeJSONObject(rawMetadata)
	if err != nil {
		return RunAssignedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	digest := sha256.Sum256(canonicalInput)
	nodeEnvelope, invocationToken, err := s.issuer.Issue(RuntimeInvocationCapability{
		RunID: attempt.RunID, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
		AgentID: attempt.AgentID, CredentialID: principal.CredentialID, NodeID: principal.NodeID,
		WorkerID: principal.WorkerID, RuntimeSessionID: principal.RuntimeSessionID, InputSHA256: digest,
		IssuedAt: attempt.OfferedAt, ExpiresAt: attempt.AttemptDeadlineAt,
	})
	if err != nil {
		return RunAssignedPayload{}, err
	}
	if strings.TrimSpace(nodeEnvelope) == "" || strings.TrimSpace(invocationToken) == "" || runDeadlineAt.IsZero() {
		return RunAssignedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, nil)
	}
	payload := RunAssignedPayload{
		AttemptIdentity: identity, OfferNo: int64(attempt.OfferNo), OfferExpiresAt: attempt.OfferExpiresAt,
		AttemptDeadlineAt: attempt.AttemptDeadlineAt, RunDeadlineAt: runDeadlineAt, Input: input, Metadata: metadata,
		NodeEnvelope: nodeEnvelope, AgentInvocationToken: invocationToken,
	}
	if err = ValidateRuntimePayload(payload); err != nil {
		return RunAssignedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	return payload, nil
}

func attemptIdentityFromRow(attempt db.RunAttempt) (AttemptIdentity, error) {
	if attempt.RuntimeWorkerID == nil || attempt.RuntimeSessionID == nil || attempt.NodeID == nil ||
		attempt.RuntimeTokenID == nil {
		return AttemptIdentity{}, newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
	}
	identity := AttemptIdentity{
		RunID: attempt.RunID, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
		NodeID: *attempt.NodeID, AgentID: attempt.AgentID, WorkerID: *attempt.RuntimeWorkerID,
		RuntimeSessionID: *attempt.RuntimeSessionID,
	}
	if err := validateAttemptIdentity(identity); err != nil {
		return AttemptIdentity{}, newRuntimeLeaseError(RuntimeLeaseErrorValidationFailed, err)
	}
	return identity, nil
}

func decodeRuntimeJSONObject(raw []byte) (map[string]any, []byte, error) {
	if len(raw) == 0 || int64(len(raw)) > MaxRuntimeMessageBytes {
		return nil, nil, errors.New("runtime JSON object is empty or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, nil, errors.New("runtime JSON value must be an object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("runtime JSON object has trailing data")
	}
	canonical, err := CanonicalizeRFC8785(object)
	if err != nil {
		return nil, nil, err
	}
	return object, canonical, nil
}

func existingOfferParams(principal RuntimeSessionPrincipal) db.GetExistingUnacceptedRunOfferForSessionParams {
	sessionID, nodeID, credentialID, workerID := principal.RuntimeSessionID, principal.NodeID, principal.CredentialID, principal.WorkerID
	return db.GetExistingUnacceptedRunOfferForSessionParams{
		RuntimeSessionID: &sessionID, NodeID: &nodeID, AgentID: principal.AgentID,
		CredentialID: &credentialID, WorkerID: &workerID,
	}
}

func attemptLockParams(principal RuntimeSessionPrincipal, identity AttemptIdentity) db.LockRuntimeRunAttemptForLeaseMutationParams {
	sessionID, nodeID, credentialID, workerID := principal.RuntimeSessionID, principal.NodeID, principal.CredentialID, principal.WorkerID
	return db.LockRuntimeRunAttemptForLeaseMutationParams{
		RunID: identity.RunID, AttemptID: identity.AttemptID, LeaseID: identity.LeaseID, FencingToken: identity.FencingToken,
		RuntimeSessionID: &sessionID, NodeID: &nodeID, CredentialID: &credentialID, WorkerID: &workerID,
	}
}

func mirrorOfferParams(principal RuntimeSessionPrincipal, attempt db.RunAttempt) db.MirrorRuntimeRunOfferParams {
	sessionID, nodeID, credentialID, workerID := principal.RuntimeSessionID, principal.NodeID, principal.CredentialID, principal.WorkerID
	return db.MirrorRuntimeRunOfferParams{
		RunID: attempt.RunID, AttemptID: attempt.ID, LeaseID: attempt.LeaseID, FencingToken: attempt.FencingToken,
		RuntimeSessionID: &sessionID, NodeID: &nodeID, CredentialID: &credentialID, WorkerID: &workerID,
		CoreInstanceID: principal.CoreInstanceID,
	}
}

func confirmAssignmentParams(principal RuntimeSessionPrincipal, attachmentID, coreInstanceID uuid.UUID, ttl time.Duration, identity AttemptIdentity) db.ConfirmRunAssignmentParams {
	sessionID, nodeID, credentialID, workerID := principal.RuntimeSessionID, principal.NodeID, principal.CredentialID, principal.WorkerID
	return db.ConfirmRunAssignmentParams{
		LeaseTtlMs: ttl.Milliseconds(), CoreInstanceID: coreInstanceID, RunID: identity.RunID,
		AttemptID: identity.AttemptID, LeaseID: identity.LeaseID, FencingToken: identity.FencingToken,
		RuntimeSessionID: &sessionID, NodeID: &nodeID, CredentialID: &credentialID, WorkerID: &workerID,
		AttachmentID: attachmentID,
	}
}

func mirrorConfirmedParams(principal RuntimeSessionPrincipal, coreInstanceID uuid.UUID, identity AttemptIdentity) db.MirrorRunConfirmedAssignmentParams {
	sessionID, nodeID, credentialID, workerID := principal.RuntimeSessionID, principal.NodeID, principal.CredentialID, principal.WorkerID
	return db.MirrorRunConfirmedAssignmentParams{
		RunID: identity.RunID, AttemptID: identity.AttemptID, LeaseID: identity.LeaseID, FencingToken: identity.FencingToken,
		RuntimeSessionID: &sessionID, NodeID: &nodeID, CredentialID: &credentialID, WorkerID: &workerID,
		CoreInstanceID: coreInstanceID,
	}
}

func renewAttemptParams(principal RuntimeSessionPrincipal, coreInstanceID uuid.UUID, ttl time.Duration, identity AttemptIdentity) db.RenewRuntimeRunAttemptParams {
	sessionID, nodeID, credentialID, workerID := principal.RuntimeSessionID, principal.NodeID, principal.CredentialID, principal.WorkerID
	return db.RenewRuntimeRunAttemptParams{
		LeaseTtlMs: ttl.Milliseconds(), CoreInstanceID: coreInstanceID, RunID: identity.RunID,
		AttemptID: identity.AttemptID, LeaseID: identity.LeaseID, FencingToken: identity.FencingToken,
		RuntimeSessionID: &sessionID, NodeID: &nodeID, CredentialID: &credentialID, WorkerID: &workerID,
	}
}

type runtimeLeaseRepository interface {
	WithTransaction(context.Context, func(runtimeLeaseTransaction) error) error
}

type runtimeLeaseTransaction interface {
	RequireRuntimeClusterOperation(context.Context, RuntimeClusterOperation) error
	LockRuntimeSessionForPrincipalValidation(context.Context, db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error)
	LockRuntimeSessionForOfferRelease(context.Context, db.LockRuntimeSessionForOfferReleaseParams) (db.LockRuntimeSessionForOfferReleaseRow, error)
	LockRuntimeSessionAttachmentForPrincipalValidation(context.Context, db.LockRuntimeSessionAttachmentForPrincipalValidationParams) (db.RuntimeSessionAttachment, error)
	LockRuntimeSessionAttachmentForOfferRelease(context.Context, db.LockRuntimeSessionAttachmentForOfferReleaseParams) (db.RuntimeSessionAttachment, error)
	LockRuntimeNodeForPrincipalValidation(context.Context, db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error)
	LockRuntimeCredentialForPrincipalValidation(context.Context, db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error)
	GetExistingUnacceptedRunOfferForSession(context.Context, db.GetExistingUnacceptedRunOfferForSessionParams) (db.GetExistingUnacceptedRunOfferForSessionRow, error)
	LockNextClaimableRuntimeRunForAgent(context.Context, uuid.UUID) (db.LockNextClaimableRuntimeRunForAgentRow, error)
	ClaimRuntimeSessionSlot(context.Context, db.ClaimRuntimeSessionSlotParams) (db.RuntimeSession, error)
	ClaimRuntimeNodeSlot(context.Context, db.ClaimRuntimeNodeSlotParams) (db.RuntimeNode, error)
	ReleaseRuntimeSessionSlot(context.Context, uuid.UUID) (db.RuntimeSession, error)
	ReleaseRuntimeNodeSlot(context.Context, uuid.UUID) (db.RuntimeNode, error)
	CreateRuntimeRunOffer(context.Context, db.CreateRuntimeRunOfferParams) (db.RunAttempt, error)
	MirrorRuntimeRunOffer(context.Context, db.MirrorRuntimeRunOfferParams) (db.MirrorRuntimeRunOfferRow, error)
	LockRunForLeaseMutation(context.Context, uuid.UUID) (db.LockRunForLeaseMutationRow, error)
	LockRuntimeRunAttemptForLeaseMutation(context.Context, db.LockRuntimeRunAttemptForLeaseMutationParams) (db.RunAttempt, error)
	ConfirmRunAssignment(context.Context, db.ConfirmRunAssignmentParams) (db.RunAttempt, error)
	MirrorRunConfirmedAssignment(context.Context, db.MirrorRunConfirmedAssignmentParams) (db.MirrorRunConfirmedAssignmentRow, error)
	RenewRuntimeRunAttempt(context.Context, db.RenewRuntimeRunAttemptParams) (db.RunAttempt, error)
	MirrorRunLeaseRenewal(context.Context, db.MirrorRunLeaseRenewalParams) (db.MirrorRunLeaseRenewalRow, error)
	FinishUnacceptedRunOffer(context.Context, db.FinishUnacceptedRunOfferParams) (db.RunAttempt, error)
	ResetRunAfterUnacceptedOffer(context.Context, db.ResetRunAfterUnacceptedOfferParams) (db.ResetRunAfterUnacceptedOfferRow, error)
	MarkRunAttemptCapacityReleased(context.Context, db.MarkRunAttemptCapacityReleasedParams) (db.MarkRunAttemptCapacityReleasedRow, error)
	CreateRuntimeSignal(context.Context, db.CreateRuntimeSignalParams) (db.RuntimeSignalOutbox, error)
}

type postgresRuntimeLeaseRepository struct {
	pool *pgxpool.Pool
}

func (r *postgresRuntimeLeaseRepository) WithTransaction(
	ctx context.Context,
	fn func(runtimeLeaseTransaction) error,
) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("runtime lease repository is not configured")
	}
	return pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite,
	}, func(tx pgx.Tx) error {
		return fn(&postgresRuntimeLeaseTransaction{tx: tx, queries: db.New(tx)})
	})
}

type postgresRuntimeLeaseTransaction struct {
	tx      pgx.Tx
	queries *db.Queries
}

func (t *postgresRuntimeLeaseTransaction) RequireRuntimeClusterOperation(ctx context.Context, operation RuntimeClusterOperation) error {
	return RequireRuntimeClusterOperation(ctx, t.tx, operation)
}

func (t *postgresRuntimeLeaseTransaction) LockRuntimeSessionForPrincipalValidation(ctx context.Context, params db.LockRuntimeSessionForPrincipalValidationParams) (db.LockRuntimeSessionForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeSessionForPrincipalValidation(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) LockRuntimeSessionForOfferRelease(ctx context.Context, params db.LockRuntimeSessionForOfferReleaseParams) (db.LockRuntimeSessionForOfferReleaseRow, error) {
	return t.queries.LockRuntimeSessionForOfferRelease(ctx, params)
}

func (t *postgresRuntimeLeaseTransaction) LockRuntimeSessionAttachmentForPrincipalValidation(ctx context.Context, params db.LockRuntimeSessionAttachmentForPrincipalValidationParams) (db.RuntimeSessionAttachment, error) {
	return t.queries.LockRuntimeSessionAttachmentForPrincipalValidation(ctx, params)
}

func (t *postgresRuntimeLeaseTransaction) LockRuntimeSessionAttachmentForOfferRelease(ctx context.Context, params db.LockRuntimeSessionAttachmentForOfferReleaseParams) (db.RuntimeSessionAttachment, error) {
	return t.queries.LockRuntimeSessionAttachmentForOfferRelease(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) LockRuntimeNodeForPrincipalValidation(ctx context.Context, params db.LockRuntimeNodeForPrincipalValidationParams) (db.LockRuntimeNodeForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeNodeForPrincipalValidation(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) LockRuntimeCredentialForPrincipalValidation(ctx context.Context, params db.LockRuntimeCredentialForPrincipalValidationParams) (db.LockRuntimeCredentialForPrincipalValidationRow, error) {
	return t.queries.LockRuntimeCredentialForPrincipalValidation(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) GetExistingUnacceptedRunOfferForSession(ctx context.Context, params db.GetExistingUnacceptedRunOfferForSessionParams) (db.GetExistingUnacceptedRunOfferForSessionRow, error) {
	return t.queries.GetExistingUnacceptedRunOfferForSession(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) LockNextClaimableRuntimeRunForAgent(ctx context.Context, agentID uuid.UUID) (db.LockNextClaimableRuntimeRunForAgentRow, error) {
	return t.queries.LockNextClaimableRuntimeRunForAgent(ctx, agentID)
}
func (t *postgresRuntimeLeaseTransaction) ClaimRuntimeSessionSlot(ctx context.Context, params db.ClaimRuntimeSessionSlotParams) (db.RuntimeSession, error) {
	return t.queries.ClaimRuntimeSessionSlot(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) ClaimRuntimeNodeSlot(ctx context.Context, params db.ClaimRuntimeNodeSlotParams) (db.RuntimeNode, error) {
	return t.queries.ClaimRuntimeNodeSlot(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) ReleaseRuntimeSessionSlot(ctx context.Context, id uuid.UUID) (db.RuntimeSession, error) {
	return t.queries.ReleaseRuntimeSessionSlot(ctx, id)
}
func (t *postgresRuntimeLeaseTransaction) ReleaseRuntimeNodeSlot(ctx context.Context, id uuid.UUID) (db.RuntimeNode, error) {
	return t.queries.ReleaseRuntimeNodeSlot(ctx, id)
}
func (t *postgresRuntimeLeaseTransaction) CreateRuntimeRunOffer(ctx context.Context, params db.CreateRuntimeRunOfferParams) (db.RunAttempt, error) {
	return t.queries.CreateRuntimeRunOffer(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) MirrorRuntimeRunOffer(ctx context.Context, params db.MirrorRuntimeRunOfferParams) (db.MirrorRuntimeRunOfferRow, error) {
	return t.queries.MirrorRuntimeRunOffer(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) LockRunForLeaseMutation(ctx context.Context, id uuid.UUID) (db.LockRunForLeaseMutationRow, error) {
	return t.queries.LockRunForLeaseMutation(ctx, id)
}
func (t *postgresRuntimeLeaseTransaction) LockRuntimeRunAttemptForLeaseMutation(ctx context.Context, params db.LockRuntimeRunAttemptForLeaseMutationParams) (db.RunAttempt, error) {
	return t.queries.LockRuntimeRunAttemptForLeaseMutation(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) ConfirmRunAssignment(ctx context.Context, params db.ConfirmRunAssignmentParams) (db.RunAttempt, error) {
	return t.queries.ConfirmRunAssignment(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) MirrorRunConfirmedAssignment(ctx context.Context, params db.MirrorRunConfirmedAssignmentParams) (db.MirrorRunConfirmedAssignmentRow, error) {
	return t.queries.MirrorRunConfirmedAssignment(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) RenewRuntimeRunAttempt(ctx context.Context, params db.RenewRuntimeRunAttemptParams) (db.RunAttempt, error) {
	return t.queries.RenewRuntimeRunAttempt(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) MirrorRunLeaseRenewal(ctx context.Context, params db.MirrorRunLeaseRenewalParams) (db.MirrorRunLeaseRenewalRow, error) {
	return t.queries.MirrorRunLeaseRenewal(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) FinishUnacceptedRunOffer(ctx context.Context, params db.FinishUnacceptedRunOfferParams) (db.RunAttempt, error) {
	return t.queries.FinishUnacceptedRunOffer(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) ResetRunAfterUnacceptedOffer(ctx context.Context, params db.ResetRunAfterUnacceptedOfferParams) (db.ResetRunAfterUnacceptedOfferRow, error) {
	return t.queries.ResetRunAfterUnacceptedOffer(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) MarkRunAttemptCapacityReleased(ctx context.Context, params db.MarkRunAttemptCapacityReleasedParams) (db.MarkRunAttemptCapacityReleasedRow, error) {
	return t.queries.MarkRunAttemptCapacityReleased(ctx, params)
}
func (t *postgresRuntimeLeaseTransaction) CreateRuntimeSignal(ctx context.Context, params db.CreateRuntimeSignalParams) (db.RuntimeSignalOutbox, error) {
	return t.queries.CreateRuntimeSignal(ctx, params)
}
