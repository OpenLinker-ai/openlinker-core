package runtime

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	runtimeCallAgentPath            = "/api/v1/agent-runtime/call-agent"
	defaultRuntimeDelegationDepth   = 8
	defaultRuntimeDelegationRunning = 500
)

// RuntimeInvocationVerifier verifies the two domain-separated capabilities
// emitted with one assignment. Both must decode to the same immutable Attempt
// authority before an Agent may create a child Run.
type RuntimeInvocationVerifier interface {
	VerifyNodeEnvelope(string, time.Time) (RuntimeInvocationCapability, error)
	VerifyInvocationToken(string, time.Time) (RuntimeInvocationCapability, error)
}

// RuntimeDelegationAuthorization contains transport-authenticated evidence.
// ProofRequest.Body is the exact byte sequence read from HTTP and later decoded
// as Payload; callers must not marshal the body a second time.
type RuntimeDelegationAuthorization struct {
	Device            RuntimeDeviceIdentity
	InvocationContext string
	InvocationToken   string
	InvocationProof   string
	IdempotencyKey    string
	ProofRequest      RuntimeInvocationProofRequest
}

// RuntimeDelegationService creates a child Run only while the signed parent
// Attempt is accepted, current, uncanceled, and unexpired. PostgreSQL locks and
// time are authoritative; the preliminary capability read is only used to
// assemble a candidate request before the creation transaction.
type RuntimeDelegationService struct {
	pool               *pgxpool.Pool
	queries            *db.Queries
	runtime            *Service
	verifier           RuntimeInvocationVerifier
	maxDepth           int32
	maxRunningChildren int32
}

func NewRuntimeDelegationService(
	pool *pgxpool.Pool,
	runtimeService *Service,
	verifier RuntimeInvocationVerifier,
) *RuntimeDelegationService {
	service := &RuntimeDelegationService{
		pool:               pool,
		runtime:            runtimeService,
		verifier:           verifier,
		maxDepth:           defaultRuntimeDelegationDepth,
		maxRunningChildren: defaultRuntimeDelegationRunning,
	}
	if pool != nil {
		service.queries = db.New(pool)
	}
	return service
}

// ResolveInvocationDevice verifies the assignment-scoped Bearer capability
// against PostgreSQL time, then resolves the same durable token-to-key binding
// used by ordinary Runtime requests. It is used only when mTLS is explicitly
// disabled; request bodies and headers never establish Node identity.
func (s *RuntimeDelegationService) ResolveInvocationDevice(
	ctx context.Context,
	invocationToken string,
) (RuntimeDeviceIdentity, error) {
	if s == nil || s.pool == nil || s.verifier == nil || strings.TrimSpace(invocationToken) == "" {
		return RuntimeDeviceIdentity{}, runtimeUnauthorizedError(ErrInvalidRuntimeInvocation)
	}
	var databaseNow time.Time
	if err := s.pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return RuntimeDeviceIdentity{}, runtimeDatabaseUnavailable(err)
	}
	capability, err := s.verifier.VerifyInvocationToken(invocationToken, databaseNow)
	if err != nil {
		return RuntimeDeviceIdentity{}, runtimeUnauthorizedError(err)
	}
	var device RuntimeDeviceIdentity
	var agentID uuid.UUID
	var status string
	err = s.pool.QueryRow(ctx, `
SELECT node.node_id, binding.agent_id, node.device_certificate_serial,
       certificate.certificate_fingerprint, binding.public_key_thumbprint,
       node.status
FROM runtime_node_bindings binding
JOIN runtime_nodes node ON node.node_id = binding.node_id
JOIN LATERAL (
    SELECT issued.certificate_fingerprint
    FROM runtime_node_certificates issued
    WHERE issued.node_id = node.node_id
      AND issued.public_key_thumbprint = binding.public_key_thumbprint
    ORDER BY issued.not_after DESC, issued.issued_at DESC
    LIMIT 1
) certificate ON TRUE
WHERE binding.credential_id = $1`, capability.CredentialID).Scan(
		&device.NodeID,
		&agentID,
		&device.CertificateSerial,
		&device.CertificateFingerprintSHA256,
		&device.PublicKeyThumbprintSHA256,
		&status,
	)
	if err != nil || device.NodeID != capability.NodeID || agentID != capability.AgentID ||
		(status != "active" && status != "draining") {
		return RuntimeDeviceIdentity{}, runtimeUnauthorizedError(ErrInvalidRuntimeInvocation)
	}
	return device, nil
}

func (s *RuntimeDelegationService) CallAgent(
	ctx context.Context,
	authorization RuntimeDelegationAuthorization,
) (RunSummary, error) {
	if s == nil || s.pool == nil || s.queries == nil || s.runtime == nil || s.verifier == nil {
		return RunSummary{}, runtimeUnavailableError()
	}
	if !validRuntimeDelegationAuthorization(authorization) {
		return RunSummary{}, runtimeUnauthorizedError(ErrInvalidRuntimeInvocation)
	}

	var databaseNow time.Time
	if err := s.pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return RunSummary{}, runtimeDatabaseUnavailable(err)
	}
	capability, err := verifyRuntimeDelegationCapabilityPair(
		s.verifier,
		authorization.InvocationContext,
		authorization.InvocationToken,
		databaseNow,
	)
	if err != nil {
		if errors.Is(err, ErrExpiredRuntimeInvocation) {
			return RunSummary{}, newRuntimeLeaseError(RuntimeLeaseErrorLeaseExpired, err)
		}
		return RunSummary{}, runtimeUnauthorizedError(err)
	}
	if capability.NodeID != authorization.Device.NodeID {
		return RunSummary{}, newRuntimeTransportError(
			RuntimeErrorPermissionDenied, runtimeErrorDefaultMessage(RuntimeErrorPermissionDenied), nil,
		)
	}
	if _, err = HashIdempotencyKey(authorization.IdempotencyKey); err != nil {
		return RunSummary{}, newRuntimeTransportError(
			RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed), err,
		)
	}
	if err = VerifyRuntimeInvocationProof(
		authorization.InvocationToken,
		authorization.InvocationProof,
		authorization.ProofRequest,
	); err != nil {
		return RunSummary{}, runtimeUnauthorizedError(err)
	}
	var request CallAgentRequest
	if err = decodeRuntimeJSON(authorization.ProofRequest.Body, &request); err != nil {
		return RunSummary{}, err
	}
	if err = ValidateRuntimePayload(request); err != nil {
		return RunSummary{}, newRuntimeTransportError(
			RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed), err,
		)
	}
	if capability.AgentID == request.TargetAgentID {
		return RunSummary{}, newRuntimeTransportError(
			RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed), nil,
		)
	}

	parentUserID, parentAgentID, err := s.preliminaryParent(ctx, capability.RunID)
	if err != nil {
		return RunSummary{}, err
	}
	if parentAgentID != capability.AgentID {
		return RunSummary{}, newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	if err = s.validateDelegationTarget(ctx, capability, request.TargetAgentID); err != nil {
		return RunSummary{}, err
	}
	childContext, err := s.childA2AContext(ctx, capability, request.TargetAgentID)
	if err != nil {
		return RunSummary{}, err
	}

	runRequest := &RunRequest{
		AgentID:          request.TargetAgentID.String(),
		Input:            request.Input,
		Metadata:         request.Metadata,
		A2AContext:       childContext,
		IdempotencyKey:   authorization.IdempotencyKey,
		CreationProtocol: "runtime-v2",
		CreationMethod:   "call-agent",
	}
	delegation := Delegation{
		ParentRunID:   capability.RunID,
		CallerAgentID: capability.AgentID,
		Reason:        strings.TrimSpace(request.Reason),
	}
	invocation, response, err := s.runtime.createRunningRun(
		ctx,
		parentUserID,
		runRequest,
		"api",
		createRunOptions{
			delegation: &delegation,
			beforeCreate: func(txCtx context.Context, tx pgx.Tx) error {
				return s.authorizeChildCreation(
					txCtx, tx, authorization, capability, parentUserID,
				)
			},
		},
	)
	if err != nil {
		return RunSummary{}, err
	}
	if response == nil {
		return RunSummary{}, newRuntimeTransportError(
			RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), nil,
		)
	}
	runID, err := uuid.Parse(response.RunID)
	if err != nil {
		return RunSummary{}, newRuntimeTransportError(
			RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), err,
		)
	}
	if invocation != nil {
		if s.runtime.isQueuedRuntime(invocation) {
			s.runtime.recordRunEventBestEffort(ctx, invocation.runID, "run.dispatch.pending", map[string]interface{}{
				"connection_mode": invocation.agent.ConnectionMode,
				"agent_id":        invocation.agent.ID.String(),
			})
		} else {
			s.runtime.executeRunAsync(invocation)
		}
	}
	return s.runSummary(ctx, runID)
}

func (s *RuntimeDelegationService) preliminaryParent(
	ctx context.Context,
	runID uuid.UUID,
) (uuid.UUID, uuid.UUID, error) {
	var userID, agentID uuid.UUID
	err := s.pool.QueryRow(ctx, `
SELECT user_id, agent_id
FROM runs
WHERE id = $1
  AND runtime_contract_id = 'openlinker.runtime.v2'`, runID).Scan(&userID, &agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, uuid.Nil, newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
	}
	if err != nil {
		return uuid.Nil, uuid.Nil, runtimeDatabaseUnavailable(err)
	}
	return userID, agentID, nil
}

func (s *RuntimeDelegationService) validateDelegationTarget(
	ctx context.Context,
	capability RuntimeInvocationCapability,
	targetAgentID uuid.UUID,
) error {
	caller, err := s.queries.GetAgentByID(ctx, capability.AgentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, err)
	}
	if err != nil {
		return runtimeDatabaseUnavailable(err)
	}
	target, err := s.queries.GetAgentByID(ctx, targetAgentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeTransportError(RuntimeErrorNotFound, runtimeErrorDefaultMessage(RuntimeErrorNotFound), err)
	}
	if err != nil {
		return runtimeDatabaseUnavailable(err)
	}
	policy, err := s.queries.GetAgentCallPolicy(ctx, targetAgentID)
	if err != nil {
		return runtimeDatabaseUnavailable(err)
	}
	if policy == "private" || (policy == "same_creator" && caller.CreatorID != target.CreatorID) {
		return newRuntimeTransportError(
			RuntimeErrorPermissionDenied, runtimeErrorDefaultMessage(RuntimeErrorPermissionDenied), nil,
		)
	}
	if s.maxDepth > 0 {
		lineage, lineageErr := s.queries.ListDelegationLineage(ctx, db.ListDelegationLineageParams{
			RunID: capability.RunID, MaxDepth: s.maxDepth + 1,
		})
		if lineageErr != nil {
			return runtimeDatabaseUnavailable(lineageErr)
		}
		for _, ancestorAgentID := range lineage {
			if ancestorAgentID == targetAgentID {
				return newRuntimeTransportError(
					RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed), nil,
				)
			}
		}
		if int32(len(lineage)) > s.maxDepth {
			return newRuntimeTransportError(
				RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed), nil,
			)
		}
	}
	if s.maxRunningChildren > 0 {
		count, countErr := s.queries.CountRunningDelegations(ctx)
		if countErr != nil {
			return runtimeDatabaseUnavailable(countErr)
		}
		if count >= s.maxRunningChildren {
			err := newRuntimeTransportError(
				RuntimeErrorRateLimited, runtimeErrorDefaultMessage(RuntimeErrorRateLimited), nil,
			)
			err.Body.Retryable = true
			return err
		}
	}
	return nil
}

func (s *RuntimeDelegationService) childA2AContext(
	ctx context.Context,
	capability RuntimeInvocationCapability,
	targetAgentID uuid.UUID,
) (*RunA2AContextRequest, error) {
	parentTaskID := capability.RunID.String()
	contextID := "ctx-" + capability.RunID.String()
	rootContextID := contextID
	parentContextID := ""
	traceID := rootContextID
	references := []string{parentTaskID}

	mapping, err := s.queries.GetA2AContextMappingByRun(ctx, capability.RunID)
	switch {
	case err == nil:
		parentContextID = strings.TrimSpace(mapping.ProtocolContextID)
		if strings.TrimSpace(mapping.ProtocolTaskID) != "" {
			parentTaskID = strings.TrimSpace(mapping.ProtocolTaskID)
		}
		contextID = parentContextID
		if contextID == "" {
			contextID = strings.TrimSpace(mapping.RootContextID)
		}
		if contextID == "" {
			contextID = "ctx-" + capability.RunID.String()
		}
		rootContextID = strings.TrimSpace(mapping.RootContextID)
		if rootContextID == "" {
			rootContextID = contextID
		}
		traceID = strings.TrimSpace(mapping.TraceID)
		if traceID == "" {
			traceID = rootContextID
		}
		references = append(append([]string(nil), mapping.ReferenceTaskIDs...), parentTaskID)
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return nil, runtimeDatabaseUnavailable(err)
	}

	return &RunA2AContextRequest{
		ProtocolContextID: contextID,
		RootContextID:     rootContextID,
		ParentContextID:   parentContextID,
		ParentTaskID:      parentTaskID,
		ParentRunID:       capability.RunID.String(),
		CallerAgentID:     capability.AgentID.String(),
		TargetAgentID:     targetAgentID.String(),
		TraceID:           traceID,
		ReferenceTaskIDs:  normalizeA2AReferenceTaskIDs(references),
		Source:            "agent_delegation",
	}, nil
}

func (s *RuntimeDelegationService) runSummary(ctx context.Context, runID uuid.UUID) (RunSummary, error) {
	var summary RunSummary
	summary.RunID = runID
	err := s.pool.QueryRow(ctx, `
SELECT status, dispatch_state
FROM runs
WHERE id = $1
  AND runtime_contract_id = 'openlinker.runtime.v2'`, runID).Scan(&summary.Status, &summary.DispatchState)
	if errors.Is(err, pgx.ErrNoRows) {
		return RunSummary{}, newRuntimeTransportError(RuntimeErrorNotFound, runtimeErrorDefaultMessage(RuntimeErrorNotFound), err)
	}
	if err != nil {
		return RunSummary{}, runtimeDatabaseUnavailable(err)
	}
	if err = ValidateRuntimePayload(summary); err != nil {
		return RunSummary{}, newRuntimeTransportError(
			RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), err,
		)
	}
	return summary, nil
}

func (s *RuntimeDelegationService) authorizeChildCreation(
	ctx context.Context,
	tx pgx.Tx,
	authorization RuntimeDelegationAuthorization,
	preliminary RuntimeInvocationCapability,
	parentUserID uuid.UUID,
) error {
	capability := preliminary
	var sessionEpoch int64
	var sessionRuntimeContractDigest string
	if err := tx.QueryRow(ctx, `
SELECT s.session_epoch, s.runtime_contract_digest
FROM runtime_sessions s
JOIN runtime_wire_contracts wire
  ON wire.runtime_contract_id = s.runtime_contract_id
 AND wire.runtime_contract_digest = s.runtime_contract_digest
 AND wire.support_tier IN ('current', 'previous')
WHERE s.runtime_session_id = $1
  AND s.node_id = $2
  AND s.agent_id = $3
  AND s.credential_id = $4
  AND s.worker_id = $5
  AND s.device_certificate_serial = $6
  AND s.status IN ('active', 'draining')
  AND s.protocol_version = 2
  AND s.runtime_contract_id = 'openlinker.runtime.v2'
FOR UPDATE OF s`,
		capability.RuntimeSessionID,
		capability.NodeID,
		capability.AgentID,
		capability.CredentialID,
		capability.WorkerID,
		authorization.Device.CertificateSerial,
	).Scan(&sessionEpoch, &sessionRuntimeContractDigest); err != nil {
		return runtimeDelegationPrincipalLockError(err)
	}
	if sessionEpoch < 1 || !runtimeWireContractSupported(sessionRuntimeContractDigest) {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}

	var lockedNodeID uuid.UUID
	if err := tx.QueryRow(ctx, `
SELECT n.node_id
FROM runtime_nodes n
WHERE n.node_id = $1
  AND n.device_certificate_serial = $2
  AND n.device_public_key_thumbprint = $3
  AND n.status IN ('active', 'draining')
  AND n.protocol_version = 2
  AND n.runtime_contract_id = 'openlinker.runtime.v2'
  AND n.runtime_contract_digest = $4
  AND n.revoked_at IS NULL
FOR UPDATE OF n`,
		capability.NodeID,
		authorization.Device.CertificateSerial,
		authorization.Device.PublicKeyThumbprintSHA256,
		sessionRuntimeContractDigest,
	).Scan(&lockedNodeID); err != nil {
		return runtimeDelegationPrincipalLockError(err)
	}
	var lockedCredentialID uuid.UUID
	var credentialExpiresAt *time.Time
	if err := tx.QueryRow(ctx, `
SELECT t.id, t.expires_at
FROM agent_tokens t
WHERE t.id = $1
  AND t.agent_id = $2
  AND t.status = 'active_runtime'
  AND t.revoked_at IS NULL
  AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())
  AND t.scopes @> ARRAY['agent:pull']::text[]
FOR SHARE OF t`, capability.CredentialID, capability.AgentID).Scan(
		&lockedCredentialID, &credentialExpiresAt,
	); err != nil {
		return runtimeDelegationPrincipalLockError(err)
	}
	if lockedNodeID != capability.NodeID || lockedCredentialID != capability.CredentialID {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}

	lockedRun, err := lockRuntimeDelegatingRun(ctx, tx, capability.RunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return err
	}
	if lockedRun.userID != parentUserID || lockedRun.agentID != capability.AgentID {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	if credentialExpiresAt != nil && !lockedRun.databaseNow.Before(*credentialExpiresAt) {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	if lockedRun.cancelRequestID != nil || lockedRun.cancelState != nil {
		return newRuntimeLeaseError(RuntimeLeaseErrorCancelRequested, nil)
	}
	if lockedRun.status != string(RuntimeRunRunning) || lockedRun.terminalEventID != nil {
		return newRuntimeLeaseError(RuntimeLeaseErrorRunTerminal, nil)
	}
	if lockedRun.dispatchState != string(RuntimeDispatchExecuting) {
		return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
	}

	attempt, err := lockRuntimeDelegatingAttempt(ctx, tx, capability)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
		}
		return err
	}
	verified, err := verifyRuntimeDelegationCapabilityPair(
		s.verifier,
		authorization.InvocationContext,
		authorization.InvocationToken,
		lockedRun.databaseNow,
	)
	if err != nil {
		if errors.Is(err, ErrExpiredRuntimeInvocation) {
			return newRuntimeLeaseError(RuntimeLeaseErrorLeaseExpired, err)
		}
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, err)
	}
	if !runtimeInvocationCapabilitiesEqual(preliminary, verified) ||
		verified.NodeID != authorization.Device.NodeID ||
		!runtimeDelegatingRunOwnsCapability(lockedRun, verified) ||
		attempt.acceptedAt == nil || attempt.finishedAt != nil || attempt.outcome != nil || attempt.resultID != nil ||
		!attempt.offeredAt.Equal(verified.IssuedAt) ||
		!attempt.attemptDeadlineAt.Equal(verified.ExpiresAt) ||
		lockedRun.leaseAcceptedAt == nil || !lockedRun.leaseAcceptedAt.Equal(*attempt.acceptedAt) ||
		lockedRun.leaseExpiresAt == nil || !lockedRun.leaseExpiresAt.Equal(attempt.leaseExpiresAt) ||
		lockedRun.attemptDeadlineAt == nil || !lockedRun.attemptDeadlineAt.Equal(attempt.attemptDeadlineAt) {
		return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil)
	}
	if !lockedRun.databaseNow.Before(attempt.leaseExpiresAt) ||
		!lockedRun.databaseNow.Before(attempt.attemptDeadlineAt) ||
		lockedRun.runDeadlineAt == nil || !lockedRun.databaseNow.Before(*lockedRun.runDeadlineAt) {
		return newRuntimeLeaseError(RuntimeLeaseErrorLeaseExpired, nil)
	}
	_, canonicalInput, err := decodeRuntimeJSONObject(lockedRun.input)
	if err != nil {
		return newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, err)
	}
	inputDigest := sha256.Sum256(canonicalInput)
	if subtle.ConstantTimeCompare(inputDigest[:], verified.InputSHA256[:]) != 1 {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil)
	}
	return nil
}

type runtimeDelegatingRun struct {
	userID            uuid.UUID
	agentID           uuid.UUID
	input             []byte
	status            string
	dispatchState     string
	activeAttemptID   *uuid.UUID
	leaseID           *uuid.UUID
	fencingToken      int64
	runtimeNodeID     *uuid.UUID
	runtimeWorkerID   *string
	runtimeSessionID  *uuid.UUID
	leaseTokenID      *uuid.UUID
	leaseAcceptedAt   *time.Time
	leaseExpiresAt    *time.Time
	attemptDeadlineAt *time.Time
	runDeadlineAt     *time.Time
	cancelRequestID   *uuid.UUID
	cancelState       *string
	terminalEventID   *uuid.UUID
	databaseNow       time.Time
}

func lockRuntimeDelegatingRun(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
) (runtimeDelegatingRun, error) {
	var run runtimeDelegatingRun
	err := tx.QueryRow(ctx, `
SELECT r.user_id, r.agent_id, r.input, r.status, r.dispatch_state,
       r.active_attempt_id, r.lease_id, r.fencing_token,
       r.runtime_node_id, r.runtime_worker_id, r.runtime_session_id,
       r.lease_token_id, r.lease_accepted_at, r.lease_expires_at,
       r.attempt_deadline_at, r.run_deadline_at, r.cancel_request_id,
       r.cancel_state, r.terminal_event_id, clock_timestamp()
FROM runs r
WHERE r.id = $1
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
FOR UPDATE OF r`, runID).Scan(
		&run.userID, &run.agentID, &run.input, &run.status, &run.dispatchState,
		&run.activeAttemptID, &run.leaseID, &run.fencingToken,
		&run.runtimeNodeID, &run.runtimeWorkerID, &run.runtimeSessionID,
		&run.leaseTokenID, &run.leaseAcceptedAt, &run.leaseExpiresAt,
		&run.attemptDeadlineAt, &run.runDeadlineAt, &run.cancelRequestID,
		&run.cancelState, &run.terminalEventID, &run.databaseNow,
	)
	return run, err
}

type runtimeDelegatingAttempt struct {
	offeredAt         time.Time
	acceptedAt        *time.Time
	finishedAt        *time.Time
	outcome           *string
	leaseExpiresAt    time.Time
	attemptDeadlineAt time.Time
	resultID          *uuid.UUID
}

func lockRuntimeDelegatingAttempt(
	ctx context.Context,
	tx pgx.Tx,
	capability RuntimeInvocationCapability,
) (runtimeDelegatingAttempt, error) {
	var attempt runtimeDelegatingAttempt
	err := tx.QueryRow(ctx, `
SELECT a.offered_at, a.accepted_at, a.finished_at, a.outcome,
       a.lease_expires_at, a.attempt_deadline_at, a.result_id
FROM run_attempts a
WHERE a.run_id = $1
  AND a.id = $2
  AND a.lease_id = $3
  AND a.fencing_token = $4
  AND a.agent_id = $5
  AND a.node_id = $6
  AND a.runtime_token_id = $7
  AND a.runtime_worker_id = $8
  AND a.runtime_session_id = $9
  AND a.executor_type = 'runtime'
FOR UPDATE OF a`,
		capability.RunID,
		capability.AttemptID,
		capability.LeaseID,
		capability.FencingToken,
		capability.AgentID,
		capability.NodeID,
		capability.CredentialID,
		capability.WorkerID,
		capability.RuntimeSessionID,
	).Scan(
		&attempt.offeredAt, &attempt.acceptedAt, &attempt.finishedAt,
		&attempt.outcome, &attempt.leaseExpiresAt, &attempt.attemptDeadlineAt,
		&attempt.resultID,
	)
	return attempt, err
}

func runtimeDelegatingRunOwnsCapability(
	run runtimeDelegatingRun,
	capability RuntimeInvocationCapability,
) bool {
	return run.agentID == capability.AgentID &&
		uuidPointerEqual(run.activeAttemptID, capability.AttemptID) &&
		uuidPointerEqual(run.leaseID, capability.LeaseID) &&
		run.fencingToken == capability.FencingToken &&
		uuidPointerEqual(run.runtimeNodeID, capability.NodeID) &&
		stringPointerEqual(run.runtimeWorkerID, capability.WorkerID) &&
		uuidPointerEqual(run.runtimeSessionID, capability.RuntimeSessionID) &&
		uuidPointerEqual(run.leaseTokenID, capability.CredentialID)
}

func verifyRuntimeDelegationCapabilityPair(
	verifier RuntimeInvocationVerifier,
	contextValue string,
	token string,
	databaseNow time.Time,
) (RuntimeInvocationCapability, error) {
	if verifier == nil || databaseNow.IsZero() {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	fromContext, err := verifier.VerifyNodeEnvelope(contextValue, databaseNow)
	if err != nil {
		return RuntimeInvocationCapability{}, err
	}
	fromToken, err := verifier.VerifyInvocationToken(token, databaseNow)
	if err != nil {
		return RuntimeInvocationCapability{}, err
	}
	if !runtimeInvocationCapabilitiesEqual(fromContext, fromToken) {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	return fromToken, nil
}

func runtimeInvocationCapabilitiesEqual(left, right RuntimeInvocationCapability) bool {
	return left.RunID == right.RunID && left.AttemptID == right.AttemptID &&
		left.LeaseID == right.LeaseID && left.FencingToken == right.FencingToken &&
		left.AgentID == right.AgentID && left.CredentialID == right.CredentialID &&
		left.NodeID == right.NodeID && left.WorkerID == right.WorkerID &&
		left.RuntimeSessionID == right.RuntimeSessionID &&
		left.IssuedAt.Equal(right.IssuedAt) && left.ExpiresAt.Equal(right.ExpiresAt) &&
		subtle.ConstantTimeCompare(left.InputSHA256[:], right.InputSHA256[:]) == 1
}

func validRuntimeDelegationAuthorization(value RuntimeDelegationAuthorization) bool {
	return value.Device.NodeID != uuid.Nil &&
		validCertificateSerial(value.Device.CertificateSerial) &&
		validSHA256Hex(value.Device.CertificateFingerprintSHA256) &&
		validSHA256Hex(value.Device.PublicKeyThumbprintSHA256) &&
		value.InvocationContext != "" && value.InvocationToken != "" && value.InvocationProof != "" &&
		value.ProofRequest.Method == http.MethodPost &&
		value.ProofRequest.Path == runtimeCallAgentPath &&
		value.ProofRequest.IdempotencyKey == value.IdempotencyKey &&
		value.ProofRequest.Context == value.InvocationContext &&
		len(value.ProofRequest.Body) > 0 && int64(len(value.ProofRequest.Body)) <= MaxRuntimeMessageBytes
}

func runtimeDelegationPrincipalLockError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, err)
	}
	return err
}

func runtimeDatabaseUnavailable(cause error) *RuntimeTransportError {
	err := newRuntimeTransportError(
		RuntimeErrorServiceUnavailable,
		runtimeErrorDefaultMessage(RuntimeErrorServiceUnavailable),
		cause,
	)
	err.Body.Retryable = true
	return err
}

var _ RuntimeInvocationVerifier = (*RuntimeInvocationSigner)(nil)
