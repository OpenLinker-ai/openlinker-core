package runtime

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const runtimeResumeUploadPermission = "upload_spool_only"

type runtimeResumeAuthorizationQueries interface {
	LockActiveRuntimeResumeGrantForAttemptTarget(context.Context, db.LockActiveRuntimeResumeGrantForAttemptTargetParams) (db.LockActiveRuntimeResumeGrantForAttemptTargetRow, error)
	LockConsumedRuntimeResumeGrantForStoredReplay(context.Context, db.LockConsumedRuntimeResumeGrantForStoredReplayParams) (db.LockConsumedRuntimeResumeGrantForStoredReplayRow, error)
	ConsumeRuntimeResumeGrant(context.Context, db.ConsumeRuntimeResumeGrantParams) (db.RuntimeResumeGrant, error)
}

type runtimeAttemptAuthorization struct {
	resumed    bool
	permission string
	grantID    uuid.UUID
}

// authorizeRuntimeAttemptPrincipal is called only after the authenticated
// target Session/Node/Credential locks and the Run -> Attempt locks are held.
// It either proves the original Session identity or locks and consumes the
// single durable resume grant in the same transaction as the Event/Result.
func authorizeRuntimeAttemptPrincipal(
	ctx context.Context,
	queries runtimeResumeAuthorizationQueries,
	principal RuntimeEventPrincipal,
	identity RuntimeAttemptIdentity,
	attempt runtimeEventAttempt,
	storedReplay bool,
) (runtimeAttemptAuthorization, error) {
	if runtimePrincipalMatchesAttempt(principal, attempt) {
		return runtimeAttemptAuthorization{}, nil
	}
	if queries == nil || principal.NodeID == nil || principal.CredentialID == nil ||
		principal.RuntimeSessionID == nil || principal.CoreInstanceID == nil || principal.WorkerID == nil ||
		attempt.nodeID == nil || attempt.credentialID == nil || attempt.runtimeSessionID == nil || attempt.workerID == nil ||
		principal.AgentID != attempt.agentID || *principal.NodeID != *attempt.nodeID ||
		*principal.WorkerID != *attempt.workerID || identity.RunID != attempt.runID ||
		identity.AttemptID != attempt.id || identity.LeaseID != attempt.leaseID ||
		identity.FencingToken != attempt.fencingToken {
		return runtimeAttemptAuthorization{}, newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, nil)
	}

	if storedReplay {
		grant, err := queries.LockConsumedRuntimeResumeGrantForStoredReplay(ctx, db.LockConsumedRuntimeResumeGrantForStoredReplayParams{
			RunID:              attempt.runID,
			AttemptID:          attempt.id,
			LeaseID:            attempt.leaseID,
			FencingToken:       attempt.fencingToken,
			AgentID:            attempt.agentID,
			NodeID:             *attempt.nodeID,
			WorkerID:           *attempt.workerID,
			TargetSessionID:    *principal.RuntimeSessionID,
			TargetCredentialID: *principal.CredentialID,
		})
		if err != nil {
			return runtimeAttemptAuthorization{}, normalizeRuntimeResumeAuthorizationError(err)
		}
		if grant.SourceSessionID != *attempt.runtimeSessionID || grant.SourceCredentialID != *attempt.credentialID {
			return runtimeAttemptAuthorization{}, newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, nil)
		}
		return runtimeAttemptAuthorization{resumed: true, permission: grant.Permission, grantID: grant.ID}, nil
	}

	grant, err := queries.LockActiveRuntimeResumeGrantForAttemptTarget(ctx, db.LockActiveRuntimeResumeGrantForAttemptTargetParams{
		RunID:              attempt.runID,
		AttemptID:          attempt.id,
		LeaseID:            attempt.leaseID,
		FencingToken:       attempt.fencingToken,
		AgentID:            attempt.agentID,
		NodeID:             *attempt.nodeID,
		WorkerID:           *attempt.workerID,
		TargetSessionID:    *principal.RuntimeSessionID,
		TargetCredentialID: *principal.CredentialID,
		AllowedPermission:  runtimeResumeUploadPermission,
		CoreInstanceID:     principal.CoreInstanceID,
	})
	if err != nil {
		return runtimeAttemptAuthorization{}, normalizeRuntimeResumeAuthorizationError(err)
	}
	if grant.SourceSessionID != *attempt.runtimeSessionID || grant.SourceCredentialID != *attempt.credentialID {
		return runtimeAttemptAuthorization{}, newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, nil)
	}
	consumed, err := queries.ConsumeRuntimeResumeGrant(ctx, db.ConsumeRuntimeResumeGrantParams{
		GrantID:            grant.ID,
		RunID:              attempt.runID,
		AttemptID:          attempt.id,
		LeaseID:            attempt.leaseID,
		FencingToken:       attempt.fencingToken,
		TargetSessionID:    *principal.RuntimeSessionID,
		TargetCredentialID: *principal.CredentialID,
		Permission:         grant.Permission,
	})
	if err != nil {
		return runtimeAttemptAuthorization{}, normalizeRuntimeResumeAuthorizationError(err)
	}
	return runtimeAttemptAuthorization{resumed: true, permission: consumed.Permission, grantID: consumed.ID}, nil
}

func normalizeRuntimeResumeAuthorizationError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return newRuntimeEventError(RuntimeEventErrorLeaseIdentityMismatch, err)
	}
	return err
}
