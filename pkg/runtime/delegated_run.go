package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

const runtimeDelegatedRunReadPath = "/api/v1/agent-runtime/delegated-runs/read"

type DelegatedRunReadRequest struct {
	RunID uuid.UUID `json:"run_id"`
}

// DelegatedRunView intentionally excludes input, credentials, transport evidence
// and owner metadata. Only the direct caller may inspect this result. A caller
// Agent from another creator reads the filtered output. The wire contract has no
// marker for that: every SDK rejects unknown fields on this response.
type DelegatedRunView struct {
	RunSummary
	Output       json.RawMessage `json:"output,omitempty"`
	ErrorCode    string          `json:"error_code,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
}

func runtimeCapabilityAudience(capability RuntimeInvocationCapability) string {
	if capability.Audience == "" {
		return runtimeInvocationAudience
	}
	return capability.Audience
}

func runtimeSessionInvocationAudience(features []string) string {
	for _, feature := range features {
		if feature == RuntimeDelegatedRunReadFeature {
			return runtimeDelegationAudience
		}
	}
	return ""
}

// ReadDelegatedRun is a separate, explicitly negotiated authority from the
// original call-agent audience. It never falls back to owner-facing Run APIs.
// Authorization and the read share a transaction: a terminal/canceled/replaced
// parent Attempt cannot race the read after its authority has been checked.
func (s *RuntimeDelegationService) ReadDelegatedRun(ctx context.Context, authorization RuntimeDelegationAuthorization) (DelegatedRunView, error) {
	if s == nil || s.pool == nil || s.verifier == nil {
		return DelegatedRunView{}, runtimeUnavailableError()
	}
	if !validRuntimeDelegationAuthorizationForPath(authorization, runtimeDelegatedRunReadPath) {
		return DelegatedRunView{}, runtimeUnauthorizedError(nil)
	}
	if _, err := HashIdempotencyKey(authorization.IdempotencyKey); err != nil {
		return DelegatedRunView{}, runtimeTransportValidationError()
	}
	if err := VerifyRuntimeInvocationProof(authorization.InvocationToken, authorization.InvocationProof, authorization.ProofRequest); err != nil {
		return DelegatedRunView{}, runtimeUnauthorizedError(err)
	}
	var request DelegatedRunReadRequest
	if err := decodeRuntimeJSON(authorization.ProofRequest.Body, &request); err != nil || request.RunID == uuid.Nil {
		return DelegatedRunView{}, runtimeTransportValidationError()
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DelegatedRunView{}, runtimeDatabaseUnavailable(err)
	}
	defer tx.Rollback(ctx)
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return DelegatedRunView{}, runtimeDatabaseUnavailable(err)
	}
	capability, err := verifyRuntimeDelegationCapabilityPair(s.verifier, authorization.InvocationContext, authorization.InvocationToken, databaseNow)
	if err != nil || runtimeCapabilityAudience(capability) != runtimeDelegationAudience || capability.NodeID != authorization.Device.NodeID {
		return DelegatedRunView{}, runtimeUnauthorizedError(err)
	}
	var parentUserID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT user_id FROM runs WHERE id = $1 AND runtime_contract_id = 'openlinker.runtime.v2'`, capability.RunID).Scan(&parentUserID); err != nil {
		return DelegatedRunView{}, runtimeDelegationPrincipalLockError(err)
	}
	if err = s.authorizeChildCreation(ctx, tx, authorization, capability, parentUserID); err != nil {
		return DelegatedRunView{}, err
	}
	var result DelegatedRunView
	var callerCreatorID, targetCreatorID *uuid.UUID
	var targetEndpointToken *string
	err = tx.QueryRow(ctx, `
SELECT r.id, r.status, r.dispatch_state, r.output,
       COALESCE(r.error_code, ''), COALESCE(r.error_message, ''),
       caller.creator_id, target.creator_id, target.endpoint_auth_header
FROM run_delegations d JOIN runs r ON r.id = d.child_run_id
LEFT JOIN agents caller ON caller.id = d.caller_agent_id
LEFT JOIN agents target ON target.id = r.agent_id
WHERE d.parent_run_id = $1 AND d.caller_agent_id = $2 AND d.child_run_id = $3
  AND r.user_id = $4 AND r.runtime_contract_id = 'openlinker.runtime.v2'`,
		capability.RunID, capability.AgentID, request.RunID, parentUserID).Scan(
		&result.RunID, &result.Status, &result.DispatchState, &result.Output, &result.ErrorCode, &result.ErrorMessage,
		&callerCreatorID, &targetCreatorID, &targetEndpointToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return DelegatedRunView{}, newRuntimeTransportError(RuntimeErrorNotFound, runtimeErrorDefaultMessage(RuntimeErrorNotFound), err)
	}
	if err != nil {
		return DelegatedRunView{}, runtimeDatabaseUnavailable(err)
	}
	if err = ValidateRuntimePayload(result.RunSummary); err != nil {
		return DelegatedRunView{}, err
	}
	redacted, err := projectDelegatedRun(&result, delegatedRunProjection(callerCreatorID, targetCreatorID, targetEndpointToken))
	if err != nil {
		return DelegatedRunView{}, newRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), err)
	}
	if err = tx.Commit(ctx); err != nil {
		return DelegatedRunView{}, runtimeDatabaseUnavailable(err)
	}
	if redacted {
		log.Info().Str("run_id", result.RunID.String()).Str("caller_agent_id", capability.AgentID.String()).
			Msg("runtime.ReadDelegatedRun: output filtered for caller from another creator")
	}
	return result, nil
}

func (h *RuntimeHTTPController) ReadDelegatedRun(c echo.Context) error {
	return h.handleDelegation(c, true)
}
