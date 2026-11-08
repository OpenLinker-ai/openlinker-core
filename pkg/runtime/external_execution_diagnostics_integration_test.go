package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/externalexecution"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// The target owner's validation reads the capability through the production
// Agent service: a stricter listing bound is executable, a looser one reports
// the capability's own constraint value.
func TestExternalExecutionValidateTargetReportsCapabilityRule(t *testing.T) {
	pool, svc, _, _, target := dryRunReadyFixture(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE agents SET visibility='public' WHERE id=$1`, target.ID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agent_capabilities(agent_id,input_schema,output_schema)
VALUES($1,'{"type":"object","required":["text"],"properties":{"text":{"type":"string","maxLength":4000}}}'::jsonb,'{"type":"object"}'::jsonb)`, target.ID)
	require.NoError(t, err)
	external := externalexecution.NewService(
		agent.NewService(pool, newTestConfig()), svc, agentTargetsOnlyWorkflows{}, externalexecution.NewSQLStore(pool),
	)
	owner := &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: target.CreatorID}
	request := func(maxLength string) *externalexecution.TargetValidationRequest {
		return &externalexecution.TargetValidationRequest{
			TargetType: "agent", TargetID: target.ID.String(),
			InputSchema: json.RawMessage(`{"type":"object","required":["text"],"properties":{"text":{"type":"string","maxLength":` + maxLength + `}}}`),
		}
	}

	stricter, err := external.ValidateTarget(ctx, owner, request("4000"))
	require.NoError(t, err)
	require.True(t, stricter.Executable, stricter.UnavailableReason)
	require.Nil(t, stricter.UnavailableDetail)

	validation, err := external.ValidateTarget(ctx, owner, request("4001"))
	require.NoError(t, err)
	require.False(t, validation.Executable)
	require.Equal(t, "input_schema_incompatible", validation.UnavailableReason)
	require.Empty(t, validation.ContractHash)
	require.NotNil(t, validation.UnavailableDetail)
	require.Equal(t, "text", validation.UnavailableDetail.Property)
	require.Equal(t, externalexecution.SchemaRuleMaxLength, validation.UnavailableDetail.Rule)
	require.JSONEq(t, `4000`, string(validation.UnavailableDetail.CapabilityValue))

	other := insertRuntimeUser(t, pool)
	hidden, err := external.ValidateTarget(ctx, &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: other}, request("4001"))
	require.NoError(t, err)
	require.Equal(t, "not_found", hidden.UnavailableReason)
	require.Nil(t, hidden.UnavailableDetail, "only the target owner may see capability detail")
}

// Starts through the production stores and Runtime: a listing bound is
// enforced before anything is reserved, and a capability rejection from
// Runtime is persisted as a terminal rejection that is replayed by every later
// Start and GetExecution, even after the capability becomes permissive.
func TestExternalExecutionStartRejectsInvalidInputTerminally(t *testing.T) {
	pool, svc, _, _, target := dryRunReadyFixture(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE agents SET visibility='public' WHERE id=$1`, target.ID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agent_capabilities(agent_id,input_schema,output_schema)
VALUES($1,'{"type":"object","required":["text"],"properties":{"text":{"type":"string","maxLength":4000}},"additionalProperties":false}'::jsonb,'{"type":"object"}'::jsonb)`, target.ID)
	require.NoError(t, err)
	external := externalexecution.NewService(
		agent.NewService(pool, newTestConfig()), svc, agentTargetsOnlyWorkflows{}, externalexecution.NewSQLStore(pool),
	)
	schema := json.RawMessage(`{"type":"object","required":["text"],"properties":{"text":{"type":"string","maxLength":10}}}`)
	validation, err := external.ValidateTarget(ctx, &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: target.CreatorID},
		&externalexecution.TargetValidationRequest{TargetType: "agent", TargetID: target.ID.String(), InputSchema: schema})
	require.NoError(t, err)
	require.True(t, validation.Executable, validation.UnavailableReason)

	buyer := &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: insertRuntimeUser(t, pool)}
	start := func(input map[string]any) (string, error) {
		requestID := uuid.NewString()
		_, err := external.StartExecution(ctx, buyer, &externalexecution.ExecutionRequest{
			ExternalRequestID: requestID, TargetType: "agent", TargetID: target.ID.String(), Input: input,
			TraceID: "trace-input-" + requestID[:8], ExpectedContractHash: validation.ContractHash, InputSchema: schema,
		})
		return requestID, err
	}
	requireCode := func(err error, status int, code string) *httpx.HTTPError {
		t.Helper()
		var httpErr *httpx.HTTPError
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, status, httpErr.Status)
		require.Equal(t, httpx.ErrorCode(code), httpErr.Code)
		encoded, marshalErr := json.Marshal(httpErr)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(encoded), "buyer-secret")
		return httpErr
	}

	requestID, err := start(map[string]any{"text": "buyer-secret too long"})
	rejected := requireCode(err, 422, "EXECUTION_INPUT_INVALID")
	require.Equal(t, "input_schema", rejected.Details.(map[string]interface{})["source"])
	var keys, executions int
	require.NoError(t, pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM external_execution_keys WHERE caller_service_id='openlinker-cloud' AND external_request_id=$1),
		(SELECT count(*) FROM external_executions WHERE caller_service_id='openlinker-cloud' AND external_request_id=$1)`, requestID).Scan(&keys, &executions))
	require.Zero(t, keys+executions, "a rejected input must not reserve anything")

	// The listing allows an undeclared key; the capability forbids it.
	input := map[string]any{"text": "ok", "buyer-secret-key": 1}
	traceID := "trace-capability-reject"
	requestID = uuid.NewString()
	startSame := func() error {
		_, err := external.StartExecution(ctx, buyer, &externalexecution.ExecutionRequest{
			ExternalRequestID: requestID, TargetType: "agent", TargetID: target.ID.String(), Input: input,
			TraceID: traceID, ExpectedContractHash: validation.ContractHash, InputSchema: schema,
		})
		return err
	}
	requireRejected := func(step string) {
		t.Helper()
		var state, code string
		var owner *uuid.UUID
		var runs int
		require.NoError(t, pool.QueryRow(ctx, `SELECT start_state, rejection_code, authorized_target_owner_id,
			(SELECT count(*) FROM runs WHERE user_id=$2)
			FROM external_executions WHERE caller_service_id='openlinker-cloud' AND external_request_id=$1`,
			requestID, buyer.ActorUserID).Scan(&state, &code, &owner, &runs), step)
		require.Equal(t, "rejected", state, step)
		require.Equal(t, "TARGET_CONTRACT_CHANGED", code, step)
		require.Nil(t, owner, step)
		require.Zero(t, runs, "%s: Runtime must not have created a Run", step)
	}
	requireCode(startSame(), 409, "TARGET_CONTRACT_CHANGED")
	requireRejected("first start")
	requireCode(startSame(), 409, "TARGET_CONTRACT_CHANGED")
	requireRejected("immediate retry")

	_, err = pool.Exec(ctx, `UPDATE agent_capabilities SET input_schema='{"type":"object","properties":{"text":{"type":"string"}}}'::jsonb WHERE agent_id=$1`, target.ID)
	require.NoError(t, err)
	requireCode(startSame(), 409, "TARGET_CONTRACT_CHANGED")
	requireRejected("retry after the capability became permissive")
	_, err = external.GetExecution(ctx, buyer, requestID)
	requireCode(err, 409, "TARGET_CONTRACT_CHANGED")
}

// A Runtime Worker failure committed by the production result finalizer
// surfaces as a bounded failure_category while error_code/error_message keep
// their existing safe values.
func TestExternalExecutionGetReportsFinalizedRunFailureCategory(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	ctx := context.Background()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	_, err := runtime.NewResultFinalizer(pool, nil, nil).Finalize(ctx, fixture.principal, runtime.RuntimeResultRequest{
		AttemptIdentity: fixture.identity,
		ResultID:        uuid.New(),
		Status:          "failed",
		Error:           &runtime.RuntimeResultFailure{ErrorCode: "HANDLER_PANIC", Message: "handler leaked a private detail"},
		DurationMS:      7,
	})
	require.NoError(t, err)

	var actorID, agentID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_id, agent_id FROM runs WHERE id=$1`, fixture.identity.RunID).Scan(&actorID, &agentID))
	store := externalexecution.NewSQLStore(pool)
	requestID := uuid.New()
	inputFingerprint := sha256.Sum256([]byte("diagnostics-input"))
	schemaFingerprint := sha256.Sum256([]byte("diagnostics-schema"))
	contractHash := "hct:v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	_, err = store.Reserve(ctx, externalexecution.ExecutionRecord{
		CallerServiceID: "openlinker-cloud", ExternalRequestID: requestID, ActorUserID: actorID,
		RequestFingerprintVersion: 2, TargetType: "agent", TargetID: agentID,
		InputFingerprint: inputFingerprint[:], ExpectedContractHash: &contractHash,
		InputSchemaFingerprint: schemaFingerprint[:], TraceID: "trace-diagnostics",
	})
	require.NoError(t, err)
	attached, err := store.Attach(ctx, "openlinker-cloud", requestID, "run", fixture.identity.RunID)
	require.NoError(t, err)
	require.NotNil(t, attached.ExecutionID)

	external := externalexecution.NewService(
		agent.NewService(pool, newTestConfig()), runtime.NewService(pool, newTestConfig()), agentTargetsOnlyWorkflows{}, store,
	)
	status, err := external.GetExecution(ctx, &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: actorID}, requestID.String())
	require.NoError(t, err)
	require.Equal(t, "failed", status.Status)
	require.Equal(t, "EXECUTION_FAILED", status.ErrorCode)
	require.Equal(t, "Execution failed.", status.ErrorMessage)
	require.Equal(t, externalexecution.FailureCategoryAgentError, status.FailureCategory)
	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private detail")
	require.NotContains(t, string(encoded), "HANDLER_PANIC")
}
