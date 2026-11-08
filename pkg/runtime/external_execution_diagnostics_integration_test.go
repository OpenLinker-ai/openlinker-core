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
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// The target owner's validation reads the capability through the production
// Agent service and reports the first unproven rule with the capability's own
// constraint value.
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
	request := &externalexecution.TargetValidationRequest{
		TargetType: "agent", TargetID: target.ID.String(),
		InputSchema: json.RawMessage(`{"type":"object","required":["text"],"properties":{"text":{"type":"string","maxLength":4000}}}`),
	}

	validation, err := external.ValidateTarget(ctx, &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: target.CreatorID}, request)
	require.NoError(t, err)
	require.False(t, validation.Executable)
	require.Equal(t, "input_schema_incompatible", validation.UnavailableReason)
	require.Empty(t, validation.ContractHash)
	require.NotNil(t, validation.UnavailableDetail)
	require.Equal(t, "text", validation.UnavailableDetail.Property)
	require.Equal(t, externalexecution.SchemaRuleMaxLength, validation.UnavailableDetail.Rule)
	require.JSONEq(t, `4000`, string(validation.UnavailableDetail.CapabilityValue))

	other := insertRuntimeUser(t, pool)
	hidden, err := external.ValidateTarget(ctx, &externalexecution.Principal{CallerServiceID: "openlinker-cloud", ActorUserID: other}, request)
	require.NoError(t, err)
	require.Equal(t, "not_found", hidden.UnavailableReason)
	require.Nil(t, hidden.UnavailableDetail, "only the target owner may see capability detail")
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
