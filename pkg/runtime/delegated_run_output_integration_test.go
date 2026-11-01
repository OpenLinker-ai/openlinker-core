package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const delegatedEchoToken = "child-endpoint-token"

func delegatedEchoEndpoint(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"output": map[string]any{
		"origin": "203.0.113.7",
		"headers": map[string]any{
			"X-Forwarded-For":    []string{"203.0.113.7"},
			"X-Openlinker-Token": []string{r.Header.Get("X-Openlinker-Token")},
			"Accept":             []string{"application/json"},
		},
		"note":   "signed with " + r.Header.Get("X-Openlinker-Token"),
		"cookie": "oatmeal",
	}})
}

type delegatedReadHarness struct {
	pool          *pgxpool.Pool
	svc           *runtime.Service
	callerCreator uuid.UUID
	call          func(target uuid.UUID, key string) runtime.DelegatedRunView
	callID        func(target uuid.UUID, key string) uuid.UUID
	read          func(id uuid.UUID) (runtime.DelegatedRunView, error)
}

func setupDelegatedRead(t *testing.T) delegatedReadHarness {
	t.Helper()
	pool := setupTestDB(t)
	ctx := context.Background()
	parent := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, runtime.RuntimeDelegatedRunReadFeature)
	svc := newTestService(t, pool)
	capability := runtime.RuntimeInvocationCapability{
		Audience: "openlinker.runtime.v2/delegation", RunID: parent.identity.RunID, AttemptID: parent.identity.AttemptID,
		LeaseID: parent.identity.LeaseID, FencingToken: parent.identity.FencingToken, AgentID: parent.identity.AgentID,
		CredentialID: parent.credentialID, NodeID: *parent.identity.NodeID, InputSHA256: sha256.Sum256([]byte(`{}`)),
	}
	device := runtime.RuntimeDeviceIdentity{NodeID: *parent.identity.NodeID, CertificateFingerprintSHA256: strings.Repeat("a", 64)}
	require.NoError(t, pool.QueryRow(ctx, `SELECT a.runtime_worker_id,a.runtime_session_id,a.offered_at,a.attempt_deadline_at,n.device_certificate_serial,n.device_public_key_thumbprint
 FROM run_attempts a JOIN runtime_nodes n ON n.node_id=a.node_id WHERE a.id=$1`, parent.identity.AttemptID).Scan(
		&capability.WorkerID, &capability.RuntimeSessionID, &capability.IssuedAt, &capability.ExpiresAt, &device.CertificateSerial, &device.PublicKeyThumbprintSHA256))
	signer, err := runtime.NewRuntimeInvocationSigner(strings.Repeat("delegation-test-secret-", 3))
	require.NoError(t, err)
	delegated := runtime.NewRuntimeDelegationService(pool, svc, signer)
	authorize := func(path, key string, body []byte) runtime.RuntimeDelegationAuthorization {
		envelope, token, err := signer.Issue(capability)
		require.NoError(t, err)
		request := runtime.RuntimeInvocationProofRequest{Method: http.MethodPost, Path: path, IdempotencyKey: key, Context: envelope, Body: body}
		proof, err := runtime.BuildRuntimeInvocationProof(token, request)
		require.NoError(t, err)
		return runtime.RuntimeDelegationAuthorization{Device: device, InvocationContext: envelope, InvocationToken: token, InvocationProof: proof, IdempotencyKey: key, ProofRequest: request}
	}
	h := delegatedReadHarness{pool: pool, svc: svc}
	require.NoError(t, pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id=$1`, parent.identity.AgentID).Scan(&h.callerCreator))
	h.read = func(id uuid.UUID) (runtime.DelegatedRunView, error) {
		body, err := json.Marshal(runtime.DelegatedRunReadRequest{RunID: id})
		require.NoError(t, err)
		return delegated.ReadDelegatedRun(ctx, authorize("/api/v1/agent-runtime/delegated-runs/read", "read-"+id.String(), body))
	}
	h.callID = func(target uuid.UUID, key string) uuid.UUID {
		body, err := json.Marshal(runtime.CallAgentRequest{TargetAgentID: target, Input: map[string]any{"text": "lookup"}})
		require.NoError(t, err)
		summary, err := delegated.CallAgent(ctx, authorize("/api/v1/agent-runtime/call-agent", key, body))
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var status string
			_ = pool.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1`, summary.RunID).Scan(&status)
			return status != "running"
		}, 8*time.Second, 20*time.Millisecond)
		return summary.RunID
	}
	h.call = func(target uuid.UUID, key string) runtime.DelegatedRunView {
		view, err := h.read(h.callID(target, key))
		require.NoError(t, err)
		return view
	}
	return h
}

func (h delegatedReadHarness) newAgent(t *testing.T, creator uuid.UUID, handler http.HandlerFunc) uuid.UUID {
	t.Helper()
	agent := insertAgent(t, h.pool, creator, startMockEndpointForService(t, h.svc, handler), 0, "approved")
	setAgentEndpointToken(t, h.pool, agent, delegatedEchoToken)
	return agent
}

func TestDelegatedReadFiltersChildFromAnotherCreator(t *testing.T) {
	h := setupDelegatedRead(t)
	otherCreator := insertCreator(t, h.pool)
	target := h.newAgent(t, otherCreator, delegatedEchoEndpoint)

	view := h.call(target, "cross-creator")
	raw, err := json.Marshal(view)
	require.NoError(t, err)
	body := string(raw)
	assert.NotContains(t, body, delegatedEchoToken)
	assert.NotContains(t, body, "203.0.113.7")
	assert.NotContains(t, body, "X-Forwarded-For")

	// 业务字段原样保留。
	var output map[string]any
	require.NoError(t, json.Unmarshal(view.Output, &output))
	assert.Equal(t, "oatmeal", output["cookie"])
	assert.Equal(t, "signed with [redacted]", output["note"])
	assert.Equal(t, map[string]any{"Accept": []any{"application/json"}}, output["headers"])

	// 与同一人直接调用同一 Agent 读到的相同。
	direct, err := h.svc.Run(context.Background(), h.callerCreator, makeRunReq(target, map[string]any{"q": "x"}), "")
	require.NoError(t, err)
	assert.Equal(t, direct.Output["headers"], output["headers"])
	assert.Equal(t, direct.Output["note"], output["note"])
	assert.NotContains(t, output, "origin")

	assertDelegatedResponseMatchesContract(t, raw)
}

func TestDelegatedReadKeepsOriginalForSameCreator(t *testing.T) {
	h := setupDelegatedRead(t)
	target := h.newAgent(t, h.callerCreator, delegatedEchoEndpoint)
	view := h.call(target, "same-creator")
	assert.Contains(t, string(view.Output), delegatedEchoToken)
	assert.Contains(t, string(view.Output), "203.0.113.7")
}

func TestDelegatedReadFiltersMCPCredentialAndFailureMessage(t *testing.T) {
	h := setupDelegatedRead(t)
	other := insertCreator(t, h.pool)
	mcp := h.newAgent(t, other, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": map[string]any{"structuredContent": map[string]any{
			"headers": map[string]any{"Authorization": r.Header.Get("Authorization")},
			"note":    "sent " + strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "),
		}}})
	})
	_, err := h.pool.Exec(context.Background(), `UPDATE agents SET connection_mode='mcp_server',mcp_tool_name='echo_tool',endpoint_auth_header=$2 WHERE id=$1`, mcp, "Bearer "+delegatedEchoToken)
	require.NoError(t, err)
	raw, err := json.Marshal(h.call(mcp, "mcp"))
	require.NoError(t, err)
	assert.NotContains(t, string(raw), delegatedEchoToken)
	assert.Contains(t, string(raw), "sent [redacted]")

	failing := h.newAgent(t, other, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "BAD", "message": "rejected " + r.Header.Get("X-Openlinker-Token")}})
	})
	view := h.call(failing, "failing")
	assert.Equal(t, "failed", string(view.Status))
	assert.Equal(t, "rejected [redacted]", view.ErrorMessage)
}

// 响应字段超出契约时三个 SDK 都会拒绝，现有 Worker 将无法读取子 Run。
func assertDelegatedResponseMatchesContract(t *testing.T, response []byte) {
	t.Helper()
	raw, err := os.ReadFile("../../contracts/delegated-run-read.json")
	require.NoError(t, err)
	var contract struct {
		Defs struct {
			Response struct {
				Properties map[string]any `json:"properties"`
			} `json:"Response"`
		} `json:"$defs"`
	}
	require.NoError(t, json.Unmarshal(raw, &contract))
	var fields map[string]any
	require.NoError(t, json.Unmarshal(response, &fields))
	for field := range fields {
		assert.Contains(t, contract.Defs.Response.Properties, field)
	}
}
