package runtime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcpbinding"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestMCPBindingOwnerGrantDispatchAndRevocation(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	parent := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, mcpbinding.Feature, runtime.RuntimeDelegatedRunReadFeature)
	agent := parent.identity.AgentID
	var owner uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id=$1`, agent).Scan(&owner))
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime',endpoint_url='openlinker-runtime://' || id::text WHERE id=$1`, agent)
	require.NoError(t, err)
	var caller uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_id FROM runs WHERE id=$1`, parent.identity.RunID).Scan(&caller))
	require.NotEqual(t, owner, caller)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "Bearer SYNTHETIC-SERVICE-CREDENTIAL", r.Header.Get("Authorization"))
		var request map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		raw, _ := json.Marshal(request)
		require.NotContains(t, string(raw), mcpbinding.MetadataKey)
		require.NotContains(t, string(raw), mcpbinding.CallMetadataKey)
		require.Equal(t, "tools/call", request["method"])
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"structuredContent":{"marker":"REAL-MCP-RESULT"}}}`))
	}))
	defer server.Close()
	target := insertAgent(t, pool, owner, server.URL, 0, "approved")
	_, err = pool.Exec(ctx, `UPDATE agents SET connection_mode='mcp_server',mcp_tool_name='lookup_marker',visibility='private',endpoint_auth_header='Bearer SYNTHETIC-SERVICE-CREDENTIAL' WHERE id=$1`, target)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agent_call_policies(agent_id,callable_by) VALUES($1,'private')`, target)
	require.NoError(t, err)
	secret := strings.Repeat("mcp-test-secret-", 3)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	mcpbinding.NewHandler(pool, newTestConfig().EffectiveRuntimeMasterSecret()).Register(e.Group("/api/v1"), auth.JWTMiddlewareWithUserStatus(secret, auth.NewDBUserStatusChecker(pool)))
	jwt, err := auth.GenerateToken(owner.String(), secret, time.Hour)
	require.NoError(t, err)
	other, err := auth.GenerateToken(caller.String(), secret, time.Hour)
	require.NoError(t, err)
	path := "/api/v1/creator/agents/" + agent.String() + "/mcp-bindings"
	call := func(method, suffix, token string, body any, status int) *httptest.ResponseRecorder {
		t.Helper()
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(method, path+suffix, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, status, rec.Code, rec.Body.String())
		return rec
	}
	call("GET", "", "", nil, 401)
	call("GET", "", other, nil, 404)
	call("GET", "/candidates", other, nil, 404)
	call("PUT", "/"+target.String(), jwt, map[string]any{}, 400)
	call("PUT", "/"+target.String(), other, map[string]any{"acknowledge_tool_access": true}, 404)
	call("PUT", "/"+target.String(), jwt, map[string]any{"acknowledge_tool_access": true, "expected_tool_digest": strings.Repeat("0", 64)}, 409)
	var tool mcpbinding.Tool
	var preview mcpbinding.Tool
	require.NoError(t, json.Unmarshal(call("GET", "/candidates/"+target.String(), jwt, nil, 200).Body.Bytes(), &preview))
	// A public preview must not provide an offline verifier for a guessed
	// endpoint credential. It is stable only under the same Core master secret.
	var config []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT jsonb_build_array(a.creator_id,a.connection_mode,a.lifecycle_status,a.visibility,a.endpoint_url,a.endpoint_auth_header,a.mcp_tool_name,COALESCE(c.input_schema,'{"type":"object"}'::jsonb),COALESCE(p.callable_by,'public'))
 FROM agents a LEFT JOIN agent_capabilities c ON c.agent_id=a.id LEFT JOIN agent_call_policies p ON p.agent_id=a.id WHERE a.id=$1`, target).Scan(&config))
	plain := sha256.Sum256(config)
	require.NotEqual(t, hex.EncodeToString(plain[:]), preview.Digest)
	again, err := mcpbinding.Resolve(ctx, pool, owner, target, newTestConfig().EffectiveRuntimeMasterSecret())
	require.NoError(t, err)
	require.Equal(t, preview.Digest, again.Digest)
	rotatedKey, err := mcpbinding.Resolve(ctx, pool, owner, target, "different-synthetic-master-secret")
	require.NoError(t, err)
	require.NotEqual(t, preview.Digest, rotatedKey.Digest)
	_, err = mcpbinding.Resolve(ctx, pool, owner, target, "")
	require.Error(t, err, "an absent key must never fall back to a public hash")
	rec := call("PUT", "/"+target.String(), jwt, map[string]any{"expected_binding_id": "", "acknowledge_tool_access": true, "expected_tool_digest": preview.Digest}, 200)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tool))
	require.NotContains(t, rec.Body.String(), "SYNTHETIC-SERVICE-CREDENTIAL")
	require.NotContains(t, rec.Body.String(), server.URL)
	require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
	call("PUT", "/"+target.String(), jwt, map[string]any{"expected_binding_id": "", "acknowledge_tool_access": true, "expected_tool_digest": preview.Digest}, 409)
	svc := newTestService(t, pool)
	request := makeRunReq(agent, map[string]any{"text": "test"})
	request.Metadata = map[string]any{mcpbinding.MetadataKey: map[string]any{"tools": "SPOOF"}, mcpbinding.CallMetadataKey: "SPOOF"}
	admitted, err := svc.Run(ctx, caller, request, "api")
	require.NoError(t, err)
	var metadata []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT request_metadata FROM runs WHERE id=$1`, admitted.RunID).Scan(&metadata))
	require.NotContains(t, string(metadata), "SPOOF")
	require.NotContains(t, string(metadata), tool.BindingID.String())
	q := db.New(pool)
	_, err = q.LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agent})
	require.ErrorIs(t, err, pgx.ErrNoRows)
	candidate, err := q.LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agent, MCPBindings: true})
	require.NoError(t, err)
	require.Contains(t, string(candidate.RequestMetadata), tool.BindingID.String())
	require.NotContains(t, string(candidate.RequestMetadata), "SYNTHETIC-SERVICE-CREDENTIAL")
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return mcpbinding.Snapshot(ctx, tx, parent.identity.RunID, agent, newTestConfig().EffectiveRuntimeMasterSecret())
	}))
	cap := runtime.RuntimeInvocationCapability{Audience: "openlinker.runtime.v2/delegation", RunID: parent.identity.RunID, AttemptID: parent.identity.AttemptID, LeaseID: parent.identity.LeaseID, FencingToken: parent.identity.FencingToken, AgentID: agent, CredentialID: parent.credentialID, NodeID: *parent.identity.NodeID, InputSHA256: sha256.Sum256([]byte(`{}`))}
	device := runtime.RuntimeDeviceIdentity{NodeID: *parent.identity.NodeID, CertificateFingerprintSHA256: strings.Repeat("a", 64)}
	require.NoError(t, pool.QueryRow(ctx, `SELECT a.runtime_worker_id,a.runtime_session_id,a.offered_at,a.attempt_deadline_at,n.device_certificate_serial,n.device_public_key_thumbprint FROM run_attempts a JOIN runtime_nodes n ON n.node_id=a.node_id WHERE a.id=$1`, parent.identity.AttemptID).Scan(&cap.WorkerID, &cap.RuntimeSessionID, &cap.IssuedAt, &cap.ExpiresAt, &device.CertificateSerial, &device.PublicKeyThumbprintSHA256))
	signer, err := runtime.NewRuntimeInvocationSigner(secret)
	require.NoError(t, err)
	authorize := func(key string, ref *mcpbinding.Reference) runtime.RuntimeDelegationAuthorization {
		envelope, token, err := signer.Issue(cap)
		require.NoError(t, err)
		metadata := map[string]any{}
		if ref != nil {
			metadata[mcpbinding.CallMetadataKey] = ref
		}
		body, err := json.Marshal(runtime.CallAgentRequest{TargetAgentID: target, Input: map[string]any{"text": "lookup"}, Metadata: metadata})
		require.NoError(t, err)
		proofReq := runtime.RuntimeInvocationProofRequest{Method: "POST", Path: "/api/v1/agent-runtime/call-agent", IdempotencyKey: key, Context: envelope, Body: body}
		proof, err := runtime.BuildRuntimeInvocationProof(token, proofReq)
		require.NoError(t, err)
		return runtime.RuntimeDelegationAuthorization{Device: device, InvocationContext: envelope, InvocationToken: token, InvocationProof: proof, IdempotencyKey: key, ProofRequest: proofReq}
	}
	delegated := runtime.NewRuntimeDelegationService(pool, svc, signer)
	_, err = delegated.CallAgent(ctx, authorize("unbound", nil))
	require.Error(t, err)
	ref := mcpbinding.Reference{BindingID: tool.BindingID, Digest: tool.Digest}
	child, err := delegated.CallAgent(ctx, authorize("bound-one", &ref))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var status string
		_ = pool.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1`, child.RunID).Scan(&status)
		return status == "success"
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, int32(1), calls.Load())
	replay, err := delegated.CallAgent(ctx, authorize("bound-one", &ref))
	require.NoError(t, err)
	require.Equal(t, child.RunID, replay.RunID)
	require.Equal(t, int32(1), calls.Load())

	receipt := map[string]any{"bindings": []any{map[string]any{"binding_id": ref.BindingID.String(), "digest": ref.Digest}}}
	store := runtime.NewEventStore(pool)
	_, err = store.Append(ctx, parent.principal, parent.identity, runtime.RuntimeEventRequest{ClientEventID: uuid.New(), ClientEventSeq: 1, EventType: "run.mcp_bindings.ready", Payload: receipt})
	require.NoError(t, err)
	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM agent_mcp_bindings WHERE binding_id=$1`, ref.BindingID).Scan(&status))
	require.Equal(t, "ready", status)
	_, err = store.Append(ctx, parent.principal, parent.identity, runtime.RuntimeEventRequest{ClientEventID: uuid.New(), ClientEventSeq: 2, EventType: "run.mcp_bindings.used", Payload: map[string]any{"bindings": []any{map[string]any{"binding_id": uuid.NewString(), "digest": ref.Digest}}}})
	require.NoError(t, err, "invalid optional receipt must not block SDK spool")
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM agent_mcp_bindings WHERE binding_id=$1`, ref.BindingID).Scan(&status))
	require.Equal(t, "ready", status)
	_, err = pool.Exec(ctx, `UPDATE agents SET endpoint_auth_header='Bearer ROTATED-CREDENTIAL' WHERE id=$1`, target)
	require.NoError(t, err)
	_, err = delegated.CallAgent(ctx, authorize("changed-service", &ref))
	require.Error(t, err)
	require.Contains(t, call("GET", "", jwt, nil, 200).Body.String(), `"needs_review":true`)
	_, err = pool.Exec(ctx, `UPDATE agents SET endpoint_auth_header='Bearer SYNTHETIC-SERVICE-CREDENTIAL' WHERE id=$1`, target)
	require.NoError(t, err)
	// Revocation applies to an already running model, not only the next Run.
	call("DELETE", "/"+target.String(), jwt, map[string]any{"expected_binding_id": tool.BindingID}, 204)
	_, err = delegated.CallAgent(ctx, authorize("after-removal", &ref))
	require.Error(t, err)
	require.Equal(t, int32(1), calls.Load())
	// Re-adding identical contents issues a new grant and cannot revive old Runs.
	call("PUT", "/"+target.String(), jwt, map[string]any{"expected_binding_id": "", "acknowledge_tool_access": true, "expected_tool_digest": preview.Digest}, 200)
	_, err = delegated.CallAgent(ctx, authorize("after-readd", &ref))
	require.Error(t, err)
	_, err = store.Append(ctx, parent.principal, parent.identity, runtime.RuntimeEventRequest{ClientEventID: uuid.New(), ClientEventSeq: 3, EventType: "run.mcp_bindings.used", Payload: receipt})
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM agent_mcp_bindings WHERE agent_id=$1`, agent).Scan(&status))
	require.Equal(t, "pending", status, "old receipt changed replacement binding")

}

func TestMCPBindingOfflineQueue(t *testing.T) {
	for _, tc := range []struct {
		name, status        string
		compatible, history bool
	}{
		{"no history", "", false, false},
		{"compatible offline", "offline", true, true},
		{"compatible closed", "closed", true, true},
		{"incompatible offline", "offline", false, true},
		{"incompatible closed", "closed", false, true},
		{"compatible revoked", "revoked", true, true},
		{"incompatible revoked", "revoked", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			ctx := context.Background()
			var agentID, creatorID uuid.UUID
			if tc.history {
				features := []string{}
				if tc.compatible {
					features = []string{mcpbinding.Feature}
				}
				fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, features...)
				agentID = fixture.identity.AgentID
				require.NoError(t, pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id=$1`, agentID).Scan(&creatorID))
				require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
					if _, err := tx.Exec(ctx, `UPDATE runtime_session_attachments SET detached_at=clock_timestamp(),disconnect_reason='test_shutdown' WHERE runtime_session_id=$1 AND detached_at IS NULL`, *fixture.identity.RuntimeSessionID); err != nil {
						return err
					}
					_, err := tx.Exec(ctx, `UPDATE runtime_sessions SET status=$2,attached_core_instance_id=NULL,disconnected_at=clock_timestamp() WHERE runtime_session_id=$1`, *fixture.identity.RuntimeSessionID, tc.status)
					return err
				}))
			} else {
				creatorID = insertCreator(t, pool)
				agentID = insertAgent(t, pool, creatorID, "https://example.test/offline", 0, "approved")
			}
			_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime',endpoint_url='openlinker-runtime://' || id::text WHERE id=$1`, agentID)
			require.NoError(t, err)
			target := insertAgent(t, pool, creatorID, "https://example.test/mcp", 0, "approved")
			_, err = pool.Exec(ctx, `UPDATE agents SET connection_mode='mcp_server',mcp_tool_name='lookup',visibility='private' WHERE id=$1`, target)
			require.NoError(t, err)
			tool, err := mcpbinding.Resolve(ctx, pool, creatorID, target, newTestConfig().EffectiveRuntimeMasterSecret())
			require.NoError(t, err)
			tool.BindingID = uuid.New()
			raw, err := json.Marshal(tool)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `INSERT INTO agent_mcp_bindings(agent_id,target_agent_id,binding_id,owner_user_id,digest,tool) VALUES($1,$2,$3,$4,$5,$6)`, agentID, target, tool.BindingID, creatorID, tool.Digest, raw)
			require.NoError(t, err)
			caller := insertRuntimeUser(t, pool)
			run, err := newTestService(t, pool).StartRun(ctx, caller, makeRunReq(agentID, map[string]any{"text": "queued"}), "api")
			if tc.history && !tc.compatible && tc.status != "revoked" {
				var unavailable *httpx.HTTPError
				require.ErrorAs(t, err, &unavailable)
				require.Equal(t, 503, unavailable.Status)
				require.Equal(t, httpx.CodeServiceUnavailable, unavailable.Code)
				rec := httptest.NewRecorder()
				require.NoError(t, httpx.SendError(echo.New().NewContext(httptest.NewRequest("GET", "/", nil), rec), err))
				require.NotContains(t, strings.ToLower(rec.Body.String()), "mcp")
				require.NotContains(t, rec.Body.String(), tool.BindingID.String())
				var count int
				require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE user_id=$1`, caller).Scan(&count))
				require.Zero(t, count)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, run.NextAction)
			var count int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1 AND event_type='run.dispatch.waiting_runtime' AND payload->>'reason'='runtime_offline'`, run.RunID).Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM run_mcp_binding_snapshots WHERE run_id=$1 AND binding_id=$2`, run.RunID, tool.BindingID).Scan(&count))
			require.Equal(t, 1, count)
			_, err = db.New(pool).LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agentID})
			require.ErrorIs(t, err, pgx.ErrNoRows)
			candidate, err := db.New(pool).LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agentID, MCPBindings: true})
			require.NoError(t, err)
			require.Equal(t, run.RunID, candidate.ID.String())
		})
	}
}
