package a2a_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/a2a"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/webhook"
)

const truncateA2ATables = "TRUNCATE runtime_signal_outbox, runtime_session_attachments, runtime_sessions, runtime_nodes, task_callback_deliveries, task_callback_subscriptions, run_artifact_chunks, run_artifacts, run_messages, run_delegations, agent_tokens, agent_call_policies, run_events, runs, agents, users RESTART IDENTITY CASCADE"

func setupService(t *testing.T) (*pgxpool.Pool, *a2a.Service, *runtime.Service) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 A2A 集成测试")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(context.Background()))
	_, err = pool.Exec(context.Background(), truncateA2ATables)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_cluster_control
SET mode = 'normal', expected_replicas = 1, reopened_at = clock_timestamp(),
    version = version + 1, updated_at = clock_timestamp()
WHERE singleton_id = 1`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), truncateA2ATables)
		_, _ = pool.Exec(context.Background(), `
UPDATE runtime_cluster_control
SET mode = 'hard_maintenance', expected_replicas = 1,
    hard_maintenance_at = clock_timestamp(), reopened_at = NULL,
    version = version + 1, updated_at = clock_timestamp()
WHERE singleton_id = 1`)
		pool.Close()
	})
	runtimeSvc := runtime.NewService(pool, &config.Config{
		RunTimeoutSeconds:       15,
		AllowLocalHTTPEndpoints: true,
	})
	runtimeSvc.ConfigureCoreRuntime(uuid.New())
	return pool, a2a.NewService(pool, runtimeSvc), runtimeSvc
}

func insertCreator(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO users (id, email, password_hash, display_name, is_creator)
		 VALUES ($1, $2, 'x', 'creator', TRUE)`,
		id, id.String()+"@example.com")
	require.NoError(t, err)
	return id
}

func insertAgent(t *testing.T, pool *pgxpool.Pool, creatorID uuid.UUID, endpoint string, tags ...string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	agentTags := tags
	if agentTags == nil {
		agentTags = []string{}
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agents (
		  id, creator_id, slug, name, description, endpoint_url, price_per_call_cents, tags,
		  lifecycle_status, visibility, certification_status
		) VALUES ($1, $2, $3, 'A2A Agent', 'test', $4, 25, $5, 'active', 'public', 'unreviewed')`,
		id, creatorID, "a2a-"+id.String()[:8], endpoint, agentTags)
	require.NoError(t, err)
	return id
}

func attachSkill(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID, skillID, skillName string) {
	t.Helper()
	category := strings.Split(skillID, "/")[0]
	_, err := pool.Exec(context.Background(),
		`INSERT INTO skills (id, category, name, description, sort_order)
		 VALUES ($1, $2, $3, 'test skill', 0)
		 ON CONFLICT (id) DO NOTHING`,
		skillID, category, skillName)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(),
		`INSERT INTO agent_skills (agent_id, skill_id)
		 VALUES ($1, $2)
		 ON CONFLICT DO NOTHING`,
		agentID, skillID)
	require.NoError(t, err)
}

func insertParentRun(t *testing.T, pool *pgxpool.Pool, userID, callerAgentID uuid.UUID) uuid.UUID {
	t.Helper()
	return insertPendingRuntimeRun(t, pool, userID, callerAgentID, "web")
}

func insertPendingRuntimeRun(t *testing.T, pool *pgxpool.Pool, userID, agentID uuid.UUID, source string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	keyHash := sha256.Sum256([]byte("a2a-test-key/" + id.String()))
	fingerprint := sha256.Sum256([]byte("a2a-test-fingerprint/" + id.String()))
	_, err := pool.Exec(context.Background(),
		`INSERT INTO runs (
		  id, user_id, agent_id, input, status, cost_cents, platform_fee_cents,
		  creator_revenue_cents, source, runtime_contract_id,
		  idempotency_key_hash, idempotency_fingerprint, request_metadata,
		  connection_mode_snapshot, endpoint_idempotency_snapshot,
		  max_offer_count, max_attempts, dispatch_deadline_at, run_deadline_at,
		  dispatch_state
		) SELECT
		  $1, $2, $3, '{}'::jsonb, 'running', 0, 0, 0, $4,
		  'openlinker.runtime.v2', $5, $6, '{}'::jsonb,
		  a.connection_mode,
		  CASE WHEN a.connection_mode IN ('direct_http', 'mcp_server') THEN FALSE ELSE NULL END,
		  20, 3, clock_timestamp() + INTERVAL '10 minutes',
		  clock_timestamp() + INTERVAL '60 minutes', 'pending'
		FROM agents a
		WHERE a.id = $3`,
		id, userID, agentID, source, keyHash[:], fingerprint[:])
	require.NoError(t, err)
	return id
}

func insertDelegatedRun(t *testing.T, pool *pgxpool.Pool, userID, childAgentID, parentRunID, callerAgentID uuid.UUID) uuid.UUID {
	t.Helper()
	childRunID := insertPendingRuntimeRun(t, pool, userID, childAgentID, "api")
	_, err := pool.Exec(context.Background(),
		`INSERT INTO run_delegations (child_run_id, parent_run_id, caller_agent_id, reason)
		 VALUES ($1, $2, $3, 'test chain')`,
		childRunID, parentRunID, callerAgentID)
	require.NoError(t, err)
	return childRunID
}

func markRunLegacyTerminal(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID, status string) {
	t.Helper()
	terminalEventID := uuid.New()
	eventType := "run.succeeded"
	if status != "success" {
		eventType = "run.failed"
	}
	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
UPDATE runs
SET runtime_contract_id = 'legacy.pre-v2',
    idempotency_key_hash = NULL,
    idempotency_fingerprint = NULL,
    connection_mode_snapshot = NULL,
    endpoint_idempotency_snapshot = NULL,
    dispatch_deadline_at = NULL,
    run_deadline_at = NULL,
    status = $2,
    dispatch_state = 'terminal',
    output = '{}'::jsonb,
    duration_ms = 1,
    finished_at = clock_timestamp(),
    terminal_event_id = $3
WHERE id = $1`, runID, status, terminalEventID); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
INSERT INTO run_events (id, run_id, sequence, event_type, payload)
VALUES ($1, $2, 1, $3, '{}'::jsonb)`, terminalEventID, runID, eventType)
		return err
	})
	require.NoError(t, err)
}

func makeRuntimePullAgent(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE agents
		    SET connection_mode='runtime',
		        endpoint_url='openlinker-runtime://' || slug
		  WHERE id=$1`,
		agentID)
	require.NoError(t, err)
}

func TestRuntimeWorkbenchShowsSessionAndBacklog(t *testing.T) {
	pool, svc, _ := setupService(t)
	ownerID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "https://example.com/runtime")
	makeRuntimePullAgent(t, pool, agentID)
	runID := insertParentRun(t, pool, ownerID, agentID)
	insertRuntimeWorkbenchSession(t, pool, ownerID, agentID, 30*time.Second)

	workbench, err := svc.GetRuntimeWorkbench(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	assert.Equal(t, agentID.String(), workbench.Agent.ID)
	assert.Equal(t, "runtime", workbench.Agent.ConnectionMode)
	assert.True(t, workbench.Agent.ReadinessCallable)
	assert.Equal(t, "ws_primary_long_poll_fallback", workbench.Runtime.TransportPolicy)
	assert.Equal(t, "websocket", workbench.Runtime.PrimaryTransport)
	assert.Equal(t, "long_poll", workbench.Runtime.FallbackTransport)
	assert.Equal(t, "long_poll", workbench.Runtime.CurrentTransport)
	assert.Equal(t, map[string]int32{"websocket": 0, "long_poll": 1}, workbench.Runtime.TransportCounts)
	require.NotNil(t, workbench.Runtime.TransportChangedAt)
	require.NotNil(t, workbench.Runtime.FallbackReason)
	assert.Equal(t, "websocket_unavailable", *workbench.Runtime.FallbackReason)
	assert.Equal(t, "online", workbench.Runtime.ConnectionStatus)
	assert.Equal(t, int32(1), workbench.Runtime.ActiveNodeCount)
	assert.Equal(t, int32(1), workbench.Runtime.ActiveSessionCount)
	assert.Equal(t, int32(1), workbench.Runtime.ReadySessionCount)
	assert.Equal(t, int32(4), workbench.Runtime.TotalCapacity)
	assert.Equal(t, int32(1), workbench.Runtime.PendingRunCount)
	assert.Equal(t, runtime.RuntimeContractID, workbench.Runtime.RuntimeContractID)
	assert.Equal(t, runtime.RuntimeContractDigest, workbench.Runtime.RuntimeContractDigest)
	require.NotNil(t, workbench.Runtime.LastSessionActivityAt)
	require.NotEmpty(t, workbench.RecentRuns)
	assert.Equal(t, runID.String(), workbench.RecentRuns[0].RunID)
	assert.Equal(t, "running", workbench.RecentRuns[0].Status)
	assert.Equal(t, "pending", workbench.RecentRuns[0].DispatchState)
	assert.Equal(t, int32(3), workbench.RecentRuns[0].MaxAttempts)

	var codes []string
	for _, item := range workbench.Diagnostics {
		codes = append(codes, item.Code)
	}
	assert.Equal(t, []string{"runtime_ready"}, codes)

	err = pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `
UPDATE runtime_session_attachments attachment
SET detached_at = clock_timestamp(), disconnect_reason = 'workbench liveness boundary'
FROM runtime_sessions session
WHERE session.runtime_session_id = attachment.runtime_session_id
  AND session.agent_id = $1
  AND attachment.detached_at IS NULL`, agentID); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
UPDATE runtime_sessions
SET status = 'offline', attached_core_instance_id = NULL,
    disconnected_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE agent_id = $1 AND status IN ('active', 'draining')`, agentID)
		return err
	})
	require.NoError(t, err)
	insertRuntimeWorkbenchSession(t, pool, ownerID, agentID, 46*time.Second)
	stale, err := svc.GetRuntimeWorkbench(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	assert.Equal(t, "offline", stale.Runtime.ConnectionStatus)
	assert.Equal(t, "unknown", stale.Runtime.CurrentTransport)
}

func TestRuntimeWorkbenchShowsOfflineBacklogWithoutClaimLanguage(t *testing.T) {
	pool, svc, _ := setupService(t)
	ownerID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "https://example.com/runtime")
	makeRuntimePullAgent(t, pool, agentID)
	insertParentRun(t, pool, ownerID, agentID)

	workbench, err := svc.GetRuntimeWorkbench(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	assert.Equal(t, "offline", workbench.Runtime.ConnectionStatus)
	assert.False(t, workbench.Agent.ReadinessCallable)

	var codes []string
	for _, item := range workbench.Diagnostics {
		codes = append(codes, item.Code)
	}
	assert.Contains(t, codes, "runtime_session_offline")
	assert.Contains(t, codes, "runtime_backlog_without_capacity")
	encoded, err := json.Marshal(workbench)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "claimed")
	assert.NotContains(t, string(encoded), "heartbeat")
}

func TestCallPolicyReadback(t *testing.T) {
	pool, svc, _ := setupService(t)
	ownerID := insertCreator(t, pool)
	otherUserID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "https://example.com/runtime")

	policy, err := svc.GetCallPolicy(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	assert.Equal(t, "public", policy.CallableBy)

	updatedPolicy, err := svc.UpdateCallPolicy(context.Background(), ownerID, agentID, &a2a.UpdateCallPolicyRequest{CallableBy: "same_creator"})
	require.NoError(t, err)
	assert.Equal(t, "same_creator", updatedPolicy.CallableBy)
	policy, err = svc.GetCallPolicy(context.Background(), ownerID, agentID)
	require.NoError(t, err)
	assert.Equal(t, "same_creator", policy.CallableBy)

	_, err = svc.GetCallPolicy(context.Background(), otherUserID, agentID)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
}

func insertRuntimeWorkbenchSession(
	t *testing.T,
	pool *pgxpool.Pool,
	ownerID, agentID uuid.UUID,
	age time.Duration,
) {
	t.Helper()
	nodeID := uuid.New()
	credentialID := uuid.New()
	sessionID := uuid.New()
	coreID := uuid.New()
	prefix := "ol_agent_" + credentialID.String()[:8]
	serial := strings.ReplaceAll(nodeID.String(), "-", "")
	thumbprint := serial + serial
	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    status, redeemed_at
) VALUES ($1, $2, $3, 'workbench-v2', $4, 'test-hash',
          ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
			credentialID, agentID, ownerID, prefix); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features,
    capacity, inflight, status, last_seen_at
) VALUES ($1, 'Workbench Node', $2, $3, 'workbench-v2', 2,
          $4, $5, $6, 4, 0, 'active',
          clock_timestamp() - ($7::bigint * INTERVAL '1 millisecond'))`,
			nodeID, serial, thumbprint, runtime.RuntimeContractID,
			runtime.RuntimeContractDigest, runtime.RuntimeRequiredFeatures(), age.Milliseconds()); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
INSERT INTO runtime_sessions (
    runtime_session_id, node_id, agent_id, credential_id, worker_id,
    session_epoch, device_certificate_serial, node_version,
    protocol_version, runtime_contract_id, runtime_contract_digest,
    features, capacity, inflight, status, attached_core_instance_id,
    heartbeat_at
) VALUES ($1, $2, $3, $4, 'workbench-worker', 1, $5, 'workbench-v2',
          2, $6, $7, $8, 2, 0, 'active', $9,
          clock_timestamp() - ($10::bigint * INTERVAL '1 millisecond'))`,
			sessionID, nodeID, agentID, credentialID, serial,
			runtime.RuntimeContractID, runtime.RuntimeContractDigest,
			runtime.RuntimeRequiredFeatures(), coreID, age.Milliseconds()); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
INSERT INTO runtime_session_attachments (
    runtime_session_id, core_instance_id, attachment_kind,
    transport, transport_reason
) VALUES ($1, $2, 'connected', 'long_poll', 'websocket_unavailable')`, sessionID, coreID)
		return err
	})
	require.NoError(t, err)
}

func TestListParentRunsAggregatesRootContextAndChildrenTree(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	rootAgent := insertAgent(t, pool, owner, "https://example.com/root", "orchestration")
	childAAgent := insertAgent(t, pool, owner, "https://example.com/child-a", "analysis")
	childBAgent := insertAgent(t, pool, owner, "https://example.com/child-b", "scraping")
	grandchildAgent := insertAgent(t, pool, owner, "https://example.com/grandchild", "grandchild")
	attachSkill(t, pool, grandchildAgent, "data/deep-analysis", "深度分析")

	rootRun := insertParentRun(t, pool, owner, rootAgent)
	childA := insertDelegatedRun(t, pool, owner, childAAgent, rootRun, rootAgent)
	childB := insertDelegatedRun(t, pool, owner, childBAgent, rootRun, rootAgent)
	grandchild := insertDelegatedRun(t, pool, owner, grandchildAgent, childA, childAAgent)

	markRunLegacyTerminal(t, pool, childA, "success")
	markRunLegacyTerminal(t, pool, grandchild, "success")
	var err error
	for _, spec := range []struct {
		runID         uuid.UUID
		agentID       uuid.UUID
		parentRunID   uuid.UUID
		callerAgentID uuid.UUID
		targetAgentID uuid.UUID
		taskID        string
		parentTaskID  string
	}{
		{childA, childAAgent, rootRun, rootAgent, childAAgent, childA.String(), rootRun.String()},
		{childB, childBAgent, rootRun, rootAgent, childBAgent, childB.String(), rootRun.String()},
		{grandchild, grandchildAgent, childA, childAAgent, grandchildAgent, grandchild.String(), childA.String()},
	} {
		_, err = pool.Exec(context.Background(),
			`INSERT INTO a2a_context_mappings (
			    run_id, user_id, agent_id, protocol_context_id, protocol_task_id,
			    root_context_id, parent_context_id, parent_task_id, parent_run_id,
			    caller_agent_id, target_agent_id, trace_id, reference_task_ids, source
			  ) VALUES (
			    $1, $2, $3, 'ctx-root-session', $4,
			    'ctx-root-session', 'ctx-root-session', $5, $6,
			    $7, $8, 'trace-root-session', ARRAY[$5]::text[], 'agent_delegation'
			  )`,
			spec.runID, owner, spec.agentID, spec.taskID, spec.parentTaskID, spec.parentRunID,
			spec.callerAgentID, spec.targetAgentID,
		)
		require.NoError(t, err)
	}

	parents, err := svc.ListParentRuns(context.Background(), owner, 1, 10, "")
	require.NoError(t, err)
	require.Len(t, parents.Items, 1)
	assert.Equal(t, int32(1), parents.Total)
	assert.Equal(t, rootRun.String(), parents.Items[0].ParentRunID)
	assert.Equal(t, int32(3), parents.Items[0].ChildCount)
	assert.Equal(t, int32(2), parents.Items[0].SuccessfulChildCount)
	assert.Equal(t, int32(1), parents.Items[0].RunningChildCount)
	require.NotNil(t, parents.Items[0].A2AContext)
	assert.Equal(t, "ctx-root-session", parents.Items[0].A2AContext.RootContextID)
	assert.Equal(t, "trace-root-session", parents.Items[0].A2AContext.TraceID)

	filtered, err := svc.ListParentRuns(context.Background(), owner, 1, 10, "grandchild")
	require.NoError(t, err)
	require.Len(t, filtered.Items, 1)
	assert.Equal(t, rootRun.String(), filtered.Items[0].ParentRunID)

	children, err := svc.ListChildren(context.Background(), owner, rootRun)
	require.NoError(t, err)
	require.Len(t, children, 2)
	assert.Equal(t, childA.String(), children[0].ChildRunID)
	require.Len(t, children[0].Children, 1)
	assert.Equal(t, grandchild.String(), children[0].Children[0].ChildRunID)
	assert.Equal(t, childB.String(), children[1].ChildRunID)
	assert.Empty(t, children[1].Children)
}

func TestProtocolMessageSendAndGetTask(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	var receivedInput map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input map[string]any `json:"input"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		receivedInput = request.Input
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": map[string]any{
				"summary": "A2A protocol adapter completed",
				"answer":  "done",
				"artifacts": []map[string]any{{
					"title":           "Result CSV",
					"artifact_type":   "file",
					"file_uri":        "https://files.example/a2a-result.csv",
					"file_name":       "a2a-result.csv",
					"mime_type":       "text/csv",
					"file_sha256":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					"file_size_bytes": 256,
					"content":         map[string]any{"rows": 2},
				}},
			},
			"events": []map[string]any{{
				"event_type": "run.message.delta",
				"payload": map[string]any{
					"text": "adapter evidence message",
				},
			}},
		})
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]

	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			Kind:             "message",
			MessageID:        "msg-1",
			ContextID:        "ctx-1",
			ReferenceTaskIDs: []string{"task-parent"},
			Role:             "user",
			Parts: []map[string]any{{
				"kind": "text",
				"text": "请完成一次标准 A2A 调用",
			}},
		},
		Metadata: map[string]any{"trace_id": "a2a-protocol-test"},
	})
	require.NoError(t, err)
	assert.Equal(t, "task", task.Kind)
	assert.Equal(t, "completed", task.Status.State)
	assert.Equal(t, "ctx-1", task.ContextID)
	require.NotNil(t, task.Status.Message)
	require.NotEmpty(t, task.Artifacts)
	assert.Equal(t, "请完成一次标准 A2A 调用", receivedInput["message"])
	var protocolContextID, protocolTaskID, rootContextID, traceID string
	var referenceTaskIDs []string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT protocol_context_id, protocol_task_id, root_context_id, trace_id, reference_task_ids
		   FROM a2a_context_mappings
		  WHERE run_id=$1`,
		task.ID,
	).Scan(&protocolContextID, &protocolTaskID, &rootContextID, &traceID, &referenceTaskIDs))
	assert.Equal(t, "ctx-1", protocolContextID)
	assert.Equal(t, task.ID, protocolTaskID)
	assert.Equal(t, "ctx-1", rootContextID)
	assert.Equal(t, "a2a-protocol-test", traceID)
	assert.Equal(t, []string{"task-parent"}, referenceTaskIDs)

	historyLength := 10
	reloaded, err := svc.GetProtocolTask(context.Background(), owner, slug, task.ID, &historyLength)
	require.NoError(t, err)
	assert.Equal(t, task.ID, reloaded.ID)
	assert.Equal(t, "completed", reloaded.Status.State)
	require.NotEmpty(t, reloaded.History)
	require.NotEmpty(t, reloaded.Artifacts)
	var fileArtifact *a2a.A2AArtifact
	for i := range reloaded.Artifacts {
		if reloaded.Artifacts[i].Name == "Result CSV" {
			fileArtifact = &reloaded.Artifacts[i]
			break
		}
	}
	require.NotNil(t, fileArtifact)
	require.Len(t, fileArtifact.Parts, 1)
	assert.Equal(t, "file", fileArtifact.Parts[0]["kind"])
	filePart, ok := fileArtifact.Parts[0]["file"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "https://files.example/a2a-result.csv", filePart["uri"])
	assert.Equal(t, "text/csv", filePart["mimeType"])
}

func TestProtocolMessageUsesMessageIDForReplayAndStableServerContext(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	var targetHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"idempotent A2A response"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	params := &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID:        "message-replay-1",
			ReferenceTaskIDs: []string{"ref-z", "ref-a", "ref-z"},
			Extensions:       []string{"urn:ext-z", "urn:ext-a"},
			Role:             "user",
			Parts:            []map[string]any{{"kind": "text", "text": "run once"}},
		},
		Metadata: map[string]any{
			"business":            "same",
			"trace_id":            "trace-first-attempt",
			"delivery_visibility": "shared",
			"a2a_extensions":      []string{"urn:ext-m", "urn:ext-a"},
			"a2a_options":         map[string]any{"priority": "normal"},
		},
	}
	returnImmediately := true
	params.Configuration = &a2a.A2ASendConfiguration{ReturnImmediately: &returnImmediately}

	first, err := svc.SendProtocolMessage(context.Background(), owner, slug, params)
	require.NoError(t, err)
	replay, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID:        "message-replay-1",
			ReferenceTaskIDs: []string{"ref-a", "ref-z"},
			Extensions:       []string{"urn:ext-m", "urn:ext-z"},
			Role:             "user",
			Parts:            []map[string]any{{"kind": "text", "text": "run once"}},
			Metadata: map[string]any{
				"business": "same",
				"traceId":  "trace-retry-attempt",
			},
		},
		Configuration: &a2a.A2ASendConfiguration{
			ReturnImmediately: &returnImmediately,
			Visibility:        "shared",
			Options:           map[string]any{"priority": "normal"},
		},
		Metadata: map[string]any{
			"a2a_extensions": []string{"urn:ext-a"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, first.ID, replay.ID)
	assert.Equal(t, "ctx-"+first.ID, first.ContextID)
	assert.Equal(t, first.ContextID, replay.ContextID)
	require.Eventually(t, func() bool { return targetHits.Load() == 1 }, time.Second, 10*time.Millisecond)
	require.NotNil(t, replay.Metadata)
	openlinker, ok := replay.Metadata["openlinker"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, true, openlinker["replayed"])
	_, err = svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "message-replay-1",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "different request"}},
		},
		Configuration: params.Configuration,
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusConflict)
	httpErr, ok := err.(*httpx.HTTPError)
	require.True(t, ok)
	assert.Equal(t, httpx.ErrorCode(runtime.IdempotencyErrorKeyReused), httpErr.Code)
	assert.Equal(t, int32(1), targetHits.Load())

	var createdEvents, signals int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM run_events WHERE run_id=$1 AND event_type='run.created'`, first.ID,
	).Scan(&createdEvents))
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM runtime_signal_outbox WHERE run_id=$1 AND event_type='run.available'`, first.ID,
	).Scan(&signals))
	assert.Equal(t, 1, createdEvents)
	assert.Equal(t, 1, signals)
}

func TestProtocolMessageAcceptsCurrentPartShapes(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	var receivedInput map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input map[string]any `json:"input"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		receivedInput = request.Input
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"current parts ok"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]

	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-current",
			ContextID: "ctx-current",
			Role:      "user",
			Parts: []map[string]any{
				{"text": "请读取附件并输出摘要"},
				{"data": map[string]any{"rows": 3}, "mediaType": "application/json"},
				{
					"url":       "https://files.example/input.csv",
					"filename":  "input.csv",
					"mediaType": "text/csv",
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", task.Status.State)
	assert.Equal(t, "请读取附件并输出摘要", receivedInput["message"])
	dataParts, ok := receivedInput["data_parts"].([]any)
	require.True(t, ok)
	require.Len(t, dataParts, 1)
	assert.Equal(t, map[string]any{"rows": float64(3)}, dataParts[0])
	files, ok := receivedInput["files"].([]any)
	require.True(t, ok)
	require.Len(t, files, 1)
	file, ok := files[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://files.example/input.csv", file["uri"])
	assert.Equal(t, "input.csv", file["name"])
	assert.Equal(t, "text/csv", file["mimeType"])
}

func TestProtocolMessageReusesProtocolTaskIDForContinuation(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	var calls []map[string]any
	var callA2AMetadata []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input    map[string]any `json:"input"`
			Metadata map[string]any `json:"metadata"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		calls = append(calls, request.Input)
		a2aMetadata, _ := request.Metadata["a2a"].(map[string]any)
		callA2AMetadata = append(callA2AMetadata, a2aMetadata)
		w.Header().Set("Content-Type", "application/json")
		if messageID, _ := a2aMetadata["message_id"].(string); strings.HasPrefix(messageID, "input-required-") {
			_, _ = w.Write([]byte(`{"output":{"summary":"need input","a2a":{"task_state":"input_required","status_message":"Need a follow-up"}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"output":{"summary":"follow-up complete","a2a":{"task_state":"completed","artifacts":[{"artifactId":"followup","parts":[{"kind":"text","text":"done"}]}]}}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]

	first, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "input-required-1",
			ContextID: "ctx-multi",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "start"}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, first.ID)
	assert.Equal(t, "ctx-multi", first.ContextID)
	assert.Equal(t, "input_required", first.Status.State)
	require.NotNil(t, first.Status.Message)
	assert.Equal(t, "Need a follow-up", first.Status.Message.Parts[0]["text"])

	followup, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "complete-1",
			TaskID:    first.ID,
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "finish"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, first.ID, followup.ID)
	assert.Equal(t, "ctx-multi", followup.ContextID)
	assert.Equal(t, "completed", followup.Status.State)
	require.Len(t, calls, 2)
	require.Len(t, callA2AMetadata, 2)
	assert.Equal(t, "input-required-1", callA2AMetadata[0]["message_id"])
	assert.Equal(t, "complete-1", callA2AMetadata[1]["message_id"])
	for _, metadata := range callA2AMetadata {
		assert.Equal(t, "a2a", metadata["protocol"])
		assert.Equal(t, "message.send", metadata["method"])
	}
	assert.Equal(t, first.ID, callA2AMetadata[1]["task_id"])
	assert.Equal(t, "ctx-multi", callA2AMetadata[1]["context_id"])
	for _, input := range calls {
		assert.NotContains(t, input, "a2a_message_id")
		assert.Equal(t, "ctx-multi", input["a2a_context_id"])
		assert.Equal(t, first.ID, input["a2a_task_id"])
		assert.Equal(t, "ctx-multi", input["a2a_root_context_id"])
		assert.NotContains(t, input, "a2a_reference_task_ids")
	}

	reloaded, err := svc.GetProtocolTask(context.Background(), owner, slug, first.ID, nil)
	require.NoError(t, err)
	assert.Equal(t, first.ID, reloaded.ID)
	assert.Equal(t, "completed", reloaded.Status.State)
	require.NotEmpty(t, reloaded.Artifacts)
	assert.Equal(t, "followup", reloaded.Artifacts[0].ArtifactID)

	_, err = svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "too-late",
			TaskID:    first.ID,
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "after terminal"}},
		},
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	second, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "input-required-2",
			ContextID: "ctx-cancel-projected",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "start cancelable"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "input_required", second.Status.State)
	canceled, err := svc.CancelProtocolTask(context.Background(), owner, slug, second.ID)
	require.NoError(t, err)
	assert.Equal(t, second.ID, canceled.ID)
	assert.Equal(t, "canceled", canceled.Status.State)
}

func TestProtocolServiceValidationAndOwnershipEdges(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	other := insertCreator(t, pool)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"edge task done"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	otherAgentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	otherSlug := "a2a-" + otherAgentID.String()[:8]
	params := &a2a.A2AMessageSendParams{Message: a2a.A2AMessage{
		Kind:      "message",
		MessageID: "validate-service-edges",
		Role:      "user",
		Parts:     []map[string]any{{"kind": "text", "text": "validate service edges"}},
	}}

	_, err := svc.SendProtocolMessage(context.Background(), owner, " ", params)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.SendProtocolMessage(context.Background(), owner, slug, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.SendProtocolMessage(context.Background(), owner, "missing-agent", params)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{Message: a2a.A2AMessage{MessageID: "invalid-empty-message", Role: "user"}})
	requireA2AServiceHTTPStatus(t, err, http.StatusUnprocessableEntity)

	_, err = svc.StartProtocolMessage(context.Background(), owner, " ", params)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.StartProtocolMessage(context.Background(), owner, slug, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.StartProtocolMessage(context.Background(), owner, "missing-agent", params)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)

	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, params)
	require.NoError(t, err)

	_, err = svc.GetProtocolTask(context.Background(), owner, "", task.ID, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.GetProtocolTask(context.Background(), owner, slug, "not-a-uuid", nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetProtocolTask(context.Background(), other, slug, task.ID, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetProtocolTask(context.Background(), owner, otherSlug, task.ID, nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)

	_, err = svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{PageToken: "bad-token"})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{StatusTimestampAfter: "not-time"})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)
	pushParams := a2a.A2ATaskPushConfigParams{
		ID:                     "not-a-uuid",
		PushNotificationConfig: a2a.A2APushNotificationConfig{URL: server.URL + "/push"},
	}
	_, err = svc.SetPushNotificationConfig(context.Background(), owner, slug, &pushParams)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)

	missingRunID := uuid.NewString()
	_, err = svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: missingRunID})
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       missingRunID,
		PushNotificationConfigID: uuid.NewString(),
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	err = svc.DeletePushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       missingRunID,
		PushNotificationConfigID: uuid.NewString(),
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
}

func TestProtocolStreamEventsExposeStatusAndArtifactUpdates(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	release := make(chan struct{})
	called := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case called <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": map[string]any{"summary": "stream completed"},
			"events": []map[string]any{
				{"event_type": "run.message.delta", "payload": map[string]any{"text": "working"}},
				{"event_type": "run.artifact.delta", "payload": map[string]any{
					"artifact_id": "stream-report",
					"title":       "Stream Report",
					"data":        map[string]any{"ok": true},
					"last_chunk":  true,
				}},
			},
		})
	}))
	defer server.Close()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]

	task, err := svc.StartProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			Kind:      "message",
			MessageID: "msg-stream",
			ContextID: "ctx-stream",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "stream it"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "working", task.Status.State)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async endpoint was not called")
	}

	items, terminal, nextSequence, err := svc.ListProtocolTaskEvents(context.Background(), owner, slug, task.ID, 0)
	require.NoError(t, err)
	assert.False(t, terminal)
	assert.GreaterOrEqual(t, len(items), 2)
	assert.Greater(t, nextSequence, int32(0))

	close(release)
	var finalItems []interface{}
	require.Eventually(t, func() bool {
		var err error
		finalItems, terminal, _, err = svc.ListProtocolTaskEvents(context.Background(), owner, slug, task.ID, 0)
		return err == nil && terminal
	}, time.Second, 20*time.Millisecond)

	var sawFinal bool
	var sawArtifact bool
	for _, item := range finalItems {
		switch event := item.(type) {
		case *a2a.A2ATaskStatusUpdateEvent:
			if event.Final && event.Status.State == "completed" {
				sawFinal = true
			}
		case *a2a.A2ATaskArtifactUpdateEvent:
			if event.Artifact.ArtifactID == "stream-report" {
				sawArtifact = true
			}
		}
	}
	assert.True(t, sawFinal, "expected final completed status-update")
	assert.True(t, sawArtifact, "expected artifact-update from run.artifact.delta")
}

func TestProtocolMessageReturnImmediatelyStartsAsyncTask(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	called := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-called:
		default:
			close(called)
		}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"async done"}}`))
	}))
	defer server.Close()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	returnImmediately := true
	done := make(chan struct {
		task *a2a.A2ATask
		err  error
	}, 1)
	go func() {
		task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
			Message: a2a.A2AMessage{
				MessageID: "msg-return-immediately",
				ContextID: "ctx-return-immediately",
				Role:      "ROLE_USER",
				Parts:     []map[string]any{{"text": "return now"}},
			},
			Configuration: &a2a.A2ASendConfiguration{ReturnImmediately: &returnImmediately},
		})
		done <- struct {
			task *a2a.A2ATask
			err  error
		}{task: task, err: err}
	}()

	var result struct {
		task *a2a.A2ATask
		err  error
	}
	select {
	case result = <-done:
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("SendProtocolMessage blocked despite returnImmediately=true")
	}
	require.NoError(t, result.err)
	require.NotNil(t, result.task)
	assert.Equal(t, "working", result.task.Status.State)
	assert.Equal(t, "ctx-return-immediately", result.task.ContextID)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async endpoint was not called")
	}
	close(release)
}

func TestProtocolListTasksSupportsPagingFiltersAndArtifacts(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input map[string]any `json:"input"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"output": map[string]any{
				"summary": "listed " + request.Input["message"].(string),
				"artifacts": []map[string]any{{
					"title":         "List Evidence",
					"artifact_type": "json",
					"content":       map[string]any{"message": request.Input["message"]},
				}},
			},
			"events": []map[string]any{{
				"event_type": "run.message.delta",
				"payload": map[string]any{
					"text": "history for " + request.Input["message"].(string),
				},
			}},
		})
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	first, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-list-1",
			ContextID: "ctx-list-1",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "first"}},
		},
	})
	require.NoError(t, err)
	second, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-list-2",
			ContextID: "ctx-list-2",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "second"}},
		},
	})
	require.NoError(t, err)

	pageSize := 1
	historyLength := 1
	includeArtifacts := true
	page, err := svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{
		Status:           "completed",
		PageSize:         &pageSize,
		HistoryLength:    &historyLength,
		IncludeArtifacts: &includeArtifacts,
	})
	require.NoError(t, err)
	require.Len(t, page.Tasks, 1)
	assert.Equal(t, int32(1), page.PageSize)
	assert.Equal(t, int32(2), page.TotalSize)
	assert.NotEmpty(t, page.NextPageToken)
	assert.NotEmpty(t, page.Tasks[0].Artifacts)
	assert.NotEmpty(t, page.Tasks[0].History)

	nextPage, err := svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{
		Status:    "completed",
		PageSize:  &pageSize,
		PageToken: page.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, nextPage.Tasks, 1)
	assert.Empty(t, nextPage.NextPageToken)

	listedIDs := map[string]bool{
		page.Tasks[0].ID:     true,
		nextPage.Tasks[0].ID: true,
	}
	assert.True(t, listedIDs[first.ID])
	assert.True(t, listedIDs[second.ID])

	byContext, err := svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{ContextID: "ctx-list-1"})
	require.NoError(t, err)
	require.Len(t, byContext.Tasks, 1)
	assert.Equal(t, first.ID, byContext.Tasks[0].ID)
	assert.Equal(t, int32(1), byContext.TotalSize)

	_, err = svc.ListProtocolTasks(context.Background(), owner, "", nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.ListProtocolTasks(context.Background(), owner, "missing", nil)
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.ListProtocolTasks(context.Background(), owner, slug, &a2a.A2ATaskListParams{Status: "strange"})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)
}

func TestProtocolCancelTaskMapsRunCancellation(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)

	release := make(chan struct{})
	called := make(chan struct{}, 1)
	releaseServer := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case called <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"too late"}}`))
	}))
	defer server.Close()
	t.Cleanup(releaseServer)

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	task, err := svc.StartProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-cancel",
			ContextID: "ctx-cancel",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "cancel me"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "working", task.Status.State)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async endpoint was not called")
	}

	canceled, err := svc.CancelProtocolTask(context.Background(), owner, slug, task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.ID, canceled.ID)
	assert.Equal(t, "ctx-cancel", canceled.ContextID)
	assert.Equal(t, "canceled", canceled.Status.State)
	require.NotNil(t, canceled.Status.Message)
	assert.Contains(t, canceled.Status.Message.Parts[0]["text"], "canceled")
	releaseServer()

	_, err = svc.CancelProtocolTask(context.Background(), owner, slug, "not-a-uuid")
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
	_, err = svc.CancelProtocolTask(context.Background(), owner, slug, uuid.NewString())
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)
}

func TestProtocolMessageCreatesInlinePushNotificationConfig(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"inline push configured"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			MessageID: "msg-inline-push",
			ContextID: "ctx-inline-push",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "configure inline push"}},
		},
		Configuration: &a2a.A2ASendConfiguration{
			PushNotificationConfig: &a2a.A2APushNotificationConfig{
				URL:   server.URL + "/inline-push",
				Token: "inline-token",
				Metadata: map[string]any{
					"client": "inline-a2a",
				},
			},
		},
	})
	require.NoError(t, err)

	list, err := svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, task.ID, list.Items[0].TaskID)
	assert.Equal(t, server.URL+"/inline-push", list.Items[0].PushNotificationConfig.URL)
	assert.Equal(t, "Bearer", list.Items[0].PushNotificationConfig.Authentication.Scheme)
	assert.Equal(t, "inline-a2a", list.Items[0].PushNotificationConfig.Metadata["client"])
}

func TestPushNotificationConfigMapsToTaskCallback(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"push config task done"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			Kind:      "message",
			MessageID: "configure-push",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "configure push"}},
		},
	})
	require.NoError(t, err)

	cfg, err := svc.SetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			URL:   server.URL + "/push",
			Token: "test-token",
			Metadata: map[string]any{
				"client": "a2a-test",
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, cfg.PushNotificationConfig.ID)
	assert.Equal(t, task.ID, cfg.TaskID)
	assert.Equal(t, "Bearer", cfg.PushNotificationConfig.Authentication.Scheme)

	list, err := svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, cfg.PushNotificationConfig.ID, list.Items[0].PushNotificationConfig.ID)
	assert.Equal(t, "Bearer", list.Items[0].PushNotificationConfig.Authentication.Scheme)
	assert.Equal(t, "a2a-test", list.Items[0].PushNotificationConfig.Metadata["client"])

	got, err := svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       task.ID,
		PushNotificationConfigID: cfg.PushNotificationConfig.ID,
	})
	require.NoError(t, err)
	assert.Equal(t, cfg.PushNotificationConfig.ID, got.PushNotificationConfig.ID)

	err = svc.DeletePushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       task.ID,
		PushNotificationConfigID: cfg.PushNotificationConfig.ID,
	})
	require.NoError(t, err)
	list, err = svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	assert.Empty(t, list.Items)
}

func TestPushNotificationConfigLookupAndValidationEdges(t *testing.T) {
	pool, svc, _ := setupService(t)
	owner := insertCreator(t, pool)
	pushSvc := webhook.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true})
	svc.SetTaskCallbackManager(pushSvc)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"summary":"push edge task done"}}`))
	}))
	defer server.Close()

	agentID := insertAgent(t, pool, owner, server.URL)
	slug := "a2a-" + agentID.String()[:8]
	task, err := svc.SendProtocolMessage(context.Background(), owner, slug, &a2a.A2AMessageSendParams{
		Message: a2a.A2AMessage{
			Kind:      "message",
			MessageID: "push-lookup-edges",
			Role:      "user",
			Parts:     []map[string]any{{"kind": "text", "text": "exercise push lookup edges"}},
		},
	})
	require.NoError(t, err)

	_, err = svc.SetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                     task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{URL: " "},
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	_, err = svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	cfg1, err := svc.SetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			URL:        server.URL + "/push-1",
			EventTypes: []string{"run.completed"},
			Authentication: &a2a.A2APushAuthenticationInfo{
				Scheme:      "HMAC",
				Credentials: "secret-1",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "HMAC", cfg1.PushNotificationConfig.Authentication.Scheme)
	assert.Empty(t, cfg1.PushNotificationConfig.Authentication.Credentials)

	auto, err := svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	assert.Equal(t, cfg1.PushNotificationConfig.ID, auto.PushNotificationConfig.ID)

	byEmbeddedID, err := svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			ID: cfg1.PushNotificationConfig.ID,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, cfg1.PushNotificationConfig.ID, byEmbeddedID.PushNotificationConfig.ID)

	cfg2, err := svc.SetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			URL:   server.URL + "/push-2",
			Token: "token-2",
		},
	})
	require.NoError(t, err)

	_, err = svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	requireA2AServiceHTTPStatus(t, err, http.StatusBadRequest)

	_, err = svc.GetPushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       task.ID,
		PushNotificationConfigID: uuid.NewString(),
	})
	requireA2AServiceHTTPStatus(t, err, http.StatusNotFound)

	err = svc.DeletePushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID:                       task.ID,
		PushNotificationConfigID: "bad",
	})
	require.NoError(t, err)

	err = svc.DeletePushNotificationConfig(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{
		ID: task.ID,
		PushNotificationConfig: a2a.A2APushNotificationConfig{
			ID: cfg2.PushNotificationConfig.ID,
		},
	})
	require.NoError(t, err)

	list, err := svc.ListPushNotificationConfigs(context.Background(), owner, slug, &a2a.A2ATaskPushConfigParams{ID: task.ID})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, cfg1.PushNotificationConfig.ID, list.Items[0].PushNotificationConfig.ID)
}

func requireA2AServiceHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	httpErr, ok := err.(*httpx.HTTPError)
	require.True(t, ok, "expected *httpx.HTTPError, got %T (%v)", err, err)
	assert.Equal(t, want, httpErr.Status)
}
