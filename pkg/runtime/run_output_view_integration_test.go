package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const echoEndpointToken = "seller-endpoint-token"

// 形状同 httpbingo /anything，外加一条 Agent 自己上报的消息和产物。
func echoingEndpoint(w http.ResponseWriter, r *http.Request) {
	headers := map[string]any{"X-Forwarded-For": []string{"203.0.113.7"}, "Accept": []string{"application/json"}}
	for _, name := range []string{"X-Openlinker-Token", "X-Openlinker-Run-Id"} {
		headers[name] = []string{r.Header.Get(name)}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"output": map[string]any{
			"origin":  "203.0.113.7",
			"headers": headers,
			"note":    "signed with " + r.Header.Get("X-Openlinker-Token"),
			"cookie":  "oatmeal",
		},
		"events": []map[string]any{
			{"event_type": "run.message.delta", "payload": map[string]any{"text": "got " + r.Header.Get("X-Openlinker-Token"), "origin": "203.0.113.7"}},
			{"event_type": "run.artifact.delta", "payload": map[string]any{"artifact_id": "a1", "title": "echo", "content": map[string]any{"headers": headers}}},
		},
	})
}

type echoRunFixture struct {
	pool             *pgxpool.Pool
	svc              *runtime.Service
	buyer, creatorID uuid.UUID
	agentID          uuid.UUID
	run              *runtime.RunResponse
	runID            uuid.UUID
}

func setupEchoRun(t *testing.T) echoRunFixture {
	t.Helper()
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	buyer := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, echoingEndpoint)
	agentID := insertAgent(t, pool, creatorID, endpoint, 0, "approved")
	setAgentEndpointToken(t, pool, agentID, echoEndpointToken)
	run, err := svc.Run(context.Background(), buyer, makeRunReq(agentID, map[string]any{"q": "hello"}), "")
	require.NoError(t, err)
	require.Equal(t, "success", run.Status)
	return echoRunFixture{pool, svc, buyer, creatorID, agentID, run, uuid.MustParse(run.RunID)}
}

func requireNoEcho(t *testing.T, label string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	for _, leaked := range []string{echoEndpointToken, "203.0.113.7", "X-Forwarded-For", "X-Openlinker-Token"} {
		assert.NotContains(t, string(encoded), leaked, label)
	}
}

func TestNonCreatorReadsAreRedactedOnEveryRunRoute(t *testing.T) {
	f := setupEchoRun(t)
	ctx := context.Background()

	requireNoEcho(t, "sync run response", f.run)
	assert.True(t, f.run.OutputRedacted)
	assert.Equal(t, "oatmeal", f.run.Output["cookie"], "headers 容器外的同名字段是业务字段")
	assert.Equal(t, "signed with [redacted]", f.run.Output["note"])

	got, err := f.svc.GetRun(ctx, f.buyer, f.runID)
	require.NoError(t, err)
	requireNoEcho(t, "GET /runs/:id", got)
	assert.True(t, got.OutputRedacted)

	// 同一幂等键重试时，已完成的 Run 也要过滤。
	retryReq := makeRunReq(f.agentID, map[string]any{"q": "retry"})
	_, err = f.svc.Run(ctx, f.buyer, retryReq, "")
	require.NoError(t, err)
	retried, err := f.svc.Run(ctx, f.buyer, retryReq, "")
	require.NoError(t, err)
	require.True(t, retried.Replayed)
	requireNoEcho(t, "idempotent POST /run", retried)
	restarted, err := f.svc.StartRun(ctx, f.buyer, retryReq, "")
	require.NoError(t, err)
	require.True(t, restarted.Replayed)
	requireNoEcho(t, "idempotent POST /runs", restarted)

	events, err := f.svc.ListRunEvents(ctx, f.buyer, f.runID, 0, 100)
	require.NoError(t, err)
	requireNoEcho(t, "ListRunEvents", events)
	page, err := f.svc.ListRunEventsPage(ctx, f.buyer, f.runID, 0, 100)
	require.NoError(t, err)
	requireNoEcho(t, "ListRunEventsPage", page)
	require.Len(t, page.Items, len(events))

	var sawCompleted, sawStarted bool
	for _, event := range page.Items {
		switch event.EventType {
		case "run.completed":
			sawCompleted = true
			assert.True(t, event.OutputRedacted)
			assert.Contains(t, event.Payload, "output")
		case "run.started":
			sawStarted = true
			assert.NotContains(t, event.Payload, "endpoint_host")
			assert.False(t, event.OutputRedacted)
		}
	}
	assert.True(t, sawCompleted && sawStarted)

	// 事件 id 和序号与库内一致，断点续传不受影响。
	stored := readRunEvents(t, f.pool, f.runID)
	require.Len(t, page.Items, len(stored))
	for i, row := range stored {
		assert.Equal(t, row.EventType, page.Items[i].EventType)
		assert.Equal(t, row.Sequence, page.Items[i].Sequence)
	}
	var storedIDs []string
	rows, err := f.pool.Query(ctx, `SELECT id::text FROM run_events WHERE run_id=$1 ORDER BY sequence`, f.runID)
	require.NoError(t, err)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		storedIDs = append(storedIDs, id)
	}
	require.NoError(t, rows.Err())
	for i, id := range storedIDs {
		assert.Equal(t, id, page.Items[i].EventID)
	}

	messages, err := f.svc.ListRunMessages(ctx, f.buyer, f.runID)
	require.NoError(t, err)
	requireNoEcho(t, "ListRunMessages", messages)
	require.NotEmpty(t, messages)

	artifacts, err := f.svc.ListRunArtifacts(ctx, f.buyer, f.runID)
	require.NoError(t, err)
	requireNoEcho(t, "ListRunArtifacts", artifacts)
	require.NotEmpty(t, artifacts)
	assert.True(t, artifacts[0].OutputRedacted)
}

func TestCreatorAndInternalReadsKeepOriginalWithoutRewritingStorage(t *testing.T) {
	f := setupEchoRun(t)
	ctx := context.Background()

	storedOutput := func() string {
		var output string
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT output::text FROM runs WHERE id=$1`, f.runID).Scan(&output))
		return output
	}
	before := storedOutput()
	assert.Contains(t, before, echoEndpointToken)

	asCreator, err := f.svc.GetRun(ctx, f.creatorID, f.runID)
	require.NoError(t, err)
	assert.False(t, asCreator.OutputRedacted)
	assert.Contains(t, asCreator.Output, "origin")

	internal, err := f.svc.GetRunUnprojected(ctx, f.buyer, f.runID)
	require.NoError(t, err)
	assert.False(t, internal.OutputRedacted)
	assert.Contains(t, internal.Output, "origin")

	rawArtifacts, err := f.svc.ListRunArtifactsUnprojected(ctx, f.buyer, f.runID)
	require.NoError(t, err)
	encoded, err := json.Marshal(rawArtifacts)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), echoEndpointToken)

	rawReq := makeRunReq(f.agentID, map[string]any{"q": "again"})
	rawRun, err := f.svc.RunUnprojected(ctx, f.buyer, rawReq, "")
	require.NoError(t, err)
	assert.Contains(t, rawRun.Output, "origin")
	rawRetry, err := f.svc.RunUnprojected(ctx, f.buyer, rawReq, "")
	require.NoError(t, err)
	require.True(t, rawRetry.Replayed)
	assert.Contains(t, rawRetry.Output, "origin", "内部调用方重试时同样拿到原文")

	assert.Equal(t, before, storedOutput(), "读取不改写库内原文")
}

func TestCreatorCallingOwnAgentSeesOriginal(t *testing.T) {
	f := setupEchoRun(t)
	own, err := f.svc.Run(context.Background(), f.creatorID, makeRunReq(f.agentID, map[string]any{"q": "own"}), "")
	require.NoError(t, err)
	assert.False(t, own.OutputRedacted)
	assert.Contains(t, own.Output, "origin")
}

func TestBusinessOutputIsNotMarkedRedacted(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	buyer := insertRuntimeUser(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK,
		`{"output":{"origin":"Shanghai","authorization":{"summary":"Reviewed"}}}`))
	agentID := insertAgent(t, pool, insertCreator(t, pool), endpoint, 0, "approved")

	run, err := svc.Run(context.Background(), buyer, makeRunReq(agentID, map[string]any{"q": "business"}), "")
	require.NoError(t, err)
	assert.False(t, run.OutputRedacted)
	assert.Equal(t, "Shanghai", run.Output["origin"])
	assert.Equal(t, map[string]any{"summary": "Reviewed"}, run.Output["authorization"])
}

func TestFailedRunErrorMessageIsRedacted(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	buyer := insertRuntimeUser(t, pool)
	endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
			"code":    "UPSTREAM",
			"message": `upstream rejected: {"headers":{"X-Openlinker-Token":["` + r.Header.Get("X-Openlinker-Token") + `"]},"origin":"203.0.113.7"}`,
		}})
	})
	agentID := insertAgent(t, pool, insertCreator(t, pool), endpoint, 0, "approved")
	setAgentEndpointToken(t, pool, agentID, echoEndpointToken)

	run, err := svc.Run(ctx, buyer, makeRunReq(agentID, map[string]any{"q": "fail"}), "")
	require.NoError(t, err)
	require.Equal(t, "failed", run.Status)
	requireNoEcho(t, "failed run", run)
	assert.True(t, run.OutputRedacted)
	assert.Equal(t, "[redacted]", run.ErrorMsg)

	got, err := svc.GetRun(ctx, buyer, uuid.MustParse(run.RunID))
	require.NoError(t, err)
	requireNoEcho(t, "GET failed run", got)
	page, err := svc.ListRunEventsPage(ctx, buyer, uuid.MustParse(run.RunID), 0, 100)
	require.NoError(t, err)
	requireNoEcho(t, "failed run events", page)
}
