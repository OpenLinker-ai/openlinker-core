// Package runtime_test - HTTP handler 集成测试。
//
// 与 service_test.go 一样需要 TEST_DATABASE_URL，否则 t.Skip()。
//
// 期望 handler 路由（subagent-4a 在写）：
//
//	POST /api/v1/run             -> handler.PostRun       (auth required)
//	POST /api/v1/runs            -> handler.PostRunAsync  (auth required)
//	GET  /api/v1/runs/:id        -> handler.GetRun        (auth required)
//	GET  /api/v1/runs/:id/events -> handler.GetRunEvents  (auth required)
//	GET  /api/v1/runs/:id/stream -> handler.StreamRunEvents (auth required)
//
// 期望 Handler 接口：
//
//	func NewHandler(svc *Service) *Handler
//	func (h *Handler) RegisterProtected(api *echo.Group, runMw, queryMw echo.MiddlewareFunc)
//
// 通过 echo.ServeHTTP + httptest.NewRecorder 驱动，不开 socket。
package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const testHandlerSecret = "test-secret-32-chars-aaaaaaaaaaaa"

// setupHandlerTest 起 echo + 挂 runtime handler。返回 echo / pool / svc，
// svc 用于 startMockEndpointForService 注入测试 https client。
func setupHandlerTest(t *testing.T) (*echo.Echo, *pgxpool.Pool, *runtime.Service) {
	t.Helper()
	pool := setupTestDB(t)

	svc := newTestService(t, pool)
	h := runtime.NewHandler(svc)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}

	api := e.Group("/api/v1")
	jwtMW := auth.JWTMiddlewareWithUserStatus(testHandlerSecret, auth.NewDBUserStatusChecker(pool))
	h.RegisterProtected(api, jwtMW, jwtMW)

	return e, pool, svc
}

func signJWT(t *testing.T, userID uuid.UUID) string {
	t.Helper()
	tok, err := auth.GenerateToken(userID.String(), testHandlerSecret, 1*time.Hour)
	require.NoError(t, err)
	return "Bearer " + tok
}

// doRequest 不开 socket 直接驱动 echo。
func doRequest(t *testing.T, e *echo.Echo, method, target string, body any, headers map[string]string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req := httptest.NewRequest(method, target, &buf)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	raw, _ := io.ReadAll(rec.Body)
	return rec, raw
}

// doRequestRaw 把 body 当字符串直接发，用于测无效 JSON 等情况。
func doRequestRaw(t *testing.T, e *echo.Echo, method, target, raw string, headers map[string]string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewBufferString(raw))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	return rec, body
}

// runRespBody 解 RunResponse 的关心字段（与 internal/runtime/dto.go 对齐）。
type runRespBody struct {
	RunID     string `json:"run_id"`
	Status    string `json:"status"`
	CostCents int32  `json:"cost_cents"`
	ErrorCode string `json:"error_code,omitempty"`
}

type runEventsRespBody struct {
	Items []struct {
		Sequence  int32  `json:"sequence"`
		EventType string `json:"event_type"`
	} `json:"items"`
	Meta runtime.RunEventPageMeta `json:"meta"`
}

type runEventRespBody struct {
	Sequence  int32          `json:"sequence"`
	EventType string         `json:"event_type"`
	Payload   map[string]any `json:"payload"`
}

func insertTerminalRunWithEvents(t *testing.T, pool *pgxpool.Pool, userID, agentID uuid.UUID) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	runID := uuid.New()
	terminalEventID := uuid.New()
	_, err = tx.Exec(ctx, `
		INSERT INTO runs (
			id, user_id, agent_id, input, status,
			cost_cents, platform_fee_cents, creator_revenue_cents,
			runtime_contract_id, idempotency_key_hash, idempotency_fingerprint,
			connection_mode_snapshot, dispatch_state,
			dispatch_deadline_at, run_deadline_at
		) VALUES (
			$1, $2, $3, '{}'::jsonb, 'running',
			10, 2, 8,
			'openlinker.runtime.v2',
			digest($1::uuid::text || ':key', 'sha256'),
			digest($1::uuid::text || ':fingerprint', 'sha256'),
			'runtime', 'pending',
			clock_timestamp() + interval '10 minutes',
			clock_timestamp() + interval '1 hour'
		)`, runID, userID, agentID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
		INSERT INTO run_events (id, run_id, sequence, event_type, payload)
		VALUES
			($2, $1, 1, 'run.created', '{"status":"running"}'::jsonb),
			(gen_random_uuid(), $1, 2, 'run.started', '{"status":"running"}'::jsonb),
			(gen_random_uuid(), $1, 3, 'run.status.changed', '{"status":"running"}'::jsonb),
			($3, $1, 4, 'run.failed', '{"status":"timeout","terminal":true}'::jsonb)`,
		runID, uuid.New(), terminalEventID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
		UPDATE runs
		SET status = 'timeout',
			dispatch_state = 'terminal',
			error_code = 'RUNTIME_TIMEOUT',
			error_message = 'run deadline reached',
			finished_at = clock_timestamp(),
			terminal_event_id = $2
		WHERE id = $1`, runID, terminalEventID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
		INSERT INTO run_accounting_ledger (
			run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
		) VALUES ($1, $2, $3, 0, 0)`, runID, terminalEventID, agentID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	return runID
}

// ────────────────────────────────────────────────────────────
// POST /api/v1/run
// ────────────────────────────────────────────────────────────

func TestPostRun_HappyPath(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	body := map[string]any{
		"agent_id": agentID.String(),
		"input":    map[string]any{"q": "hi"},
	}
	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/run", body, map[string]string{
		"Authorization":   signJWT(t, userID),
		"Idempotency-Key": "handler-happy-path",
	})
	assert.Equal(t, http.StatusCreated, rec.Code, "body=%s", string(raw))

	var out runRespBody
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, "success", out.Status)
	assert.NotEmpty(t, out.RunID)
	assert.Equal(t, int32(0), out.CostCents)

	assertRunAccountingConsistent(t, pool)
}

func TestPostRun_NoAuth(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	body := map[string]any{"agent_id": agentID.String(), "input": map[string]any{}}
	rec, _ := doRequest(t, e, http.MethodPost, "/api/v1/run", body, nil)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestPostRun_InvalidJSON(t *testing.T) {
	e, pool, _ := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)

	rec, raw := doRequestRaw(t, e, http.MethodPost, "/api/v1/run", `{not-json}`, map[string]string{
		"Authorization": signJWT(t, userID),
	})
	// 400 / 422 都接受
	assert.Contains(t, []int{http.StatusBadRequest, http.StatusUnprocessableEntity}, rec.Code,
		"body=%s", string(raw))
}

func TestPostRun_MissingAgentID(t *testing.T) {
	e, pool, _ := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)

	body := map[string]any{
		// 缺 agent_id
		"input": map[string]any{"q": "hi"},
	}
	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/run", body, map[string]string{
		"Authorization": signJWT(t, userID),
	})
	// 422 (validator) 或 400 (parse) 都接受
	assert.Contains(t, []int{http.StatusUnprocessableEntity, http.StatusBadRequest}, rec.Code,
		"body=%s", string(raw))
}

func TestPostRun_FreePhaseDoesNotRequireBalance(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool) // $0.01
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	body := map[string]any{"agent_id": agentID.String(), "input": map[string]any{}}
	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/run", body, map[string]string{
		"Authorization":   signJWT(t, userID),
		"Idempotency-Key": "handler-free-phase",
	})
	assert.Equal(t, http.StatusCreated, rec.Code, "body=%s", string(raw))
}

func TestPostRunsAsync_Handler_ReturnsAccepted(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)

	called := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseEndpoint := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseEndpoint()

	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
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
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"output":{"text":"async ok"}}`))
	})
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	body := map[string]any{
		"agent_id": agentID.String(),
		"input":    map[string]any{"q": "hi"},
	}
	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/runs", body, map[string]string{
		"Authorization":   signJWT(t, userID),
		"Idempotency-Key": "handler-async",
		"Prefer":          "wait=0",
	})
	assert.Equal(t, http.StatusAccepted, rec.Code, "body=%s", string(raw))

	var out runRespBody
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, "running", out.Status)
	require.NotEmpty(t, out.RunID)

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("async endpoint was not called")
	}
	releaseEndpoint()

	require.Eventually(t, func() bool {
		getRec, getRaw := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+out.RunID, nil,
			map[string]string{"Authorization": signJWT(t, userID)})
		if getRec.Code != http.StatusOK {
			t.Logf("GET run failed: status=%d body=%s", getRec.Code, string(getRaw))
			return false
		}
		var got runRespBody
		if err := json.Unmarshal(getRaw, &got); err != nil {
			return false
		}
		return got.Status == "success"
	}, 3*time.Second, 20*time.Millisecond)

	assertRunAccountingConsistent(t, pool)
}

// ────────────────────────────────────────────────────────────
// GET /api/v1/runs/:id
// ────────────────────────────────────────────────────────────

func TestGetRun_Handler_HappyPath(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	tok := signJWT(t, userID)
	body := map[string]any{"agent_id": agentID.String(), "input": map[string]any{}}
	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/run", body,
		map[string]string{"Authorization": tok, "Idempotency-Key": "handler-events-page"})
	require.Equal(t, http.StatusCreated, createRec.Code, "body=%s", string(createRaw))
	var created runRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))

	rec, raw := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+created.RunID, nil,
		map[string]string{"Authorization": tok})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))
	var got runRespBody
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, created.RunID, got.RunID)
	assert.Equal(t, created.Status, got.Status)
}

func TestGetRun_Handler_NotFound(t *testing.T) {
	e, pool, _ := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	tok := signJWT(t, userID)

	rec, _ := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+uuid.New().String(), nil,
		map[string]string{"Authorization": tok})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetRunEvents_Handler_HappyPath(t *testing.T) {
	e, pool, _ := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/agent", 10, "approved")
	runID := insertTerminalRunWithEvents(t, pool, userID, agentID)

	tok := signJWT(t, userID)
	rec, raw := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+runID.String()+"/events?after_sequence=1", nil,
		map[string]string{"Authorization": tok})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

	var got runEventsRespBody
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Len(t, got.Items, 3)
	assert.Equal(t, int32(2), got.Items[0].Sequence)
	assert.Equal(t, "run.started", got.Items[0].EventType)
	assert.Equal(t, "run.status.changed", got.Items[1].EventType)
	assert.Equal(t, "run.failed", got.Items[2].EventType)
	assert.Equal(t, int32(1), got.Meta.RequestedAfterSequence)
	assert.Equal(t, int32(1), got.Meta.EffectiveAfterSequence)
	assert.False(t, got.Meta.RetentionGap)
	assert.True(t, got.Meta.Terminal)
	assert.True(t, got.Meta.StreamComplete)

	_, err := pool.Exec(context.Background(), `
		INSERT INTO run_event_retention_watermarks (run_id, retained_through_sequence)
		VALUES ($1, 2)`, runID)
	require.NoError(t, err)

	rec, raw = doRequest(t, e, http.MethodGet, "/api/v1/runs/"+runID.String()+"/events?after_sequence=0", nil,
		map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Len(t, got.Items, 2)
	assert.Equal(t, int32(3), got.Items[0].Sequence)
	assert.Equal(t, int32(2), got.Meta.RetainedThroughSequence)
	assert.Equal(t, int32(2), got.Meta.EffectiveAfterSequence)
	assert.True(t, got.Meta.RetentionGap)
	require.NotNil(t, got.Meta.EarliestAvailableSequence)
	assert.Equal(t, int32(3), *got.Meta.EarliestAvailableSequence)
	require.NotNil(t, got.Meta.LatestAvailableSequence)
	assert.Equal(t, int32(4), *got.Meta.LatestAvailableSequence)

	_, err = pool.Exec(context.Background(), `
		UPDATE run_event_retention_watermarks
		SET retained_through_sequence = 4
		WHERE run_id = $1`, runID)
	require.NoError(t, err)
	rec, raw = doRequest(t, e, http.MethodGet, "/api/v1/runs/"+runID.String()+"/stream", nil,
		map[string]string{
			"Authorization": tok,
			"Accept":        "text/event-stream",
		})
	require.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))
	assert.Contains(t, string(raw), "event: run.stream.gap")
	assert.NotContains(t, string(raw), "id:")
}

func TestStreamRunEvents_Handler_ReplaysEvents(t *testing.T) {
	e, pool, _ := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/agent", 10, "approved")
	runID := insertTerminalRunWithEvents(t, pool, userID, agentID)

	tok := signJWT(t, userID)
	rec, raw := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+runID.String()+"/stream?after_sequence=1", nil,
		map[string]string{
			"Authorization": tok,
			"Accept":        "text/event-stream",
		})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
	assert.NotContains(t, string(raw), "event: run.created")
	assert.Contains(t, string(raw), "id: 2")
	assert.Contains(t, string(raw), "event: run.started")
	assert.Contains(t, string(raw), "event: run.failed")
}

func TestStreamRunEvents_Handler_UsesLastEventID(t *testing.T) {
	e, pool, _ := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/agent", 10, "approved")
	runID := insertTerminalRunWithEvents(t, pool, userID, agentID)

	tok := signJWT(t, userID)
	rec, raw := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+runID.String()+"/stream", nil,
		map[string]string{
			"Authorization": tok,
			"Accept":        "text/event-stream",
			"Last-Event-ID": "2",
		})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))
	assert.NotContains(t, string(raw), "event: run.created")
	assert.NotContains(t, string(raw), "event: run.started")
	assert.Contains(t, string(raw), "event: run.failed")
}
