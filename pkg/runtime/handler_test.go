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
//	POST /api/v1/runs/:id/events -> handler.PostRunEvent   (agent token required)
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

	svc := runtime.NewService(pool, newTestConfig())
	h := runtime.NewHandler(svc)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}

	api := e.Group("/api/v1")
	jwtMW := auth.JWTMiddleware(testHandlerSecret)
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
	Events []struct {
		Sequence  int32  `json:"sequence"`
		EventType string `json:"event_type"`
	} `json:"events"`
}

type runEventRespBody struct {
	Sequence  int32          `json:"sequence"`
	EventType string         `json:"event_type"`
	Payload   map[string]any `json:"payload"`
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
		"Authorization": signJWT(t, userID),
	})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

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
		"Authorization": signJWT(t, userID),
	})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))
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
		"Authorization": signJWT(t, userID),
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
		map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusOK, createRec.Code, "body=%s", string(createRaw))
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
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	tok := signJWT(t, userID)
	body := map[string]any{"agent_id": agentID.String(), "input": map[string]any{}}
	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/run", body,
		map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusOK, createRec.Code, "body=%s", string(createRaw))
	var created runRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))

	rec, raw := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+created.RunID+"/events?after_sequence=1", nil,
		map[string]string{"Authorization": tok})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))

	var got runEventsRespBody
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Len(t, got.Events, 3)
	assert.Equal(t, int32(2), got.Events[0].Sequence)
	assert.Equal(t, "run.started", got.Events[0].EventType)
	assert.Equal(t, "run.status.changed", got.Events[1].EventType)
	assert.Equal(t, "run.completed", got.Events[2].EventType)
}

func TestPostRunEvent_Handler_UsesAgentTokenNoJWT(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	setAgentEndpointToken(t, pool, agentID, "agent-secret")
	runID := insertRunningRun(t, pool, userID, agentID)

	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/runs/"+runID.String()+"/events", map[string]any{
		"event_type": "run.message.delta",
		"payload":    map[string]any{"text": "streaming"},
	}, map[string]string{
		"X-OpenLinker-Token": "agent-secret",
	})
	assert.Equal(t, http.StatusCreated, rec.Code, "body=%s", string(raw))

	var got runEventRespBody
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, int32(1), got.Sequence)
	assert.Equal(t, "run.message.delta", got.EventType)
	assert.Equal(t, "streaming", got.Payload["text"])
}

func TestPostRunEvent_Handler_RejectsWrongAgentToken(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	setAgentEndpointToken(t, pool, agentID, "agent-secret")
	runID := insertRunningRun(t, pool, userID, agentID)

	rec, raw := doRequest(t, e, http.MethodPost, "/api/v1/runs/"+runID.String()+"/events", map[string]any{
		"event_type": "run.message.delta",
		"payload":    map[string]any{"text": "streaming"},
	}, map[string]string{
		"X-OpenLinker-Token": "wrong-secret",
	})
	assert.Equal(t, http.StatusUnauthorized, rec.Code, "body=%s", string(raw))
}

func TestStreamRunEvents_Handler_ReplaysEvents(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	tok := signJWT(t, userID)
	body := map[string]any{"agent_id": agentID.String(), "input": map[string]any{}}
	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/run", body,
		map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusOK, createRec.Code, "body=%s", string(createRaw))
	var created runRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))

	rec, raw := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+created.RunID+"/stream?after_sequence=1", nil,
		map[string]string{
			"Authorization": tok,
			"Accept":        "text/event-stream",
		})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
	assert.NotContains(t, string(raw), "event: run.created")
	assert.Contains(t, string(raw), "id: 2")
	assert.Contains(t, string(raw), "event: run.started")
	assert.Contains(t, string(raw), "event: run.completed")
}

func TestStreamRunEvents_Handler_UsesLastEventID(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")

	tok := signJWT(t, userID)
	body := map[string]any{"agent_id": agentID.String(), "input": map[string]any{}}
	createRec, createRaw := doRequest(t, e, http.MethodPost, "/api/v1/run", body,
		map[string]string{"Authorization": tok})
	require.Equal(t, http.StatusOK, createRec.Code, "body=%s", string(createRaw))
	var created runRespBody
	require.NoError(t, json.Unmarshal(createRaw, &created))

	rec, raw := doRequest(t, e, http.MethodGet, "/api/v1/runs/"+created.RunID+"/stream", nil,
		map[string]string{
			"Authorization": tok,
			"Accept":        "text/event-stream",
			"Last-Event-ID": "2",
		})
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", string(raw))
	assert.NotContains(t, string(raw), "event: run.created")
	assert.NotContains(t, string(raw), "event: run.started")
	assert.Contains(t, string(raw), "event: run.completed")
}

func TestStreamRunEvents_Handler_EmitsEventReportedAfterConnectionStarts(t *testing.T) {
	e, pool, svc := setupHandlerTest(t)
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)
	endpoint := startMockEndpointForService(t, svc, mockEndpointReturning(http.StatusOK, `{"output":{"text":"ok"}}`))
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	setAgentEndpointToken(t, pool, agentID, "agent-secret")
	runID := insertRunningRun(t, pool, userID, agentID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID.String()+"/stream", nil).WithContext(ctx)
	req.Header.Set(echo.HeaderAuthorization, signJWT(t, userID))
	req.Header.Set(echo.HeaderAccept, "text/event-stream")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		e.ServeHTTP(rec, req)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	eventRec, eventRaw := doRequest(t, e, http.MethodPost, "/api/v1/runs/"+runID.String()+"/events", map[string]any{
		"event_type": "run.message.delta",
		"payload":    map[string]any{"text": "live event"},
	}, map[string]string{
		"X-OpenLinker-Token": "agent-secret",
	})
	require.Equal(t, http.StatusCreated, eventRec.Code, "body=%s", string(eventRaw))

	time.Sleep(1500 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not stop after request cancellation")
	}

	raw := rec.Body.String()
	assert.Equal(t, http.StatusOK, rec.Code, "body=%s", raw)
	assert.Contains(t, rec.Header().Get(echo.HeaderContentType), "text/event-stream")
	assert.Contains(t, raw, "event: run.message.delta")
	assert.Contains(t, raw, "live event")
}
