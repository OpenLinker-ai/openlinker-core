package runtime_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
)

func setupAgentRuntimeHandlerTest(t *testing.T) (*echo.Echo, *pgxpool.Pool) {
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
	h.RegisterAgentRuntime(e.Group("/api/v1"))
	return e, pool
}

func insertRuntimePullAgentWithToken(t *testing.T, pool *pgxpool.Pool) (uuid.UUID, string) {
	t.Helper()
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/not-used", 10, "approved")
	setRuntimePullMode(t, pool, agentID)
	token := insertRuntimeToken(t, pool, agentID, creatorID, []string{"agent:call", "agent:pull"})
	return agentID, token
}

func runtimeAuthHeader(token string) map[string]string {
	return map[string]string{echo.HeaderAuthorization: "Bearer " + token}
}

func TestAgentRuntimeRateLimiter_HeartbeatRejectsHighFrequency(t *testing.T) {
	e, pool := setupAgentRuntimeHandlerTest(t)
	_, token := insertRuntimePullAgentWithToken(t, pool)

	first, _ := doRequest(t, e, http.MethodPost, "/api/v1/agent-runtime/heartbeat", nil, runtimeAuthHeader(token))
	require.Equal(t, http.StatusOK, first.Code)

	second, body := doRequest(t, e, http.MethodPost, "/api/v1/agent-runtime/heartbeat", nil, runtimeAuthHeader(token))
	assert.Equal(t, http.StatusTooManyRequests, second.Code)
	assert.Equal(t, "10", second.Header().Get(echo.HeaderRetryAfter))
	assert.Contains(t, string(body), "RATE_LIMITED")
}

func TestAgentRuntimeRateLimiter_EmptyClaimStartsCooldown(t *testing.T) {
	e, pool := setupAgentRuntimeHandlerTest(t)
	_, token := insertRuntimePullAgentWithToken(t, pool)

	first, _ := doRequest(t, e, http.MethodGet, "/api/v1/agent-runtime/runs/claim", nil, runtimeAuthHeader(token))
	require.Equal(t, http.StatusNoContent, first.Code)
	assert.Equal(t, "30", first.Header().Get(echo.HeaderRetryAfter))

	second, body := doRequest(t, e, http.MethodGet, "/api/v1/agent-runtime/runs/claim", nil, runtimeAuthHeader(token))
	assert.Equal(t, http.StatusTooManyRequests, second.Code)
	assert.Equal(t, "30", second.Header().Get(echo.HeaderRetryAfter))
	assert.Contains(t, string(body), "RATE_LIMITED")
}

func TestAgentRuntimeRateLimiter_RejectsConcurrentLongPollClaim(t *testing.T) {
	e, pool := setupAgentRuntimeHandlerTest(t)
	_, token := insertRuntimePullAgentWithToken(t, pool)

	done := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/agent-runtime/runs/claim?wait=2", nil)
		req.Header.Set(echo.HeaderAuthorization, "Bearer "+token)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		done <- rec.Code
	}()
	time.Sleep(100 * time.Millisecond)

	second, body := doRequest(t, e, http.MethodGet, "/api/v1/agent-runtime/runs/claim?wait=2", nil, runtimeAuthHeader(token))
	assert.Equal(t, http.StatusTooManyRequests, second.Code)
	assert.Equal(t, "5", second.Header().Get(echo.HeaderRetryAfter))
	assert.Contains(t, string(body), "RATE_LIMITED")

	select {
	case code := <-done:
		assert.Equal(t, http.StatusNoContent, code)
	case <-time.After(3 * time.Second):
		t.Fatal("first long-poll claim did not finish")
	}
}
