package runtime

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestPostRunRejectsAPIKeyWithoutRunScope(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/run", nil), httptest.NewRecorder())
	c.Set(string(httpx.CtxKeyAuthMethod), "apikey")
	c.Set(string(httpx.CtxKeyAuthScopes), []string{"runs:read"})

	err := NewHandler(nil).PostRun(c)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusForbidden, httpErr.Status)
}

func TestWriteSSEHeartbeat(t *testing.T) {
	rec := httptest.NewRecorder()

	require.NoError(t, writeSSEHeartbeat(rec))

	assert.Equal(t, ": heartbeat\n\n", rec.Body.String())
}

func TestWriteSSEEventsStopsOnTerminalEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	events := []RunEventResponse{
		{
			Sequence:  1,
			EventType: "run.started",
			Payload:   map[string]interface{}{"status": "running"},
			CreatedAt: time.Now(),
		},
		{
			Sequence:  2,
			EventType: "run.completed",
			Payload:   map[string]interface{}{"status": "success"},
			CreatedAt: time.Now(),
		},
	}

	terminal, nextSequence, err := writeSSEEvents(rec, events, 0)

	require.NoError(t, err)
	assert.True(t, terminal)
	assert.Equal(t, int32(2), nextSequence)
	assert.Contains(t, rec.Body.String(), "id: 1\nevent: run.started")
	assert.Contains(t, rec.Body.String(), "id: 2\nevent: run.completed")
}
