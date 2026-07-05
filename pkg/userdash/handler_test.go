package userdash

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestUserDashHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	run := RunListItem{
		ID:        uuid.NewString(),
		AgentID:   agentID.String(),
		AgentSlug: "research-agent",
		AgentName: "Research Agent",
		Status:    "success",
		CostCents: 25,
		StartedAt: time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
		Source:    "api",
	}

	t.Run("list user runs", func(t *testing.T) {
		mock := &mockUserDashService{listUserRunsResp: &RunListResponse{
			Items: []RunListItem{run},
			Total: 7,
			Page:  2,
			Size:  3,
		}}
		c, rec := newDashRecorderContext(http.MethodGet, "/api/v1/runs?page=2&size=3", "", userID.String(), nil)

		if err := NewHandler(mock).ListRuns(c); err != nil {
			t.Fatalf("ListRuns error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.listUserID != userID || mock.listPage != 2 || mock.listSize != 3 {
			t.Fatalf("captured list = user %s page %d size %d", mock.listUserID, mock.listPage, mock.listSize)
		}
		var body RunListResponse
		decodeUserDashJSON(t, rec, &body)
		if body.Total != 7 || len(body.Items) != 1 || body.Items[0].ID != run.ID {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("list creator agent runs", func(t *testing.T) {
		mock := &mockUserDashService{creatorRunsResp: &RunListResponse{
			Items: []RunListItem{run},
			Total: 1,
			Page:  1,
			Size:  20,
		}}
		c, rec := newDashRecorderContext(
			http.MethodGet,
			"/api/v1/creator/agents/"+agentID.String()+"/runs?size=bad",
			"",
			userID.String(),
			map[string]string{"id": agentID.String()},
		)

		if err := NewHandler(mock).ListCreatorAgentRuns(c); err != nil {
			t.Fatalf("ListCreatorAgentRuns error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.creatorUserID != userID || mock.creatorAgentID != agentID || mock.creatorPage != defaultPage || mock.creatorSize != defaultSize {
			t.Fatalf("captured creator runs = user %s agent %s page %d size %d", mock.creatorUserID, mock.creatorAgentID, mock.creatorPage, mock.creatorSize)
		}
	})

	t.Run("user dashboard", func(t *testing.T) {
		mock := &mockUserDashService{userDashboardResp: &UserDashboardResponse{
			IsCreator:  true,
			Usage:      UsageStats{ThisMonthCalls: 2, ThisMonthSpent: 50, TotalCalls: 9},
			RecentRuns: []RunListItem{run},
		}}
		c, rec := newDashRecorderContext(http.MethodGet, "/api/v1/dashboard", "", userID.String(), nil)

		if err := NewHandler(mock).GetDashboard(c); err != nil {
			t.Fatalf("GetDashboard error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.userDashboardID != userID {
			t.Fatalf("captured dashboard user = %s", mock.userDashboardID)
		}
		var body UserDashboardResponse
		decodeUserDashJSON(t, rec, &body)
		if !body.IsCreator || body.Usage.TotalCalls != 9 || len(body.RecentRuns) != 1 {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("creator dashboard", func(t *testing.T) {
		mock := &mockUserDashService{creatorDashboardResp: &CreatorDashboardResponse{
			Summary: CreatorSummary{ThisMonthCalls: 3, ThisMonthRevenue: 120, TotalAgents: 4, PendingAgents: 1},
			Agents:  []AgentStatsItem{{ID: agentID.String(), Slug: "research-agent", Name: "Research Agent"}},
		}}
		c, rec := newDashRecorderContext(http.MethodGet, "/api/v1/creator/dashboard", "", userID.String(), nil)

		if err := NewHandler(mock).GetCreatorDashboard(c); err != nil {
			t.Fatalf("GetCreatorDashboard error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.creatorDashboardID != userID {
			t.Fatalf("captured creator dashboard user = %s", mock.creatorDashboardID)
		}
		var body CreatorDashboardResponse
		decodeUserDashJSON(t, rec, &body)
		if body.Summary.TotalAgents != 4 || len(body.Agents) != 1 {
			t.Fatalf("body = %#v", body)
		}
	})
}

func TestUserDashHandlerValidationAndServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()

	t.Run("missing user", func(t *testing.T) {
		c, _ := newDashRecorderContext(http.MethodGet, "/api/v1/runs", "", "", nil)
		requireUserDashHTTPStatus(t, NewHandler(nil).ListRuns(c), http.StatusUnauthorized)
	})

	t.Run("invalid user", func(t *testing.T) {
		c, _ := newDashRecorderContext(http.MethodGet, "/api/v1/dashboard", "", "bad-user", nil)
		requireUserDashHTTPStatus(t, NewHandler(nil).GetDashboard(c), http.StatusUnauthorized)
	})

	t.Run("invalid agent id", func(t *testing.T) {
		c, _ := newDashRecorderContext(
			http.MethodGet,
			"/api/v1/creator/agents/bad/runs",
			"",
			userID.String(),
			map[string]string{"id": "bad"},
		)
		requireUserDashHTTPStatus(t, NewHandler(nil).ListCreatorAgentRuns(c), http.StatusBadRequest)
	})

	t.Run("list runs service error", func(t *testing.T) {
		mock := &mockUserDashService{listUserRunsErr: httpx.Internal("runs failed")}
		c, _ := newDashRecorderContext(http.MethodGet, "/api/v1/runs", "", userID.String(), nil)
		requireUserDashHTTPStatus(t, NewHandler(mock).ListRuns(c), http.StatusInternalServerError)
	})

	t.Run("creator agent runs service error", func(t *testing.T) {
		mock := &mockUserDashService{creatorRunsErr: httpx.NotFound("missing")}
		c, _ := newDashRecorderContext(
			http.MethodGet,
			"/api/v1/creator/agents/"+agentID.String()+"/runs",
			"",
			userID.String(),
			map[string]string{"id": agentID.String()},
		)
		requireUserDashHTTPStatus(t, NewHandler(mock).ListCreatorAgentRuns(c), http.StatusNotFound)
	})

	t.Run("dashboard service error", func(t *testing.T) {
		mock := &mockUserDashService{userDashboardErr: httpx.NotFound("missing")}
		c, _ := newDashRecorderContext(http.MethodGet, "/api/v1/dashboard", "", userID.String(), nil)
		requireUserDashHTTPStatus(t, NewHandler(mock).GetDashboard(c), http.StatusNotFound)
	})

	t.Run("creator dashboard service error", func(t *testing.T) {
		mock := &mockUserDashService{creatorDashboardErr: httpx.Forbidden("creator only")}
		c, _ := newDashRecorderContext(http.MethodGet, "/api/v1/creator/dashboard", "", userID.String(), nil)
		requireUserDashHTTPStatus(t, NewHandler(mock).GetCreatorDashboard(c), http.StatusForbidden)
	})
}

func TestUserDashRegisterAddsAPIRoutes(t *testing.T) {
	e := echo.New()
	api := e.Group("/api/v1")
	NewHandler(nil).RegisterCoreAPI(api, func(next echo.HandlerFunc) echo.HandlerFunc { return next })

	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"GET /api/v1/runs",
		"GET /api/v1/dashboard",
		"GET /api/v1/creator/dashboard",
		"GET /api/v1/creator/agents/:id/runs",
	} {
		if !routes[route] {
			t.Fatalf("missing route %s", route)
		}
	}
}

type mockUserDashService struct {
	listUserID       uuid.UUID
	listPage         int32
	listSize         int32
	listUserRunsResp *RunListResponse
	listUserRunsErr  error

	creatorUserID   uuid.UUID
	creatorAgentID  uuid.UUID
	creatorPage     int32
	creatorSize     int32
	creatorRunsResp *RunListResponse
	creatorRunsErr  error

	userDashboardID   uuid.UUID
	userDashboardResp *UserDashboardResponse
	userDashboardErr  error

	creatorDashboardID   uuid.UUID
	creatorDashboardResp *CreatorDashboardResponse
	creatorDashboardErr  error
}

func (m *mockUserDashService) ListUserRuns(_ context.Context, userID uuid.UUID, page, size int32) (*RunListResponse, error) {
	m.listUserID = userID
	m.listPage = page
	m.listSize = size
	return m.listUserRunsResp, m.listUserRunsErr
}

func (m *mockUserDashService) ListCreatorAgentRuns(_ context.Context, userID, agentID uuid.UUID, page, size int32) (*RunListResponse, error) {
	m.creatorUserID = userID
	m.creatorAgentID = agentID
	m.creatorPage = page
	m.creatorSize = size
	return m.creatorRunsResp, m.creatorRunsErr
}

func (m *mockUserDashService) GetUserDashboard(_ context.Context, userID uuid.UUID) (*UserDashboardResponse, error) {
	m.userDashboardID = userID
	return m.userDashboardResp, m.userDashboardErr
}

func (m *mockUserDashService) GetCreatorDashboard(_ context.Context, userID uuid.UUID) (*CreatorDashboardResponse, error) {
	m.creatorDashboardID = userID
	return m.creatorDashboardResp, m.creatorDashboardErr
}

func newDashRecorderContext(method, target, body, userID string, params map[string]string) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if userID != "" {
		c.Set(string(httpx.CtxKeyUserID), userID)
	}
	if len(params) > 0 {
		names := make([]string, 0, len(params))
		values := make([]string, 0, len(params))
		for name, value := range params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c, rec
}

func decodeUserDashJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}

func requireUserDashHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if he.Status != want {
		t.Fatalf("HTTP status = %d (%s), want %d", he.Status, he.Message, want)
	}
}
