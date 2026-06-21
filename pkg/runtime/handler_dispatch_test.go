package runtime

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

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestRuntimeHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	artifactID := uuid.New()
	messageID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	runResp := &RunResponse{
		RunID:     runID.String(),
		Status:    "success",
		Output:    map[string]interface{}{"text": "ok"},
		CostCents: 0,
		Source:    "api",
	}
	eventResp := RunEventResponse{
		EventID:   uuid.NewString(),
		RunID:     runID.String(),
		Sequence:  3,
		EventType: "run.completed",
		Payload:   map[string]interface{}{"status": "success"},
		CreatedAt: now,
	}
	artifactResp := RunArtifactResponse{
		ID:           artifactID.String(),
		RunID:        runID.String(),
		ArtifactType: "file",
		Title:        "Orders",
		Content:      map[string]interface{}{"rows": float64(2)},
		Visibility:   "owner",
		CreatedAt:    now,
	}
	messageResp := RunMessageResponse{
		ID:        messageID.String(),
		RunID:     runID.String(),
		Role:      "assistant",
		Content:   "done",
		Payload:   map[string]interface{}{"text": "done"},
		CreatedAt: now,
	}
	heartbeatResp := &AgentHeartbeatResponse{
		AgentID:                          agentID.String(),
		AvailabilityStatus:               "healthy",
		LastCheckedAt:                    &now,
		PendingRunCount:                  1,
		ClaimNow:                         true,
		RecommendedHeartbeatAfterSeconds: 60,
		MaxClaimWaitSeconds:              30,
	}
	claimResp := &RuntimePullRunResponse{
		RunID:          runID.String(),
		AgentID:        agentID.String(),
		Input:          map[string]interface{}{"q": "hi"},
		Source:         "api",
		ResultEndpoint: "/api/v1/agent-runtime/runs/" + runID.String() + "/result",
		ResultMethod:   http.MethodPost,
		ResultRequired: true,
		A2A: &AgentA2AContext{
			CurrentRunID:      runID.String(),
			CallAgentEndpoint: "/api/v1/a2a",
			CallAgentMethod:   http.MethodPost,
			RuntimeTokenType:  "agent_runtime",
			RuntimeScopes:     []string{"agent:pull"},
		},
	}

	t.Run("run lifecycle reads and events", func(t *testing.T) {
		mock := &mockRuntimeService{
			runResp:       runResp,
			startRunResp:  &RunResponse{RunID: runID.String(), Status: "running", Source: "api"},
			getRunResp:    runResp,
			runEventsResp: []RunEventResponse{eventResp},
			artifactsResp: []RunArtifactResponse{artifactResp},
			messagesResp:  []RunMessageResponse{messageResp},
			reportResp:    &eventResp,
		}
		h := NewHandler(mock)

		postCtx, postRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodPost,
			target:     "/api/v1/run",
			userID:     userID.String(),
			authMethod: "apikey",
			scopes:     []string{"agents:run"},
			body:       `{"agent_id":"` + agentID.String() + `","input":{"q":"hi"}}`,
		})
		if err := h.PostRun(postCtx); err != nil {
			t.Fatalf("PostRun error = %v", err)
		}
		if postRec.Code != http.StatusOK || mock.runUserID != userID || mock.runReq.AgentID != agentID.String() || mock.runSource != "api" {
			t.Fatalf("post run code=%d user=%s source=%q req=%#v", postRec.Code, mock.runUserID, mock.runSource, mock.runReq)
		}

		asyncCtx, asyncRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodPost,
			target:     "/api/v1/runs",
			userID:     userID.String(),
			authMethod: "apikey",
			scopes:     []string{"agents:run"},
			body:       `{"agent_id":"` + agentID.String() + `","input":{"q":"async"}}`,
		})
		if err := h.PostRunAsync(asyncCtx); err != nil {
			t.Fatalf("PostRunAsync error = %v", err)
		}
		if asyncRec.Code != http.StatusAccepted || mock.startRunUserID != userID || mock.startRunSource != "api" {
			t.Fatalf("post async code=%d user=%s source=%q", asyncRec.Code, mock.startRunUserID, mock.startRunSource)
		}

		getCtx, getRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String(),
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.GetRun(getCtx); err != nil {
			t.Fatalf("GetRun error = %v", err)
		}
		if getRec.Code != http.StatusOK || mock.getRunID != runID || mock.getRunUserID != userID {
			t.Fatalf("get run code=%d run=%s user=%s", getRec.Code, mock.getRunID, mock.getRunUserID)
		}

		eventsCtx, eventsRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/events?after_sequence=2&limit=7",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.GetRunEvents(eventsCtx); err != nil {
			t.Fatalf("GetRunEvents error = %v", err)
		}
		if eventsRec.Code != http.StatusOK || mock.eventsRunID != runID || mock.eventsAfter != 2 || mock.eventsLimit != 7 {
			t.Fatalf("events code=%d run=%s after=%d limit=%d", eventsRec.Code, mock.eventsRunID, mock.eventsAfter, mock.eventsLimit)
		}
		var eventsBody struct {
			Events []RunEventResponse `json:"events"`
		}
		decodeRuntimeDispatchJSON(t, eventsRec, &eventsBody)
		if len(eventsBody.Events) != 1 || eventsBody.Events[0].EventType != "run.completed" {
			t.Fatalf("events body = %#v", eventsBody)
		}

		artifactsCtx, artifactsRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/artifacts",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.GetRunArtifacts(artifactsCtx); err != nil {
			t.Fatalf("GetRunArtifacts error = %v", err)
		}
		if artifactsRec.Code != http.StatusOK || mock.artifactsRunID != runID || mock.artifactsUserID != userID {
			t.Fatalf("artifacts code=%d run=%s user=%s", artifactsRec.Code, mock.artifactsRunID, mock.artifactsUserID)
		}

		messagesCtx, messagesRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/messages",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.GetRunMessages(messagesCtx); err != nil {
			t.Fatalf("GetRunMessages error = %v", err)
		}
		if messagesRec.Code != http.StatusOK || mock.messagesRunID != runID || mock.messagesUserID != userID {
			t.Fatalf("messages code=%d run=%s user=%s", messagesRec.Code, mock.messagesRunID, mock.messagesUserID)
		}

		streamCtx, streamRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/stream",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.StreamRunEvents(streamCtx); err != nil {
			t.Fatalf("StreamRunEvents error = %v", err)
		}
		if streamRec.Code != http.StatusOK || !strings.Contains(streamRec.Body.String(), "event: run.completed") || mock.eventsLimit != defaultRunEventsLimit {
			t.Fatalf("stream code=%d limit=%d body=%s", streamRec.Code, mock.eventsLimit, streamRec.Body.String())
		}

		reportCtx, reportRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:  http.MethodPost,
			target:  "/api/v1/runs/" + runID.String() + "/events",
			body:    `{"event_type":"run.message.delta","payload":{"text":"hi"}}`,
			params:  map[string]string{"id": runID.String()},
			headers: map[string]string{"X-OpenLinker-Token": "agent-secret"},
		})
		if err := h.PostRunEvent(reportCtx); err != nil {
			t.Fatalf("PostRunEvent error = %v", err)
		}
		if reportRec.Code != http.StatusCreated || mock.reportRunID != runID || mock.reportToken != "agent-secret" || mock.reportReq.EventType != "run.message.delta" {
			t.Fatalf("report code=%d run=%s token=%q req=%#v", reportRec.Code, mock.reportRunID, mock.reportToken, mock.reportReq)
		}
	})

	t.Run("runtime pull endpoints", func(t *testing.T) {
		mock := &mockRuntimeService{
			heartbeatResp: heartbeatResp,
			claimResp:     claimResp,
			completeResp:  &RunResponse{RunID: runID.String(), Status: "success"},
		}
		h := NewHandler(mock)

		heartbeatCtx, heartbeatRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:  http.MethodPost,
			target:  "/api/v1/agent-runtime/heartbeat",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_secret"},
		})
		if err := h.PostAgentHeartbeat(heartbeatCtx); err != nil {
			t.Fatalf("PostAgentHeartbeat error = %v", err)
		}
		if heartbeatRec.Code != http.StatusOK || mock.heartbeatToken != "rt_live_secret" {
			t.Fatalf("heartbeat code=%d token=%q", heartbeatRec.Code, mock.heartbeatToken)
		}

		claimCtx, claimRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:  http.MethodGet,
			target:  "/api/v1/agent-runtime/runs/claim?wait=90",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_secret"},
		})
		if err := h.ClaimRuntimePullRun(claimCtx); err != nil {
			t.Fatalf("ClaimRuntimePullRun error = %v", err)
		}
		if claimRec.Code != http.StatusOK || mock.claimToken != "rt_live_secret" || mock.claimWait != runtimePullMaxLongPollWait {
			t.Fatalf("claim code=%d token=%q wait=%s", claimRec.Code, mock.claimToken, mock.claimWait)
		}

		mock.claimResp = nil
		emptyCtx, emptyRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:  http.MethodGet,
			target:  "/api/v1/agent-runtime/runs/claim",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_empty"},
		})
		if err := h.ClaimRuntimePullRun(emptyCtx); err != nil {
			t.Fatalf("ClaimRuntimePullRun empty error = %v", err)
		}
		if emptyRec.Code != http.StatusNoContent || emptyRec.Header().Get(echo.HeaderRetryAfter) == "" {
			t.Fatalf("empty claim code=%d retry-after=%q", emptyRec.Code, emptyRec.Header().Get(echo.HeaderRetryAfter))
		}

		completeCtx, completeRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:  http.MethodPost,
			target:  "/api/v1/agent-runtime/runs/" + runID.String() + "/result",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_secret"},
			body:    `{"status":"success","output":{"answer":"done"}}`,
			params:  map[string]string{"id": runID.String()},
		})
		if err := h.PostRuntimePullResult(completeCtx); err != nil {
			t.Fatalf("PostRuntimePullResult error = %v", err)
		}
		if completeRec.Code != http.StatusOK || mock.completeToken != "rt_live_secret" || mock.completeRunID != runID || mock.completeReq.Status != "success" {
			t.Fatalf("complete code=%d token=%q run=%s req=%#v", completeRec.Code, mock.completeToken, mock.completeRunID, mock.completeReq)
		}

		wsCtx, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:  http.MethodGet,
			target:  "/api/v1/agent-runtime/ws",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_ws"},
		})
		if err := h.RuntimeWebSocket(wsCtx); err != nil {
			t.Fatalf("RuntimeWebSocket error = %v", err)
		}
		if mock.wsToken != "rt_live_ws" {
			t.Fatalf("websocket token=%q", mock.wsToken)
		}
	})
}

func TestRuntimeHandlerPropagatesServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	serviceErr := httpx.Conflict("service failed")

	tests := []struct {
		name string
		call func(*Handler, echo.Context) error
		ctx  echo.Context
	}{
		{
			name: "post run",
			call: (*Handler).PostRun,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodPost,
				target:     "/api/v1/run",
				userID:     userID.String(),
				authMethod: "jwt",
				body:       `{"agent_id":"` + agentID.String() + `","input":{"q":"hi"}}`,
			}),
		},
		{
			name: "post run async",
			call: (*Handler).PostRunAsync,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodPost,
				target:     "/api/v1/runs",
				userID:     userID.String(),
				authMethod: "jwt",
				body:       `{"agent_id":"` + agentID.String() + `","input":{"q":"hi"}}`,
			}),
		},
		{
			name: "get run",
			call: (*Handler).GetRun,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String(),
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "get events",
			call: (*Handler).GetRunEvents,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/events",
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "get artifacts",
			call: (*Handler).GetRunArtifacts,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/artifacts",
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "get messages",
			call: (*Handler).GetRunMessages,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/messages",
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "stream events",
			call: (*Handler).StreamRunEvents,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/stream",
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "report event",
			call: (*Handler).PostRunEvent,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:  http.MethodPost,
				target:  "/api/v1/runs/" + runID.String() + "/events",
				body:    `{"event_type":"run.message.delta"}`,
				params:  map[string]string{"id": runID.String()},
				headers: map[string]string{"X-OpenLinker-Token": "agent-secret"},
			}),
		},
		{
			name: "heartbeat",
			call: (*Handler).PostAgentHeartbeat,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:  http.MethodPost,
				target:  "/api/v1/agent-runtime/heartbeat",
				headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_secret"},
			}),
		},
		{
			name: "claim",
			call: (*Handler).ClaimRuntimePullRun,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:  http.MethodGet,
				target:  "/api/v1/agent-runtime/runs/claim",
				headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_secret"},
			}),
		},
		{
			name: "complete",
			call: (*Handler).PostRuntimePullResult,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:  http.MethodPost,
				target:  "/api/v1/agent-runtime/runs/" + runID.String() + "/result",
				headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_secret"},
				body:    `{"status":"success","output":{"answer":"done"}}`,
				params:  map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "websocket",
			call: (*Handler).RuntimeWebSocket,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:  http.MethodGet,
				target:  "/api/v1/agent-runtime/ws",
				headers: map[string]string{echo.HeaderAuthorization: "Bearer rt_live_secret"},
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireRuntimeDispatchHTTPStatus(t, tt.call(NewHandler(&mockRuntimeService{err: serviceErr}), tt.ctx), http.StatusConflict)
		})
	}
}

type mockRuntimeService struct {
	err error

	runUserID uuid.UUID
	runReq    *RunRequest
	runSource string
	runResp   *RunResponse

	startRunUserID uuid.UUID
	startRunReq    *RunRequest
	startRunSource string
	startRunResp   *RunResponse

	getRunUserID uuid.UUID
	getRunID     uuid.UUID
	getRunResp   *RunResponse

	eventsUserID  uuid.UUID
	eventsRunID   uuid.UUID
	eventsAfter   int32
	eventsLimit   int32
	runEventsResp []RunEventResponse

	artifactsUserID uuid.UUID
	artifactsRunID  uuid.UUID
	artifactsResp   []RunArtifactResponse

	messagesUserID uuid.UUID
	messagesRunID  uuid.UUID
	messagesResp   []RunMessageResponse

	reportRunID uuid.UUID
	reportToken string
	reportReq   *ReportRunEventRequest
	reportResp  *RunEventResponse

	claimToken string
	claimWait  time.Duration
	claimResp  *RuntimePullRunResponse

	heartbeatToken string
	heartbeatResp  *AgentHeartbeatResponse

	completeToken string
	completeRunID uuid.UUID
	completeReq   *RuntimePullResultRequest
	completeResp  *RunResponse

	wsToken string
}

func (m *mockRuntimeService) Run(_ context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	m.runUserID = userID
	m.runReq = req
	m.runSource = source
	return m.runResp, m.err
}

func (m *mockRuntimeService) StartRun(_ context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	m.startRunUserID = userID
	m.startRunReq = req
	m.startRunSource = source
	return m.startRunResp, m.err
}

func (m *mockRuntimeService) GetRun(_ context.Context, userID, runID uuid.UUID) (*RunResponse, error) {
	m.getRunUserID = userID
	m.getRunID = runID
	return m.getRunResp, m.err
}

func (m *mockRuntimeService) ListRunEvents(_ context.Context, userID, runID uuid.UUID, afterSequence, limit int32) ([]RunEventResponse, error) {
	m.eventsUserID = userID
	m.eventsRunID = runID
	m.eventsAfter = afterSequence
	m.eventsLimit = limit
	return m.runEventsResp, m.err
}

func (m *mockRuntimeService) ListRunArtifacts(_ context.Context, userID, runID uuid.UUID) ([]RunArtifactResponse, error) {
	m.artifactsUserID = userID
	m.artifactsRunID = runID
	return m.artifactsResp, m.err
}

func (m *mockRuntimeService) ListRunMessages(_ context.Context, userID, runID uuid.UUID) ([]RunMessageResponse, error) {
	m.messagesUserID = userID
	m.messagesRunID = runID
	return m.messagesResp, m.err
}

func (m *mockRuntimeService) ReportRunEvent(_ context.Context, runID uuid.UUID, token string, req *ReportRunEventRequest) (*RunEventResponse, error) {
	m.reportRunID = runID
	m.reportToken = token
	m.reportReq = req
	return m.reportResp, m.err
}

func (m *mockRuntimeService) ClaimRuntimePullRun(_ context.Context, token string, opts ...RuntimePullClaimOptions) (*RuntimePullRunResponse, error) {
	m.claimToken = token
	if len(opts) > 0 {
		m.claimWait = opts[0].Wait
	}
	return m.claimResp, m.err
}

func (m *mockRuntimeService) HeartbeatAgent(_ context.Context, token string) (*AgentHeartbeatResponse, error) {
	m.heartbeatToken = token
	return m.heartbeatResp, m.err
}

func (m *mockRuntimeService) CompleteRuntimePullRun(_ context.Context, token string, runID uuid.UUID, req *RuntimePullResultRequest) (*RunResponse, error) {
	m.completeToken = token
	m.completeRunID = runID
	m.completeReq = req
	return m.completeResp, m.err
}

func (m *mockRuntimeService) ServeRuntimeWebSocket(_ http.ResponseWriter, _ *http.Request, token string) error {
	m.wsToken = token
	return m.err
}

type runtimeDispatchRequest struct {
	method     string
	target     string
	body       string
	userID     string
	authMethod string
	scopes     []string
	params     map[string]string
	headers    map[string]string
}

func newRuntimeDispatchContext(spec *runtimeDispatchRequest) (echo.Context, *httptest.ResponseRecorder) {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	if spec.authMethod != "" {
		c.Set(string(httpx.CtxKeyAuthMethod), spec.authMethod)
	}
	if spec.scopes != nil {
		c.Set(string(httpx.CtxKeyAuthScopes), spec.scopes)
	}
	if len(spec.params) > 0 {
		names := make([]string, 0, len(spec.params))
		values := make([]string, 0, len(spec.params))
		for name, value := range spec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c, rec
}

func mustRuntimeDispatchContext(spec *runtimeDispatchRequest) echo.Context {
	c, _ := newRuntimeDispatchContext(spec)
	return c
}

func decodeRuntimeDispatchJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}

func requireRuntimeDispatchHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var httpErr *httpx.HTTPError
	if !strings.Contains(err.Error(), "service failed") {
		t.Fatalf("error = %v, want propagated service failure", err)
	}
	if ok := errors.As(err, &httpErr); !ok || httpErr.Status != want {
		t.Fatalf("HTTP status = %#v, want %d", httpErr, want)
	}
}
