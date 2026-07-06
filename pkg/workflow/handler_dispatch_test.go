package workflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestWorkflowHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	otherRunID := uuid.New()
	agentID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)

	workflowResp := &WorkflowResponse{
		ID:          workflowID.String(),
		Name:        "A2A Review",
		Description: "multi-agent flow",
		Status:      "active",
		Nodes: []WorkflowNodeResponse{{
			ID:       uuid.NewString(),
			Key:      "review",
			Type:     "agent",
			AgentID:  agentID.String(),
			Title:    "Review",
			Config:   map[string]interface{}{"mode": "fast"},
			Position: 0,
		}},
		Edges:     []map[string]interface{}{},
		CreatedAt: now,
		UpdatedAt: now,
	}
	runResp := &WorkflowRunResponse{
		ID:           runID.String(),
		WorkflowID:   workflowID.String(),
		Status:       "success",
		Input:        map[string]interface{}{"topic": "a2a"},
		Output:       map[string]interface{}{"summary": "done"},
		Steps:        []WorkflowRunStepResponse{},
		AttemptCount: 1,
		MaxAttempts:  3,
		StartedAt:    now,
		FinishedAt:   now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	comparisonResp := &WorkflowRunComparisonResponse{
		BaseRunID:      runID.String(),
		CandidateRunID: otherRunID.String(),
		WorkflowID:     workflowID.String(),
	}
	rerunResp := &WorkflowStepRerunResponse{
		SourceRunID:   runID.String(),
		RerunRunID:    otherRunID.String(),
		NodeKey:       "review",
		RerunNodeKeys: []string{"review"},
		Run:           *runResp,
		Comparison:    *comparisonResp,
	}

	mock := &mockWorkflowService{
		workflowResp: workflowResp,
		listResp:     &WorkflowListResponse{Items: []WorkflowResponse{*workflowResp}, Total: 1},
		runResp:      runResp,
		runListResp:  &WorkflowRunListResponse{Items: []WorkflowRunResponse{*runResp}, Total: 1},
		rerunResp:    rerunResp,
		compareResp:  comparisonResp,
	}
	h := NewHandler(mock)

	t.Run("definitions", func(t *testing.T) {
		createCtx, createRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodPost,
			target: "/workflows",
			userID: userID.String(),
			body:   `{"name":"A2A Review","nodes":[{"key":"review","agent_id":"` + agentID.String() + `"}]}`,
		})
		if err := h.Create(createCtx); err != nil {
			t.Fatalf("Create error = %v", err)
		}
		if createRec.Code != http.StatusCreated || mock.createUserID != userID || mock.createReq.Name != "A2A Review" || len(mock.createReq.Nodes) != 1 {
			t.Fatalf("create code=%d user=%s req=%#v", createRec.Code, mock.createUserID, mock.createReq)
		}

		listCtx, listRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodGet,
			target: "/workflows",
			userID: userID.String(),
		})
		if err := h.List(listCtx); err != nil {
			t.Fatalf("List error = %v", err)
		}
		if listRec.Code != http.StatusOK || mock.listUserID != userID || mock.listLimit != 20 {
			t.Fatalf("list code=%d user=%s limit=%d", listRec.Code, mock.listUserID, mock.listLimit)
		}
		var listBody WorkflowListResponse
		decodeWorkflowDispatchJSON(t, listRec, &listBody)
		if listBody.Total != 1 || len(listBody.Items) != 1 || listBody.Items[0].ID != workflowID.String() {
			t.Fatalf("list body = %#v", listBody)
		}

		getCtx, getRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodGet,
			target: "/workflows/" + workflowID.String(),
			userID: userID.String(),
			params: map[string]string{"id": workflowID.String()},
		})
		if err := h.Get(getCtx); err != nil {
			t.Fatalf("Get error = %v", err)
		}
		if getRec.Code != http.StatusOK || mock.getUserID != userID || mock.getWorkflowID != workflowID {
			t.Fatalf("get code=%d user=%s workflow=%s", getRec.Code, mock.getUserID, mock.getWorkflowID)
		}
	})

	t.Run("run lifecycle", func(t *testing.T) {
		runCtx, runRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodPost,
			target: "/workflows/" + workflowID.String() + "/run",
			userID: userID.String(),
			body:   `{"input":{"topic":"a2a"},"max_attempts":2}`,
			params: map[string]string{"id": workflowID.String()},
		})
		if err := h.Run(runCtx); err != nil {
			t.Fatalf("Run error = %v", err)
		}
		if runRec.Code != http.StatusOK || mock.runUserID != userID || mock.runWorkflowID != workflowID || mock.runReq.MaxAttempts != 2 {
			t.Fatalf("run code=%d user=%s workflow=%s req=%#v", runRec.Code, mock.runUserID, mock.runWorkflowID, mock.runReq)
		}

		startCtx, startRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodPost,
			target: "/workflows/" + workflowID.String() + "/runs",
			userID: userID.String(),
			body:   `{"input":{"topic":"async"}}`,
			params: map[string]string{"id": workflowID.String()},
		})
		if err := h.StartRun(startCtx); err != nil {
			t.Fatalf("StartRun error = %v", err)
		}
		if startRec.Code != http.StatusAccepted || mock.startUserID != userID || mock.startWorkflowID != workflowID {
			t.Fatalf("start code=%d user=%s workflow=%s", startRec.Code, mock.startUserID, mock.startWorkflowID)
		}

		listRunsCtx, listRunsRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodGet,
			target: "/workflows/" + workflowID.String() + "/runs?q=extract&status=success&sort=updated_desc&page=2&size=5",
			userID: userID.String(),
			params: map[string]string{"id": workflowID.String()},
		})
		if err := h.ListRuns(listRunsCtx); err != nil {
			t.Fatalf("ListRuns error = %v", err)
		}
		if listRunsRec.Code != http.StatusOK || mock.listRunsUserID != userID || mock.listRunsWorkflowID != workflowID || mock.listRunsLimit != 5 || mock.listRunsPage != 2 || mock.listRunsQuery != "extract" || mock.listRunsStatus != "success" || mock.listRunsSort != "updated_desc" {
			t.Fatalf("list runs code=%d user=%s workflow=%s page=%d limit=%d q=%q status=%q sort=%q", listRunsRec.Code, mock.listRunsUserID, mock.listRunsWorkflowID, mock.listRunsPage, mock.listRunsLimit, mock.listRunsQuery, mock.listRunsStatus, mock.listRunsSort)
		}

		getRunCtx, getRunRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodGet,
			target: "/workflow-runs/" + runID.String(),
			userID: userID.String(),
			params: map[string]string{"id": runID.String()},
		})
		if err := h.GetRun(getRunCtx); err != nil {
			t.Fatalf("GetRun error = %v", err)
		}
		if getRunRec.Code != http.StatusOK || mock.getRunUserID != userID || mock.getRunID != runID {
			t.Fatalf("get run code=%d user=%s run=%s", getRunRec.Code, mock.getRunUserID, mock.getRunID)
		}
	})

	t.Run("control and compare", func(t *testing.T) {
		retryCtx, retryRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodPost,
			target: "/workflow-runs/" + runID.String() + "/retry",
			userID: userID.String(),
			params: map[string]string{"id": runID.String()},
		})
		if err := h.RetryRun(retryCtx); err != nil {
			t.Fatalf("RetryRun error = %v", err)
		}
		if retryRec.Code != http.StatusAccepted || mock.retryUserID != userID || mock.retryRunID != runID {
			t.Fatalf("retry code=%d user=%s run=%s", retryRec.Code, mock.retryUserID, mock.retryRunID)
		}

		rerunCtx, rerunRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodPost,
			target: "/workflow-runs/" + runID.String() + "/steps/rerun",
			userID: userID.String(),
			body:   `{"node_key":"review"}`,
			params: map[string]string{"id": runID.String()},
		})
		if err := h.RerunStep(rerunCtx); err != nil {
			t.Fatalf("RerunStep error = %v", err)
		}
		if rerunRec.Code != http.StatusOK || mock.rerunUserID != userID || mock.rerunRunID != runID || mock.rerunReq.NodeKey != "review" {
			t.Fatalf("rerun code=%d user=%s run=%s req=%#v", rerunRec.Code, mock.rerunUserID, mock.rerunRunID, mock.rerunReq)
		}

		compareCtx, compareRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodGet,
			target: "/workflow-runs/" + runID.String() + "/compare/" + otherRunID.String(),
			userID: userID.String(),
			params: map[string]string{"id": runID.String(), "other_id": otherRunID.String()},
		})
		if err := h.CompareRuns(compareCtx); err != nil {
			t.Fatalf("CompareRuns error = %v", err)
		}
		if compareRec.Code != http.StatusOK || mock.compareUserID != userID || mock.compareBaseRunID != runID || mock.compareCandidateRunID != otherRunID {
			t.Fatalf("compare code=%d user=%s base=%s candidate=%s", compareRec.Code, mock.compareUserID, mock.compareBaseRunID, mock.compareCandidateRunID)
		}

		pauseCtx, pauseRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodPost,
			target: "/workflow-runs/" + runID.String() + "/pause",
			userID: userID.String(),
			params: map[string]string{"id": runID.String()},
		})
		if err := h.PauseRun(pauseCtx); err != nil {
			t.Fatalf("PauseRun error = %v", err)
		}
		if pauseRec.Code != http.StatusOK || mock.pauseUserID != userID || mock.pauseRunID != runID {
			t.Fatalf("pause code=%d user=%s run=%s", pauseRec.Code, mock.pauseUserID, mock.pauseRunID)
		}

		resumeCtx, resumeRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodPost,
			target: "/workflow-runs/" + runID.String() + "/resume",
			userID: userID.String(),
			params: map[string]string{"id": runID.String()},
		})
		if err := h.ResumeRun(resumeCtx); err != nil {
			t.Fatalf("ResumeRun error = %v", err)
		}
		if resumeRec.Code != http.StatusAccepted || mock.resumeUserID != userID || mock.resumeRunID != runID {
			t.Fatalf("resume code=%d user=%s run=%s", resumeRec.Code, mock.resumeUserID, mock.resumeRunID)
		}

		cancelCtx, cancelRec := newWorkflowDispatchContext(&workflowDispatchRequest{
			method: http.MethodPost,
			target: "/workflow-runs/" + runID.String() + "/cancel",
			userID: userID.String(),
			params: map[string]string{"id": runID.String()},
		})
		if err := h.CancelRun(cancelCtx); err != nil {
			t.Fatalf("CancelRun error = %v", err)
		}
		if cancelRec.Code != http.StatusOK || mock.cancelUserID != userID || mock.cancelRunID != runID {
			t.Fatalf("cancel code=%d user=%s run=%s", cancelRec.Code, mock.cancelUserID, mock.cancelRunID)
		}
	})
}

func TestWorkflowHandlerPropagatesServiceErrors(t *testing.T) {
	userID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	otherRunID := uuid.New()
	agentID := uuid.New()
	serviceErr := httpx.Conflict("service failed")

	tests := []struct {
		name string
		call func(*Handler, echo.Context) error
		ctx  echo.Context
	}{
		{
			name: "create",
			call: (*Handler).Create,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodPost,
				target: "/workflows",
				userID: userID.String(),
				body:   `{"name":"A2A Review","nodes":[{"key":"review","agent_id":"` + agentID.String() + `"}]}`,
			}),
		},
		{
			name: "list",
			call: (*Handler).List,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodGet,
				target: "/workflows",
				userID: userID.String(),
			}),
		},
		{
			name: "get",
			call: (*Handler).Get,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodGet,
				target: "/workflows/" + workflowID.String(),
				userID: userID.String(),
				params: map[string]string{"id": workflowID.String()},
			}),
		},
		{
			name: "run",
			call: (*Handler).Run,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodPost,
				target: "/workflows/" + workflowID.String() + "/run",
				userID: userID.String(),
				body:   `{"input":{"topic":"a2a"}}`,
				params: map[string]string{"id": workflowID.String()},
			}),
		},
		{
			name: "start",
			call: (*Handler).StartRun,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodPost,
				target: "/workflows/" + workflowID.String() + "/runs",
				userID: userID.String(),
				body:   `{"input":{"topic":"a2a"}}`,
				params: map[string]string{"id": workflowID.String()},
			}),
		},
		{
			name: "list runs",
			call: (*Handler).ListRuns,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodGet,
				target: "/workflows/" + workflowID.String() + "/runs",
				userID: userID.String(),
				params: map[string]string{"id": workflowID.String()},
			}),
		},
		{
			name: "get run",
			call: (*Handler).GetRun,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodGet,
				target: "/workflow-runs/" + runID.String(),
				userID: userID.String(),
				params: map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "retry",
			call: (*Handler).RetryRun,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodPost,
				target: "/workflow-runs/" + runID.String() + "/retry",
				userID: userID.String(),
				params: map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "rerun",
			call: (*Handler).RerunStep,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodPost,
				target: "/workflow-runs/" + runID.String() + "/steps/rerun",
				userID: userID.String(),
				body:   `{"node_key":"review"}`,
				params: map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "compare",
			call: (*Handler).CompareRuns,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodGet,
				target: "/workflow-runs/" + runID.String() + "/compare/" + otherRunID.String(),
				userID: userID.String(),
				params: map[string]string{"id": runID.String(), "other_id": otherRunID.String()},
			}),
		},
		{
			name: "pause",
			call: (*Handler).PauseRun,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodPost,
				target: "/workflow-runs/" + runID.String() + "/pause",
				userID: userID.String(),
				params: map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "resume",
			call: (*Handler).ResumeRun,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodPost,
				target: "/workflow-runs/" + runID.String() + "/resume",
				userID: userID.String(),
				params: map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "cancel",
			call: (*Handler).CancelRun,
			ctx: mustWorkflowDispatchContext(&workflowDispatchRequest{
				method: http.MethodPost,
				target: "/workflow-runs/" + runID.String() + "/cancel",
				userID: userID.String(),
				params: map[string]string{"id": runID.String()},
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireWorkflowHTTPStatus(t, tt.call(NewHandler(&mockWorkflowService{err: serviceErr}), tt.ctx), http.StatusConflict)
		})
	}
}

type mockWorkflowService struct {
	err error

	createUserID uuid.UUID
	createReq    *CreateWorkflowRequest
	workflowResp *WorkflowResponse

	listUserID uuid.UUID
	listLimit  int32
	listQuery  string
	listStatus string
	listSort   string
	listPage   int32
	listResp   *WorkflowListResponse

	getUserID     uuid.UUID
	getWorkflowID uuid.UUID

	runUserID     uuid.UUID
	runWorkflowID uuid.UUID
	runReq        *RunWorkflowRequest
	runResp       *WorkflowRunResponse

	startUserID     uuid.UUID
	startWorkflowID uuid.UUID
	startReq        *RunWorkflowRequest

	listRunsUserID     uuid.UUID
	listRunsWorkflowID uuid.UUID
	listRunsLimit      int32
	listRunsPage       int32
	listRunsQuery      string
	listRunsStatus     string
	listRunsSort       string
	runListResp        *WorkflowRunListResponse

	getRunUserID uuid.UUID
	getRunID     uuid.UUID

	retryUserID uuid.UUID
	retryRunID  uuid.UUID

	rerunUserID uuid.UUID
	rerunRunID  uuid.UUID
	rerunReq    *RerunWorkflowStepRequest
	rerunResp   *WorkflowStepRerunResponse

	compareUserID         uuid.UUID
	compareBaseRunID      uuid.UUID
	compareCandidateRunID uuid.UUID
	compareResp           *WorkflowRunComparisonResponse

	pauseUserID uuid.UUID
	pauseRunID  uuid.UUID

	resumeUserID uuid.UUID
	resumeRunID  uuid.UUID

	cancelUserID uuid.UUID
	cancelRunID  uuid.UUID
}

func (m *mockWorkflowService) CreateWorkflow(_ context.Context, userID uuid.UUID, req *CreateWorkflowRequest) (*WorkflowResponse, error) {
	m.createUserID = userID
	m.createReq = req
	return m.workflowResp, m.err
}

func (m *mockWorkflowService) ListWorkflows(_ context.Context, userID uuid.UUID, limit int32) (*WorkflowListResponse, error) {
	m.listUserID = userID
	m.listLimit = limit
	return m.listResp, m.err
}

func (m *mockWorkflowService) ListWorkflowsPage(_ context.Context, userID uuid.UUID, query, status, sort string, page, size int32) (*WorkflowListResponse, error) {
	m.listUserID = userID
	m.listLimit = size
	m.listQuery = query
	m.listStatus = status
	m.listSort = sort
	m.listPage = page
	return m.listResp, m.err
}

func (m *mockWorkflowService) GetWorkflow(_ context.Context, userID, workflowID uuid.UUID) (*WorkflowResponse, error) {
	m.getUserID = userID
	m.getWorkflowID = workflowID
	return m.workflowResp, m.err
}

func (m *mockWorkflowService) RunWorkflow(_ context.Context, userID, workflowID uuid.UUID, req *RunWorkflowRequest) (*WorkflowRunResponse, error) {
	m.runUserID = userID
	m.runWorkflowID = workflowID
	m.runReq = req
	return m.runResp, m.err
}

func (m *mockWorkflowService) StartWorkflowRun(_ context.Context, userID, workflowID uuid.UUID, req *RunWorkflowRequest) (*WorkflowRunResponse, error) {
	m.startUserID = userID
	m.startWorkflowID = workflowID
	m.startReq = req
	return m.runResp, m.err
}

func (m *mockWorkflowService) ListWorkflowRuns(_ context.Context, userID, workflowID uuid.UUID, limit int32) (*WorkflowRunListResponse, error) {
	m.listRunsUserID = userID
	m.listRunsWorkflowID = workflowID
	m.listRunsLimit = limit
	return m.runListResp, m.err
}

func (m *mockWorkflowService) ListWorkflowRunsPage(_ context.Context, userID, workflowID uuid.UUID, query, status, sort string, page, size int32) (*WorkflowRunListResponse, error) {
	m.listRunsUserID = userID
	m.listRunsWorkflowID = workflowID
	m.listRunsQuery = query
	m.listRunsStatus = status
	m.listRunsSort = sort
	m.listRunsPage = page
	m.listRunsLimit = size
	return m.runListResp, m.err
}

func (m *mockWorkflowService) GetWorkflowRun(_ context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	m.getRunUserID = userID
	m.getRunID = workflowRunID
	return m.runResp, m.err
}

func (m *mockWorkflowService) RetryWorkflowRun(_ context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	m.retryUserID = userID
	m.retryRunID = workflowRunID
	return m.runResp, m.err
}

func (m *mockWorkflowService) RerunWorkflowStep(_ context.Context, userID, workflowRunID uuid.UUID, req *RerunWorkflowStepRequest) (*WorkflowStepRerunResponse, error) {
	m.rerunUserID = userID
	m.rerunRunID = workflowRunID
	m.rerunReq = req
	return m.rerunResp, m.err
}

func (m *mockWorkflowService) CompareWorkflowRuns(_ context.Context, userID, baseRunID, candidateRunID uuid.UUID) (*WorkflowRunComparisonResponse, error) {
	m.compareUserID = userID
	m.compareBaseRunID = baseRunID
	m.compareCandidateRunID = candidateRunID
	return m.compareResp, m.err
}

func (m *mockWorkflowService) PauseWorkflowRun(_ context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	m.pauseUserID = userID
	m.pauseRunID = workflowRunID
	return m.runResp, m.err
}

func (m *mockWorkflowService) ResumeWorkflowRun(_ context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	m.resumeUserID = userID
	m.resumeRunID = workflowRunID
	return m.runResp, m.err
}

func (m *mockWorkflowService) CancelWorkflowRun(_ context.Context, userID, workflowRunID uuid.UUID) (*WorkflowRunResponse, error) {
	m.cancelUserID = userID
	m.cancelRunID = workflowRunID
	return m.runResp, m.err
}

type workflowDispatchRequest struct {
	method string
	target string
	body   string
	userID string
	params map[string]string
}

func newWorkflowDispatchContext(spec *workflowDispatchRequest) (echo.Context, *httptest.ResponseRecorder) {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
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

func mustWorkflowDispatchContext(spec *workflowDispatchRequest) echo.Context {
	c, _ := newWorkflowDispatchContext(spec)
	return c
}

func decodeWorkflowDispatchJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}
