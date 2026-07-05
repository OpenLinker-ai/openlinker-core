package skill

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
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestSkillHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	skills := []db.Skill{
		{ID: "data/sql", Category: "data", Name: "SQL", Description: "query data", SortOrder: 1},
		{ID: "dev/review", Category: "dev", Name: "Review", Description: "review code", SortOrder: 2},
	}

	t.Run("list all", func(t *testing.T) {
		mock := &mockSkillService{listAllResp: skills}
		c, rec := newSkillDispatchContext(http.MethodGet, "/skills", "", "", nil)

		if err := NewHandler(mock, nil).ListAll(c); err != nil {
			t.Fatalf("ListAll error = %v", err)
		}
		if rec.Code != http.StatusOK || !mock.listAllCalled {
			t.Fatalf("list all code=%d called=%v", rec.Code, mock.listAllCalled)
		}
		var body struct {
			Items []SkillItem `json:"items"`
		}
		decodeSkillDispatchJSON(t, rec, &body)
		if len(body.Items) != 2 || body.Items[0].ID != "data/sql" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("set agent skills", func(t *testing.T) {
		mock := &mockSkillService{listForAgentResp: skills[:1]}
		h := NewHandler(mock, nil)
		h.q = &mockSkillAgentReader{agent: db.Agent{ID: agentID, CreatorID: userID}}
		c, rec := newSkillDispatchContext(
			http.MethodPatch,
			"/creator/agents/"+agentID.String()+"/skills",
			`{"skill_ids":["data/sql","dev/review"]}`,
			userID.String(),
			map[string]string{"id": agentID.String()},
		)

		if err := h.SetAgentSkills(c); err != nil {
			t.Fatalf("SetAgentSkills error = %v", err)
		}
		if rec.Code != http.StatusOK || mock.setAgentID != agentID || mock.listForAgentID != agentID {
			t.Fatalf("set code=%d setAgent=%s listAgent=%s", rec.Code, mock.setAgentID, mock.listForAgentID)
		}
		if len(mock.setSkillIDs) != 2 || mock.setSkillIDs[0] != "data/sql" {
			t.Fatalf("skill ids = %#v", mock.setSkillIDs)
		}
		var body SetSkillsResponse
		decodeSkillDispatchJSON(t, rec, &body)
		if body.AgentID != agentID.String() || len(body.Items) != 1 || body.Items[0].ID != "data/sql" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("create proposal", func(t *testing.T) {
		proposalID := uuid.NewString()
		mock := &mockSkillService{proposalResp: &SkillProposalItem{ID: proposalID, ProposedSkillID: "data/pdf-parse", Status: "pending"}}
		c, rec := newSkillDispatchContext(
			http.MethodPost,
			"/skills/proposals",
			`{"proposed_skill_id":"data/pdf-parse","category":"data","name":"PDF 解析","description":"解析 PDF 表格","source":"manual"}`,
			userID.String(),
			nil,
		)

		if err := NewHandler(mock, nil).CreateProposal(c); err != nil {
			t.Fatalf("CreateProposal error = %v", err)
		}
		if rec.Code != http.StatusCreated || mock.proposalOwnerID != userID || mock.proposalReq.ProposedSkillID != "data/pdf-parse" {
			t.Fatalf("proposal code=%d owner=%s req=%#v", rec.Code, mock.proposalOwnerID, mock.proposalReq)
		}
		var body SkillProposalItem
		decodeSkillDispatchJSON(t, rec, &body)
		if body.ID != proposalID || body.Status != "pending" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("list proposals", func(t *testing.T) {
		mock := &mockSkillService{proposalListResp: []SkillProposalItem{{ID: uuid.NewString(), ProposedSkillID: "data/pdf-parse", Status: "pending"}}}
		c, rec := newSkillDispatchContext(http.MethodGet, "/creator/skill-proposals", "", userID.String(), nil)

		if err := NewHandler(mock, nil).ListProposals(c); err != nil {
			t.Fatalf("ListProposals error = %v", err)
		}
		if rec.Code != http.StatusOK || mock.proposalListOwnerID != userID {
			t.Fatalf("list proposals code=%d owner=%s", rec.Code, mock.proposalListOwnerID)
		}
		var body SkillProposalListResponse
		decodeSkillDispatchJSON(t, rec, &body)
		if len(body.Items) != 1 || body.Items[0].ProposedSkillID != "data/pdf-parse" {
			t.Fatalf("body = %#v", body)
		}
	})
}

func TestSkillHandlerPropagatesServiceAndOwnershipErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()

	t.Run("list all service error", func(t *testing.T) {
		mock := &mockSkillService{listAllErr: httpx.Internal("list failed")}
		c, _ := newSkillDispatchContext(http.MethodGet, "/skills", "", "", nil)
		requireSkillDispatchHTTPStatus(t, NewHandler(mock, nil).ListAll(c), http.StatusInternalServerError)
	})

	t.Run("set missing agent", func(t *testing.T) {
		h := NewHandler(&mockSkillService{}, nil)
		h.q = &mockSkillAgentReader{err: pgx.ErrNoRows}
		c, _ := newSkillDispatchContext(
			http.MethodPatch,
			"/creator/agents/"+agentID.String()+"/skills",
			`{"skill_ids":["data/sql"]}`,
			userID.String(),
			map[string]string{"id": agentID.String()},
		)
		requireSkillDispatchHTTPStatus(t, h.SetAgentSkills(c), http.StatusNotFound)
	})

	t.Run("set wrong owner", func(t *testing.T) {
		h := NewHandler(&mockSkillService{}, nil)
		h.q = &mockSkillAgentReader{agent: db.Agent{ID: agentID, CreatorID: uuid.New()}}
		c, _ := newSkillDispatchContext(
			http.MethodPatch,
			"/creator/agents/"+agentID.String()+"/skills",
			`{"skill_ids":["data/sql"]}`,
			userID.String(),
			map[string]string{"id": agentID.String()},
		)
		requireSkillDispatchHTTPStatus(t, h.SetAgentSkills(c), http.StatusForbidden)
	})

	t.Run("set service error", func(t *testing.T) {
		mock := &mockSkillService{setErr: httpx.BadRequest("bad skill")}
		h := NewHandler(mock, nil)
		h.q = &mockSkillAgentReader{agent: db.Agent{ID: agentID, CreatorID: userID}}
		c, _ := newSkillDispatchContext(
			http.MethodPatch,
			"/creator/agents/"+agentID.String()+"/skills",
			`{"skill_ids":["missing"]}`,
			userID.String(),
			map[string]string{"id": agentID.String()},
		)
		requireSkillDispatchHTTPStatus(t, h.SetAgentSkills(c), http.StatusBadRequest)
	})

	t.Run("set list response error", func(t *testing.T) {
		mock := &mockSkillService{listForAgentErr: httpx.Internal("reload failed")}
		h := NewHandler(mock, nil)
		h.q = &mockSkillAgentReader{agent: db.Agent{ID: agentID, CreatorID: userID}}
		c, _ := newSkillDispatchContext(
			http.MethodPatch,
			"/creator/agents/"+agentID.String()+"/skills",
			`{"skill_ids":["data/sql"]}`,
			userID.String(),
			map[string]string{"id": agentID.String()},
		)
		requireSkillDispatchHTTPStatus(t, h.SetAgentSkills(c), http.StatusInternalServerError)
	})
}

func TestBenchmarkHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	batchID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)
	score := int32(91)
	scoreItem := SkillScoreItem{
		SkillID:      "data/sql",
		SkillName:    "SQL",
		Status:       BenchmarkStatusVerified,
		AverageScore: &score,
		PassCount:    3,
		TotalCount:   4,
		UpdatedAt:    now,
	}
	batchDetail := &BenchmarkBatchDetail{
		BatchID:      batchID.String(),
		AgentID:      agentID.String(),
		SkillID:      "data/sql",
		Status:       BenchmarkStatusVerified,
		AverageScore: &score,
		Items:        []BenchmarkRunItem{{ID: uuid.NewString(), TestCaseTitle: "SQL joins", Status: "success", Score: &score, StartedAt: now}},
	}
	summary := BenchmarkBatchSummary{
		BatchID:      batchID.String(),
		SkillID:      "data/sql",
		TotalCount:   4,
		SuccessCount: 3,
		AverageScore: &score,
		StartedAt:    now,
	}
	topAgent := TopAgentForSkill{
		AgentID:           agentID.String(),
		Slug:              "sql-agent",
		Name:              "SQL Agent",
		Description:       "query data",
		Tags:              []string{"data"},
		PricePerCallCents: 10,
		TotalCalls:        20,
		AverageScore:      &score,
	}
	mock := &mockBenchmarkService{
		status:         BenchmarkRuntimeStatus{CanRun: true, Reasons: []string{}, Message: "ready"},
		runResp:        &RunBenchmarkResponse{BatchID: batchID.String(), SkillID: "data/sql", Status: "running"},
		scoresResp:     []SkillScoreItem{scoreItem},
		slugScoresResp: []SkillScoreItem{scoreItem},
		batchDetail:    batchDetail,
		publicDetail:   batchDetail,
		summariesResp:  []BenchmarkBatchSummary{summary},
		topAgentsResp:  []TopAgentForSkill{topAgent},
	}
	h := NewBenchmarkHandler(mock)

	t.Run("runtime status", func(t *testing.T) {
		c, rec := newSkillDispatchContext(http.MethodGet, "/benchmark/status", "", "", nil)
		if err := h.GetRuntimeStatus(c); err != nil {
			t.Fatalf("GetRuntimeStatus error = %v", err)
		}
		if rec.Code != http.StatusOK || !mock.statusCalled {
			t.Fatalf("status code=%d called=%v", rec.Code, mock.statusCalled)
		}
	})

	t.Run("run benchmark", func(t *testing.T) {
		c, rec := newSkillDispatchContext(
			http.MethodPost,
			"/creator/agents/"+agentID.String()+"/benchmarks",
			`{"skill_id":"data/sql"}`,
			userID.String(),
			map[string]string{"id": agentID.String()},
		)
		if err := h.RunBenchmark(c); err != nil {
			t.Fatalf("RunBenchmark error = %v", err)
		}
		if rec.Code != http.StatusAccepted || mock.runAgentID != agentID || mock.runCreatorID != userID || mock.runSkillID != "data/sql" {
			t.Fatalf("run code=%d agent=%s creator=%s skill=%q", rec.Code, mock.runAgentID, mock.runCreatorID, mock.runSkillID)
		}
	})

	t.Run("creator scores and batch", func(t *testing.T) {
		scoresCtx, scoresRec := newSkillDispatchContext(http.MethodGet, "/creator/agents/"+agentID.String()+"/skill-scores", "", userID.String(), map[string]string{"id": agentID.String()})
		if err := h.ListMyScores(scoresCtx); err != nil {
			t.Fatalf("ListMyScores error = %v", err)
		}
		if scoresRec.Code != http.StatusOK || mock.ownerAgentID != agentID || mock.ownerUserID != userID || mock.scoresAgentID != agentID {
			t.Fatalf("scores code=%d ownerAgent=%s ownerUser=%s scoresAgent=%s", scoresRec.Code, mock.ownerAgentID, mock.ownerUserID, mock.scoresAgentID)
		}

		batchCtx, batchRec := newSkillDispatchContext(http.MethodGet, "/creator/agents/"+agentID.String()+"/benchmarks/"+batchID.String(), "", userID.String(), map[string]string{"id": agentID.String(), "batchID": batchID.String()})
		if err := h.GetBatch(batchCtx); err != nil {
			t.Fatalf("GetBatch error = %v", err)
		}
		if batchRec.Code != http.StatusOK || mock.batchAgentID != agentID || mock.batchCreatorID != userID || mock.batchID != batchID {
			t.Fatalf("batch code=%d agent=%s creator=%s batch=%s", batchRec.Code, mock.batchAgentID, mock.batchCreatorID, mock.batchID)
		}
	})

	t.Run("public surfaces", func(t *testing.T) {
		slugCtx, slugRec := newSkillDispatchContext(http.MethodGet, "/agents/sql-agent/skill-scores", "", "", map[string]string{"slug": "sql-agent"})
		if err := h.ListScoresBySlug(slugCtx); err != nil {
			t.Fatalf("ListScoresBySlug error = %v", err)
		}
		if slugRec.Code != http.StatusOK || mock.slug != "sql-agent" {
			t.Fatalf("slug code=%d slug=%q", slugRec.Code, mock.slug)
		}

		topCtx, topRec := newSkillDispatchContext(http.MethodGet, "/skills/data%2Fsql/top-agents?limit=2", "", "", map[string]string{"id": "data%2Fsql"})
		if err := h.ListTopAgents(topCtx); err != nil {
			t.Fatalf("ListTopAgents error = %v", err)
		}
		if topRec.Code != http.StatusOK || mock.topSkillID != "data/sql" || mock.topLimit != 2 {
			t.Fatalf("top code=%d skill=%q limit=%d", topRec.Code, mock.topSkillID, mock.topLimit)
		}

		summariesCtx, summariesRec := newSkillDispatchContext(http.MethodGet, "/agents/"+agentID.String()+"/benchmarks?limit=4", "", "", map[string]string{"id": agentID.String()})
		if err := h.ListBatchSummariesPublic(summariesCtx); err != nil {
			t.Fatalf("ListBatchSummariesPublic error = %v", err)
		}
		if summariesRec.Code != http.StatusOK || mock.summariesAgentID != agentID || mock.summariesLimit != 4 {
			t.Fatalf("summaries code=%d agent=%s limit=%d", summariesRec.Code, mock.summariesAgentID, mock.summariesLimit)
		}

		publicBatchCtx, publicBatchRec := newSkillDispatchContext(http.MethodGet, "/agents/"+agentID.String()+"/benchmarks/"+batchID.String(), "", "", map[string]string{"id": agentID.String(), "batchID": batchID.String()})
		if err := h.GetBatchPublic(publicBatchCtx); err != nil {
			t.Fatalf("GetBatchPublic error = %v", err)
		}
		if publicBatchRec.Code != http.StatusOK || mock.publicBatchAgentID != agentID || mock.publicBatchID != batchID {
			t.Fatalf("public batch code=%d agent=%s batch=%s", publicBatchRec.Code, mock.publicBatchAgentID, mock.publicBatchID)
		}

		resultsCtx, resultsRec := newSkillDispatchContext(http.MethodGet, "/agents/"+agentID.String()+"/benchmark-results", "", "", map[string]string{"id": agentID.String()})
		if err := h.ListBenchmarkResults(resultsCtx); err != nil {
			t.Fatalf("ListBenchmarkResults error = %v", err)
		}
		if resultsRec.Code != http.StatusOK || mock.publicAgentID != agentID || mock.scoresAgentID != agentID {
			t.Fatalf("results code=%d public=%s scores=%s", resultsRec.Code, mock.publicAgentID, mock.scoresAgentID)
		}
	})
}

func TestBenchmarkHandlerPropagatesServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	batchID := uuid.New()

	tests := []struct {
		name string
		call func(*BenchmarkHandler, echo.Context) error
		mock *mockBenchmarkService
		ctx  echo.Context
		want int
	}{
		{
			name: "run benchmark",
			call: (*BenchmarkHandler).RunBenchmark,
			mock: &mockBenchmarkService{runErr: httpx.ServiceUnavailable("runtime unavailable")},
			ctx:  mustSkillDispatchContext(http.MethodPost, "/creator/agents/"+agentID.String()+"/benchmarks", `{"skill_id":"data/sql"}`, userID.String(), map[string]string{"id": agentID.String()}),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "owner check",
			call: (*BenchmarkHandler).ListMyScores,
			mock: &mockBenchmarkService{ownerErr: httpx.NotFound("missing")},
			ctx:  mustSkillDispatchContext(http.MethodGet, "/creator/agents/"+agentID.String()+"/skill-scores", "", userID.String(), map[string]string{"id": agentID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "scores",
			call: (*BenchmarkHandler).ListScoresBySlug,
			mock: &mockBenchmarkService{slugScoresErr: httpx.Internal("scores failed")},
			ctx:  mustSkillDispatchContext(http.MethodGet, "/agents/sql-agent/skill-scores", "", "", map[string]string{"slug": "sql-agent"}),
			want: http.StatusInternalServerError,
		},
		{
			name: "top agents",
			call: (*BenchmarkHandler).ListTopAgents,
			mock: &mockBenchmarkService{topAgentsErr: httpx.Internal("top failed")},
			ctx:  mustSkillDispatchContext(http.MethodGet, "/skills/data%2Fsql/top-agents", "", "", map[string]string{"id": "data%2Fsql"}),
			want: http.StatusInternalServerError,
		},
		{
			name: "batch detail",
			call: (*BenchmarkHandler).GetBatch,
			mock: &mockBenchmarkService{batchErr: httpx.NotFound("batch missing")},
			ctx:  mustSkillDispatchContext(http.MethodGet, "/creator/agents/"+agentID.String()+"/benchmarks/"+batchID.String(), "", userID.String(), map[string]string{"id": agentID.String(), "batchID": batchID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "public visibility",
			call: (*BenchmarkHandler).ListBenchmarkResults,
			mock: &mockBenchmarkService{publicErr: httpx.NotFound("hidden")},
			ctx:  mustSkillDispatchContext(http.MethodGet, "/agents/"+agentID.String()+"/benchmark-results", "", "", map[string]string{"id": agentID.String()}),
			want: http.StatusNotFound,
		},
		{
			name: "public summaries",
			call: (*BenchmarkHandler).ListBatchSummariesPublic,
			mock: &mockBenchmarkService{summariesErr: httpx.Internal("summaries failed")},
			ctx:  mustSkillDispatchContext(http.MethodGet, "/agents/"+agentID.String()+"/benchmarks", "", "", map[string]string{"id": agentID.String()}),
			want: http.StatusInternalServerError,
		},
		{
			name: "public batch",
			call: (*BenchmarkHandler).GetBatchPublic,
			mock: &mockBenchmarkService{publicBatchErr: httpx.NotFound("missing")},
			ctx:  mustSkillDispatchContext(http.MethodGet, "/agents/"+agentID.String()+"/benchmarks/"+batchID.String(), "", "", map[string]string{"id": agentID.String(), "batchID": batchID.String()}),
			want: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireSkillDispatchHTTPStatus(t, tt.call(NewBenchmarkHandler(tt.mock), tt.ctx), tt.want)
		})
	}
}

type mockSkillService struct {
	listAllCalled bool
	listAllResp   []db.Skill
	listAllErr    error

	setAgentID  uuid.UUID
	setSkillIDs []string
	setErr      error

	listForAgentID   uuid.UUID
	listForAgentResp []db.Skill
	listForAgentErr  error

	proposalOwnerID uuid.UUID
	proposalReq     *CreateSkillProposalRequest
	proposalResp    *SkillProposalItem
	proposalErr     error

	proposalListOwnerID uuid.UUID
	proposalListResp    []SkillProposalItem
	proposalListErr     error
}

func (m *mockSkillService) ListAll(context.Context) ([]db.Skill, error) {
	m.listAllCalled = true
	return m.listAllResp, m.listAllErr
}

func (m *mockSkillService) SetAgentSkills(_ context.Context, agentID uuid.UUID, skillIDs []string) error {
	m.setAgentID = agentID
	m.setSkillIDs = skillIDs
	return m.setErr
}

func (m *mockSkillService) ListForAgent(_ context.Context, agentID uuid.UUID) ([]db.Skill, error) {
	m.listForAgentID = agentID
	return m.listForAgentResp, m.listForAgentErr
}

func (m *mockSkillService) CreateProposal(_ context.Context, ownerID uuid.UUID, req *CreateSkillProposalRequest) (*SkillProposalItem, error) {
	m.proposalOwnerID = ownerID
	m.proposalReq = req
	return m.proposalResp, m.proposalErr
}

func (m *mockSkillService) ListProposals(_ context.Context, ownerID uuid.UUID) ([]SkillProposalItem, error) {
	m.proposalListOwnerID = ownerID
	return m.proposalListResp, m.proposalListErr
}

type mockSkillAgentReader struct {
	agent db.Agent
	err   error
}

func (m *mockSkillAgentReader) GetAgentByID(context.Context, uuid.UUID) (db.Agent, error) {
	return m.agent, m.err
}

type mockBenchmarkService struct {
	status       BenchmarkRuntimeStatus
	statusCalled bool

	runAgentID   uuid.UUID
	runCreatorID uuid.UUID
	runSkillID   string
	runResp      *RunBenchmarkResponse
	runErr       error

	ownerAgentID uuid.UUID
	ownerUserID  uuid.UUID
	ownerErr     error

	scoresAgentID uuid.UUID
	scoresResp    []SkillScoreItem
	scoresErr     error

	batchAgentID   uuid.UUID
	batchCreatorID uuid.UUID
	batchID        uuid.UUID
	batchDetail    *BenchmarkBatchDetail
	batchErr       error

	slug           string
	slugScoresResp []SkillScoreItem
	slugScoresErr  error

	topSkillID    string
	topLimit      int
	topAgentsResp []TopAgentForSkill
	topAgentsErr  error

	summariesAgentID uuid.UUID
	summariesLimit   int
	summariesResp    []BenchmarkBatchSummary
	summariesErr     error

	publicBatchAgentID uuid.UUID
	publicBatchID      uuid.UUID
	publicDetail       *BenchmarkBatchDetail
	publicBatchErr     error

	publicAgentID uuid.UUID
	publicErr     error
}

func (m *mockBenchmarkService) RunBenchmark(_ context.Context, agentID, creatorID uuid.UUID, skillID string) (*RunBenchmarkResponse, error) {
	m.runAgentID = agentID
	m.runCreatorID = creatorID
	m.runSkillID = skillID
	return m.runResp, m.runErr
}

func (m *mockBenchmarkService) RuntimeStatus() BenchmarkRuntimeStatus {
	m.statusCalled = true
	return m.status
}

func (m *mockBenchmarkService) assertOwner(_ context.Context, agentID, creatorID uuid.UUID) error {
	m.ownerAgentID = agentID
	m.ownerUserID = creatorID
	return m.ownerErr
}

func (m *mockBenchmarkService) ListAgentScores(_ context.Context, agentID uuid.UUID) ([]SkillScoreItem, error) {
	m.scoresAgentID = agentID
	return m.scoresResp, m.scoresErr
}

func (m *mockBenchmarkService) GetBatchDetail(_ context.Context, agentID, creatorID, batchID uuid.UUID) (*BenchmarkBatchDetail, error) {
	m.batchAgentID = agentID
	m.batchCreatorID = creatorID
	m.batchID = batchID
	return m.batchDetail, m.batchErr
}

func (m *mockBenchmarkService) ListAgentScoresBySlug(_ context.Context, slug string) ([]SkillScoreItem, error) {
	m.slug = slug
	return m.slugScoresResp, m.slugScoresErr
}

func (m *mockBenchmarkService) ListTopAgents(_ context.Context, skillID string, limit int) ([]TopAgentForSkill, error) {
	m.topSkillID = skillID
	m.topLimit = limit
	return m.topAgentsResp, m.topAgentsErr
}

func (m *mockBenchmarkService) ListBatchSummariesPublic(_ context.Context, agentID uuid.UUID, limit int) ([]BenchmarkBatchSummary, error) {
	m.summariesAgentID = agentID
	m.summariesLimit = limit
	return m.summariesResp, m.summariesErr
}

func (m *mockBenchmarkService) GetBatchDetailPublic(_ context.Context, agentID, batchID uuid.UUID) (*BenchmarkBatchDetail, error) {
	m.publicBatchAgentID = agentID
	m.publicBatchID = batchID
	return m.publicDetail, m.publicBatchErr
}

func (m *mockBenchmarkService) assertPublicVisible(_ context.Context, agentID uuid.UUID) error {
	m.publicAgentID = agentID
	return m.publicErr
}

func newSkillDispatchContext(method, target, body, userID string, params map[string]string) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
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

func mustSkillDispatchContext(method, target, body, userID string, params map[string]string) echo.Context {
	c, _ := newSkillDispatchContext(method, target, body, userID, params)
	return c
}

func decodeSkillDispatchJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}

func requireSkillDispatchHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if httpErr.Status != want {
		t.Fatalf("HTTP status = %d (%s), want %d", httpErr.Status, httpErr.Message, want)
	}
}
