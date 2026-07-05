package skill

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestNormalizeSkillIDsAndDTOs(t *testing.T) {
	require.Equal(t, []string{}, dedupNonEmpty(nil))
	require.Equal(t, []string{"data/sql", "dev/review"}, dedupNonEmpty([]string{" data/sql ", "", "dev/review", "data/sql"}))

	got, err := normalizeSkillIDs([]string{" data/sql ", "data/sql", "dev/review"})
	require.NoError(t, err)
	require.Equal(t, []string{"data/sql", "dev/review"}, got)
	_, err = normalizeSkillIDs([]string{"a", "b", "c", "d", "e", "f"})
	require.Error(t, err)

	item := toSkillItem(&db.Skill{
		ID:          "data/sql",
		Category:    "data",
		Name:        "SQL",
		Description: "query",
		SortOrder:   7,
	})
	require.Equal(t, SkillItem{ID: "data/sql", Category: "data", Name: "SQL", Description: "query", SortOrder: 7}, item)

	req, err := normalizeSkillProposalRequest(&CreateSkillProposalRequest{
		ProposedSkillID: " Data/PDF-Parse ",
		Category:        " Data ",
		Name:            " PDF 解析 ",
		Description:     " 解析 PDF 表格 ",
	})
	require.NoError(t, err)
	require.Equal(t, "data/pdf-parse", req.ProposedSkillID)
	require.Equal(t, "data", req.Category)
	require.Equal(t, "manual", req.Source)

	_, err = normalizeSkillProposalRequest(&CreateSkillProposalRequest{
		ProposedSkillID: "-bad",
		Category:        "data",
		Name:            "Bad",
		Description:     "bad skill",
	})
	require.Error(t, err)

	proposalID := uuid.New()
	agentID := uuid.New()
	matched := "data/sql"
	now := time.Date(2026, 7, 4, 10, 0, 0, 0, time.UTC)
	proposal := toSkillProposalItem(&db.SkillProposal{
		ID:              proposalID,
		OwnerUserID:     uuid.New(),
		AgentID:         &agentID,
		ProposedSkillID: "data/sql",
		Category:        "data",
		Name:            "SQL",
		Description:     "query",
		Source:          "manual",
		Status:          "merged",
		MatchedSkillID:  &matched,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.Equal(t, proposalID.String(), proposal.ID)
	require.Equal(t, agentID.String(), *proposal.AgentID)
	require.Equal(t, &matched, proposal.MatchedSkillID)
}

func TestSkillContextAndPathHelpers(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/creator/agents/bad/skills", nil), httptest.NewRecorder())

	_, err := userIDFromCtx(c)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusUnauthorized, httpErr.Status)

	c.Set(string(httpx.CtxKeyUserID), "not-a-uuid")
	_, err = userIDFromCtx(c)
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, "token 无效", httpErr.Message)

	want := uuid.New()
	c.Set(string(httpx.CtxKeyUserID), want.String())
	got, err := userIDFromCtx(c)
	require.NoError(t, err)
	require.Equal(t, want, got)

	c.SetParamNames("id")
	c.SetParamValues("bad")
	_, err = pathID(c)
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusBadRequest, httpErr.Status)

	c.SetParamValues(want.String())
	got, err = pathID(c)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBenchmarkRuntimeAndJudgeHelpers(t *testing.T) {
	require.Equal(t, BenchmarkRuntimeStatus{
		CanRun:  false,
		Reasons: []string{"endpoint_runner_unavailable", "llm_not_configured"},
		Message: "Benchmark 需要 LLM 支持。自托管用户请配置 LLM_OPENAI_URL + LLM_OPENAI_API_KEY，或联系 openlinker.ai 获取 LLM 代理地址。",
	}, NewBenchmarkService(&Service{}, nil, nil).RuntimeStatus())

	readyLLM := &fakeBenchmarkLLM{response: `{"score":101,"reason":"great"}`}
	ready := NewBenchmarkService(&Service{}, fakeEndpointRunner{}, readyLLM)
	require.Equal(t, BenchmarkRuntimeStatus{CanRun: true, Reasons: []string{}, Message: "Benchmark runtime is ready."}, ready.RuntimeStatus())

	score, reason, err := parseJudgeResponse("```json\n{\"score\":88.7,\"reason\":\"ok\"}\n```")
	require.NoError(t, err)
	require.Equal(t, int32(88), score)
	require.Equal(t, "ok", reason)
	score, reason, err = parseJudgeResponse("```\n{\"score\":72,\"reason\":\"plain fence\"}\n```")
	require.NoError(t, err)
	require.Equal(t, int32(72), score)
	require.Equal(t, "plain fence", reason)
	score, reason, err = parseJudgeResponse(`{"score":66}`)
	require.NoError(t, err)
	require.Equal(t, int32(66), score)
	require.Equal(t, "", reason)

	score, reason, err = parseJudgeResponse("score: 77 because it works")
	require.NoError(t, err)
	require.Equal(t, int32(77), score)
	require.Contains(t, reason, "score: 77")

	_, _, err = parseJudgeResponse("no number")
	require.Error(t, err)
	require.Equal(t, int32(0), clampScore(-5))
	require.Equal(t, int32(42), clampScore(42))
	require.Equal(t, int32(100), clampScore(150))
	require.True(t, skillDeclared([]db.Skill{{ID: "data/sql"}}, "data/sql"))
	require.False(t, skillDeclared([]db.Skill{{ID: "data/sql"}}, "missing"))
	require.Equal(t, "abc", truncateText("abcdef", 3))
	require.Equal(t, "abc", truncateText("abc", 3))

	score, reason, err = ready.judge(context.Background(), "请评估 {output}", map[string]interface{}{"answer": "ok"})
	require.NoError(t, err)
	require.Equal(t, int32(100), score)
	require.Equal(t, "great", reason)
	require.Contains(t, readyLLM.user, `"answer": "ok"`)
	require.NotContains(t, readyLLM.user, "{output}")

	score, reason, err = NewBenchmarkService(&Service{}, fakeEndpointRunner{}, &fakeBenchmarkLLM{response: `{"score":-9,"reason":"too low"}`}).
		judge(context.Background(), "{output}", map[string]interface{}{"answer": "bad"})
	require.NoError(t, err)
	require.Equal(t, int32(0), score)
	require.Equal(t, "too low", reason)
	_, _, err = NewBenchmarkService(&Service{}, fakeEndpointRunner{}, &fakeBenchmarkLLM{response: `not parseable`}).
		judge(context.Background(), "{output}", map[string]interface{}{"answer": "bad"})
	require.Error(t, err)
	_, _, err = NewBenchmarkService(&Service{}, nil, &fakeBenchmarkLLM{err: errors.New("llm down")}).
		judge(context.Background(), "{output}", map[string]interface{}{"answer": "ok"})
	require.Error(t, err)
	_, _, err = ready.judge(context.Background(), "{output}", map[string]interface{}{"bad": make(chan int)})
	require.Error(t, err)
}

func TestBenchmarkDTOHelpers(t *testing.T) {
	updated := time.Date(2026, 6, 20, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	verified := updated.Add(time.Minute)
	score := int32(91)
	batchID := uuid.New()

	item := toSkillScoreItem(&db.AgentSkillScore{
		SkillID:      "data/sql",
		Status:       BenchmarkStatusVerified,
		AverageScore: &score,
		PassCount:    3,
		TotalCount:   4,
		LastBatchID:  &batchID,
		VerifiedAt:   &verified,
		UpdatedAt:    updated,
	}, "SQL")
	require.Equal(t, "data/sql", item.SkillID)
	require.Equal(t, "SQL", item.SkillName)
	require.Equal(t, &score, item.AverageScore)
	require.Equal(t, batchID.String(), *item.LastBatchID)
	require.Equal(t, "2026-06-20T04:01:00Z", *item.VerifiedAt)
	require.Equal(t, "2026-06-20T04:00:00Z", item.UpdatedAt)

	require.Nil(t, formatTimePtr(nil))
	require.Equal(t, "2026-06-20T04:00:00Z", *formatTimePtr(&updated))
	require.Nil(t, uuidPtrString(nil))
	require.Equal(t, batchID.String(), *uuidPtrString(&batchID))
}

func TestBenchmarkPathAndLimitHelpers(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/skills/data%2Fsql/top-agents", nil), httptest.NewRecorder())
	c.SetParamNames("id")
	c.SetParamValues("data%2Fsql")
	require.Equal(t, "data/sql", skillIDFromPath(c))

	c = e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/skills/data/sql/top-agents", nil), httptest.NewRecorder())
	c.SetParamNames("category", "name")
	c.SetParamValues("data", "sql")
	require.Equal(t, "data/sql", skillIDFromPath(c))

	c.SetParamNames("batchID")
	c.SetParamValues("bad")
	_, err := pathUUIDParam(c, "batchID")
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusBadRequest, httpErr.Status)

	id := uuid.New()
	c.SetParamValues(id.String())
	got, err := pathUUIDParam(c, "batchID")
	require.NoError(t, err)
	require.Equal(t, id, got)

	require.Equal(t, 3, parseLimit("3"))
	require.Equal(t, 100, parseLimit("100"))
	require.Equal(t, 0, parseLimit("101"))
	require.Equal(t, 0, parseLimit("1x"))
	require.Equal(t, 0, parseLimit(""))
}

func TestSkillHandlersValidateBeforeServiceDispatch(t *testing.T) {
	validUser := uuid.NewString()
	validAgent := uuid.NewString()
	validBatch := uuid.NewString()

	tests := []struct {
		name      string
		call      func(*Handler, echo.Context) error
		method    string
		path      string
		body      string
		userID    string
		paramID   string
		wantHTTP  int
		wantError string
	}{
		{name: "set skills missing user", call: (*Handler).SetAgentSkills, method: http.MethodPatch, path: "/api/v1/creator/agents/" + validAgent + "/skills", body: `{"skill_ids":["data/sql"]}`, paramID: validAgent, wantHTTP: http.StatusUnauthorized, wantError: "认证失败"},
		{name: "set skills invalid id", call: (*Handler).SetAgentSkills, method: http.MethodPatch, path: "/api/v1/creator/agents/bad/skills", body: `{"skill_ids":["data/sql"]}`, userID: validUser, paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "set skills invalid json", call: (*Handler).SetAgentSkills, method: http.MethodPatch, path: "/api/v1/creator/agents/" + validAgent + "/skills", body: `{`, userID: validUser, paramID: validAgent, wantHTTP: http.StatusBadRequest, wantError: "请求体格式错误"},
		{name: "set skills validation", call: (*Handler).SetAgentSkills, method: http.MethodPatch, path: "/api/v1/creator/agents/" + validAgent + "/skills", body: `{}`, userID: validUser, paramID: validAgent, wantHTTP: http.StatusUnprocessableEntity, wantError: "SkillIDs"},
		{name: "proposal missing user", call: (*Handler).CreateProposal, method: http.MethodPost, path: "/api/v1/skills/proposals", body: `{"proposed_skill_id":"data/pdf","category":"data","name":"PDF","description":"parse pdf"}`, wantHTTP: http.StatusUnauthorized, wantError: "认证失败"},
		{name: "proposal invalid json", call: (*Handler).CreateProposal, method: http.MethodPost, path: "/api/v1/skills/proposals", body: `{`, userID: validUser, wantHTTP: http.StatusBadRequest, wantError: "请求体格式错误"},
		{name: "proposal validation", call: (*Handler).CreateProposal, method: http.MethodPost, path: "/api/v1/skills/proposals", body: `{}`, userID: validUser, wantHTTP: http.StatusUnprocessableEntity, wantError: "ProposedSkillID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			c := e.NewContext(skillJSONRequest(tt.method, tt.path, tt.body), httptest.NewRecorder())
			if tt.userID != "" {
				c.Set(string(httpx.CtxKeyUserID), tt.userID)
			}
			if tt.paramID != "" {
				c.SetParamNames("id")
				c.SetParamValues(tt.paramID)
			}
			err := tt.call(NewHandler(nil, nil), c)
			require.Error(t, err)

			var httpErr *httpx.HTTPError
			require.True(t, errors.As(err, &httpErr))
			require.Equal(t, tt.wantHTTP, httpErr.Status)
			require.Contains(t, httpErr.Message, tt.wantError)
		})
	}

	benchTests := []struct {
		name       string
		call       func(*BenchmarkHandler, echo.Context) error
		method     string
		path       string
		body       string
		userID     string
		paramNames []string
		paramVals  []string
		wantHTTP   int
		wantError  string
	}{
		{name: "run benchmark missing user", call: (*BenchmarkHandler).RunBenchmark, method: http.MethodPost, path: "/api/v1/creator/agents/" + validAgent + "/benchmarks", body: `{"skill_id":"data/sql"}`, paramNames: []string{"id"}, paramVals: []string{validAgent}, wantHTTP: http.StatusUnauthorized, wantError: "认证失败"},
		{name: "run benchmark invalid id", call: (*BenchmarkHandler).RunBenchmark, method: http.MethodPost, path: "/api/v1/creator/agents/bad/benchmarks", body: `{"skill_id":"data/sql"}`, userID: validUser, paramNames: []string{"id"}, paramVals: []string{"bad"}, wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "run benchmark invalid json", call: (*BenchmarkHandler).RunBenchmark, method: http.MethodPost, path: "/api/v1/creator/agents/" + validAgent + "/benchmarks", body: `{`, userID: validUser, paramNames: []string{"id"}, paramVals: []string{validAgent}, wantHTTP: http.StatusBadRequest, wantError: "请求体格式错误"},
		{name: "run benchmark validation", call: (*BenchmarkHandler).RunBenchmark, method: http.MethodPost, path: "/api/v1/creator/agents/" + validAgent + "/benchmarks", body: `{}`, userID: validUser, paramNames: []string{"id"}, paramVals: []string{validAgent}, wantHTTP: http.StatusUnprocessableEntity, wantError: "SkillID"},
		{name: "list my scores invalid id", call: (*BenchmarkHandler).ListMyScores, method: http.MethodGet, path: "/api/v1/creator/agents/bad/skill-scores", userID: validUser, paramNames: []string{"id"}, paramVals: []string{"bad"}, wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "get batch invalid batch", call: (*BenchmarkHandler).GetBatch, method: http.MethodGet, path: "/api/v1/creator/agents/" + validAgent + "/benchmarks/bad", userID: validUser, paramNames: []string{"id", "batchID"}, paramVals: []string{validAgent, "bad"}, wantHTTP: http.StatusBadRequest, wantError: "batchID 不是合法 uuid"},
		{name: "scores by slug empty", call: (*BenchmarkHandler).ListScoresBySlug, method: http.MethodGet, path: "/api/v1/agents//skill-scores", paramNames: []string{"slug"}, paramVals: []string{""}, wantHTTP: http.StatusBadRequest, wantError: "slug 不能为空"},
		{name: "top agents empty skill", call: (*BenchmarkHandler).ListTopAgents, method: http.MethodGet, path: "/api/v1/skills//top-agents", paramNames: []string{"id"}, paramVals: []string{""}, wantHTTP: http.StatusBadRequest, wantError: "skill id 不能为空"},
		{name: "batch summaries invalid id", call: (*BenchmarkHandler).ListBatchSummariesPublic, method: http.MethodGet, path: "/api/v1/agents/bad/benchmarks", paramNames: []string{"id"}, paramVals: []string{"bad"}, wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "public batch invalid batch", call: (*BenchmarkHandler).GetBatchPublic, method: http.MethodGet, path: "/api/v1/agents/" + validAgent + "/benchmarks/bad", paramNames: []string{"id", "batchID"}, paramVals: []string{validAgent, "bad"}, wantHTTP: http.StatusBadRequest, wantError: "batchID 不是合法 uuid"},
		{name: "benchmark results invalid id", call: (*BenchmarkHandler).ListBenchmarkResults, method: http.MethodGet, path: "/api/v1/agents/bad/benchmark-results", paramNames: []string{"id"}, paramVals: []string{"bad"}, wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "get batch invalid id before batch", call: (*BenchmarkHandler).GetBatch, method: http.MethodGet, path: "/api/v1/creator/agents/bad/benchmarks/" + validBatch, userID: validUser, paramNames: []string{"id", "batchID"}, paramVals: []string{"bad", validBatch}, wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
	}

	for _, tt := range benchTests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			c := e.NewContext(skillJSONRequest(tt.method, tt.path, tt.body), httptest.NewRecorder())
			if tt.userID != "" {
				c.Set(string(httpx.CtxKeyUserID), tt.userID)
			}
			c.SetParamNames(tt.paramNames...)
			c.SetParamValues(tt.paramVals...)
			err := tt.call(NewBenchmarkHandler(nil), c)
			require.Error(t, err)

			var httpErr *httpx.HTTPError
			require.True(t, errors.As(err, &httpErr))
			require.Equal(t, tt.wantHTTP, httpErr.Status)
			require.Contains(t, httpErr.Message, tt.wantError)
		})
	}
}

func TestSkillAndBenchmarkRoutesAndRuntimeStatus(t *testing.T) {
	e := echo.New()
	NewHandler(nil, nil).Register(e.Group("/api/v1"))
	NewHandler(nil, nil).RegisterProtected(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc { return next })
	bh := NewBenchmarkHandler(NewBenchmarkService(&Service{}, nil, nil))
	bh.Register(e.Group("/api/v1"))
	bh.RegisterProtected(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc { return next })

	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, key := range []string{
		http.MethodGet + " /api/v1/skills",
		http.MethodPost + " /api/v1/skills/proposals",
		http.MethodPatch + " /api/v1/creator/agents/:id/skills",
		http.MethodGet + " /api/v1/creator/skill-proposals",
		http.MethodGet + " /api/v1/benchmark/status",
		http.MethodGet + " /api/v1/skills/:id/top-agents",
		http.MethodGet + " /api/v1/skills/:category/:name/top-agents",
		http.MethodGet + " /api/v1/agents/:slug/skill-scores",
		http.MethodPost + " /api/v1/creator/agents/:id/benchmarks",
		http.MethodGet + " /api/v1/creator/agents/:id/skill-scores",
		http.MethodGet + " /api/v1/creator/agents/:id/benchmarks/:batchID",
	} {
		require.True(t, routes[key], key)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/benchmark/status", nil), rec)
	require.NoError(t, bh.GetRuntimeStatus(c))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "endpoint_runner_unavailable")
}

func skillJSONRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	return req
}

type fakeEndpointRunner struct{}

func (fakeEndpointRunner) DryRun(context.Context, *db.Agent, map[string]interface{}) (map[string]interface{}, string) {
	return map[string]interface{}{"ok": true}, ""
}

type fakeBenchmarkLLM struct {
	response string
	err      error
	user     string
}

func (f *fakeBenchmarkLLM) Complete(_ context.Context, _, user string) (string, error) {
	f.user = user
	return f.response, f.err
}
