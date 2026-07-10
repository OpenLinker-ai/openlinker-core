package task

import (
	"bytes"
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
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestTaskActionHonorsResourceSpecificGrant(t *testing.T) {
	userID := uuid.New()
	targetTaskID := uuid.New()
	otherTaskID := uuid.New()
	newContext := func(grantedTaskID uuid.UUID) echo.Context {
		e := echo.New()
		c := e.NewContext(
			taskJSONRequest(http.MethodPost, "/api/v1/tasks/"+targetTaskID.String()+"/publish", `{`),
			httptest.NewRecorder(),
		)
		c.SetParamNames("id")
		c.SetParamValues(targetTaskID.String())
		auth.SetPrincipal(c, &auth.AuthPrincipal{
			UserID: userID, AuthMethod: auth.AuthMethodUserToken,
			Grants: []auth.Grant{{
				Permission: "tasks:publish", ResourceType: "task", ResourceID: &grantedTaskID,
				Constraints: json.RawMessage(`{}`),
			}},
		})
		return c
	}

	err := NewHandler(nil).Publish(newContext(otherTaskID))
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, httpx.CodePermissionDenied, httpErr.Code)

	err = NewHandler(nil).Publish(newContext(targetTaskID))
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusBadRequest, httpErr.Status)
	require.Equal(t, "请求体格式错误", httpErr.Message)
}

func TestTokenizeAndRuleParse(t *testing.T) {
	tokens := tokenize("SQL2 R2 API a I 7 é 数据分析 中, SQL2!")
	require.ElementsMatch(t, []string{"sql2", "r2", "api", "é", "数据分析", "数", "据", "分", "析", "中"}, tokens)

	skills := []db.Skill{
		{ID: "data/sql", Name: "SQL Query", Description: "schema analysis"},
		{ID: "dev/review", Name: "Code Review", Description: "bug risk review"},
		{ID: "content/summary", Name: "Summary", Description: "meeting notes"},
		{ID: "data/chart", Name: "Chart", Description: "analysis dashboard sql"},
	}

	got := ruleParse("sql review analysis", skills)
	require.Equal(t, []string{"data/sql", "dev/review", "data/chart"}, got)
	require.Empty(t, ruleParse("unknown", skills))
	require.Empty(t, ruleParse("sql", nil))
	require.Empty(t, ruleParse("", skills))

	require.Empty(t, ruleParse("I need an Agent for a missing capability", []db.Skill{
		{ID: "ops/web-scraping", Name: "网页抓取", Description: "抓取站点 / API / 监控 / 价格追踪"},
		{ID: "data/sql-query", Name: "SQL 查询", Description: "自然语言转 SQL、慢查询优化、schema 解读"},
	}))
}

func TestLLMParsingBuildsPromptFiltersAndLimits(t *testing.T) {
	skills := []db.Skill{
		{ID: "known-1", Name: "One", Description: "first"},
		{ID: "known-2", Name: "Two", Description: "second"},
		{ID: "known-3", Name: "Three", Description: "third"},
		{ID: "known-4", Name: "Four", Description: "fourth"},
	}

	system := buildLLMSystem(skills)
	require.Contains(t, system, "严格只输出 JSON")
	require.Contains(t, system, "known-1: One")

	ids, err := parseLLMResp("prefix ```json\n{\"skills\":[\"known-1\",\"known-2\"]}\n``` suffix")
	require.NoError(t, err)
	require.Equal(t, []string{"known-1", "known-2"}, ids)
	ids, err = parseLLMResp("```json\n{\"skills\":[\"known-3\"]}\n```")
	require.NoError(t, err)
	require.Equal(t, []string{"known-3"}, ids)
	ids, err = parseLLMResp("```\n{\"skills\":[\"known-4\"]}\n```")
	require.NoError(t, err)
	require.Equal(t, []string{"known-4"}, ids)
	ids, err = parseLLMResp(`{"other":[]}`)
	require.NoError(t, err)
	require.Empty(t, ids)
	_, err = parseLLMResp("no json here")
	require.Error(t, err)
	_, err = parseLLMResp(`{"skills":`)
	require.Error(t, err)
	_, err = parseLLMResp(`{"skills":[}`)
	require.Error(t, err)

	client := &fakeLLMClient{response: `{"skills":["known-1","unknown","known-2","known-3","known-4"]}`}
	ids, err = llmParse(context.Background(), client, "route this", skills)
	require.NoError(t, err)
	require.Equal(t, []string{"known-1", "known-2", "known-3"}, ids)
	require.Contains(t, client.system, "known-4")
	require.Equal(t, "用户任务：route this", client.user)

	_, err = llmParse(context.Background(), &fakeLLMClient{response: `{"skills":["unknown"]}`}, "route", skills)
	require.Error(t, err)
	_, err = llmParse(context.Background(), &fakeLLMClient{err: errors.New("offline")}, "route", skills)
	require.Error(t, err)
}

func TestNormalizeAndMergeReferences(t *testing.T) {
	byID := skillCatalogByID([]db.Skill{
		{ID: "data/sql", Name: "SQL"},
		{ID: "dev/review", Name: "Review"},
	})

	got, err := normalizeExplicitSkillIDs([]string{" data/sql ", "data/sql", "dev/review"}, byID)
	require.NoError(t, err)
	require.Equal(t, []string{"data/sql", "dev/review"}, got)
	_, err = normalizeExplicitSkillIDs([]string{""}, byID)
	require.Error(t, err)
	got, err = normalizeExplicitSkillIDs([]string{"ai/missing-capability"}, byID)
	require.NoError(t, err)
	require.Equal(t, []string{"ai/missing-capability"}, got)
	_, err = normalizeExplicitSkillIDs([]string{"Missing Skill"}, byID)
	require.Error(t, err)
	_, err = normalizeExplicitSkillIDs([]string{"a", "b", "c", "d", "e", "f"}, byID)
	require.Error(t, err)

	require.Equal(t, []string{"a", "b", "c"}, mergeSkillIDs([]string{"a", "b"}, []string{"b", "c", "d"}, 3))
	require.Empty(t, mergeSkillIDs([]string{"a"}, []string{"b"}, 0))

	tools, err := normalizeMCPTools([]string{" create_task ", "run_agent", "run_agent"})
	require.NoError(t, err)
	require.Equal(t, []string{"create_task", "run_agent"}, tools)
	_, err = normalizeMCPTools([]string{""})
	require.Error(t, err)
	_, err = normalizeMCPTools([]string{"missing"})
	require.Error(t, err)
	_, err = normalizeMCPTools([]string{"a", "b", "c", "d", "e", "f"})
	require.Error(t, err)

	refs := mcpToolRefsForNames([]string{"run_agent", "missing", "get_run"})
	require.Equal(t, []string{"run_agent", "get_run"}, []string{refs[0].Name, refs[1].Name})
	require.Contains(t, mcpToolCatalogByName(), "create_task")
}

func TestPublicSummaryVisibilityAndTemplateHelpers(t *testing.T) {
	summary, err := normalizePublicSummary(&PublishRequest{PublicSummary: "  hello \n world  "}, "private")
	require.NoError(t, err)
	require.Equal(t, "hello world", summary)
	_, err = normalizePublicSummary(&PublishRequest{PublicSummary: "abc"}, "fallback")
	require.Error(t, err)

	long := strings.Repeat("数", 260)
	summary, err = normalizePublicSummary(nil, long)
	require.NoError(t, err)
	require.Len(t, []rune(summary), maxTaskPublicSummaryLen)
	require.Equal(t, "a b c", compactWhitespace("  a\tb\nc  "))
	require.Equal(t, "数据", truncateRunes("数据分析", 2))
	require.Equal(t, "", truncateRunes("abc", 0))

	public := "公开摘要"
	owner := uuid.New()
	worker := uuid.New()
	task := db.TaskQuery{UserID: owner, Query: "private query", Visibility: taskVisibilityPublic, PublicSummary: &public}
	require.Equal(t, "公开摘要", publicTaskSummary(&task))
	require.Equal(t, "公开摘要", runnableTaskText(&task, worker))
	require.Equal(t, "private query", runnableTaskText(&task, owner))
	require.Equal(t, "公开摘要", workResponseQuery(&task))
	task.PublicSummary = nil
	require.Equal(t, "private query", publicTaskSummary(&task))
	task.Visibility = taskVisibilityPrivate
	require.Equal(t, "private query", workResponseQuery(&task))
	require.Equal(t, taskVisibilityPrivate, normalizedTaskVisibility(""))
	require.Equal(t, taskVisibilityPublic, normalizedTaskVisibility(taskVisibilityPublic))

	svc := &Service{allSkills: []db.Skill{{ID: "content/summarization", Category: "content", Name: "摘要"}}}
	tmpl, err := svc.taskTemplateByID(context.Background(), "support-review")
	require.NoError(t, err)
	require.NotNil(t, tmpl)
	resp := taskTemplateResponse(*tmpl, skillCatalogByID(svc.allSkills))
	require.Equal(t, "support-review", resp.ID)
	require.Equal(t, taskVisibilityPrivate, resp.DefaultVisibility)
	require.NotSame(t, &tmpl.RequiredSkillIDs[0], &resp.RequiredSkillIDs[0])
	require.Equal(t, mergeSkillIDs(tmpl.RequiredSkillIDs, []string{"extra"}, maxTaskSkillRefs), mergeTemplateSkillIDs(tmpl, []string{"extra"}))
	require.Equal(t, []string{"extra"}, mergeTemplateSkillIDs(nil, []string{"extra"}))
	require.Equal(t, []string{"run_agent"}, mergeTemplateMCPTools(&taskTemplate{RequiredMCPTools: []string{"run_agent"}}, nil))
	require.Equal(t, []string{"get_agent"}, mergeTemplateMCPTools(nil, []string{"get_agent"}))
	require.Equal(t,
		[]string{"run_agent", "search_agents", "get_agent", "create_task", "get_run"},
		mergeTemplateMCPTools(
			&taskTemplate{RequiredMCPTools: []string{"run_agent", "search_agents"}},
			[]string{"run_agent", "get_agent", "create_task", "get_run", "ignored"},
		),
	)
	empty, err := svc.taskTemplateByID(context.Background(), " ")
	require.NoError(t, err)
	require.Nil(t, empty)
	_, err = svc.taskTemplateByID(context.Background(), "missing")
	require.Error(t, err)

	items, err := svc.ListTaskTemplates(context.Background())
	require.NoError(t, err)
	require.Len(t, items, len(taskTemplateCatalog))
}

func TestTaskDTOHelpers(t *testing.T) {
	created := time.Date(2026, 6, 20, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	later := created.Add(30 * time.Minute)
	taskID := uuid.New()
	owner := uuid.New()
	agentID := uuid.New()
	worker := uuid.New()
	runID := uuid.New()
	summary := "done"
	revision := "please adjust"
	publicSummary := "public task"
	artifact := []byte(`{"answer":42}`)

	tq := db.TaskQuery{
		ID:                  taskID,
		UserID:              owner,
		Query:               "private task",
		ParsedSkills:        []string{"data/sql", "missing"},
		MCPTools:            []string{"run_agent"},
		RecommendedAgentIDs: []uuid.UUID{agentID},
		ChosenAgentID:       &agentID,
		ChosenAt:            &later,
		ClaimedAgentID:      &agentID,
		ClaimedByUserID:     &worker,
		ClaimedAt:           &later,
		CompletedAt:         &later,
		CompletionSummary:   &summary,
		CompletionRunID:     &runID,
		DeliveryStatus:      "revision_requested",
		DeliveryVisibility:  "shared",
		DeliveryArtifact:    artifact,
		RevisionRequestedAt: &later,
		RevisionNote:        &revision,
		Visibility:          taskVisibilityPublic,
		PublicSummary:       &publicSummary,
		PublishedAt:         &later,
		CreatedAt:           created,
	}

	require.Equal(t, "revision_requested", taskStatus(&tq))
	require.Equal(t, "private", normalizeDeliveryVisibility(""))
	require.Equal(t, "shared", normalizeDeliveryVisibility(" shared "))
	require.Equal(t, "public_example", normalizeDeliveryVisibility("public_example"))
	require.Equal(t, []string{"run_agent"}, taskRunUsedMCPTools([]string{"search_agents", "run_agent", "run_agent"}))
	require.Empty(t, taskRunUsedMCPTools([]string{" search_agents ", " get_run "}))
	require.Nil(t, deliveryArtifact(nil))
	require.Nil(t, deliveryArtifact([]byte(`not-json`)))
	require.Equal(t, DeliveryArtifact{"answer": float64(42)}, deliveryArtifact(artifact))

	history := toHistoryItem(&tq)
	require.Equal(t, taskID.String(), history.ID)
	require.Equal(t, "revision_requested", history.Status)
	require.Equal(t, []string{agentID.String()}, history.RecommendedAgentIDs)
	require.Equal(t, "2026-06-20T02:00:00Z", history.CreatedAt)
	require.Equal(t, "2026-06-20T02:30:00Z", *history.ChosenAt)
	require.Equal(t, "shared", history.DeliveryVisibility)
	require.Equal(t, &revision, history.RevisionNote)

	publicItem := toPublicTaskItem(&tq, skillCatalogByID([]db.Skill{{ID: "data/sql", Category: "data", Name: "SQL", Description: "query"}}))
	require.Equal(t, "public task", publicItem.Query)
	require.Equal(t, "public task", publicItem.PublicSummary)
	require.Equal(t, 1, publicItem.RecommendedAgentCount)
	require.Equal(t, "SQL", publicItem.ParsedSkillRefs[0].Name)
	require.Equal(t, "revision_requested", publicItem.Status)

	work := toWorkResponse(&tq, agentID)
	require.Equal(t, taskID.String(), work.TaskID)
	require.Equal(t, "public task", work.Query)
	require.Equal(t, agentID.String(), work.AgentID)
	require.Equal(t, runID.String(), *work.CompletionRunID)
	require.Equal(t, "shared", work.DeliveryVisibility)

	require.Equal(t, "accepted", taskStatus(&db.TaskQuery{DeliveryStatus: "accepted"}))
	require.Equal(t, "completed", taskStatus(&db.TaskQuery{CompletedAt: &later}))
	require.Equal(t, "in_progress", taskStatus(&db.TaskQuery{ClaimedAgentID: &agentID}))
	require.Equal(t, "matched", taskStatus(&db.TaskQuery{ChosenAgentID: &agentID}))
	require.Equal(t, "needs_agent", taskStatus(&db.TaskQuery{}))
	require.Equal(t, "open", taskStatus(&db.TaskQuery{RecommendedAgentIDs: []uuid.UUID{agentID}}))

	action := nextActionForNeedsAgent(taskID, "no_public_agent", "no agent")
	require.Equal(t, "publish_task", action.Type)
	require.Equal(t, "no_public_agent", action.ReasonCode)
	require.Contains(t, action.Href, taskID.String())

	ptr := copyStringPtr(&summary)
	require.NotSame(t, &summary, ptr)
	require.Equal(t, summary, *ptr)
	require.Nil(t, copyStringPtr(nil))

	agent := toAgentSummary(&db.GetAgentsByIDsRow{
		Agent: db.Agent{
			ID:                agentID,
			Slug:              "agent",
			Name:              "Agent",
			Description:       "desc",
			PricePerCallCents: 12,
			TotalCalls:        3,
			Tags:              []string{"data"},
		},
		CreatorName: "Creator",
	})
	require.Equal(t, "agent", agent.Slug)
	require.Equal(t, []string{"data"}, agent.Tags)
	agent.Tags[0] = "changed"
	require.Equal(t, []string{"data"}, toAgentSummary(&db.GetAgentsByIDsRow{Agent: db.Agent{Tags: []string{"data"}}}).Tags)
	require.Equal(t, "匹配 SQL + missing", buildWhy([]SkillRef{{ID: "data/sql", Name: "SQL"}, {ID: "missing"}}))
	require.Equal(t, "", buildWhy(nil))
	require.Equal(t, []SkillRef{{ID: "missing", Name: "missing"}}, skillRefsForIDs([]string{"missing"}, nil))
}

func TestTaskServiceConstructionAndSkillLoading(t *testing.T) {
	svc := NewService(nil, nil, nil)
	require.NotNil(t, svc)
	require.NotNil(t, svc.queries)
	require.Nil(t, svc.runner)

	runner := &fakeTaskRuntimeStarter{}
	svc.SetRunStarter(runner)
	require.Same(t, runner, svc.runner)

	cachedSkill := db.Skill{ID: "cached", Name: "Cached"}
	svc.allSkills = []db.Skill{cachedSkill}
	got, err := svc.skills(context.Background())
	require.NoError(t, err)
	require.Equal(t, []db.Skill{cachedSkill}, got)

	loadedSkill := db.Skill{ID: "loaded", Name: "Loaded"}
	recommender := &fakeTaskSkillRecommender{skills: []db.Skill{loadedSkill}}
	svc = &Service{skillSvc: recommender}
	got, err = svc.skills(context.Background())
	require.NoError(t, err)
	require.Equal(t, []db.Skill{loadedSkill}, got)
	require.Equal(t, 1, recommender.listCalls)
	require.Equal(t, []db.Skill{loadedSkill}, svc.allSkills)

	refreshedSkill := db.Skill{ID: "refreshed", Name: "Refreshed"}
	recommender = &fakeTaskSkillRecommender{skills: []db.Skill{refreshedSkill}}
	svc = &Service{
		skillSvc:          recommender,
		allSkills:         []db.Skill{cachedSkill},
		allSkillsLoadedAt: time.Now().Add(-skillCatalogTTL - time.Second),
	}
	got, err = svc.skills(context.Background())
	require.NoError(t, err)
	require.Equal(t, []db.Skill{refreshedSkill}, got)
	require.Equal(t, 1, recommender.listCalls)

	recommender = &fakeTaskSkillRecommender{err: errors.New("offline")}
	svc = &Service{
		skillSvc:          recommender,
		allSkills:         []db.Skill{cachedSkill},
		allSkillsLoadedAt: time.Now().Add(-skillCatalogTTL - time.Second),
	}
	got, err = svc.skills(context.Background())
	require.NoError(t, err)
	require.Equal(t, []db.Skill{cachedSkill}, got)

	svc = &Service{skillSvc: &fakeTaskSkillRecommender{err: errors.New("offline")}}
	_, err = svc.skills(context.Background())
	require.Error(t, err)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusInternalServerError, httpErr.Status)
}

func TestTaskUserIDFromCtx(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/tasks/me", nil), httptest.NewRecorder())

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
}

func TestTaskHandlersValidateBeforeServiceDispatch(t *testing.T) {
	validUser := uuid.NewString()
	validID := uuid.NewString()
	validAgent := uuid.NewString()

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
		{name: "recommend missing user", call: (*Handler).Recommend, method: http.MethodPost, path: "/api/v1/tasks/recommend", body: `{"query":"valid query"}`, wantHTTP: http.StatusUnauthorized, wantError: "认证失败"},
		{name: "recommend invalid json", call: (*Handler).Recommend, method: http.MethodPost, path: "/api/v1/tasks/recommend", body: `{`, userID: validUser, wantHTTP: http.StatusBadRequest, wantError: "请求体格式错误"},
		{name: "recommend validation", call: (*Handler).Recommend, method: http.MethodPost, path: "/api/v1/tasks/recommend", body: `{"query":"abc"}`, userID: validUser, wantHTTP: http.StatusUnprocessableEntity, wantError: "Query"},
		{name: "choose invalid task id", call: (*Handler).Choose, method: http.MethodPost, path: "/api/v1/tasks/bad/choose", body: `{"agent_id":"` + validAgent + `"}`, userID: validUser, paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "choose invalid body", call: (*Handler).Choose, method: http.MethodPost, path: "/api/v1/tasks/" + validID + "/choose", body: `{`, userID: validUser, paramID: validID, wantHTTP: http.StatusBadRequest, wantError: "请求体格式错误"},
		{name: "choose validation", call: (*Handler).Choose, method: http.MethodPost, path: "/api/v1/tasks/" + validID + "/choose", body: `{}`, userID: validUser, paramID: validID, wantHTTP: http.StatusUnprocessableEntity, wantError: "AgentID"},
		{name: "publish invalid id", call: (*Handler).Publish, method: http.MethodPost, path: "/api/v1/tasks/bad/publish", body: `{}`, userID: validUser, paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "claim validation", call: (*Handler).Claim, method: http.MethodPost, path: "/api/v1/tasks/" + validID + "/claim", body: `{}`, userID: validUser, paramID: validID, wantHTTP: http.StatusUnprocessableEntity, wantError: "AgentID"},
		{name: "complete validation", call: (*Handler).Complete, method: http.MethodPost, path: "/api/v1/tasks/" + validID + "/complete", body: `{}`, userID: validUser, paramID: validID, wantHTTP: http.StatusUnprocessableEntity, wantError: "AgentID"},
		{name: "run validation", call: (*Handler).Run, method: http.MethodPost, path: "/api/v1/tasks/" + validID + "/run", body: `{}`, userID: validUser, paramID: validID, wantHTTP: http.StatusUnprocessableEntity, wantError: "AgentID"},
		{name: "accept invalid id", call: (*Handler).Accept, method: http.MethodPost, path: "/api/v1/tasks/bad/accept", userID: validUser, paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "revision validation", call: (*Handler).RequestRevision, method: http.MethodPost, path: "/api/v1/tasks/" + validID + "/revision", body: `{}`, userID: validUser, paramID: validID, wantHTTP: http.StatusUnprocessableEntity, wantError: "Note"},
		{name: "get by id invalid id", call: (*Handler).GetByID, method: http.MethodGet, path: "/api/v1/tasks/bad", userID: validUser, paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "list mine missing user", call: (*Handler).ListMine, method: http.MethodGet, path: "/api/v1/tasks/me?limit=100", wantHTTP: http.StatusUnauthorized, wantError: "认证失败"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := taskJSONRequest(tt.method, tt.path, tt.body)
			c := e.NewContext(req, httptest.NewRecorder())
			if tt.userID != "" {
				c.Set(string(httpx.CtxKeyUserID), tt.userID)
			}
			if tt.paramID != "" {
				c.SetParamNames("id")
				c.SetParamValues(tt.paramID)
			}

			err := tt.call(NewHandler(nil), c)
			require.Error(t, err)

			var httpErr *httpx.HTTPError
			require.True(t, errors.As(err, &httpErr), "expected *httpx.HTTPError, got %T", err)
			require.Equal(t, tt.wantHTTP, httpErr.Status)
			require.Contains(t, httpErr.Message, tt.wantError)
		})
	}
}

func TestTaskHandlerRoutesAndTemplates(t *testing.T) {
	e := echo.New()
	h := NewHandler(&Service{allSkills: []db.Skill{{ID: "content/summarization", Category: "content", Name: "摘要"}}})
	h.RegisterProtected(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc { return next })

	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, key := range []string{
		http.MethodGet + " /api/v1/tasks/board",
		http.MethodGet + " /api/v1/task-templates",
		http.MethodPost + " /api/v1/tasks/recommend",
		http.MethodPost + " /api/v1/tasks/:id/choose",
		http.MethodPost + " /api/v1/tasks/:id/publish",
		http.MethodPost + " /api/v1/tasks/:id/claim",
		http.MethodPost + " /api/v1/tasks/:id/run",
		http.MethodPost + " /api/v1/tasks/:id/complete",
		http.MethodPost + " /api/v1/tasks/:id/accept",
		http.MethodPost + " /api/v1/tasks/:id/revision",
		http.MethodGet + " /api/v1/tasks/me",
		http.MethodGet + " /api/v1/tasks/:id",
	} {
		require.True(t, routes[key], key)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/task-templates", nil), rec)
	require.NoError(t, h.ListTaskTemplates(c))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "support-review")
}

func taskJSONRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	return req
}

type fakeLLMClient struct {
	response string
	err      error
	system   string
	user     string
}

func (f *fakeLLMClient) Complete(_ context.Context, system, user string) (string, error) {
	f.system = system
	f.user = user
	return f.response, f.err
}

type fakeTaskSkillRecommender struct {
	skills    []db.Skill
	err       error
	listCalls int
}

func (f *fakeTaskSkillRecommender) ListAll(context.Context) ([]db.Skill, error) {
	f.listCalls++
	if f.err != nil {
		return nil, f.err
	}
	return append([]db.Skill{}, f.skills...), nil
}

func (f *fakeTaskSkillRecommender) RecommendAgentsBySkills(context.Context, []string, int) ([]AgentMatch, error) {
	return nil, nil
}

type fakeTaskRuntimeStarter struct{}

func (f *fakeTaskRuntimeStarter) StartRun(context.Context, uuid.UUID, *runtime.RunRequest, string) (*runtime.RunResponse, error) {
	return nil, nil
}
