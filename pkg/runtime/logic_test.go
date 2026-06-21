package runtime

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
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/config"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestRuntimeAuthScopeAndParsingHelpers(t *testing.T) {
	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/runs/id", nil), httptest.NewRecorder())

	_, err := userIDFromCtx(c)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusUnauthorized, httpErr.Status)

	c.Set(string(httpx.CtxKeyUserID), "not-a-uuid")
	_, err = userIDFromCtx(c)
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, "token 无效", httpErr.Message)

	uid := uuid.New()
	c.Set(string(httpx.CtxKeyUserID), uid.String())
	got, err := userIDFromCtx(c)
	require.NoError(t, err)
	require.Equal(t, uid, got)

	c.Set(string(httpx.CtxKeyAuthMethod), "jwt")
	require.Equal(t, "web", sourceFromCtx(c))
	require.NoError(t, requireAPIKeyScope(c, "agents:run"))
	c.Set(string(httpx.CtxKeyAuthMethod), "unknown")
	require.Equal(t, "web", sourceFromCtx(c))

	c.Set(string(httpx.CtxKeyAuthMethod), "apikey")
	c.Set(string(httpx.CtxKeyAuthScopes), []string{"runs:read"})
	require.Equal(t, "api", sourceFromCtx(c))
	err = requireAPIKeyScope(c, "agents:run")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusForbidden, httpErr.Status)

	token, err := runtimeBearerToken(" Bearer  ol_live_test  ")
	require.NoError(t, err)
	require.Equal(t, "ol_live_test", token)
	token, err = runtimeBearerToken("bearer rt_lower")
	require.NoError(t, err)
	require.Equal(t, "rt_lower", token)
	_, err = runtimeBearerToken("Basic abc")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusUnauthorized, httpErr.Status)
	require.Equal(t, runtimeLimiterTokenKey("secret"), runtimeLimiterTokenKey("secret"))
	require.NotEqual(t, runtimeLimiterTokenKey("secret"), runtimeLimiterTokenKey("other"))
	require.True(t, strings.HasPrefix(runtimeLimiterIPKey(c), "ip:"))
	require.Equal(t, "ip:unknown", runtimeLimiterIPKey(blankIPContext{Context: c}))

	require.Equal(t, 1, retryAfterSeconds(0))
	require.Equal(t, 1, retryAfterSeconds(999*time.Millisecond))
	require.Equal(t, 2, retryAfterSeconds(1500*time.Millisecond))
	require.Equal(t, 3, retryAfterSeconds(3*time.Second))
	rec := httptest.NewRecorder()
	c = e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
	err = runtimeRateLimitError(c, 1500*time.Millisecond, "slow down")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusTooManyRequests, httpErr.Status)
	require.Equal(t, "2", rec.Header().Get(echo.HeaderRetryAfter))

	n, err := parseOptionalInt32("")
	require.NoError(t, err)
	require.Equal(t, int32(0), n)
	n, err = parseOptionalInt32("42")
	require.NoError(t, err)
	require.Equal(t, int32(42), n)
	_, err = parseOptionalInt32("bad")
	require.Error(t, err)

	req := httptest.NewRequest(http.MethodGet, "/runs/id/stream?after_sequence=7", nil)
	c = e.NewContext(req, httptest.NewRecorder())
	n, err = afterSequenceFromSSE(c)
	require.NoError(t, err)
	require.Equal(t, int32(7), n)
	req = httptest.NewRequest(http.MethodGet, "/runs/id/stream", nil)
	req.Header.Set("Last-Event-ID", "9")
	c = e.NewContext(req, httptest.NewRecorder())
	n, err = afterSequenceFromSSE(c)
	require.NoError(t, err)
	require.Equal(t, int32(9), n)
	req = httptest.NewRequest(http.MethodGet, "/runs/id/stream", nil)
	c = e.NewContext(req, httptest.NewRecorder())
	n, err = afterSequenceFromSSE(c)
	require.NoError(t, err)
	require.Equal(t, int32(0), n)
}

func TestRuntimePullAndTimeoutOptionHelpers(t *testing.T) {
	wait, err := runtimePullClaimWait("")
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), wait)
	wait, err = runtimePullClaimWait("5")
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, wait)
	wait, err = runtimePullClaimWait("999")
	require.NoError(t, err)
	require.Equal(t, runtimePullMaxLongPollWait, wait)
	_, err = runtimePullClaimWait("-1")
	require.Error(t, err)
	_, err = runtimePullClaimWait("bad")
	require.Error(t, err)

	require.Equal(t, RuntimePullClaimOptions{}, normalizeRuntimePullClaimOptions())
	require.Equal(t, time.Duration(0), normalizeRuntimePullClaimOptions(RuntimePullClaimOptions{Wait: -time.Second}).Wait)
	require.Equal(t, runtimePullMaxLongPollWait, normalizeRuntimePullClaimOptions(RuntimePullClaimOptions{Wait: time.Hour}).Wait)

	cfg := normalizeRuntimePullRunTimeoutConfig(RuntimePullRunTimeoutConfig{})
	require.Equal(t, 2*time.Minute, cfg.DispatchTimeout)
	require.Equal(t, 15*time.Minute, cfg.ResultTimeout)
	require.Equal(t, int32(50), cfg.BatchSize)
	cfg = normalizeRuntimePullRunTimeoutConfig(RuntimePullRunTimeoutConfig{ResultTimeout: time.Minute})
	require.Equal(t, 2*time.Minute, cfg.DispatchTimeout)
	require.Equal(t, runtimePullClaimTTL, cfg.ResultTimeout)
	require.Equal(t, int32(50), cfg.BatchSize)
	cfg = normalizeRuntimePullRunTimeoutConfig(RuntimePullRunTimeoutConfig{DispatchTimeout: time.Second, ResultTimeout: time.Hour, BatchSize: 3})
	require.Equal(t, time.Second, cfg.DispatchTimeout)
	require.Equal(t, time.Hour, cfg.ResultTimeout)
	require.Equal(t, int32(3), cfg.BatchSize)

	require.True(t, hasRuntimeScope([]string{"agent:pull"}, "agent:pull"))
	require.False(t, hasRuntimeScope([]string{"agent:call"}, "agent:pull"))
	require.True(t, hasAnyRuntimeScope([]string{"agent:call"}, "agent:pull", "agent:call"))
	require.False(t, hasAnyRuntimeScope([]string{"runs:read"}, "agent:pull", "agent:call"))
}

func TestRuntimeMCPAndLowLevelHelpers(t *testing.T) {
	require.Equal(t, map[string]interface{}{}, normalizeMCPResult(nil))
	require.Equal(t, map[string]interface{}{"answer": 1}, normalizeMCPResult(map[string]interface{}{"output": map[string]interface{}{"answer": 1}}))
	require.Equal(t, map[string]interface{}{"summary": "done"}, normalizeMCPResult(map[string]interface{}{"structuredContent": map[string]interface{}{"summary": "done"}}))
	require.Equal(t, map[string]interface{}{"mcp_result": map[string]interface{}{"content": []interface{}{"raw"}}}, normalizeMCPResult(map[string]interface{}{"content": []interface{}{"raw"}}))

	require.Equal(t, "abc", truncate("abcdef", 3))
	require.Equal(t, "ab", truncate("ab", 3))
	require.True(t, isTimeoutErr(fakeTimeoutErr{timeout: true}))
	require.False(t, isTimeoutErr(fakeTimeoutErr{timeout: false}))
	require.False(t, isTimeoutErr(errors.New("plain error")))
}

func TestA2AContextAndRequirementEvidenceHelpers(t *testing.T) {
	runID := uuid.New()
	parentRunID := uuid.New()
	callerAgentID := uuid.New()
	taskID := uuid.New()
	agentID := uuid.New()
	userID := uuid.New()
	svc := NewService(nil, &config.Config{APIURL: " https://api.example.com/ "})

	ctx := svc.agentA2AContext(runID, &Delegation{ParentRunID: parentRunID, CallerAgentID: callerAgentID})
	require.Equal(t, runID.String(), ctx.CurrentRunID)
	require.Equal(t, parentRunID.String(), ctx.ParentRunID)
	require.Equal(t, callerAgentID.String(), ctx.CallerAgentID)
	require.Equal(t, "https://api.example.com/api/v1/agent-runtime/call-agent", ctx.CallAgentEndpoint)
	require.Equal(t, []string{"agent:call"}, ctx.RuntimeScopes)

	m := agentA2AContextMap(ctx)
	require.Equal(t, runID.String(), m["current_run_id"])
	require.Equal(t, parentRunID.String(), m["parent_run_id"])
	require.Nil(t, agentA2AContextMap(nil))
	require.Equal(t, "http://localhost:8080/api/v1/agent-runtime/call-agent", NewService(nil, &config.Config{}).callAgentEndpointURL())

	id, ok, err := taskIDFromRunMetadata(nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uuid.Nil, id)
	id, ok, err = taskIDFromRunMetadata(map[string]interface{}{"task_id": " "})
	require.NoError(t, err)
	require.False(t, ok)
	id, ok, err = taskIDFromRunMetadata(map[string]interface{}{"task_id": taskID.String()})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, taskID, id)
	_, _, err = taskIDFromRunMetadata(map[string]interface{}{"task_id": "bad"})
	require.Error(t, err)

	require.Equal(t, []string{"a", "b"}, uniqueStrings([]string{" a ", "", "b", "a"}))
	require.Equal(t, []string{"run_agent", "get_run"}, normalizeUsedMCPTools(map[string]interface{}{"used_mcp_tools": []interface{}{"run_agent", "get_run", "run_agent"}}, "web"))
	require.Equal(t, []string{"x", "run_agent"}, normalizeUsedMCPTools(map[string]interface{}{"used_mcp_tools": "x"}, "mcp"))
	require.Nil(t, stringListFromMetadata(nil))
	require.Equal(t, []string{"a", "b"}, stringListFromMetadata([]string{"a", "b"}))
	require.Equal(t, []string{" a ", "2", "true"}, stringListFromMetadata([]interface{}{" a ", 2, true}))
	require.Nil(t, stringListFromMetadata(" "))
	require.Equal(t, []string{"1"}, stringListFromMetadata(1))
	matched, missing := splitCoverage([]string{"a", "b"}, []string{"b", "c"})
	require.Equal(t, []string{"b"}, matched)
	require.Equal(t, []string{"a"}, missing)
	require.Equal(t, "no_requirements", coverageStatus(nil, nil, nil, nil, nil, nil))
	require.Equal(t, "covered", coverageStatus([]string{"a"}, []string{"run_agent"}, []string{"a"}, nil, []string{"run_agent"}, nil))
	require.Equal(t, "partial", coverageStatus([]string{"a", "b"}, nil, []string{"a"}, []string{"b"}, nil, nil))
	require.Equal(t, "missing_requirements", coverageStatus([]string{"a"}, nil, nil, []string{"a"}, nil, nil))

	claimedBy := userID
	claimedAgent := agentID
	chosenAgent := agentID
	task := db.TaskQuery{UserID: userID, ClaimedByUserID: &claimedBy, ClaimedAgentID: &claimedAgent}
	require.NoError(t, requireTaskRunAssociation(task, userID, agentID))
	task.ClaimedAgentID = &chosenAgent
	require.Error(t, requireTaskRunAssociation(task, uuid.New(), agentID))
	otherAgent := uuid.New()
	require.Error(t, requireTaskRunAssociation(task, userID, otherAgent))
	task = db.TaskQuery{UserID: userID, ChosenAgentID: &chosenAgent}
	require.NoError(t, requireTaskRunAssociation(task, userID, agentID))
	task = db.TaskQuery{UserID: userID, RecommendedAgentIDs: []uuid.UUID{agentID}}
	require.NoError(t, requireTaskRunAssociation(task, userID, agentID))
	require.True(t, uuidInList(agentID, []uuid.UUID{agentID}))
	require.False(t, uuidInList(agentID, []uuid.UUID{otherAgent}))

	snapshot := &runRequirementSnapshot{
		TaskID:           taskID,
		AgentID:          agentID,
		UserID:           userID,
		RequiredSkills:   []string{"data/sql"},
		RequiredMCPTools: []string{"run_agent"},
		AgentSkills:      []string{"data/sql", "data/analysis"},
		MatchedSkills:    []string{"data/sql"},
		UsedMCPTools:     []string{"run_agent"},
		CoverageStatus:   "covered",
		EvidenceSource:   "mcp",
	}
	params := snapshot.createParams(runID)
	require.Equal(t, runID, params.RunID)
	require.Equal(t, []string{"data/sql"}, params.RequiredSkillIDs)

	created := time.Date(2026, 6, 20, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	evidence := db.RunRequirementEvidence{
		RunID:            runID,
		TaskID:           taskID,
		AgentID:          agentID,
		UserID:           userID,
		RequiredSkillIDs: []string{"data/sql"},
		RequiredMCPTools: []string{"run_agent"},
		AgentSkillIDs:    []string{"data/sql"},
		MatchedSkillIDs:  []string{"data/sql"},
		UsedMCPTools:     []string{"run_agent"},
		CoverageStatus:   "covered",
		EvidenceSource:   "mcp",
		CreatedAt:        created,
	}
	payload := runRequirementEvidencePayload(evidence)
	require.Equal(t, taskID.String(), payload["task_id"])
	require.Equal(t, "2026-06-20T04:00:00Z", payload["evidence_created_at"])
	resp := runRequirementEvidenceToResponse(evidence)
	require.Equal(t, runID.String(), resp.RunID)
	resp.RequiredSkillIDs[0] = "changed"
	require.Equal(t, []string{"data/sql"}, evidence.RequiredSkillIDs)
}

func TestRuntimeRequirementSnapshotQueries(t *testing.T) {
	ctx := context.Background()
	userID := uuid.New()
	agentID := uuid.New()
	taskID := uuid.New()
	q := &fakeRunRequirementQueries{
		task: db.TaskQuery{
			ID:                  taskID,
			UserID:              userID,
			ParsedSkills:        []string{"data/sql", "content/summary", "data/sql"},
			MCPTools:            []string{"run_agent", "files/export", "run_agent"},
			RecommendedAgentIDs: []uuid.UUID{agentID},
		},
		skills: []db.Skill{
			{ID: "data/sql"},
			{ID: "data/analysis"},
			{ID: "data/sql"},
		},
	}
	svc := &Service{requirements: q}
	req := &RunRequest{Metadata: map[string]interface{}{
		"task_id":        taskID.String(),
		"used_mcp_tools": []interface{}{"files/export", "files/export"},
	}}

	snapshot, err := svc.buildRunRequirementSnapshot(ctx, userID, agentID, req, "mcp")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, taskID, q.taskID)
	require.Equal(t, agentID, q.agentID)
	require.Equal(t, []string{"data/sql", "content/summary"}, snapshot.RequiredSkills)
	require.Equal(t, []string{"run_agent", "files/export"}, snapshot.RequiredMCPTools)
	require.Equal(t, []string{"data/sql", "data/analysis"}, snapshot.AgentSkills)
	require.Equal(t, []string{"data/sql"}, snapshot.MatchedSkills)
	require.Equal(t, []string{"content/summary"}, snapshot.MissingSkills)
	require.Equal(t, []string{"files/export", "run_agent"}, snapshot.UsedMCPTools)
	require.Empty(t, snapshot.MissingMCPTools)
	require.Equal(t, "partial", snapshot.CoverageStatus)
	require.Equal(t, "mcp", snapshot.EvidenceSource)

	withoutTask, err := (&Service{requirements: &fakeRunRequirementQueries{}}).buildRunRequirementSnapshot(ctx, userID, agentID, &RunRequest{}, "web")
	require.NoError(t, err)
	require.Nil(t, withoutTask)

	_, err = (&Service{requirements: &fakeRunRequirementQueries{taskErr: pgx.ErrNoRows}}).buildRunRequirementSnapshot(ctx, userID, agentID, req, "web")
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusNotFound, httpErr.Status)

	_, err = (&Service{requirements: &fakeRunRequirementQueries{task: q.task, skillsErr: errors.New("skill store down")}}).buildRunRequirementSnapshot(ctx, userID, agentID, req, "web")
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusInternalServerError, httpErr.Status)
}

func TestRuntimeAttachRequirementEvidenceFromQueries(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	taskID := uuid.New()
	agentID := uuid.New()
	userID := uuid.New()
	created := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	q := &fakeRunRequirementQueries{
		evidence: db.RunRequirementEvidence{
			RunID:            runID,
			TaskID:           taskID,
			AgentID:          agentID,
			UserID:           userID,
			RequiredSkillIDs: []string{"data/sql"},
			MatchedSkillIDs:  []string{"data/sql"},
			CoverageStatus:   "covered",
			EvidenceSource:   "api",
			CreatedAt:        created,
		},
	}
	resp := &RunResponse{}

	(&Service{requirements: q}).attachRunRequirementEvidence(ctx, runID, resp)
	require.Equal(t, runID, q.runID)
	require.NotNil(t, resp.RequirementEvidence)
	require.Equal(t, taskID.String(), resp.RequirementEvidence.TaskID)
	require.Equal(t, "covered", resp.RequirementEvidence.CoverageStatus)

	missing := &RunResponse{}
	(&Service{requirements: &fakeRunRequirementQueries{evidenceErr: pgx.ErrNoRows}}).attachRunRequirementEvidence(ctx, runID, missing)
	require.Nil(t, missing.RequirementEvidence)
	(&Service{requirements: q}).attachRunRequirementEvidence(ctx, runID, nil)
}

func TestRuntimeServiceDependencySettersAndRunWebhookTrigger(t *testing.T) {
	svc := &Service{}
	webhook := &fakeRuntimeWebhookEnqueuer{}
	runWebhook := &recordingRunWebhookEnqueuer{events: make(chan db.RunEvent, 1)}
	delivery := &fakeRuntimeDeliveryEnqueuer{}
	wallet := &fakeRuntimeWalletCharger{}

	svc.SetWebhookEnqueuer(webhook)
	svc.SetRunWebhookEnqueuer(runWebhook)
	svc.SetDeliveryEnqueuer(delivery)
	svc.SetWalletCharger(wallet)
	require.Equal(t, webhook, svc.webhookSvc)
	require.Equal(t, runWebhook, svc.runWebhookSvc)
	require.Equal(t, delivery, svc.deliverySvc)
	require.Equal(t, wallet, svc.walletCharger)

	svc.triggerRunWebhookEvent(nil)
	event := &db.RunEvent{ID: uuid.New(), RunID: uuid.New(), EventType: "run.completed"}
	svc.triggerRunWebhookEvent(event)
	select {
	case got := <-runWebhook.events:
		require.Equal(t, event.ID, got.ID)
		require.Equal(t, event.RunID, got.RunID)
	case <-time.After(time.Second):
		t.Fatal("run webhook event was not enqueued")
	}
}

func TestRuntimeResponseAndNextActionHelpers(t *testing.T) {
	runID := uuid.New()
	agentID := uuid.New()
	duration := int32(123)
	errCode := "AGENT_ERROR"
	errMsg := "failed"
	outputJSON := []byte(`{"answer":42,"next_action":{"label":"Ship","hint":"Deploy it","href":"/deploy","method":"POST","resource_type":"task","resource_id":"t1","type":"deploy"}}`)

	success := runToResponse(&db.Run{ID: runID, Status: "success", Output: outputJSON, CostCents: 99, DurationMs: &duration, Source: "mcp"})
	require.Equal(t, "success", success.Status)
	require.Equal(t, int32(99), success.CostCents)
	require.Equal(t, float64(42), success.Output["answer"])
	require.Equal(t, "deploy", success.NextAction.Type)
	require.Equal(t, "Ship", success.NextAction.Label)

	failed := runToResponse(&db.Run{ID: runID, Status: "timeout", CostCents: 99, ErrorCode: &errCode, ErrorMessage: &errMsg, DurationMs: &duration})
	require.Equal(t, int32(0), failed.CostCents)
	require.Equal(t, "检查超时并重试", failed.NextAction.Label)
	require.Equal(t, "AGENT_ERROR", failed.NextAction.AdditionalProps["error_code"])

	running := runToResponse(&db.Run{ID: runID, Status: "running", CostCents: 12})
	require.Equal(t, "wait", running.NextAction.Type)
	require.Contains(t, running.NextAction.Href, runID.String())

	delegated := nextActionForSuccess(map[string]interface{}{"next_action": "ignored"}, runID.String(), "free_delegation")
	require.Equal(t, "return_to_parent", delegated.Type)
	require.Equal(t, runID.String(), delegated.ResourceID)
	require.Equal(t, "review_output", nextActionForSuccess(nil, "", "").Type)
	suggested, ok := nextActionFromOutput(map[string]interface{}{"next_action": " Review the result "})
	require.True(t, ok)
	require.Equal(t, "agent_suggested", suggested.Type)
	require.Equal(t, "Review the result", suggested.Hint)
	described, ok := nextActionFromOutput(map[string]interface{}{"next_action": map[string]interface{}{"description": "Use result", "method": 123}})
	require.True(t, ok)
	require.Equal(t, "执行 Agent 建议", described.Label)
	require.Equal(t, "Use result", described.Hint)
	require.Equal(t, "123", described.Method)
	emptyAction, ok := nextActionFromOutput(map[string]interface{}{"next_action": map[string]interface{}{"label": " "}})
	require.False(t, ok)
	require.Nil(t, emptyAction)
	require.Nil(t, nextActionFromUnsupportedOutput(t))
	resp := &RunResponse{Status: "canceled"}
	decorateNextAction(resp)
	require.Nil(t, resp.NextAction)
	decorateNextAction(nil)
	require.Equal(t, "fallback", coalesceString(" ", "fallback"))
	require.Equal(t, "value", coalesceString(" value ", "fallback"))
	require.Equal(t, "7", stringFromMap(map[string]interface{}{"count": 7}, "count"))
	require.Equal(t, "", stringPtrValue(nil))
	require.Equal(t, "value", *stringPtrOrNil(" value "))
	require.Nil(t, stringPtrOrNil(" "))

	wait := runtimePullWaitingNextAction("run-1", agentID)
	require.Equal(t, "start_runtime_worker", wait.Type)
	require.Equal(t, agentID.String(), wait.AdditionalProps["agent_id"])
}

func TestRuntimeArtifactMessageAndEventHelpers(t *testing.T) {
	runID := uuid.New()
	artifactID := uuid.New()
	created := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	source := "artifact-1"
	mimeType := "application/json"
	fileURI := "s3://bucket/file.json"
	fileName := "file.json"
	fileSHA := strings.Repeat("a", 64)
	size := int64(42)

	artifact := runArtifactToResponse(db.RunArtifact{
		ID:               artifactID,
		RunID:            runID,
		ArtifactType:     "file",
		Title:            "File",
		Content:          []byte(`{"ok":true}`),
		Visibility:       "shared",
		SourceArtifactID: &source,
		MimeType:         &mimeType,
		FileUri:          &fileURI,
		FileName:         &fileName,
		FileSha256:       &fileSHA,
		FileSizeBytes:    &size,
		CreatedAt:        created,
	})
	require.Equal(t, artifactID.String(), artifact.ID)
	require.Equal(t, true, artifact.Content["ok"])
	require.Equal(t, fileURI, artifact.FileURI)
	require.Equal(t, &size, artifact.FileSizeBytes)
	require.Equal(t, map[string]interface{}{"raw": "not-json"}, runArtifactToResponse(db.RunArtifact{Content: []byte("not-json")}).Content)

	seq := int32(3)
	message := runMessageToResponse(db.RunMessage{ID: uuid.New(), RunID: runID, EventSequence: &seq, Role: "agent", Content: "hello", Payload: []byte(`{"text":"hello"}`), CreatedAt: created})
	require.Equal(t, &seq, message.EventSequence)
	require.Equal(t, "hello", message.Payload["text"])
	require.Equal(t, map[string]interface{}{"raw": "bad"}, runMessageToResponse(db.RunMessage{Payload: []byte("bad")}).Payload)

	parentID := uuid.New()
	event := runEventToResponse(db.RunEvent{ID: uuid.New(), RunID: runID, ParentRunID: &parentID, Sequence: 9, EventType: "run.completed", Payload: []byte(`{"status":"success"}`), CreatedAt: created})
	require.Equal(t, parentID.String(), event.ParentRunID)
	require.Equal(t, "success", event.Payload["status"])
	require.Empty(t, runEventToResponse(db.RunEvent{Payload: []byte("bad")}).Payload)

	require.Equal(t, "hello", messageContentFromMap(map[string]interface{}{"text": " hello "}))
	require.Equal(t, `{"value":3}`, messageContentFromMap(map[string]interface{}{"value": 3}))
	require.Equal(t, "", messageContentFromMap(nil))
	require.Len(t, []rune(truncateRunMessageContent(strings.Repeat("数", maxRunMessageContentLen+1))), maxRunMessageContentLen)
	require.True(t, constantTimeEqual("secret", "secret"))
	require.False(t, constantTimeEqual("secret", "other"))
	require.False(t, constantTimeEqual("secret", "secret2"))
}

func TestRuntimeArtifactDraftHelpers(t *testing.T) {
	size := int64(123)
	fileSHA := strings.Repeat("b", 64)
	draft := artifactDraftFromMap(map[string]interface{}{
		"title":           "  Report  ",
		"type":            "file",
		"visibility":      "public_example",
		"content":         map[string]interface{}{"text": "hello"},
		"file_uri":        "https://example.com/report.pdf",
		"file_name":       "report.pdf",
		"mime_type":       "application/pdf",
		"file_sha256":     fileSHA,
		"file_size_bytes": float64(size),
	}, "fallback")
	require.Equal(t, "file", draft.ArtifactType)
	require.Equal(t, "Report", draft.Title)
	require.Equal(t, "public_example", draft.Visibility)
	require.Equal(t, "https://example.com/report.pdf", draft.FileURI)
	require.Equal(t, size, *draft.FileSizeBytes)
	require.Equal(t, size, draft.Content["file_size_bytes"])

	items := runArtifactsFromOutput(map[string]interface{}{"artifacts": []interface{}{map[string]interface{}{"title": "A"}, "raw"}})
	require.Len(t, items, 2)
	require.Equal(t, "A", items[0].Title)
	require.Equal(t, map[string]interface{}{"value": "raw"}, items[1].Content)
	require.Equal(t, "Agent 输出", runArtifactsFromOutput(map[string]interface{}{"answer": 1})[0].Title)
	explicitParts := []interface{}{"part-a", map[string]interface{}{"text": "part-b"}}
	require.Equal(t, explicitParts, artifactDeltaPartsFromPayload(map[string]interface{}{"parts": explicitParts}))
	require.Equal(t, []interface{}{map[string]interface{}{"type": "data", "data": map[string]interface{}{"value": 1}}}, artifactDeltaPartsFromPayload(map[string]interface{}{"content": map[string]interface{}{"value": 1}}))
	require.Equal(t, []interface{}{map[string]interface{}{"type": "data", "data": []interface{}{"x"}}}, artifactDeltaPartsFromPayload(map[string]interface{}{"data": []interface{}{"x"}}))

	delta := artifactDeltaDraftFromPayload(map[string]interface{}{
		"artifact_id": "stream-1",
		"text":        "hello",
		"append":      false,
		"last_chunk":  true,
		"file_uri":    "https://example.com/out.txt",
	})
	require.Equal(t, "stream-1", delta.SourceArtifactID)
	require.Equal(t, "file", delta.ArtifactType)
	require.False(t, delta.Append)
	require.True(t, delta.LastChunk)
	require.Equal(t, []interface{}{map[string]interface{}{"type": "text", "text": "hello"}}, delta.Parts)

	seq := int32(7)
	partsSHA := "parts"
	payloadSHA := "payload"
	declaredSHA := "declared"
	content := mergeArtifactDeltaContent(nil, delta, db.RunArtifactChunk{
		EventSequence:  &seq,
		ChunkIndex:     2,
		Append:         false,
		LastChunk:      true,
		PartsSha256:    &partsSHA,
		PayloadSha256:  &payloadSHA,
		DeclaredSha256: &declaredSHA,
		ChecksumStatus: "verified",
	})
	require.Equal(t, "stream-1", content["artifact_id"])
	require.Equal(t, "hello", content["text"])
	require.Equal(t, "verified", content["last_checksum_status"])

	require.Equal(t, []interface{}{"x"}, interfaceSliceFromAny("x"))
	require.Equal(t, []interface{}{"x", "y"}, interfaceSliceFromAny([]interface{}{"x", "y"}))
	require.Equal(t, []interface{}{}, interfaceSliceFromAny(nil))
	require.Equal(t, "ab", artifactTextFromParts([]interface{}{"a", map[string]interface{}{"text": "b"}}))
	declared, status := artifactChunkChecksum(map[string]interface{}{"parts_sha256": sha256Hex([]byte("parts"))}, sha256Hex([]byte("parts")))
	require.Equal(t, sha256Hex([]byte("parts")), declared)
	require.Equal(t, "verified", status)
	declared, status = artifactChunkChecksum(map[string]interface{}{"parts_sha256": strings.Repeat("d", 64)}, strings.Repeat("e", 64))
	require.Equal(t, strings.Repeat("d", 64), declared)
	require.Equal(t, "mismatch", status)
	_, status = artifactChunkChecksum(map[string]interface{}{}, "x")
	require.Equal(t, "not_provided", status)
	_, status = artifactChunkChecksum(map[string]interface{}{"parts_sha256": "bad"}, "x")
	require.Equal(t, "invalid", status)
	require.Equal(t, strings.Repeat("a", 64), normalizeSHA256(strings.ToUpper(strings.Repeat("a", 64))))
	require.Equal(t, "", normalizeSHA256(strings.Repeat("g", 64)))
	require.Equal(t, "", normalizeSHA256("bad"))
	require.Equal(t, "ab", normalizeArtifactMetadataString("abcd", 2))
	partsMeta := artifactFileMetadataFromParts([]interface{}{
		"skip",
		map[string]interface{}{
			"file": map[string]interface{}{
				"uri":      "https://files.example/export.csv",
				"mimeType": "text/csv",
				"size":     int32(64),
			},
		},
		map[string]interface{}{
			"file_name": "export.csv",
			"checksum":  strings.Repeat("c", 64),
		},
	})
	require.Equal(t, "text/csv", partsMeta.MimeType)
	require.Equal(t, "https://files.example/export.csv", partsMeta.FileURI)
	require.Equal(t, "export.csv", partsMeta.FileName)
	require.Equal(t, strings.Repeat("c", 64), partsMeta.FileSHA256)
	require.NotNil(t, partsMeta.FileSizeBytes)
	require.Equal(t, int64(64), *partsMeta.FileSizeBytes)
	sizeFromInt64, ok := firstArtifactInt64(map[string]interface{}{"size": int64(9)}, "size")
	require.True(t, ok)
	require.Equal(t, int64(9), sizeFromInt64)
	sizeFromInt, ok := firstArtifactInt64(map[string]interface{}{"size": 8}, "size")
	require.True(t, ok)
	require.Equal(t, int64(8), sizeFromInt)
	sizeFromFloat64, ok := firstArtifactInt64(map[string]interface{}{"size": float64(6.9)}, "size")
	require.True(t, ok)
	require.Equal(t, int64(6), sizeFromFloat64)
	sizeFromFloat, ok := firstArtifactInt64(map[string]interface{}{"bad": -1, "size": float32(7.8)}, "bad", "size")
	require.True(t, ok)
	require.Equal(t, int64(7), sizeFromFloat)
	_, ok = firstArtifactInt64(map[string]interface{}{"size": int32(-1)}, "size")
	require.False(t, ok)
	_, ok = firstArtifactInt64(map[string]interface{}{"size": "7"}, "size")
	require.False(t, ok)
	require.Equal(t, "default", normalizeArtifactSourceID(""))
	require.Equal(t, "Agent 产物", normalizeArtifactTitle(""))
	require.Len(t, []rune(normalizeArtifactSourceID(strings.Repeat("s", 201))), 200)
	require.Len(t, []rune(normalizeArtifactTitle(strings.Repeat("t", 201))), 200)
	require.Equal(t, "fallback", coalesceArtifactString(map[string]interface{}{"x": " "}, "x", "fallback"))
	require.True(t, validArtifactType("json"))
	require.False(t, validArtifactType("html"))
	require.True(t, validArtifactVisibility("shared"))
	require.False(t, validArtifactVisibility("world"))
}

func TestRuntimeSSEAndHandlerValidation(t *testing.T) {
	rec := httptest.NewRecorder()
	err := writeSSEStreamError(rec, errors.New("boom"))
	require.NoError(t, err)
	require.Contains(t, rec.Body.String(), "run.stream.error")
	require.True(t, isTerminalRunEvent("run.failed"))
	require.True(t, isTerminalRunEvent("run.canceled"))
	require.False(t, isTerminalRunEvent("run.started"))

	validUser := uuid.NewString()
	validRun := uuid.NewString()
	validAgent := uuid.NewString()
	tests := []struct {
		name      string
		call      func(*Handler, echo.Context) error
		method    string
		path      string
		body      string
		userID    string
		auth      string
		scopes    []string
		paramID   string
		query     string
		wantHTTP  int
		wantError string
	}{
		{name: "post run invalid json", call: (*Handler).PostRun, method: http.MethodPost, path: "/api/v1/run", body: `{`, userID: validUser, auth: "jwt", wantHTTP: http.StatusBadRequest, wantError: "请求体格式错误"},
		{name: "post run validation", call: (*Handler).PostRun, method: http.MethodPost, path: "/api/v1/run", body: `{}`, userID: validUser, auth: "jwt", wantHTTP: http.StatusUnprocessableEntity, wantError: "AgentID"},
		{name: "post async missing scope", call: (*Handler).PostRunAsync, method: http.MethodPost, path: "/api/v1/runs", body: `{"agent_id":"` + validAgent + `","input":{"x":1}}`, userID: validUser, auth: "apikey", scopes: []string{"runs:read"}, wantHTTP: http.StatusForbidden, wantError: "agents:run"},
		{name: "get run invalid id", call: (*Handler).GetRun, method: http.MethodGet, path: "/api/v1/runs/bad", userID: validUser, auth: "jwt", paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "events bad after sequence", call: (*Handler).GetRunEvents, method: http.MethodGet, path: "/api/v1/runs/" + validRun + "/events", userID: validUser, auth: "jwt", paramID: validRun, query: "after_sequence=bad", wantHTTP: http.StatusBadRequest, wantError: "after_sequence"},
		{name: "events bad limit", call: (*Handler).GetRunEvents, method: http.MethodGet, path: "/api/v1/runs/" + validRun + "/events", userID: validUser, auth: "jwt", paramID: validRun, query: "limit=bad", wantHTTP: http.StatusBadRequest, wantError: "limit"},
		{name: "artifacts invalid id", call: (*Handler).GetRunArtifacts, method: http.MethodGet, path: "/api/v1/runs/bad/artifacts", userID: validUser, auth: "jwt", paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "messages invalid id", call: (*Handler).GetRunMessages, method: http.MethodGet, path: "/api/v1/runs/bad/messages", userID: validUser, auth: "jwt", paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "stream invalid last event", call: (*Handler).StreamRunEvents, method: http.MethodGet, path: "/api/v1/runs/" + validRun + "/stream", userID: validUser, auth: "jwt", paramID: validRun, query: "after_sequence=bad", wantHTTP: http.StatusBadRequest, wantError: "Last-Event-ID"},
		{name: "report event invalid id", call: (*Handler).PostRunEvent, method: http.MethodPost, path: "/api/v1/runs/bad/events", body: `{"event_type":"run.message.delta"}`, paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "runtime result invalid id", call: (*Handler).PostRuntimePullResult, method: http.MethodPost, path: "/api/v1/agent-runtime/runs/bad/result", body: `{"status":"success"}`, auth: "runtime", paramID: "bad", wantHTTP: http.StatusBadRequest, wantError: "id 不是合法 uuid"},
		{name: "runtime result validation", call: (*Handler).PostRuntimePullResult, method: http.MethodPost, path: "/api/v1/agent-runtime/runs/" + validRun + "/result", body: `{}`, auth: "runtime", paramID: validRun, wantHTTP: http.StatusUnprocessableEntity, wantError: "Status"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			target := tt.path
			if tt.query != "" {
				target += "?" + tt.query
			}
			c := e.NewContext(runtimeJSONRequest(tt.method, target, tt.body), httptest.NewRecorder())
			if tt.userID != "" {
				c.Set(string(httpx.CtxKeyUserID), tt.userID)
			}
			switch tt.auth {
			case "jwt":
				c.Set(string(httpx.CtxKeyAuthMethod), "jwt")
			case "apikey":
				c.Set(string(httpx.CtxKeyAuthMethod), "apikey")
				c.Set(string(httpx.CtxKeyAuthScopes), tt.scopes)
			case "runtime":
				c.Request().Header.Set(echo.HeaderAuthorization, "Bearer rt_live_validtoken")
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

func TestRuntimeRoutes(t *testing.T) {
	e := echo.New()
	h := NewHandler(nil)
	h.RegisterProtected(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc { return next }, func(next echo.HandlerFunc) echo.HandlerFunc { return next })
	h.RegisterAgentRuntime(e.Group("/api/v1"))

	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, key := range []string{
		http.MethodPost + " /api/v1/run",
		http.MethodPost + " /api/v1/runs",
		http.MethodGet + " /api/v1/runs/:id",
		http.MethodGet + " /api/v1/runs/:id/events",
		http.MethodGet + " /api/v1/runs/:id/artifacts",
		http.MethodGet + " /api/v1/runs/:id/messages",
		http.MethodGet + " /api/v1/runs/:id/stream",
		http.MethodPost + " /api/v1/runs/:id/events",
		http.MethodPost + " /api/v1/agent-runtime/heartbeat",
		http.MethodGet + " /api/v1/agent-runtime/runs/claim",
		http.MethodPost + " /api/v1/agent-runtime/runs/:id/result",
	} {
		require.True(t, routes[key], key)
	}
}

func nextActionFromUnsupportedOutput(t *testing.T) *RunNextAction {
	t.Helper()
	action, ok := nextActionFromOutput(map[string]interface{}{"next_action": 42})
	require.False(t, ok)
	return action
}

func runtimeJSONRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	return req
}

func TestRuntimeJSONHelperIsValid(t *testing.T) {
	req := runtimeJSONRequest(http.MethodPost, "/", `{"ok":true}`)
	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
	require.Equal(t, true, body["ok"])
}

type fakeRunRequirementQueries struct {
	task      db.TaskQuery
	taskID    uuid.UUID
	taskErr   error
	skills    []db.Skill
	agentID   uuid.UUID
	skillsErr error

	evidence    db.RunRequirementEvidence
	runID       uuid.UUID
	evidenceErr error
}

func (q *fakeRunRequirementQueries) GetTaskQuery(_ context.Context, taskID uuid.UUID) (db.TaskQuery, error) {
	q.taskID = taskID
	return q.task, q.taskErr
}

func (q *fakeRunRequirementQueries) ListAgentSkills(_ context.Context, agentID uuid.UUID) ([]db.Skill, error) {
	q.agentID = agentID
	return q.skills, q.skillsErr
}

func (q *fakeRunRequirementQueries) GetRunRequirementEvidenceByRun(_ context.Context, runID uuid.UUID) (db.RunRequirementEvidence, error) {
	q.runID = runID
	return q.evidence, q.evidenceErr
}

type fakeRuntimeWebhookEnqueuer struct{}

func (f *fakeRuntimeWebhookEnqueuer) EnqueueDelivery(context.Context, *db.Run, string, map[string]interface{}) error {
	return nil
}

type recordingRunWebhookEnqueuer struct {
	events chan db.RunEvent
}

func (r *recordingRunWebhookEnqueuer) EnqueueRunEvent(_ context.Context, event db.RunEvent) error {
	r.events <- event
	return nil
}

type fakeRuntimeDeliveryEnqueuer struct{}

func (f *fakeRuntimeDeliveryEnqueuer) EnqueueIfDefault(context.Context, *db.Run) error {
	return nil
}

type fakeRuntimeWalletCharger struct{}

func (f *fakeRuntimeWalletCharger) Charge(context.Context, pgx.Tx, uuid.UUID, int64) (bool, error) {
	return true, nil
}

func (f *fakeRuntimeWalletCharger) CreditCreator(context.Context, pgx.Tx, uuid.UUID, int64) error {
	return nil
}

func (f *fakeRuntimeWalletCharger) Refund(context.Context, pgx.Tx, uuid.UUID, int64) error {
	return nil
}

type blankIPContext struct {
	echo.Context
}

func (blankIPContext) RealIP() string {
	return " "
}

type fakeTimeoutErr struct {
	timeout bool
}

func (e fakeTimeoutErr) Error() string {
	return "timeout"
}

func (e fakeTimeoutErr) Timeout() bool {
	return e.timeout
}
