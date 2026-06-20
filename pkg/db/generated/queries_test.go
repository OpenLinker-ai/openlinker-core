package db

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestUserQueriesScanRowsAndUseExpectedArgs(t *testing.T) {
	userID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	passwordHash := "hash"
	provider := "github"
	oauthID := "gh-1"
	avatar := "https://cdn.example/avatar.png"
	dbtx := &fakeDBTX{
		row:     fakeRow{values: userRow(userID, now, &passwordHash, &provider, &oauthID, &avatar, nil)},
		execTag: pgconn.NewCommandTag("UPDATE 2"),
	}
	q := New(dbtx)

	created, err := q.CreateUser(context.Background(), CreateUserParams{
		Email:         "user@example.com",
		PasswordHash:  &passwordHash,
		OauthProvider: &provider,
		OauthID:       &oauthID,
		DisplayName:   "Test User",
		AvatarURL:     &avatar,
	})
	if err != nil {
		t.Fatalf("CreateUser error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateUser")
	if created.ID != userID || created.PasswordHash == nil || *created.OauthProvider != provider || !created.IsCreator {
		t.Fatalf("CreateUser scan = %#v", created)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{"user@example.com", &passwordHash, &provider, &oauthID, "Test User", &avatar}) {
		t.Fatalf("CreateUser args = %#v", dbtx.queryRowArgs)
	}

	got, err := q.GetUserByID(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetUserByID error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetUserByID")
	if got.ID != userID || got.Email != "user@example.com" || got.AvatarURL == nil {
		t.Fatalf("GetUserByID scan = %#v", got)
	}

	affected, err := q.UpdateUserBecomeCreator(context.Background(), userID)
	if err != nil || affected != 2 {
		t.Fatalf("UpdateUserBecomeCreator = %d, %v", affected, err)
	}
	requireSQLName(t, dbtx.execSQL, "UpdateUserBecomeCreator")
}

func TestRunQueriesScanRowsAndGuardAffectedRows(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	duration := int32(123)
	finished := now.Add(time.Second)
	output := []byte(`{"ok":true}`)
	runValues := runRow(runID, userID, agentID, []byte(`{"prompt":"hi"}`), output, "success", nil, nil, 100, 25, 75, &duration, now, &finished, "api")
	rows := &fakeRows{rows: [][]any{append(append([]any{}, runValues...), "agent-slug", "Agent Name")}}
	dbtx := &fakeDBTX{
		row:       fakeRow{values: runValues},
		queryRows: rows,
		execTag:   pgconn.NewCommandTag("UPDATE 1"),
	}
	q := New(dbtx)

	created, err := q.CreateRun(context.Background(), CreateRunParams{
		UserID:              userID,
		AgentID:             agentID,
		Input:               []byte(`{"prompt":"hi"}`),
		CostCents:           100,
		PlatformFeeCents:    25,
		CreatorRevenueCents: 75,
		Source:              "api",
	})
	if err != nil {
		t.Fatalf("CreateRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRun")
	if created.ID != runID || created.DurationMs == nil || *created.DurationMs != duration || created.Source != "api" {
		t.Fatalf("CreateRun scan = %#v", created)
	}

	listed, err := q.ListRunsByUserWithAgent(context.Background(), ListRunsByUserWithAgentParams{UserID: userID, Limit: 10, Offset: 5})
	if err != nil {
		t.Fatalf("ListRunsByUserWithAgent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunsByUserWithAgent")
	if !rows.closed {
		t.Fatalf("ListRunsByUserWithAgent should close rows")
	}
	if len(listed) != 1 || listed[0].AgentSlug != "agent-slug" || listed[0].ID != runID {
		t.Fatalf("ListRunsByUserWithAgent scan = %#v", listed)
	}

	if err := q.MarkRunSuccess(context.Background(), MarkRunSuccessParams{ID: runID, Output: output, DurationMs: duration}); err != nil {
		t.Fatalf("MarkRunSuccess affected row error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkRunSuccess")
	if !reflect.DeepEqual(dbtx.execArgs, []any{runID, output, duration}) {
		t.Fatalf("MarkRunSuccess args = %#v", dbtx.execArgs)
	}

	noRows := &fakeDBTX{execTag: pgconn.NewCommandTag("UPDATE 0")}
	if err := New(noRows).MarkRunFailed(context.Background(), MarkRunFailedParams{ID: runID, Status: "failed"}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("MarkRunFailed zero rows error = %v, want pgx.ErrNoRows", err)
	}
}

func TestRuntimePullCountQueryScansScalar(t *testing.T) {
	agentID := uuid.New()
	dbtx := &fakeDBTX{row: fakeRow{values: []any{int32(4)}}}
	count, err := New(dbtx).CountClaimableRuntimePullRuns(context.Background(), agentID)
	if err != nil || count != 4 {
		t.Fatalf("CountClaimableRuntimePullRuns = %d, %v", count, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountClaimableRuntimePullRuns")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID}) {
		t.Fatalf("CountClaimableRuntimePullRuns args = %#v", dbtx.queryRowArgs)
	}
}

func TestAgentQueriesScanRowsAndAffectedRows(t *testing.T) {
	creatorID := uuid.New()
	agentID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	authHeader := "Bearer secret"
	webhookURL := "https://example.com/hook"
	toolName := "search"
	agentValues := agentRow(agentID, creatorID, now, &authHeader, &webhookURL, &toolName)
	rows := &fakeRows{rows: [][]any{agentValues}}
	dbtx := &fakeDBTX{
		row:       fakeRow{values: agentValues},
		queryRows: rows,
		execTag:   pgconn.NewCommandTag("UPDATE 3"),
	}
	q := New(dbtx)

	created, err := q.CreateAgent(context.Background(), CreateAgentParams{
		CreatorID:          creatorID,
		Slug:               "agent-one",
		Name:               "Agent One",
		Description:        "does work",
		EndpointURL:        "https://example.com/agent",
		EndpointAuthHeader: &authHeader,
		PricePerCallCents:  12,
		Tags:               []string{"data"},
		Visibility:         "public",
		ConnectionMode:     "mcp_server",
		MCPToolName:        &toolName,
	})
	if err != nil {
		t.Fatalf("CreateAgent error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateAgent")
	if created.ID != agentID || created.EndpointAuthHeader == nil || *created.MCPToolName != toolName || created.WebhookURL == nil {
		t.Fatalf("CreateAgent scan = %#v", created)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{creatorID, "agent-one", "Agent One", "does work", "https://example.com/agent", &authHeader, int32(12), []string{"data"}, "public", "mcp_server", &toolName}) {
		t.Fatalf("CreateAgent args = %#v", dbtx.queryRowArgs)
	}

	listed, err := q.ListAgentsByCreator(context.Background(), creatorID)
	if err != nil {
		t.Fatalf("ListAgentsByCreator error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentsByCreator")
	if !rows.closed {
		t.Fatalf("ListAgentsByCreator should close rows")
	}
	if len(listed) != 1 || listed[0].ID != agentID || listed[0].Tags[0] != "data" {
		t.Fatalf("ListAgentsByCreator scan = %#v", listed)
	}

	affected, err := q.DisableAgent(context.Background(), DisableAgentParams{ID: agentID, CreatorID: creatorID})
	if err != nil || affected != 3 {
		t.Fatalf("DisableAgent = %d, %v", affected, err)
	}
	requireSQLName(t, dbtx.execSQL, "DisableAgent")
}

func TestSkillQueriesScanRowsAndMatches(t *testing.T) {
	agentID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	skillRows := &fakeRows{rows: [][]any{skillRow("data/sql-query", "data", "SQL", "query data", 7, now)}}
	dbtx := &fakeDBTX{
		row:       fakeRow{values: skillRow("data/sql-query", "data", "SQL", "query data", 7, now)},
		queryRows: skillRows,
	}
	q := New(dbtx)

	skill, err := q.GetSkill(context.Background(), "data/sql-query")
	if err != nil {
		t.Fatalf("GetSkill error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetSkill")
	if skill.ID != "data/sql-query" || skill.SortOrder != 7 || !skill.CreatedAt.Equal(now) {
		t.Fatalf("GetSkill scan = %#v", skill)
	}

	listed, err := q.ListSkills(context.Background())
	if err != nil {
		t.Fatalf("ListSkills error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListSkills")
	if len(listed) != 1 || listed[0].Name != "SQL" {
		t.Fatalf("ListSkills scan = %#v", listed)
	}

	matchRows := &fakeRows{rows: [][]any{{agentID, int32(2), int32(99)}}}
	matchDB := &fakeDBTX{queryRows: matchRows}
	matches, err := New(matchDB).ListAgentsBySkills(context.Background(), []string{"data/sql-query", "data/analysis"})
	if err != nil {
		t.Fatalf("ListAgentsBySkills error = %v", err)
	}
	requireSQLName(t, matchDB.querySQL, "ListAgentsBySkills")
	if len(matches) != 1 || matches[0].AgentID != agentID || matches[0].MatchCount != 2 || matches[0].TotalCalls != 99 {
		t.Fatalf("ListAgentsBySkills scan = %#v", matches)
	}
}

func TestAgentRegistrationTokenQueriesScanRowsAndAffectedRows(t *testing.T) {
	creatorID := uuid.New()
	tokenID := uuid.New()
	expiresAt := time.Date(2026, 6, 20, 13, 0, 0, 0, time.UTC)
	revokedAt := expiresAt.Add(time.Minute)
	lastUsedAt := expiresAt.Add(2 * time.Minute)
	createdAt := expiresAt.Add(-time.Hour)
	tokenValues := registrationTokenRow(tokenID, creatorID, expiresAt, &revokedAt, &lastUsedAt, createdAt)
	rows := &fakeRows{rows: [][]any{tokenValues}}
	dbtx := &fakeDBTX{
		row:       fakeRow{values: tokenValues},
		queryRows: rows,
		execTag:   pgconn.NewCommandTag("UPDATE 1"),
	}
	q := New(dbtx)

	token, err := q.CreateAgentRegistrationToken(context.Background(), CreateAgentRegistrationTokenParams{
		CreatorUserID: creatorID,
		Label:         "bootstrap",
		Prefix:        "rt_live_abcd",
		TokenHash:     "hash",
		MaxAgents:     3,
		ExpiresAt:     expiresAt,
	})
	if err != nil {
		t.Fatalf("CreateAgentRegistrationToken error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateAgentRegistrationToken")
	if token.ID != tokenID || token.RevokedAt == nil || token.LastUsedAt == nil {
		t.Fatalf("CreateAgentRegistrationToken scan = %#v", token)
	}

	listed, err := q.ListAgentRegistrationTokensByCreator(context.Background(), creatorID)
	if err != nil {
		t.Fatalf("ListAgentRegistrationTokensByCreator error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentRegistrationTokensByCreator")
	if len(listed) != 1 || listed[0].TokenHash != "hash" {
		t.Fatalf("ListAgentRegistrationTokensByCreator scan = %#v", listed)
	}

	affected, err := q.RevokeAgentRegistrationTokenForCreator(context.Background(), RevokeAgentRegistrationTokenForCreatorParams{ID: tokenID, CreatorUserID: creatorID})
	if err != nil || affected != 1 {
		t.Fatalf("RevokeAgentRegistrationTokenForCreator = %d, %v", affected, err)
	}
	requireSQLName(t, dbtx.execSQL, "RevokeAgentRegistrationTokenForCreator")

	consumed, err := q.ConsumeAgentRegistrationToken(context.Background(), tokenID)
	if err != nil {
		t.Fatalf("ConsumeAgentRegistrationToken error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ConsumeAgentRegistrationToken")
	if consumed.ID != tokenID || consumed.UsedCount != 1 {
		t.Fatalf("ConsumeAgentRegistrationToken scan = %#v", consumed)
	}
}

func TestRunEventQueriesScanRows(t *testing.T) {
	runID := uuid.New()
	parentID := uuid.New()
	eventID := uuid.New()
	createdAt := time.Date(2026, 6, 20, 14, 0, 0, 0, time.UTC)
	eventValues := runEventRow(eventID, runID, &parentID, 7, "run.message.delta", []byte(`{"text":"hi"}`), createdAt)
	rows := &fakeRows{rows: [][]any{eventValues}}
	dbtx := &fakeDBTX{
		row:       fakeRow{values: eventValues},
		queryRows: rows,
	}
	q := New(dbtx)

	event, err := q.CreateRunEvent(context.Background(), CreateRunEventParams{
		RunID:       runID,
		ParentRunID: &parentID,
		EventType:   "run.message.delta",
		Payload:     []byte(`{"text":"hi"}`),
	})
	if err != nil {
		t.Fatalf("CreateRunEvent error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunEvent")
	if event.ID != eventID || event.ParentRunID == nil || event.Sequence != 7 || string(event.Payload) != `{"text":"hi"}` {
		t.Fatalf("CreateRunEvent scan = %#v", event)
	}

	listed, err := q.ListRunEventsByRun(context.Background(), ListRunEventsByRunParams{RunID: runID, AfterSequence: 3, Limit: 20})
	if err != nil {
		t.Fatalf("ListRunEventsByRun error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunEventsByRun")
	if len(listed) != 1 || listed[0].EventType != "run.message.delta" || !listed[0].CreatedAt.Equal(createdAt) {
		t.Fatalf("ListRunEventsByRun scan = %#v", listed)
	}
}

func TestDeliveryQueriesScanRowsAndAffectedRows(t *testing.T) {
	userID := uuid.New()
	targetID := uuid.New()
	runID := uuid.New()
	deliveryID := uuid.New()
	now := time.Date(2026, 6, 20, 15, 0, 0, 0, time.UTC)
	nextRetry := now.Add(time.Minute)
	status := int32(503)
	body := "temporarily unavailable"
	errMsg := "HTTP 503"
	targetValues := deliveryTargetRow(targetID, userID, now)
	runDeliveryValues := runDeliveryRow(deliveryID, runID, targetID, userID, now, &status, &body, &errMsg, &nextRetry)
	dbtx := &fakeDBTX{
		row:       fakeRow{values: targetValues},
		queryRows: &fakeRows{rows: [][]any{targetValues}},
		execTag:   pgconn.NewCommandTag("UPDATE 2"),
	}
	q := New(dbtx)

	target, err := q.CreateDeliveryTarget(context.Background(), CreateDeliveryTargetParams{
		UserID:    userID,
		Name:      "Primary Hook",
		Type:      "webhook",
		Config:    []byte(`{"url":"https://example.com/hook"}`),
		Secret:    "secret",
		IsDefault: true,
	})
	if err != nil {
		t.Fatalf("CreateDeliveryTarget error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateDeliveryTarget")
	if target.ID != targetID || target.UserID != userID || !target.IsDefault {
		t.Fatalf("CreateDeliveryTarget scan = %#v", target)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{userID, "Primary Hook", "webhook", []byte(`{"url":"https://example.com/hook"}`), "secret", true}) {
		t.Fatalf("CreateDeliveryTarget args = %#v", dbtx.queryRowArgs)
	}

	listed, err := q.ListDeliveryTargetsByUser(context.Background(), userID)
	if err != nil {
		t.Fatalf("ListDeliveryTargetsByUser error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListDeliveryTargetsByUser")
	if len(listed) != 1 || listed[0].Secret != "secret" || !dbtx.queryRows.(*fakeRows).closed {
		t.Fatalf("ListDeliveryTargetsByUser scan = %#v", listed)
	}

	dbtx.row = fakeRow{values: runDeliveryValues}
	createdDelivery, err := q.CreateRunDelivery(context.Background(), CreateRunDeliveryParams{
		RunID:      runID,
		TargetID:   targetID,
		UserID:     userID,
		TargetType: "webhook",
		TargetURL:  "https://example.com/hook",
		Payload:    []byte(`{"event":"run.completed"}`),
	})
	if err != nil {
		t.Fatalf("CreateRunDelivery error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunDelivery")
	if createdDelivery.ID != deliveryID || createdDelivery.ResponseStatus == nil || *createdDelivery.ResponseStatus != status {
		t.Fatalf("CreateRunDelivery scan = %#v", createdDelivery)
	}

	targetSecret := "secret"
	targetConfig := []byte(`{"url":"https://example.com/hook"}`)
	dbtx.row = fakeRow{values: append(append([]any{}, runDeliveryValues...), &targetSecret, targetConfig)}
	gotDelivery, err := q.GetRunDeliveryByID(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetRunDeliveryByID error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetRunDeliveryByID")
	if gotDelivery.ID != deliveryID || gotDelivery.TargetSecret == nil || *gotDelivery.TargetSecret != targetSecret || string(gotDelivery.TargetConfig) != string(targetConfig) {
		t.Fatalf("GetRunDeliveryByID scan = %#v", gotDelivery)
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{runDeliveryValues}}
	pending, err := q.ListPendingRunDeliveries(context.Background())
	if err != nil {
		t.Fatalf("ListPendingRunDeliveries error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListPendingRunDeliveries")
	if len(pending) != 1 || pending[0].ID != deliveryID {
		t.Fatalf("ListPendingRunDeliveries scan = %#v", pending)
	}

	if rows, err := q.DeleteDeliveryTarget(context.Background(), DeleteDeliveryTargetParams{ID: targetID, UserID: userID}); err != nil || rows != 2 {
		t.Fatalf("DeleteDeliveryTarget = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "DeleteDeliveryTarget")
	if rows, err := q.ResetRunDeliveryForRetry(context.Background(), ResetRunDeliveryForRetryParams{ID: deliveryID, UserID: userID}); err != nil || rows != 2 {
		t.Fatalf("ResetRunDeliveryForRetry = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "ResetRunDeliveryForRetry")

	if err := q.MarkRunDeliveryFailedRetry(context.Background(), MarkRunDeliveryFailedRetryParams{
		ID:             deliveryID,
		ResponseStatus: &status,
		ResponseBody:   &body,
		ErrorMessage:   &errMsg,
		NextRetryAt:    nextRetry,
	}); err != nil {
		t.Fatalf("MarkRunDeliveryFailedRetry error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkRunDeliveryFailedRetry")
	if !reflect.DeepEqual(dbtx.execArgs, []any{deliveryID, &status, &body, &errMsg, nextRetry}) {
		t.Fatalf("MarkRunDeliveryFailedRetry args = %#v", dbtx.execArgs)
	}
}

func TestWebhookQueriesScanRowsAndAffectedRows(t *testing.T) {
	creatorID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	deliveryID := uuid.New()
	now := time.Date(2026, 6, 20, 16, 0, 0, 0, time.UTC)
	nextRetry := now.Add(5 * time.Minute)
	status := int32(202)
	body := "accepted"
	errMsg := "retry later"
	url := "https://example.com/webhook"
	secret := "webhook-secret"
	deliveryValues := webhookDeliveryRow(deliveryID, agentID, runID, now, &status, &body, &errMsg, &nextRetry)
	dbtx := &fakeDBTX{
		row:       fakeRow{values: []any{agentID, creatorID, "agent-one", &url, &secret}},
		queryRows: &fakeRows{rows: [][]any{deliveryValues}},
		execTag:   pgconn.NewCommandTag("UPDATE 4"),
	}
	q := New(dbtx)

	if rows, err := q.SetAgentWebhook(context.Background(), SetAgentWebhookParams{ID: agentID, WebhookURL: &url, WebhookSecret: &secret, CreatorID: creatorID}); err != nil || rows != 4 {
		t.Fatalf("SetAgentWebhook = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "SetAgentWebhook")
	if !reflect.DeepEqual(dbtx.execArgs, []any{agentID, &url, &secret, creatorID}) {
		t.Fatalf("SetAgentWebhook args = %#v", dbtx.execArgs)
	}

	cfg, err := q.GetAgentWebhookConfig(context.Background(), agentID)
	if err != nil {
		t.Fatalf("GetAgentWebhookConfig error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentWebhookConfig")
	if cfg.ID != agentID || cfg.WebhookURL == nil || *cfg.WebhookSecret != secret {
		t.Fatalf("GetAgentWebhookConfig scan = %#v", cfg)
	}

	dbtx.row = fakeRow{values: deliveryValues}
	created, err := q.CreateWebhookDelivery(context.Background(), CreateWebhookDeliveryParams{
		AgentID: agentID,
		RunID:   runID,
		URL:     url,
		Payload: []byte(`{"event":"run.completed"}`),
	})
	if err != nil {
		t.Fatalf("CreateWebhookDelivery error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateWebhookDelivery")
	if created.ID != deliveryID || created.ResponseBody == nil || *created.ResponseBody != body {
		t.Fatalf("CreateWebhookDelivery scan = %#v", created)
	}

	dbtx.row = fakeRow{values: append(append([]any{}, deliveryValues...), &secret)}
	got, err := q.GetWebhookDeliveryByID(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetWebhookDeliveryByID error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetWebhookDeliveryByID")
	if got.ID != deliveryID || got.WebhookSecret == nil || *got.WebhookSecret != secret {
		t.Fatalf("GetWebhookDeliveryByID scan = %#v", got)
	}

	pending, err := q.ListPendingDeliveries(context.Background())
	if err != nil {
		t.Fatalf("ListPendingDeliveries error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListPendingDeliveries")
	if len(pending) != 1 || pending[0].ID != deliveryID {
		t.Fatalf("ListPendingDeliveries scan = %#v", pending)
	}

	if err := q.MarkDeliveryFailedFinal(context.Background(), MarkDeliveryFailedFinalParams{ID: deliveryID, ResponseStatus: &status, ResponseBody: &body, ErrorMessage: &errMsg}); err != nil {
		t.Fatalf("MarkDeliveryFailedFinal error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkDeliveryFailedFinal")
	if err := q.MarkDeliveryFailedRetry(context.Background(), MarkDeliveryFailedRetryParams{ID: deliveryID, ResponseStatus: &status, ResponseBody: &body, ErrorMessage: &errMsg, NextRetryAt: nextRetry}); err != nil {
		t.Fatalf("MarkDeliveryFailedRetry error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkDeliveryFailedRetry")
	if !reflect.DeepEqual(dbtx.execArgs, []any{deliveryID, &status, &body, &errMsg, nextRetry}) {
		t.Fatalf("MarkDeliveryFailedRetry args = %#v", dbtx.execArgs)
	}
}

func TestRegistryBridgeQueriesScanRowsAndScalars(t *testing.T) {
	ownerID := uuid.New()
	nodeID := uuid.New()
	listingID := uuid.New()
	agentID := uuid.New()
	linkID := uuid.New()
	now := time.Date(2026, 6, 20, 17, 0, 0, 0, time.UTC)
	baseURL := "https://node.example"
	nodeValues := registryNodeRow(nodeID, ownerID, now, &baseURL)
	linkValues := cloudListingLinkRow(linkID, listingID, nodeID, agentID, now)
	linkRowValues := cloudListingLinkOwnerRow(linkID, listingID, nodeID, agentID, now)
	dbtx := &fakeDBTX{
		row:       fakeRow{values: nodeValues},
		queryRows: &fakeRows{rows: [][]any{nodeValues}},
	}
	q := New(dbtx)

	node, err := q.CreateRegistryNode(context.Background(), CreateRegistryNodeParams{
		OwnerUserID:  ownerID,
		NodeName:     "edge-one",
		NodeType:     "bridge_proxy",
		BaseURL:      &baseURL,
		SecretPrefix: "rn_live_abcd",
		SecretHash:   "hash",
		Scopes:       []string{"heartbeat", "proxy:pull"},
	})
	if err != nil {
		t.Fatalf("CreateRegistryNode error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRegistryNode")
	if node.ID != nodeID || node.BaseURL == nil || len(node.Scopes) != 2 {
		t.Fatalf("CreateRegistryNode scan = %#v", node)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{ownerID, "edge-one", "bridge_proxy", &baseURL, "rn_live_abcd", "hash", []string{"heartbeat", "proxy:pull"}}) {
		t.Fatalf("CreateRegistryNode args = %#v", dbtx.queryRowArgs)
	}

	nodes, err := q.ListRegistryNodesByOwner(context.Background(), ownerID)
	if err != nil {
		t.Fatalf("ListRegistryNodesByOwner error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRegistryNodesByOwner")
	if len(nodes) != 1 || nodes[0].ID != nodeID {
		t.Fatalf("ListRegistryNodesByOwner scan = %#v", nodes)
	}

	dbtx.row = fakeRow{values: linkValues}
	link, err := q.UpsertCloudListingLink(context.Background(), UpsertCloudListingLinkParams{
		CloudListingID:       listingID,
		RegistryNodeID:       nodeID,
		LocalAgentID:         agentID,
		RoutingMode:          "pull_proxy",
		PayloadPolicy:        "metadata_only",
		PayloadRedactionKeys: []string{"secret"},
	})
	if err != nil {
		t.Fatalf("UpsertCloudListingLink error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpsertCloudListingLink")
	if link.ID != linkID || link.SyncStatus != "linked" || link.MetadataSyncedAt == nil {
		t.Fatalf("UpsertCloudListingLink scan = %#v", link)
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{linkRowValues}}
	links, err := q.ListCloudListingLinksByOwner(context.Background(), ownerID)
	if err != nil {
		t.Fatalf("ListCloudListingLinksByOwner error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListCloudListingLinksByOwner")
	if len(links) != 1 || links[0].NodeName != "edge-one" || links[0].AgentSlug != "local-agent" {
		t.Fatalf("ListCloudListingLinksByOwner scan = %#v", links)
	}

	dbtx.row = fakeRow{values: []any{int32(6)}}
	count, err := q.CountPendingProxyRunsByNode(context.Background(), nodeID)
	if err != nil || count != 6 {
		t.Fatalf("CountPendingProxyRunsByNode = %d, %v", count, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountPendingProxyRunsByNode")
}

func TestA2AQueriesScanRowsAndPolicies(t *testing.T) {
	agentID := uuid.New()
	userID := uuid.New()
	tokenID := uuid.New()
	childRunID := uuid.New()
	parentRunID := uuid.New()
	callerAgentID := uuid.New()
	now := time.Date(2026, 6, 20, 18, 0, 0, 0, time.UTC)
	lastUsedAt := now.Add(-time.Minute)
	tokenValues := agentRuntimeTokenRow(tokenID, agentID, userID, now, &lastUsedAt, nil)
	dbtx := &fakeDBTX{
		row:       fakeRow{values: tokenValues},
		queryRows: &fakeRows{rows: [][]any{tokenValues}},
		execTag:   pgconn.NewCommandTag("UPDATE 5"),
	}
	q := New(dbtx)

	token, err := q.CreateAgentRuntimeToken(context.Background(), CreateAgentRuntimeTokenParams{
		AgentID:         agentID,
		CreatedByUserID: userID,
		Name:            "runtime",
		Prefix:          "rt_live_abcd",
		TokenHash:       "hash",
		Scopes:          []string{"agent:pull"},
	})
	if err != nil {
		t.Fatalf("CreateAgentRuntimeToken error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateAgentRuntimeToken")
	if token.ID != tokenID || token.LastUsedAt == nil || token.Scopes[0] != "agent:pull" {
		t.Fatalf("CreateAgentRuntimeToken scan = %#v", token)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, userID, "runtime", "rt_live_abcd", "hash", []string{"agent:pull"}}) {
		t.Fatalf("CreateAgentRuntimeToken args = %#v", dbtx.queryRowArgs)
	}

	listed, err := q.ListAgentRuntimeTokensForOwner(context.Background(), ListAgentRuntimeTokensForOwnerParams{AgentID: agentID, UserID: userID})
	if err != nil {
		t.Fatalf("ListAgentRuntimeTokensForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentRuntimeTokensForOwner")
	if len(listed) != 1 || listed[0].ID != tokenID {
		t.Fatalf("ListAgentRuntimeTokensForOwner scan = %#v", listed)
	}

	dbtx.row = fakeRow{values: []any{int32(3)}}
	count, err := q.CountActiveAgentRuntimeTokens(context.Background(), agentID)
	if err != nil || count != 3 {
		t.Fatalf("CountActiveAgentRuntimeTokens = %d, %v", count, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountActiveAgentRuntimeTokens")

	dbtx.row = fakeRow{values: []any{true}}
	recent, err := q.HasRecentRuntimePullToken(context.Background(), agentID)
	if err != nil || !recent {
		t.Fatalf("HasRecentRuntimePullToken = %v, %v", recent, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "HasRecentRuntimePullToken")

	dbtx.row = fakeRow{values: []any{agentID, "allowlist", now}}
	policy, err := q.UpsertAgentCallPolicyForOwner(context.Background(), UpsertAgentCallPolicyForOwnerParams{
		AgentID:    agentID,
		UserID:     userID,
		CallableBy: "allowlist",
	})
	if err != nil {
		t.Fatalf("UpsertAgentCallPolicyForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpsertAgentCallPolicyForOwner")
	if policy.AgentID != agentID || policy.CallableBy != "allowlist" {
		t.Fatalf("UpsertAgentCallPolicyForOwner scan = %#v", policy)
	}

	dbtx.row = fakeRow{values: runDelegationRow(childRunID, parentRunID, callerAgentID, now)}
	delegation, err := q.CreateRunDelegation(context.Background(), CreateRunDelegationParams{
		ChildRunID:    childRunID,
		ParentRunID:   parentRunID,
		CallerAgentID: callerAgentID,
		Reason:        "delegate analysis",
	})
	if err != nil {
		t.Fatalf("CreateRunDelegation error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunDelegation")
	if delegation.ChildRunID != childRunID || delegation.ParentRunID != parentRunID || delegation.Reason != "delegate analysis" {
		t.Fatalf("CreateRunDelegation scan = %#v", delegation)
	}

	if rows, err := q.RevokeAgentRuntimeTokenForOwner(context.Background(), RevokeAgentRuntimeTokenForOwnerParams{ID: tokenID, UserID: userID}); err != nil || rows != 5 {
		t.Fatalf("RevokeAgentRuntimeTokenForOwner = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "RevokeAgentRuntimeTokenForOwner")
}

func TestWorkflowQueriesScanRowsAndControlUpdates(t *testing.T) {
	userID := uuid.New()
	workflowID := uuid.New()
	nodeID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	now := time.Date(2026, 6, 20, 19, 0, 0, 0, time.UTC)
	finishedAt := now.Add(2 * time.Minute)
	nextRetry := now.Add(time.Minute)
	claimedAt := now.Add(30 * time.Second)
	lastWorkerError := "worker failed"
	workflowValues := workflowRow(workflowID, userID, now)
	nodeValues := workflowNodeRow(nodeID, workflowID, agentID, now)
	runValues := workflowRunRow(runID, workflowID, userID, now, &finishedAt, &nextRetry, &claimedAt, &lastWorkerError)
	dbtx := &fakeDBTX{
		row:       fakeRow{values: workflowValues},
		queryRows: &fakeRows{rows: [][]any{workflowValues}},
		execTag:   pgconn.NewCommandTag("UPDATE 7"),
	}
	q := New(dbtx)

	workflow, err := q.CreateWorkflow(context.Background(), CreateWorkflowParams{
		UserID:      userID,
		Name:        "Review flow",
		Description: "reviews work",
		Edges:       []byte(`[{"from":"a","to":"b"}]`),
	})
	if err != nil {
		t.Fatalf("CreateWorkflow error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateWorkflow")
	if workflow.ID != workflowID || workflow.Status != "active" || string(workflow.Edges) == "" {
		t.Fatalf("CreateWorkflow scan = %#v", workflow)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{userID, "Review flow", "reviews work", []byte(`[{"from":"a","to":"b"}]`)}) {
		t.Fatalf("CreateWorkflow args = %#v", dbtx.queryRowArgs)
	}

	listedWorkflows, err := q.ListWorkflowsByUser(context.Background(), ListWorkflowsByUserParams{UserID: userID, Limit: 20})
	if err != nil {
		t.Fatalf("ListWorkflowsByUser error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListWorkflowsByUser")
	if len(listedWorkflows) != 1 || listedWorkflows[0].ID != workflowID {
		t.Fatalf("ListWorkflowsByUser scan = %#v", listedWorkflows)
	}

	dbtx.row = fakeRow{values: nodeValues}
	node, err := q.CreateWorkflowNode(context.Background(), CreateWorkflowNodeParams{
		WorkflowID: workflowID,
		NodeKey:    "analyze",
		NodeType:   "agent",
		AgentID:    agentID,
		Title:      "Analyze",
		Config:     []byte(`{"temperature":0}`),
		Position:   1,
	})
	if err != nil {
		t.Fatalf("CreateWorkflowNode error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateWorkflowNode")
	if node.ID != nodeID || node.NodeKey != "analyze" || node.AgentID != agentID {
		t.Fatalf("CreateWorkflowNode scan = %#v", node)
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{nodeValues}}
	nodes, err := q.ListWorkflowNodes(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("ListWorkflowNodes error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListWorkflowNodes")
	if len(nodes) != 1 || nodes[0].ID != nodeID {
		t.Fatalf("ListWorkflowNodes scan = %#v", nodes)
	}

	dbtx.row = fakeRow{values: runValues}
	run, err := q.CreatePendingWorkflowRun(context.Background(), CreatePendingWorkflowRunParams{
		WorkflowID:  workflowID,
		UserID:      userID,
		Input:       []byte(`{"prompt":"go"}`),
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("CreatePendingWorkflowRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreatePendingWorkflowRun")
	if run.ID != runID || run.FinishedAt == nil || run.LastWorkerError == nil {
		t.Fatalf("CreatePendingWorkflowRun scan = %#v", run)
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{runValues}}
	runs, err := q.ListWorkflowRunsByWorkflow(context.Background(), ListWorkflowRunsByWorkflowParams{WorkflowID: workflowID, Limit: 10})
	if err != nil {
		t.Fatalf("ListWorkflowRunsByWorkflow error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListWorkflowRunsByWorkflow")
	if len(runs) != 1 || runs[0].ID != runID {
		t.Fatalf("ListWorkflowRunsByWorkflow scan = %#v", runs)
	}

	dbtx.row = fakeRow{values: []any{int32(8)}}
	count, err := q.CountWorkflowRunsByWorkflow(context.Background(), workflowID)
	if err != nil || count != 8 {
		t.Fatalf("CountWorkflowRunsByWorkflow = %d, %v", count, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountWorkflowRunsByWorkflow")

	before := now.Add(-time.Hour)
	if rows, err := q.RequeueStaleWorkflowRuns(context.Background(), before); err != nil || rows != 7 {
		t.Fatalf("RequeueStaleWorkflowRuns = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "RequeueStaleWorkflowRuns")
	if !reflect.DeepEqual(dbtx.execArgs, []any{before}) {
		t.Fatalf("RequeueStaleWorkflowRuns args = %#v", dbtx.execArgs)
	}
}

func userRow(id uuid.UUID, now time.Time, passwordHash, provider, oauthID, avatar *string, deletedAt *time.Time) []any {
	return []any{
		id,
		"user@example.com",
		passwordHash,
		provider,
		oauthID,
		"Test User",
		avatar,
		true,
		true,
		false,
		now,
		now.Add(time.Minute),
		deletedAt,
	}
}

func runRow(
	id, userID, agentID uuid.UUID,
	input, output []byte,
	status string,
	errorCode, errorMessage *string,
	cost, fee, revenue int32,
	duration *int32,
	started time.Time,
	finished *time.Time,
	source string,
) []any {
	return []any{
		id,
		userID,
		agentID,
		input,
		output,
		status,
		errorCode,
		errorMessage,
		cost,
		fee,
		revenue,
		duration,
		started,
		finished,
		source,
	}
}

func agentRow(id, creatorID uuid.UUID, now time.Time, authHeader, webhookURL, toolName *string) []any {
	return []any{
		id,
		creatorID,
		"agent-one",
		"Agent One",
		"does work",
		"https://example.com/agent",
		authHeader,
		int32(12),
		[]string{"data"},
		"active",
		"public",
		"certified",
		nil,
		&now,
		int32(9),
		int64(1234),
		webhookURL,
		"mcp_server",
		toolName,
		now,
		now.Add(time.Minute),
	}
}

func skillRow(id, category, name, description string, sortOrder int32, createdAt time.Time) []any {
	return []any{id, category, name, description, sortOrder, createdAt}
}

func registrationTokenRow(id, creatorID uuid.UUID, expiresAt time.Time, revokedAt, lastUsedAt *time.Time, createdAt time.Time) []any {
	return []any{id, creatorID, "bootstrap", "rt_live_abcd", "hash", int32(3), int32(1), expiresAt, revokedAt, lastUsedAt, createdAt}
}

func runEventRow(id, runID uuid.UUID, parentRunID *uuid.UUID, sequence int32, eventType string, payload []byte, createdAt time.Time) []any {
	return []any{id, runID, parentRunID, sequence, eventType, payload, createdAt}
}

func deliveryTargetRow(id, userID uuid.UUID, now time.Time) []any {
	return []any{id, userID, "Primary Hook", "webhook", []byte(`{"url":"https://example.com/hook"}`), "secret", true, now, now.Add(time.Minute)}
}

func runDeliveryRow(id, runID, targetID, userID uuid.UUID, now time.Time, status *int32, body, errMsg *string, nextRetry *time.Time) []any {
	return []any{
		id,
		runID,
		targetID,
		userID,
		"webhook",
		"https://example.com/hook",
		[]byte(`{"event":"run.completed"}`),
		"pending",
		status,
		body,
		errMsg,
		int32(1),
		nextRetry,
		now,
		now.Add(time.Minute),
	}
}

func webhookDeliveryRow(id, agentID, runID uuid.UUID, now time.Time, status *int32, body, errMsg *string, nextRetry *time.Time) []any {
	return []any{
		id,
		agentID,
		runID,
		"https://example.com/webhook",
		[]byte(`{"event":"run.completed"}`),
		"pending",
		status,
		body,
		errMsg,
		int32(1),
		nextRetry,
		now,
		now.Add(time.Minute),
	}
}

func registryNodeRow(id, ownerID uuid.UUID, now time.Time, baseURL *string) []any {
	return []any{
		id,
		ownerID,
		"edge-one",
		"bridge_proxy",
		baseURL,
		"rn_live_abcd",
		"hash",
		[]string{"heartbeat", "proxy:pull"},
		"healthy",
		&now,
		nil,
		now,
		now.Add(time.Minute),
	}
}

func cloudListingLinkRow(id, listingID, nodeID, agentID uuid.UUID, now time.Time) []any {
	syncedAt := now.Add(30 * time.Second)
	return []any{
		id,
		listingID,
		nodeID,
		agentID,
		"pull_proxy",
		"metadata_only",
		[]string{"secret"},
		"linked",
		"local-agent",
		"Local Agent",
		"does local work",
		[]string{"data"},
		"healthy",
		&syncedAt,
		nil,
		now,
		now,
		now.Add(time.Minute),
	}
}

func cloudListingLinkOwnerRow(id, listingID, nodeID, agentID uuid.UUID, now time.Time) []any {
	link := cloudListingLinkRow(id, listingID, nodeID, agentID, now)
	return []any{
		link[0],
		link[1],
		link[2],
		"edge-one",
		link[3],
		"local-agent",
		"Local Agent",
		link[4],
		link[5],
		link[6],
		link[7],
		"does local work",
		[]string{"data"},
		"healthy",
		link[13],
		link[14],
		link[15],
		link[16],
		link[17],
	}
}

func agentRuntimeTokenRow(id, agentID, userID uuid.UUID, now time.Time, lastUsedAt, revokedAt *time.Time) []any {
	return []any{id, agentID, userID, "runtime", "rt_live_abcd", "hash", []string{"agent:pull"}, lastUsedAt, revokedAt, now}
}

func runDelegationRow(childRunID, parentRunID, callerAgentID uuid.UUID, createdAt time.Time) []any {
	return []any{childRunID, parentRunID, callerAgentID, "delegate analysis", createdAt}
}

func workflowRow(id, userID uuid.UUID, now time.Time) []any {
	return []any{id, userID, "Review flow", "reviews work", "active", []byte(`[{"from":"a","to":"b"}]`), now, now.Add(time.Minute)}
}

func workflowNodeRow(id, workflowID, agentID uuid.UUID, now time.Time) []any {
	return []any{id, workflowID, "analyze", "agent", agentID, "Analyze", []byte(`{"temperature":0}`), int32(1), now}
}

func workflowRunRow(id, workflowID, userID uuid.UUID, now time.Time, finishedAt, nextRetry, claimedAt *time.Time, lastWorkerError *string) []any {
	return []any{
		id,
		workflowID,
		userID,
		"running",
		[]byte(`{"prompt":"go"}`),
		[]byte(`{"ok":true}`),
		nil,
		now,
		finishedAt,
		now,
		now.Add(time.Minute),
		int32(2),
		int32(3),
		nextRetry,
		claimedAt,
		lastWorkerError,
	}
}

type fakeDBTX struct {
	execSQL      string
	execArgs     []any
	execTag      pgconn.CommandTag
	execErr      error
	querySQL     string
	queryArgs    []any
	queryRows    pgx.Rows
	queryErr     error
	queryRowSQL  string
	queryRowArgs []any
	row          pgx.Row
}

func (f *fakeDBTX) Exec(_ context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	f.execSQL = sql
	f.execArgs = append([]any(nil), args...)
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	return f.execTag, nil
}

func (f *fakeDBTX) Query(_ context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	f.querySQL = sql
	f.queryArgs = append([]any(nil), args...)
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return f.queryRows, nil
}

func (f *fakeDBTX) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	f.queryRowSQL = sql
	f.queryRowArgs = append([]any(nil), args...)
	return f.row
}

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scanValues(r.values, dest...)
}

type fakeRows struct {
	rows   [][]any
	idx    int
	err    error
	closed bool
}

func (r *fakeRows) Close() {
	r.closed = true
}

func (r *fakeRows) Err() error {
	return r.err
}

func (r *fakeRows) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag("SELECT 1")
}

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.idx == 0 || r.idx > len(r.rows) {
		return errors.New("scan called before Next")
	}
	return scanValues(r.rows[r.idx-1], dest...)
}

func (r *fakeRows) Values() ([]any, error) {
	if r.idx == 0 || r.idx > len(r.rows) {
		return nil, errors.New("values called before Next")
	}
	return append([]any(nil), r.rows[r.idx-1]...), nil
}

func (r *fakeRows) RawValues() [][]byte {
	return nil
}

func (r *fakeRows) Conn() *pgx.Conn {
	return nil
}

func scanValues(values []any, dest ...any) error {
	if len(values) != len(dest) {
		return errors.New("scan destination count mismatch")
	}
	for i := range dest {
		target := reflect.ValueOf(dest[i])
		if target.Kind() != reflect.Ptr || target.IsNil() {
			return errors.New("scan target must be a non-nil pointer")
		}
		slot := target.Elem()
		if values[i] == nil {
			slot.Set(reflect.Zero(slot.Type()))
			continue
		}
		value := reflect.ValueOf(values[i])
		if value.Type().AssignableTo(slot.Type()) {
			slot.Set(value)
			continue
		}
		if value.Type().ConvertibleTo(slot.Type()) {
			slot.Set(value.Convert(slot.Type()))
			continue
		}
		return errors.New("scan value type mismatch")
	}
	return nil
}

func requireSQLName(t *testing.T, sql, name string) {
	t.Helper()
	if !strings.Contains(sql, "-- name: "+name+" ") {
		t.Fatalf("sql %q does not contain sqlc name %s", sql, name)
	}
}
