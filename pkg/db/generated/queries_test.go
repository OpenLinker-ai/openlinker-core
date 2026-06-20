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

func TestCapabilityMessageArtifactQueriesScanRowsAndArgs(t *testing.T) {
	creatorID := uuid.New()
	agentID := uuid.New()
	capabilityID := uuid.New()
	exampleID := uuid.New()
	runID := uuid.New()
	messageID := uuid.New()
	artifactID := uuid.New()
	chunkID := uuid.New()
	now := time.Date(2026, 6, 20, 20, 0, 0, 0, time.UTC)
	dryRunErr := "schema mismatch"
	dryRunAt := now.Add(time.Minute)
	eventSequence := int32(7)
	sourceArtifactID := "report-1"
	mimeType := "application/json"
	fileURI := "https://files.example/run-report.json"
	fileName := "run-report.json"
	fileSHA := strings.Repeat("a", 64)
	fileSize := int64(128)
	partsSHA := strings.Repeat("b", 64)
	payloadSHA := strings.Repeat("c", 64)
	declaredSHA := strings.Repeat("d", 64)
	inputSchema := []byte(`{"type":"object","required":["prompt"]}`)
	outputSchema := []byte(`{"type":"object","required":["answer"]}`)
	exampleInput := []byte(`{"prompt":"hi"}`)
	exampleOutput := []byte(`{"answer":"ok"}`)
	messagePayload := []byte(`{"tool":"writer"}`)
	artifactContent := []byte(`{"answer":"ok"}`)
	chunkParts := []byte(`[{"text":"ok"}]`)
	chunkPayload := []byte(`{"delta":"ok"}`)

	capabilityValues := agentCapabilityRow(capabilityID, agentID, now, inputSchema, outputSchema)
	exampleValues := agentExampleRow(exampleID, agentID, now, exampleInput, exampleOutput)
	onboardingValues := agentOnboardingStatusRow(agentID, now, &dryRunErr, &dryRunAt)
	messageValues := runMessageRow(messageID, runID, &eventSequence, messagePayload, now)
	artifactValues := runArtifactRow(
		artifactID,
		runID,
		now,
		artifactContent,
		&sourceArtifactID,
		&mimeType,
		&fileURI,
		&fileName,
		&fileSHA,
		&fileSize,
	)
	chunkValues := runArtifactChunkRow(
		chunkID,
		runID,
		artifactID,
		now,
		sourceArtifactID,
		&eventSequence,
		chunkParts,
		chunkPayload,
		&partsSHA,
		&payloadSHA,
		&declaredSHA,
	)
	dbtx := &fakeDBTX{
		row:     fakeRow{values: capabilityValues},
		execTag: pgconn.NewCommandTag("UPDATE 9"),
	}
	q := New(dbtx)

	capability, err := q.UpsertAgentCapability(context.Background(), UpsertAgentCapabilityParams{
		AgentID:      agentID,
		CreatorID:    creatorID,
		InputSchema:  inputSchema,
		OutputSchema: outputSchema,
		Summary:      "A2A output contract",
	})
	if err != nil {
		t.Fatalf("UpsertAgentCapability error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpsertAgentCapability")
	if capability.ID != capabilityID || capability.Version != 3 || string(capability.InputSchema) == "" {
		t.Fatalf("UpsertAgentCapability scan = %#v", capability)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, creatorID, inputSchema, outputSchema, "A2A output contract"}) {
		t.Fatalf("UpsertAgentCapability args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: capabilityValues}
	if got, err := q.GetAgentCapabilityByAgentID(context.Background(), agentID); err != nil || got.ID != capabilityID {
		t.Fatalf("GetAgentCapabilityByAgentID = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentCapabilityByAgentID")

	dbtx.row = fakeRow{values: capabilityValues}
	if got, err := q.GetAgentCapabilityBySlug(context.Background(), "agent-one"); err != nil || got.AgentID != agentID {
		t.Fatalf("GetAgentCapabilityBySlug = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentCapabilityBySlug")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{"agent-one"}) {
		t.Fatalf("GetAgentCapabilityBySlug args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: exampleValues}
	example, err := q.CreateAgentExample(context.Background(), CreateAgentExampleParams{
		AgentID:            agentID,
		CreatorID:          creatorID,
		Title:              "happy path",
		InputJSON:          exampleInput,
		ExpectedOutputJSON: exampleOutput,
		SortOrder:          4,
	})
	if err != nil {
		t.Fatalf("CreateAgentExample error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateAgentExample")
	if example.ID != exampleID || example.SortOrder != 4 || string(example.ExpectedOutputJSON) == "" {
		t.Fatalf("CreateAgentExample scan = %#v", example)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, creatorID, "happy path", exampleInput, exampleOutput, int32(4)}) {
		t.Fatalf("CreateAgentExample args = %#v", dbtx.queryRowArgs)
	}

	exampleRows := &fakeRows{rows: [][]any{exampleValues}}
	dbtx.queryRows = exampleRows
	examples, err := q.ListAgentExamplesByAgentID(context.Background(), agentID)
	if err != nil {
		t.Fatalf("ListAgentExamplesByAgentID error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentExamplesByAgentID")
	if !exampleRows.closed || len(examples) != 1 || examples[0].ID != exampleID {
		t.Fatalf("ListAgentExamplesByAgentID scan = %#v closed=%v", examples, exampleRows.closed)
	}

	slugRows := &fakeRows{rows: [][]any{exampleValues}}
	dbtx.queryRows = slugRows
	examplesBySlug, err := q.ListAgentExamplesBySlug(context.Background(), "agent-one")
	if err != nil {
		t.Fatalf("ListAgentExamplesBySlug error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentExamplesBySlug")
	if !slugRows.closed || len(examplesBySlug) != 1 || examplesBySlug[0].AgentID != agentID {
		t.Fatalf("ListAgentExamplesBySlug scan = %#v closed=%v", examplesBySlug, slugRows.closed)
	}

	dbtx.row = fakeRow{values: []any{int32(2)}}
	count, err := q.CountAgentExamplesByAgentID(context.Background(), agentID)
	if err != nil || count != 2 {
		t.Fatalf("CountAgentExamplesByAgentID = %d, %v", count, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountAgentExamplesByAgentID")

	dbtx.row = fakeRow{values: []any{exampleInput}}
	firstInput, err := q.GetFirstExampleInputByAgentID(context.Background(), agentID)
	if err != nil || string(firstInput) != string(exampleInput) {
		t.Fatalf("GetFirstExampleInputByAgentID = %s, %v", firstInput, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetFirstExampleInputByAgentID")

	if rows, err := q.DeleteAgentExampleForOwner(context.Background(), DeleteAgentExampleForOwnerParams{ID: exampleID, AgentID: agentID, CreatorID: creatorID}); err != nil || rows != 9 {
		t.Fatalf("DeleteAgentExampleForOwner = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "DeleteAgentExampleForOwner")

	if err := q.EnsureOnboardingStatus(context.Background(), agentID); err != nil {
		t.Fatalf("EnsureOnboardingStatus error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "EnsureOnboardingStatus")
	if !reflect.DeepEqual(dbtx.execArgs, []any{agentID}) {
		t.Fatalf("EnsureOnboardingStatus args = %#v", dbtx.execArgs)
	}

	dbtx.row = fakeRow{values: onboardingValues}
	onboarding, err := q.GetOnboardingStatusForOwner(context.Background(), GetOnboardingStatusForOwnerParams{AgentID: agentID, CreatorID: creatorID})
	if err != nil {
		t.Fatalf("GetOnboardingStatusForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetOnboardingStatusForOwner")
	if onboarding.AgentID != agentID || onboarding.DryRunError == nil || *onboarding.DryRunError != dryRunErr {
		t.Fatalf("GetOnboardingStatusForOwner scan = %#v", onboarding)
	}

	if rows, err := q.MarkCapabilitiesSet(context.Background(), agentID); err != nil || rows != 9 {
		t.Fatalf("MarkCapabilitiesSet = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkCapabilitiesSet")

	if rows, err := q.MarkExamplesSet(context.Background(), MarkExamplesSetParams{AgentID: agentID, ExamplesSet: true}); err != nil || rows != 9 {
		t.Fatalf("MarkExamplesSet = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkExamplesSet")
	if !reflect.DeepEqual(dbtx.execArgs, []any{agentID, true}) {
		t.Fatalf("MarkExamplesSet args = %#v", dbtx.execArgs)
	}

	if rows, err := q.UpdateDryRunResult(context.Background(), UpdateDryRunResultParams{AgentID: agentID, Result: "fail", Error: &dryRunErr}); err != nil || rows != 9 {
		t.Fatalf("UpdateDryRunResult = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "UpdateDryRunResult")
	if !reflect.DeepEqual(dbtx.execArgs, []any{agentID, "fail", &dryRunErr}) {
		t.Fatalf("UpdateDryRunResult args = %#v", dbtx.execArgs)
	}

	dbtx.row = fakeRow{values: messageValues}
	message, err := q.CreateRunMessage(context.Background(), CreateRunMessageParams{
		RunID:         runID,
		EventSequence: &eventSequence,
		Role:          "agent",
		Content:       "done",
		Payload:       messagePayload,
	})
	if err != nil {
		t.Fatalf("CreateRunMessage error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunMessage")
	if message.ID != messageID || message.EventSequence == nil || *message.EventSequence != eventSequence {
		t.Fatalf("CreateRunMessage scan = %#v", message)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{runID, &eventSequence, "agent", "done", messagePayload}) {
		t.Fatalf("CreateRunMessage args = %#v", dbtx.queryRowArgs)
	}

	messageRows := &fakeRows{rows: [][]any{messageValues}}
	dbtx.queryRows = messageRows
	messages, err := q.ListRunMessagesByRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListRunMessagesByRun error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunMessagesByRun")
	if !messageRows.closed || len(messages) != 1 || messages[0].ID != messageID {
		t.Fatalf("ListRunMessagesByRun scan = %#v closed=%v", messages, messageRows.closed)
	}

	dbtx.row = fakeRow{values: artifactValues}
	artifact, err := q.CreateRunArtifact(context.Background(), CreateRunArtifactParams{
		RunID:            runID,
		ArtifactType:     "json",
		Title:            "Run report",
		Content:          artifactContent,
		Visibility:       "shared",
		SourceArtifactID: &sourceArtifactID,
		MimeType:         &mimeType,
		FileUri:          &fileURI,
		FileName:         &fileName,
		FileSha256:       &fileSHA,
		FileSizeBytes:    &fileSize,
	})
	if err != nil {
		t.Fatalf("CreateRunArtifact error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunArtifact")
	if artifact.ID != artifactID || artifact.FileSha256 == nil || *artifact.FileSha256 != fileSHA {
		t.Fatalf("CreateRunArtifact scan = %#v", artifact)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{runID, "json", "Run report", artifactContent, "shared", &sourceArtifactID, &mimeType, &fileURI, &fileName, &fileSHA, &fileSize}) {
		t.Fatalf("CreateRunArtifact args = %#v", dbtx.queryRowArgs)
	}

	artifactRows := &fakeRows{rows: [][]any{artifactValues}}
	dbtx.queryRows = artifactRows
	artifacts, err := q.ListRunArtifactsByRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListRunArtifactsByRun error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunArtifactsByRun")
	if !artifactRows.closed || len(artifacts) != 1 || artifacts[0].ID != artifactID {
		t.Fatalf("ListRunArtifactsByRun scan = %#v closed=%v", artifacts, artifactRows.closed)
	}

	dbtx.row = fakeRow{values: artifactValues}
	if got, err := q.GetRunArtifactBySourceID(context.Background(), GetRunArtifactBySourceIDParams{RunID: runID, SourceArtifactID: sourceArtifactID}); err != nil || got.ID != artifactID {
		t.Fatalf("GetRunArtifactBySourceID = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetRunArtifactBySourceID")

	dbtx.row = fakeRow{values: artifactValues}
	updatedArtifact, err := q.UpdateRunArtifactContent(context.Background(), UpdateRunArtifactContentParams{
		ID:            artifactID,
		RunID:         runID,
		ArtifactType:  "json",
		Title:         "Run report",
		Content:       artifactContent,
		Visibility:    "shared",
		MimeType:      &mimeType,
		FileUri:       &fileURI,
		FileName:      &fileName,
		FileSha256:    &fileSHA,
		FileSizeBytes: &fileSize,
	})
	if err != nil {
		t.Fatalf("UpdateRunArtifactContent error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpdateRunArtifactContent")
	if updatedArtifact.ID != artifactID || updatedArtifact.SourceArtifactID == nil {
		t.Fatalf("UpdateRunArtifactContent scan = %#v", updatedArtifact)
	}

	dbtx.row = fakeRow{values: chunkValues}
	chunk, err := q.CreateRunArtifactChunk(context.Background(), CreateRunArtifactChunkParams{
		RunID:            runID,
		RunArtifactID:    artifactID,
		SourceArtifactID: sourceArtifactID,
		EventSequence:    &eventSequence,
		Append:           true,
		LastChunk:        false,
		Parts:            chunkParts,
		Payload:          chunkPayload,
		PartsSha256:      &partsSHA,
		PayloadSha256:    &payloadSHA,
		DeclaredSha256:   &declaredSHA,
		ChecksumStatus:   "verified",
	})
	if err != nil {
		t.Fatalf("CreateRunArtifactChunk error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunArtifactChunk")
	if chunk.ID != chunkID || chunk.ChunkIndex != 1 || chunk.ChecksumStatus != "verified" {
		t.Fatalf("CreateRunArtifactChunk scan = %#v", chunk)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{runID, artifactID, sourceArtifactID, &eventSequence, true, false, chunkParts, chunkPayload, &partsSHA, &payloadSHA, &declaredSHA, "verified"}) {
		t.Fatalf("CreateRunArtifactChunk args = %#v", dbtx.queryRowArgs)
	}

	chunkRows := &fakeRows{rows: [][]any{chunkValues}}
	dbtx.queryRows = chunkRows
	chunks, err := q.ListRunArtifactChunksByRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListRunArtifactChunksByRun error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunArtifactChunksByRun")
	if !chunkRows.closed || len(chunks) != 1 || chunks[0].ID != chunkID {
		t.Fatalf("ListRunArtifactChunksByRun scan = %#v closed=%v", chunks, chunkRows.closed)
	}
}

func TestAvailabilityMetricApprovalRequirementQueriesScanRowsAndArgs(t *testing.T) {
	creatorID := uuid.New()
	userID := uuid.New()
	tokenID := uuid.New()
	agentID := uuid.New()
	alertID := uuid.New()
	approvalID := uuid.New()
	runID := uuid.New()
	taskID := uuid.New()
	now := time.Date(2026, 6, 20, 21, 0, 0, 0, time.UTC)
	lastSuccessful := now.Add(-time.Hour)
	lastFailed := now.Add(-30 * time.Minute)
	lastChecked := now.Add(-5 * time.Minute)
	readAt := now.Add(time.Minute)
	latencyMedian := int32(120)
	latencyP95 := int32(450)
	lastError := "timeout contacting endpoint"
	decisionNote := "looks safe"
	approvalExpires := now.Add(time.Hour)
	approvalDecided := now.Add(10 * time.Minute)
	approvalPayload := []byte(`{"action":"publish"}`)

	availabilityValues := agentAvailabilitySnapshotRow(agentID, now, &lastSuccessful, &lastFailed, &lastChecked)
	alertValues := agentAvailabilityAlertRow(alertID, agentID, creatorID, now, &lastError, &readAt)
	metricValues := agentMetricSnapshotRow(agentID, now, &latencyMedian, &latencyP95)
	approvalValues := agentApprovalRow(approvalID, agentID, &userID, &tokenID, now, approvalPayload, approvalExpires, &approvalDecided, &creatorID, &decisionNote)
	evidenceValues := runRequirementEvidenceRow(runID, taskID, agentID, userID, now)
	dbtx := &fakeDBTX{
		row:     fakeRow{values: availabilityValues},
		execTag: pgconn.NewCommandTag("UPDATE 6"),
	}
	q := New(dbtx)

	snapshot, err := q.GetAgentAvailabilitySnapshot(context.Background(), agentID)
	if err != nil {
		t.Fatalf("GetAgentAvailabilitySnapshot error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentAvailabilitySnapshot")
	if snapshot.AgentID != agentID || snapshot.LastCheckedAt == nil || *snapshot.LastCheckedAt != lastChecked {
		t.Fatalf("GetAgentAvailabilitySnapshot scan = %#v", snapshot)
	}

	dbtx.row = fakeRow{values: availabilityValues}
	if got, err := q.MarkAgentAvailabilitySuccess(context.Background(), agentID); err != nil || got.AvailabilityStatus != "degraded" {
		t.Fatalf("MarkAgentAvailabilitySuccess = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkAgentAvailabilitySuccess")

	dbtx.row = fakeRow{values: availabilityValues}
	if got, err := q.MarkAgentAvailabilityHeartbeat(context.Background(), agentID); err != nil || got.AgentID != agentID {
		t.Fatalf("MarkAgentAvailabilityHeartbeat = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkAgentAvailabilityHeartbeat")

	dbtx.row = fakeRow{values: availabilityValues}
	if got, err := q.MarkAgentAvailabilityFailure(context.Background(), agentID); err != nil || got.ConsecutiveFailures != 3 {
		t.Fatalf("MarkAgentAvailabilityFailure = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkAgentAvailabilityFailure")

	dueRows := &fakeRows{rows: [][]any{agentRow(agentID, creatorID, now, nil, nil, nil)}}
	dbtx.queryRows = dueRows
	dueAgents, err := q.ListAgentsDueAvailabilityCheck(context.Background(), ListAgentsDueAvailabilityCheckParams{StaleSeconds: 300, Limit: 20})
	if err != nil {
		t.Fatalf("ListAgentsDueAvailabilityCheck error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentsDueAvailabilityCheck")
	if !dueRows.closed || len(dueAgents) != 1 || dueAgents[0].ID != agentID {
		t.Fatalf("ListAgentsDueAvailabilityCheck scan = %#v closed=%v", dueAgents, dueRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{int32(300), int32(20)}) {
		t.Fatalf("ListAgentsDueAvailabilityCheck args = %#v", dbtx.queryArgs)
	}

	dbtx.row = fakeRow{values: alertValues}
	alert, err := q.UpsertAgentAvailabilityAlert(context.Background(), UpsertAgentAvailabilityAlertParams{
		AgentID:             agentID,
		CreatorID:           creatorID,
		AlertType:           "availability",
		Severity:            "critical",
		AvailabilityStatus:  "unreachable",
		ConsecutiveFailures: 3,
		Title:               "Agent unreachable",
		Message:             "Endpoint timed out",
		LastError:           &lastError,
		RepairHints:         []string{"check endpoint", "rotate token"},
	})
	if err != nil {
		t.Fatalf("UpsertAgentAvailabilityAlert error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpsertAgentAvailabilityAlert")
	if alert.ID != alertID || alert.LastError == nil || *alert.LastError != lastError || alert.RepairHints[0] != "check endpoint" {
		t.Fatalf("UpsertAgentAvailabilityAlert scan = %#v", alert)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, creatorID, "availability", "critical", "unreachable", int32(3), "Agent unreachable", "Endpoint timed out", &lastError, []string{"check endpoint", "rotate token"}}) {
		t.Fatalf("UpsertAgentAvailabilityAlert args = %#v", dbtx.queryRowArgs)
	}

	alertRows := &fakeRows{rows: [][]any{agentAvailabilityAlertWithAgentRow(alertID, agentID, creatorID, now, &lastError, &readAt)}}
	dbtx.queryRows = alertRows
	alerts, err := q.ListAgentAvailabilityAlertsByCreator(context.Background(), ListAgentAvailabilityAlertsByCreatorParams{CreatorID: creatorID, Limit: 10})
	if err != nil {
		t.Fatalf("ListAgentAvailabilityAlertsByCreator error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentAvailabilityAlertsByCreator")
	if !alertRows.closed || len(alerts) != 1 || alerts[0].AgentSlug != "agent-one" || alerts[0].AgentName != "Agent One" {
		t.Fatalf("ListAgentAvailabilityAlertsByCreator scan = %#v closed=%v", alerts, alertRows.closed)
	}

	dbtx.row = fakeRow{values: []any{int32(4)}}
	alertCount, err := q.CountAgentAvailabilityAlertsByCreator(context.Background(), creatorID)
	if err != nil || alertCount != 4 {
		t.Fatalf("CountAgentAvailabilityAlertsByCreator = %d, %v", alertCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountAgentAvailabilityAlertsByCreator")

	dbtx.row = fakeRow{values: []any{int32(2)}}
	unreadCount, err := q.CountUnreadAgentAvailabilityAlertsByCreator(context.Background(), creatorID)
	if err != nil || unreadCount != 2 {
		t.Fatalf("CountUnreadAgentAvailabilityAlertsByCreator = %d, %v", unreadCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountUnreadAgentAvailabilityAlertsByCreator")

	dbtx.row = fakeRow{values: alertValues}
	readAlert, err := q.MarkAgentAvailabilityAlertRead(context.Background(), MarkAgentAvailabilityAlertReadParams{ID: alertID, CreatorID: creatorID})
	if err != nil {
		t.Fatalf("MarkAgentAvailabilityAlertRead error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkAgentAvailabilityAlertRead")
	if readAlert.ID != alertID || readAlert.ReadAt == nil {
		t.Fatalf("MarkAgentAvailabilityAlertRead scan = %#v", readAlert)
	}

	if err := q.UpsertAgentMetricSnapshot(context.Background(), UpsertAgentMetricSnapshotParams{
		AgentID:         agentID,
		TimeWindow:      "24h",
		CallCount:       12,
		SuccessCount:    10,
		FailureCount:    2,
		SuccessRateBps:  8333,
		MedianLatencyMs: &latencyMedian,
		P95LatencyMs:    &latencyP95,
	}); err != nil {
		t.Fatalf("UpsertAgentMetricSnapshot error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "UpsertAgentMetricSnapshot")
	if !reflect.DeepEqual(dbtx.execArgs, []any{agentID, "24h", int32(12), int32(10), int32(2), int32(8333), &latencyMedian, &latencyP95}) {
		t.Fatalf("UpsertAgentMetricSnapshot args = %#v", dbtx.execArgs)
	}

	metricRows := &fakeRows{rows: [][]any{metricValues}}
	dbtx.queryRows = metricRows
	metrics, err := q.ListAgentMetricSnapshotsByAgent(context.Background(), agentID)
	if err != nil {
		t.Fatalf("ListAgentMetricSnapshotsByAgent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentMetricSnapshotsByAgent")
	if !metricRows.closed || len(metrics) != 1 || metrics[0].SuccessRateBps != 8333 {
		t.Fatalf("ListAgentMetricSnapshotsByAgent scan = %#v closed=%v", metrics, metricRows.closed)
	}

	aggregateRows := &fakeRows{rows: [][]any{{agentID, int32(12), int32(10), int32(2), &latencyMedian, &latencyP95}}}
	dbtx.queryRows = aggregateRows
	aggregates, err := q.AggregateAgentRunsForWindow(context.Background(), "24 hours")
	if err != nil {
		t.Fatalf("AggregateAgentRunsForWindow error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "AggregateAgentRunsForWindow")
	if !aggregateRows.closed || len(aggregates) != 1 || aggregates[0].FailureCount != 2 {
		t.Fatalf("AggregateAgentRunsForWindow scan = %#v closed=%v", aggregates, aggregateRows.closed)
	}

	dbtx.row = fakeRow{values: approvalValues}
	approval, err := q.CreateAgentApproval(context.Background(), CreateAgentApprovalParams{
		AgentID:            agentID,
		RequestedByUserID:  &userID,
		RequestedByTokenID: &tokenID,
		Action:             "publish",
		PayloadJSON:        approvalPayload,
		ApprovalURLSlug:    "approve-123",
		ExpiresAt:          approvalExpires,
	})
	if err != nil {
		t.Fatalf("CreateAgentApproval error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateAgentApproval")
	if approval.ID != approvalID || approval.DecisionNote == nil || *approval.DecisionNote != decisionNote {
		t.Fatalf("CreateAgentApproval scan = %#v", approval)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, &userID, &tokenID, "publish", approvalPayload, "approve-123", approvalExpires}) {
		t.Fatalf("CreateAgentApproval args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: approvalValues}
	if got, err := q.GetAgentApprovalForCreator(context.Background(), GetAgentApprovalForCreatorParams{ID: approvalID, CreatorID: creatorID}); err != nil || got.ID != approvalID {
		t.Fatalf("GetAgentApprovalForCreator = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentApprovalForCreator")

	approvalRows := &fakeRows{rows: [][]any{approvalValues}}
	dbtx.queryRows = approvalRows
	approvals, err := q.ListAgentApprovalsForCreator(context.Background(), creatorID)
	if err != nil {
		t.Fatalf("ListAgentApprovalsForCreator error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentApprovalsForCreator")
	if !approvalRows.closed || len(approvals) != 1 || approvals[0].ID != approvalID {
		t.Fatalf("ListAgentApprovalsForCreator scan = %#v closed=%v", approvals, approvalRows.closed)
	}

	if rows, err := q.ConfirmAgentApproval(context.Background(), ConfirmAgentApprovalParams{ID: approvalID, CreatorID: creatorID, DecisionNote: &decisionNote}); err != nil || rows != 6 {
		t.Fatalf("ConfirmAgentApproval = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "ConfirmAgentApproval")
	if !reflect.DeepEqual(dbtx.execArgs, []any{approvalID, creatorID, &decisionNote}) {
		t.Fatalf("ConfirmAgentApproval args = %#v", dbtx.execArgs)
	}

	if rows, err := q.RejectAgentApproval(context.Background(), RejectAgentApprovalParams{ID: approvalID, CreatorID: creatorID, DecisionNote: &decisionNote}); err != nil || rows != 6 {
		t.Fatalf("RejectAgentApproval = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "RejectAgentApproval")

	if rows, err := q.ExpireAgentApprovals(context.Background()); err != nil || rows != 6 {
		t.Fatalf("ExpireAgentApprovals = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "ExpireAgentApprovals")

	dbtx.row = fakeRow{values: evidenceValues}
	evidence, err := q.CreateRunRequirementEvidence(context.Background(), CreateRunRequirementEvidenceParams{
		RunID:            runID,
		TaskID:           taskID,
		AgentID:          agentID,
		UserID:           userID,
		RequiredSkillIDs: []string{"data/sql-query"},
		RequiredMCPTools: []string{"run_agent"},
		AgentSkillIDs:    []string{"data/sql-query", "writing/summary"},
		MatchedSkillIDs:  []string{"data/sql-query"},
		MissingSkillIDs:  []string{"writing/chart"},
		UsedMCPTools:     []string{"run_agent"},
		MissingMCPTools:  []string{"browser.search"},
		CoverageStatus:   "partial",
		EvidenceSource:   "task_requirements",
	})
	if err != nil {
		t.Fatalf("CreateRunRequirementEvidence error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRunRequirementEvidence")
	if evidence.RunID != runID || evidence.CoverageStatus != "partial" || evidence.MissingMCPTools[0] != "browser.search" {
		t.Fatalf("CreateRunRequirementEvidence scan = %#v", evidence)
	}

	dbtx.row = fakeRow{values: evidenceValues}
	if got, err := q.GetRunRequirementEvidenceByRun(context.Background(), runID); err != nil || got.TaskID != taskID {
		t.Fatalf("GetRunRequirementEvidenceByRun = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetRunRequirementEvidenceByRun")
}

func TestTaskAndBenchmarkQueriesScanRowsAndArgs(t *testing.T) {
	userID := uuid.New()
	creatorID := uuid.New()
	agentID := uuid.New()
	taskID := uuid.New()
	runID := uuid.New()
	testCaseID := uuid.New()
	batchID := uuid.New()
	benchmarkRunID := uuid.New()
	now := time.Date(2026, 6, 20, 22, 0, 0, 0, time.UTC)
	chosenAt := now.Add(time.Minute)
	claimedAt := now.Add(2 * time.Minute)
	completedAt := now.Add(3 * time.Minute)
	acceptedAt := now.Add(4 * time.Minute)
	revisionAt := now.Add(5 * time.Minute)
	publishedAt := now.Add(6 * time.Minute)
	finishedAt := now.Add(7 * time.Minute)
	completionSummary := "dashboard delivered"
	publicSummary := "Need a SQL dashboard"
	revisionNote := "add chart labels"
	deliveryArtifact := []byte(`{"artifact":"dashboard"}`)
	score := int32(92)
	judgeReasoning := "complete and accurate"
	errorMessage := "judge failed"

	taskValues := taskQueryRow(taskID, userID, agentID, creatorID, runID, now, &chosenAt, &claimedAt, &completedAt, &acceptedAt, &revisionAt, &publishedAt, &completionSummary, &publicSummary, &revisionNote, deliveryArtifact)
	testCaseValues := skillTestCaseRow(testCaseID, now)
	benchmarkValues := benchmarkRunRow(benchmarkRunID, batchID, agentID, testCaseID, now, &finishedAt, &score, []byte(`{"answer":"ok"}`), &judgeReasoning, &errorMessage)
	scoreValues := agentSkillScoreRow(agentID, batchID, now, &score, &finishedAt)
	dbtx := &fakeDBTX{
		row:     fakeRow{values: taskValues},
		execTag: pgconn.NewCommandTag("UPDATE 5"),
	}
	q := New(dbtx)

	task, err := q.CreateTaskQuery(context.Background(), CreateTaskQueryParams{
		UserID:              userID,
		Query:               "build a dashboard",
		ParsedSkills:        []string{"data/sql-query"},
		MCPTools:            []string{"browser.search"},
		RecommendedAgentIDs: []uuid.UUID{agentID},
	})
	if err != nil {
		t.Fatalf("CreateTaskQuery error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateTaskQuery")
	if task.ID != taskID || task.ChosenAgentID == nil || *task.ChosenAgentID != agentID || string(task.DeliveryArtifact) == "" {
		t.Fatalf("CreateTaskQuery scan = %#v", task)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{userID, "build a dashboard", []string{"data/sql-query"}, []string{"browser.search"}, []uuid.UUID{agentID}}) {
		t.Fatalf("CreateTaskQuery args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: taskValues}
	if got, err := q.GetTaskQuery(context.Background(), taskID); err != nil || got.ID != taskID {
		t.Fatalf("GetTaskQuery = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetTaskQuery")

	dbtx.row = fakeRow{values: taskValues}
	if got, err := q.MarkTaskQueryChosen(context.Background(), MarkTaskQueryChosenParams{ID: taskID, UserID: userID, ChosenAgentID: agentID}); err != nil || got.ChosenAt == nil {
		t.Fatalf("MarkTaskQueryChosen = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkTaskQueryChosen")

	dbtx.row = fakeRow{values: taskValues}
	if got, err := q.PublishTaskQuery(context.Background(), PublishTaskQueryParams{ID: taskID, UserID: userID, PublicSummary: publicSummary}); err != nil || got.PublicSummary == nil {
		t.Fatalf("PublishTaskQuery = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "PublishTaskQuery")

	dbtx.row = fakeRow{values: taskValues}
	if got, err := q.ClaimTaskQuery(context.Background(), ClaimTaskQueryParams{ID: taskID, UserID: creatorID, AgentID: agentID}); err != nil || got.ClaimedByUserID == nil {
		t.Fatalf("ClaimTaskQuery = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ClaimTaskQuery")

	dbtx.row = fakeRow{values: taskValues}
	if got, err := q.CompleteTaskQuery(context.Background(), CompleteTaskQueryParams{
		ID:                 taskID,
		UserID:             creatorID,
		AgentID:            agentID,
		CompletionRunID:    runID,
		CompletionSummary:  completionSummary,
		DeliveryArtifact:   deliveryArtifact,
		DeliveryVisibility: "shared",
	}); err != nil || got.CompletedAt == nil {
		t.Fatalf("CompleteTaskQuery = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CompleteTaskQuery")

	dbtx.row = fakeRow{values: taskValues}
	if got, err := q.AcceptTaskDelivery(context.Background(), AcceptTaskDeliveryParams{ID: taskID, UserID: userID}); err != nil || got.AcceptedAt == nil {
		t.Fatalf("AcceptTaskDelivery = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "AcceptTaskDelivery")

	dbtx.row = fakeRow{values: taskValues}
	if got, err := q.RequestTaskRevision(context.Background(), RequestTaskRevisionParams{ID: taskID, UserID: userID, RevisionNote: revisionNote}); err != nil || got.RevisionNote == nil {
		t.Fatalf("RequestTaskRevision = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "RequestTaskRevision")

	taskRows := &fakeRows{rows: [][]any{taskValues}}
	dbtx.queryRows = taskRows
	tasks, err := q.ListTaskQueriesByUser(context.Background(), ListTaskQueriesByUserParams{UserID: userID, Limit: 10})
	if err != nil {
		t.Fatalf("ListTaskQueriesByUser error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListTaskQueriesByUser")
	if !taskRows.closed || len(tasks) != 1 || tasks[0].ID != taskID {
		t.Fatalf("ListTaskQueriesByUser scan = %#v closed=%v", tasks, taskRows.closed)
	}

	publicTaskRows := &fakeRows{rows: [][]any{taskValues}}
	dbtx.queryRows = publicTaskRows
	publicTasks, err := q.ListPublicTaskQueries(context.Background(), 15)
	if err != nil {
		t.Fatalf("ListPublicTaskQueries error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListPublicTaskQueries")
	if !publicTaskRows.closed || len(publicTasks) != 1 || publicTasks[0].Visibility != "public" {
		t.Fatalf("ListPublicTaskQueries scan = %#v closed=%v", publicTasks, publicTaskRows.closed)
	}

	agentRows := &fakeRows{rows: [][]any{agentWithCreatorRow(agentID, creatorID, now)}}
	dbtx.queryRows = agentRows
	agents, err := q.GetAgentsByIDs(context.Background(), []uuid.UUID{agentID})
	if err != nil {
		t.Fatalf("GetAgentsByIDs error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "GetAgentsByIDs")
	if !agentRows.closed || len(agents) != 1 || agents[0].CreatorName != "Creator Name" {
		t.Fatalf("GetAgentsByIDs scan = %#v closed=%v", agents, agentRows.closed)
	}

	testCaseRows := &fakeRows{rows: [][]any{testCaseValues}}
	dbtx.queryRows = testCaseRows
	testCases, err := q.ListTestCasesBySkill(context.Background(), "data/sql-query")
	if err != nil {
		t.Fatalf("ListTestCasesBySkill error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListTestCasesBySkill")
	if !testCaseRows.closed || len(testCases) != 1 || testCases[0].ID != testCaseID {
		t.Fatalf("ListTestCasesBySkill scan = %#v closed=%v", testCases, testCaseRows.closed)
	}

	dbtx.row = fakeRow{values: []any{int32(3)}}
	testCaseCount, err := q.CountTestCasesBySkill(context.Background(), "data/sql-query")
	if err != nil || testCaseCount != 3 {
		t.Fatalf("CountTestCasesBySkill = %d, %v", testCaseCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountTestCasesBySkill")

	dbtx.row = fakeRow{values: benchmarkValues}
	benchmarkRun, err := q.CreateBenchmarkRun(context.Background(), CreateBenchmarkRunParams{
		BatchID:    batchID,
		AgentID:    agentID,
		SkillID:    "data/sql-query",
		TestCaseID: testCaseID,
	})
	if err != nil {
		t.Fatalf("CreateBenchmarkRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateBenchmarkRun")
	if benchmarkRun.ID != benchmarkRunID || benchmarkRun.Score == nil || *benchmarkRun.Score != score {
		t.Fatalf("CreateBenchmarkRun scan = %#v", benchmarkRun)
	}

	if rows, err := q.MarkBenchmarkRunSuccess(context.Background(), MarkBenchmarkRunSuccessParams{ID: benchmarkRunID, Score: score, RawOutput: []byte(`{"ok":true}`), JudgeReasoning: judgeReasoning}); err != nil || rows != 5 {
		t.Fatalf("MarkBenchmarkRunSuccess = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkBenchmarkRunSuccess")

	if rows, err := q.MarkBenchmarkRunFailed(context.Background(), MarkBenchmarkRunFailedParams{ID: benchmarkRunID, ErrorMessage: errorMessage}); err != nil || rows != 5 {
		t.Fatalf("MarkBenchmarkRunFailed = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkBenchmarkRunFailed")

	benchmarkRows := &fakeRows{rows: [][]any{append(append([]any{}, benchmarkValues...), "SQL happy path")}}
	dbtx.queryRows = benchmarkRows
	benchmarkRuns, err := q.ListBenchmarkRunsByBatch(context.Background(), batchID)
	if err != nil {
		t.Fatalf("ListBenchmarkRunsByBatch error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListBenchmarkRunsByBatch")
	if !benchmarkRows.closed || len(benchmarkRuns) != 1 || benchmarkRuns[0].TestCaseTitle != "SQL happy path" {
		t.Fatalf("ListBenchmarkRunsByBatch scan = %#v closed=%v", benchmarkRuns, benchmarkRows.closed)
	}

	dbtx.row = fakeRow{values: scoreValues}
	skillScore, err := q.UpsertAgentSkillScore(context.Background(), UpsertAgentSkillScoreParams{
		AgentID:      agentID,
		SkillID:      "data/sql-query",
		Status:       "verified",
		AverageScore: &score,
		PassCount:    2,
		TotalCount:   3,
		LastBatchID:  &batchID,
		VerifiedAt:   &finishedAt,
	})
	if err != nil {
		t.Fatalf("UpsertAgentSkillScore error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpsertAgentSkillScore")
	if skillScore.AgentID != agentID || skillScore.AverageScore == nil || *skillScore.AverageScore != score {
		t.Fatalf("UpsertAgentSkillScore scan = %#v", skillScore)
	}

	dbtx.row = fakeRow{values: scoreValues}
	if got, err := q.GetAgentSkillScore(context.Background(), GetAgentSkillScoreParams{AgentID: agentID, SkillID: "data/sql-query"}); err != nil || got.SkillID != "data/sql-query" {
		t.Fatalf("GetAgentSkillScore = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentSkillScore")

	scoreRows := &fakeRows{rows: [][]any{scoreValues}}
	dbtx.queryRows = scoreRows
	scores, err := q.ListAgentSkillScoresByAgent(context.Background(), agentID)
	if err != nil {
		t.Fatalf("ListAgentSkillScoresByAgent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentSkillScoresByAgent")
	if !scoreRows.closed || len(scores) != 1 || scores[0].Status != "verified" {
		t.Fatalf("ListAgentSkillScoresByAgent scan = %#v closed=%v", scores, scoreRows.closed)
	}

	slugScoreRows := &fakeRows{rows: [][]any{scoreValues}}
	dbtx.queryRows = slugScoreRows
	scoresBySlug, err := q.ListAgentSkillScoresBySlug(context.Background(), "agent-one")
	if err != nil {
		t.Fatalf("ListAgentSkillScoresBySlug error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentSkillScoresBySlug")
	if !slugScoreRows.closed || len(scoresBySlug) != 1 || scoresBySlug[0].AgentID != agentID {
		t.Fatalf("ListAgentSkillScoresBySlug scan = %#v closed=%v", scoresBySlug, slugScoreRows.closed)
	}

	topRows := &fakeRows{rows: [][]any{{agentID, &score, &finishedAt, "agent-one", "Agent One", "does work", []string{"data"}, int32(12), int32(34)}}}
	dbtx.queryRows = topRows
	topAgents, err := q.ListTopAgentsBySkill(context.Background(), ListTopAgentsBySkillParams{SkillID: "data/sql-query", Limit: 5})
	if err != nil {
		t.Fatalf("ListTopAgentsBySkill error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListTopAgentsBySkill")
	if !topRows.closed || len(topAgents) != 1 || topAgents[0].Slug != "agent-one" {
		t.Fatalf("ListTopAgentsBySkill scan = %#v closed=%v", topAgents, topRows.closed)
	}

	matchRows := &fakeRows{rows: [][]any{{agentID, int32(2), int32(1), int32(34)}}}
	dbtx.queryRows = matchRows
	matches, err := q.ListAgentsBySkillsWithVerified(context.Background(), []string{"data/sql-query", "writing/summary"})
	if err != nil {
		t.Fatalf("ListAgentsBySkillsWithVerified error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentsBySkillsWithVerified")
	if !matchRows.closed || len(matches) != 1 || matches[0].VerifiedCount != 1 {
		t.Fatalf("ListAgentsBySkillsWithVerified scan = %#v closed=%v", matches, matchRows.closed)
	}

	summaryRows := &fakeRows{rows: [][]any{{batchID, "data/sql-query", now, &finishedAt, int32(3), int32(2), &score}}}
	dbtx.queryRows = summaryRows
	summaries, err := q.ListBenchmarkBatchSummariesByAgent(context.Background(), ListBenchmarkBatchSummariesByAgentParams{AgentID: agentID, Limit: 10})
	if err != nil {
		t.Fatalf("ListBenchmarkBatchSummariesByAgent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListBenchmarkBatchSummariesByAgent")
	if !summaryRows.closed || len(summaries) != 1 || summaries[0].AverageScore == nil {
		t.Fatalf("ListBenchmarkBatchSummariesByAgent scan = %#v closed=%v", summaries, summaryRows.closed)
	}

	dbtx.row = fakeRow{values: []any{int32(4), &batchID}}
	verifiedStats, err := q.GetAgentVerifiedSkillStats(context.Background(), agentID)
	if err != nil {
		t.Fatalf("GetAgentVerifiedSkillStats error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentVerifiedSkillStats")
	if verifiedStats.VerifiedCount != 4 || verifiedStats.LatestBatchID == nil {
		t.Fatalf("GetAgentVerifiedSkillStats scan = %#v", verifiedStats)
	}
}

func TestDashboardRunQueriesScanRowsAndScalars(t *testing.T) {
	userID := uuid.New()
	creatorID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	tokenID := uuid.New()
	now := time.Date(2026, 6, 20, 23, 0, 0, 0, time.UTC)
	duration := int32(80)
	finishedAt := now.Add(time.Minute)
	claimedAt := now.Add(10 * time.Second)
	runValues := runRow(runID, userID, agentID, []byte(`{"prompt":"hi"}`), []byte(`{"ok":true}`), "success", nil, nil, 100, 25, 75, &duration, now, &finishedAt, "dashboard")
	creatorRunValues := append(append([]any{}, runValues...), &tokenID, &claimedAt, "agent-one", "Agent One")
	dbtx := &fakeDBTX{row: fakeRow{values: []any{int32(9)}}}
	q := New(dbtx)

	runRows := &fakeRows{rows: [][]any{runValues}}
	dbtx.queryRows = runRows
	runs, err := q.ListRunsByUser(context.Background(), ListRunsByUserParams{UserID: userID, Limit: 10, Offset: 2})
	if err != nil {
		t.Fatalf("ListRunsByUser error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunsByUser")
	if !runRows.closed || len(runs) != 1 || runs[0].Source != "dashboard" {
		t.Fatalf("ListRunsByUser scan = %#v closed=%v", runs, runRows.closed)
	}

	filteredRows := &fakeRows{rows: [][]any{runValues}}
	dbtx.queryRows = filteredRows
	filtered, err := q.ListRunsByUserAndAgent(context.Background(), ListRunsByUserAndAgentParams{
		UserID:          userID,
		AgentID:         agentID,
		NoCursor:        false,
		CursorStartedAt: now,
		CursorID:        runID,
		NoStatusFilter:  false,
		Statuses:        []string{"success"},
		NoSinceFilter:   false,
		Since:           now.Add(-time.Hour),
		ContextID:       "ctx-1",
		Limit:           20,
	})
	if err != nil {
		t.Fatalf("ListRunsByUserAndAgent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunsByUserAndAgent")
	if !filteredRows.closed || len(filtered) != 1 || filtered[0].ID != runID {
		t.Fatalf("ListRunsByUserAndAgent scan = %#v closed=%v", filtered, filteredRows.closed)
	}

	dbtx.row = fakeRow{values: []any{int32(7)}}
	filteredCount, err := q.CountRunsByUserAndAgent(context.Background(), CountRunsByUserAndAgentParams{
		UserID: userID, AgentID: agentID, NoStatusFilter: false,
		Statuses: []string{"success"}, NoSinceFilter: true, Since: now, ContextID: "",
	})
	if err != nil || filteredCount != 7 {
		t.Fatalf("CountRunsByUserAndAgent = %d, %v", filteredCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountRunsByUserAndAgent")

	creatorRows := &fakeRows{rows: [][]any{creatorRunValues}}
	dbtx.queryRows = creatorRows
	creatorRuns, err := q.ListRunsByCreatorAgentWithAgent(context.Background(), ListRunsByCreatorAgentWithAgentParams{CreatorID: creatorID, AgentID: agentID, Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("ListRunsByCreatorAgentWithAgent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunsByCreatorAgentWithAgent")
	if !creatorRows.closed || len(creatorRuns) != 1 || creatorRuns[0].ClaimedByRuntimeTokenID == nil || creatorRuns[0].AgentName != "Agent One" {
		t.Fatalf("ListRunsByCreatorAgentWithAgent scan = %#v closed=%v", creatorRuns, creatorRows.closed)
	}

	scalarInt32Checks := []struct {
		name string
		call func() (int32, error)
	}{
		{"CountRunsByUser", func() (int32, error) { return q.CountRunsByUser(context.Background(), userID) }},
		{"CountRunsByCreatorAgent", func() (int32, error) {
			return q.CountRunsByCreatorAgent(context.Background(), CountRunsByCreatorAgentParams{CreatorID: creatorID, AgentID: agentID})
		}},
		{"CountRunsByUserThisMonth", func() (int32, error) { return q.CountRunsByUserThisMonth(context.Background(), userID) }},
		{"CountRunsForCreatorThisMonth", func() (int32, error) { return q.CountRunsForCreatorThisMonth(context.Background(), creatorID) }},
		{"CountAgentsByCreator", func() (int32, error) { return q.CountAgentsByCreator(context.Background(), creatorID) }},
		{"CountPendingAgentsByCreator", func() (int32, error) { return q.CountPendingAgentsByCreator(context.Background(), creatorID) }},
	}
	for _, tc := range scalarInt32Checks {
		dbtx.row = fakeRow{values: []any{int32(9)}}
		got, err := tc.call()
		if err != nil || got != 9 {
			t.Fatalf("%s = %d, %v", tc.name, got, err)
		}
		requireSQLName(t, dbtx.queryRowSQL, tc.name)
	}

	dbtx.row = fakeRow{values: []any{int64(1234)}}
	spent, err := q.SumSpentByUserThisMonth(context.Background(), userID)
	if err != nil || spent != 1234 {
		t.Fatalf("SumSpentByUserThisMonth = %d, %v", spent, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "SumSpentByUserThisMonth")

	dbtx.row = fakeRow{values: []any{int64(5678)}}
	earned, err := q.SumEarningsByCreatorThisMonth(context.Background(), creatorID)
	if err != nil || earned != 5678 {
		t.Fatalf("SumEarningsByCreatorThisMonth = %d, %v", earned, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "SumEarningsByCreatorThisMonth")

	statsRows := &fakeRows{rows: [][]any{{agentID, "agent-one", "Agent One", "approved", int32(12), int32(34), int64(5600), int64(8), int64(900)}}}
	dbtx.queryRows = statsRows
	stats, err := q.ListAgentStatsForCreator(context.Background(), creatorID)
	if err != nil {
		t.Fatalf("ListAgentStatsForCreator error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentStatsForCreator")
	if !statsRows.closed || len(stats) != 1 || stats[0].RevenueThisMonth != 900 {
		t.Fatalf("ListAgentStatsForCreator scan = %#v closed=%v", stats, statsRows.closed)
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

func agentCapabilityRow(id, agentID uuid.UUID, now time.Time, inputSchema, outputSchema []byte) []any {
	return []any{id, agentID, inputSchema, outputSchema, "A2A output contract", int32(3), now, now.Add(time.Minute)}
}

func agentExampleRow(id, agentID uuid.UUID, now time.Time, input, expectedOutput []byte) []any {
	return []any{id, agentID, "happy path", input, expectedOutput, int32(4), now, now.Add(time.Minute)}
}

func agentOnboardingStatusRow(agentID uuid.UUID, now time.Time, dryRunErr *string, dryRunAt *time.Time) []any {
	return []any{agentID, true, true, true, false, "fail", dryRunErr, dryRunAt, now.Add(time.Minute)}
}

func runMessageRow(id, runID uuid.UUID, eventSequence *int32, payload []byte, createdAt time.Time) []any {
	return []any{id, runID, eventSequence, "agent", "done", payload, createdAt}
}

func runArtifactRow(
	id, runID uuid.UUID,
	createdAt time.Time,
	content []byte,
	sourceArtifactID, mimeType, fileURI, fileName, fileSHA *string,
	fileSize *int64,
) []any {
	return []any{
		id,
		runID,
		"json",
		"Run report",
		content,
		"shared",
		sourceArtifactID,
		mimeType,
		fileURI,
		fileName,
		fileSHA,
		fileSize,
		createdAt,
	}
}

func runArtifactChunkRow(
	id, runID, artifactID uuid.UUID,
	createdAt time.Time,
	sourceArtifactID string,
	eventSequence *int32,
	parts, payload []byte,
	partsSHA, payloadSHA, declaredSHA *string,
) []any {
	return []any{
		id,
		runID,
		artifactID,
		sourceArtifactID,
		eventSequence,
		int32(1),
		true,
		false,
		parts,
		payload,
		partsSHA,
		payloadSHA,
		declaredSHA,
		"verified",
		createdAt,
	}
}

func agentAvailabilitySnapshotRow(agentID uuid.UUID, now time.Time, lastSuccessful, lastFailed, lastChecked *time.Time) []any {
	return []any{agentID, "degraded", lastSuccessful, lastFailed, lastChecked, int32(3), now.Add(time.Minute)}
}

func agentAvailabilityAlertRow(id, agentID, creatorID uuid.UUID, now time.Time, lastError *string, readAt *time.Time) []any {
	return []any{
		id,
		agentID,
		creatorID,
		"availability",
		"critical",
		"unreachable",
		int32(3),
		"Agent unreachable",
		"Endpoint timed out",
		lastError,
		[]string{"check endpoint", "rotate token"},
		readAt,
		now,
		now.Add(time.Minute),
	}
}

func agentAvailabilityAlertWithAgentRow(id, agentID, creatorID uuid.UUID, now time.Time, lastError *string, readAt *time.Time) []any {
	return []any{
		id,
		agentID,
		"agent-one",
		"Agent One",
		creatorID,
		"availability",
		"critical",
		"unreachable",
		int32(3),
		"Agent unreachable",
		"Endpoint timed out",
		lastError,
		[]string{"check endpoint", "rotate token"},
		readAt,
		now,
		now.Add(time.Minute),
	}
}

func agentMetricSnapshotRow(agentID uuid.UUID, now time.Time, medianLatency, p95Latency *int32) []any {
	return []any{agentID, "24h", int32(12), int32(10), int32(2), int32(8333), medianLatency, p95Latency, now}
}

func agentApprovalRow(
	id, agentID uuid.UUID,
	requestedByUserID, requestedByTokenID *uuid.UUID,
	createdAt time.Time,
	payload []byte,
	expiresAt time.Time,
	decidedAt *time.Time,
	decidedByUserID *uuid.UUID,
	decisionNote *string,
) []any {
	return []any{
		id,
		agentID,
		requestedByUserID,
		requestedByTokenID,
		"publish",
		payload,
		"pending",
		"approve-123",
		expiresAt,
		decidedAt,
		decidedByUserID,
		decisionNote,
		createdAt,
	}
}

func runRequirementEvidenceRow(runID, taskID, agentID, userID uuid.UUID, createdAt time.Time) []any {
	return []any{
		runID,
		taskID,
		agentID,
		userID,
		[]string{"data/sql-query"},
		[]string{"run_agent"},
		[]string{"data/sql-query", "writing/summary"},
		[]string{"data/sql-query"},
		[]string{"writing/chart"},
		[]string{"run_agent"},
		[]string{"browser.search"},
		"partial",
		"task_requirements",
		createdAt,
	}
}

func taskQueryRow(
	id, userID, agentID, claimedByUserID, runID uuid.UUID,
	createdAt time.Time,
	chosenAt, claimedAt, completedAt, acceptedAt, revisionRequestedAt, publishedAt *time.Time,
	completionSummary, publicSummary, revisionNote *string,
	deliveryArtifact []byte,
) []any {
	return []any{
		id,
		userID,
		"build a dashboard",
		[]string{"data/sql-query"},
		[]string{"browser.search"},
		[]uuid.UUID{agentID},
		&agentID,
		chosenAt,
		&agentID,
		&claimedByUserID,
		claimedAt,
		&runID,
		completedAt,
		completionSummary,
		&runID,
		"submitted",
		"shared",
		deliveryArtifact,
		acceptedAt,
		revisionRequestedAt,
		revisionNote,
		"public",
		publicSummary,
		publishedAt,
		createdAt,
	}
}

func agentWithCreatorRow(agentID, creatorID uuid.UUID, now time.Time) []any {
	return []any{
		agentID,
		creatorID,
		"agent-one",
		"Agent One",
		"does work",
		"https://example.com/agent",
		nil,
		int32(12),
		[]string{"data"},
		"active",
		"public",
		"certified",
		nil,
		&now,
		int32(34),
		int64(5600),
		nil,
		now,
		now.Add(time.Minute),
		"Creator Name",
	}
}

func skillTestCaseRow(id uuid.UUID, createdAt time.Time) []any {
	return []any{id, "data/sql-query", "SQL happy path", []byte(`{"prompt":"query"}`), "judge {output}", int32(1), createdAt}
}

func benchmarkRunRow(
	id, batchID, agentID, testCaseID uuid.UUID,
	startedAt time.Time,
	finishedAt *time.Time,
	score *int32,
	rawOutput []byte,
	judgeReasoning, errorMessage *string,
) []any {
	return []any{
		id,
		batchID,
		agentID,
		"data/sql-query",
		testCaseID,
		"success",
		score,
		rawOutput,
		judgeReasoning,
		errorMessage,
		startedAt,
		finishedAt,
	}
}

func agentSkillScoreRow(agentID, batchID uuid.UUID, updatedAt time.Time, averageScore *int32, verifiedAt *time.Time) []any {
	return []any{
		agentID,
		"data/sql-query",
		"verified",
		averageScore,
		int32(2),
		int32(3),
		&batchID,
		verifiedAt,
		updatedAt,
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
