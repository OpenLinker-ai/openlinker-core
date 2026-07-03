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

	dbtx.row = fakeRow{values: userRow(userID, now, &passwordHash, nil, nil, nil, nil)}
	adminCreated, err := q.CreateAdminUser(context.Background(), CreateAdminUserParams{
		Email:           "admin-created@example.com",
		PasswordHash:    &passwordHash,
		DisplayName:     "Admin Created",
		IsAdmin:         true,
		IsCreator:       true,
		CreatorVerified: true,
	})
	if err != nil {
		t.Fatalf("CreateAdminUser error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateAdminUser")
	if adminCreated.ID != userID {
		t.Fatalf("CreateAdminUser scan = %#v", adminCreated)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{"admin-created@example.com", &passwordHash, "Admin Created", true, true, true}) {
		t.Fatalf("CreateAdminUser args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: userRow(userID, now, &passwordHash, &provider, &oauthID, &avatar, nil)}
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

	adminRows := &fakeRows{rows: [][]any{userRow(userID, now, &passwordHash, &provider, &oauthID, &avatar, nil)}}
	dbtx.queryRows = adminRows
	adminUsers, err := q.ListAdminUsers(context.Background(), ListAdminUsersParams{Query: "user", Role: "admin", Limit: 25, Offset: 5})
	if err != nil {
		t.Fatalf("ListAdminUsers error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAdminUsers")
	if !adminRows.closed || len(adminUsers) != 1 || adminUsers[0].ID != userID {
		t.Fatalf("ListAdminUsers scan = %#v closed=%v", adminUsers, adminRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{"user", "admin", int32(25), int32(5)}) {
		t.Fatalf("ListAdminUsers args = %#v", dbtx.queryArgs)
	}

	dbtx.row = fakeRow{values: []any{int32(7)}}
	totalUsers, err := q.CountAdminUsers(context.Background(), CountAdminUsersParams{Query: "user", Role: "creator"})
	if err != nil || totalUsers != 7 {
		t.Fatalf("CountAdminUsers = %d, %v", totalUsers, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountAdminUsers")

	dbtx.row = fakeRow{values: userRow(userID, now, &passwordHash, &provider, &oauthID, &avatar, nil)}
	updated, err := q.UpdateAdminUserFlags(context.Background(), UpdateAdminUserFlagsParams{
		ID:              userID,
		IsAdmin:         true,
		IsCreator:       true,
		CreatorVerified: true,
	})
	if err != nil {
		t.Fatalf("UpdateAdminUserFlags error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpdateAdminUserFlags")
	if updated.ID != userID || !reflect.DeepEqual(dbtx.queryRowArgs, []any{userID, true, true, true}) {
		t.Fatalf("UpdateAdminUserFlags scan/args = %#v args=%#v", updated, dbtx.queryRowArgs)
	}
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

	dbtx.row = fakeRow{values: []any{0.15}}
	feeRate, err := q.GetAgentPlatformFeeRate(context.Background(), GetAgentPlatformFeeRateParams{
		ID:           agentID,
		FallbackRate: 0.25,
	})
	if err != nil || feeRate != 0.15 {
		t.Fatalf("GetAgentPlatformFeeRate = %f, %v", feeRate, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentPlatformFeeRate")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, 0.25}) {
		t.Fatalf("GetAgentPlatformFeeRate args = %#v", dbtx.queryRowArgs)
	}

	affected, err := q.DisableAgent(context.Background(), DisableAgentParams{ID: agentID, CreatorID: creatorID})
	if err != nil || affected != 3 {
		t.Fatalf("DisableAgent = %d, %v", affected, err)
	}
	requireSQLName(t, dbtx.execSQL, "DisableAgent")

	adminAgentRows := &fakeRows{rows: [][]any{append(append([]any{}, agentValues...), "creator@example.com", "Creator Name")}}
	dbtx.queryRows = adminAgentRows
	adminAgents, err := q.ListAdminAgents(context.Background(), ListAdminAgentsParams{
		Query:               "agent",
		LifecycleStatus:     "active",
		Visibility:          "public",
		CertificationStatus: "certified",
		Limit:               50,
		Offset:              10,
	})
	if err != nil {
		t.Fatalf("ListAdminAgents error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAdminAgents")
	if !adminAgentRows.closed || len(adminAgents) != 1 || adminAgents[0].CreatorEmail != "creator@example.com" {
		t.Fatalf("ListAdminAgents scan = %#v closed=%v", adminAgents, adminAgentRows.closed)
	}

	dbtx.row = fakeRow{values: []any{int32(8)}}
	totalAgents, err := q.CountAdminAgents(context.Background(), CountAdminAgentsParams{
		Query:               "agent",
		LifecycleStatus:     "active",
		Visibility:          "public",
		CertificationStatus: "pending",
	})
	if err != nil || totalAgents != 8 {
		t.Fatalf("CountAdminAgents = %d, %v", totalAgents, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountAdminAgents")

	dbtx.row = fakeRow{values: agentValues}
	moderated, err := q.UpdateAdminAgentModeration(context.Background(), UpdateAdminAgentModerationParams{
		ID:                  agentID,
		LifecycleStatus:     "active",
		Visibility:          "unlisted",
		CertificationStatus: "rejected",
		RejectionReason:     "missing dry run",
	})
	if err != nil {
		t.Fatalf("UpdateAdminAgentModeration error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpdateAdminAgentModeration")
	if moderated.ID != agentID || !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, "active", "unlisted", "rejected", "missing dry run"}) {
		t.Fatalf("UpdateAdminAgentModeration scan/args = %#v args=%#v", moderated, dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: []any{int32(12), int32(2), int32(5), int32(3), int32(9), int32(8), int32(1), int32(4), int32(6)}}
	summary, err := q.GetAdminSummary(context.Background())
	if err != nil || summary.TotalUsers != 12 || summary.PendingAgents != 4 {
		t.Fatalf("GetAdminSummary = %#v, %v", summary, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAdminSummary")
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

	agentSkillRows := &fakeRows{rows: [][]any{skillRow("ai/agent-orchestration", "ai", "Agent orchestration", "delegate safely", 3, now)}}
	dbtx.queryRows = agentSkillRows
	agentSkills, err := q.ListAgentSkills(context.Background(), agentID)
	if err != nil {
		t.Fatalf("ListAgentSkills error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentSkills")
	if !agentSkillRows.closed || len(agentSkills) != 1 || agentSkills[0].ID != "ai/agent-orchestration" {
		t.Fatalf("ListAgentSkills scan = %#v closed=%v", agentSkills, agentSkillRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{agentID}) {
		t.Fatalf("ListAgentSkills args = %#v", dbtx.queryArgs)
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

	tx := &fakeTx{tag: pgconn.NewCommandTag("INSERT 0 1")}
	if err := ReplaceAgentSkills(context.Background(), tx, agentID, []string{"ai/agent-orchestration", "data/sql-query"}); err != nil {
		t.Fatalf("ReplaceAgentSkills error = %v", err)
	}
	if len(tx.execSQLs) != 3 {
		t.Fatalf("ReplaceAgentSkills exec count = %d", len(tx.execSQLs))
	}
	requireSQLName(t, tx.execSQLs[0], "DeleteAgentSkills")
	requireSQLName(t, tx.execSQLs[1], "InsertAgentSkill")
	requireSQLName(t, tx.execSQLs[2], "InsertAgentSkill")
	if !reflect.DeepEqual(tx.execArgs[0], []any{agentID}) || !reflect.DeepEqual(tx.execArgs[2], []any{agentID, "data/sql-query"}) {
		t.Fatalf("ReplaceAgentSkills args = %#v", tx.execArgs)
	}

	withTx := q.WithTx(tx)
	if withTx.db != tx {
		t.Fatalf("WithTx did not bind the provided transaction")
	}
}

func TestAgentTokenQueriesScanRowsAndAffectedRows(t *testing.T) {
	creatorID := uuid.New()
	agentID := uuid.New()
	tokenID := uuid.New()
	expiresAt := time.Date(2026, 6, 20, 13, 0, 0, 0, time.UTC)
	redeemedAt := expiresAt.Add(30 * time.Minute)
	revokedAt := expiresAt.Add(time.Minute)
	lastUsedAt := expiresAt.Add(2 * time.Minute)
	createdAt := expiresAt.Add(-time.Hour)
	tokenValues := agentTokenRow(tokenID, &agentID, creatorID, "active_runtime", &expiresAt, &redeemedAt, &revokedAt, &lastUsedAt, createdAt)
	rows := &fakeRows{rows: [][]any{tokenValues}}
	dbtx := &fakeDBTX{
		row:       fakeRow{values: tokenValues},
		queryRows: rows,
		execTag:   pgconn.NewCommandTag("UPDATE 1"),
	}
	q := New(dbtx)

	token, err := q.CreateAgentToken(context.Background(), CreateAgentTokenParams{
		AgentID:       &agentID,
		CreatorUserID: creatorID,
		Name:          "worker",
		Prefix:        "ol_agent_abcd",
		TokenHash:     "hash",
		Scopes:        []string{"agent:call", "agent:pull"},
		Status:        "active_runtime",
		ExpiresAt:     &expiresAt,
		RedeemedAt:    &redeemedAt,
	})
	if err != nil {
		t.Fatalf("CreateAgentToken error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateAgentToken")
	if token.ID != tokenID || token.AgentID == nil || token.RevokedAt == nil || token.LastUsedAt == nil {
		t.Fatalf("CreateAgentToken scan = %#v", token)
	}

	listed, err := q.ListAgentTokensByCreator(context.Background(), creatorID)
	if err != nil {
		t.Fatalf("ListAgentTokensByCreator error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentTokensByCreator")
	if len(listed) != 1 || listed[0].TokenHash != "hash" {
		t.Fatalf("ListAgentTokensByCreator scan = %#v", listed)
	}

	dbtx.queryRows = &fakeRows{rows: [][]any{tokenValues}}
	filtered, err := q.ListAgentTokensByCreatorAndAgent(context.Background(), ListAgentTokensByCreatorAndAgentParams{CreatorUserID: creatorID, AgentID: agentID})
	if err != nil {
		t.Fatalf("ListAgentTokensByCreatorAndAgent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListAgentTokensByCreatorAndAgent")
	if len(filtered) != 1 || filtered[0].AgentID == nil || *filtered[0].AgentID != agentID {
		t.Fatalf("ListAgentTokensByCreatorAndAgent scan = %#v", filtered)
	}

	activeTokenValues := append([]any{}, tokenValues...)
	activeTokenValues[11] = nil
	activeRows := &fakeRows{rows: [][]any{activeTokenValues}}
	dbtx.queryRows = activeRows
	activeTokens, err := q.ListActiveAgentTokensByPrefix(context.Background(), "ol_agent_abcd")
	if err != nil {
		t.Fatalf("ListActiveAgentTokensByPrefix error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListActiveAgentTokensByPrefix")
	if !activeRows.closed || len(activeTokens) != 1 || activeTokens[0].RevokedAt != nil {
		t.Fatalf("ListActiveAgentTokensByPrefix scan = %#v closed=%v", activeTokens, activeRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{"ol_agent_abcd"}) {
		t.Fatalf("ListActiveAgentTokensByPrefix args = %#v", dbtx.queryArgs)
	}

	affected, err := q.RevokeAgentTokenForCreator(context.Background(), RevokeAgentTokenForCreatorParams{ID: tokenID, CreatorUserID: creatorID})
	if err != nil || affected != 1 {
		t.Fatalf("RevokeAgentTokenForCreator = %d, %v", affected, err)
	}
	requireSQLName(t, dbtx.execSQL, "RevokeAgentTokenForCreator")

	redeemed, err := q.RedeemPendingAgentToken(context.Background(), RedeemPendingAgentTokenParams{
		ID:            tokenID,
		AgentID:       agentID,
		Scopes:        []string{"agent:call"},
		CreatorUserID: creatorID,
	})
	if err != nil {
		t.Fatalf("RedeemPendingAgentToken error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "RedeemPendingAgentToken")
	if redeemed.ID != tokenID || redeemed.AgentID == nil {
		t.Fatalf("RedeemPendingAgentToken scan = %#v", redeemed)
	}

	dbtx.row = fakeRow{values: []any{int32(2)}}
	count, err := q.CountActiveAgentTokensByAgent(context.Background(), agentID)
	if err != nil || count != 2 {
		t.Fatalf("CountActiveAgentTokensByAgent = %d, %v", count, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountActiveAgentTokensByAgent")
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
	historyAgentID := uuid.New()
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

	dbtx.row = fakeRow{values: targetValues}
	gotTarget, err := q.GetDeliveryTargetByID(context.Background(), targetID)
	if err != nil {
		t.Fatalf("GetDeliveryTargetByID error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetDeliveryTargetByID")
	if gotTarget.ID != targetID || gotTarget.UserID != userID {
		t.Fatalf("GetDeliveryTargetByID scan = %#v", gotTarget)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{targetID}) {
		t.Fatalf("GetDeliveryTargetByID args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: targetValues}
	defaultTarget, err := q.GetDefaultDeliveryTarget(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetDefaultDeliveryTarget error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetDefaultDeliveryTarget")
	if defaultTarget.ID != targetID || !defaultTarget.IsDefault {
		t.Fatalf("GetDefaultDeliveryTarget scan = %#v", defaultTarget)
	}

	if err := q.ClearDefaultDeliveryTarget(context.Background(), userID); err != nil {
		t.Fatalf("ClearDefaultDeliveryTarget error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "ClearDefaultDeliveryTarget")
	if !reflect.DeepEqual(dbtx.execArgs, []any{userID}) {
		t.Fatalf("ClearDefaultDeliveryTarget args = %#v", dbtx.execArgs)
	}

	if rows, err := q.SetDeliveryTargetDefault(context.Background(), SetDeliveryTargetDefaultParams{ID: targetID, UserID: userID}); err != nil || rows != 2 {
		t.Fatalf("SetDeliveryTargetDefault = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "SetDeliveryTargetDefault")
	if !reflect.DeepEqual(dbtx.execArgs, []any{targetID, userID}) {
		t.Fatalf("SetDeliveryTargetDefault args = %#v", dbtx.execArgs)
	}

	dbtx.row = fakeRow{values: targetValues}
	updatedTarget, err := q.UpdateDeliveryTargetConfig(context.Background(), UpdateDeliveryTargetConfigParams{ID: targetID, UserID: userID, Config: []byte(`{"url":"https://example.com/hook","event_types":["run.failed"]}`)})
	if err != nil {
		t.Fatalf("UpdateDeliveryTargetConfig error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpdateDeliveryTargetConfig")
	if updatedTarget.ID != targetID || !reflect.DeepEqual(dbtx.queryRowArgs, []any{targetID, userID, []byte(`{"url":"https://example.com/hook","event_types":["run.failed"]}`)}) {
		t.Fatalf("UpdateDeliveryTargetConfig scan/args = %#v/%#v", updatedTarget, dbtx.queryRowArgs)
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

	runDeliveryRows := &fakeRows{rows: [][]any{runDeliveryValues}}
	dbtx.queryRows = runDeliveryRows
	byRun, err := q.ListRunDeliveriesByRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListRunDeliveriesByRun error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunDeliveriesByRun")
	if !runDeliveryRows.closed || len(byRun) != 1 || byRun[0].RunID != runID {
		t.Fatalf("ListRunDeliveriesByRun scan = %#v closed=%v", byRun, runDeliveryRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{runID}) {
		t.Fatalf("ListRunDeliveriesByRun args = %#v", dbtx.queryArgs)
	}

	runDeliveryRows = &fakeRows{rows: [][]any{runDeliveryValues}}
	dbtx.queryRows = runDeliveryRows
	byUser, err := q.ListRunDeliveriesByUser(context.Background(), ListRunDeliveriesByUserParams{
		UserID:     userID,
		HasAgentID: true,
		AgentID:    historyAgentID,
		HasRunID:   true,
		RunID:      runID,
		Status:     "failed",
		Limit:      25,
	})
	if err != nil {
		t.Fatalf("ListRunDeliveriesByUser error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRunDeliveriesByUser")
	if !runDeliveryRows.closed || len(byUser) != 1 || byUser[0].UserID != userID {
		t.Fatalf("ListRunDeliveriesByUser scan = %#v closed=%v", byUser, runDeliveryRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{userID, true, historyAgentID, true, runID, "failed", int32(25)}) {
		t.Fatalf("ListRunDeliveriesByUser args = %#v", dbtx.queryArgs)
	}

	if rows, err := q.DeleteDeliveryTarget(context.Background(), DeleteDeliveryTargetParams{ID: targetID, UserID: userID}); err != nil || rows != 2 {
		t.Fatalf("DeleteDeliveryTarget = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "DeleteDeliveryTarget")
	if rows, err := q.ResetRunDeliveryForRetry(context.Background(), ResetRunDeliveryForRetryParams{ID: deliveryID, UserID: userID}); err != nil || rows != 2 {
		t.Fatalf("ResetRunDeliveryForRetry = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "ResetRunDeliveryForRetry")

	if err := q.MarkRunDeliverySuccess(context.Background(), MarkRunDeliverySuccessParams{ID: deliveryID, ResponseStatus: &status, ResponseBody: &body}); err != nil {
		t.Fatalf("MarkRunDeliverySuccess error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkRunDeliverySuccess")
	if !reflect.DeepEqual(dbtx.execArgs, []any{deliveryID, &status, &body}) {
		t.Fatalf("MarkRunDeliverySuccess args = %#v", dbtx.execArgs)
	}

	if err := q.MarkRunDeliveryFailedFinal(context.Background(), MarkRunDeliveryFailedFinalParams{ID: deliveryID, ResponseStatus: &status, ResponseBody: &body, ErrorMessage: &errMsg}); err != nil {
		t.Fatalf("MarkRunDeliveryFailedFinal error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkRunDeliveryFailedFinal")
	if !reflect.DeepEqual(dbtx.execArgs, []any{deliveryID, &status, &body, &errMsg}) {
		t.Fatalf("MarkRunDeliveryFailedFinal args = %#v", dbtx.execArgs)
	}

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

	if rows, err := q.ClearAgentWebhook(context.Background(), ClearAgentWebhookParams{ID: agentID, CreatorID: creatorID}); err != nil || rows != 4 {
		t.Fatalf("ClearAgentWebhook = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "ClearAgentWebhook")
	if !reflect.DeepEqual(dbtx.execArgs, []any{agentID, creatorID}) {
		t.Fatalf("ClearAgentWebhook args = %#v", dbtx.execArgs)
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

	agentDeliveryRows := &fakeRows{rows: [][]any{deliveryValues}}
	dbtx.queryRows = agentDeliveryRows
	agentDeliveries, err := q.ListDeliveriesByAgent(context.Background(), ListDeliveriesByAgentParams{AgentID: agentID, Limit: 25})
	if err != nil {
		t.Fatalf("ListDeliveriesByAgent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListDeliveriesByAgent")
	if !agentDeliveryRows.closed || len(agentDeliveries) != 1 || agentDeliveries[0].AgentID != agentID {
		t.Fatalf("ListDeliveriesByAgent scan = %#v closed=%v", agentDeliveries, agentDeliveryRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{agentID, int32(25)}) {
		t.Fatalf("ListDeliveriesByAgent args = %#v", dbtx.queryArgs)
	}

	if err := q.MarkDeliverySuccess(context.Background(), MarkDeliverySuccessParams{ID: deliveryID, ResponseStatus: &status, ResponseBody: &body}); err != nil {
		t.Fatalf("MarkDeliverySuccess error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkDeliverySuccess")
	if !reflect.DeepEqual(dbtx.execArgs, []any{deliveryID, &status, &body}) {
		t.Fatalf("MarkDeliverySuccess args = %#v", dbtx.execArgs)
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

	dbtx.row = fakeRow{values: nodeValues}
	ownedNode, err := q.GetRegistryNodeByIDForOwner(context.Background(), GetRegistryNodeByIDForOwnerParams{ID: nodeID, OwnerUserID: ownerID})
	if err != nil {
		t.Fatalf("GetRegistryNodeByIDForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetRegistryNodeByIDForOwner")
	if ownedNode.ID != nodeID || ownedNode.OwnerUserID != ownerID {
		t.Fatalf("GetRegistryNodeByIDForOwner scan = %#v", ownedNode)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{nodeID, ownerID}) {
		t.Fatalf("GetRegistryNodeByIDForOwner args = %#v", dbtx.queryRowArgs)
	}

	activeNodeRows := &fakeRows{rows: [][]any{nodeValues}}
	dbtx.queryRows = activeNodeRows
	activeNodes, err := q.ListActiveRegistryNodesBySecretPrefix(context.Background(), "rn_live_abcd")
	if err != nil {
		t.Fatalf("ListActiveRegistryNodesBySecretPrefix error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListActiveRegistryNodesBySecretPrefix")
	if !activeNodeRows.closed || len(activeNodes) != 1 || activeNodes[0].SecretPrefix != "rn_live_abcd" {
		t.Fatalf("ListActiveRegistryNodesBySecretPrefix scan = %#v closed=%v", activeNodes, activeNodeRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{"rn_live_abcd"}) {
		t.Fatalf("ListActiveRegistryNodesBySecretPrefix args = %#v", dbtx.queryArgs)
	}

	dbtx.row = fakeRow{values: nodeValues}
	heartbeatNode, err := q.MarkRegistryNodeHeartbeat(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("MarkRegistryNodeHeartbeat error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkRegistryNodeHeartbeat")
	if heartbeatNode.ID != nodeID || heartbeatNode.HeartbeatStatus != "healthy" {
		t.Fatalf("MarkRegistryNodeHeartbeat scan = %#v", heartbeatNode)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{nodeID}) {
		t.Fatalf("MarkRegistryNodeHeartbeat args = %#v", dbtx.queryRowArgs)
	}

	revokedAt := now.Add(2 * time.Minute)
	revokedNodeValues := append([]any{}, nodeValues...)
	revokedNodeValues[8] = "revoked"
	revokedNodeValues[10] = &revokedAt
	dbtx.row = fakeRow{values: revokedNodeValues}
	revokedNode, err := q.RevokeRegistryNodeForOwner(context.Background(), RevokeRegistryNodeForOwnerParams{ID: nodeID, OwnerUserID: ownerID})
	if err != nil {
		t.Fatalf("RevokeRegistryNodeForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "RevokeRegistryNodeForOwner")
	if revokedNode.ID != nodeID || revokedNode.HeartbeatStatus != "revoked" || revokedNode.RevokedAt == nil {
		t.Fatalf("RevokeRegistryNodeForOwner scan = %#v", revokedNode)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{nodeID, ownerID}) {
		t.Fatalf("RevokeRegistryNodeForOwner args = %#v", dbtx.queryRowArgs)
	}

	rotatedNodeValues := append([]any{}, nodeValues...)
	rotatedNodeValues[5] = "rn_live_wxyz"
	rotatedNodeValues[6] = "new-hash"
	dbtx.row = fakeRow{values: rotatedNodeValues}
	rotatedNode, err := q.RotateRegistryNodeSecretForOwner(context.Background(), RotateRegistryNodeSecretForOwnerParams{
		ID:           nodeID,
		OwnerUserID:  ownerID,
		SecretPrefix: "rn_live_wxyz",
		SecretHash:   "new-hash",
	})
	if err != nil {
		t.Fatalf("RotateRegistryNodeSecretForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "RotateRegistryNodeSecretForOwner")
	if rotatedNode.ID != nodeID || rotatedNode.SecretPrefix != "rn_live_wxyz" {
		t.Fatalf("RotateRegistryNodeSecretForOwner scan = %#v", rotatedNode)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{nodeID, ownerID, "rn_live_wxyz", "new-hash"}) {
		t.Fatalf("RotateRegistryNodeSecretForOwner args = %#v", dbtx.queryRowArgs)
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

	dbtx.row = fakeRow{values: linkValues}
	ownerLink, err := q.GetCloudListingLinkForOwner(context.Background(), GetCloudListingLinkForOwnerParams{CloudListingID: listingID, OwnerUserID: ownerID})
	if err != nil {
		t.Fatalf("GetCloudListingLinkForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetCloudListingLinkForOwner")
	if ownerLink.ID != linkID || ownerLink.RegistryNodeID != nodeID {
		t.Fatalf("GetCloudListingLinkForOwner scan = %#v", ownerLink)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{listingID, ownerID}) {
		t.Fatalf("GetCloudListingLinkForOwner args = %#v", dbtx.queryRowArgs)
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

	dbtx.row = fakeRow{values: linkRowValues}
	linkRow, err := q.GetCloudListingLinkRowForOwner(context.Background(), GetCloudListingLinkRowForOwnerParams{ID: linkID, OwnerUserID: ownerID})
	if err != nil {
		t.Fatalf("GetCloudListingLinkRowForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetCloudListingLinkRowForOwner")
	if linkRow.ID != linkID || linkRow.NodeName != "edge-one" || linkRow.AgentName != "Local Agent" {
		t.Fatalf("GetCloudListingLinkRowForOwner scan = %#v", linkRow)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{linkID, ownerID}) {
		t.Fatalf("GetCloudListingLinkRowForOwner args = %#v", dbtx.queryRowArgs)
	}

	pausedLinkRowValues := append([]any{}, linkRowValues...)
	pausedLinkRowValues[10] = "paused"
	dbtx.row = fakeRow{values: pausedLinkRowValues}
	pausedLink, err := q.UpdateCloudListingLinkStatusForOwner(context.Background(), UpdateCloudListingLinkStatusForOwnerParams{
		CloudListingID: listingID,
		OwnerUserID:    ownerID,
		SyncStatus:     "paused",
	})
	if err != nil {
		t.Fatalf("UpdateCloudListingLinkStatusForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpdateCloudListingLinkStatusForOwner")
	if pausedLink.ID != linkID || pausedLink.SyncStatus != "paused" {
		t.Fatalf("UpdateCloudListingLinkStatusForOwner scan = %#v", pausedLink)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{listingID, ownerID, "paused"}) {
		t.Fatalf("UpdateCloudListingLinkStatusForOwner args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: linkRowValues}
	syncedLink, err := q.SyncCloudListingMetadataForOwner(context.Background(), SyncCloudListingMetadataForOwnerParams{CloudListingID: listingID, OwnerUserID: ownerID})
	if err != nil {
		t.Fatalf("SyncCloudListingMetadataForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "SyncCloudListingMetadataForOwner")
	if syncedLink.ID != linkID || syncedLink.MetadataSyncedAt == nil || syncedLink.AvailabilityStatus != "healthy" {
		t.Fatalf("SyncCloudListingMetadataForOwner scan = %#v", syncedLink)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{listingID, ownerID}) {
		t.Fatalf("SyncCloudListingMetadataForOwner args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: []any{int32(4)}}
	linkCount, err := q.CountCloudListingLinksByNode(context.Background(), nodeID)
	if err != nil || linkCount != 4 {
		t.Fatalf("CountCloudListingLinksByNode = %d, %v", linkCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountCloudListingLinksByNode")

	dbtx.row = fakeRow{values: []any{int32(5)}}
	syncedCount, err := q.SyncCloudListingMetadataByNode(context.Background(), nodeID)
	if err != nil || syncedCount != 5 {
		t.Fatalf("SyncCloudListingMetadataByNode = %d, %v", syncedCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "SyncCloudListingMetadataByNode")

	dbtx.row = fakeRow{values: []any{int32(6)}}
	count, err := q.CountPendingProxyRunsByNode(context.Background(), nodeID)
	if err != nil || count != 6 {
		t.Fatalf("CountPendingProxyRunsByNode = %d, %v", count, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountPendingProxyRunsByNode")
}

func TestRegistryPeerFederationQueriesScanRowsAndAffectedRows(t *testing.T) {
	ownerID := uuid.New()
	peerID := uuid.New()
	inviteID := uuid.New()
	now := time.Date(2026, 6, 20, 17, 15, 0, 0, time.UTC)
	lastUsed := now.Add(30 * time.Second)
	expiresAt := now.Add(15 * time.Minute)
	consumedAt := now.Add(time.Minute)
	peerValues := registryPeerRow(peerID, ownerID, now, &lastUsed)
	inviteValues := registryFederationInviteRow(inviteID, ownerID, now, expiresAt, &consumedAt)
	dbtx := &fakeDBTX{
		row:       fakeRow{values: peerValues},
		queryRows: &fakeRows{rows: [][]any{peerValues}},
		execTag:   pgconn.NewCommandTag("UPDATE 2"),
	}
	q := New(dbtx)

	peer, err := q.CreateRegistryPeer(context.Background(), CreateRegistryPeerParams{
		OwnerUserID:    ownerID,
		Name:           "Peer",
		APIBaseURL:     "https://peer.example/api/v1",
		BearerToken:    "peer-token",
		CredentialHint: "sha256:abc",
		Status:         "active",
	})
	if err != nil {
		t.Fatalf("CreateRegistryPeer error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRegistryPeer")
	if peer.ID != peerID || peer.BearerToken != "peer-token" || peer.LastUsedAt == nil {
		t.Fatalf("CreateRegistryPeer scan = %#v", peer)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{ownerID, "Peer", "https://peer.example/api/v1", "peer-token", "sha256:abc", "active"}) {
		t.Fatalf("CreateRegistryPeer args = %#v", dbtx.queryRowArgs)
	}

	peerRows := &fakeRows{rows: [][]any{peerValues}}
	dbtx.queryRows = peerRows
	peers, err := q.ListRegistryPeersByOwner(context.Background(), ownerID)
	if err != nil {
		t.Fatalf("ListRegistryPeersByOwner error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListRegistryPeersByOwner")
	if !peerRows.closed || len(peers) != 1 || peers[0].ID != peerID {
		t.Fatalf("ListRegistryPeersByOwner scan = %#v closed=%v", peers, peerRows.closed)
	}

	dbtx.row = fakeRow{values: peerValues}
	activePeer, err := q.GetActiveRegistryPeerForOwner(context.Background(), GetActiveRegistryPeerForOwnerParams{ID: peerID, OwnerUserID: ownerID})
	if err != nil {
		t.Fatalf("GetActiveRegistryPeerForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetActiveRegistryPeerForOwner")
	if activePeer.ID != peerID || activePeer.Status != "active" {
		t.Fatalf("GetActiveRegistryPeerForOwner scan = %#v", activePeer)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{peerID, ownerID}) {
		t.Fatalf("GetActiveRegistryPeerForOwner args = %#v", dbtx.queryRowArgs)
	}

	autoPeerRows := &fakeRows{rows: [][]any{peerValues}}
	dbtx.queryRows = autoPeerRows
	autoPeers, err := q.ListActiveRegistryPeersForAutoRoute(context.Background(), ownerID)
	if err != nil {
		t.Fatalf("ListActiveRegistryPeersForAutoRoute error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListActiveRegistryPeersForAutoRoute")
	if !autoPeerRows.closed || len(autoPeers) != 1 || autoPeers[0].APIBaseURL != "https://peer.example/api/v1" {
		t.Fatalf("ListActiveRegistryPeersForAutoRoute scan = %#v closed=%v", autoPeers, autoPeerRows.closed)
	}

	if rows, err := q.DeleteRegistryPeerForOwner(context.Background(), DeleteRegistryPeerForOwnerParams{ID: peerID, OwnerUserID: ownerID}); err != nil || rows != 2 {
		t.Fatalf("DeleteRegistryPeerForOwner = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "DeleteRegistryPeerForOwner")
	if !reflect.DeepEqual(dbtx.execArgs, []any{peerID, ownerID}) {
		t.Fatalf("DeleteRegistryPeerForOwner args = %#v", dbtx.execArgs)
	}

	if err := q.MarkRegistryPeerUsed(context.Background(), MarkRegistryPeerUsedParams{ID: peerID, OwnerUserID: ownerID}); err != nil {
		t.Fatalf("MarkRegistryPeerUsed error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkRegistryPeerUsed")

	dbtx.row = fakeRow{values: inviteValues}
	invite, err := q.CreateRegistryFederationInvite(context.Background(), CreateRegistryFederationInviteParams{
		OwnerUserID:      ownerID,
		Name:             "Invite",
		APIBaseURL:       "https://peer.example/api/v1",
		BearerToken:      "peer-token",
		TokenPrefix:      "rf_live_abcd",
		TokenHash:        "hash",
		CredentialHint:   "sha256:def",
		ExpiresInSeconds: 900,
	})
	if err != nil {
		t.Fatalf("CreateRegistryFederationInvite error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateRegistryFederationInvite")
	if invite.ID != inviteID || invite.TokenPrefix != "rf_live_abcd" || invite.ConsumedAt == nil {
		t.Fatalf("CreateRegistryFederationInvite scan = %#v", invite)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{ownerID, "Invite", "https://peer.example/api/v1", "peer-token", "rf_live_abcd", "hash", "sha256:def", int32(900)}) {
		t.Fatalf("CreateRegistryFederationInvite args = %#v", dbtx.queryRowArgs)
	}

	inviteRows := &fakeRows{rows: [][]any{inviteValues}}
	dbtx.queryRows = inviteRows
	invites, err := q.ListActiveRegistryFederationInvitesByPrefixForUpdate(context.Background(), "rf_live_abcd")
	if err != nil {
		t.Fatalf("ListActiveRegistryFederationInvitesByPrefixForUpdate error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListActiveRegistryFederationInvitesByPrefixForUpdate")
	if !inviteRows.closed || len(invites) != 1 || invites[0].TokenHash != "hash" {
		t.Fatalf("ListActiveRegistryFederationInvitesByPrefixForUpdate scan = %#v closed=%v", invites, inviteRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{"rf_live_abcd"}) {
		t.Fatalf("ListActiveRegistryFederationInvitesByPrefixForUpdate args = %#v", dbtx.queryArgs)
	}

	if err := q.MarkRegistryFederationInviteExpired(context.Background(), inviteID); err != nil {
		t.Fatalf("MarkRegistryFederationInviteExpired error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkRegistryFederationInviteExpired")

	if rows, err := q.MarkRegistryFederationInviteConsumed(context.Background(), inviteID); err != nil || rows != 2 {
		t.Fatalf("MarkRegistryFederationInviteConsumed = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkRegistryFederationInviteConsumed")
	if !reflect.DeepEqual(dbtx.execArgs, []any{inviteID}) {
		t.Fatalf("MarkRegistryFederationInviteConsumed args = %#v", dbtx.execArgs)
	}
}

func TestProxyRunAndTaskCallbackQueriesScanRowsAndArgs(t *testing.T) {
	ownerID := uuid.New()
	requestingUserID := uuid.New()
	nodeID := uuid.New()
	listingID := uuid.New()
	linkID := uuid.New()
	localAgentID := uuid.New()
	proxyRunID := uuid.New()
	cloudRunID := uuid.New()
	artifactID := uuid.New()
	runID := uuid.New()
	callerAgentID := uuid.New()
	subscriptionID := uuid.New()
	runEventID := uuid.New()
	deliveryID := uuid.New()
	now := time.Date(2026, 6, 20, 17, 30, 0, 0, time.UTC)
	nextRetry := now.Add(2 * time.Minute)
	claimedAt := now.Add(time.Minute)
	finishedAt := now.Add(3 * time.Minute)
	deliveredAt := now.Add(4 * time.Minute)
	inputSummary := "summarized input"
	outputSummary := "summarized output"
	errCode := "PROXY_UPSTREAM"
	errMsg := "node failed"
	mimeType := "application/json"
	fileURI := "https://files.example/proxy-result.json"
	fileName := "proxy-result.json"
	fileSHA := strings.Repeat("e", 64)
	fileSize := int64(512)
	pushScheme := "Bearer"
	pushCredentials := "push-secret"
	responseStatus := int32(202)
	responseBody := "accepted"
	deliveryErr := "retry later"
	linkValues := cloudListingLinkRow(linkID, listingID, nodeID, localAgentID, now)
	proxyValues := proxyRunRow(proxyRunID, cloudRunID, linkID, listingID, nodeID, localAgentID, requestingUserID, now, &inputSummary, &outputSummary, &errCode, &errMsg, &nextRetry, &claimedAt, &finishedAt)
	artifactValues := proxyRunArtifactRow(artifactID, proxyRunID, cloudRunID, now, &mimeType, &fileURI, &fileName, &fileSHA, &fileSize)
	subscriptionValues := taskCallbackSubscriptionRow(subscriptionID, runID, ownerID, &callerAgentID, now, &pushScheme, &pushCredentials, nil)
	deliveryValues := taskCallbackDeliveryRow(deliveryID, subscriptionID, runEventID, now, &responseStatus, &responseBody, &deliveryErr, &nextRetry, &deliveredAt)
	dbtx := &fakeDBTX{
		row:     fakeRow{values: linkValues},
		execTag: pgconn.NewCommandTag("UPDATE 8"),
	}
	q := New(dbtx)

	link, err := q.GetCloudListingLinkForProxyRun(context.Background(), listingID)
	if err != nil {
		t.Fatalf("GetCloudListingLinkForProxyRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetCloudListingLinkForProxyRun")
	if link.ID != linkID || link.RegistryNodeID != nodeID {
		t.Fatalf("GetCloudListingLinkForProxyRun scan = %#v", link)
	}

	dbtx.row = fakeRow{values: proxyValues}
	proxy, err := q.CreateProxyRun(context.Background(), CreateProxyRunParams{
		CloudListingID:     listingID,
		CloudListingLinkID: linkID,
		RequestingUserID:   requestingUserID,
		IdempotencyKey:     "idem-1",
		Input:              []byte(`{"prompt":"hi"}`),
		InputSummary:       &inputSummary,
		NodeInput:          []byte(`{"node":"input"}`),
	})
	if err != nil {
		t.Fatalf("CreateProxyRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateProxyRun")
	if proxy.ID != proxyRunID || proxy.OutputSummary == nil || *proxy.OutputSummary != outputSummary {
		t.Fatalf("CreateProxyRun scan = %#v", proxy)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{listingID, linkID, requestingUserID, "idem-1", []byte(`{"prompt":"hi"}`), &inputSummary, []byte(`{"node":"input"}`)}) {
		t.Fatalf("CreateProxyRun args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: proxyValues}
	if got, err := q.GetProxyRunForRequester(context.Background(), GetProxyRunForRequesterParams{ID: proxyRunID, RequestingUserID: requestingUserID}); err != nil || got.ID != proxyRunID {
		t.Fatalf("GetProxyRunForRequester = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetProxyRunForRequester")

	dbtx.row = fakeRow{values: proxyValues}
	if got, err := q.GetProxyRunForNode(context.Background(), GetProxyRunForNodeParams{ID: proxyRunID, RegistryNodeID: nodeID}); err != nil || got.RegistryNodeID != nodeID {
		t.Fatalf("GetProxyRunForNode = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetProxyRunForNode")

	dbtx.row = fakeRow{values: proxyValues}
	if got, err := q.ClaimPendingProxyRun(context.Background(), nodeID); err != nil || got.Status != "claimed" {
		t.Fatalf("ClaimPendingProxyRun = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ClaimPendingProxyRun")

	dbtx.row = fakeRow{values: proxyValues}
	if got, err := q.CompleteProxyRun(context.Background(), CompleteProxyRunParams{
		ID:             proxyRunID,
		RegistryNodeID: nodeID,
		Status:         "success",
		Output:         []byte(`{"ok":true}`),
		OutputSummary:  &outputSummary,
		ErrorCode:      nil,
		ErrorMessage:   nil,
		Retryable:      false,
		RetryAfterSecs: 0,
	}); err != nil || got.FinishedAt == nil {
		t.Fatalf("CompleteProxyRun = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CompleteProxyRun")

	if err := q.DeleteProxyRunArtifacts(context.Background(), proxyRunID); err != nil {
		t.Fatalf("DeleteProxyRunArtifacts error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "DeleteProxyRunArtifacts")

	dbtx.row = fakeRow{values: artifactValues}
	artifact, err := q.CreateProxyRunArtifact(context.Background(), CreateProxyRunArtifactParams{
		ProxyRunID:       proxyRunID,
		CloudRunID:       cloudRunID,
		SourceArtifactID: "artifact-1",
		ArtifactType:     "json",
		Title:            "Proxy result",
		Content:          []byte(`{"ok":true}`),
		MimeType:         &mimeType,
		FileURI:          &fileURI,
		FileName:         &fileName,
		FileSHA256:       &fileSHA,
		FileSizeBytes:    &fileSize,
	})
	if err != nil {
		t.Fatalf("CreateProxyRunArtifact error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateProxyRunArtifact")
	if artifact.ID != artifactID || artifact.FileURI == nil || *artifact.FileURI != fileURI {
		t.Fatalf("CreateProxyRunArtifact scan = %#v", artifact)
	}

	artifactRows := &fakeRows{rows: [][]any{artifactValues}}
	dbtx.queryRows = artifactRows
	artifacts, err := q.ListProxyRunArtifactsForRequester(context.Background(), ListProxyRunArtifactsForRequesterParams{ProxyRunID: proxyRunID, RequestingUserID: requestingUserID})
	if err != nil {
		t.Fatalf("ListProxyRunArtifactsForRequester error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListProxyRunArtifactsForRequester")
	if !artifactRows.closed || len(artifacts) != 1 || artifacts[0].ID != artifactID {
		t.Fatalf("ListProxyRunArtifactsForRequester scan = %#v closed=%v", artifacts, artifactRows.closed)
	}

	dbtx.row = fakeRow{values: artifactValues}
	if got, err := q.GetProxyRunArtifactForRequester(context.Background(), GetProxyRunArtifactForRequesterParams{ID: artifactID, ProxyRunID: proxyRunID, RequestingUserID: requestingUserID}); err != nil || got.ProxyRunID != proxyRunID {
		t.Fatalf("GetProxyRunArtifactForRequester = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetProxyRunArtifactForRequester")

	dbtx.row = fakeRow{values: []any{int32(3)}}
	timedOut, err := q.TimeoutStaleProxyRuns(context.Background(), now.Add(-time.Hour))
	if err != nil || timedOut != 3 {
		t.Fatalf("TimeoutStaleProxyRuns = %d, %v", timedOut, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "TimeoutStaleProxyRuns")

	dbtx.row = fakeRow{values: subscriptionValues}
	subscription, err := q.CreateTaskCallbackSubscription(context.Background(), CreateTaskCallbackSubscriptionParams{
		RunID:           runID,
		OwnerUserID:     ownerID,
		CallerAgentID:   &callerAgentID,
		TargetURL:       "https://hooks.example/run",
		Secret:          "secret",
		EventTypes:      []string{"completed", "artifact"},
		AuthScheme:      &pushScheme,
		AuthCredentials: &pushCredentials,
		Metadata:        []byte(`{"source":"a2a"}`),
	})
	if err != nil {
		t.Fatalf("CreateTaskCallbackSubscription error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateTaskCallbackSubscription")
	if subscription.ID != subscriptionID || subscription.AuthScheme == nil || *subscription.AuthScheme != pushScheme {
		t.Fatalf("CreateTaskCallbackSubscription scan = %#v", subscription)
	}

	subscriptionRows := &fakeRows{rows: [][]any{subscriptionValues}}
	dbtx.queryRows = subscriptionRows
	byRun, err := q.ListTaskCallbackSubscriptionsByRun(context.Background(), ListTaskCallbackSubscriptionsByRunParams{RunID: runID, OwnerUserID: ownerID})
	if err != nil {
		t.Fatalf("ListTaskCallbackSubscriptionsByRun error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListTaskCallbackSubscriptionsByRun")
	if !subscriptionRows.closed || len(byRun) != 1 || byRun[0].ID != subscriptionID {
		t.Fatalf("ListTaskCallbackSubscriptionsByRun scan = %#v closed=%v", byRun, subscriptionRows.closed)
	}

	ownerRows := &fakeRows{rows: [][]any{subscriptionValues}}
	dbtx.queryRows = ownerRows
	byOwner, err := q.ListTaskCallbackSubscriptionsByOwner(context.Background(), ListTaskCallbackSubscriptionsByOwnerParams{OwnerUserID: ownerID, Status: "active", Limit: 20})
	if err != nil {
		t.Fatalf("ListTaskCallbackSubscriptionsByOwner error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListTaskCallbackSubscriptionsByOwner")
	if !ownerRows.closed || len(byOwner) != 1 || byOwner[0].RunID != runID {
		t.Fatalf("ListTaskCallbackSubscriptionsByOwner scan = %#v closed=%v", byOwner, ownerRows.closed)
	}

	if rows, err := q.DeleteTaskCallbackSubscriptionForOwner(context.Background(), DeleteTaskCallbackSubscriptionForOwnerParams{ID: subscriptionID, RunID: runID, OwnerUserID: ownerID}); err != nil || rows != 8 {
		t.Fatalf("DeleteTaskCallbackSubscriptionForOwner = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "DeleteTaskCallbackSubscriptionForOwner")

	dbtx.row = fakeRow{values: subscriptionValues}
	if got, err := q.UpdateTaskCallbackSubscriptionStatusForOwner(context.Background(), UpdateTaskCallbackSubscriptionStatusForOwnerParams{ID: subscriptionID, RunID: runID, OwnerUserID: ownerID, Status: "paused"}); err != nil || got.ID != subscriptionID {
		t.Fatalf("UpdateTaskCallbackSubscriptionStatusForOwner = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpdateTaskCallbackSubscriptionStatusForOwner")

	batchRows := &fakeRows{rows: [][]any{subscriptionValues}}
	dbtx.queryRows = batchRows
	batchUpdated, err := q.BatchUpdateTaskCallbackSubscriptionsForOwner(context.Background(), BatchUpdateTaskCallbackSubscriptionsForOwnerParams{OwnerUserID: ownerID, IDs: []uuid.UUID{subscriptionID}, Status: "active"})
	if err != nil {
		t.Fatalf("BatchUpdateTaskCallbackSubscriptionsForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "BatchUpdateTaskCallbackSubscriptionsForOwner")
	if !batchRows.closed || len(batchUpdated) != 1 || batchUpdated[0].Status != "active" {
		t.Fatalf("BatchUpdateTaskCallbackSubscriptionsForOwner scan = %#v closed=%v", batchUpdated, batchRows.closed)
	}

	activeRows := &fakeRows{rows: [][]any{subscriptionValues}}
	dbtx.queryRows = activeRows
	active, err := q.ListActiveTaskCallbackSubscriptionsForEvent(context.Background(), ListActiveTaskCallbackSubscriptionsForEventParams{RunID: runID, EventType: "completed"})
	if err != nil {
		t.Fatalf("ListActiveTaskCallbackSubscriptionsForEvent error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListActiveTaskCallbackSubscriptionsForEvent")
	if !activeRows.closed || len(active) != 1 || active[0].ID != subscriptionID {
		t.Fatalf("ListActiveTaskCallbackSubscriptionsForEvent scan = %#v closed=%v", active, activeRows.closed)
	}

	dbtx.row = fakeRow{values: deliveryValues}
	delivery, err := q.CreateTaskCallbackDelivery(context.Background(), CreateTaskCallbackDeliveryParams{SubscriptionID: subscriptionID, RunEventID: runEventID, Payload: []byte(`{"event":"completed"}`)})
	if err != nil {
		t.Fatalf("CreateTaskCallbackDelivery error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateTaskCallbackDelivery")
	if delivery.ID != deliveryID || delivery.NextRetryAt == nil {
		t.Fatalf("CreateTaskCallbackDelivery scan = %#v", delivery)
	}

	deliveryWithTarget := append(append([]any{}, deliveryValues...), "https://hooks.example/run", "secret", &pushScheme, &pushCredentials, "completed")
	dbtx.row = fakeRow{values: deliveryWithTarget}
	deliveryDetail, err := q.GetTaskCallbackDeliveryByID(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetTaskCallbackDeliveryByID error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetTaskCallbackDeliveryByID")
	if deliveryDetail.ID != deliveryID || deliveryDetail.TargetURL == "" || deliveryDetail.EventType != "completed" {
		t.Fatalf("GetTaskCallbackDeliveryByID scan = %#v", deliveryDetail)
	}

	deliveryListValues := append(append([]any{}, deliveryValues...), "https://hooks.example/run", "completed")
	deliveryListRows := &fakeRows{rows: [][]any{deliveryListValues}}
	dbtx.queryRows = deliveryListRows
	deliveryList, err := q.ListTaskCallbackDeliveriesByRun(context.Background(), ListTaskCallbackDeliveriesByRunParams{RunID: runID, OwnerUserID: ownerID, Limit: 10})
	if err != nil {
		t.Fatalf("ListTaskCallbackDeliveriesByRun error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListTaskCallbackDeliveriesByRun")
	if !deliveryListRows.closed || len(deliveryList) != 1 || deliveryList[0].TargetURL == "" || deliveryList[0].EventType != "completed" {
		t.Fatalf("ListTaskCallbackDeliveriesByRun scan = %#v closed=%v", deliveryList, deliveryListRows.closed)
	}

	if err := q.MarkTaskCallbackDeliverySuccess(context.Background(), MarkTaskCallbackDeliverySuccessParams{ID: deliveryID, ResponseStatus: &responseStatus, ResponseBody: &responseBody}); err != nil {
		t.Fatalf("MarkTaskCallbackDeliverySuccess error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkTaskCallbackDeliverySuccess")

	if err := q.MarkTaskCallbackDeliveryFailedRetry(context.Background(), MarkTaskCallbackDeliveryFailedRetryParams{ID: deliveryID, ResponseStatus: &responseStatus, ResponseBody: &responseBody, ErrorMessage: &deliveryErr, NextRetryAt: nextRetry}); err != nil {
		t.Fatalf("MarkTaskCallbackDeliveryFailedRetry error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkTaskCallbackDeliveryFailedRetry")

	if err := q.MarkTaskCallbackDeliveryFailedFinal(context.Background(), MarkTaskCallbackDeliveryFailedFinalParams{ID: deliveryID, ResponseStatus: &responseStatus, ResponseBody: &responseBody, ErrorMessage: &deliveryErr}); err != nil {
		t.Fatalf("MarkTaskCallbackDeliveryFailedFinal error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "MarkTaskCallbackDeliveryFailedFinal")

	if err := q.IncrementTaskCallbackSubscriptionFailure(context.Background(), subscriptionID); err != nil {
		t.Fatalf("IncrementTaskCallbackSubscriptionFailure error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "IncrementTaskCallbackSubscriptionFailure")

	if err := q.ResetTaskCallbackSubscriptionFailures(context.Background(), subscriptionID); err != nil {
		t.Fatalf("ResetTaskCallbackSubscriptionFailures error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "ResetTaskCallbackSubscriptionFailures")

	pendingDeliveryRows := &fakeRows{rows: [][]any{deliveryValues}}
	dbtx.queryRows = pendingDeliveryRows
	pendingDeliveries, err := q.ListPendingTaskCallbackDeliveries(context.Background())
	if err != nil {
		t.Fatalf("ListPendingTaskCallbackDeliveries error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListPendingTaskCallbackDeliveries")
	if !pendingDeliveryRows.closed || len(pendingDeliveries) != 1 || pendingDeliveries[0].ID != deliveryID {
		t.Fatalf("ListPendingTaskCallbackDeliveries scan = %#v closed=%v", pendingDeliveries, pendingDeliveryRows.closed)
	}

	dbtx.row = fakeRow{values: runEventRow(runEventID, runID, nil, 12, "completed", []byte(`{"ok":true}`), now)}
	latestEvent, err := q.GetLatestRunEventForTypes(context.Background(), GetLatestRunEventForTypesParams{RunID: runID, EventTypes: []string{"completed", "failed"}})
	if err != nil {
		t.Fatalf("GetLatestRunEventForTypes error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetLatestRunEventForTypes")
	if latestEvent.ID != runEventID || latestEvent.Sequence != 12 {
		t.Fatalf("GetLatestRunEventForTypes scan = %#v", latestEvent)
	}
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
		Prefix:          "ol_agent_abcd",
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
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID, userID, "runtime", "ol_agent_abcd", "hash", []string{"agent:pull"}}) {
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

	activeTokenRows := &fakeRows{rows: [][]any{tokenValues}}
	dbtx.queryRows = activeTokenRows
	activeTokens, err := q.ListActiveAgentRuntimeTokensByPrefix(context.Background(), "ol_agent_abcd")
	if err != nil {
		t.Fatalf("ListActiveAgentRuntimeTokensByPrefix error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListActiveAgentRuntimeTokensByPrefix")
	if !activeTokenRows.closed || len(activeTokens) != 1 || activeTokens[0].ID != tokenID {
		t.Fatalf("ListActiveAgentRuntimeTokensByPrefix scan = %#v closed=%v", activeTokens, activeTokenRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{"ol_agent_abcd"}) {
		t.Fatalf("ListActiveAgentRuntimeTokensByPrefix args = %#v", dbtx.queryArgs)
	}

	if err := q.TouchAgentRuntimeToken(context.Background(), tokenID); err != nil {
		t.Fatalf("TouchAgentRuntimeToken error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "TouchAgentRuntimeToken")
	if !reflect.DeepEqual(dbtx.execArgs, []any{tokenID}) {
		t.Fatalf("TouchAgentRuntimeToken args = %#v", dbtx.execArgs)
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

	dbtx.row = fakeRow{values: []any{"private"}}
	callPolicy, err := q.GetAgentCallPolicy(context.Background(), agentID)
	if err != nil || callPolicy != "private" {
		t.Fatalf("GetAgentCallPolicy = %q, %v", callPolicy, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentCallPolicy")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID}) {
		t.Fatalf("GetAgentCallPolicy args = %#v", dbtx.queryRowArgs)
	}

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

	dbtx.row = fakeRow{values: runDelegationRow(childRunID, parentRunID, callerAgentID, now)}
	gotDelegation, err := q.GetRunDelegationByChild(context.Background(), childRunID)
	if err != nil {
		t.Fatalf("GetRunDelegationByChild error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetRunDelegationByChild")
	if gotDelegation.ChildRunID != childRunID || gotDelegation.ParentRunID != parentRunID {
		t.Fatalf("GetRunDelegationByChild scan = %#v", gotDelegation)
	}

	lineageRows := &fakeRows{rows: [][]any{{agentID}, {callerAgentID}}}
	dbtx.queryRows = lineageRows
	lineage, err := q.ListDelegationLineage(context.Background(), ListDelegationLineageParams{RunID: childRunID, MaxDepth: 9})
	if err != nil {
		t.Fatalf("ListDelegationLineage error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListDelegationLineage")
	if !lineageRows.closed || len(lineage) != 2 || lineage[1] != callerAgentID {
		t.Fatalf("ListDelegationLineage scan = %#v closed=%v", lineage, lineageRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{childRunID, int32(9)}) {
		t.Fatalf("ListDelegationLineage args = %#v", dbtx.queryArgs)
	}

	dbtx.row = fakeRow{values: []any{int32(4)}}
	runningDelegations, err := q.CountRunningDelegations(context.Background())
	if err != nil || runningDelegations != 4 {
		t.Fatalf("CountRunningDelegations = %d, %v", runningDelegations, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountRunningDelegations")
	if len(dbtx.queryRowArgs) != 0 {
		t.Fatalf("CountRunningDelegations args = %#v", dbtx.queryRowArgs)
	}

	targetAgentID := uuid.New()
	mappingID := uuid.New()
	dbtx.row = fakeRow{values: a2aContextMappingRow(mappingID, childRunID, userID, agentID, parentRunID, callerAgentID, targetAgentID, now)}
	mapping, err := q.UpsertA2AContextMapping(context.Background(), UpsertA2AContextMappingParams{
		RunID:             childRunID,
		UserID:            userID,
		AgentID:           agentID,
		ProtocolContextID: "ctx-root",
		ProtocolTaskID:    "task-child",
		RootContextID:     "ctx-root",
		ParentContextID:   "ctx-root",
		ParentTaskID:      "task-parent",
		ParentRunID:       &parentRunID,
		CallerAgentID:     &callerAgentID,
		TargetAgentID:     &targetAgentID,
		TraceID:           "trace-root",
		ReferenceTaskIDs:  []string{"task-parent"},
		Source:            "agent_delegation",
	})
	if err != nil {
		t.Fatalf("UpsertA2AContextMapping error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpsertA2AContextMapping")
	if mapping.ID != mappingID || mapping.ProtocolContextID != "ctx-root" || mapping.ParentRunID == nil {
		t.Fatalf("UpsertA2AContextMapping scan = %#v", mapping)
	}

	dbtx.row = fakeRow{values: a2aContextMappingRow(mappingID, childRunID, userID, agentID, parentRunID, callerAgentID, targetAgentID, now)}
	gotMapping, err := q.GetA2AContextMappingByRun(context.Background(), childRunID)
	if err != nil {
		t.Fatalf("GetA2AContextMappingByRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetA2AContextMappingByRun")
	if gotMapping.RunID != childRunID || gotMapping.RootContextID != "ctx-root" {
		t.Fatalf("GetA2AContextMappingByRun scan = %#v", gotMapping)
	}

	mappingRows := &fakeRows{rows: [][]any{
		a2aContextMappingRow(mappingID, childRunID, userID, agentID, parentRunID, callerAgentID, targetAgentID, now),
	}}
	dbtx.queryRows = mappingRows
	mappings, err := q.ListA2AContextMappingsByRoot(context.Background(), ListA2AContextMappingsByRootParams{
		UserID: userID, RootContextID: "ctx-root", Limit: 20,
	})
	if err != nil {
		t.Fatalf("ListA2AContextMappingsByRoot error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListA2AContextMappingsByRoot")
	if !mappingRows.closed || len(mappings) != 1 || mappings[0].TraceID != "trace-root" {
		t.Fatalf("ListA2AContextMappingsByRoot scan = %#v closed=%v", mappings, mappingRows.closed)
	}

	duration := int32(250)
	finishedAt := now.Add(time.Minute)
	childRows := &fakeRows{rows: [][]any{{
		childRunID,
		parentRunID,
		callerAgentID,
		"delegate analysis",
		"success",
		int32(15),
		&duration,
		now,
		&finishedAt,
		"a2a",
		"caller-slug",
		"Caller Agent",
		[]string{"orchestration"},
		[]string{"ai/agent-orchestration"},
		[]string{"Agent orchestration"},
		targetAgentID,
		"target-slug",
		"Target Agent",
		[]string{"data"},
		[]string{"data/sql-query"},
		[]string{"SQL query"},
		"ctx-root",
		"task-child",
		"ctx-root",
		"ctx-root",
		"task-parent",
		"trace-root",
		[]string{"task-parent"},
		"agent_delegation",
	}}}
	dbtx.queryRows = childRows
	childRuns, err := q.ListChildRunsByParentAndUser(context.Background(), ListChildRunsByParentAndUserParams{ParentRunID: parentRunID, UserID: userID})
	if err != nil {
		t.Fatalf("ListChildRunsByParentAndUser error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListChildRunsByParentAndUser")
	if !childRows.closed || len(childRuns) != 1 || childRuns[0].TargetAgentID != targetAgentID || childRuns[0].CallerSkillIDs[0] != "ai/agent-orchestration" {
		t.Fatalf("ListChildRunsByParentAndUser scan = %#v closed=%v", childRuns, childRows.closed)
	}
	if childRuns[0].RootContextID != "ctx-root" || childRuns[0].ContextSource != "agent_delegation" {
		t.Fatalf("ListChildRunsByParentAndUser context = %#v", childRuns[0])
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{parentRunID, userID}) {
		t.Fatalf("ListChildRunsByParentAndUser args = %#v", dbtx.queryArgs)
	}

	parentRows := &fakeRows{rows: [][]any{{
		parentRunID,
		callerAgentID,
		"caller-slug",
		"Caller Agent",
		[]string{"orchestration"},
		[]string{"ai/agent-orchestration"},
		[]string{"Agent orchestration"},
		"api",
		"success",
		&duration,
		now,
		&finishedAt,
		int32(2),
		int32(1),
		int32(1),
		int32(3),
		&lastUsedAt,
		"ctx-root",
		"task-parent",
		"ctx-root",
		"trace-root",
	}}}
	dbtx.queryRows = parentRows
	parentRuns, err := q.ListParentRunsWithDelegationsByUser(context.Background(), ListParentRunsWithDelegationsByUserParams{UserID: userID, Search: "caller", Limit: 20, Offset: 5})
	if err != nil {
		t.Fatalf("ListParentRunsWithDelegationsByUser error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListParentRunsWithDelegationsByUser")
	if !parentRows.closed || len(parentRuns) != 1 || parentRuns[0].ChildCount != 2 || parentRuns[0].ActiveRuntimeTokenCount != 3 {
		t.Fatalf("ListParentRunsWithDelegationsByUser scan = %#v closed=%v", parentRuns, parentRows.closed)
	}
	if parentRuns[0].RootContextID != "ctx-root" || parentRuns[0].TraceID != "trace-root" {
		t.Fatalf("ListParentRunsWithDelegationsByUser context = %#v", parentRuns[0])
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{userID, "caller", int32(20), int32(5)}) {
		t.Fatalf("ListParentRunsWithDelegationsByUser args = %#v", dbtx.queryArgs)
	}

	dbtx.row = fakeRow{values: []any{int32(6)}}
	parentRunCount, err := q.CountParentRunsWithDelegationsByUser(context.Background(), CountParentRunsWithDelegationsByUserParams{UserID: userID, Search: "caller"})
	if err != nil || parentRunCount != 6 {
		t.Fatalf("CountParentRunsWithDelegationsByUser = %d, %v", parentRunCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountParentRunsWithDelegationsByUser")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{userID, "caller"}) {
		t.Fatalf("CountParentRunsWithDelegationsByUser args = %#v", dbtx.queryRowArgs)
	}

	if rows, err := q.RevokeAgentRuntimeTokenForOwner(context.Background(), RevokeAgentRuntimeTokenForOwnerParams{ID: tokenID, UserID: userID}); err != nil || rows != 5 {
		t.Fatalf("RevokeAgentRuntimeTokenForOwner = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "RevokeAgentRuntimeTokenForOwner")
}

func TestGeneratedListQueriesPropagateQueryErrors(t *testing.T) {
	sentinel := errors.New("query failed")
	ctx := context.Background()
	id := uuid.New()
	now := time.Date(2026, 6, 20, 20, 0, 0, 0, time.UTC)
	q := New(&fakeDBTX{queryErr: sentinel})

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "ListAgentRuntimeTokensForOwner", run: func() error {
			_, err := q.ListAgentRuntimeTokensForOwner(ctx, ListAgentRuntimeTokensForOwnerParams{AgentID: id, UserID: id})
			return err
		}},
		{name: "ListActiveAgentRuntimeTokensByPrefix", run: func() error {
			_, err := q.ListActiveAgentRuntimeTokensByPrefix(ctx, "ol_agent_abcd")
			return err
		}},
		{name: "ListChildRunsByParentAndUser", run: func() error {
			_, err := q.ListChildRunsByParentAndUser(ctx, ListChildRunsByParentAndUserParams{ParentRunID: id, UserID: id})
			return err
		}},
		{name: "ListParentRunsWithDelegationsByUser", run: func() error {
			_, err := q.ListParentRunsWithDelegationsByUser(ctx, ListParentRunsWithDelegationsByUserParams{UserID: id, Limit: 10})
			return err
		}},
		{name: "ListAgentApprovalsForCreator", run: func() error {
			_, err := q.ListAgentApprovalsForCreator(ctx, id)
			return err
		}},
		{name: "ListAgentsDueAvailabilityCheck", run: func() error {
			_, err := q.ListAgentsDueAvailabilityCheck(ctx, ListAgentsDueAvailabilityCheckParams{StaleSeconds: 60, Limit: 5})
			return err
		}},
		{name: "ListAgentAvailabilityAlertsByCreator", run: func() error {
			_, err := q.ListAgentAvailabilityAlertsByCreator(ctx, ListAgentAvailabilityAlertsByCreatorParams{CreatorID: id, Limit: 5})
			return err
		}},
		{name: "ListAgentMetricSnapshotsByAgent", run: func() error {
			_, err := q.ListAgentMetricSnapshotsByAgent(ctx, id)
			return err
		}},
		{name: "AggregateAgentRunsForWindow", run: func() error {
			_, err := q.AggregateAgentRunsForWindow(ctx, "24 hours")
			return err
		}},
		{name: "ListAgentTokensByCreator", run: func() error {
			_, err := q.ListAgentTokensByCreator(ctx, id)
			return err
		}},
		{name: "ListActiveAgentTokensByPrefix", run: func() error {
			_, err := q.ListActiveAgentTokensByPrefix(ctx, "ol_agent_abcd")
			return err
		}},
		{name: "ListAgentsByCreator", run: func() error {
			_, err := q.ListAgentsByCreator(ctx, id)
			return err
		}},
		{name: "ListAdminUsers", run: func() error {
			_, err := q.ListAdminUsers(ctx, ListAdminUsersParams{Query: "user", Role: "admin", Limit: 10})
			return err
		}},
		{name: "ListAdminAgents", run: func() error {
			_, err := q.ListAdminAgents(ctx, ListAdminAgentsParams{Query: "agent", LifecycleStatus: "active", Limit: 10})
			return err
		}},
		{name: "ListPendingAgents", run: func() error {
			_, err := q.ListPendingAgents(ctx)
			return err
		}},
		{name: "ListPublicAgents", run: func() error {
			_, err := q.ListPublicAgents(ctx, ListPublicAgentsParams{Tags: []string{"data"}, Keyword: "agent", Limit: 10, CallableOnly: true})
			return err
		}},
		{name: "ListTestCasesBySkill", run: func() error {
			_, err := q.ListTestCasesBySkill(ctx, "data/sql-query")
			return err
		}},
		{name: "ListBenchmarkRunsByBatch", run: func() error {
			_, err := q.ListBenchmarkRunsByBatch(ctx, id)
			return err
		}},
		{name: "ListAgentSkillScoresByAgent", run: func() error {
			_, err := q.ListAgentSkillScoresByAgent(ctx, id)
			return err
		}},
		{name: "ListAgentSkillScoresBySlug", run: func() error {
			_, err := q.ListAgentSkillScoresBySlug(ctx, "agent-one")
			return err
		}},
		{name: "ListTopAgentsBySkill", run: func() error {
			_, err := q.ListTopAgentsBySkill(ctx, ListTopAgentsBySkillParams{SkillID: "data/sql-query", Limit: 5})
			return err
		}},
		{name: "ListAgentsBySkillsWithVerified", run: func() error {
			_, err := q.ListAgentsBySkillsWithVerified(ctx, []string{"data/sql-query"})
			return err
		}},
		{name: "ListBenchmarkBatchSummariesByAgent", run: func() error {
			_, err := q.ListBenchmarkBatchSummariesByAgent(ctx, ListBenchmarkBatchSummariesByAgentParams{AgentID: id, Limit: 5})
			return err
		}},
		{name: "ListAgentExamplesByAgentID", run: func() error {
			_, err := q.ListAgentExamplesByAgentID(ctx, id)
			return err
		}},
		{name: "ListAgentExamplesBySlug", run: func() error {
			_, err := q.ListAgentExamplesBySlug(ctx, "agent-one")
			return err
		}},
		{name: "ListDeliveryTargetsByUser", run: func() error {
			_, err := q.ListDeliveryTargetsByUser(ctx, id)
			return err
		}},
		{name: "ListPendingRunDeliveries", run: func() error {
			_, err := q.ListPendingRunDeliveries(ctx)
			return err
		}},
		{name: "ListRunDeliveriesByRun", run: func() error {
			_, err := q.ListRunDeliveriesByRun(ctx, id)
			return err
		}},
		{name: "ListRegistryNodesByOwner", run: func() error {
			_, err := q.ListRegistryNodesByOwner(ctx, id)
			return err
		}},
		{name: "ListActiveRegistryNodesBySecretPrefix", run: func() error {
			_, err := q.ListActiveRegistryNodesBySecretPrefix(ctx, "rn_live_abcd")
			return err
		}},
		{name: "ListCloudListingLinksByOwner", run: func() error {
			_, err := q.ListCloudListingLinksByOwner(ctx, id)
			return err
		}},
		{name: "ListProxyRunArtifactsForRequester", run: func() error {
			_, err := q.ListProxyRunArtifactsForRequester(ctx, ListProxyRunArtifactsForRequesterParams{ProxyRunID: id, RequestingUserID: id})
			return err
		}},
		{name: "ListRunArtifactsByRun", run: func() error {
			_, err := q.ListRunArtifactsByRun(ctx, id)
			return err
		}},
		{name: "ListRunArtifactChunksByRun", run: func() error {
			_, err := q.ListRunArtifactChunksByRun(ctx, id)
			return err
		}},
		{name: "ListRunEventsByRun", run: func() error {
			_, err := q.ListRunEventsByRun(ctx, ListRunEventsByRunParams{RunID: id, Limit: 10})
			return err
		}},
		{name: "ListRunMessagesByRun", run: func() error {
			_, err := q.ListRunMessagesByRun(ctx, id)
			return err
		}},
		{name: "ListTaskCallbackSubscriptionsByRun", run: func() error {
			_, err := q.ListTaskCallbackSubscriptionsByRun(ctx, ListTaskCallbackSubscriptionsByRunParams{RunID: id, OwnerUserID: id})
			return err
		}},
		{name: "ListTaskCallbackSubscriptionsByOwner", run: func() error {
			_, err := q.ListTaskCallbackSubscriptionsByOwner(ctx, ListTaskCallbackSubscriptionsByOwnerParams{OwnerUserID: id, Status: "active", Limit: 10})
			return err
		}},
		{name: "BatchUpdateTaskCallbackSubscriptionsForOwner", run: func() error {
			_, err := q.BatchUpdateTaskCallbackSubscriptionsForOwner(ctx, BatchUpdateTaskCallbackSubscriptionsForOwnerParams{OwnerUserID: id, IDs: []uuid.UUID{id}, Status: "active"})
			return err
		}},
		{name: "ListActiveTaskCallbackSubscriptionsForEvent", run: func() error {
			_, err := q.ListActiveTaskCallbackSubscriptionsForEvent(ctx, ListActiveTaskCallbackSubscriptionsForEventParams{RunID: id, EventType: "completed"})
			return err
		}},
		{name: "ListTaskCallbackDeliveriesByRun", run: func() error {
			_, err := q.ListTaskCallbackDeliveriesByRun(ctx, ListTaskCallbackDeliveriesByRunParams{RunID: id, OwnerUserID: id, Limit: 10})
			return err
		}},
		{name: "ListPendingTaskCallbackDeliveries", run: func() error {
			_, err := q.ListPendingTaskCallbackDeliveries(ctx)
			return err
		}},
		{name: "ListStaleRuntimePullRuns", run: func() error {
			_, err := q.ListStaleRuntimePullRuns(ctx, ListStaleRuntimePullRunsParams{DispatchStaleBefore: now, ResultStaleBefore: now, Limit: 10})
			return err
		}},
		{name: "ListStaleEndpointRuns", run: func() error {
			_, err := q.ListStaleEndpointRuns(ctx, ListStaleEndpointRunsParams{StaleBefore: now, Limit: 10})
			return err
		}},
		{name: "ListRunsByUser", run: func() error {
			_, err := q.ListRunsByUser(ctx, ListRunsByUserParams{UserID: id, Limit: 10})
			return err
		}},
		{name: "ListRunsByUserWithAgent", run: func() error {
			_, err := q.ListRunsByUserWithAgent(ctx, ListRunsByUserWithAgentParams{UserID: id, Limit: 10})
			return err
		}},
		{name: "ListRunsByUserAndAgent", run: func() error {
			_, err := q.ListRunsByUserAndAgent(ctx, ListRunsByUserAndAgentParams{UserID: id, AgentID: id, NoCursor: true, NoStatusFilter: true, NoSinceFilter: true, Limit: 10})
			return err
		}},
		{name: "ListRunsByCreatorAgentWithAgent", run: func() error {
			_, err := q.ListRunsByCreatorAgentWithAgent(ctx, ListRunsByCreatorAgentWithAgentParams{CreatorID: id, AgentID: id, Limit: 10})
			return err
		}},
		{name: "ListAgentStatsForCreator", run: func() error {
			_, err := q.ListAgentStatsForCreator(ctx, id)
			return err
		}},
		{name: "ListSkills", run: func() error {
			_, err := q.ListSkills(ctx)
			return err
		}},
		{name: "ListAgentSkills", run: func() error {
			_, err := q.ListAgentSkills(ctx, id)
			return err
		}},
		{name: "ListAgentsBySkills", run: func() error {
			_, err := q.ListAgentsBySkills(ctx, []string{"data/sql-query"})
			return err
		}},
		{name: "GetAgentsByIDs", run: func() error {
			_, err := q.GetAgentsByIDs(ctx, []uuid.UUID{id})
			return err
		}},
		{name: "ListTaskQueriesByUser", run: func() error {
			_, err := q.ListTaskQueriesByUser(ctx, ListTaskQueriesByUserParams{UserID: id, Limit: 10})
			return err
		}},
		{name: "ListPublicTaskQueries", run: func() error {
			_, err := q.ListPublicTaskQueries(ctx, 10)
			return err
		}},
		{name: "ListPendingDeliveries", run: func() error {
			_, err := q.ListPendingDeliveries(ctx)
			return err
		}},
		{name: "ListDeliveriesByAgent", run: func() error {
			_, err := q.ListDeliveriesByAgent(ctx, ListDeliveriesByAgentParams{AgentID: id, Limit: 10})
			return err
		}},
		{name: "ListWorkflowNodes", run: func() error {
			_, err := q.ListWorkflowNodes(ctx, id)
			return err
		}},
		{name: "ListWorkflowNodesByWorkflowIDs", run: func() error {
			_, err := q.ListWorkflowNodesByWorkflowIDs(ctx, []uuid.UUID{id})
			return err
		}},
		{name: "ListWorkflowsByUser", run: func() error {
			_, err := q.ListWorkflowsByUser(ctx, ListWorkflowsByUserParams{UserID: id, Limit: 10})
			return err
		}},
		{name: "ListWorkflowRunsByWorkflow", run: func() error {
			_, err := q.ListWorkflowRunsByWorkflow(ctx, ListWorkflowRunsByWorkflowParams{WorkflowID: id, Limit: 10})
			return err
		}},
		{name: "ListWorkflowRunSteps", run: func() error {
			_, err := q.ListWorkflowRunSteps(ctx, id)
			return err
		}},
		{name: "ListWorkflowRunStepsByRunIDs", run: func() error {
			_, err := q.ListWorkflowRunStepsByRunIDs(ctx, []uuid.UUID{id})
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want %v", err, sentinel)
			}
		})
	}
}

func TestGeneratedExecQueriesPropagateExecErrors(t *testing.T) {
	sentinel := errors.New("exec failed")
	ctx := context.Background()
	id := uuid.New()
	message := "network timeout"
	url := "https://example.com/webhook"
	secret := "secret"
	q := New(&fakeDBTX{execErr: sentinel})

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "RevokeAgentRuntimeTokenForOwner", run: func() error {
			_, err := q.RevokeAgentRuntimeTokenForOwner(ctx, RevokeAgentRuntimeTokenForOwnerParams{ID: id, UserID: id})
			return err
		}},
		{name: "ConfirmAgentApproval", run: func() error {
			_, err := q.ConfirmAgentApproval(ctx, ConfirmAgentApprovalParams{ID: id, CreatorID: id, DecisionNote: &message})
			return err
		}},
		{name: "RejectAgentApproval", run: func() error {
			_, err := q.RejectAgentApproval(ctx, RejectAgentApprovalParams{ID: id, CreatorID: id, DecisionNote: &message})
			return err
		}},
		{name: "ExpireAgentApprovals", run: func() error {
			_, err := q.ExpireAgentApprovals(ctx)
			return err
		}},
		{name: "RevokeAgentTokenForCreator", run: func() error {
			_, err := q.RevokeAgentTokenForCreator(ctx, RevokeAgentTokenForCreatorParams{ID: id, CreatorUserID: id})
			return err
		}},
		{name: "DisableAgent", run: func() error {
			_, err := q.DisableAgent(ctx, DisableAgentParams{ID: id, CreatorID: id})
			return err
		}},
		{name: "RequestCertification", run: func() error {
			_, err := q.RequestCertification(ctx, RequestCertificationParams{ID: id, CreatorID: id})
			return err
		}},
		{name: "CertifyAgent", run: func() error {
			_, err := q.CertifyAgent(ctx, id)
			return err
		}},
		{name: "RejectCertification", run: func() error {
			_, err := q.RejectCertification(ctx, RejectCertificationParams{ID: id, RejectionReason: message})
			return err
		}},
		{name: "MarkBenchmarkRunSuccess", run: func() error {
			_, err := q.MarkBenchmarkRunSuccess(ctx, MarkBenchmarkRunSuccessParams{ID: id, Score: 95, RawOutput: []byte(`{"ok":true}`), JudgeReasoning: "pass"})
			return err
		}},
		{name: "MarkBenchmarkRunFailed", run: func() error {
			_, err := q.MarkBenchmarkRunFailed(ctx, MarkBenchmarkRunFailedParams{ID: id, ErrorMessage: message})
			return err
		}},
		{name: "DeleteAgentExampleForOwner", run: func() error {
			_, err := q.DeleteAgentExampleForOwner(ctx, DeleteAgentExampleForOwnerParams{ID: id, AgentID: id, CreatorID: id})
			return err
		}},
		{name: "MarkCapabilitiesSet", run: func() error {
			_, err := q.MarkCapabilitiesSet(ctx, id)
			return err
		}},
		{name: "MarkExamplesSet", run: func() error {
			_, err := q.MarkExamplesSet(ctx, MarkExamplesSetParams{AgentID: id, ExamplesSet: true})
			return err
		}},
		{name: "UpdateDryRunResult", run: func() error {
			_, err := q.UpdateDryRunResult(ctx, UpdateDryRunResultParams{AgentID: id, Result: "fail", Error: &message})
			return err
		}},
		{name: "DeleteDeliveryTarget", run: func() error {
			_, err := q.DeleteDeliveryTarget(ctx, DeleteDeliveryTargetParams{ID: id, UserID: id})
			return err
		}},
		{name: "SetDeliveryTargetDefault", run: func() error {
			_, err := q.SetDeliveryTargetDefault(ctx, SetDeliveryTargetDefaultParams{ID: id, UserID: id})
			return err
		}},
		{name: "ResetRunDeliveryForRetry", run: func() error {
			_, err := q.ResetRunDeliveryForRetry(ctx, ResetRunDeliveryForRetryParams{ID: id, UserID: id})
			return err
		}},
		{name: "MarkRunSuccess", run: func() error {
			return q.MarkRunSuccess(ctx, MarkRunSuccessParams{ID: id, Output: []byte(`{"ok":true}`), DurationMs: 100})
		}},
		{name: "MarkRunFailed", run: func() error {
			return q.MarkRunFailed(ctx, MarkRunFailedParams{ID: id, Status: "failed", ErrorMessage: &message, DurationMs: 100})
		}},
		{name: "UpdateUserBecomeCreator", run: func() error {
			_, err := q.UpdateUserBecomeCreator(ctx, id)
			return err
		}},
		{name: "SetAgentWebhook", run: func() error {
			_, err := q.SetAgentWebhook(ctx, SetAgentWebhookParams{ID: id, WebhookURL: &url, WebhookSecret: &secret, CreatorID: id})
			return err
		}},
		{name: "ClearAgentWebhook", run: func() error {
			_, err := q.ClearAgentWebhook(ctx, ClearAgentWebhookParams{ID: id, CreatorID: id})
			return err
		}},
		{name: "RequeueStaleWorkflowRuns", run: func() error {
			_, err := q.RequeueStaleWorkflowRuns(ctx, time.Date(2026, 6, 20, 20, 0, 0, 0, time.UTC))
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want %v", err, sentinel)
			}
		})
	}
}

func TestWorkflowQueriesScanRowsAndControlUpdates(t *testing.T) {
	userID := uuid.New()
	workflowID := uuid.New()
	nodeID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	stepID := uuid.New()
	childRunID := uuid.New()
	now := time.Date(2026, 6, 20, 19, 0, 0, 0, time.UTC)
	finishedAt := now.Add(2 * time.Minute)
	nextRetry := now.Add(time.Minute)
	claimedAt := now.Add(30 * time.Second)
	lastWorkerError := "worker failed"
	workflowValues := workflowRow(workflowID, userID, now)
	nodeValues := workflowNodeRow(nodeID, workflowID, agentID, now)
	runValues := workflowRunRow(runID, workflowID, userID, now, &finishedAt, &nextRetry, &claimedAt, &lastWorkerError)
	stepValues := workflowRunStepRow(stepID, runID, nodeID, agentID, &childRunID, now, &finishedAt, nil)
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

	dbtx.row = fakeRow{values: workflowValues}
	gotWorkflow, err := q.GetWorkflowByID(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("GetWorkflowByID error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetWorkflowByID")
	if gotWorkflow.ID != workflowID || gotWorkflow.UserID != userID {
		t.Fatalf("GetWorkflowByID scan = %#v", gotWorkflow)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{workflowID}) {
		t.Fatalf("GetWorkflowByID args = %#v", dbtx.queryRowArgs)
	}

	dbtx.row = fakeRow{values: []any{int32(4)}}
	workflowCount, err := q.CountWorkflowsByUser(context.Background(), userID)
	if err != nil || workflowCount != 4 {
		t.Fatalf("CountWorkflowsByUser = %d, %v", workflowCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountWorkflowsByUser")

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

	nodeRows := &fakeRows{rows: [][]any{nodeValues}}
	dbtx.queryRows = nodeRows
	nodes, err = q.ListWorkflowNodesByWorkflowIDs(context.Background(), []uuid.UUID{workflowID})
	if err != nil {
		t.Fatalf("ListWorkflowNodesByWorkflowIDs error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListWorkflowNodesByWorkflowIDs")
	if !nodeRows.closed || len(nodes) != 1 || nodes[0].WorkflowID != workflowID {
		t.Fatalf("ListWorkflowNodesByWorkflowIDs scan = %#v closed=%v", nodes, nodeRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{[]uuid.UUID{workflowID}}) {
		t.Fatalf("ListWorkflowNodesByWorkflowIDs args = %#v", dbtx.queryArgs)
	}

	dbtx.row = fakeRow{values: runValues}
	runningRun, err := q.CreateWorkflowRun(context.Background(), CreateWorkflowRunParams{
		WorkflowID: workflowID,
		UserID:     userID,
		Input:      []byte(`{"prompt":"go"}`),
	})
	if err != nil {
		t.Fatalf("CreateWorkflowRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateWorkflowRun")
	if runningRun.ID != runID || runningRun.Status != "running" {
		t.Fatalf("CreateWorkflowRun scan = %#v", runningRun)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{workflowID, userID, []byte(`{"prompt":"go"}`)}) {
		t.Fatalf("CreateWorkflowRun args = %#v", dbtx.queryRowArgs)
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

	successRunValues := append([]any{}, runValues...)
	successRunValues[3] = "success"
	successRunValues[8] = &finishedAt
	dbtx.row = fakeRow{values: successRunValues}
	successRun, err := q.MarkWorkflowRunSuccess(context.Background(), MarkWorkflowRunSuccessParams{ID: runID, Output: []byte(`{"ok":true}`)})
	if err != nil {
		t.Fatalf("MarkWorkflowRunSuccess error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkWorkflowRunSuccess")
	if successRun.ID != runID || successRun.Status != "success" || successRun.FinishedAt == nil {
		t.Fatalf("MarkWorkflowRunSuccess scan = %#v", successRun)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{runID, []byte(`{"ok":true}`)}) {
		t.Fatalf("MarkWorkflowRunSuccess args = %#v", dbtx.queryRowArgs)
	}

	failedRunValues := append([]any{}, runValues...)
	failedRunValues[3] = "failed"
	failedRunValues[6] = &lastWorkerError
	failedRunValues[8] = &finishedAt
	dbtx.row = fakeRow{values: failedRunValues}
	failedRun, err := q.MarkWorkflowRunFailed(context.Background(), MarkWorkflowRunFailedParams{ID: runID, ErrorMessage: &lastWorkerError})
	if err != nil {
		t.Fatalf("MarkWorkflowRunFailed error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkWorkflowRunFailed")
	if failedRun.ID != runID || failedRun.Status != "failed" || failedRun.ErrorMessage == nil {
		t.Fatalf("MarkWorkflowRunFailed scan = %#v", failedRun)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{runID, &lastWorkerError}) {
		t.Fatalf("MarkWorkflowRunFailed args = %#v", dbtx.queryRowArgs)
	}

	pausedRunValues := append([]any{}, runValues...)
	pausedRunValues[3] = "paused"
	pausedRunValues[13] = nil
	pausedRunValues[14] = nil
	pausedRunValues[15] = nil
	dbtx.row = fakeRow{values: pausedRunValues}
	pausedRun, err := q.PauseWorkflowRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("PauseWorkflowRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "PauseWorkflowRun")
	if pausedRun.ID != runID || pausedRun.Status != "paused" {
		t.Fatalf("PauseWorkflowRun scan = %#v", pausedRun)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{runID}) {
		t.Fatalf("PauseWorkflowRun args = %#v", dbtx.queryRowArgs)
	}

	pendingRunValues := append([]any{}, runValues...)
	pendingRunValues[3] = "pending"
	pendingRunValues[13] = &nextRetry
	pendingRunValues[14] = nil
	pendingRunValues[15] = nil
	dbtx.row = fakeRow{values: pendingRunValues}
	resumedRun, err := q.ResumeWorkflowRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("ResumeWorkflowRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ResumeWorkflowRun")
	if resumedRun.ID != runID || resumedRun.Status != "pending" || resumedRun.NextRetryAt == nil {
		t.Fatalf("ResumeWorkflowRun scan = %#v", resumedRun)
	}

	canceledRunValues := append([]any{}, runValues...)
	canceledRunValues[3] = "canceled"
	canceledRunValues[6] = &lastWorkerError
	canceledRunValues[8] = &finishedAt
	canceledRunValues[13] = nil
	canceledRunValues[14] = nil
	dbtx.row = fakeRow{values: canceledRunValues}
	canceledRun, err := q.CancelWorkflowRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("CancelWorkflowRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CancelWorkflowRun")
	if canceledRun.ID != runID || canceledRun.Status != "canceled" || canceledRun.FinishedAt == nil {
		t.Fatalf("CancelWorkflowRun scan = %#v", canceledRun)
	}

	dbtx.row = fakeRow{values: runValues}
	gotRun, err := q.GetWorkflowRunByID(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetWorkflowRunByID error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetWorkflowRunByID")
	if gotRun.ID != runID || gotRun.WorkflowID != workflowID {
		t.Fatalf("GetWorkflowRunByID scan = %#v", gotRun)
	}

	dbtx.row = fakeRow{values: runValues}
	claimedRun, err := q.ClaimPendingWorkflowRun(context.Background())
	if err != nil {
		t.Fatalf("ClaimPendingWorkflowRun error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ClaimPendingWorkflowRun")
	if claimedRun.ID != runID || claimedRun.ClaimedAt == nil {
		t.Fatalf("ClaimPendingWorkflowRun scan = %#v", claimedRun)
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

	if err := q.DeleteWorkflowRunSteps(context.Background(), runID); err != nil {
		t.Fatalf("DeleteWorkflowRunSteps error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "DeleteWorkflowRunSteps")
	if !reflect.DeepEqual(dbtx.execArgs, []any{runID}) {
		t.Fatalf("DeleteWorkflowRunSteps args = %#v", dbtx.execArgs)
	}

	dbtx.row = fakeRow{values: stepValues}
	step, err := q.CreateWorkflowRunStep(context.Background(), CreateWorkflowRunStepParams{
		WorkflowRunID:  runID,
		WorkflowNodeID: nodeID,
		NodeKey:        "analyze",
		AgentID:        agentID,
		Input:          []byte(`{"step":1}`),
		Sequence:       1,
	})
	if err != nil {
		t.Fatalf("CreateWorkflowRunStep error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CreateWorkflowRunStep")
	if step.ID != stepID || step.WorkflowRunID != runID || step.RunID == nil {
		t.Fatalf("CreateWorkflowRunStep scan = %#v", step)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{runID, nodeID, "analyze", agentID, []byte(`{"step":1}`), int32(1)}) {
		t.Fatalf("CreateWorkflowRunStep args = %#v", dbtx.queryRowArgs)
	}

	successStepValues := append([]any{}, stepValues...)
	successStepValues[6] = "success"
	successStepValues[8] = []byte(`{"step":"ok"}`)
	successStepValues[12] = &finishedAt
	dbtx.row = fakeRow{values: successStepValues}
	successStep, err := q.MarkWorkflowRunStepSuccess(context.Background(), MarkWorkflowRunStepSuccessParams{
		ID:     stepID,
		RunID:  &childRunID,
		Output: []byte(`{"step":"ok"}`),
	})
	if err != nil {
		t.Fatalf("MarkWorkflowRunStepSuccess error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkWorkflowRunStepSuccess")
	if successStep.ID != stepID || successStep.Status != "success" || successStep.FinishedAt == nil {
		t.Fatalf("MarkWorkflowRunStepSuccess scan = %#v", successStep)
	}

	failedStepValues := append([]any{}, stepValues...)
	failedStepValues[6] = "failed"
	failedStepValues[9] = &lastWorkerError
	failedStepValues[12] = &finishedAt
	dbtx.row = fakeRow{values: failedStepValues}
	failedStep, err := q.MarkWorkflowRunStepFailed(context.Background(), MarkWorkflowRunStepFailedParams{
		ID:           stepID,
		RunID:        &childRunID,
		ErrorMessage: &lastWorkerError,
	})
	if err != nil {
		t.Fatalf("MarkWorkflowRunStepFailed error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "MarkWorkflowRunStepFailed")
	if failedStep.ID != stepID || failedStep.Status != "failed" || failedStep.ErrorMessage == nil {
		t.Fatalf("MarkWorkflowRunStepFailed scan = %#v", failedStep)
	}

	stepRows := &fakeRows{rows: [][]any{stepValues}}
	dbtx.queryRows = stepRows
	steps, err := q.ListWorkflowRunSteps(context.Background(), runID)
	if err != nil {
		t.Fatalf("ListWorkflowRunSteps error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListWorkflowRunSteps")
	if !stepRows.closed || len(steps) != 1 || steps[0].ID != stepID {
		t.Fatalf("ListWorkflowRunSteps scan = %#v closed=%v", steps, stepRows.closed)
	}

	stepRows = &fakeRows{rows: [][]any{stepValues}}
	dbtx.queryRows = stepRows
	steps, err = q.ListWorkflowRunStepsByRunIDs(context.Background(), []uuid.UUID{runID})
	if err != nil {
		t.Fatalf("ListWorkflowRunStepsByRunIDs error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListWorkflowRunStepsByRunIDs")
	if !stepRows.closed || len(steps) != 1 || steps[0].WorkflowRunID != runID {
		t.Fatalf("ListWorkflowRunStepsByRunIDs scan = %#v closed=%v", steps, stepRows.closed)
	}
	if !reflect.DeepEqual(dbtx.queryArgs, []any{[]uuid.UUID{runID}}) {
		t.Fatalf("ListWorkflowRunStepsByRunIDs args = %#v", dbtx.queryArgs)
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
		{"CountPublicAgentsByCreator", func() (int32, error) { return q.CountPublicAgentsByCreator(context.Background(), creatorID) }},
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

func TestMarketAgentRunUserQueriesScanRowsAndArgs(t *testing.T) {
	userID := uuid.New()
	creatorID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	runtimeTokenID := uuid.New()
	latestBenchmarkID := uuid.New()
	now := time.Date(2026, 6, 21, 0, 0, 0, 0, time.UTC)
	authHeader := "Bearer secret"
	webhookURL := "https://example.com/hook"
	mcpTool := "search"
	avatar := "https://cdn.example/avatar.png"
	provider := "github"
	oauthID := "gh-1"
	rejectionReason := "missing dry run"
	duration := int32(42)
	finishedAt := now.Add(time.Minute)
	runValues := runRow(runID, userID, agentID, []byte(`{"prompt":"hi"}`), []byte(`{"ok":true}`), "running", nil, nil, 100, 25, 75, &duration, now, &finishedAt, "api")
	agentValues := agentRow(agentID, creatorID, now, &authHeader, &webhookURL, &mcpTool)
	agentMarketValues := append(append([]any{}, agentValues...), "Creator Name")
	agentListMarketValues := append(
		append([]any{}, agentMarketValues...),
		"healthy",
		&now,
		nil,
		&now,
		int32(0),
		&now,
		int32(2),
		&latestBenchmarkID,
	)
	pendingAgentValues := append(append([]any{}, agentValues...), "creator@example.com", "Creator Name")
	userValues := userRow(userID, now, nil, &provider, &oauthID, &avatar, nil)
	dbtx := &fakeDBTX{
		row:       fakeRow{values: []any{int32(11)}},
		queryRows: &fakeRows{rows: [][]any{agentMarketValues}},
		execTag:   pgconn.NewCommandTag("UPDATE 4"),
	}
	q := New(dbtx)

	dbtx.row = fakeRow{values: []any{int32(11)}}
	agentCount, err := q.AgentsCount(context.Background())
	if err != nil || agentCount != 11 {
		t.Fatalf("AgentsCount = %d, %v", agentCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "AgentsCount")

	dbtx.row = fakeRow{values: agentValues}
	updatedAgent, err := q.UpdateAgentDraft(context.Background(), UpdateAgentDraftParams{
		ID:                 agentID,
		Name:               "Agent One",
		Description:        "does work",
		EndpointURL:        "https://example.com/agent",
		EndpointAuthHeader: &authHeader,
		PricePerCallCents:  12,
		Tags:               []string{"data"},
		CreatorID:          creatorID,
		Visibility:         "public",
		ConnectionMode:     "mcp_server",
		MCPToolName:        &mcpTool,
	})
	if err != nil {
		t.Fatalf("UpdateAgentDraft error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "UpdateAgentDraft")
	if updatedAgent.ID != agentID || updatedAgent.ConnectionMode != "mcp_server" {
		t.Fatalf("UpdateAgentDraft scan = %#v", updatedAgent)
	}

	if err := q.SetAgentVisibilityForOwner(context.Background(), SetAgentVisibilityForOwnerParams{ID: agentID, CreatorID: creatorID, Visibility: "unlisted"}); err != nil {
		t.Fatalf("SetAgentVisibilityForOwner error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "SetAgentVisibilityForOwner")

	dbtx.row = fakeRow{values: agentValues}
	if got, err := q.GetAgentByIDForOwner(context.Background(), GetAgentByIDForOwnerParams{ID: agentID, CreatorID: creatorID}); err != nil || got.ID != agentID {
		t.Fatalf("GetAgentByIDForOwner = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentByIDForOwner")

	dbtx.row = fakeRow{values: agentValues}
	if got, err := q.GetAgentByID(context.Background(), agentID); err != nil || got.CreatorID != creatorID {
		t.Fatalf("GetAgentByID = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentByID")

	if rows, err := q.RequestCertification(context.Background(), RequestCertificationParams{ID: agentID, CreatorID: creatorID}); err != nil || rows != 4 {
		t.Fatalf("RequestCertification = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "RequestCertification")

	if rows, err := q.CertifyAgent(context.Background(), agentID); err != nil || rows != 4 {
		t.Fatalf("CertifyAgent = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "CertifyAgent")

	if rows, err := q.RejectCertification(context.Background(), RejectCertificationParams{ID: agentID, RejectionReason: rejectionReason}); err != nil || rows != 4 {
		t.Fatalf("RejectCertification = %d, %v", rows, err)
	}
	requireSQLName(t, dbtx.execSQL, "RejectCertification")

	dbtx.row = fakeRow{values: []any{true}}
	available, err := q.CheckSlugAvailable(context.Background(), "agent-one")
	if err != nil || !available {
		t.Fatalf("CheckSlugAvailable = %v, %v", available, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CheckSlugAvailable")

	pendingRows := &fakeRows{rows: [][]any{pendingAgentValues}}
	dbtx.queryRows = pendingRows
	pendingAgents, err := q.ListPendingAgents(context.Background())
	if err != nil {
		t.Fatalf("ListPendingAgents error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListPendingAgents")
	if !pendingRows.closed || len(pendingAgents) != 1 || pendingAgents[0].CreatorEmail != "creator@example.com" {
		t.Fatalf("ListPendingAgents scan = %#v closed=%v", pendingAgents, pendingRows.closed)
	}

	if err := q.IncrementAgentStats(context.Background(), IncrementAgentStatsParams{ID: agentID, RevenueCents: 75}); err != nil {
		t.Fatalf("IncrementAgentStats error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "IncrementAgentStats")

	publicRows := &fakeRows{rows: [][]any{agentListMarketValues}}
	dbtx.queryRows = publicRows
	publicAgents, err := q.ListPublicAgents(context.Background(), ListPublicAgentsParams{
		Tags:         []string{"data"},
		Keyword:      "agent",
		Limit:        20,
		Offset:       5,
		CallableOnly: true,
	})
	if err != nil {
		t.Fatalf("ListPublicAgents error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListPublicAgents")
	if !publicRows.closed || len(publicAgents) != 1 || publicAgents[0].CreatorName != "Creator Name" {
		t.Fatalf("ListPublicAgents scan = %#v closed=%v", publicAgents, publicRows.closed)
	}
	if publicAgents[0].AvailabilityStatus != "healthy" ||
		publicAgents[0].AvailabilityLastSuccessfulRunAt == nil ||
		publicAgents[0].AvailabilityConsecutiveFailures != 0 ||
		publicAgents[0].LastRuntimeTokenUsedAt == nil ||
		publicAgents[0].VerifiedSkillCount != 2 ||
		publicAgents[0].LatestBenchmarkID == nil ||
		*publicAgents[0].LatestBenchmarkID != latestBenchmarkID {
		t.Fatalf("ListPublicAgents availability/benchmark scan = %#v", publicAgents[0])
	}

	dbtx.row = fakeRow{values: []any{int32(6)}}
	publicCount, err := q.CountPublicAgents(context.Background(), CountPublicAgentsParams{Tags: []string{"data"}, Keyword: "agent", CallableOnly: true})
	if err != nil || publicCount != 6 {
		t.Fatalf("CountPublicAgents = %d, %v", publicCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CountPublicAgents")

	dbtx.row = fakeRow{values: agentMarketValues}
	if got, err := q.GetAgentBySlug(context.Background(), "agent-one"); err != nil || got.CreatorName != "Creator Name" {
		t.Fatalf("GetAgentBySlug = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentBySlug")

	dbtx.row = fakeRow{values: agentMarketValues}
	if got, err := q.GetAgentBySlugForOwner(context.Background(), GetAgentBySlugForOwnerParams{Slug: "agent-one", CreatorID: creatorID}); err != nil || got.ID != agentID {
		t.Fatalf("GetAgentBySlugForOwner = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetAgentBySlugForOwner")

	dbtx.row = fakeRow{values: userValues}
	if got, err := q.GetUserByEmail(context.Background(), "user@example.com"); err != nil || got.ID != userID {
		t.Fatalf("GetUserByEmail = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetUserByEmail")

	dbtx.row = fakeRow{values: userValues}
	if got, err := q.GetUserByOAuth(context.Background(), GetUserByOAuthParams{OauthProvider: &provider, OauthID: &oauthID}); err != nil || got.OauthID == nil {
		t.Fatalf("GetUserByOAuth = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetUserByOAuth")

	if err := q.UpdateUserOAuth(context.Background(), UpdateUserOAuthParams{ID: userID, OauthProvider: &provider, OauthID: &oauthID, AvatarURL: avatar}); err != nil {
		t.Fatalf("UpdateUserOAuth error = %v", err)
	}
	requireSQLName(t, dbtx.execSQL, "UpdateUserOAuth")

	dbtx.row = fakeRow{values: []any{int32(12)}}
	runCount, err := q.RunsCount(context.Background())
	if err != nil || runCount != 12 {
		t.Fatalf("RunsCount = %d, %v", runCount, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "RunsCount")

	dbtx.row = fakeRow{values: runValues}
	if got, err := q.CancelRun(context.Background(), CancelRunParams{ID: runID, UserID: userID, ErrorMessage: "user canceled"}); err != nil || got.ID != runID {
		t.Fatalf("CancelRun = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "CancelRun")

	dbtx.row = fakeRow{values: runValues}
	if got, err := q.GetRunByID(context.Background(), runID); err != nil || got.UserID != userID {
		t.Fatalf("GetRunByID = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetRunByID")

	dbtx.row = fakeRow{values: runValues}
	if got, err := q.ClaimRuntimePullRun(context.Background(), ClaimRuntimePullRunParams{AgentID: agentID, RuntimeTokenID: runtimeTokenID}); err != nil || got.AgentID != agentID {
		t.Fatalf("ClaimRuntimePullRun = %#v, %v", got, err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "ClaimRuntimePullRun")

	dbtx.row = fakeRow{values: []any{runID, userID, agentID, "running", int32(100), int32(75), now, &runtimeTokenID}}
	runState, err := q.GetRuntimePullRunState(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRuntimePullRunState error = %v", err)
	}
	requireSQLName(t, dbtx.queryRowSQL, "GetRuntimePullRunState")
	if runState.ID != runID || runState.ClaimedByRuntimeTokenID == nil {
		t.Fatalf("GetRuntimePullRunState scan = %#v", runState)
	}

	staleRows := &fakeRows{rows: [][]any{{runID, userID, agentID, int32(100), now, "RUNTIME_PULL_RESULT_TIMEOUT", "Agent runtime timed out"}}}
	dbtx.queryRows = staleRows
	staleRuns, err := q.ListStaleRuntimePullRuns(context.Background(), ListStaleRuntimePullRunsParams{
		DispatchStaleBefore: now.Add(-time.Hour),
		ResultStaleBefore:   now.Add(-30 * time.Minute),
		Limit:               25,
	})
	if err != nil {
		t.Fatalf("ListStaleRuntimePullRuns error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListStaleRuntimePullRuns")
	if !staleRows.closed || len(staleRuns) != 1 || staleRuns[0].ErrorCode != "RUNTIME_PULL_RESULT_TIMEOUT" {
		t.Fatalf("ListStaleRuntimePullRuns scan = %#v closed=%v", staleRuns, staleRows.closed)
	}

	endpointRows := &fakeRows{rows: [][]any{{runID, userID, agentID, int32(100), now, "mcp_server", "ENDPOINT_RUN_TIMEOUT", "Agent endpoint timed out"}}}
	dbtx.queryRows = endpointRows
	endpointRuns, err := q.ListStaleEndpointRuns(context.Background(), ListStaleEndpointRunsParams{
		StaleBefore: now.Add(-time.Hour),
		Limit:       25,
	})
	if err != nil {
		t.Fatalf("ListStaleEndpointRuns error = %v", err)
	}
	requireSQLName(t, dbtx.querySQL, "ListStaleEndpointRuns")
	if !endpointRows.closed || len(endpointRuns) != 1 || endpointRuns[0].ConnectionMode != "mcp_server" || endpointRuns[0].ErrorCode != "ENDPOINT_RUN_TIMEOUT" {
		t.Fatalf("ListStaleEndpointRuns scan = %#v closed=%v", endpointRuns, endpointRows.closed)
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

func agentTokenRow(id uuid.UUID, agentID *uuid.UUID, creatorID uuid.UUID, status string, expiresAt, redeemedAt, revokedAt, lastUsedAt *time.Time, createdAt time.Time) []any {
	return []any{
		id,
		agentID,
		creatorID,
		"worker",
		"ol_agent_abcd",
		"hash",
		[]string{"agent:call", "agent:pull"},
		status,
		expiresAt,
		redeemedAt,
		lastUsedAt,
		revokedAt,
		createdAt,
	}
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

func registryPeerRow(id, ownerID uuid.UUID, now time.Time, lastUsedAt *time.Time) []any {
	return []any{
		id,
		ownerID,
		"Peer",
		"https://peer.example/api/v1",
		"peer-token",
		"sha256:abc",
		"active",
		lastUsedAt,
		now,
		now.Add(time.Minute),
	}
}

func registryFederationInviteRow(id, ownerID uuid.UUID, now, expiresAt time.Time, consumedAt *time.Time) []any {
	return []any{
		id,
		ownerID,
		"Invite",
		"https://peer.example/api/v1",
		"peer-token",
		"rf_live_abcd",
		"hash",
		"sha256:def",
		"active",
		expiresAt,
		consumedAt,
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

func proxyRunRow(
	id, cloudRunID, linkID, listingID, nodeID, localAgentID, requestingUserID uuid.UUID,
	now time.Time,
	inputSummary, outputSummary, errorCode, errorMessage *string,
	nextRetryAt, claimedAt, finishedAt *time.Time,
) []any {
	return []any{
		id,
		cloudRunID,
		linkID,
		listingID,
		nodeID,
		localAgentID,
		requestingUserID,
		"idem-1",
		"claimed",
		"metadata_only",
		[]string{"secret"},
		[]byte(`{"prompt":"hi"}`),
		inputSummary,
		[]byte(`{"ok":true}`),
		outputSummary,
		errorCode,
		errorMessage,
		int32(2),
		int32(3),
		nextRetryAt,
		claimedAt,
		finishedAt,
		now,
		now.Add(time.Minute),
	}
}

func proxyRunArtifactRow(id, proxyRunID, cloudRunID uuid.UUID, now time.Time, mimeType, fileURI, fileName, fileSHA *string, fileSize *int64) []any {
	return []any{
		id,
		proxyRunID,
		cloudRunID,
		"artifact-1",
		"json",
		"Proxy result",
		[]byte(`{"ok":true}`),
		mimeType,
		fileURI,
		fileName,
		fileSHA,
		fileSize,
		now,
	}
}

func taskCallbackSubscriptionRow(
	id, runID, ownerUserID uuid.UUID,
	callerAgentID *uuid.UUID,
	now time.Time,
	authScheme, authCredentials *string,
	deletedAt *time.Time,
) []any {
	return []any{
		id,
		runID,
		ownerUserID,
		callerAgentID,
		"https://hooks.example/run",
		"secret",
		[]string{"completed", "artifact"},
		authScheme,
		authCredentials,
		[]byte(`{"source":"a2a"}`),
		"active",
		int32(1),
		now,
		now.Add(time.Minute),
		deletedAt,
	}
}

func taskCallbackDeliveryRow(
	id, subscriptionID, runEventID uuid.UUID,
	now time.Time,
	responseStatus *int32,
	responseBody, errorMessage *string,
	nextRetryAt, deliveredAt *time.Time,
) []any {
	return []any{
		id,
		subscriptionID,
		runEventID,
		[]byte(`{"event":"completed"}`),
		"pending",
		responseStatus,
		responseBody,
		errorMessage,
		int32(2),
		nextRetryAt,
		deliveredAt,
		now,
		now.Add(time.Minute),
	}
}

func agentRuntimeTokenRow(id, agentID, userID uuid.UUID, now time.Time, lastUsedAt, revokedAt *time.Time) []any {
	return []any{id, agentID, userID, "runtime", "ol_agent_abcd", "hash", []string{"agent:pull"}, lastUsedAt, revokedAt, now}
}

func runDelegationRow(childRunID, parentRunID, callerAgentID uuid.UUID, createdAt time.Time) []any {
	return []any{childRunID, parentRunID, callerAgentID, "delegate analysis", createdAt}
}

func a2aContextMappingRow(id, runID, userID, agentID, parentRunID, callerAgentID, targetAgentID uuid.UUID, now time.Time) []any {
	return []any{
		id,
		runID,
		userID,
		agentID,
		"ctx-root",
		"task-child",
		"ctx-root",
		"ctx-root",
		"task-parent",
		&parentRunID,
		&callerAgentID,
		&targetAgentID,
		"trace-root",
		[]string{"task-parent"},
		"agent_delegation",
		now,
		now.Add(time.Minute),
	}
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

func workflowRunStepRow(id, workflowRunID, workflowNodeID, agentID uuid.UUID, runID *uuid.UUID, now time.Time, finishedAt *time.Time, errorMessage *string) []any {
	return []any{
		id,
		workflowRunID,
		workflowNodeID,
		"analyze",
		agentID,
		runID,
		"running",
		[]byte(`{"step":1}`),
		[]byte(`{"step":"ok"}`),
		errorMessage,
		int32(1),
		now,
		finishedAt,
		now,
		now.Add(time.Minute),
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

type fakeTx struct {
	execSQLs []string
	execArgs [][]any
	tag      pgconn.CommandTag
	err      error
}

func (f *fakeTx) Begin(context.Context) (pgx.Tx, error) {
	return f, f.err
}

func (f *fakeTx) Commit(context.Context) error {
	return f.err
}

func (f *fakeTx) Rollback(context.Context) error {
	return f.err
}

func (f *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return 0, nil
}

func (f *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (f *fakeTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (f *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, f.err
}

func (f *fakeTx) Exec(_ context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	f.execSQLs = append(f.execSQLs, sql)
	f.execArgs = append(f.execArgs, append([]any(nil), args...))
	if f.err != nil {
		return pgconn.CommandTag{}, f.err
	}
	return f.tag, nil
}

func (f *fakeTx) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	return nil, f.err
}

func (f *fakeTx) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	return fakeRow{err: f.err}
}

func (f *fakeTx) Conn() *pgx.Conn {
	return nil
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
