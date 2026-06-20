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
