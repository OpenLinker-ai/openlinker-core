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
