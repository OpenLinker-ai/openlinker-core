package db

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestHasActiveRuntimeV2SessionForAgentUsesDurableCurrentTruth(t *testing.T) {
	agentID := uuid.New()
	dbtx := &signalQueryDBTX{row: signalQueryRow{values: []any{true}}}
	active, err := New(dbtx).HasActiveRuntimeV2SessionForAgent(context.Background(), agentID)
	if err != nil || !active {
		t.Fatalf("HasActiveRuntimeV2SessionForAgent = %v, %v", active, err)
	}
	requireSignalQueryName(t, dbtx.queryRowSQL, "HasActiveRuntimeV2SessionForAgent")
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{agentID}) {
		t.Fatalf("HasActiveRuntimeV2SessionForAgent args = %#v", dbtx.queryRowArgs)
	}
	for _, guard := range []string{
		"s.status IN ('active', 'draining')",
		"n.status IN ('active', 'draining')",
		"n.revoked_at IS NULL",
		"s.heartbeat_at >= clock_timestamp() - INTERVAL '45 seconds'",
		"n.last_seen_at >= clock_timestamp() - INTERVAL '45 seconds'",
		"t.status = 'active_runtime'",
		"t.revoked_at IS NULL",
		"t.scopes @> ARRAY['agent:pull']::text[]",
		"t.expires_at > clock_timestamp()",
		"contract.is_current",
		"runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53'",
		"'persistent_spool'",
		"attachment.detached_at IS NULL",
	} {
		if !strings.Contains(dbtx.queryRowSQL, guard) {
			t.Fatalf("HasActiveRuntimeV2SessionForAgent missing guard %q:\n%s", guard, dbtx.queryRowSQL)
		}
	}
}

func TestRetryRuntimeSignalSchedulesWithDatabaseClock(t *testing.T) {
	now := time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC)
	id, agentID, owner := uuid.New(), uuid.New(), uuid.New()
	lastError := "SIGNAL_PUBLISH_FAILED"
	values := []any{
		id, "run.available", agentID, (*uuid.UUID)(nil), []byte(`{}`),
		now, now.Add(time.Second), "pending", (*uuid.UUID)(nil), (*time.Time)(nil),
		(*time.Time)(nil), int32(2), &lastError,
	}
	dbtx := &signalQueryDBTX{row: signalQueryRow{values: values}}
	row, err := New(dbtx).RetryRuntimeSignal(context.Background(), RetryRuntimeSignalParams{
		ID: id, LeaseOwner: owner, RetryAfterMs: 750, LastError: "SIGNAL_PUBLISH_FAILED",
	})
	if err != nil || row.ID != id {
		t.Fatalf("RetryRuntimeSignal = %#v, %v", row, err)
	}
	requireSignalQueryName(t, dbtx.queryRowSQL, "RetryRuntimeSignal")
	if !strings.Contains(dbtx.queryRowSQL, "available_at = clock_timestamp() +") ||
		!strings.Contains(dbtx.queryRowSQL, "INTERVAL '1 millisecond'") {
		t.Fatalf("RetryRuntimeSignal does not use database clock:\n%s", dbtx.queryRowSQL)
	}
	if !reflect.DeepEqual(dbtx.queryRowArgs, []any{id, owner, int64(750), "SIGNAL_PUBLISH_FAILED"}) {
		t.Fatalf("RetryRuntimeSignal args = %#v", dbtx.queryRowArgs)
	}
}

func requireSignalQueryName(t *testing.T, sql, name string) {
	t.Helper()
	if !strings.Contains(sql, "-- name: "+name) {
		t.Fatalf("query is not %s:\n%s", name, sql)
	}
}

type signalQueryDBTX struct {
	queryRowSQL  string
	queryRowArgs []any
	row          pgx.Row
}

func (*signalQueryDBTX) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (*signalQueryDBTX) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	return nil, nil
}

func (f *signalQueryDBTX) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	f.queryRowSQL = sql
	f.queryRowArgs = append([]any(nil), args...)
	return f.row
}

type signalQueryRow struct {
	values []any
}

func (r signalQueryRow) Scan(dest ...any) error {
	if len(dest) != len(r.values) {
		return pgx.ErrNoRows
	}
	for index, value := range r.values {
		target := reflect.ValueOf(dest[index])
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return pgx.ErrNoRows
		}
		target = target.Elem()
		if value == nil {
			target.SetZero()
			continue
		}
		source := reflect.ValueOf(value)
		if !source.Type().AssignableTo(target.Type()) {
			return pgx.ErrNoRows
		}
		target.Set(source)
	}
	return nil
}
