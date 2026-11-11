package migrationinit

import (
	"context"
	"crypto/sha256"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// 104 adds attempts, logical node state, stop records and run leases. It
// refuses while workflow runs are not drained, numbers historical steps in
// listing order, keeps interrupted and orphaned history without inventing
// attempts, and its rollback refuses once the new engine produced data.
func TestWorkflowAttemptsMigrationBackfillsHistoryOnly(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, base)
	migrateTestDatabaseToVersion(t, databaseURL, uint(CoreWorkflowAttemptsPredecessorVersion))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { conn.Close(context.Background()) }()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	owner, creator, agentID, workflowID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	exec(`INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'synthetic','Owner'),($3,$4,'synthetic','Creator')`,
		owner, owner.String()+"@example.invalid", creator, creator.String()+"@example.invalid")
	exec(`INSERT INTO agents(id,creator_id,slug,name,description,endpoint_url,price_per_call_cents)
VALUES($1,$2,'wf-attempts','Agent','Synthetic','https://agent.example/run',0)`, agentID, creator)
	exec(`INSERT INTO workflows(id,user_id,name) VALUES($1,$2,'synthetic')`, workflowID, owner)
	nodeA, nodeB, nodeC := uuid.New(), uuid.New(), uuid.New()
	for i, node := range []uuid.UUID{nodeA, nodeB, nodeC} {
		key := []string{"a", "b", "c"}[i]
		exec(`INSERT INTO workflow_nodes(id,workflow_id,node_key,agent_id,title,position) VALUES($1,$2,$3,$4,$5,$6)`,
			node, workflowID, key, agentID, key, i)
	}

	// Run 1 (failed): node a has two duplicate rows with the same sequence and
	// created_at, the later id succeeded; node b's step is still 'running'
	// (interrupted); node c has only a launch whose step was deleted.
	run1 := uuid.New()
	exec(`INSERT INTO workflow_runs(id,workflow_id,user_id,status,finished_at) VALUES($1,$2,$3,'failed',now())`, run1, workflowID, owner)
	lowA := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	highA := uuid.MustParse("ffffffff-ffff-4fff-bfff-ffffffffffa1")
	created := "2026-11-11T00:00:00Z"
	exec(`INSERT INTO workflow_run_steps(id,workflow_run_id,workflow_node_id,node_key,agent_id,status,sequence,created_at,updated_at,finished_at)
VALUES($1,$3,$4,'a',$5,'failed',0,$6,$6,$6),($2,$3,$4,'a',$5,'success',0,$6,$6,$6)`, lowA, highA, run1, nodeA, agentID, created)
	stepB := uuid.New()
	exec(`INSERT INTO workflow_run_steps(id,workflow_run_id,workflow_node_id,node_key,agent_id,status,sequence,created_at,updated_at)
VALUES($1,$2,$3,'b',$4,'running',1,$5,$5)`, stepB, run1, nodeB, agentID, created)
	launch := func(run, node uuid.UUID, key string, step *uuid.UUID, state string) {
		t.Helper()
		sum := sha256.Sum256([]byte(run.String() + key))
		exec(`INSERT INTO workflow_step_launches(workflow_run_id,workflow_node_id,workflow_run_step_id,node_key,launch_token,idempotency_key_hash,creation_fingerprint,state)
VALUES($1,$2,$3,$4,$5,$6,$6,$7)`, run, node, step, key, uuid.New(), sum[:], state)
	}
	launch(run1, nodeA, "a", nil, "invalidated") // link cleared, node has steps
	launch(run1, nodeC, "c", nil, "invalidated") // node has no step at all

	// Before draining, 104 refuses.
	pendingRun := uuid.New()
	exec(`INSERT INTO workflow_runs(id,workflow_id,user_id,status) VALUES($1,$2,$3,'paused')`, pendingRun, workflowID, owner)
	if err := applyMigrationFileErr(ctx, conn, "104_workflow_execution_attempts.up.sql"); err == nil ||
		!strings.Contains(err.Error(), "drain workflow runs first") {
		t.Fatalf("104 with a paused run: %v", err)
	}
	exec(`UPDATE workflow_runs SET status='canceled', finished_at=now() WHERE id=$1`, pendingRun)

	migrateTestDatabaseToCurrent(t, databaseURL)
	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || !noop {
		t.Fatalf("upgraded 104 preflight: noop=%t err=%v shape=%s", noop, err, formatShape(snapshot.CoreShape))
	}

	var attempt int
	var updatedAt time.Time
	for id, want := range map[uuid.UUID]int{lowA: 1, highA: 2} {
		if err := conn.QueryRow(ctx, `SELECT attempt, updated_at FROM workflow_run_steps WHERE id=$1`, id).Scan(&attempt, &updatedAt); err != nil {
			t.Fatal(err)
		}
		if attempt != want || !updatedAt.Equal(time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("step %s attempt=%d updated_at=%s, want attempt %d and untouched timestamp", id, attempt, updatedAt, want)
		}
	}
	var legacy bool
	if err := conn.QueryRow(ctx, `SELECT legacy_unfinished FROM workflow_run_steps WHERE id=$1`, stepB).Scan(&legacy); err != nil || !legacy {
		t.Fatalf("interrupted step legacy_unfinished=%t err=%v", legacy, err)
	}
	var stepLink *uuid.UUID
	var orphan bool
	if err := conn.QueryRow(ctx, `SELECT workflow_run_step_id, attempt, legacy_orphan FROM workflow_step_launches WHERE workflow_run_id=$1 AND workflow_node_id=$2`, run1, nodeA).Scan(&stepLink, &attempt, &orphan); err != nil {
		t.Fatal(err)
	}
	if stepLink == nil || *stepLink != highA || attempt != 2 || orphan {
		t.Fatalf("relinked launch = (%v,%d,%t), want effective step %s attempt 2", stepLink, attempt, orphan, highA)
	}
	if err := conn.QueryRow(ctx, `SELECT workflow_run_step_id, attempt, legacy_orphan FROM workflow_step_launches WHERE workflow_run_id=$1 AND workflow_node_id=$2`, run1, nodeC).Scan(&stepLink, &attempt, &orphan); err != nil {
		t.Fatal(err)
	}
	if stepLink != nil || attempt != 1 || !orphan {
		t.Fatalf("orphan launch = (%v,%d,%t), want unlinked attempt 1 legacy_orphan", stepLink, attempt, orphan)
	}
	rows, err := conn.Query(ctx, `SELECT node_key, state, effective_attempt FROM workflow_run_nodes WHERE workflow_run_id=$1 ORDER BY node_key`, run1)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for rows.Next() {
		var key, state string
		var effective int
		if err := rows.Scan(&key, &state, &effective); err != nil {
			t.Fatal(err)
		}
		got = append(got, key+":"+state+":"+string(rune('0'+effective)))
	}
	rows.Close()
	if strings.Join(got, ",") != "a:success:2" {
		t.Fatalf("logical nodes = %v, want only a:success:2 (b interrupted, c orphaned)", got)
	}

	// Constraints of the new schema.
	for _, statement := range []string{
		`UPDATE workflow_run_steps SET attempt=0 WHERE id='` + lowA.String() + `'`,
		`UPDATE workflow_runs SET lease_owner=gen_random_uuid() WHERE id='` + run1.String() + `'`,
		`UPDATE workflow_runs SET lease_mode='steal' WHERE id='` + run1.String() + `'`,
		`INSERT INTO workflow_run_nodes(workflow_run_id,workflow_node_id,node_key,state) VALUES('` + run1.String() + `','` + nodeB.String() + `','b','success')`,
	} {
		if _, err := conn.Exec(ctx, statement); err == nil || !strings.Contains(err.Error(), "violates check constraint") {
			t.Fatalf("%s: err=%v, want CHECK violation", statement, err)
		}
	}
	if _, err := conn.Exec(ctx, `UPDATE workflow_run_steps SET attempt=1 WHERE id=$1`, highA); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("duplicate attempt: %v", err)
	}

	rollback := func() error {
		raw, err := os.ReadFile("../../migrations/104_workflow_execution_attempts.down.sql")
		if err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(ctx, string(raw))
		if err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
		}
		return err
	}
	exec(`INSERT INTO workflow_step_stops(workflow_run_id,workflow_node_id,attempt,reason) VALUES($1,$2,2,'node_timeout')`, run1, nodeA)
	if err := rollback(); err == nil || !strings.Contains(err.Error(), "stop records exist") {
		t.Fatalf("rollback with a stop record: %v", err)
	}
	exec(`DELETE FROM workflow_step_stops`)
	if err := rollback(); err != nil {
		t.Fatalf("rollback of backfilled history: %v", err)
	}
	exec(`UPDATE schema_migrations SET version=103,dirty=false`)
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CoreShape.Tables != 92 {
		t.Fatalf("rollback did not remove the 104 tables: %s", formatShape(snapshot.CoreShape))
	}
}

func applyMigrationFileErr(ctx context.Context, conn *pgx.Conn, name string) error {
	raw, err := os.ReadFile("../../migrations/" + name)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, string(raw))
	if err != nil {
		_, _ = conn.Exec(ctx, "ROLLBACK")
	}
	return err
}
