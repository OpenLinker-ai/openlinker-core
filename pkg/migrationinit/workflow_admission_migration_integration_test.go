package migrationinit

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// 103 adds the workflow admission singleton and the cancellation request
// columns. Existing cancellations are backfilled from their reason code, the
// constraints hold, and the rollback refuses while it would reopen admission
// or erase administrator cancellations.
func TestWorkflowAdmissionMigrationBackfillChecksAndGuardedRollback(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, base)
	migrateTestDatabaseToVersion(t, databaseURL, uint(CoreWorkflowAdmissionPredecessorVersion))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || noop {
		t.Fatalf("102 predecessor preflight: noop=%t err=%v shape=%s", noop, err, formatShape(snapshot.CoreShape))
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	owner, admin := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{owner, admin} {
		if _, err := conn.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'synthetic','Synthetic')`, id, id.String()+"@example.invalid"); err != nil {
			t.Fatal(err)
		}
	}
	workflowID := uuid.New()
	if _, err := conn.Exec(ctx, `INSERT INTO workflows(id,user_id,name) VALUES($1,$2,'synthetic')`, workflowID, owner); err != nil {
		t.Fatal(err)
	}
	ownerRun, externalRun := uuid.New(), uuid.New()
	for _, item := range []struct {
		run    uuid.UUID
		reason string
	}{{ownerRun, "OWNER_CANCEL_REQUESTED"}, {externalRun, "CALLER_REQUESTED"}} {
		if _, err := conn.Exec(ctx, `INSERT INTO workflow_runs(id,workflow_id,user_id,status,finished_at) VALUES($1,$2,$3,'canceled',now())`, item.run, workflowID, owner); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO workflow_run_cancellations(workflow_run_id,id,actor_user_id,reason_code,state) VALUES($1,$2,$3,$4,'requested')`, item.run, uuid.New(), owner, item.reason); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close(ctx)

	// 103 is now an exact predecessor of the current schema; stop there so the
	// rollback below exercises 103 itself.
	migrateTestDatabaseToVersion(t, databaseURL, uint(CoreWorkflowAttemptsPredecessorVersion))
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || noop {
		t.Fatalf("upgraded 103 preflight: noop=%t err=%v shape=%s", noop, err, formatShape(snapshot.CoreShape))
	}

	conn, err = pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { conn.Close(context.Background()) }()
	var state string
	if err := conn.QueryRow(ctx, `SELECT state FROM workflow_admission`).Scan(&state); err != nil || state != "open" {
		t.Fatalf("admission = %q err=%v, want open", state, err)
	}
	for _, item := range []struct {
		run uuid.UUID
		via string
	}{{ownerRun, "owner"}, {externalRun, "external"}} {
		var by uuid.UUID
		var via string
		if err := conn.QueryRow(ctx, `SELECT requested_by, requested_via FROM workflow_run_cancellations WHERE workflow_run_id=$1`, item.run).Scan(&by, &via); err != nil {
			t.Fatal(err)
		}
		if by != owner || via != item.via {
			t.Fatalf("backfill for %s = (%s,%s), want (%s,%s)", item.run, by, via, owner, item.via)
		}
	}
	for _, statement := range []string{
		`INSERT INTO workflow_admission(singleton,state) VALUES(false,'open')`,
		`UPDATE workflow_admission SET state='closed'`,
		`UPDATE workflow_run_cancellations SET requested_via='robot' WHERE workflow_run_id='` + ownerRun.String() + `'`,
	} {
		if _, err := conn.Exec(ctx, statement); err == nil || !strings.Contains(err.Error(), "violates check constraint") {
			t.Fatalf("%s: err=%v, want CHECK violation", statement, err)
		}
	}
	if _, err := conn.Exec(ctx, `INSERT INTO workflow_admission(singleton,state) VALUES(true,'open')`); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("second admission row: err=%v, want duplicate key", err)
	}

	rollback := func() error {
		raw, err := os.ReadFile("../../migrations/103_workflow_admission_drain.down.sql")
		if err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(ctx, string(raw))
		if err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
		}
		return err
	}
	if _, err := conn.Exec(ctx, `UPDATE workflow_admission SET state='draining', changed_by=$1`, admin); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err == nil || !strings.Contains(err.Error(), "admission is draining") {
		t.Fatalf("rollback while draining: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE workflow_admission SET state='open'`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `UPDATE workflow_run_cancellations SET requested_by=$1, requested_via='admin' WHERE workflow_run_id=$2`, admin, ownerRun); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err == nil || !strings.Contains(err.Error(), "administrator cancellations exist") {
		t.Fatalf("rollback with administrator cancellation: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE workflow_run_cancellations SET requested_by=actor_user_id, requested_via='owner' WHERE workflow_run_id=$1`, ownerRun); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err != nil {
		t.Fatalf("rollback without new data: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE schema_migrations SET version=102,dirty=false`); err != nil {
		t.Fatal(err)
	}
	snapshot, err = Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CoreShape.Digest != CoreWorkflowAdmissionPredecessorDigest || snapshot.CoreShape.Tables != 91 {
		t.Fatalf("rollback did not restore the exact 102 schema: %s", formatShape(snapshot.CoreShape))
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || noop {
		t.Fatalf("102 predecessor preflight after rollback: noop=%t err=%v", noop, err)
	}
}
