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

// 105 gives every workflow an immutable version 1 that keeps its node UUIDs,
// configs and edges (so execution contract hashes do not change), switches
// step, launch and logical-node foreign keys to the version nodes inside one
// transaction, pins every run to version 1, refuses an undrained database and
// guards its rollback.
func TestWorkflowVersionsMigrationBackfillsVersionOne(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, base)
	migrateTestDatabaseToVersion(t, databaseURL, uint(CoreWorkflowVersionsPredecessorVersion))
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
VALUES($1,$2,'wf-versions','Agent','Synthetic','https://agent.example/run',0)`, agentID, creator)
	edges := `[{"from":"a","to":"b"}]`
	exec(`INSERT INTO workflows(id,user_id,name,description,edges) VALUES($1,$2,'versioned','kept',$3::jsonb)`, workflowID, owner, edges)
	nodeA, nodeB := uuid.New(), uuid.New()
	exec(`INSERT INTO workflow_nodes(id,workflow_id,node_key,agent_id,title,config,position) VALUES
($1,$3,'a',$4,'A','{"mode":"strict","k":[1,2]}'::jsonb,0),($2,$3,'b',$4,'B','{}'::jsonb,1)`, nodeA, nodeB, workflowID, agentID)
	run := uuid.New()
	exec(`INSERT INTO workflow_runs(id,workflow_id,user_id,status,finished_at) VALUES($1,$2,$3,'success',now())`, run, workflowID, owner)
	step := uuid.New()
	exec(`INSERT INTO workflow_run_steps(id,workflow_run_id,workflow_node_id,node_key,agent_id,status,sequence) VALUES($1,$2,$3,'a',$4,'success',0)`,
		step, run, nodeA, agentID)
	sum := sha256.Sum256([]byte(run.String()))
	exec(`INSERT INTO workflow_step_launches(workflow_run_id,workflow_node_id,workflow_run_step_id,node_key,launch_token,idempotency_key_hash,creation_fingerprint,state)
VALUES($1,$2,$3,'a',$4,$5,$5,'invalidated')`, run, nodeA, step, uuid.New(), sum[:])
	exec(`INSERT INTO workflow_run_nodes(workflow_run_id,workflow_node_id,node_key,state,effective_attempt,terminal_at) VALUES($1,$2,'a','success',1,now())`, run, nodeA)

	// Undrained: refused.
	pending := uuid.New()
	exec(`INSERT INTO workflow_runs(id,workflow_id,user_id,status) VALUES($1,$2,$3,'pending')`, pending, workflowID, owner)
	if err := applyMigrationFileErr(ctx, conn, "105_workflow_versions.up.sql"); err == nil || !strings.Contains(err.Error(), "drain workflow runs first") {
		t.Fatalf("105 with a pending run: %v", err)
	}
	exec(`UPDATE workflow_runs SET status='canceled', finished_at=now() WHERE id=$1`, pending)

	migrateTestDatabaseToVersion(t, databaseURL, uint(CoreWorkflowRevisionPredecessorVersion))
	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if noop, err := snapshot.ValidateCoreUp(); err != nil || noop {
		t.Fatalf("upgraded 105 (predecessor of current): noop=%t err=%v shape=%s", noop, err, formatShape(snapshot.CoreShape))
	}

	// Version 1 keeps the definition byte for byte.
	var versionID, currentID uuid.UUID
	var version int
	var semantics, name, description, versionEdges, digest string
	if err := conn.QueryRow(ctx, `
		SELECT v.id, v.version, v.semantics, v.name, v.description, v.edges::text, v.definition_digest, w.current_version_id
		FROM workflow_versions v JOIN workflows w ON w.id = v.workflow_id WHERE v.workflow_id = $1`, workflowID,
	).Scan(&versionID, &version, &semantics, &name, &description, &versionEdges, &digest, &currentID); err != nil {
		t.Fatal(err)
	}
	if version != 1 || semantics != "v1" || name != "versioned" || description != "kept" || currentID != versionID || len(digest) != 64 || strings.Trim(digest, "0") == "" {
		t.Fatalf("version 1 = %d %s %q %q current=%s digest=%s", version, semantics, name, description, currentID, digest)
	}
	var sameEdges bool
	if err := conn.QueryRow(ctx, `SELECT edges = $2::jsonb FROM workflow_versions WHERE id = $1`, versionID, edges).Scan(&sameEdges); err != nil || !sameEdges {
		t.Fatalf("version edges changed: %s err=%v", versionEdges, err)
	}
	var matching int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM workflow_version_nodes vn JOIN workflow_nodes n ON n.id = vn.id
		WHERE vn.workflow_version_id = $1 AND vn.config = n.config AND vn.created_at = n.created_at`, versionID).Scan(&matching); err != nil || matching != 2 {
		t.Fatalf("version nodes kept ids/config/created_at = %d err=%v", matching, err)
	}
	var runVersion uuid.UUID
	if err := conn.QueryRow(ctx, `SELECT workflow_version_id FROM workflow_runs WHERE id=$1`, run).Scan(&runVersion); err != nil || runVersion != versionID {
		t.Fatalf("run version = %s err=%v", runVersion, err)
	}

	// History now references version nodes: the projection row can be
	// replaced, but the version node history uses cannot be deleted.
	exec(`DELETE FROM workflow_nodes WHERE id = $1`, nodeA)
	if _, err := conn.Exec(ctx, `DELETE FROM workflow_version_nodes WHERE id = $1`, nodeA); err == nil || !strings.Contains(err.Error(), "foreign key") {
		t.Fatalf("deleting a referenced version node: %v", err)
	}
	// A run cannot point at another workflow's version.
	otherWorkflow := uuid.New()
	exec(`INSERT INTO workflows(id,user_id,name) VALUES($1,$2,'other')`, otherWorkflow, owner)
	if _, err := conn.Exec(ctx, `INSERT INTO workflow_runs(workflow_id,user_id,status,workflow_version_id) VALUES($1,$2,'success',$3)`,
		otherWorkflow, owner, versionID); err == nil || !strings.Contains(err.Error(), "workflow_runs_workflow_version_fkey") {
		t.Fatalf("cross-workflow version: %v", err)
	}

	// Rollback refuses once a later version exists.
	exec(`INSERT INTO workflow_versions(workflow_id,version,name,definition_digest) VALUES($1,2,'v2',repeat('a',64))`, workflowID)
	if err := applyMigrationFileErr(ctx, conn, "105_workflow_versions.down.sql"); err == nil || !strings.Contains(err.Error(), "versions beyond the backfill") {
		t.Fatalf("rollback with version 2: %v", err)
	}
}
