package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHasActiveRuntimeSessionForAgentAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err = tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		t.Fatalf("defer constraints: %v", err)
	}

	userID, agentID, tokenID := uuid.New(), uuid.New(), uuid.New()
	nodeID, sessionID, coreID := uuid.New(), uuid.New(), uuid.New()
	features := []string{
		"lease_fence", "assignment_confirm", "renew", "resume",
		"event_ack", "result_ack", "cancel", "persistent_spool", "session_drain",
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, display_name, is_creator)
		VALUES ($1, $2, 'hash', 'Signal Test', TRUE)`, userID, userID.String()+"@example.test"); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO agents (
			id, creator_id, slug, name, description, endpoint_url,
			price_per_call_cents, connection_mode
		) VALUES ($1, $2, $3, 'Signal Agent', 'Signal integration fixture',
			'openlinker-runtime://node', 0, 'runtime')`,
		agentID, userID, "signal-"+agentID.String()); err != nil {
		t.Fatalf("insert agent: %v", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO agent_tokens (
			id, agent_id, creator_user_id, name, prefix, token_hash,
			scopes, status, redeemed_at
		) VALUES ($1, $2, $3, 'Signal token', $4, $5,
			ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		tokenID, agentID, userID, "ol_agent_"+tokenID.String()[:8], tokenID.String()); err != nil {
		t.Fatalf("insert token: %v", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO runtime_nodes (
			node_id, display_name, device_certificate_serial,
			device_public_key_thumbprint, node_version, protocol_version,
			runtime_contract_id, runtime_contract_digest, features, capacity,
			last_seen_at
		) VALUES ($1, 'Signal Node', $2, $3, 'test-v2', 2,
			'openlinker.runtime.v2',
			'3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9',
			$4, 4, clock_timestamp() - INTERVAL '46 seconds')`,
		nodeID, nodeID.String(), "sha256:"+nodeID.String(), features); err != nil {
		t.Fatalf("insert node: %v", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO runtime_sessions (
			runtime_session_id, node_id, agent_id, credential_id, worker_id,
			session_epoch, device_certificate_serial, node_version,
			protocol_version, runtime_contract_id, runtime_contract_digest,
			features, capacity, status, attached_core_instance_id, heartbeat_at
		) VALUES ($1, $2, $3, $4, 'worker-signal', 1, $5, 'test-v2', 2,
			'openlinker.runtime.v2',
			'3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9',
			$6, 4, 'active', $7, clock_timestamp() - INTERVAL '46 seconds')`,
		sessionID, nodeID, agentID, tokenID, nodeID.String(), features, coreID); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO runtime_session_attachments (
			runtime_session_id, core_instance_id, attachment_kind
		) VALUES ($1, $2, 'connected')`, sessionID, coreID); err != nil {
		t.Fatalf("insert attachment: %v", err)
	}

	queries := New(tx)
	active, err := queries.HasActiveRuntimeSessionForAgent(ctx, agentID)
	if err != nil || !active {
		t.Fatalf("lease-backed v2 session with replaced database heartbeat active = %v, %v", active, err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE runtime_session_attachments
		SET detached_at = clock_timestamp()
		WHERE runtime_session_id = $1`, sessionID); err != nil {
		t.Fatalf("detach session: %v", err)
	}
	active, err = queries.HasActiveRuntimeSessionForAgent(ctx, agentID)
	if err != nil || active {
		t.Fatalf("detached v2 session active = %v, %v", active, err)
	}
	active, err = queries.HasActiveRuntimeSessionForAgent(ctx, uuid.New())
	if err != nil || active {
		t.Fatalf("unknown Agent active = %v, %v", active, err)
	}
}
