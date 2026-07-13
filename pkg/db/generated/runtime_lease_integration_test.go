package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const runtimeV2ContractDigest = "3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9"

var runtimeV2RequiredFeatures = []string{
	"lease_fence",
	"assignment_confirm",
	"renew",
	"resume",
	"event_ack",
	"result_ack",
	"cancel",
	"persistent_spool",
}

func TestRuntimeLeaseAndResumeQueriesPostgres16(t *testing.T) {
	dsn := os.Getenv("RUNTIME_V2_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("RUNTIME_V2_TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	fixture := newRuntimeLeaseFixture()
	insertRuntimeLeaseFixture(t, ctx, pool, fixture)

	t.Run("stale attachment generation is fenced after reattach commits", func(t *testing.T) {
		oldAttachment, err := New(pool).GetActiveRuntimeSessionAttachment(ctx, fixture.sourceSessionID)
		if err != nil {
			t.Fatal(err)
		}

		rotateTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rotateQueries := New(rotateTx)
		if _, err = rotateQueries.GetRuntimeSessionForUpdate(ctx, fixture.sourceSessionID); err != nil {
			_ = rotateTx.Rollback(ctx)
			t.Fatal(err)
		}
		reason := "test transport reattached"
		if _, err = rotateQueries.CloseRuntimeSessionAttachment(ctx, CloseRuntimeSessionAttachmentParams{
			RuntimeSessionID: fixture.sourceSessionID,
			CoreInstanceID:   fixture.coreID,
			AttachmentID:     oldAttachment.ID,
			DisconnectReason: &reason,
		}); err != nil {
			_ = rotateTx.Rollback(ctx)
			t.Fatal(err)
		}
		newAttachment, err := rotateQueries.CreateRuntimeSessionAttachment(ctx, CreateRuntimeSessionAttachmentParams{
			RuntimeSessionID: fixture.sourceSessionID,
			CoreInstanceID:   fixture.coreID,
			AttachmentKind:   "resumed",
		})
		if err != nil {
			_ = rotateTx.Rollback(ctx)
			t.Fatal(err)
		}
		if err = rotateTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		staleTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = staleTx.Rollback(ctx) }()
		staleQueries := New(staleTx)
		if _, err = staleQueries.LockRuntimeSessionForPrincipalValidation(ctx, LockRuntimeSessionForPrincipalValidationParams{
			RuntimeSessionID: fixture.sourceSessionID, NodeID: fixture.nodeID, AgentID: fixture.agentID,
			CredentialID: fixture.tokenID, WorkerID: fixture.workerID, AttachedCoreInstanceID: &fixture.coreID,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err = staleQueries.LockRuntimeNodeForPrincipalValidation(ctx, LockRuntimeNodeForPrincipalValidationParams{
			NodeID: fixture.nodeID, DeviceCertificateSerial: fixture.certificateSerial,
			DevicePublicKeyThumbprint: fixture.thumbprint,
		}); err != nil {
			t.Fatal(err)
		}
		agentID := fixture.agentID
		if _, err = staleQueries.LockRuntimeCredentialForPrincipalValidation(ctx, LockRuntimeCredentialForPrincipalValidationParams{
			CredentialID: fixture.tokenID, AgentID: &agentID,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err = staleQueries.LockRuntimeSessionAttachmentForPrincipalValidation(ctx, LockRuntimeSessionAttachmentForPrincipalValidationParams{
			AttachmentID: oldAttachment.ID, RuntimeSessionID: fixture.sourceSessionID, CoreInstanceID: fixture.coreID,
		}); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("stale attachment lock error = %v", err)
		}
		if _, err = staleQueries.LockRuntimeSessionAttachmentForPrincipalValidation(ctx, LockRuntimeSessionAttachmentForPrincipalValidationParams{
			AttachmentID: newAttachment.ID, RuntimeSessionID: fixture.sourceSessionID, CoreInstanceID: fixture.coreID,
		}); err != nil {
			t.Fatalf("current attachment lock error = %v", err)
		}
	})

	t.Run("offer and exact mirror", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		q := New(tx)
		lockRuntimeFixturePrincipal(t, ctx, q, fixture, fixture.sourceSessionID)
		freshAfter := time.Now().Add(-time.Minute)
		if _, err := q.ClaimRuntimeSessionSlot(ctx, ClaimRuntimeSessionSlotParams{
			RuntimeSessionID: fixture.sourceSessionID, AgentID: fixture.agentID,
			CoreInstanceID: fixture.coreID, HeartbeatAfter: freshAfter,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := q.ClaimRuntimeNodeSlot(ctx, ClaimRuntimeNodeSlotParams{
			NodeID: fixture.nodeID, LastSeenAfter: freshAfter,
		}); err != nil {
			t.Fatal(err)
		}

		candidate, err := q.LockNextClaimableRuntimeV2RunForAgent(ctx, fixture.agentID)
		if err != nil || candidate.ID != fixture.runID || candidate.DatabaseNow.IsZero() {
			t.Fatalf("claim candidate = %#v, %v", candidate, err)
		}
		attempt, err := q.CreateAgentNodeRunOffer(ctx, CreateAgentNodeRunOfferParams{
			AttemptID: fixture.attemptID, LeaseID: fixture.leaseID,
			CoreInstanceID: fixture.coreID, OfferTtlMs: 30_000,
			LeaseTtlMs: 60_000, AttemptTtlMs: 300_000,
			RuntimeSessionID: fixture.sourceSessionID, RunID: fixture.runID,
			NodeID: fixture.nodeID, CredentialID: fixture.tokenID,
			WorkerID: fixture.workerID,
		})
		if err != nil || attempt.OfferNo != 1 || attempt.FencingToken != 1 {
			t.Fatalf("CreateAgentNodeRunOffer = %#v, %v", attempt, err)
		}
		mirrored, err := q.MirrorRunAgentNodeOffer(ctx, MirrorRunAgentNodeOfferParams{
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: attempt.FencingToken,
			RuntimeSessionID: &fixture.sourceSessionID, NodeID: &fixture.nodeID,
			CredentialID: &fixture.tokenID, WorkerID: &fixture.workerID,
			CoreInstanceID: fixture.coreID,
		})
		if err != nil || mirrored.DispatchState != "offered" ||
			mirrored.ActiveAttemptID == nil || *mirrored.ActiveAttemptID != fixture.attemptID {
			t.Fatalf("MirrorRunAgentNodeOffer = %#v, %v", mirrored, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("offer transaction invariant failed: %v", err)
		}
	})

	t.Run("repeat claim returns the original live offer", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		q := New(tx)
		lockRuntimeFixturePrincipal(t, ctx, q, fixture, fixture.sourceSessionID)
		existing, err := q.GetExistingUnacceptedRunOfferForSession(ctx, GetExistingUnacceptedRunOfferForSessionParams{
			RuntimeSessionID: &fixture.sourceSessionID, NodeID: &fixture.nodeID,
			AgentID: fixture.agentID, CredentialID: &fixture.tokenID,
			WorkerID: &fixture.workerID,
		})
		if err != nil || existing.AttemptID != fixture.attemptID || existing.LeaseID != fixture.leaseID {
			t.Fatalf("GetExistingUnacceptedRunOfferForSession = %#v, %v", existing, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("assignment ACK and renew preserve the Attempt mirror", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		q := New(tx)
		lockRuntimeFixturePrincipal(t, ctx, q, fixture, fixture.sourceSessionID)
		if _, err := q.LockRunForLeaseMutation(ctx, fixture.runID); err != nil {
			t.Fatal(err)
		}
		if _, err := q.LockAgentNodeRunAttemptForLeaseMutation(ctx, LockAgentNodeRunAttemptForLeaseMutationParams{
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			RuntimeSessionID: &fixture.sourceSessionID, NodeID: &fixture.nodeID,
			CredentialID: &fixture.tokenID, WorkerID: &fixture.workerID,
		}); err != nil {
			t.Fatal(err)
		}
		attempt, err := q.ConfirmRunAssignment(ctx, ConfirmRunAssignmentParams{
			LeaseTtlMs: 60_000, CoreInstanceID: fixture.coreID,
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			RuntimeSessionID: &fixture.sourceSessionID, NodeID: &fixture.nodeID,
			CredentialID: &fixture.tokenID, WorkerID: &fixture.workerID,
		})
		if err != nil || attempt.AttemptNo == nil || *attempt.AttemptNo != 1 {
			t.Fatalf("ConfirmRunAssignment = %#v, %v", attempt, err)
		}
		mirrored, err := q.MirrorRunConfirmedAssignment(ctx, MirrorRunConfirmedAssignmentParams{
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			RuntimeSessionID: &fixture.sourceSessionID, NodeID: &fixture.nodeID,
			CredentialID: &fixture.tokenID, WorkerID: &fixture.workerID,
			CoreInstanceID: fixture.coreID,
		})
		if err != nil || mirrored.DispatchState != "executing" || mirrored.AttemptCount != 1 {
			t.Fatalf("MirrorRunConfirmedAssignment = %#v, %v", mirrored, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("ACK transaction invariant failed: %v", err)
		}

		tx, err = pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		q = New(tx)
		lockRuntimeFixturePrincipal(t, ctx, q, fixture, fixture.sourceSessionID)
		if _, err := q.LockRunForLeaseMutation(ctx, fixture.runID); err != nil {
			t.Fatal(err)
		}
		renewed, err := q.RenewAgentNodeRunAttempt(ctx, RenewAgentNodeRunAttemptParams{
			LeaseTtlMs: 90_000, CoreInstanceID: fixture.coreID,
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			RuntimeSessionID: &fixture.sourceSessionID, NodeID: &fixture.nodeID,
			CredentialID: &fixture.tokenID, WorkerID: &fixture.workerID,
		})
		if err != nil || renewed.LastRenewedAt == nil {
			t.Fatalf("RenewAgentNodeRunAttempt = %#v, %v", renewed, err)
		}
		if _, err := q.MirrorRunLeaseRenewal(ctx, MirrorRunLeaseRenewalParams{
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			CoreInstanceID: fixture.coreID,
		}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("renew transaction invariant failed: %v", err)
		}
	})

	t.Run("capacity release evidence is a one-winner CAS", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		q := New(tx)
		if _, err := q.LockRunForLeaseMutation(ctx, fixture.runID); err != nil {
			t.Fatal(err)
		}
		if _, err := q.LockRunAttemptForResult(ctx, LockRunAttemptForResultParams{
			RunID: fixture.runID, ID: fixture.attemptID,
		}); err != nil {
			t.Fatal(err)
		}
		params := MarkRunAttemptCapacityReleasedParams{
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
		}
		released, err := q.MarkRunAttemptCapacityReleased(ctx, params)
		if err != nil || released.RuntimeSessionID != fixture.sourceSessionID ||
			released.NodeID != fixture.nodeID || released.SlotReleasedAt.IsZero() {
			t.Fatalf("first capacity release = %#v, %v", released, err)
		}
		if _, err := q.MarkRunAttemptCapacityReleased(ctx, params); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("capacity release replay error = %v", err)
		}
		// The deferred finished<=>released invariant intentionally rejects a
		// standalone release. Rollback keeps the live fixture for resume tests.
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("consumed grant survives only natural expiry for stored ACK replay", func(t *testing.T) {
		moveRuntimeFixtureToReplacementSession(t, ctx, pool, fixture)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		q := New(tx)
		lockRuntimeFixturePrincipal(t, ctx, q, fixture, fixture.targetSessionID)
		if _, err := q.LockRunForLeaseMutation(ctx, fixture.runID); err != nil {
			t.Fatal(err)
		}
		if _, err := q.LockRunAttemptForResult(ctx, LockRunAttemptForResultParams{
			RunID: fixture.runID, ID: fixture.attemptID,
		}); err != nil {
			t.Fatal(err)
		}
		grant, err := q.CreateRuntimeResumeGrant(ctx, CreateRuntimeResumeGrantParams{
			GrantID: fixture.grantID, Permission: "continue_execution",
			CoreInstanceID: fixture.coreID, GrantTtlMs: 250,
			TargetSessionID: fixture.targetSessionID, RunID: fixture.runID,
			AttemptID: fixture.attemptID, LeaseID: fixture.leaseID,
			FencingToken: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		active, err := q.LockActiveRuntimeResumeGrantForAttemptTarget(ctx, LockActiveRuntimeResumeGrantForAttemptTargetParams{
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			AgentID: fixture.agentID, NodeID: fixture.nodeID,
			WorkerID: fixture.workerID, TargetSessionID: fixture.targetSessionID,
			TargetCredentialID: fixture.tokenID,
			AllowedPermission:  "upload_spool_only", CoreInstanceID: &fixture.coreID,
		})
		if err != nil || active.ID != grant.ID {
			t.Fatalf("LockActiveRuntimeResumeGrantForAttemptTarget = %#v, %v", active, err)
		}
		if _, err := q.ConsumeRuntimeResumeGrant(ctx, ConsumeRuntimeResumeGrantParams{
			GrantID: grant.ID, RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			TargetSessionID:    fixture.targetSessionID,
			TargetCredentialID: fixture.tokenID, Permission: grant.Permission,
		}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		time.Sleep(300 * time.Millisecond)
		tx, err = pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		q = New(tx)
		lockRuntimeFixturePrincipal(t, ctx, q, fixture, fixture.targetSessionID)
		if _, err := q.LockConsumedRuntimeResumeGrantForStoredReplay(ctx, LockConsumedRuntimeResumeGrantForStoredReplayParams{
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			AgentID: fixture.agentID, NodeID: fixture.nodeID,
			WorkerID: fixture.workerID, TargetSessionID: fixture.targetSessionID,
			TargetCredentialID: fixture.tokenID,
		}); err != nil {
			t.Fatalf("natural-expiry stored replay = %v", err)
		}
		if _, err := q.RevokeExpiredRuntimeResumeGrant(ctx, RevokeExpiredRuntimeResumeGrantParams{
			GrantID: grant.ID, ExpectedExpiresAt: grant.ExpiresAt,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := q.LockConsumedRuntimeResumeGrantForStoredReplay(ctx, LockConsumedRuntimeResumeGrantForStoredReplayParams{
			RunID: fixture.runID, AttemptID: fixture.attemptID,
			LeaseID: fixture.leaseID, FencingToken: 1,
			AgentID: fixture.agentID, NodeID: fixture.nodeID,
			WorkerID: fixture.workerID, TargetSessionID: fixture.targetSessionID,
			TargetCredentialID: fixture.tokenID,
		}); err != nil {
			t.Fatalf("reaped natural-expiry stored replay = %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

type runtimeLeaseFixture struct {
	userID, agentID, tokenID, nodeID, coreID uuid.UUID
	sourceSessionID, targetSessionID         uuid.UUID
	runID, attemptID, leaseID, grantID       uuid.UUID
	workerID, certificateSerial, thumbprint  string
}

func newRuntimeLeaseFixture() runtimeLeaseFixture {
	suffix := stringsWithoutDashes(uuid.New().String())[:12]
	return runtimeLeaseFixture{
		userID: uuid.New(), agentID: uuid.New(), tokenID: uuid.New(),
		nodeID: uuid.New(), coreID: uuid.New(), sourceSessionID: uuid.New(),
		targetSessionID: uuid.New(), runID: uuid.New(), attemptID: uuid.New(),
		leaseID: uuid.New(), grantID: uuid.New(), workerID: "worker-" + suffix,
		certificateSerial: "cert-" + suffix, thumbprint: "sha256-" + suffix,
	}
}

func stringsWithoutDashes(value string) string {
	result := make([]byte, 0, len(value))
	for i := range len(value) {
		if value[i] != '-' {
			result = append(result, value[i])
		}
	}
	return string(result)
}

func insertRuntimeLeaseFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f runtimeLeaseFixture) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	suffix := stringsWithoutDashes(f.userID.String())[:12]
	statements := []struct {
		sql  string
		args []any
	}{
		{
			`INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
			 VALUES ($1, $2, 'hash', 'Runtime Lease Test', TRUE, TRUE)`,
			[]any{f.userID, "runtime-lease-" + suffix + "@example.test"},
		},
		{
			`INSERT INTO agents (
			     id, creator_id, slug, name, description, endpoint_url,
			     price_per_call_cents, connection_mode
			 ) VALUES ($1, $2, $3, 'Runtime Lease Test', 'runtime test fixture',
			           'openlinker-runtime://test', 0, 'runtime')`,
			[]any{f.agentID, f.userID, "runtime-lease-" + suffix},
		},
		{
			`INSERT INTO agent_tokens (
			     id, agent_id, creator_user_id, name, prefix, token_hash,
			     scopes, status, redeemed_at
			 ) VALUES ($1, $2, $3, 'Runtime Lease Test', $4, 'hash',
			           ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
			[]any{f.tokenID, f.agentID, f.userID, "ol_agent_" + suffix},
		},
		{
			`INSERT INTO runtime_nodes (
			     node_id, display_name, device_certificate_serial,
			     device_public_key_thumbprint, node_version, protocol_version,
			     runtime_contract_id, runtime_contract_digest, features,
			     capacity, last_seen_at
			 ) VALUES ($1, 'Runtime Lease Test', $2, $3, 'test-v2', 2,
			           'openlinker.runtime.v2', $4, $5, 2, clock_timestamp())`,
			[]any{f.nodeID, f.certificateSerial, f.thumbprint, runtimeV2ContractDigest, runtimeV2RequiredFeatures},
		},
		{
			`INSERT INTO runtime_sessions (
			     runtime_session_id, node_id, agent_id, credential_id, worker_id,
			     session_epoch, device_certificate_serial, node_version,
			     protocol_version, runtime_contract_id, runtime_contract_digest,
			     features, capacity, attached_core_instance_id
			 ) VALUES ($1, $2, $3, $4, $5, 1, $6, 'test-v2', 2,
			           'openlinker.runtime.v2', $7, $8, 2, $9)`,
			[]any{f.sourceSessionID, f.nodeID, f.agentID, f.tokenID, f.workerID, f.certificateSerial, runtimeV2ContractDigest, runtimeV2RequiredFeatures, f.coreID},
		},
		{
			`INSERT INTO runtime_session_attachments (
			     runtime_session_id, core_instance_id, attachment_kind
			 ) VALUES ($1, $2, 'connected')`,
			[]any{f.sourceSessionID, f.coreID},
		},
		{
			`INSERT INTO runs (
			     id, user_id, agent_id, input, status, cost_cents,
			     platform_fee_cents, creator_revenue_cents, source,
			     idempotency_key_hash, idempotency_fingerprint,
			     request_metadata, connection_mode_snapshot,
			     endpoint_idempotency_snapshot, max_offer_count, max_attempts,
			     dispatch_deadline_at, run_deadline_at
			 ) VALUES ($1, $2, $3, '{"prompt":"lease test"}'::jsonb,
			           'running', 0, 0, 0, 'api', decode(repeat('aa', 32), 'hex'),
			           decode(repeat('bb', 32), 'hex'), '{}'::jsonb,
			           'runtime', NULL, 5, 3,
			           clock_timestamp() + INTERVAL '5 minutes',
			           clock_timestamp() + INTERVAL '15 minutes')`,
			[]any{f.runID, f.userID, f.agentID},
		},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("fixture statement failed: %v\n%s", err, statement.sql)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("fixture invariant failed: %v", err)
	}
}

func lockRuntimeFixturePrincipal(t *testing.T, ctx context.Context, q *Queries, f runtimeLeaseFixture, sessionID uuid.UUID) {
	t.Helper()
	resolved, err := q.ResolveRuntimeWorkerSessionPrincipal(ctx, ResolveRuntimeWorkerSessionPrincipalParams{
		NodeID: f.nodeID, AgentID: f.agentID, CredentialID: f.tokenID,
		WorkerID: f.workerID, DeviceCertificateSerial: f.certificateSerial,
		DevicePublicKeyThumbprint: f.thumbprint, CoreInstanceID: f.coreID,
	})
	if err != nil || resolved.RuntimeSessionID != sessionID || resolved.DatabaseNow.IsZero() {
		t.Fatalf("ResolveRuntimeWorkerSessionPrincipal = %#v, %v", resolved, err)
	}
	if _, err := q.LockRuntimeSessionForPrincipalValidation(ctx, LockRuntimeSessionForPrincipalValidationParams{
		RuntimeSessionID: sessionID, NodeID: f.nodeID, AgentID: f.agentID,
		CredentialID: f.tokenID, WorkerID: f.workerID,
		AttachedCoreInstanceID: &f.coreID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.LockRuntimeNodeForPrincipalValidation(ctx, LockRuntimeNodeForPrincipalValidationParams{
		NodeID: f.nodeID, DeviceCertificateSerial: f.certificateSerial,
		DevicePublicKeyThumbprint: f.thumbprint,
	}); err != nil {
		t.Fatal(err)
	}
	agentID := f.agentID
	if _, err := q.LockRuntimeCredentialForPrincipalValidation(ctx, LockRuntimeCredentialForPrincipalValidationParams{
		CredentialID: f.tokenID, AgentID: &agentID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.LockRuntimeSessionAttachmentForPrincipalValidation(ctx, LockRuntimeSessionAttachmentForPrincipalValidationParams{
		AttachmentID: resolved.AttachmentID, RuntimeSessionID: sessionID, CoreInstanceID: f.coreID,
	}); err != nil {
		t.Fatal(err)
	}
}

func moveRuntimeFixtureToReplacementSession(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f runtimeLeaseFixture) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := New(tx)
	if _, err := q.GetRuntimeSessionForUpdate(ctx, f.sourceSessionID); err != nil {
		t.Fatal(err)
	}
	attachment, err := q.GetActiveRuntimeSessionAttachment(ctx, f.sourceSessionID)
	if err != nil {
		t.Fatal(err)
	}
	disconnectReason := "test process restart"
	if _, err := q.CloseRuntimeSessionAttachment(ctx, CloseRuntimeSessionAttachmentParams{
		RuntimeSessionID: f.sourceSessionID, CoreInstanceID: f.coreID,
		AttachmentID:     attachment.ID,
		DisconnectReason: &disconnectReason,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.CloseRuntimeSession(ctx, CloseRuntimeSessionParams{
		RuntimeSessionID: f.sourceSessionID, CoreInstanceID: f.coreID,
		Status: "offline",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO runtime_sessions (
		    runtime_session_id, node_id, agent_id, credential_id, worker_id,
		    session_epoch, device_certificate_serial, node_version,
		    protocol_version, runtime_contract_id, runtime_contract_digest,
		    features, capacity, attached_core_instance_id
		) VALUES ($1, $2, $3, $4, $5, 2, $6, 'test-v2', 2,
		          'openlinker.runtime.v2', $7, $8, 2, $9)`,
		f.targetSessionID, f.nodeID, f.agentID, f.tokenID, f.workerID,
		f.certificateSerial, runtimeV2ContractDigest, runtimeV2RequiredFeatures,
		f.coreID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO runtime_session_attachments (
		    runtime_session_id, core_instance_id, attachment_kind
		) VALUES ($1, $2, 'resumed')`, f.targetSessionID, f.coreID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("replacement Session invariant failed: %v", err)
	}
}

func TestRuntimeSessionScopeTriggerRejectsCallOnlyCredential(t *testing.T) {
	dsn := os.Getenv("RUNTIME_V2_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("RUNTIME_V2_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	f := newRuntimeLeaseFixture()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	suffix := stringsWithoutDashes(f.userID.String())[:12]
	setup := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO users (id, email, password_hash, display_name, is_creator)
		  VALUES ($1, $2, 'hash', 'Scope Test', TRUE)`, []any{f.userID, "runtime-scope-" + suffix + "@example.test"}},
		{`INSERT INTO agents (id, creator_id, slug, name, description, endpoint_url,
		      price_per_call_cents, connection_mode)
		  VALUES ($1, $2, $3, 'Scope Test', 'scope fixture',
		      'openlinker-runtime://test', 0, 'runtime')`, []any{f.agentID, f.userID, "runtime-scope-" + suffix}},
		{`INSERT INTO agent_tokens (id, agent_id, creator_user_id, name, prefix,
		      token_hash, scopes, status, redeemed_at)
		  VALUES ($1, $2, $3, 'Scope Test', $4, 'hash', ARRAY['agent:call']::text[],
		      'active_runtime', clock_timestamp())`, []any{f.tokenID, f.agentID, f.userID, "ol_agent_" + suffix}},
		{`INSERT INTO runtime_nodes (node_id, display_name, device_certificate_serial,
		      device_public_key_thumbprint, node_version, protocol_version,
		      runtime_contract_id, runtime_contract_digest, features, capacity,
		      last_seen_at)
		  VALUES ($1, 'Scope Test', $2, $3, 'test-v2', 2,
		      'openlinker.runtime.v2', $4, $5, 1, clock_timestamp())`, []any{f.nodeID, f.certificateSerial, f.thumbprint, runtimeV2ContractDigest, runtimeV2RequiredFeatures}},
	}
	for _, statement := range setup {
		if _, err := tx.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO runtime_sessions (
		    runtime_session_id, node_id, agent_id, credential_id, worker_id,
		    session_epoch, device_certificate_serial, node_version,
		    protocol_version, runtime_contract_id, runtime_contract_digest,
		    features, capacity, attached_core_instance_id
		) VALUES ($1, $2, $3, $4, $5, 1, $6, 'test-v2', 2,
		          'openlinker.runtime.v2', $7, $8, 1, $9)`,
		f.sourceSessionID, f.nodeID, f.agentID, f.tokenID, f.workerID,
		f.certificateSerial, runtimeV2ContractDigest, runtimeV2RequiredFeatures,
		f.coreID,
	)
	if err == nil || !containsPostgresMessage(err, "inactive runtime credential") {
		t.Fatalf("call-only credential Session insert error = %v", err)
	}
}

func containsPostgresMessage(err error, fragment string) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.Contains(pgErr.Message, fragment)
	}
	return strings.Contains(fmt.Sprint(err), fragment)
}
