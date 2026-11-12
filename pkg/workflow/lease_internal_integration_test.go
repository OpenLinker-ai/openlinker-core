package workflow

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// leaseTestRun inserts a user, workflow and running run held by owner/epoch,
// with its lease expiring after ttl (negative ttl: already expired).
func leaseTestRun(t *testing.T, ttl time.Duration) (*Service, workflowRunLease) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置，跳过 workflow lease 集成测试")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	userID, workflowID, runID := uuid.New(), uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'x','lease')`,
		userID, userID.String()+"@example.invalid")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO workflows(id,user_id,name) VALUES($1,$2,'lease')`, workflowID, userID)
	require.NoError(t, err)
	svc := NewService(pool, nil)
	lease := workflowRunLease{runID: runID, owner: svc.instanceID, epoch: 3}
	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, user_id, status, attempt_count, max_attempts,
		                           lease_owner, lease_epoch, lease_mode, lease_expires_at, claimed_at)
		VALUES ($1, $2, $3, 'running', 1, 3, $4, 3, 'execute', clock_timestamp() + make_interval(secs => $5), NOW())`,
		runID, workflowID, userID, lease.owner, ttl.Seconds())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM workflow_runs WHERE id = $1`, runID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM workflows WHERE id = $1`, workflowID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, userID)
	})
	return svc, lease
}

func TestWorkflowRunLeaseRenewAndFencedWrites(t *testing.T) {
	svc, lease := leaseTestRun(t, time.Minute)
	ctx := context.Background()

	require.NoError(t, svc.renewWorkflowRunLease(ctx, lease))
	require.NoError(t, svc.fencedWrite(withWorkflowRunLease(ctx, lease), lease.runID, func(q *db.Queries) error { return nil }))

	stale := lease
	stale.epoch = lease.epoch - 1
	require.ErrorIs(t, svc.renewWorkflowRunLease(ctx, stale), errWorkflowRunLeaseLost)
	err := svc.fencedWrite(withWorkflowRunLease(ctx, stale), lease.runID, func(q *db.Queries) error {
		t.Fatal("a write ran under a stale epoch")
		return nil
	})
	require.ErrorIs(t, err, errWorkflowRunLeaseLost)

	// No lease in context is never treated as permission.
	err = svc.fencedWrite(ctx, lease.runID, func(q *db.Queries) error {
		t.Fatal("a write ran without a lease")
		return nil
	})
	require.ErrorIs(t, err, errWorkflowRunLeaseLost)
}

func TestWorkflowRunLeaseExpiredIsNeverRevived(t *testing.T) {
	svc, lease := leaseTestRun(t, -time.Second)
	require.ErrorIs(t, svc.renewWorkflowRunLease(context.Background(), lease), errWorkflowRunLeaseLost)
	err := svc.fencedWrite(withWorkflowRunLease(context.Background(), lease), lease.runID, func(q *db.Queries) error {
		t.Fatal("a write ran under an expired lease")
		return nil
	})
	require.ErrorIs(t, err, errWorkflowRunLeaseLost)
}

// A renewal that waits on the run row lock across the expiry must not revive
// the lease: expiry is checked after the lock is acquired, with the database
// clock.
func TestWorkflowRunLeaseRenewalWaitingAcrossExpiryFails(t *testing.T) {
	svc, lease := leaseTestRun(t, 1500*time.Millisecond)
	ctx := context.Background()

	blocker, err := svc.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = blocker.Exec(ctx, `SELECT 1 FROM workflow_runs WHERE id = $1 FOR UPDATE`, lease.runID)
	require.NoError(t, err)

	renewed := make(chan error, 1)
	go func() { renewed <- svc.renewWorkflowRunLease(ctx, lease) }()
	time.Sleep(2500 * time.Millisecond) // the lease expires while renewal waits
	require.NoError(t, blocker.Commit(ctx))
	select {
	case err := <-renewed:
		require.ErrorIs(t, err, errWorkflowRunLeaseLost)
	case <-time.After(5 * time.Second):
		t.Fatal("renewal did not return after the lock was released")
	}

	// Recovery takes the run back to pending and counts one recovery.
	recovered, err := svc.RecoverExpiredWorkflowRunLeases(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, recovered, int64(1))
	var status string
	var recoveries int32
	var owner *uuid.UUID
	require.NoError(t, svc.pool.QueryRow(ctx,
		`SELECT status, recovery_count, lease_owner FROM workflow_runs WHERE id = $1`, lease.runID,
	).Scan(&status, &recoveries, &owner))
	require.Equal(t, "pending", status)
	require.EqualValues(t, 1, recoveries)
	require.Nil(t, owner)
}

// Recovery never acts on a run whose row another transaction holds (here a
// renewal in flight); once that renewal commits the lease is valid again and
// recovery leaves it alone.
func TestWorkflowRunLeaseRecoveryLeavesARenewedLeaseAlone(t *testing.T) {
	svc, lease := leaseTestRun(t, -time.Second)
	ctx := context.Background()

	// Simulate a concurrent renewal that committed between recovery's
	// candidate scan and its update: the lease is valid again by the time
	// recovery acts.
	tx, err := svc.pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
		UPDATE workflow_runs SET lease_expires_at = clock_timestamp() + INTERVAL '1 minute'
		WHERE id = $1`, lease.runID)
	require.NoError(t, err)
	recovered := make(chan int64, 1)
	go func() {
		n, err := svc.RecoverExpiredWorkflowRunLeases(ctx)
		if err != nil {
			n = -1
		}
		recovered <- n
	}()
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, tx.Commit(ctx))
	select {
	case <-recovered:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not return")
	}
	var status string
	var owner *uuid.UUID
	require.NoError(t, svc.pool.QueryRow(ctx,
		`SELECT status, lease_owner FROM workflow_runs WHERE id = $1`, lease.runID,
	).Scan(&status, &owner))
	require.Equal(t, "running", status)
	require.NotNil(t, owner)
	require.Equal(t, lease.owner, *owner)
	require.NoError(t, svc.renewWorkflowRunLease(ctx, lease))
}

func TestWorkflowRunLeaseRecoveryBudgetSpentFailsRun(t *testing.T) {
	svc, lease := leaseTestRun(t, -time.Second)
	ctx := context.Background()
	_, err := svc.pool.Exec(ctx, `UPDATE workflow_runs SET recovery_count = 2 WHERE id = $1`, lease.runID)
	require.NoError(t, err)

	_, err = svc.RecoverExpiredWorkflowRunLeases(ctx)
	require.NoError(t, err)
	var status string
	var errorMessage *string
	var owner *uuid.UUID
	require.NoError(t, svc.pool.QueryRow(ctx,
		`SELECT status, error_message, lease_owner FROM workflow_runs WHERE id = $1`, lease.runID,
	).Scan(&status, &errorMessage, &owner))
	require.Equal(t, "failed", status)
	require.NotNil(t, errorMessage)
	require.Equal(t, workflowRunLeaseRecoveryFailureMsg, *errorMessage)
	require.Nil(t, owner)
}

func TestWorkflowRunLeaseReleaseKeepsRunningRunForRecovery(t *testing.T) {
	svc, lease := leaseTestRun(t, time.Minute)
	ctx := context.Background()
	svc.releaseWorkflowRunLease(ctx, lease)
	var owner *uuid.UUID
	require.NoError(t, svc.pool.QueryRow(ctx, `SELECT lease_owner FROM workflow_runs WHERE id = $1`, lease.runID).Scan(&owner))
	require.NotNil(t, owner, "a running run must keep its lease for recovery")

	_, err := svc.pool.Exec(ctx, `UPDATE workflow_runs SET status = 'paused' WHERE id = $1`, lease.runID)
	require.NoError(t, err)
	svc.releaseWorkflowRunLease(ctx, lease)
	var claimedAt *time.Time
	require.NoError(t, svc.pool.QueryRow(ctx, `SELECT lease_owner, claimed_at FROM workflow_runs WHERE id = $1`, lease.runID).Scan(&owner, &claimedAt))
	require.Nil(t, owner)
	require.Nil(t, claimedAt, "a paused run must drop its claim so it can resume")
}

// The child launch claim locks the parent and checks the lease before
// touching the launch row.
func TestWorkflowChildLaunchClaimRequiresLease(t *testing.T) {
	svc, lease := leaseTestRun(t, time.Minute)
	stale := lease
	stale.epoch++
	_, _, err := svc.claimWorkflowChildLaunch(
		withWorkflowRunLease(context.Background(), stale),
		dbWorkflowRunIdentity{ID: lease.runID, UserID: leaseRunOwner(t, svc, lease.runID)},
		uuid.New(), uuid.New(), "worker", 1, testRunCreationIdentity(),
	)
	require.True(t, errors.Is(err, errWorkflowRunLeaseLost), "err = %v", err)
}

func leaseRunOwner(t *testing.T, svc *Service, runID uuid.UUID) uuid.UUID {
	t.Helper()
	var owner uuid.UUID
	require.NoError(t, svc.pool.QueryRow(context.Background(), `SELECT user_id FROM workflow_runs WHERE id = $1`, runID).Scan(&owner))
	return owner
}

func testRunCreationIdentity() runtime.RunCreationIdentity {
	hash := make([]byte, 32)
	fingerprint := make([]byte, 32)
	hash[0], fingerprint[0] = 1, 2
	return runtime.RunCreationIdentity{IdempotencyKeyHash: hash, CreationFingerprint: fingerprint}
}
