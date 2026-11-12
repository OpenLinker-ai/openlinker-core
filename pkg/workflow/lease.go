package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Workflow run leases.
//
// A run is executed by exactly one lease holder at a time. Claiming a run
// increases lease_epoch and sets lease_owner and lease_expires_at from the
// database clock. Every write the executor makes (steps, child launches, the
// run's terminal state) happens in a transaction that first locks the parent
// run row and then checks owner, epoch and expiry against clock_timestamp()
// inside that lock, so a lock wait that crosses the expiry cannot let a stale
// executor through. Renewal and recovery use the same lock-then-check order.

const (
	defaultWorkflowRunLeaseTTL         = 60 * time.Second
	defaultWorkflowRunLeaseRenewEvery  = 15 * time.Second
	workflowRunLeaseModeExecute        = "execute"
	workflowRunLeaseRecoveryBatch      = 50
	workflowRunLeaseRecoveryLastError  = "workflow run lease expired before the executor finished"
	workflowRunLeaseRecoveryFailureMsg = "workflow run lease expired and its recovery budget is spent"
)

// errWorkflowRunLeaseLost means this executor no longer holds the run. It is
// not a workflow failure: nothing is written and the run is left to recovery.
var errWorkflowRunLeaseLost = errors.New("workflow run lease lost")

type workflowRunLease struct {
	runID uuid.UUID
	owner uuid.UUID
	epoch int64
}

type workflowRunLeaseKey struct{}

func withWorkflowRunLease(ctx context.Context, lease workflowRunLease) context.Context {
	return context.WithValue(ctx, workflowRunLeaseKey{}, lease)
}

func workflowRunLeaseFrom(ctx context.Context) (workflowRunLease, bool) {
	lease, ok := ctx.Value(workflowRunLeaseKey{}).(workflowRunLease)
	return lease, ok && lease.owner != uuid.Nil && lease.epoch > 0
}

// SetWorkflowRunLeaseTiming overrides lease timing; it exists for tests.
func (s *Service) SetWorkflowRunLeaseTiming(ttl, renewEvery time.Duration) {
	if ttl > 0 {
		s.leaseTTL = ttl
	}
	if renewEvery > 0 {
		s.leaseRenewEvery = renewEvery
	}
}

func (s *Service) workflowRunLeaseTTL() time.Duration {
	if s.leaseTTL > 0 {
		return s.leaseTTL
	}
	return defaultWorkflowRunLeaseTTL
}

func (s *Service) workflowRunLeaseRenewEvery() time.Duration {
	if s.leaseRenewEvery > 0 {
		return s.leaseRenewEvery
	}
	return defaultWorkflowRunLeaseRenewEvery
}

// lockWorkflowRunLease locks the parent run row and checks the lease held in
// ctx. It must be the first statement of the transaction.
func (s *Service) lockWorkflowRunLease(ctx context.Context, tx pgx.Tx, runID uuid.UUID) error {
	lease, ok := workflowRunLeaseFrom(ctx)
	if !ok || lease.runID != runID {
		return fmt.Errorf("%w: no lease for workflow run %s", errWorkflowRunLeaseLost, runID)
	}
	// Two statements on purpose. A single SELECT ... FOR UPDATE computes its
	// target list, clock_timestamp() included, before a lock wait ends, so a
	// caller that waited across the expiry would still see the lease as
	// held. Lock first; check with a fresh clock once the lock is ours.
	var locked int
	err := tx.QueryRow(ctx, `SELECT 1 FROM workflow_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return errWorkflowRunLeaseLost
	}
	if err != nil {
		return err
	}
	var held bool
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(lease_owner = $2 AND lease_epoch = $3 AND lease_expires_at > clock_timestamp(), false)
		FROM workflow_runs
		WHERE id = $1
	`, runID, lease.owner, lease.epoch).Scan(&held); err != nil {
		return err
	}
	if !held {
		return errWorkflowRunLeaseLost
	}
	return nil
}

// fencedWrite runs fn after locking the parent run and checking the lease.
// Services built on a fake query layer (no pool) keep the unfenced path.
func (s *Service) fencedWrite(ctx context.Context, runID uuid.UUID, fn func(*db.Queries) error) error {
	if s.pool == nil {
		return fn(s.queries)
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.lockWorkflowRunLease(ctx, tx, runID); err != nil {
			return err
		}
		if err := fn(s.queries.WithTx(tx)); err != nil {
			return err
		}
		return notifyWorkflowRunChanged(ctx, tx, runID)
	})
}

// claimPendingWorkflowRunWithLease claims the oldest runnable pending run.
// It honours workflow admission like the generated claim.
func (s *Service) claimPendingWorkflowRunWithLease(ctx context.Context) (db.WorkflowRun, workflowRunLease, error) {
	var runID uuid.UUID
	var epoch int64
	err := s.pool.QueryRow(ctx, `
		WITH candidate AS (
		    SELECT id
		    FROM workflow_runs
		    WHERE status = 'pending'
		      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		      AND EXISTS (
		          SELECT 1 FROM workflow_admission
		          WHERE singleton AND state = 'open'
		          FOR SHARE
		      )
		    ORDER BY created_at ASC
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		UPDATE workflow_runs r
		SET status = 'running',
		    claimed_at = NOW(),
		    next_retry_at = NULL,
		    attempt_count = r.attempt_count + 1,
		    last_worker_error = NULL,
		    updated_at = NOW(),
		    lease_owner = $1,
		    lease_expires_at = clock_timestamp() + make_interval(secs => $2),
		    lease_epoch = r.lease_epoch + 1,
		    lease_mode = 'execute',
		    revision = r.revision + 1
		FROM candidate
		WHERE r.id = candidate.id
		RETURNING r.id, r.lease_epoch
	`, s.instanceID, s.workflowRunLeaseTTL().Seconds()).Scan(&runID, &epoch)
	if err != nil {
		return db.WorkflowRun{}, workflowRunLease{}, err
	}
	// The claim already advanced the revision; this notification is best
	// effort (streams also poll).
	_, _ = s.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, workflowRunChangedChannel, runID.String())
	run, err := s.queries.GetWorkflowRunByID(ctx, runID)
	if err != nil {
		return db.WorkflowRun{}, workflowRunLease{}, err
	}
	return run, workflowRunLease{runID: runID, owner: s.instanceID, epoch: epoch}, nil
}

// acquireNewWorkflowRunLease takes the first lease of a run this request just
// created as 'running' (synchronous /run and step rerun). The run has never
// been leased, so epoch 0 identifies it; nothing else can claim a running run.
func (s *Service) acquireNewWorkflowRunLease(ctx context.Context, runID uuid.UUID) (workflowRunLease, error) {
	var epoch int64
	err := s.pool.QueryRow(ctx, `
		UPDATE workflow_runs
		SET lease_owner = $2,
		    lease_expires_at = clock_timestamp() + make_interval(secs => $3),
		    lease_epoch = lease_epoch + 1,
		    lease_mode = 'execute',
		    claimed_at = COALESCE(claimed_at, NOW())
		WHERE id = $1 AND status = 'running' AND lease_owner IS NULL AND lease_epoch = 0
		RETURNING lease_epoch
	`, runID, s.instanceID, s.workflowRunLeaseTTL().Seconds()).Scan(&epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowRunLease{}, errWorkflowRunLeaseLost
	}
	if err != nil {
		return workflowRunLease{}, err
	}
	return workflowRunLease{runID: runID, owner: s.instanceID, epoch: epoch}, nil
}

// renewWorkflowRunLease extends a lease that is still held and unexpired. An
// expired lease is never revived; only a new claim can take the run again.
func (s *Service) renewWorkflowRunLease(ctx context.Context, lease workflowRunLease) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.lockWorkflowRunLease(withWorkflowRunLease(ctx, lease), tx, lease.runID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE workflow_runs
			SET lease_expires_at = clock_timestamp() + make_interval(secs => $2)
			WHERE id = $1
		`, lease.runID, s.workflowRunLeaseTTL().Seconds())
		return err
	})
}

// releaseWorkflowRunLease gives the run up after the executor stops. A run
// that is still 'running' keeps its lease so that recovery takes it over
// when the lease expires; a paused run also drops its claim so it can resume.
func (s *Service) releaseWorkflowRunLease(ctx context.Context, lease workflowRunLease) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workflowRunClaimReleaseTimeout)
	defer cancel()
	if _, err := s.pool.Exec(releaseCtx, `
		UPDATE workflow_runs
		SET lease_owner = NULL, lease_expires_at = NULL, lease_mode = NULL,
		    claimed_at = CASE WHEN status = 'paused' THEN NULL ELSE claimed_at END,
		    updated_at = NOW()
		WHERE id = $1 AND lease_owner = $2 AND lease_epoch = $3 AND status <> 'running'
	`, lease.runID, lease.owner, lease.epoch); err != nil {
		log.Warn().Err(err).Str("workflow_run_id", lease.runID.String()).Msg("workflow: release run lease")
	}
}

// keepWorkflowRunLease renews the lease until stop is called. If the lease is
// lost, or cannot be renewed before it would have expired, the returned
// context is canceled so the executor makes no further side effects.
func (s *Service) keepWorkflowRunLease(ctx context.Context, lease workflowRunLease) (context.Context, func()) {
	execCtx, cancel := context.WithCancel(withWorkflowRunLease(ctx, lease))
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(s.workflowRunLeaseRenewEvery())
		defer ticker.Stop()
		lastRenewed := time.Now()
		for {
			select {
			case <-done:
				return
			case <-execCtx.Done():
				return
			case <-ticker.C:
				renewCtx, cancelRenew := context.WithTimeout(context.WithoutCancel(execCtx), s.workflowRunLeaseRenewEvery())
				err := s.renewWorkflowRunLease(renewCtx, lease)
				cancelRenew()
				switch {
				case err == nil:
					lastRenewed = time.Now()
				case errors.Is(err, errWorkflowRunLeaseLost):
					log.Warn().Str("workflow_run_id", lease.runID.String()).Int64("epoch", lease.epoch).Msg("workflow: run lease lost; stopping executor")
					cancel()
					return
				default:
					log.Warn().Err(err).Str("workflow_run_id", lease.runID.String()).Msg("workflow: run lease renewal failed")
					if time.Since(lastRenewed) >= s.workflowRunLeaseTTL() {
						cancel()
						return
					}
				}
			}
		}
	}()
	return execCtx, func() {
		close(done)
		<-stopped
		cancel()
	}
}

// RecoverExpiredWorkflowRunLeases handles runs whose lease expired:
//   - running, with recovery budget left: back to pending for a new claim;
//   - running, budget spent: failed in one transaction that also invalidates
//     unlaunched children and records stop requests for in-flight ones;
//   - paused: the claim is dropped so the run can resume;
//   - already finished: the stale lease is cleared.
//
// The recovery budget is max_attempts - 1, matching the old "at most
// max_attempts claims" when the run is never paused.
func (s *Service) RecoverExpiredWorkflowRunLeases(ctx context.Context) (int64, error) {
	var total int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, status, recovery_count, max_attempts
			FROM workflow_runs
			WHERE (lease_owner IS NOT NULL AND lease_expires_at < clock_timestamp())
			   -- A run created as running whose first lease was never taken
			   -- (the request died between creating and leasing it).
			   OR (status = 'running' AND lease_owner IS NULL AND lease_epoch = 0
			       AND updated_at < clock_timestamp() - make_interval(secs => $2))
			ORDER BY COALESCE(lease_expires_at, updated_at) ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		`, workflowRunLeaseRecoveryBatch, s.workflowRunLeaseTTL().Seconds())
		if err != nil {
			return err
		}
		type expired struct {
			id                     uuid.UUID
			status                 string
			recoveries, maxAttempt int32
		}
		items := []expired{}
		for rows.Next() {
			var item expired
			if err := rows.Scan(&item.id, &item.status, &item.recoveries, &item.maxAttempt); err != nil {
				rows.Close()
				return err
			}
			items = append(items, item)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, item := range items {
			// The row lock is held; re-check expiry under it with the
			// database clock before acting.
			var stillExpired bool
			if err := tx.QueryRow(ctx, `
				SELECT (lease_owner IS NOT NULL AND lease_expires_at < clock_timestamp())
				    OR (status = 'running' AND lease_owner IS NULL AND lease_epoch = 0)
				FROM workflow_runs WHERE id = $1
			`, item.id).Scan(&stillExpired); err != nil {
				return err
			}
			if !stillExpired {
				continue
			}
			if err := s.recoverExpiredWorkflowRun(ctx, tx, item.id, item.status, item.recoveries, item.maxAttempt); err != nil {
				return err
			}
			if err := notifyWorkflowRunChanged(ctx, tx, item.id); err != nil {
				return err
			}
			total++
		}
		return nil
	})
	return total, err
}

func (s *Service) recoverExpiredWorkflowRun(ctx context.Context, tx pgx.Tx, runID uuid.UUID, status string, recoveries, maxAttempts int32) error {
	switch {
	case status == workflowRunStatusRunning && recoveries < maxAttempts-1:
		_, err := tx.Exec(ctx, `
			UPDATE workflow_runs
			SET status = 'pending', next_retry_at = NOW(), claimed_at = NULL,
			    recovery_count = recovery_count + 1,
			    lease_owner = NULL, lease_expires_at = NULL, lease_mode = NULL,
			    last_worker_error = $2, updated_at = NOW()
			WHERE id = $1
		`, runID, workflowRunLeaseRecoveryLastError)
		return err
	case status == workflowRunStatusRunning:
		return s.finalizeExpiredWorkflowRun(ctx, tx, runID)
	case status == workflowRunStatusPaused:
		_, err := tx.Exec(ctx, `
			UPDATE workflow_runs
			SET claimed_at = NULL, lease_owner = NULL, lease_expires_at = NULL, lease_mode = NULL,
			    last_worker_error = $2, updated_at = NOW()
			WHERE id = $1
		`, runID, workflowRunLeaseRecoveryLastError)
		return err
	default:
		_, err := tx.Exec(ctx, `
			UPDATE workflow_runs
			SET lease_owner = NULL, lease_expires_at = NULL, lease_mode = NULL, updated_at = NOW()
			WHERE id = $1
		`, runID)
		return err
	}
}

// finalizeExpiredWorkflowRun fails a run whose recovery budget is spent, in
// the recovery transaction that holds the parent lock: the run becomes
// failed and cancelAttempts cancels every open attempt, invalidates claimed
// launches and records a stop request for every created child. The run is
// never failed without its children being asked to stop.
func (s *Service) finalizeExpiredWorkflowRun(ctx context.Context, tx pgx.Tx, runID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `
		UPDATE workflow_runs
		SET status = 'failed', finished_at = COALESCE(finished_at, NOW()),
		    error_message = COALESCE(error_message, $2),
		    claimed_at = NULL, next_retry_at = NULL,
		    lease_owner = NULL, lease_expires_at = NULL, lease_mode = NULL,
		    last_worker_error = $2, updated_at = NOW()
		WHERE id = $1
	`, runID, workflowRunLeaseRecoveryFailureMsg); err != nil {
		return err
	}
	return s.cancelAttempts(ctx, tx, runID, stopReasonWorkflowFailure, true)
}

// leaseNewWorkflowRun takes the first lease of a run the caller just created
// as 'running' and keeps it renewed. The returned finish stops renewal and
// releases the lease (a still-running run keeps it for recovery).
func (s *Service) leaseNewWorkflowRun(ctx context.Context, runID uuid.UUID) (context.Context, func(), error) {
	if s.pool == nil {
		return ctx, func() {}, nil
	}
	lease, err := s.acquireNewWorkflowRunLease(ctx, runID)
	if err != nil {
		log.Error().Err(err).Str("workflow_run_id", runID.String()).Msg("workflow: acquire new run lease")
		return nil, nil, httpx.Internal("获取 workflow_run 执行租约失败")
	}
	execCtx, stop := s.keepWorkflowRunLease(ctx, lease)
	return execCtx, func() {
		stop()
		s.releaseWorkflowRunLease(ctx, lease)
	}, nil
}
