package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

const (
	defaultRuntimeDispatchWakeReconcileInterval = time.Minute
	defaultRuntimeDispatchWakeReconcileBatch    = 512
	maxRuntimeDispatchWakeReconcileBatch        = 1000
)

var ErrRuntimeDispatchWakeReconcilerNotConfigured = errors.New("Runtime dispatch wake reconciler is not configured")

type RuntimeDispatchWakeReconcilerConfig struct {
	Interval  time.Duration
	BatchSize int
	Observer  WorkerObserver
}

type RuntimeDispatchWakeReconcileResult struct {
	Scanned int  `json:"scanned"`
	Woken   int  `json:"woken"`
	Wrapped bool `json:"wrapped"`
}

type runtimeDispatchWakeRepository interface {
	ListClaimableRuntimeAgentIDs(context.Context, *uuid.UUID, int) ([]uuid.UUID, error)
}

// RuntimeDispatchWakeReconciler is the durable safety net for lossy wake
// hints. It performs one process-level, bounded PostgreSQL discovery pass and
// wakes only Agents with claimable Runtime work and a registered local waiter.
// WebSocket and Pull connections remain free of periodic database polling.
type RuntimeDispatchWakeReconciler struct {
	repository runtimeDispatchWakeRepository
	hub        *RuntimeWakeHub

	mu     sync.Mutex
	cursor *uuid.UUID
}

func NewRuntimeDispatchWakeReconciler(pool *pgxpool.Pool, hub *RuntimeWakeHub) *RuntimeDispatchWakeReconciler {
	if pool == nil {
		return &RuntimeDispatchWakeReconciler{hub: hub}
	}
	return &RuntimeDispatchWakeReconciler{
		repository: &postgresRuntimeDispatchWakeRepository{pool: pool},
		hub:        hub,
	}
}

func newRuntimeDispatchWakeReconciler(
	repository runtimeDispatchWakeRepository,
	hub *RuntimeWakeHub,
) *RuntimeDispatchWakeReconciler {
	return &RuntimeDispatchWakeReconciler{repository: repository, hub: hub}
}

func normalizeRuntimeDispatchWakeReconcilerConfig(
	cfg RuntimeDispatchWakeReconcilerConfig,
) RuntimeDispatchWakeReconcilerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRuntimeDispatchWakeReconcileInterval
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > maxRuntimeDispatchWakeReconcileBatch {
		cfg.BatchSize = defaultRuntimeDispatchWakeReconcileBatch
	}
	return cfg
}

// ReconcileOnce advances a process-local UUID cursor so a large offline
// backlog cannot permanently starve later Agent IDs. A cursor wrap may issue a
// second bounded query; the normal empty/partial-page path issues exactly one.
func (r *RuntimeDispatchWakeReconciler) ReconcileOnce(
	ctx context.Context,
	limit int,
) (RuntimeDispatchWakeReconcileResult, error) {
	var result RuntimeDispatchWakeReconcileResult
	if r == nil || r.repository == nil || r.hub == nil || limit < 1 || limit > maxRuntimeDispatchWakeReconcileBatch {
		return result, ErrRuntimeDispatchWakeReconcilerNotConfigured
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	agentIDs, err := r.repository.ListClaimableRuntimeAgentIDs(ctx, r.cursor, limit)
	if err != nil {
		return result, fmt.Errorf("list claimable Runtime Agent IDs: %w", err)
	}
	if len(agentIDs) == 0 && r.cursor != nil {
		r.cursor = nil
		result.Wrapped = true
		agentIDs, err = r.repository.ListClaimableRuntimeAgentIDs(ctx, nil, limit)
		if err != nil {
			return result, fmt.Errorf("list claimable Runtime Agent IDs after cursor wrap: %w", err)
		}
	}
	result.Scanned = len(agentIDs)
	for _, agentID := range agentIDs {
		if r.hub.WakeDispatchIfRegistered(agentID) {
			result.Woken++
		}
	}
	if len(agentIDs) == limit {
		cursor := agentIDs[len(agentIDs)-1]
		r.cursor = &cursor
	} else {
		r.cursor = nil
	}
	return result, nil
}

func StartRuntimeDispatchWakeReconciler(
	ctx context.Context,
	reconciler *RuntimeDispatchWakeReconciler,
	cfg RuntimeDispatchWakeReconcilerConfig,
) {
	cfg = normalizeRuntimeDispatchWakeReconcilerConfig(cfg)
	run := func(reason string) {
		observeWorker(cfg.Observer, "runtime.dispatch_wake.reconcile", reason, cfg.BatchSize)
		result, err := reconciler.ReconcileOnce(ctx, cfg.BatchSize)
		if err != nil {
			if ctx.Err() == nil {
				log.Error().Err(err).Msg("Runtime dispatch wake reconciliation failed")
			}
			return
		}
		if result.Woken > 0 {
			log.Warn().Int("scanned", result.Scanned).Int("woken", result.Woken).
				Bool("cursor_wrapped", result.Wrapped).
				Msg("Runtime dispatch wake reconciliation recovered pending work")
		} else if result.Scanned > 0 {
			log.Debug().Int("scanned", result.Scanned).Bool("cursor_wrapped", result.Wrapped).
				Msg("Runtime dispatch wake reconciliation found no local waiter")
		}
	}

	run("startup")
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run("ticker")
		}
	}
}

type postgresRuntimeDispatchWakeRepository struct {
	pool *pgxpool.Pool
}

const listClaimableRuntimeAgentIDsSQL = `
WITH database_clock AS MATERIALIZED (
    SELECT clock_timestamp() AS database_now
)
SELECT r.agent_id
FROM runs r
CROSS JOIN database_clock c
WHERE ($1::uuid IS NULL OR r.agent_id > $1::uuid)
  AND r.status = 'running'
  AND r.runtime_contract_id = 'openlinker.runtime.v2'
  AND r.connection_mode_snapshot = 'runtime'
  AND (
      r.dispatch_state = 'pending'
      OR (r.dispatch_state = 'retry_wait' AND r.next_attempt_at <= c.database_now)
  )
  AND r.active_attempt_id IS NULL
  AND r.offer_count < r.max_offer_count
  AND r.attempt_count < r.max_attempts
  AND r.dispatch_deadline_at > c.database_now
  AND r.run_deadline_at > c.database_now
GROUP BY r.agent_id
ORDER BY r.agent_id
LIMIT $2`

func (r *postgresRuntimeDispatchWakeRepository) ListClaimableRuntimeAgentIDs(
	ctx context.Context,
	after *uuid.UUID,
	limit int,
) ([]uuid.UUID, error) {
	if r == nil || r.pool == nil {
		return nil, ErrRuntimeDispatchWakeReconcilerNotConfigured
	}
	rows, err := r.pool.Query(ctx, listClaimableRuntimeAgentIDsSQL, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	agentIDs := make([]uuid.UUID, 0, limit)
	for rows.Next() {
		var agentID uuid.UUID
		if err = rows.Scan(&agentID); err != nil {
			return nil, err
		}
		agentIDs = append(agentIDs, agentID)
	}
	return agentIDs, rows.Err()
}
