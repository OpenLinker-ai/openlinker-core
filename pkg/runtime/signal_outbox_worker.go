package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

const (
	defaultRuntimeSignalWorkerInterval       = 250 * time.Millisecond
	defaultRuntimeSignalWorkerLeaseDuration  = 5 * time.Second
	defaultRuntimeSignalWorkerBatchSize      = int32(64)
	defaultRuntimeSignalWorkerCatchUpBatches = 4
	defaultRuntimeSignalRetryBase            = 250 * time.Millisecond
	defaultRuntimeSignalRetryMaximum         = 30 * time.Second
	runtimeSignalWorkerWakeTopic             = "work.runtime_signal.available"
)

type RuntimeSignalOutboxWorkerConfig struct {
	Interval          time.Duration
	LeaseDuration     time.Duration
	BatchSize         int32
	MaxCatchUpBatches int
	RetryBase         time.Duration
	RetryMaximum      time.Duration
	Observer          WorkerObserver
}

type RuntimeSignalOutboxBatchResult struct {
	Claimed   int
	Published int
	Retried   int
}

type runtimeSignalOutboxStore interface {
	ClaimRuntimeSignals(context.Context, db.ClaimRuntimeSignalsParams) ([]db.RuntimeSignalOutbox, error)
	MarkRuntimeSignalPublished(context.Context, db.MarkRuntimeSignalPublishedParams) (db.RuntimeSignalOutbox, error)
	RetryRuntimeSignal(context.Context, db.RetryRuntimeSignalParams) (db.RuntimeSignalOutbox, error)
	CountPendingRuntimeSignals(context.Context) (int32, error)
}

type runtimeSignalNextDueStore interface {
	NextRuntimeSignalDue(context.Context) (db.NextRuntimeSignalDueRow, error)
}

type RuntimeSignalOutboxWorker struct {
	queries runtimeSignalOutboxStore
	bus     RuntimeSignalBus
}

func NewRuntimeSignalOutboxWorker(queries runtimeSignalOutboxStore, bus RuntimeSignalBus) *RuntimeSignalOutboxWorker {
	return &RuntimeSignalOutboxWorker{queries: queries, bus: bus}
}

func normalizeRuntimeSignalOutboxWorkerConfig(cfg RuntimeSignalOutboxWorkerConfig) RuntimeSignalOutboxWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultRuntimeSignalWorkerInterval
	}
	if cfg.LeaseDuration < time.Second || cfg.LeaseDuration > 30*time.Second {
		cfg.LeaseDuration = defaultRuntimeSignalWorkerLeaseDuration
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > 256 {
		cfg.BatchSize = defaultRuntimeSignalWorkerBatchSize
	}
	if cfg.MaxCatchUpBatches <= 0 || cfg.MaxCatchUpBatches > 32 {
		cfg.MaxCatchUpBatches = defaultRuntimeSignalWorkerCatchUpBatches
	}
	if cfg.RetryBase <= 0 || cfg.RetryBase > time.Minute {
		cfg.RetryBase = defaultRuntimeSignalRetryBase
	}
	if cfg.RetryMaximum < cfg.RetryBase || cfg.RetryMaximum > 5*time.Minute {
		cfg.RetryMaximum = defaultRuntimeSignalRetryMaximum
	}
	return cfg
}

// ProcessOnce claims one bounded batch. The lease owner changes for every
// batch so a late mark cannot complete a signal reacquired by another Core.
// A crash after Publish and before Mark intentionally causes a duplicate
// signal after lease expiry; consumers must return to PostgreSQL to dedupe.
func (w *RuntimeSignalOutboxWorker) ProcessOnce(
	ctx context.Context,
	cfg RuntimeSignalOutboxWorkerConfig,
) (RuntimeSignalOutboxBatchResult, error) {
	var result RuntimeSignalOutboxBatchResult
	if w == nil || w.queries == nil || w.bus == nil {
		return result, errors.New("runtime signal outbox worker is not configured")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	cfg = normalizeRuntimeSignalOutboxWorkerConfig(cfg)
	leaseOwner := uuid.New()
	signals, err := w.queries.ClaimRuntimeSignals(ctx, db.ClaimRuntimeSignalsParams{
		LeaseOwner:      leaseOwner,
		LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
		Limit:           cfg.BatchSize,
	})
	if err != nil {
		return result, fmt.Errorf("claim runtime signals: %w", err)
	}
	result.Claimed = len(signals)

	var combined error
	for _, claimed := range signals {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(combined, err)
		}
		published, retryScheduled, processErr := w.processClaimed(ctx, cfg, leaseOwner, claimed)
		if published {
			result.Published++
		}
		if retryScheduled {
			result.Retried++
		}
		combined = errors.Join(combined, processErr)
	}
	return result, combined
}

func (w *RuntimeSignalOutboxWorker) processClaimed(
	ctx context.Context,
	cfg RuntimeSignalOutboxWorkerConfig,
	leaseOwner uuid.UUID,
	claimed db.RuntimeSignalOutbox,
) (published bool, retryScheduled bool, err error) {
	if claimed.Status != "processing" || claimed.LeaseOwner == nil ||
		*claimed.LeaseOwner != leaseOwner || claimed.AttemptCount <= 0 {
		return false, false, errors.New("claimed runtime signal has invalid lease state")
	}
	signal, signalErr := runtimeSignalFromOutbox(claimed)
	if signalErr == nil {
		signalErr = w.bus.Publish(ctx, signal)
	}
	if signalErr != nil {
		_, retryErr := w.queries.RetryRuntimeSignal(ctx, db.RetryRuntimeSignalParams{
			ID:           claimed.ID,
			LeaseOwner:   leaseOwner,
			RetryAfterMs: runtimeSignalRetryDelay(cfg, claimed.AttemptCount).Milliseconds(),
			LastError:    runtimeSignalPersistedError(signalErr),
		})
		if errors.Is(retryErr, pgx.ErrNoRows) {
			// The short lease expired or another owner recovered it. That owner
			// is now solely responsible for the next publish/mark transition.
			return false, false, nil
		}
		if retryErr != nil {
			return false, false, fmt.Errorf("schedule runtime signal retry: %w", retryErr)
		}
		return false, true, nil
	}

	_, markErr := w.queries.MarkRuntimeSignalPublished(ctx, db.MarkRuntimeSignalPublishedParams{
		ID: claimed.ID, LeaseOwner: leaseOwner,
	})
	if errors.Is(markErr, pgx.ErrNoRows) {
		// Publish already happened. Leaving the row recoverable is the
		// intended at-least-once boundary, not a reason to republish inline.
		return false, false, nil
	}
	if markErr != nil {
		return false, false, fmt.Errorf("mark runtime signal published: %w", markErr)
	}
	return true, false, nil
}

func (w *RuntimeSignalOutboxWorker) Backlog(ctx context.Context) (int32, error) {
	if w == nil || w.queries == nil {
		return 0, errors.New("runtime signal outbox worker is not configured")
	}
	return w.queries.CountPendingRuntimeSignals(ctx)
}

func StartRuntimeSignalOutboxWorker(
	ctx context.Context,
	worker *RuntimeSignalOutboxWorker,
	cfg RuntimeSignalOutboxWorkerConfig,
) {
	cfg = normalizeRuntimeSignalOutboxWorkerConfig(cfg)
	run := func(reason string) {
		if err := runtimeSignalOutboxPass(ctx, worker, cfg, reason); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("runtime signal outbox pass failed")
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

// StartRuntimeSignalOutboxWorkerWithWake keeps the legacy polling entry point
// intact and cuts over only while the advisory LISTEN source is healthy. A
// disconnect immediately returns to the legacy interval; durable claims and
// database-clock retry timestamps remain authoritative in both modes.
func StartRuntimeSignalOutboxWorkerWithWake(
	ctx context.Context,
	worker *RuntimeSignalOutboxWorker,
	cfg RuntimeSignalOutboxWorkerConfig,
	source eventwake.TopicSource,
) {
	cfg = normalizeRuntimeSignalOutboxWorkerConfig(cfg)
	if source == nil || worker == nil {
		StartRuntimeSignalOutboxWorker(ctx, worker, cfg)
		return
	}
	if _, ok := worker.queries.(runtimeSignalNextDueStore); !ok {
		StartRuntimeSignalOutboxWorker(ctx, worker, cfg)
		return
	}

	for ctx.Err() == nil {
		if !eventWakeSourceHealthy(source) {
			if err := runtimeSignalOutboxPass(ctx, worker, cfg, "degraded_poll"); err != nil && ctx.Err() == nil {
				log.Error().Err(err).Msg("runtime signal outbox degraded pass failed")
			}
			if !waitWorkerFallbackInterval(ctx, cfg.Interval) {
				return
			}
			continue
		}

		subscription, err := source.SubscribeTopic(runtimeSignalWorkerWakeTopic)
		if err != nil {
			if passErr := runtimeSignalOutboxPass(ctx, worker, cfg, "degraded_poll"); passErr != nil && ctx.Err() == nil {
				log.Error().Err(passErr).Msg("runtime signal outbox degraded pass failed")
			}
			if !waitWorkerFallbackInterval(ctx, cfg.Interval) {
				return
			}
			continue
		}
		if !eventWakeSourceHealthy(source) {
			subscription.Close()
			continue
		}
		err = eventwake.RunScheduler(ctx, subscription, eventwake.SchedulerConfig{
			ReconcileInterval: time.Minute,
			ErrorRetry:        cfg.Interval,
			HealthCheck:       cfg.Interval,
			Healthy:           func() bool { return eventWakeSourceHealthy(source) },
		}, func(runCtx context.Context, reason string) (eventwake.SchedulerResult, error) {
			if passErr := runtimeSignalOutboxPass(runCtx, worker, cfg, reason); passErr != nil {
				return eventwake.SchedulerResult{}, passErr
			}
			return worker.nextDueSchedule(runCtx)
		})
		subscription.Close()
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, eventwake.ErrWakeSourceDegraded) {
			log.Error().Err(err).Msg("runtime signal event scheduler stopped; polling fallback enabled")
		}
	}
}

func runtimeSignalOutboxPass(
	ctx context.Context,
	worker *RuntimeSignalOutboxWorker,
	cfg RuntimeSignalOutboxWorkerConfig,
	reason string,
) error {
	for batch := 0; batch < cfg.MaxCatchUpBatches; batch++ {
		observeWorker(cfg.Observer, "runtime.signal_outbox.claim", reason, int(cfg.BatchSize))
		result, err := worker.ProcessOnce(ctx, cfg)
		if err != nil {
			return err
		}
		if result.Published > 0 || result.Retried > 0 {
			log.Debug().
				Int("published", result.Published).
				Int("retried", result.Retried).
				Msg("runtime signal outbox pass completed")
		}
		if result.Claimed < int(cfg.BatchSize) {
			return nil
		}
	}
	return nil
}

func (w *RuntimeSignalOutboxWorker) nextDueSchedule(
	ctx context.Context,
) (eventwake.SchedulerResult, error) {
	store, ok := w.queries.(runtimeSignalNextDueStore)
	if !ok {
		return eventwake.SchedulerResult{}, errors.New("runtime signal next-due query is not configured")
	}
	next, err := store.NextRuntimeSignalDue(ctx)
	if err != nil {
		return eventwake.SchedulerResult{}, fmt.Errorf("read next runtime signal due time: %w", err)
	}
	return databaseDueSchedule(next.NextDueAt, next.DatabaseNow), nil
}

func runtimeSignalFromOutbox(outbox db.RuntimeSignalOutbox) (RuntimeSignal, error) {
	signal := RuntimeSignal{
		SignalID: outbox.ID,
		Type:     outbox.EventType,
		AgentID:  outbox.AgentID,
		RunID:    outbox.RunID,
	}
	if len(bytes.TrimSpace(outbox.Payload)) > 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(outbox.Payload, &fields); err != nil {
			return RuntimeSignal{}, fmt.Errorf("%w: invalid outbox payload", ErrRuntimeSignalInvalid)
		}
		if encodedTarget, ok := fields["target_instance_id"]; ok && !bytes.Equal(bytes.TrimSpace(encodedTarget), []byte("null")) {
			var target uuid.UUID
			if err := json.Unmarshal(encodedTarget, &target); err != nil || target == uuid.Nil {
				return RuntimeSignal{}, fmt.Errorf("%w: invalid target_instance_id", ErrRuntimeSignalInvalid)
			}
			signal.TargetInstanceID = &target
		}
		if encodedNode, ok := fields["node_id"]; ok && !bytes.Equal(bytes.TrimSpace(encodedNode), []byte("null")) {
			var nodeID uuid.UUID
			if err := json.Unmarshal(encodedNode, &nodeID); err != nil || nodeID == uuid.Nil {
				return RuntimeSignal{}, fmt.Errorf("%w: invalid node_id", ErrRuntimeSignalInvalid)
			}
			signal.NodeID = &nodeID
		}
		if encodedCredential, ok := fields["credential_id"]; ok &&
			!bytes.Equal(bytes.TrimSpace(encodedCredential), []byte("null")) {
			var credentialID uuid.UUID
			if err := json.Unmarshal(encodedCredential, &credentialID); err != nil ||
				credentialID == uuid.Nil {
				return RuntimeSignal{}, fmt.Errorf("%w: invalid credential_id", ErrRuntimeSignalInvalid)
			}
			signal.CredentialID = &credentialID
		}
		if encodedConnections, ok := fields["connections"]; ok &&
			!bytes.Equal(bytes.TrimSpace(encodedConnections), []byte("null")) {
			if err := json.Unmarshal(encodedConnections, &signal.Connections); err != nil {
				return RuntimeSignal{}, fmt.Errorf("%w: invalid connections", ErrRuntimeSignalInvalid)
			}
		}
	}
	if err := ValidateRuntimeSignal(signal); err != nil {
		return RuntimeSignal{}, err
	}
	return signal, nil
}

func runtimeSignalPersistedError(err error) string {
	if errors.Is(err, ErrRuntimeSignalInvalid) {
		return "SIGNAL_INVALID"
	}
	return "SIGNAL_PUBLISH_FAILED"
}

func runtimeSignalRetryDelay(cfg RuntimeSignalOutboxWorkerConfig, attemptCount int32) time.Duration {
	delay := cfg.RetryBase
	for attempt := int32(1); attempt < attemptCount && delay < cfg.RetryMaximum; attempt++ {
		if delay > cfg.RetryMaximum/2 {
			return cfg.RetryMaximum
		}
		delay *= 2
	}
	if delay > cfg.RetryMaximum {
		return cfg.RetryMaximum
	}
	return delay
}
