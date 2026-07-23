package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/eventwake"
)

const (
	runtimeCredentialReconcileInterval   = 20 * time.Second
	runtimeCredentialAuditInterval       = 5 * time.Minute
	runtimeCredentialAuditMaximum        = 10 * time.Minute
	runtimeCredentialQueryTimeout        = 5 * time.Second
	runtimeCredentialProjectionSettle    = 5 * time.Second
	runtimeCredentialReconcileBatch      = 1000
	RuntimeCredentialRevocationWakeTopic = "runtime.credential.revoke"
)

type RuntimeCredentialValidationResult struct {
	Registration RuntimeConnectionRegistration
	Valid        bool
}

type RuntimeCredentialConnectionValidator interface {
	Validate(
		context.Context,
		[]RuntimeConnectionRegistration,
	) ([]RuntimeCredentialValidationResult, error)
}

type postgresRuntimeCredentialConnectionValidator struct {
	pool           *pgxpool.Pool
	coreInstanceID uuid.UUID
}

func NewPostgresRuntimeCredentialConnectionValidator(
	pool *pgxpool.Pool,
	coreInstanceID uuid.UUID,
) RuntimeCredentialConnectionValidator {
	return &postgresRuntimeCredentialConnectionValidator{
		pool:           pool,
		coreInstanceID: coreInstanceID,
	}
}

type runtimeCredentialValidationRecord struct {
	Ordinal          int64     `json:"ordinal"`
	RuntimeSessionID uuid.UUID `json:"runtime_session_id"`
	SessionEpoch     int64     `json:"session_epoch"`
	AttachmentID     uuid.UUID `json:"attachment_id"`
	CredentialID     uuid.UUID `json:"credential_id"`
}

func (v *postgresRuntimeCredentialConnectionValidator) Validate(
	ctx context.Context,
	registrations []RuntimeConnectionRegistration,
) ([]RuntimeCredentialValidationResult, error) {
	if v == nil || v.pool == nil || v.coreInstanceID == uuid.Nil {
		return nil, errors.New("runtime credential validator is not configured")
	}
	if len(registrations) == 0 {
		return nil, nil
	}
	records := make([]runtimeCredentialValidationRecord, len(registrations))
	for index, registration := range registrations {
		if !validRuntimeConnectionIdentity(registration.Identity) ||
			registration.CredentialID == uuid.Nil {
			return nil, errors.New("runtime credential validation identity is invalid")
		}
		records[index] = runtimeCredentialValidationRecord{
			Ordinal:          int64(index + 1),
			RuntimeSessionID: registration.Identity.RuntimeSessionID,
			SessionEpoch:     registration.Identity.SessionEpoch,
			AttachmentID:     registration.Identity.AttachmentID,
			CredentialID:     registration.CredentialID,
		}
	}
	encoded, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("encode runtime credential validation batch: %w", err)
	}
	const statement = `
WITH requested AS (
    SELECT runtime_session_id,
           session_epoch,
           attachment_id,
           credential_id,
           ordinal
    FROM jsonb_to_recordset($1::jsonb) AS input(
        ordinal bigint,
        runtime_session_id uuid,
        session_epoch bigint,
        attachment_id uuid,
        credential_id uuid
    )
)
SELECT requested.ordinal,
       (
           session.runtime_session_id IS NOT NULL
           AND session.session_epoch = requested.session_epoch
           AND session.credential_id = requested.credential_id
           AND session.status IN ('active', 'draining')
           AND session.attached_core_instance_id = $2
           AND attachment.id IS NOT NULL
           AND attachment.core_instance_id = $2
           AND attachment.detached_at IS NULL
           AND token.id IS NOT NULL
           AND token.status = 'active_runtime'
           AND token.revoked_at IS NULL
           AND token.scopes @> ARRAY['agent:pull']::text[]
           AND (token.expires_at IS NULL OR token.expires_at > clock_timestamp())
           AND node.node_id IS NOT NULL
           AND node.status IN ('active', 'draining')
       ) AS valid
FROM requested
LEFT JOIN runtime_sessions session
  ON session.runtime_session_id = requested.runtime_session_id
LEFT JOIN runtime_session_attachments attachment
  ON attachment.id = requested.attachment_id
 AND attachment.runtime_session_id = requested.runtime_session_id
LEFT JOIN agent_tokens token
  ON token.id = requested.credential_id
 AND token.agent_id = session.agent_id
LEFT JOIN runtime_nodes node
  ON node.node_id = session.node_id
ORDER BY requested.ordinal`
	rows, err := v.pool.Query(ctx, statement, encoded, v.coreInstanceID)
	if err != nil {
		return nil, fmt.Errorf("validate runtime credential connections: %w", err)
	}
	defer rows.Close()
	results := make([]RuntimeCredentialValidationResult, 0, len(registrations))
	for rows.Next() {
		var ordinal int64
		var valid bool
		if err = rows.Scan(&ordinal, &valid); err != nil {
			return nil, err
		}
		if ordinal < 1 || ordinal > int64(len(registrations)) {
			return nil, errors.New("runtime credential validation ordinal is invalid")
		}
		results = append(results, RuntimeCredentialValidationResult{
			Registration: registrations[ordinal-1],
			Valid:        valid,
		})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(results) != len(registrations) {
		return nil, errors.New("runtime credential validation result count changed")
	}
	return results, nil
}

type RuntimeCredentialReconcilerConfig struct {
	Interval         time.Duration
	AuditInterval    time.Duration
	QueryTimeout     time.Duration
	ProjectionSettle time.Duration
	BatchSize        int
	Observer         WorkerObserver
	clock            runtimeCredentialClock
}

type RuntimeCredentialReconciler struct {
	hub        *RuntimeWakeHub
	projection RuntimeCredentialProjectionStore
	validator  RuntimeCredentialConnectionValidator
	config     RuntimeCredentialReconcilerConfig
}

type runtimeCredentialTicker interface {
	C() <-chan time.Time
	Stop()
}

type runtimeCredentialTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type runtimeCredentialClock interface {
	NewTicker(time.Duration) runtimeCredentialTicker
	NewTimer(time.Duration) runtimeCredentialTimer
}

type runtimeCredentialRealClock struct{}

type runtimeCredentialRealTicker struct{ ticker *time.Ticker }

func (runtimeCredentialRealClock) NewTicker(interval time.Duration) runtimeCredentialTicker {
	return runtimeCredentialRealTicker{ticker: time.NewTicker(interval)}
}

func (ticker runtimeCredentialRealTicker) C() <-chan time.Time { return ticker.ticker.C }
func (ticker runtimeCredentialRealTicker) Stop()               { ticker.ticker.Stop() }

type runtimeCredentialRealTimer struct{ timer *time.Timer }

func (runtimeCredentialRealClock) NewTimer(interval time.Duration) runtimeCredentialTimer {
	return runtimeCredentialRealTimer{timer: time.NewTimer(interval)}
}

func (timer runtimeCredentialRealTimer) C() <-chan time.Time { return timer.timer.C }
func (timer runtimeCredentialRealTimer) Stop() bool          { return timer.timer.Stop() }

func NewRuntimeCredentialReconciler(
	hub *RuntimeWakeHub,
	projection RuntimeCredentialProjectionStore,
	validator RuntimeCredentialConnectionValidator,
	config RuntimeCredentialReconcilerConfig,
) *RuntimeCredentialReconciler {
	return &RuntimeCredentialReconciler{
		hub:        hub,
		projection: projection,
		validator:  validator,
		config:     normalizeRuntimeCredentialReconcilerConfig(config),
	}
}

func normalizeRuntimeCredentialReconcilerConfig(
	config RuntimeCredentialReconcilerConfig,
) RuntimeCredentialReconcilerConfig {
	if config.Interval <= 0 || config.Interval > runtimeCredentialReconcileInterval {
		config.Interval = runtimeCredentialReconcileInterval
	}
	if config.AuditInterval < runtimeCredentialAuditInterval ||
		config.AuditInterval > runtimeCredentialAuditMaximum {
		config.AuditInterval = runtimeCredentialAuditInterval
	}
	if config.QueryTimeout <= 0 || config.QueryTimeout > runtimeCredentialQueryTimeout {
		config.QueryTimeout = runtimeCredentialQueryTimeout
	}
	if config.ProjectionSettle <= 0 ||
		config.ProjectionSettle > runtimeCredentialProjectionSettle {
		config.ProjectionSettle = runtimeCredentialProjectionSettle
	}
	if config.BatchSize <= 0 || config.BatchSize > runtimeCredentialReconcileBatch {
		config.BatchSize = runtimeCredentialReconcileBatch
	}
	if config.clock == nil {
		config.clock = runtimeCredentialRealClock{}
	}
	return config
}

func (r *RuntimeCredentialReconciler) Run(ctx context.Context) error {
	if r == nil || r.hub == nil || r.validator == nil {
		return errors.New("runtime credential reconciler is not configured")
	}
	if err := r.reconcile(ctx, false, "startup"); err != nil && ctx.Err() == nil {
		log.Warn().Err(err).Msg("runtime credential startup reconciliation failed")
	}
	reconcileTicker := r.config.clock.NewTicker(r.config.Interval)
	auditTicker := r.config.clock.NewTicker(r.config.AuditInterval)
	defer reconcileTicker.Stop()
	defer auditTicker.Stop()
	var fallbackTimer runtimeCredentialTimer
	var fallback <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			if fallbackTimer != nil {
				fallbackTimer.Stop()
			}
			return nil
		case <-r.hub.CredentialRevalidationWake():
			if fallbackTimer == nil {
				fallbackTimer = r.config.clock.NewTimer(r.config.ProjectionSettle)
				fallback = fallbackTimer.C()
			}
		case <-fallback:
			fallbackTimer = nil
			fallback = nil
			if err := r.reconcile(ctx, true, "projection_uncertain"); err != nil && ctx.Err() == nil {
				log.Warn().Err(err).Msg("runtime credential fallback reconciliation failed")
			}
		case <-reconcileTicker.C():
			if err := r.reconcile(ctx, false, "projection"); err != nil && ctx.Err() == nil {
				log.Warn().Err(err).Msg("runtime credential projection reconciliation failed")
			}
		case <-auditTicker.C():
			if err := r.reconcile(ctx, true, "audit"); err != nil && ctx.Err() == nil {
				log.Warn().Err(err).Msg("runtime credential audit reconciliation failed")
			}
		}
	}
}

// StartRuntimeCredentialRevocationWake turns the credential-specific
// transactional NOTIFY into a bounded database-fallback hint. The notification
// carries no credential material and never makes an authorization decision.
func StartRuntimeCredentialRevocationWake(
	ctx context.Context,
	source eventwake.TopicSource,
	hub *RuntimeWakeHub,
) {
	if source == nil || hub == nil {
		return
	}
	for ctx.Err() == nil {
		subscription, err := source.SubscribeTopic(RuntimeCredentialRevocationWakeTopic)
		if err != nil {
			hub.RequireCredentialRevalidation()
			if !waitWorkerFallbackInterval(ctx, time.Second) {
				return
			}
			continue
		}
		for ctx.Err() == nil {
			waitCtx, cancel := context.WithTimeout(ctx, time.Second)
			_, waitErr := subscription.Wait(waitCtx)
			cancel()
			if waitErr == nil {
				hub.RequireCredentialRevalidation()
				continue
			}
			if errors.Is(waitErr, context.DeadlineExceeded) {
				if !eventWakeSourceHealthy(source) {
					hub.RequireCredentialRevalidation()
				}
				continue
			}
			break
		}
		subscription.Close()
		hub.RequireCredentialRevalidation()
	}
}

func (r *RuntimeCredentialReconciler) reconcile(
	ctx context.Context,
	forceDatabase bool,
	reason string,
) error {
	registrations := r.hub.ConnectionSnapshot()
	if len(registrations) == 0 {
		return nil
	}
	for offset := 0; offset < len(registrations); offset += r.config.BatchSize {
		end := offset + r.config.BatchSize
		if end > len(registrations) {
			end = len(registrations)
		}
		if err := r.reconcileBatch(ctx, registrations[offset:end], forceDatabase, reason); err != nil {
			return err
		}
	}
	return nil
}

func (r *RuntimeCredentialReconciler) reconcileBatch(
	ctx context.Context,
	registrations []RuntimeConnectionRegistration,
	forceDatabase bool,
	reason string,
) error {
	databaseCandidates := registrations
	if !forceDatabase && r.projection != nil {
		projectionCtx, projectionCancel := context.WithTimeout(ctx, r.config.QueryTimeout)
		results, err := r.projection.Check(projectionCtx, registrations)
		projectionCancel()
		if err == nil {
			databaseCandidates = make([]RuntimeConnectionRegistration, 0)
			for _, result := range results {
				switch result.State {
				case RuntimeCredentialProjectionActive:
				case RuntimeCredentialProjectionRevoked:
					r.hub.RevokeCredentialConnections(
						result.Registration.CredentialID,
						[]RuntimeConnectionIdentity{result.Registration.Identity},
					)
				default:
					databaseCandidates = append(databaseCandidates, result.Registration)
				}
			}
			if len(databaseCandidates) == 0 {
				observeWorker(
					r.config.Observer,
					"runtime.credential_projection.batch",
					reason,
					len(registrations),
				)
				return nil
			}
		}
	}
	queryCtx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()
	validated, err := r.validator.Validate(queryCtx, databaseCandidates)
	if err != nil {
		return err
	}
	active := make([]RuntimeConnectionRegistration, 0, len(validated))
	for _, result := range validated {
		if result.Valid {
			active = append(active, result.Registration)
			continue
		}
		r.hub.RevokeCredentialConnections(
			result.Registration.CredentialID,
			[]RuntimeConnectionIdentity{result.Registration.Identity},
		)
	}
	observeWorker(
		r.config.Observer,
		"runtime.credential_database.batch",
		reason,
		len(databaseCandidates),
	)
	if r.projection != nil && len(active) > 0 {
		projectionCtx, projectionCancel := context.WithTimeout(ctx, r.config.QueryTimeout)
		err = r.projection.MarkActive(projectionCtx, active)
		projectionCancel()
		if err != nil {
			return err
		}
	}
	return nil
}
