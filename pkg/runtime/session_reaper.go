package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	maxRuntimeSessionReapBatch            = 1000
	runtimeSessionDatabaseReconcilePeriod = time.Minute
)

var (
	ErrRuntimeSessionReaperNotConfigured = errors.New("runtime session reaper is not configured")
	errRuntimeSessionReapSkipped         = errors.New("runtime session reap skipped")
)

type runtimeSessionReaperRepository interface {
	WithTransaction(context.Context, func(runtimeSessionTransaction) error) error
	ListStaleRuntimeSessionCandidates(context.Context, time.Duration, int) ([]db.RuntimeSession, error)
	GetRuntimeSessionReapCandidate(context.Context, uuid.UUID) (db.RuntimeSession, time.Time, error)
}

type runtimeSessionLeaseEvidence interface {
	Lookup(context.Context, uuid.UUID) (RuntimeSessionLease, bool, error)
	ListExpired(context.Context, int) ([]uuid.UUID, error)
	ScheduleCheck(context.Context, uuid.UUID, time.Duration) error
	Forget(context.Context, uuid.UUID) error
	AbsenceReady() bool
}

// RuntimeSessionReaper closes transport Sessions whose database heartbeat has
// expired. It changes only Session/attachment state; offer and Attempt expiry
// remain owned by the lease/deadline reconciler.
type RuntimeSessionReaper struct {
	repository   runtimeSessionReaperRepository
	heartbeatTTL time.Duration
	leases       runtimeSessionLeaseEvidence

	databaseReconcileMu     sync.Mutex
	databaseReconcileActive bool
	nextDatabaseReconcileAt time.Time
}

func NewRuntimeSessionReaper(pool *pgxpool.Pool, heartbeatTTL time.Duration) *RuntimeSessionReaper {
	return NewRuntimeSessionReaperWithLeases(pool, heartbeatTTL, nil)
}

// NewRuntimeSessionReaperWithLeases keeps the database fencing transaction
// authoritative while allowing a healthy Redis lease to prove that a
// database-stale WebSocket is still connected. Redis errors and startup
// uncertainty can only delay a reap; they can never close a Session.
func NewRuntimeSessionReaperWithLeases(
	pool *pgxpool.Pool,
	heartbeatTTL time.Duration,
	leases *RuntimeSessionLeaseManager,
) *RuntimeSessionReaper {
	if pool == nil {
		return &RuntimeSessionReaper{heartbeatTTL: heartbeatTTL, leases: leases}
	}
	return &RuntimeSessionReaper{
		repository:              &postgresRuntimeSessionRepository{pool: pool, queries: db.New(pool)},
		heartbeatTTL:            heartbeatTTL,
		leases:                  leases,
		nextDatabaseReconcileAt: time.Now().Add(runtimeSessionDatabaseReconcilePeriod),
	}
}

func newRuntimeSessionReaper(repository runtimeSessionReaperRepository, heartbeatTTL time.Duration) *RuntimeSessionReaper {
	return &RuntimeSessionReaper{repository: repository, heartbeatTTL: heartbeatTTL}
}

func newRuntimeSessionReaperWithLeases(
	repository runtimeSessionReaperRepository,
	heartbeatTTL time.Duration,
	leases runtimeSessionLeaseEvidence,
) *RuntimeSessionReaper {
	return &RuntimeSessionReaper{
		repository: repository, heartbeatTTL: heartbeatTTL, leases: leases,
		nextDatabaseReconcileAt: time.Now().Add(runtimeSessionDatabaseReconcilePeriod),
	}
}

func (r *RuntimeSessionReaper) ReapStaleSessions(ctx context.Context, limit int) (int, error) {
	if r == nil || r.repository == nil || r.heartbeatTTL < time.Millisecond || limit <= 0 || limit > maxRuntimeSessionReapBatch {
		return 0, ErrRuntimeSessionReaperNotConfigured
	}
	if r.leases != nil {
		reaped, err := r.reapExpiredLeaseSessions(ctx, min(limit, maxRuntimeSessionLeaseBatch))
		if err != nil || !r.leases.AbsenceReady() || !r.beginDatabaseReconcile(time.Now()) {
			return reaped, err
		}
		reconciled, _, reconcileErr := r.reapStaleDatabaseSessions(ctx, limit)
		// Continue immediately only when the whole bounded batch was actually
		// closed. A full candidate page can consist entirely of live Redis
		// leases whose intentionally stale database heartbeat must not turn the
		// one-minute recovery scan back into a one-second poll.
		r.finishDatabaseReconcile(time.Now(), reconciled >= limit, reconcileErr)
		return reaped + reconciled, reconcileErr
	}
	reaped, _, err := r.reapStaleDatabaseSessions(ctx, limit)
	return reaped, err
}

func (r *RuntimeSessionReaper) reapStaleDatabaseSessions(ctx context.Context, limit int) (int, int, error) {
	candidates, err := r.repository.ListStaleRuntimeSessionCandidates(ctx, r.heartbeatTTL, limit)
	if err != nil {
		return 0, 0, err
	}

	reaped := 0
	var errs []error
	for _, candidate := range candidates {
		if err = ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		closed, reapErr := r.reapCandidate(ctx, candidate)
		if reapErr != nil {
			errs = append(errs, reapErr)
			continue
		}
		if closed {
			reaped++
		}
	}
	return reaped, len(candidates), errors.Join(errs...)
}

func (r *RuntimeSessionReaper) beginDatabaseReconcile(now time.Time) bool {
	r.databaseReconcileMu.Lock()
	defer r.databaseReconcileMu.Unlock()
	if r.databaseReconcileActive {
		return false
	}
	if !r.nextDatabaseReconcileAt.IsZero() && now.Before(r.nextDatabaseReconcileAt) {
		return false
	}
	r.databaseReconcileActive = true
	return true
}

func (r *RuntimeSessionReaper) finishDatabaseReconcile(now time.Time, full bool, err error) {
	r.databaseReconcileMu.Lock()
	defer r.databaseReconcileMu.Unlock()
	r.databaseReconcileActive = false
	switch {
	case err != nil:
		r.nextDatabaseReconcileAt = now.Add(time.Second)
	case full:
		r.nextDatabaseReconcileAt = time.Time{}
	default:
		r.nextDatabaseReconcileAt = now.Add(runtimeSessionDatabaseReconcilePeriod)
	}
}

func (r *RuntimeSessionReaper) reapExpiredLeaseSessions(ctx context.Context, limit int) (int, error) {
	expired, err := r.leases.ListExpired(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("list expired Runtime Session leases: %w", err)
	}
	reaped := 0
	var errs []error
	for _, runtimeSessionID := range expired {
		if err = ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		_, live, lookupErr := r.leases.Lookup(ctx, runtimeSessionID)
		if lookupErr != nil {
			errs = append(errs, fmt.Errorf("check Runtime Session lease %s: %w", runtimeSessionID, lookupErr))
			continue
		}
		if live || !r.leases.AbsenceReady() {
			continue
		}

		candidate, databaseNow, candidateErr := r.repository.GetRuntimeSessionReapCandidate(ctx, runtimeSessionID)
		if errors.Is(candidateErr, pgx.ErrNoRows) {
			if forgetErr := r.leases.Forget(ctx, runtimeSessionID); forgetErr != nil {
				errs = append(errs, forgetErr)
			}
			continue
		}
		if candidateErr != nil {
			errs = append(errs, candidateErr)
			continue
		}
		if candidate.Status != "active" && candidate.Status != "draining" {
			if forgetErr := r.leases.Forget(ctx, runtimeSessionID); forgetErr != nil {
				errs = append(errs, forgetErr)
			}
			continue
		}
		remaining := candidate.HeartbeatAt.Add(r.heartbeatTTL).Sub(databaseNow)
		if remaining >= 0 {
			if scheduleErr := r.leases.ScheduleCheck(ctx, runtimeSessionID, remaining+time.Millisecond); scheduleErr != nil {
				errs = append(errs, scheduleErr)
			}
			continue
		}

		closed, reapErr := r.reapCandidate(ctx, candidate)
		if reapErr != nil {
			errs = append(errs, reapErr)
			continue
		}
		if closed {
			reaped++
			if forgetErr := r.leases.Forget(ctx, runtimeSessionID); forgetErr != nil {
				errs = append(errs, forgetErr)
			}
		}
	}
	return reaped, errors.Join(errs...)
}

func (r *RuntimeSessionReaper) reapCandidate(ctx context.Context, candidate db.RuntimeSession) (bool, error) {
	if r.leases != nil {
		_, live, err := r.leases.Lookup(ctx, candidate.RuntimeSessionID)
		if err != nil {
			return false, fmt.Errorf("check Runtime Session lease %s: %w", candidate.RuntimeSessionID, err)
		}
		if live || !r.leases.AbsenceReady() {
			return false, nil
		}
	}
	closed := false
	err := r.repository.WithTransaction(ctx, func(tx runtimeSessionTransaction) error {
		if err := tx.LockSessionIdentity(ctx, candidate.RuntimeSessionID); err != nil {
			return err
		}
		if _, err := lockRuntimeSessionPrincipal(
			ctx,
			tx,
			candidate.NodeID,
			candidate.CredentialID,
			candidate.RuntimeSessionID,
		); err != nil {
			return err
		}

		current, err := tx.GetRuntimeSessionForUpdate(ctx, candidate.RuntimeSessionID)
		if err != nil {
			return err
		}
		if (current.Status != "active" && current.Status != "draining") ||
			!current.HeartbeatAt.Equal(candidate.HeartbeatAt) ||
			current.AttachedCoreInstanceID == nil || *current.AttachedCoreInstanceID == uuid.Nil {
			return errRuntimeSessionReapSkipped
		}
		coreInstanceID := *current.AttachedCoreInstanceID

		attachment, attachmentErr := tx.GetActiveRuntimeSessionAttachment(ctx, current.RuntimeSessionID)
		if attachmentErr != nil && !errors.Is(attachmentErr, pgx.ErrNoRows) {
			return attachmentErr
		}
		if attachmentErr == nil {
			if attachment.CoreInstanceID != coreInstanceID {
				return errRuntimeSessionReapSkipped
			}
			reason := "heartbeat timeout"
			if _, err = tx.CloseRuntimeSessionAttachment(ctx, db.CloseRuntimeSessionAttachmentParams{
				RuntimeSessionID: current.RuntimeSessionID,
				CoreInstanceID:   coreInstanceID,
				AttachmentID:     attachment.ID,
				DisconnectReason: &reason,
			}); err != nil {
				return err
			}
		}

		if _, err = tx.CloseStaleRuntimeSession(ctx, db.CloseStaleRuntimeSessionParams{
			RuntimeSessionID: current.RuntimeSessionID,
			HeartbeatAt:      current.HeartbeatAt,
			CoreInstanceID:   coreInstanceID,
		}); err != nil {
			return err
		}
		closed = true
		return nil
	})
	if errors.Is(err, errRuntimeSessionReapSkipped) || errors.Is(err, pgx.ErrNoRows) ||
		IsRuntimeSessionError(err, RuntimeSessionErrorPrincipalInactive) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reap Runtime Session %s: %w", candidate.RuntimeSessionID, err)
	}
	return closed, nil
}
