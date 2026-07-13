package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const maxRuntimeSessionReapBatch = 1000

var (
	ErrRuntimeSessionReaperNotConfigured = errors.New("runtime session reaper is not configured")
	errRuntimeSessionReapSkipped         = errors.New("runtime session reap skipped")
)

type runtimeSessionReaperRepository interface {
	WithTransaction(context.Context, func(runtimeSessionTransaction) error) error
	ListStaleRuntimeSessionCandidates(context.Context, time.Duration, int) ([]db.RuntimeSession, error)
}

// RuntimeSessionReaper closes transport Sessions whose database heartbeat has
// expired. It changes only Session/attachment state; offer and Attempt expiry
// remain owned by the lease/deadline reconciler.
type RuntimeSessionReaper struct {
	repository   runtimeSessionReaperRepository
	heartbeatTTL time.Duration
}

func NewRuntimeSessionReaper(pool *pgxpool.Pool, heartbeatTTL time.Duration) *RuntimeSessionReaper {
	if pool == nil {
		return &RuntimeSessionReaper{heartbeatTTL: heartbeatTTL}
	}
	return &RuntimeSessionReaper{
		repository:   &postgresRuntimeSessionRepository{pool: pool, queries: db.New(pool)},
		heartbeatTTL: heartbeatTTL,
	}
}

func newRuntimeSessionReaper(repository runtimeSessionReaperRepository, heartbeatTTL time.Duration) *RuntimeSessionReaper {
	return &RuntimeSessionReaper{repository: repository, heartbeatTTL: heartbeatTTL}
}

func (r *RuntimeSessionReaper) ReapStaleSessions(ctx context.Context, limit int) (int, error) {
	if r == nil || r.repository == nil || r.heartbeatTTL < time.Millisecond || limit <= 0 || limit > maxRuntimeSessionReapBatch {
		return 0, ErrRuntimeSessionReaperNotConfigured
	}
	candidates, err := r.repository.ListStaleRuntimeSessionCandidates(ctx, r.heartbeatTTL, limit)
	if err != nil {
		return 0, err
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
	return reaped, errors.Join(errs...)
}

func (r *RuntimeSessionReaper) reapCandidate(ctx context.Context, candidate db.RuntimeSession) (bool, error) {
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
