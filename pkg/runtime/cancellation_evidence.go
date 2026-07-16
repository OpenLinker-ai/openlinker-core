package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type RunCancellationEvidence struct {
	CancellationID uuid.UUID
	State          string
	ErrorCode      string
	RequestedAt    time.Time
	FinishedAt     *time.Time
}

// GetRunCancellationEvidence is a read-only projection of physical stop
// evidence. A public Run status of canceled is deliberately not sufficient.
func (s *Service) GetRunCancellationEvidence(
	ctx context.Context,
	actorUserID, runID uuid.UUID,
) (RunCancellationEvidence, error) {
	if s == nil || s.pool == nil || actorUserID == uuid.Nil || runID == uuid.Nil {
		return RunCancellationEvidence{}, ErrRuntimeCancellationInvalid
	}
	var ownerID uuid.UUID
	var evidence RunCancellationEvidence
	var rawState string
	var stoppedAt *time.Time
	var errorCode *string
	err := s.pool.QueryRow(ctx, `
		SELECT r.user_id, c.id, c.state, c.error_code, c.requested_at, c.stopped_at
		FROM runs r
		JOIN run_cancellations c ON c.run_id = r.id
		WHERE r.id = $1
	`, runID).Scan(
		&ownerID, &evidence.CancellationID, &rawState, &errorCode,
		&evidence.RequestedAt, &stoppedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return RunCancellationEvidence{}, ErrRuntimeCancellationNotFound
	}
	if err != nil {
		return RunCancellationEvidence{}, err
	}
	if ownerID != actorUserID {
		return RunCancellationEvidence{}, ErrRuntimeCancellationNotFound
	}
	evidence.State, evidence.ErrorCode = normalizePhysicalCancellationEvidence(rawState, errorCode)
	if evidence.State == "stopped" {
		evidence.FinishedAt = stoppedAt
	}
	return evidence, nil
}

func normalizePhysicalCancellationEvidence(state string, errorCode *string) (string, string) {
	switch state {
	case "stopped":
		return "stopped", ""
	case "unconfirmed", "unsupported", "failed":
		return "unconfirmed", runtimeCancellationUnconfirmedCode
	case "requested", "delivered", "stopping":
		return "stopping", ""
	default:
		code := ""
		if errorCode != nil {
			code = *errorCode
		}
		return "unconfirmed", code
	}
}
