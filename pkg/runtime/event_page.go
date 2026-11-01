package runtime

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// ListRunEventsPage returns the readable event window and enough retention
// state for clients to distinguish an empty page from a complete history.
func (s *Service) ListRunEventsPage(
	ctx context.Context,
	userID, runID uuid.UUID,
	afterSequence, limit int32,
) (*RunEventPageResponse, error) {
	if afterSequence < 0 {
		return nil, httpx.BadRequest("after_sequence 不能小于 0")
	}
	if limit <= 0 {
		limit = defaultRunEventsLimit
	}
	if limit > maxRunEventsLimit {
		limit = maxRunEventsLimit
	}
	if s.pool == nil {
		return nil, httpx.Internal("事件分页存储未初始化")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunEventsPage: BeginTx")
		return nil, httpx.Internal("查询调用事件失败")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := s.queries.WithTx(tx)

	// Ownership and retention state share one repeatable-read snapshot. user_id
	// is immutable, but keeping the check here also makes the read contract
	// explicit if run lifecycle fields advance while the page is assembled.
	run, err := queries.GetRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunEventsPage: GetRunByID")
		return nil, httpx.Internal("查询调用记录失败")
	}
	if run.UserID != userID {
		return nil, httpx.NotFound("调用记录不存在")
	}

	bounds, err := queries.GetRunEventBounds(ctx, runID)
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunEventsPage: retention state")
		return nil, httpx.Internal("查询调用事件失败")
	}
	retainedThrough := bounds.RetainedThroughSequence
	earliestAvailable := bounds.FirstAvailableSequence
	var latestAvailable *int32
	if earliestAvailable != nil {
		latest := bounds.LastSequence
		latestAvailable = &latest
	}

	effectiveAfter := afterSequence
	if retainedThrough > effectiveAfter {
		effectiveAfter = retainedThrough
	}
	events, err := queries.ListRunEventsByRun(ctx, db.ListRunEventsByRunParams{
		RunID:         runID,
		AfterSequence: effectiveAfter,
		Limit:         limit,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunEventsPage: ListRunEventsByRun")
		return nil, httpx.Internal("查询调用事件失败")
	}
	projection, err := s.runOutputProjection(ctx, queries, userID, run.AgentID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.ListRunEventsPage: Commit")
		return nil, httpx.Internal("查询调用事件失败")
	}

	items := make([]RunEventResponse, 0, len(events))
	for _, event := range events {
		items = append(items, projectRunEvent(projection, runEventToResponse(event)))
	}
	terminal := isTerminalRunStatus(run.Status)
	streamComplete := false
	if terminal {
		streamComplete = latestAvailable == nil
		if len(items) > 0 && latestAvailable != nil {
			streamComplete = items[len(items)-1].Sequence >= *latestAvailable
		}
		if len(items) == 0 && latestAvailable != nil {
			streamComplete = effectiveAfter >= *latestAvailable
		}
	}
	return &RunEventPageResponse{
		Items: items,
		Meta: RunEventPageMeta{
			RequestedAfterSequence:    afterSequence,
			EffectiveAfterSequence:    effectiveAfter,
			RetainedThroughSequence:   retainedThrough,
			EarliestAvailableSequence: earliestAvailable,
			LatestAvailableSequence:   latestAvailable,
			RetentionGap:              afterSequence < retainedThrough,
			Terminal:                  terminal,
			StreamComplete:            streamComplete,
		},
	}, nil
}

func isTerminalRunStatus(status string) bool {
	switch status {
	case "success", "failed", "timeout", "canceled":
		return true
	default:
		return false
	}
}
