package runtime

import (
	"context"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/google/uuid"
)

type replayCapabilityQueries interface {
	GetRunDeadLetterByRun(context.Context, uuid.UUID) (db.RunDeadLetter, error)
}

func isReplaySource(run db.Run) bool {
	return run.RuntimeContractID == RuntimeContractID &&
		run.Status == "failed" && run.DispatchState == string(RuntimeDispatchDeadLetter)
}

// This is a viewer-specific hint. ReplayRun still rechecks authorization and
// source evidence when the user submits; an unavailable evidence read hides it.
func runReplayCapability(ctx context.Context, queries replayCapabilityQueries, viewer uuid.UUID, run db.Run) bool {
	if run.UserID != viewer || !isReplaySource(run) {
		return false
	}
	_, err := queries.GetRunDeadLetterByRun(ctx, run.ID)
	return err == nil
}
