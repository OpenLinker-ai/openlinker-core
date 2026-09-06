package runtime

import (
	"context"
	"errors"
	"testing"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type replayCapabilityFake struct {
	calls int
	err   error
}

func (f *replayCapabilityFake) GetRunDeadLetterByRun(context.Context, uuid.UUID) (db.RunDeadLetter, error) {
	f.calls++
	return db.RunDeadLetter{}, f.err
}

func TestRunReplayCapabilityRequiresOwnedVerifiedDeadLetter(t *testing.T) {
	owner := uuid.New()
	base := db.Run{ID: uuid.New(), UserID: owner, RuntimeContractID: RuntimeContractID, Status: "failed", DispatchState: string(RuntimeDispatchDeadLetter)}
	for _, tc := range []struct {
		name                       string
		viewer                     uuid.UUID
		status, dispatch, contract string
		err                        error
		want                       bool
		queries                    int
	}{
		{"owner verified", owner, "failed", string(RuntimeDispatchDeadLetter), RuntimeContractID, nil, true, 1},
		{"creator can read but cannot replay", uuid.New(), "failed", string(RuntimeDispatchDeadLetter), RuntimeContractID, nil, false, 0},
		{"automatic retry still running", owner, "running", "retry_wait", RuntimeContractID, nil, false, 0},
		{"failed without dead letter", owner, "failed", "terminal", RuntimeContractID, nil, false, 0},
		{"wrong contract", owner, "failed", string(RuntimeDispatchDeadLetter), "other", nil, false, 0},
		{"missing evidence", owner, "failed", string(RuntimeDispatchDeadLetter), RuntimeContractID, pgx.ErrNoRows, false, 1},
		{"evidence read failed", owner, "failed", string(RuntimeDispatchDeadLetter), RuntimeContractID, errors.New("unavailable"), false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := base
			run.Status = tc.status
			run.DispatchState = tc.dispatch
			run.RuntimeContractID = tc.contract
			queries := &replayCapabilityFake{err: tc.err}
			require.Equal(t, tc.want, runReplayCapability(context.Background(), queries, tc.viewer, run))
			require.Equal(t, tc.queries, queries.calls)
		})
	}
}
