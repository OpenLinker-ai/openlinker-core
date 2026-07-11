package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeV2DeadlineReconcilerRejectsInvalidConfigurationAndBatch(t *testing.T) {
	_, err := (*RuntimeV2DeadlineReconciler)(nil).ReconcileBatch(t.Context(), 1)
	require.ErrorIs(t, err, ErrRuntimeV2ReconcilerNotConfigured)

	reconciler := &RuntimeV2DeadlineReconciler{retryPlanner: fixedResultRetryPlanner{}}
	_, err = reconciler.ReconcileBatch(t.Context(), 1)
	require.ErrorIs(t, err, ErrRuntimeV2ReconcilerNotConfigured)

	configured := &RuntimeV2DeadlineReconciler{pool: nil, retryPlanner: fixedResultRetryPlanner{}}
	_, err = configured.ReconcileBatch(t.Context(), 0)
	require.True(t, errors.Is(err, ErrRuntimeV2ReconcilerNotConfigured))
}

func TestRuntimeV2ReconcileTerminalPrecedence(t *testing.T) {
	now := time.Date(2026, 7, 11, 20, 0, 0, 0, time.UTC)
	base := db.RuntimeV2ReconcileLockedRunRow{
		ID: uuid.New(), DatabaseNow: now,
		DispatchDeadlineAt: now.Add(time.Minute), RunDeadlineAt: now.Add(2 * time.Minute),
		OfferCount: 1, MaxOfferCount: 2, AttemptCount: 1, MaxAttempts: 3,
	}

	require.Nil(t, runtimeV2DeadlineTerminalForRun(base))
	require.Nil(t, runtimeV2OfferTerminalForRun(base, db.RunAttempt{OfferExpiresAt: now}))
	require.Nil(t, runtimeV2ExecutionTerminalForRun(base))

	dispatchDue := base
	dispatchDue.DispatchDeadlineAt = now
	terminal := runtimeV2ExecutionTerminalForRun(dispatchDue)
	require.NotNil(t, terminal)
	require.Equal(t, "timeout", terminal.status)
	require.Equal(t, "RUNTIME_DISPATCH_TIMEOUT", terminal.errorCode)

	runDue := dispatchDue
	runDue.RunDeadlineAt = now
	terminal = runtimeV2ExecutionTerminalForRun(runDue)
	require.NotNil(t, terminal)
	require.Equal(t, "RUN_DEADLINE_EXCEEDED", terminal.errorCode)

	exhausted := base
	exhausted.AttemptCount = exhausted.MaxAttempts
	terminal = runtimeV2ExecutionTerminalForRun(exhausted)
	require.NotNil(t, terminal)
	require.Equal(t, "failed", terminal.status)
	require.Equal(t, "dead_letter", terminal.dispatchState)
	require.Equal(t, RuntimeResultClassificationDeadLetter, terminal.classification)

	offerExhausted := base
	offerExhausted.OfferCount = offerExhausted.MaxOfferCount
	terminal = runtimeV2OfferTerminalForRun(offerExhausted, db.RunAttempt{OfferExpiresAt: now})
	require.NotNil(t, terminal)
	require.Equal(t, "RUNTIME_DISPATCH_TIMEOUT", terminal.errorCode)
}
