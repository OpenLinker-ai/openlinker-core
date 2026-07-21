package runtime_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRuntimeMaintenanceSchedulesExecuteAgainstPostgreSQL(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)

	deadline, err := db.New(pool).NextRuntimeReconcileDue(ctx)
	require.NoError(t, err)
	require.NotNil(t, deadline.NextDueAt)
	require.True(t, deadline.NextDueAt.After(deadline.DatabaseNow))
	require.False(t, deadline.DatabaseNow.IsZero())

	seedFinalizerEffectTargets(t, pool, fixture.identity.RunID, fixture.identity.AgentID, "run.canceled")
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT user_id FROM runs WHERE id = $1`, fixture.identity.RunID).Scan(&ownerID))
	_, err = runtime.NewRuntimeCancellationCoordinator(pool).CancelOwnedRun(
		ctx, ownerID, fixture.identity.RunID, "schedule integration",
	)
	require.NoError(t, err)

	cancellation, err := db.New(pool).NextRuntimeCancellationReapDue(ctx, 30_000)
	require.NoError(t, err)
	require.NotNil(t, cancellation.NextDueAt)
	require.True(t, cancellation.NextDueAt.After(cancellation.DatabaseNow))
	require.LessOrEqual(t, cancellation.NextDueAt.Sub(cancellation.DatabaseNow), 30*time.Second)
	require.False(t, cancellation.DatabaseNow.IsZero())
}
