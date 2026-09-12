package runtime_test

import (
	"context"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestQueuedRunInitialEventCommitsBeforeDispatchNotification(t *testing.T) {
	for _, mode := range []string{"run", "start", "offline start"} {
		t.Run(mode, func(t *testing.T) {
			pool := setupTestDB(t)
			resetRuntimeNodeAdminTables(t, pool)
			fixture := insertRuntimeNodeAdminFixture(t, pool)
			ctx := context.Background()
			user := insertRuntimeUser(t, pool)
			if mode == "offline start" {
				resetRuntimeNodeAdminTables(t, pool)
			}
			// A deferred constraint runs just before NOTIFY becomes visible.
			// Previously run.available committed first and its post-commit event
			// append could lock the Run while the receiver used SKIP LOCKED.
			_, err := pool.Exec(ctx, `
CREATE FUNCTION qa_require_initial_dispatch_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.event_type = 'run.available' AND NOT EXISTS (
   SELECT 1 FROM run_events WHERE run_id = NEW.run_id
   AND event_type IN ('run.dispatch.pending', 'run.dispatch.waiting_runtime')
 ) THEN RAISE EXCEPTION 'dispatch notification committed before initial event'; END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER qa_initial_dispatch_event AFTER INSERT ON runtime_signal_outbox
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION qa_require_initial_dispatch_event();`)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := pool.Exec(context.Background(), `DROP TRIGGER qa_initial_dispatch_event ON runtime_signal_outbox; DROP FUNCTION qa_require_initial_dispatch_event();`)
				require.NoError(t, err)
			})
			svc := newTestService(t, pool)
			call := svc.StartRun
			if mode == "run" {
				call = svc.Run
			}
			req := &runtime.RunRequest{AgentID: fixture.agentID.String(), Input: map[string]any{"task": "dispatch order"}, IdempotencyKey: "dispatch-order"}
			first, err := call(ctx, user, req, "api")
			require.NoError(t, err)
			replayed, err := call(ctx, user, req, "api")
			require.NoError(t, err)
			require.Equal(t, first.RunID, replayed.RunID)
			var count int
			var sameTransaction bool
			require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*), bool_and(e.xmin = r.xmin)
FROM run_events e JOIN runs r ON r.id = e.run_id WHERE r.id = $1
AND e.event_type IN ('run.dispatch.pending', 'run.dispatch.waiting_runtime')`, uuid.MustParse(first.RunID)).Scan(&count, &sameTransaction))
			require.Equal(t, 1, count)
			require.True(t, sameTransaction)
		})
	}
}
