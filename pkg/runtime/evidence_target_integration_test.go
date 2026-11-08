package runtime_test

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// These tests cover the finalizer side of connection-target evidence: a Run
// that executed against an Agent's previous connection target must not write
// the availability snapshot of the target the Agent points at now.

type evidenceSnapshot struct {
	exists              bool
	status              string
	lastSuccessfulRunAt *time.Time
	lastFailedRunAt     *time.Time
	lastCheckedAt       *time.Time
	consecutiveFailures int32
}

func readEvidenceSnapshot(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) evidenceSnapshot {
	t.Helper()
	var snap evidenceSnapshot
	err := pool.QueryRow(context.Background(), `
		SELECT availability_status, last_successful_run_at, last_failed_run_at,
		       last_checked_at, consecutive_failures
		FROM agent_availability_snapshots WHERE agent_id = $1`, agentID).Scan(
		&snap.status, &snap.lastSuccessfulRunAt, &snap.lastFailedRunAt,
		&snap.lastCheckedAt, &snap.consecutiveFailures)
	if err == pgx.ErrNoRows {
		return snap
	}
	require.NoError(t, err)
	snap.exists = true
	return snap
}

func requireNoAvailabilityEvidence(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	snap := readEvidenceSnapshot(t, pool, agentID)
	if !snap.exists {
		return
	}
	require.Equal(t, "unknown", snap.status, "an outcome of the previous target must not rate the new one")
	require.Nil(t, snap.lastSuccessfulRunAt)
	require.Nil(t, snap.lastFailedRunAt)
	require.Nil(t, snap.lastCheckedAt, "the monitor must still probe the new target first")
	require.Zero(t, snap.consecutiveFailures)
}

func seedHealthyAvailability(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO agent_availability_snapshots (
		    agent_id, availability_status, last_successful_run_at, last_checked_at, consecutive_failures
		) VALUES ($1, 'healthy', NOW(), NOW(), 0)
		ON CONFLICT (agent_id) DO UPDATE SET availability_status = 'healthy',
		    last_successful_run_at = NOW(), last_checked_at = NOW(), consecutive_failures = 0`, agentID)
	require.NoError(t, err)
}

// makeFixtureAgentRuntime aligns the event-store fixture Agent with its Run's
// runtime connection snapshot.
func makeFixtureAgentRuntime(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		UPDATE agents SET connection_mode = 'runtime', endpoint_url = 'openlinker-runtime://' || slug
		WHERE id = $1`, agentID)
	require.NoError(t, err)
}

// editAgent changes the Agent through the production owner edit, which resets
// the stored evidence in its own transaction when the target changes.
func editAgent(ctx context.Context, pool *pgxpool.Pool, agentID uuid.UUID, mutate func(*agent.UpdateAgentRequest)) error {
	var creatorID uuid.UUID
	var name string
	if err := pool.QueryRow(ctx,
		`SELECT creator_id, name FROM agents WHERE id = $1`, agentID).Scan(&creatorID, &name); err != nil {
		return err
	}
	req := &agent.UpdateAgentRequest{Name: name, Tags: []string{"evidence"}}
	mutate(req)
	_, err := agent.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true}).
		UpdateAgent(ctx, agentID, creatorID, req)
	return err
}

func updateAgentConnection(t *testing.T, ctx context.Context, pool *pgxpool.Pool, agentID uuid.UUID, mutate func(*agent.UpdateAgentRequest)) {
	t.Helper()
	require.NoError(t, editAgent(ctx, pool, agentID, mutate))
}

func permanentRuntimeResult(fixture eventStoreFixture) runtime.RuntimeResultRequest {
	return runtime.RuntimeResultRequest{
		AttemptIdentity: fixture.identity,
		ResultID:        uuid.New(),
		Status:          "failed",
		Error: &runtime.RuntimeResultFailure{
			ErrorCode: "BAD_INPUT",
			Message:   "permanent failure",
		},
		DurationMS: 30,
	}
}

func repointToUntestedEndpoint(req *agent.UpdateAgentRequest) {
	req.ConnectionMode = agent.ConnectionModeDirectHTTP
	req.EndpointURL = "https://untested.example.com/agent"
}

func TestRuntimeWorkerResultAfterRepointDoesNotRateNewTarget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request func(eventStoreFixture) runtime.RuntimeResultRequest
		status  string
	}{
		{"success", func(f eventStoreFixture) runtime.RuntimeResultRequest {
			return successfulRuntimeResult(f, map[string]any{"result": "pong"})
		}, "success"},
		{"failure", func(f eventStoreFixture) runtime.RuntimeResultRequest {
			return permanentRuntimeResult(f)
		}, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			requireReliableRuntimeSchema(t, pool)
			ctx := context.Background()
			// A Runtime dry-run or ordinary Run is executing on the worker.
			fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
			agentID := fixture.identity.AgentID
			makeFixtureAgentRuntime(t, pool, agentID)
			seedHealthyAvailability(t, pool, agentID)

			// The owner switches the Agent to an endpoint nobody has tested.
			updateAgentConnection(t, ctx, pool, agentID, repointToUntestedEndpoint)
			requireNoAvailabilityEvidence(t, pool, agentID)

			// The worker then finishes through the production Result entrypoint.
			svc := newTestService(t, pool)
			ack, err := svc.FinalizeRuntimeResult(ctx, fixture.principal, tc.request(fixture))
			require.NoError(t, err)
			require.Equal(t, tc.status, ack.RunStatus)

			requireNoAvailabilityEvidence(t, pool, agentID)
			owned, err := agent.NewService(pool, &config.Config{}).GetMyAgent(ctx, agentID, creatorIDOf(t, pool, agentID))
			require.NoError(t, err)
			require.False(t, owned.Readiness.Callable, "the untested endpoint must not become callable")
			require.Equal(t, "unknown", owned.Availability.Status)
		})
	}
}

func TestRuntimeWorkerResultForUnchangedTargetRecordsAvailability(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	ctx := context.Background()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	agentID := fixture.identity.AgentID
	makeFixtureAgentRuntime(t, pool, agentID)
	// Editing fields that are not the connection target keeps the Run's
	// outcome attributable to the Agent.
	updateAgentConnection(t, ctx, pool, agentID, func(req *agent.UpdateAgentRequest) {
		req.Description = "renamed while the worker runs"
	})

	svc := newTestService(t, pool)
	ack, err := svc.FinalizeRuntimeResult(ctx, fixture.principal, successfulRuntimeResult(fixture, map[string]any{"result": "pong"}))
	require.NoError(t, err)
	require.Equal(t, "success", ack.RunStatus)

	snap := readEvidenceSnapshot(t, pool, agentID)
	require.True(t, snap.exists)
	require.Equal(t, "healthy", snap.status)
	require.NotNil(t, snap.lastSuccessfulRunAt)
}

func creatorIDOf(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) uuid.UUID {
	t.Helper()
	var creatorID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT creator_id FROM agents WHERE id = $1`, agentID).Scan(&creatorID))
	return creatorID
}

// A Core-executed Run calls the endpoint captured when it was created. When
// the owner changes the target (or only its credential) during the call, the
// outcome is not evidence about the Agent's current target.
func TestCoreRunRepointedDuringCallDoesNotRateNewTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*agent.UpdateAgentRequest)
		body   string
		status int
	}{
		{"endpoint success", repointToUntestedEndpoint, `{"output":{"text":"ok"}}`, http.StatusOK},
		{"endpoint failure", repointToUntestedEndpoint, `{"error":"boom"}`, http.StatusInternalServerError},
		{"credential rotation", func(req *agent.UpdateAgentRequest) {
			req.EndpointAuthHeader = "Bearer rotated"
		}, `{"output":{"text":"ok"}}`, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			svc := newTestService(t, pool)
			ctx := context.Background()
			userID := insertRuntimeUser(t, pool)
			creatorID := insertCreator(t, pool)

			var target atomic.Value
			edited := make(chan error, 1)
			endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
				edited <- editAgent(context.Background(), pool, target.Load().(uuid.UUID), tc.mutate)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
			target.Store(agentID)
			seedHealthyAvailability(t, pool, agentID)

			resp, err := svc.Run(ctx, userID, makeRunReq(agentID, map[string]any{"q": "hello"}), "")
			require.NoError(t, err)
			require.NoError(t, <-edited)
			require.NotEqual(t, "running", resp.Status)

			requireNoAvailabilityEvidence(t, pool, agentID)
			totalCalls, _ := readAgentStats(t, pool, agentID)
			if resp.Status == "success" {
				require.Equal(t, int32(1), totalCalls, "call statistics still count the Run")
			}
		})
	}
}

func TestCoreRunForUnchangedTargetRecordsAvailability(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	userID := insertRuntimeUser(t, pool)
	creatorID := insertCreator(t, pool)

	var target atomic.Value
	edited := make(chan error, 1)
	endpoint := startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
		edited <- editAgent(context.Background(), pool, target.Load().(uuid.UUID), func(req *agent.UpdateAgentRequest) {
			req.Description = "edited during the call"
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"output":{"text":"ok"}}`)
	})
	agentID := insertAgent(t, pool, creatorID, endpoint, 10, "approved")
	target.Store(agentID)

	resp, err := svc.Run(ctx, userID, makeRunReq(agentID, nil), "")
	require.NoError(t, err)
	require.NoError(t, <-edited)
	require.Equal(t, "success", resp.Status)
	snap := readEvidenceSnapshot(t, pool, agentID)
	require.True(t, snap.exists)
	require.Equal(t, "healthy", snap.status)
	require.NotNil(t, snap.lastSuccessfulRunAt)
}
