package runtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// Run only against a disposable TEST_DATABASE_URL: setupTestDB truncates.
// No outbox subscriber, compensation scanner or cancellation reaper is started.
type cancellationAppendGateKey struct{}
type cancellationAppendGate struct {
	locked, release chan struct{}
	once            sync.Once
}

func (g *cancellationAppendGate) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, cancellationAppendGateKey{}, strings.HasPrefix(data.SQL, "-- name: LockRunForEventAppend "))
}

func (g *cancellationAppendGate) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if held, _ := ctx.Value(cancellationAppendGateKey{}).(bool); !held || data.Err != nil {
		return
	}
	g.once.Do(func() {
		close(g.locked)
		select {
		case <-g.release:
		case <-ctx.Done():
		}
	})
}

func TestRuntimeCancellationNormalAppendSerializesOnSession(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_id FROM runs WHERE id=$1`, fixture.identity.RunID).Scan(&ownerID))
	_, err := coordinator.CancelOwnedRun(ctx, ownerID, fixture.identity.RunID, "synthetic review")
	require.NoError(t, err)
	principal := runtimeCancellationSessionPrincipal(t, pool, fixture)
	gate := &cancellationAppendGate{locked: make(chan struct{}), release: make(chan struct{})}
	config := pool.Config()
	config.ConnConfig.Tracer = gate
	tracedPool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer tracedPool.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	defer release()
	appendDone := make(chan error, 1)
	go func() {
		_, err := runtime.NewEventStore(tracedPool).Append(ctx, fixture.principal, fixture.identity, eventStoreRequest(1))
		appendDone <- err
	}()
	select {
	case <-gate.locked:
	case <-ctx.Done():
		t.Fatal("real Append did not acquire its Run lock")
	}
	commands := make(chan cancellationNextResult, 1)
	go func() {
		cmd, _, err := coordinator.NextCommand(ctx, principal)
		commands <- cancellationNextResult{cmd, err}
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '-- name: LockRuntimeSessionForPrincipalValidation%')`).Scan(&blocked)
		return err == nil && blocked
	}, 2*time.Second, 10*time.Millisecond, "NextCommand must wait on the shared Session lock, before considering SKIP LOCKED")
	select {
	case result := <-commands:
		t.Fatalf("NextCommand unexpectedly returned while Append held Session: %+v", result)
	default:
	}
	release()
	require.True(t, runtime.IsRuntimeEventError(<-appendDone, runtime.RuntimeEventErrorRunAlreadyTerminal))
	result := <-commands
	require.NoError(t, result.err)
	require.NotNil(t, result.command)
	decoded, err := runtime.DecodePendingCommand(*result.command)
	require.NoError(t, err)
	require.Equal(t, fixture.identity.RunID, decoded.Cancel.AttemptIdentity.RunID)
	t.Log("production Append holds Session before Run; cancellation waits and is delivered after release")
}

type cancellationNextResult struct {
	command *runtime.PendingCommand
	err     error
}
type cancellationNextProbe struct {
	*runtime.RuntimeCancellationCoordinator
	calls chan cancellationNextResult
}

func (p *cancellationNextProbe) NextCommand(ctx context.Context, principal runtime.RuntimeSessionPrincipal) (*runtime.PendingCommand, time.Time, error) {
	command, now, err := p.RuntimeCancellationCoordinator.NextCommand(ctx, principal)
	select {
	case p.calls <- cancellationNextResult{command, err}:
	case <-ctx.Done():
	}
	return command, now, err
}

func (p *cancellationNextProbe) PollCommands(ctx context.Context, principal runtime.RuntimeSessionPrincipal) (runtime.RuntimeCommandsResponse, error) {
	response, err := p.RuntimeCancellationCoordinator.PollCommands(ctx, principal)
	var command *runtime.PendingCommand
	if len(response.Commands) != 0 {
		command = &response.Commands[0]
	}
	select {
	case p.calls <- cancellationNextResult{command, err}:
	case <-ctx.Done():
	}
	return response, err
}

type cancellationTransport struct {
	coordinator *runtime.RuntimeCancellationCoordinator
	probe       *cancellationNextProbe
	hub         *runtime.RuntimeWakeHub
	server      *httptest.Server
	loopReady   <-chan struct{}
}

func newCancellationTransport(t *testing.T, pool *pgxpool.Pool, fixture eventStoreFixture) cancellationTransport {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime', endpoint_url='openlinker-runtime://review' WHERE id=$1`, fixture.identity.AgentID)
	require.NoError(t, err)
	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	probe := &cancellationNextProbe{coordinator, make(chan cancellationNextResult, 128)}
	hub := runtime.NewRuntimeWakeHub()
	service := runtime.NewService(pool, newTestConfig())
	service.ConfigureCoreRuntime(fixture.coreInstanceID)
	signer, err := runtime.NewRuntimeInvocationSigner(strings.Repeat("synthetic-review-", 4))
	require.NoError(t, err)
	loopReady := make(chan struct{})
	var loopOnce sync.Once
	controller := runtime.NewRuntimeHTTPController(runtime.RuntimeHTTPDependencies{
		TransportPolicy:     runtime.CurrentRuntimeTransportPolicy,
		TokenValidator:      dispatchIntegrationTokens{"review-token": db.AgentRuntimeToken{ID: fixture.credentialID, AgentID: fixture.identity.AgentID, Scopes: []string{"agent:pull", "agent:call"}}},
		DeviceAuthenticator: dispatchIntegrationDevice{runtime.RuntimeDeviceIdentity{NodeID: *fixture.identity.NodeID, CertificateSerial: *fixture.principal.DeviceCertificateSerial, CertificateFingerprintSHA256: strings.Repeat("a", 64), PublicKeyThumbprintSHA256: *fixture.principal.DevicePublicKeyThumbprintSHA256}},
		Sessions:            runtime.NewRuntimeSessionService(pool, fixture.coreInstanceID),
		Leases:              runtime.NewRuntimeLeaseService(pool, fixture.coreInstanceID, signer, runtime.RuntimeLeaseConfig{}),
		EventProjector:      service, Finalizer: runtime.NewResultFinalizer(pool, nil, nil),
		Cancellations: probe, WakeHub: hub, CoreInstanceID: fixture.coreInstanceID,
		Observer: runtime.WorkerObserverFunc(func(o runtime.WorkerObservation) {
			if o.Category == "runtime.websocket.policy_check" {
				loopOnce.Do(func() { close(loopReady) })
			}
		}),
	})
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	server := httptest.NewServer(e)
	t.Cleanup(func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		require.NoError(t, controller.Shutdown(shutdownCtx))
		server.Close()
	})
	return cancellationTransport{coordinator, probe, hub, server, loopReady}
}

func TestRuntimeCancellationContentionRetriesWithoutCompensation(t *testing.T) {
	for _, lockKind := range []string{"direct_event_run_lock", "production_system_event_wrapper", "production_child_completed_wrapper"} {
		t.Run(lockKind, func(t *testing.T) {
			pool := setupTestDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
			transport := newCancellationTransport(t, pool, fixture)
			coordinator, probe, hub := transport.coordinator, transport.probe, transport.hub
			server, loopReady := transport.server, transport.loopReady
			conn, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/agent-runtime/ws", http.Header{"Authorization": []string{"Bearer review-token"}})
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, sendDispatchIntegrationMessage(conn, runtime.RuntimeMessageHello, runtime.RuntimeHelloPayload{
				NodeID: *fixture.identity.NodeID, AgentID: fixture.identity.AgentID, WorkerID: *fixture.identity.WorkerID,
				RuntimeSessionID: *fixture.identity.RuntimeSessionID, SessionEpoch: 1, NodeVersion: "test-v2", Capacity: 1,
				Features: runtime.RuntimeRequiredFeatures(), ContractDigest: fixture.principal.RuntimeContractDigest,
			}))
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(8*time.Second)))
			var ready runtime.RuntimeEnvelope
			require.NoError(t, conn.ReadJSON(&ready))
			require.Equal(t, runtime.RuntimeMessageReady, ready.Type, string(ready.Payload))
			select {
			case <-loopReady:
			case <-ctx.Done():
				t.Fatal("WS maintenance loop not ready")
			}
			var ownerID uuid.UUID
			require.NoError(t, pool.QueryRow(ctx, `SELECT user_id FROM runs WHERE id=$1`, fixture.identity.RunID).Scan(&ownerID))
			created, err := coordinator.CancelOwnedRun(ctx, ownerID, fixture.identity.RunID, "synthetic lock review")
			require.NoError(t, err)
			held, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer held.Rollback(context.Background())
			switch lockKind {
			case "direct_event_run_lock":
				_, err = db.New(held).LockRunForEventAppend(ctx, fixture.identity.RunID)
			case "production_system_event_wrapper":
				_, err = db.New(held).CreateRunEvent(ctx, db.CreateRunEventParams{RunID: fixture.identity.RunID, EventType: "run.review.note", Payload: []byte(`{"synthetic":true}`)})
			case "production_child_completed_wrapper":
				_, err = db.New(held).CreateRunEffectParentEvent(ctx, db.CreateRunEffectParentEventParams{ID: uuid.New(), ParentRunID: fixture.identity.RunID, Payload: []byte(`{"synthetic":true}`)})
			}
			require.NoError(t, err)
			messages := make(chan runtime.RuntimeEnvelope, 10)
			readErrors := make(chan error, 1)
			go func() {
				for {
					var message runtime.RuntimeEnvelope
					if err := conn.ReadJSON(&message); err != nil {
						readErrors <- err
						return
					}
					messages <- message
				}
			}()
			hub.WakeControl(fixture.identity.AgentID)
			select {
			case first := <-probe.calls:
				require.Nil(t, first.command, "Run-only lock causes the real candidate query to skip")
				require.ErrorContains(t, first.err, "contended")
			case <-ctx.Done():
				t.Fatal("control wake never called NextCommand")
			}
			require.NoError(t, held.Commit(ctx))
			select {
			case message := <-messages:
				require.Equal(t, runtime.RuntimeMessageRunCancel, message.Type, string(message.Payload))
				var command runtime.RunCancelPayload
				require.NoError(t, json.Unmarshal(message.Payload, &command))
				require.Equal(t, fixture.identity.RunID, command.AttemptIdentity.RunID)
				require.Equal(t, created.Cancellation.ID, command.CancellationID)
				// Stop evidence must still release the slot through the normal ACK
				// path after a retried delivery, with the original correlation ID.
				require.NoError(t, sendDispatchIntegrationMessage(conn, runtime.RuntimeMessageRunCancelAck, runtime.RunCancelAckPayload{
					CancellationID: command.CancellationID, AttemptIdentity: command.AttemptIdentity, CancelState: runtime.RuntimeCancelStopped,
				}, &message.MessageID))
				require.Eventually(t, func() bool {
					var state string
					return pool.QueryRow(ctx, `SELECT state FROM run_cancellations WHERE id=$1`, command.CancellationID).Scan(&state) == nil && state == "stopped"
				}, time.Second, 10*time.Millisecond)
				assertRuntimeCancellationAttemptCapacity(t, pool, fixture, true, 0, 0)
			case err := <-readErrors:
				t.Fatal(err)
			case <-time.After(2 * time.Second):
				t.Fatal("single control wake did not deliver cancellation after Run lock release")
			}
			t.Log("one control wake delivers after Run lock release, without a second wake or compensation")
		})
	}
}

func TestRuntimeCancellationHTTPContentionRetriesWithoutWake(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	transport := newCancellationTransport(t, pool, fixture)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_id FROM runs WHERE id=$1`, fixture.identity.RunID).Scan(&ownerID))
	created, err := transport.coordinator.CancelOwnedRun(ctx, ownerID, fixture.identity.RunID, "HTTP contention")
	require.NoError(t, err)
	held, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer held.Rollback(context.Background())
	_, err = db.New(held).CreateRunEffectParentEvent(ctx, db.CreateRunEffectParentEventParams{
		ID: uuid.New(), ParentRunID: fixture.identity.RunID, Payload: []byte(`{"synthetic":true}`),
	})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		transport.server.URL+"/api/v1/agent-runtime/commands?runtime_session_id="+fixture.identity.RuntimeSessionID.String()+"&wait=3", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer review-token")
	request.Header.Set(runtime.RuntimeAttachmentIDHeader, fixture.principal.AttachmentID.String())
	type result struct {
		body runtime.RuntimeCommandsResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer response.Body.Close()
		var body runtime.RuntimeCommandsResponse
		raw, err := io.ReadAll(response.Body)
		if err == nil {
			err = json.Unmarshal(raw, &body)
		}
		if response.StatusCode != http.StatusOK {
			err = fmt.Errorf("HTTP commands returned %d: %s", response.StatusCode, raw)
		}
		done <- result{body, err}
	}()
	select {
	case first := <-transport.probe.calls:
		require.Nil(t, first.command)
		require.ErrorContains(t, first.err, "contended")
	case got := <-done:
		t.Fatalf("commands returned before checking contention: %v", got.err)
	case <-ctx.Done():
		t.Fatal("HTTP request did not reach the actual command query")
	}
	require.NoError(t, held.Commit(ctx))
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Len(t, got.body.Commands, 1)
		decoded, err := runtime.DecodePendingCommand(got.body.Commands[0])
		require.NoError(t, err)
		require.Equal(t, created.Cancellation.ID, decoded.Cancel.CancellationID)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP commands waited for timeout instead of retrying after lock release")
	}
}

func TestRuntimeCancellationPresenceUsesSessionAndDeadline(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_id FROM runs WHERE id=$1`, fixture.identity.RunID).Scan(&ownerID))
	created, err := coordinator.CancelOwnedRun(ctx, ownerID, fixture.identity.RunID, "scope and deadline")
	require.NoError(t, err)
	params := db.HasPendingRuntimeCancellationCommandParams{
		AgentID: fixture.identity.AgentID, NodeID: *fixture.identity.NodeID,
		CredentialID: fixture.credentialID, WorkerID: *fixture.identity.WorkerID,
		RuntimeSessionID: *fixture.identity.RuntimeSessionID, CommandDeadlineMs: 30_000,
	}
	queries := db.New(pool)
	pending, err := queries.HasPendingRuntimeCancellationCommand(ctx, params)
	require.NoError(t, err)
	require.True(t, pending)
	for _, tc := range []struct {
		name   string
		change func(*db.HasPendingRuntimeCancellationCommandParams)
	}{
		{"agent", func(p *db.HasPendingRuntimeCancellationCommandParams) { p.AgentID = uuid.New() }},
		{"node", func(p *db.HasPendingRuntimeCancellationCommandParams) { p.NodeID = uuid.New() }},
		{"credential", func(p *db.HasPendingRuntimeCancellationCommandParams) { p.CredentialID = uuid.New() }},
		{"worker", func(p *db.HasPendingRuntimeCancellationCommandParams) { p.WorkerID += "-other" }},
		{"session", func(p *db.HasPendingRuntimeCancellationCommandParams) { p.RuntimeSessionID = uuid.New() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			other := params
			tc.change(&other)
			pending, err := queries.HasPendingRuntimeCancellationCommand(ctx, other)
			require.NoError(t, err)
			require.False(t, pending, "another principal must not sustain retries")
		})
	}
	expireRuntimeCancellationRequestAtDatabaseClock(t, pool, fixture.identity.RunID, created.Cancellation.ID)
	pending, err = queries.HasPendingRuntimeCancellationCommand(ctx, params)
	require.NoError(t, err)
	require.False(t, pending, "expired commands must stop retries even without a reaper")
	command, _, err := coordinator.NextCommand(ctx, runtimeCancellationSessionPrincipal(t, pool, fixture))
	require.NoError(t, err)
	require.Nil(t, command)
}
