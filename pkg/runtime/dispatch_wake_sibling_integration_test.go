package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcpbinding"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// A Session that cannot take a Run (its slot is full) must not swallow the
// Agent's coalesced dispatch hint: an idle sibling on the same Core receives
// the Run without RuntimeDispatchWakeReconciler, which this test never starts.
func TestRuntimeDispatchWakeReachesIdleSiblingOfFullSession(t *testing.T) {
	h := newDispatchSiblingHarness(t)
	full := h.addNode(t, runtime.RuntimeRequiredFeatures())
	idle := h.addNode(t, runtime.RuntimeRequiredFeatures())
	h.start(t, true)

	fullSession := h.connect(t, full, 1, false)
	held := h.createRun(t, nil)
	require.Equal(t, held, fullSession.next(t), "the first Session accepts and holds its only slot")
	// Let the attachment probe, the held claim and its post-ACK continuation
	// finish so later Runs only reach the full Session through the Agent hint.
	h.leases.waitQuiet(t, 2)

	// The idle Session's attachment token is shared with its same-class
	// sibling; whichever Session takes it, wait for that probe to finish.
	before := h.leases.total()
	idleSession := h.connect(t, idle, 4, true)
	h.leases.waitQuiet(t, before+1)

	for i := 0; i < 6; i++ {
		runID := h.createRun(t, nil)
		require.Equal(t, runID, idleSession.next(t), "run %d must reach the idle sibling", i)
	}
	fullSession.requireNone(t)
}

// A Session whose features exclude a Run must not consume the only dispatch
// hint for it: the eligible sibling of another eligibility class gets the Run.
func TestRuntimeDispatchWakeReachesEligibleSiblingAcrossFeatureClasses(t *testing.T) {
	h := newDispatchSiblingHarness(t)
	standard := h.addNode(t, runtime.RuntimeRequiredFeatures())
	mcpFeatures := append(runtime.RuntimeRequiredFeatures(), mcpbinding.Feature)
	mcp := h.addNode(t, mcpFeatures)
	// Signals are published by hand so each Run's MCP snapshot exists before
	// any Session can learn about it.
	h.start(t, false)

	standardSession := h.connect(t, standard, 4, true)
	mcpSession := h.connect(t, mcp, 4, true)
	h.leases.waitClaims(t, standardSession.sessionID, 1)
	h.leases.waitClaims(t, mcpSession.sessionID, 1)

	for i := 0; i < 6; i++ {
		runID := h.createRun(t, func(runID uuid.UUID) {
			digest := fmt.Sprintf("%x", sha256.Sum256([]byte(runID.String())))
			_, err := h.pool.Exec(h.ctx, `
INSERT INTO run_mcp_binding_snapshots
    (run_id, binding_id, agent_id, target_agent_id, owner_user_id, digest, tool)
VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb)`,
				runID, uuid.New(), h.agentID, uuid.New(), h.ownerID, digest)
			require.NoError(t, err)
		})
		h.publishSignals(t)
		require.Equal(t, runID, mcpSession.next(t), "MCP run %d must reach the MCP-capable Session", i)
	}
	standardSession.requireNone(t)
}

// Backlog whose signal never reaches this Core (published before a restart,
// or consumed by a Session that is gone) is claimed when a Worker attaches,
// without waiting for RuntimeDispatchWakeReconciler.
func TestRuntimeDispatchWakeAttachClaimsBacklogWithoutReconciler(t *testing.T) {
	h := newDispatchSiblingHarness(t)
	node := h.addNode(t, runtime.RuntimeRequiredFeatures())
	// No outbox publication: the backlog's run.available signal stays pending.
	h.start(t, false)
	backlog := []uuid.UUID{h.createRun(t, nil), h.createRun(t, nil)}

	session := h.connect(t, node, 4, true)
	got := []uuid.UUID{session.next(t), session.next(t)}
	require.ElementsMatch(t, backlog, got)
}

type dispatchSiblingNode struct {
	device    runtime.RuntimeDeviceIdentity
	features  []string
	token     string
	agentID   uuid.UUID
	nodeIndex int
}

type dispatchSiblingHarness struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	coreID  uuid.UUID
	ownerID uuid.UUID
	agentID uuid.UUID
	service *runtime.Service
	leases  *countingRuntimeLeases
	bus     *runtime.LocalSignalBus
	tokens  dispatchIntegrationTokens
	devices dispatchSiblingDevices
	nodes   int
	url     string
}

func newDispatchSiblingHarness(t *testing.T) *dispatchSiblingHarness {
	t.Helper()
	pool := setupTestDB(t)
	resetRuntimeNodeAdminTables(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	ownerID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "openlinker-runtime://dispatch-sibling", 0, "approved")
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime' WHERE id=$1`, agentID)
	require.NoError(t, err)
	cfg := newTestConfig()
	cfg.RunTimeoutSeconds = 60
	coreID := uuid.New()
	service := runtime.NewService(pool, cfg)
	service.ConfigureCoreRuntime(coreID)
	return &dispatchSiblingHarness{
		ctx: ctx, pool: pool, coreID: coreID, ownerID: ownerID, agentID: agentID,
		service: service, tokens: dispatchIntegrationTokens{}, devices: dispatchSiblingDevices{},
		bus: runtime.NewLocalSignalBus(coreID),
	}
}

func (h *dispatchSiblingHarness) addNode(t *testing.T, features []string) dispatchSiblingNode {
	t.Helper()
	h.nodes++
	nodeID := uuid.New()
	thumbprint := fmt.Sprintf("%x", sha256.Sum256([]byte("dispatch-sibling/"+nodeID.String())))
	device := runtime.RuntimeDeviceIdentity{
		NodeID: nodeID, CertificateSerial: strings.ReplaceAll(nodeID.String(), "-", ""),
		CertificateFingerprintSHA256: strings.Repeat("a", 64), PublicKeyThumbprintSHA256: thumbprint,
	}
	_, err := h.pool.Exec(h.ctx, `INSERT INTO runtime_nodes
    (node_id,display_name,device_certificate_serial,device_public_key_thumbprint,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,capacity,inflight,status,last_seen_at)
    VALUES ($1,'dispatch sibling',$2,$3,'dispatch-sibling-v1',2,$4,$5,$6,8,0,'active',clock_timestamp())`,
		nodeID, device.CertificateSerial, thumbprint, runtime.RuntimeContractID, runtime.RuntimeContractDigest, features)
	require.NoError(t, err)
	tokenID := uuid.New()
	plaintext := tokenID.String()
	_, err = h.pool.Exec(h.ctx, `INSERT INTO agent_tokens (id,agent_id,creator_user_id,name,prefix,token_hash,scopes,status,redeemed_at)
     VALUES ($1,$2,$3,'dispatch sibling',$4,$4,ARRAY['agent:pull']::text[],'active_runtime',clock_timestamp())`,
		tokenID, h.agentID, h.ownerID, "ol_agent_"+plaintext[:8])
	require.NoError(t, err)
	h.tokens[plaintext] = db.AgentRuntimeToken{ID: tokenID, AgentID: h.agentID, Scopes: []string{"agent:pull"}}
	h.devices[plaintext] = device
	return dispatchSiblingNode{device: device, features: features, token: plaintext, agentID: h.agentID, nodeIndex: h.nodes}
}

// start serves Runtime WebSockets and the signal subscriber. With
// continuousOutbox the outbox worker publishes like production; otherwise the
// test publishes explicitly. No dispatch wake reconciler is started.
func (h *dispatchSiblingHarness) start(t *testing.T, continuousOutbox bool) {
	t.Helper()
	pool := h.pool
	signer, err := runtime.NewRuntimeInvocationSigner(strings.Repeat("integration-only-", 4))
	require.NoError(t, err)
	hub := runtime.NewRuntimeWakeHub()
	h.leases = &countingRuntimeLeases{
		RuntimeLeaseAPI: runtime.NewRuntimeLeaseService(pool, h.coreID, signer, runtime.RuntimeLeaseConfig{}),
		claims:          map[uuid.UUID]int{},
	}
	controller := runtime.NewRuntimeHTTPController(runtime.RuntimeHTTPDependencies{
		TokenValidator: h.tokens, DeviceAuthenticator: h.devices,
		Sessions:       runtime.NewRuntimeSessionService(pool, h.coreID),
		Leases:         h.leases,
		EventProjector: h.service, Finalizer: runtime.NewResultFinalizer(pool, nil, nil),
		Cancellations: runtime.NewRuntimeCancellationCoordinator(pool), WakeHub: hub, CoreInstanceID: h.coreID,
	})
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	server := httptest.NewServer(e)
	h.url = "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/agent-runtime/ws"

	ctx, cancel := context.WithCancel(h.ctx)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		runtime.StartRuntimeSignalSubscriber(ctx, h.bus, h.coreID, hub, h.service)
	}()
	if continuousOutbox {
		workers.Add(1)
		go func() {
			defer workers.Done()
			runtime.StartRuntimeSignalOutboxWorker(ctx, runtime.NewRuntimeSignalOutboxWorker(db.New(pool), h.bus),
				runtime.RuntimeSignalOutboxWorkerConfig{Interval: 10 * time.Millisecond})
		}()
	}
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		require.NoError(t, controller.Shutdown(shutdownCtx))
		server.Close()
		cancel()
		_ = h.bus.Close()
		workers.Wait()
	})
}

func (h *dispatchSiblingHarness) publishSignals(t *testing.T) {
	t.Helper()
	_, err := runtime.NewRuntimeSignalOutboxWorker(db.New(h.pool), h.bus).
		ProcessOnce(h.ctx, runtime.RuntimeSignalOutboxWorkerConfig{})
	require.NoError(t, err)
}

func (h *dispatchSiblingHarness) createRun(t *testing.T, beforeSignal func(uuid.UUID)) uuid.UUID {
	t.Helper()
	run, err := h.service.StartRun(h.ctx, h.ownerID, &runtime.RunRequest{
		AgentID: h.agentID.String(), Input: map[string]any{"text": "dispatch sibling"},
		IdempotencyKey: uuid.NewString(),
	}, "api")
	require.NoError(t, err)
	runID := uuid.MustParse(run.RunID)
	if beforeSignal != nil {
		beforeSignal(runID)
	}
	return runID
}

// connect attaches one Worker Session. A completing Worker ACKs and finishes
// every assignment; a holding Worker ACKs and keeps its slots occupied.
func (h *dispatchSiblingHarness) connect(
	t *testing.T,
	node dispatchSiblingNode,
	capacity int64,
	complete bool,
) *dispatchSiblingClient {
	t.Helper()
	conn, response, err := websocket.DefaultDialer.Dial(h.url, http.Header{"Authorization": []string{"Bearer " + node.token}})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	require.NoError(t, err)
	client := &dispatchSiblingClient{conn: conn, sessionID: uuid.New(), arrived: make(chan uuid.UUID, 32), errs: make(chan error, 1)}
	hello := runtime.RuntimeHelloPayload{
		NodeID: node.device.NodeID, AgentID: node.agentID, WorkerID: fmt.Sprintf("dispatch-sibling-%d", node.nodeIndex),
		RuntimeSessionID: client.sessionID, SessionEpoch: 1, NodeVersion: "dispatch-sibling-v1",
		Capacity: capacity, Features: node.features, ContractDigest: runtime.RuntimeContractDigest,
	}
	require.NoError(t, sendDispatchIntegrationMessage(conn, runtime.RuntimeMessageHello, hello))
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	var ready runtime.RuntimeEnvelope
	require.NoError(t, conn.ReadJSON(&ready))
	require.Equal(t, runtime.RuntimeMessageReady, ready.Type, string(ready.Payload))
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.errs <- client.serve(complete)
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		<-done
	})
	return client
}

type dispatchSiblingClient struct {
	conn      *websocket.Conn
	sessionID uuid.UUID
	arrived   chan uuid.UUID
	errs      chan error
}

func (c *dispatchSiblingClient) serve(complete bool) error {
	for {
		if err := c.conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return err
		}
		var message runtime.RuntimeEnvelope
		if err := c.conn.ReadJSON(&message); err != nil {
			return err
		}
		switch message.Type {
		case runtime.RuntimeMessageRunAssigned:
			var assigned runtime.RunAssignedPayload
			if err := json.Unmarshal(message.Payload, &assigned); err != nil {
				return err
			}
			c.arrived <- assigned.AttemptIdentity.RunID
			if err := sendDispatchIntegrationMessage(c.conn, runtime.RuntimeMessageAssignmentAck,
				runtime.RunAssignmentAckPayload{AttemptIdentity: assigned.AttemptIdentity}, &message.MessageID); err != nil {
				return err
			}
		case runtime.RuntimeMessageAssignmentConfirmed:
			if !complete {
				continue
			}
			var confirmed runtime.RunAssignmentConfirmedPayload
			if err := json.Unmarshal(message.Payload, &confirmed); err != nil {
				return err
			}
			if err := sendDispatchIntegrationMessage(c.conn, runtime.RuntimeMessageRunResult, runtime.RunResultPayload{
				AttemptIdentity: confirmed.AttemptIdentity, ResultID: uuid.New(), Status: "success",
				Output: map[string]any{"summary": "dispatch sibling"}, DurationMS: 1,
			}); err != nil {
				return err
			}
		case runtime.RuntimeMessageRunResultAck:
		default:
			return fmt.Errorf("unexpected runtime message %s", message.Type)
		}
	}
}

func (c *dispatchSiblingClient) next(t *testing.T) uuid.UUID {
	t.Helper()
	select {
	case runID := <-c.arrived:
		return runID
	case err := <-c.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run was not dispatched to the eligible Session without the reconciler")
	}
	return uuid.Nil
}

func (c *dispatchSiblingClient) requireNone(t *testing.T) {
	t.Helper()
	select {
	case runID := <-c.arrived:
		t.Fatalf("ineligible Session unexpectedly received Run %s", runID)
	default:
	}
}

type dispatchSiblingDevices map[string]runtime.RuntimeDeviceIdentity

func (d dispatchSiblingDevices) AuthenticateHTTP(_ context.Context, request *http.Request) (runtime.RuntimeDeviceIdentity, error) {
	device, ok := d[strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")]
	if !ok {
		return runtime.RuntimeDeviceIdentity{}, fmt.Errorf("unknown dispatch sibling device")
	}
	return device, nil
}

// countingRuntimeLeases records completed claims per Session so the test can
// wait for a Session's attachment probe without sleeping.
type countingRuntimeLeases struct {
	runtime.RuntimeLeaseAPI
	mu     sync.Mutex
	claims map[uuid.UUID]int
}

func (l *countingRuntimeLeases) ClaimOffer(
	ctx context.Context,
	principal runtime.RuntimeSessionPrincipal,
) (*runtime.RunAssignedPayload, error) {
	assigned, err := l.RuntimeLeaseAPI.ClaimOffer(ctx, principal)
	l.mu.Lock()
	l.claims[principal.RuntimeSessionID]++
	l.mu.Unlock()
	return assigned, err
}

func (l *countingRuntimeLeases) total() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	total := 0
	for _, claims := range l.claims {
		total += claims
	}
	return total
}

// waitQuiet waits until all Sessions made at least minimum claims in total
// and then made no further claim for a short interval.
func (l *countingRuntimeLeases) waitQuiet(t *testing.T, minimum int) {
	t.Helper()
	last, stableSince := l.total(), time.Now()
	require.Eventually(t, func() bool {
		if current := l.total(); current != last {
			last, stableSince = current, time.Now()
		}
		return last >= minimum && time.Since(stableSince) >= 300*time.Millisecond
	}, 5*time.Second, 10*time.Millisecond, "claims: %v", l.snapshot())
}

func (l *countingRuntimeLeases) waitClaims(t *testing.T, sessionID uuid.UUID, minimum int) {
	t.Helper()
	require.Eventually(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.claims[sessionID] >= minimum
	}, 5*time.Second, 5*time.Millisecond, "Session %s claims: %v", sessionID, l.snapshot())
}

func (l *countingRuntimeLeases) snapshot() map[uuid.UUID]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.claims)
}

// A full Session's background claim must not lock a queued Run: an idle
// sibling claiming with the real hint would skip it and drop its demand. Both
// claims are real ClaimOffer transactions; the full Session's background claim
// is held at the point where it would hold the candidate lock.
func TestRuntimeDispatchWakeBackgroundClaimDoesNotHideRunFromSibling(t *testing.T) {
	pool := setupTestDB(t)
	resetRuntimeNodeAdminTables(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	coreID := uuid.New()
	service := runtime.NewService(pool, newTestConfig())
	service.ConfigureCoreRuntime(coreID)
	ownerID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, ownerID, "openlinker-runtime://claim-precheck", 0, "approved")
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime' WHERE id=$1`, agentID)
	require.NoError(t, err)
	full := insertDispatchClaimSession(ctx, t, pool, agentID, ownerID, coreID)
	idle := insertDispatchClaimSession(ctx, t, pool, agentID, ownerID, coreID)
	signer, err := runtime.NewRuntimeInvocationSigner(strings.Repeat("integration-only-", 4))
	require.NoError(t, err)
	leases := runtime.NewRuntimeLeaseService(pool, coreID, signer, runtime.RuntimeLeaseConfig{})
	startRun := func() uuid.UUID {
		t.Helper()
		run, startErr := service.StartRun(ctx, ownerID, &runtime.RunRequest{
			AgentID: agentID.String(), Input: map[string]any{"text": "claim pre-check"},
			IdempotencyKey: uuid.NewString(),
		}, "api")
		require.NoError(t, startErr)
		return uuid.MustParse(run.RunID)
	}

	// The full Session accepts the Run that occupies its only slot.
	held := startRun()
	offer, err := leases.ClaimOffer(ctx, full)
	require.NoError(t, err)
	require.NotNil(t, offer)
	require.Equal(t, held, offer.AttemptIdentity.RunID)
	_, err = leases.AckAssignment(ctx, full, runtime.RunAssignmentAckPayload{AttemptIdentity: offer.AttemptIdentity})
	require.NoError(t, err)

	queued := startRun()
	reached, release := make(chan struct{}), make(chan struct{})
	var reachedOnce sync.Once
	hooked := runtime.NewRuntimeLeaseServiceWithCandidateHookForTest(
		pool, coreID, signer, runtime.RuntimeLeaseConfig{},
		func() {
			reachedOnce.Do(func() { close(reached) })
			<-release
		},
	)
	type claimResult struct {
		assignment *runtime.RunAssignedPayload
		err        error
	}
	background := make(chan claimResult, 1)
	go func() {
		assignment, claimErr := hooked.ClaimOffer(runtime.WithRuntimeBackgroundClaimForTest(ctx), full)
		background <- claimResult{assignment, claimErr}
	}()
	var fullResult *claimResult
	select {
	case <-reached: // only possible if the background claim locked the queued Run
	case result := <-background:
		fullResult = &result
	case <-ctx.Done():
		t.Fatal("background claim did not progress")
	}

	// The idle sibling claims with the real hint while the full Session's
	// background claim is in flight or already finished.
	got, err := leases.ClaimOffer(ctx, idle)
	close(release)
	if fullResult == nil {
		result := <-background
		fullResult = &result
	}
	require.NoError(t, err)
	require.NotNil(t, got, "the idle sibling must not skip a Run the full Session cannot take")
	require.Equal(t, queued, got.AttemptIdentity.RunID)
	require.Nil(t, fullResult.assignment)
	require.True(t, runtime.IsRuntimeLeaseError(fullResult.err, runtime.RuntimeLeaseErrorNodeAtCapacity), "%v", fullResult.err)

	// Pull and signalled claims keep today's answer for a full Session with
	// nothing claimable: no assignment (HTTP 204), not NODE_AT_CAPACITY.
	none, err := leases.ClaimOffer(ctx, full)
	require.NoError(t, err)
	require.Nil(t, none)
}

func insertDispatchClaimSession(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	agentID, ownerID, coreID uuid.UUID,
) runtime.RuntimeSessionPrincipal {
	t.Helper()
	tokenID, nodeID, sessionID, attachmentID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	workerID := "claim-precheck-" + sessionID.String()[:8]
	serial := strings.ReplaceAll(nodeID.String(), "-", "")
	thumbprint := fmt.Sprintf("%x", sha256.Sum256([]byte("claim-precheck/"+nodeID.String())))
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `INSERT INTO agent_tokens (id,agent_id,creator_user_id,name,prefix,token_hash,scopes,status,redeemed_at)
     VALUES ($1,$2,$3,'claim pre-check',$4,$4,ARRAY['agent:pull']::text[],'active_runtime',clock_timestamp())`,
		tokenID, agentID, ownerID, "ol_agent_"+tokenID.String()[:8])
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO runtime_nodes
    (node_id,display_name,device_certificate_serial,device_public_key_thumbprint,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,capacity,inflight,status,last_seen_at)
    VALUES ($1,'claim pre-check',$2,$3,'claim-precheck-v1',2,$4,$5,$6,1,0,'active',clock_timestamp())`,
		nodeID, serial, thumbprint, runtime.RuntimeContractID, runtime.RuntimeContractDigest, runtime.RuntimeRequiredFeatures())
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO runtime_sessions (
        runtime_session_id, node_id, agent_id, credential_id, worker_id,
        session_epoch, device_certificate_serial, node_version,
        protocol_version, runtime_contract_id, runtime_contract_digest,
        features, capacity, attached_core_instance_id
    ) VALUES ($1, $2, $3, $4, $5, 1, $6, 'claim-precheck-v1', 2, $7, $8, $9, 1, $10)`,
		sessionID, nodeID, agentID, tokenID, workerID, serial,
		runtime.RuntimeContractID, runtime.RuntimeContractDigest, runtime.RuntimeRequiredFeatures(), coreID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO runtime_session_attachments (id, runtime_session_id, core_instance_id, attachment_kind)
     VALUES ($1, $2, $3, 'connected')`, attachmentID, sessionID, coreID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	return runtime.RuntimeSessionPrincipal{
		RuntimeSessionID: sessionID, NodeID: nodeID, AgentID: agentID, CredentialID: tokenID,
		WorkerID: workerID, SessionEpoch: 1, RuntimeContractDigest: runtime.RuntimeContractDigest,
		CoreInstanceID: coreID, AttachmentID: attachmentID, DeviceCertificateSerial: serial,
		DevicePublicKeyThumbprintSHA256: thumbprint, Status: "active",
	}
}
