package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcpbinding"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
)

func TestRuntimeDispatchEligibilityClassMatchesClaimFilter(t *testing.T) {
	required := RuntimeRequiredFeatures()
	require.Empty(t, runtimeDispatchEligibilityClass(nil))
	require.Empty(t, runtimeDispatchEligibilityClass(required))
	require.Empty(t, runtimeDispatchEligibilityClass(append(slices.Clone(required), "unrelated_feature")),
		"features outside the claim filter must not split the default class")

	withMCP := append(slices.Clone(required), mcpbinding.Feature)
	skills := append(slices.Clone(required), skillpackage.Feature, "skill_packages.codex.v1", "skill_packages.claude.v1")
	reordered := slices.Clone(skills)
	slices.Reverse(reordered)
	require.NotEmpty(t, runtimeDispatchEligibilityClass(withMCP))
	require.NotEmpty(t, runtimeDispatchEligibilityClass(skills))
	require.Equal(t, runtimeDispatchEligibilityClass(skills), runtimeDispatchEligibilityClass(reordered))
	require.NotEqual(t, runtimeDispatchEligibilityClass(skills), runtimeDispatchEligibilityClass(withMCP))
}

func TestRuntimeWakeHubDispatchTokensArePerEligibilityClass(t *testing.T) {
	hub := NewRuntimeWakeHub()
	agentID := uuid.New()
	standard, _ := hub.registerWebSocketDispatch(agentID, "")
	mcp, _ := hub.registerWebSocketDispatch(agentID, "mcp")
	require.NotEqual(t, standard, mcp)

	hub.WakeDispatch(agentID)
	require.Len(t, standard, 1, "every class receives a new Run hint")
	require.Len(t, mcp, 1)
	<-standard
	<-mcp

	require.True(t, hub.passDispatchClass(agentID, "mcp"))
	require.Empty(t, standard, "a handed-on token never wakes another class")
	require.Len(t, mcp, 1)
	require.False(t, hub.passDispatchClass(agentID, "mcp"), "class tokens still coalesce")
	<-mcp

	registered, queued := hub.wakeDispatchIfRegistered(agentID)
	require.True(t, registered)
	require.True(t, queued)
	require.Len(t, standard, 1)
	require.Len(t, mcp, 1)
	require.False(t, hub.passDispatchClass(uuid.New(), ""), "an unknown Agent is never allocated")
	require.Len(t, hub.channels, 1)
}

func TestRuntimeWakeHubPassedDispatchTokenNeedsAnotherListener(t *testing.T) {
	hub := NewRuntimeWakeHub()
	agentID := uuid.New()
	token, _ := hub.registerWebSocketDispatch(agentID, "")

	hub.setDispatchListening(agentID, "", false)
	require.False(t, hub.passDispatchClass(agentID, ""), "a parked sole waiter must not hand the hint to itself")
	require.Empty(t, token)

	_, stopListening := hub.listenDispatchClass(agentID, "")
	require.True(t, hub.passDispatchClass(agentID, ""), "a waiting Pull request can act on the hint")
	<-token
	stopListening()
	stopListening()
	require.False(t, hub.passDispatchClass(agentID, ""))

	sibling, _ := hub.registerWebSocketDispatch(agentID, "")
	require.True(t, hub.passDispatchClass(agentID, ""))
	<-sibling
	hub.unregisterWebSocketDispatch(agentID, "", true)
	require.False(t, hub.passDispatchClass(agentID, ""))
	hub.setDispatchListening(agentID, "", true)
	require.True(t, hub.passDispatchClass(agentID, ""))
}

// A Session that cannot take new work must hand the Agent hint to a sibling.
// The idle sibling is kept busy while the hint arrives, so only the full
// Session can consume it; without the hand-off the Run waits for reconciliation.
func TestRuntimeWebSocketIneligibleSessionHandsDispatchTokenToSibling(t *testing.T) {
	full := newRuntimeWSTestFixture()
	idle := newRuntimeWSSiblingFixture(full, nil)
	connectRuntimeWSFixture(t, full)
	require.Eventually(t, func() bool { return full.leases.claimCallCount() == 1 },
		time.Second, 5*time.Millisecond, "attachment probe")
	idleConn := connectRuntimeWSFixture(t, idle)
	require.Eventually(t, func() bool {
		return full.leases.claimCallCount()+idle.leases.claimCallCount() == 2
	}, time.Second, 5*time.Millisecond, "the second attachment probe is taken by either listener")

	full.leases.setClaimError(newRuntimeLeaseError(RuntimeLeaseErrorNodeAtCapacity, nil))
	assignment := idle.assignment()
	idle.leases.setAssignment(&assignment)
	release := holdRuntimeWSMaintenance(t, full, idle)

	fullClaims := full.leases.claimCallCount()
	full.wakeHub.WakeDispatch(full.principal.AgentID)
	require.Eventually(t, func() bool { return full.leases.claimCallCount() == fullClaims+1 },
		time.Second, 5*time.Millisecond, "the full Session took the hint and found no slot")
	close(release)

	assigned := readRuntimeWSEnvelope(t, idleConn)
	require.Equal(t, RuntimeMessageRunAssigned, assigned.Type)
	payload, err := DecodeRuntimeMessagePayload[RunAssignedPayload](assigned, RuntimeMessageRunAssigned)
	require.NoError(t, err)
	require.Equal(t, assignment.AttemptIdentity, payload.AttemptIdentity)
}

// A Session whose features cannot see a Run must not consume the hint of a
// Session that can: each eligibility class receives its own token.
func TestRuntimeWebSocketDispatchWakeReachesEveryEligibilityClass(t *testing.T) {
	standard := newRuntimeWSTestFixture()
	mcp := newRuntimeWSSiblingFixture(standard, append(RuntimeRequiredFeatures(), mcpbinding.Feature))
	connectRuntimeWSFixture(t, standard)
	require.Eventually(t, func() bool { return standard.leases.claimCallCount() == 1 },
		time.Second, 5*time.Millisecond, "standard attachment probe")
	mcpConn := connectRuntimeWSFixture(t, mcp)
	require.Eventually(t, func() bool { return mcp.leases.claimCallCount() == 1 },
		time.Second, 5*time.Millisecond, "MCP attachment probe uses its own class token")

	assignment := mcp.assignment()
	mcp.leases.setAssignment(&assignment)
	release := holdRuntimeWSMaintenance(t, standard, mcp)

	standard.wakeHub.WakeDispatch(standard.principal.AgentID)
	require.Eventually(t, func() bool { return standard.leases.claimCallCount() == 2 },
		time.Second, 5*time.Millisecond, "the standard class probes its own token")
	close(release)

	assigned := readRuntimeWSEnvelope(t, mcpConn)
	require.Equal(t, RuntimeMessageRunAssigned, assigned.Type)
}

func TestRuntimeTransientClaimErrorClassification(t *testing.T) {
	for _, code := range []string{"40001", "40P01", "55P03", "57014"} {
		require.True(t, runtimeTransientClaimError(&pgconn.PgError{Code: code}), code)
	}
	require.True(t, runtimeTransientClaimError(context.DeadlineExceeded))
	for _, err := range []error{
		&pgconn.PgError{Code: "23505"},
		context.Canceled,
		errors.New("unexpected"),
		newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil),
		newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil),
		httpx.NewError(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable, "maintenance"),
	} {
		require.False(t, runtimeTransientClaimError(err), "%v", err)
	}
}

// A deadlock or serialization failure in the claim transaction says nothing
// about the Session. Demand survives and the claim is retried with backoff.
func TestRuntimeWebSocketTransientClaimErrorRetriesWithoutClosing(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	conn := connectRuntimeWSFixture(t, fixture)
	waitRuntimeWSAttachProbe(t, fixture)
	fixture.leases.setClaimError(&pgconn.PgError{Code: "40P01", Message: "deadlock detected"})
	fixture.wakeHub.WakeDispatch(fixture.principal.AgentID)
	require.Eventually(t, func() bool { return fixture.leases.claimCallCount() >= 3 },
		2*time.Second, 5*time.Millisecond, "the failed claim is retried without a new hint")

	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	fixture.leases.setClaimError(nil)
	assigned := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageRunAssigned, assigned.Type)
}

// Continuous hints must not shorten the backoff after a transient failure:
// their demand is coalesced and served when the retry timer fires.
func TestRuntimeWebSocketClaimRetryBackoffHoldsUnderContinuousWakes(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	conn := connectRuntimeWSFixture(t, fixture)
	waitRuntimeWSAttachProbe(t, fixture)
	fixture.leases.setClaimError(&pgconn.PgError{Code: "40001", Message: "could not serialize access"})

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				fixture.wakeHub.WakeDispatch(fixture.principal.AgentID)
				fixture.wakeHub.WakeNodeDispatch(fixture.principal.NodeID)
			}
		}
	}()
	time.Sleep(time.Second)
	close(stop)
	<-done

	times := fixture.leases.claimTimesSnapshot()[1:] // skip the attachment probe
	require.GreaterOrEqual(t, len(times), 2, "the failed claim is retried")
	require.LessOrEqual(t, len(times), 6, "about 500 hints in one second must not drive claims: %d", len(times))
	for i := 1; i < len(times); i++ {
		require.GreaterOrEqual(t, times[i].Sub(times[i-1]), 80*time.Millisecond,
			"retry %d came %s after the previous claim", i, times[i].Sub(times[i-1]))
	}

	// The coalesced demand is served by the next retry once the database recovers.
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	fixture.leases.setClaimError(nil)
	assigned := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageRunAssigned, assigned.Type)
}

// A failed attachment probe drops its own demand for every error class: it
// is not retried on its own, and a later capacity hint does not resume it.
func TestRuntimeWebSocketFailedAttachProbeLeavesNoDemand(t *testing.T) {
	for name, probeErr := range map[string]error{
		"transient":        &pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
		"node_at_capacity": newRuntimeLeaseError(RuntimeLeaseErrorNodeAtCapacity, nil),
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newRuntimeWSTestFixture()
			fixture.leases.setClaimError(probeErr)
			conn := connectRuntimeWSFixture(t, fixture)
			waitRuntimeWSAttachProbe(t, fixture)
			fixture.wakeHub.WakeNodeDispatch(fixture.principal.NodeID)

			require.NoError(t, conn.SetReadDeadline(time.Now().Add(400*time.Millisecond)))
			_, _, err := conn.ReadMessage()
			var netErr interface{ Timeout() bool }
			require.ErrorAs(t, err, &netErr, "the connection must stay open: %v", err)
			require.True(t, netErr.Timeout())
			require.Equal(t, 1, fixture.leases.claimCallCount(), "no retry and no capacity-driven claim")
		})
	}
}

// Demand a real hint established while the probe was in flight survives the
// probe's failure and is served by a normal claim; probe-only demand does not.
func TestRuntimeWebSocketFailedAttachProbeKeepsIndependentDemand(t *testing.T) {
	newConnection := func() *runtimeWSConnection {
		return &runtimeWSConnection{dispatchContinue: make(chan struct{}, 1), dispatchLimit: 1}
	}

	for name, hintFirst := range map[string]bool{"coalesced_before_claim": true, "during_claim": false} {
		independent := newConnection()
		independent.requestAttachProbe()
		if hintFirst {
			independent.requestDispatch(1, true, "run_available") // capped into the probe's credit
		}
		require.True(t, independent.takeDispatchCredit(), name)
		require.True(t, independent.takeAttachProbe(), name)
		if !hintFirst {
			independent.requestDispatch(1, true, "run_available")
		}
		independent.endAttachProbe(true)
		require.True(t, independent.takeDispatchCredit(), "%s: the hint's demand keeps a credit", name)
		require.False(t, independent.takeAttachProbe(), "%s: the follow-up claim is a normal claim", name)
	}

	probeOnly := newConnection()
	probeOnly.requestAttachProbe()
	require.True(t, probeOnly.takeDispatchCredit())
	require.True(t, probeOnly.takeAttachProbe())
	probeOnly.requestDispatch(1, false, "node_capacity") // continues, but does not establish, demand
	probeOnly.endAttachProbe(true)
	require.False(t, probeOnly.takeDispatchCredit(), "probe-only demand is dropped")
	probeOnly.requestDispatch(1, false, "node_capacity")
	require.False(t, probeOnly.takeDispatchCredit(), "capacity alone does not restore it")

	succeeded := newConnection()
	succeeded.requestAttachProbe()
	require.True(t, succeeded.takeDispatchCredit())
	require.True(t, succeeded.takeAttachProbe())
	succeeded.endAttachProbe(false)
	succeeded.requestDispatch(1, false, "assignment_ack")
	require.True(t, succeeded.takeDispatchCredit(), "a successful probe leaves ordinary demand")
}

// hard_maintenance's claim fence is an intentional disconnect and keeps its
// existing close code.
func TestRuntimeWebSocketClaimMaintenanceFenceStillCloses(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	conn := connectRuntimeWSFixture(t, fixture)
	// Ready precedes the attachment probe; let it finish so the fence is seen
	// by the wake's claim rather than absorbed by the probe.
	waitRuntimeWSAttachProbe(t, fixture)
	fixture.leases.setClaimError(httpx.NewError(
		http.StatusServiceUnavailable, httpx.CodeServiceUnavailable,
		"runtime is temporarily unavailable during maintenance",
	))
	fixture.wakeHub.WakeDispatch(fixture.principal.AgentID)
	requireRuntimeWSCloseCode(t, conn, RuntimeWSCloseInternalError)
}

// hard_maintenance still admits reattachment so accepted work can finish. The
// attachment probe must not turn its claim fence into a disconnect, nor leave
// demand behind that a later capacity hint would turn into a fenced claim.
func TestRuntimeWebSocketAttachProbeToleratesMaintenanceFence(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	fixture.leases.setClaimError(httpx.NewError(
		http.StatusServiceUnavailable, httpx.CodeServiceUnavailable,
		"runtime is temporarily unavailable during maintenance",
	))
	conn := connectRuntimeWSFixture(t, fixture)
	require.Eventually(t, func() bool { return fixture.leases.claimCallCount() == 1 },
		time.Second, 5*time.Millisecond, "attachment probe")
	fixture.wakeHub.WakeNodeDispatch(fixture.principal.NodeID)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
	_, _, err := conn.ReadMessage()
	var netErr interface{ Timeout() bool }
	require.ErrorAs(t, err, &netErr, "the connection must stay open: %v", err)
	require.True(t, netErr.Timeout())
	require.Equal(t, 1, fixture.leases.claimCallCount(), "a failed probe leaves no dispatch demand")
}

func TestRuntimePullHandsFailedDispatchTokenToListeningSibling(t *testing.T) {
	principal := RuntimeSessionPrincipal{AgentID: uuid.New(), NodeID: uuid.New()}
	hub := NewRuntimeWakeHub()
	sibling, _ := hub.registerWebSocketDispatch(principal.AgentID, "")
	firstPoll := make(chan struct{})
	var calls int
	controller := NewRuntimeHTTPController(RuntimeHTTPDependencies{
		WakeHub: hub,
		Leases: &runtimeLeaseServiceFake{
			claim: func(context.Context, RuntimeSessionPrincipal) (*RunAssignedPayload, error) {
				calls++
				if calls == 1 {
					close(firstPoll)
					return nil, nil
				}
				return nil, newRuntimeLeaseError(RuntimeLeaseErrorNodeAtCapacity, nil)
			},
		},
	})
	done := make(chan error, 1)
	go func() {
		_, err := controller.claimWithWait(context.Background(), principal, 5*time.Second)
		done <- err
	}()
	<-firstPoll
	hub.WakeDispatch(principal.AgentID)
	select {
	case err := <-done:
		require.True(t, IsRuntimeLeaseError(err, RuntimeLeaseErrorNodeAtCapacity))
	case <-time.After(3 * time.Second):
		t.Fatal("Pull claim did not return")
	}
	require.Len(t, sibling, 1, "the consumed hint is handed to the listening sibling")
}

// newRuntimeWSSiblingFixture builds another Worker Session for base's Agent on
// the same Core process (shared wake hub), optionally with other features.
func newRuntimeWSSiblingFixture(base *runtimeWSTestFixture, features []string) *runtimeWSTestFixture {
	sibling := newRuntimeWSTestFixture()
	agentID := base.principal.AgentID
	sibling.wakeHub = base.wakeHub
	sibling.authenticated.AgentID = agentID
	sibling.principal.AgentID = agentID
	sibling.hello.AgentID = agentID
	sibling.tokens.token.AgentID = agentID
	sibling.sessions.principal.AgentID = agentID
	sibling.sessions.state.Session.AgentID = agentID
	if features != nil {
		sibling.hello.Features = features
		sibling.principal.Features = features
		sibling.sessions.principal.Features = features
	}
	return sibling
}

// The last same-class listener parks on NODE_AT_CAPACITY with its demand kept
// and no token left. If the capacity release is never signalled, the
// heartbeat alone must retry that demand: no capacity wake, no dispatch
// signal and no reconciler are involved.
func TestRuntimeWebSocketHeartbeatResumesParkedDemand(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	conn := connectRuntimeWSFixtureWithHeartbeat(t, fixture, 150*time.Millisecond)
	waitRuntimeWSAttachProbe(t, fixture)

	fixture.leases.setClaimError(newRuntimeLeaseError(RuntimeLeaseErrorNodeAtCapacity, nil))
	fixture.wakeHub.WakeDispatch(fixture.principal.AgentID) // the only signal in this test
	require.Eventually(t, func() bool { return fixture.leases.claimCallCount() >= 2 },
		time.Second, 5*time.Millisecond, "the full Session parked with its demand")

	// Capacity frees up without any hint reaching this Core.
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	fixture.leases.setClaimError(nil)
	assigned := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageRunAssigned, assigned.Type)
	payload, err := DecodeRuntimeMessagePayload[RunAssignedPayload](assigned, RuntimeMessageRunAssigned)
	require.NoError(t, err)
	require.Equal(t, assignment.AttemptIdentity, payload.AttemptIdentity)
}

// A heartbeat retry of parked demand is a background claim. When the cluster
// has entered hard_maintenance meanwhile, its fence must not close a session
// that is still finishing accepted work.
func TestRuntimeWebSocketHeartbeatClaimDuringMaintenanceKeepsAcceptedWork(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	executing := fixture.assignment()
	fixture.leases.setAssignment(&executing)
	fixture.leases.ackResponse = RunAssignmentConfirmedPayload{
		AttemptIdentity: executing.AttemptIdentity,
		AttemptNo:       1,
		LeaseExpiresAt:  fixture.now.Add(time.Minute),
	}
	fixture.finalizer.ack = RuntimeResultAck{
		ResultID:       uuid.New(),
		Classification: RuntimeResultClassificationSuccess,
		RunStatus:      string(RuntimeRunSuccess),
		DispatchState:  string(RuntimeDispatchTerminal),
	}
	// Once the Session accepts its Run it is full: later claims find no slot.
	fixture.leases.setPostAckClaimError(newRuntimeLeaseError(RuntimeLeaseErrorNodeAtCapacity, nil))
	conn := connectRuntimeWSFixtureWithHeartbeat(t, fixture, 150*time.Millisecond)

	assignedEnvelope := readRuntimeWSEnvelope(t, conn) // delivered by the attachment probe
	require.Equal(t, RuntimeMessageRunAssigned, assignedEnvelope.Type)
	ack, err := NewRuntimeTypedMessage(
		RuntimeMessageAssignmentAck,
		&assignedEnvelope.MessageID,
		RunAssignmentAckPayload{AttemptIdentity: executing.AttemptIdentity},
	)
	require.NoError(t, err)
	require.NoError(t, conn.WriteJSON(ack))
	require.Equal(t, RuntimeMessageAssignmentConfirmed, readRuntimeWSEnvelope(t, conn).Type)
	require.Eventually(t, func() bool { return fixture.leases.claimCallCount() >= 2 },
		time.Second, 5*time.Millisecond, "the ACK continuation parked on NODE_AT_CAPACITY")

	// A sibling drained the backlog; the cluster then entered hard_maintenance.
	fixture.leases.setPostAckClaimError(httpx.NewError(
		http.StatusServiceUnavailable, httpx.CodeServiceUnavailable,
		"runtime is temporarily unavailable during maintenance",
	))
	claims := fixture.leases.claimCallCount()
	require.Eventually(t, func() bool { return fixture.leases.claimCallCount() > claims },
		2*time.Second, 5*time.Millisecond, "the heartbeat retried the parked demand")

	// The socket stays open and the accepted Attempt still finalizes.
	result, err := NewRuntimeTypedMessage(RuntimeMessageRunResult, nil, RunResultPayload{
		AttemptIdentity: executing.AttemptIdentity,
		ResultID:        fixture.finalizer.ack.ResultID,
		Status:          "success",
		Output:          map[string]any{"answer": "ok"},
		DurationMS:      25,
	})
	require.NoError(t, err)
	require.NoError(t, conn.WriteJSON(result))
	require.Equal(t, RuntimeMessageRunResultAck, readRuntimeWSEnvelope(t, conn).Type)
}

// A background claim (here a heartbeat resume that finds the Session's own
// unacknowledged offer) must not hand a token to a sibling: the sibling would
// treat it as a real signal, and once hard_maintenance has started its fenced
// claim would close a connection that may be finishing accepted work.
func TestRuntimeWebSocketBackgroundClaimNeverHandsOffToken(t *testing.T) {
	parked := newRuntimeWSTestFixture()
	sibling := newRuntimeWSSiblingFixture(parked, nil)
	offer := parked.assignment()
	parked.leases.setAssignment(&offer) // returned until ACKed, which never happens
	parkedConn := connectRuntimeWSFixtureWithHeartbeat(t, parked, 150*time.Millisecond)
	require.Equal(t, RuntimeMessageRunAssigned, readRuntimeWSEnvelope(t, parkedConn).Type)
	siblingConn := connectRuntimeWSFixture(t, sibling)
	waitRuntimeWSAttachProbe(t, sibling)

	// A real hint reaches the parked Session first: its claim sees its own
	// outstanding offer, so it parks and hands the real hint to the sibling,
	// which finds nothing before maintenance.
	release := holdRuntimeWSMaintenance(t, parked, sibling)
	parkedClaims := parked.leases.claimCallCount()
	parked.wakeHub.WakeDispatch(parked.principal.AgentID)
	require.Eventually(t, func() bool { return parked.leases.claimCallCount() > parkedClaims },
		time.Second, 5*time.Millisecond, "the real hint reached the parked Session")
	siblingClaims := sibling.leases.claimCallCount()
	close(release)
	require.Eventually(t, func() bool { return sibling.leases.claimCallCount() > siblingClaims },
		time.Second, 5*time.Millisecond, "a real hint is still handed off")

	// Keep the sibling from consuming tokens while the parked Session's
	// heartbeat claims again and finds the same unacknowledged offer.
	release = holdRuntimeWSMaintenance(t, parked, sibling)
	parkedClaims = parked.leases.claimCallCount()
	require.Eventually(t, func() bool { return parked.leases.claimCallCount() > parkedClaims },
		2*time.Second, 5*time.Millisecond, "the heartbeat retried the parked demand")

	// hard_maintenance starts before the sibling could consume any token.
	sibling.leases.setClaimError(httpx.NewError(
		http.StatusServiceUnavailable, httpx.CodeServiceUnavailable,
		"runtime is temporarily unavailable during maintenance",
	))
	siblingClaims = sibling.leases.claimCallCount()
	close(release)

	require.NoError(t, siblingConn.SetReadDeadline(time.Now().Add(400*time.Millisecond)))
	_, _, err := siblingConn.ReadMessage()
	var netErr interface{ Timeout() bool }
	require.ErrorAs(t, err, &netErr, "the sibling must stay open: %v", err)
	require.True(t, netErr.Timeout())
	require.Equal(t, siblingClaims, sibling.leases.claimCallCount(), "no token reached the sibling")
}

func connectRuntimeWSFixtureWithHeartbeat(
	t *testing.T,
	fixture *runtimeWSTestFixture,
	heartbeat time.Duration,
) *websocket.Conn {
	t.Helper()
	controller := fixture.controller()
	controller.webSocketHeartbeat = heartbeat
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)
	conn := dialRuntimeWS(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/agent-runtime/ws")
	t.Cleanup(func() { _ = conn.Close() })
	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)
	return conn
}

// waitRuntimeWSAttachProbe waits until the attachment probe's claim has read
// its result, so errors injected afterwards reach a later, ordinary claim.
func waitRuntimeWSAttachProbe(t *testing.T, fixture *runtimeWSTestFixture) {
	t.Helper()
	require.Eventually(t, func() bool { return fixture.leases.claimCallCount() >= 1 },
		time.Second, 5*time.Millisecond, "attachment probe")
}

func connectRuntimeWSFixture(t *testing.T, fixture *runtimeWSTestFixture) *websocket.Conn {
	t.Helper()
	server, target := fixture.server(t)
	t.Cleanup(server.Close)
	conn := dialRuntimeWS(t, target)
	t.Cleanup(func() { _ = conn.Close() })
	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)
	return conn
}

// holdRuntimeWSMaintenance parks busy's maintenance loop inside a control
// command query (after other has processed the same control wake), so the
// next dispatch hint can only be consumed by other. Close the result to resume.
func holdRuntimeWSMaintenance(t *testing.T, other, busy *runtimeWSTestFixture) chan struct{} {
	t.Helper()
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	busy.cancellations.mu.Lock()
	busy.cancellations.nextGate = release
	busy.cancellations.mu.Unlock()
	otherCalls, busyCalls := other.cancellations.nextCallCount(), busy.cancellations.nextCallCount()
	other.wakeHub.WakeControl(other.principal.AgentID)
	require.Eventually(t, func() bool {
		return other.cancellations.nextCallCount() > otherCalls && busy.cancellations.nextCallCount() > busyCalls
	}, time.Second, 5*time.Millisecond)
	return release
}
