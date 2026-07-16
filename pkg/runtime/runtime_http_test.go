package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const runtimeTestAttachmentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

func TestRuntimeControllerRegistersLifecycleAndExecutionRoutes(t *testing.T) {
	e := echo.New()
	NewRuntimeHTTPController(RuntimeHTTPDependencies{}).Register(e.Group("/api/v1"))

	routes := make(map[string]bool)
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/agent-runtime/sessions",
		"POST /api/v1/agent-runtime/sessions/:id/heartbeat",
		"POST /api/v1/agent-runtime/sessions/:id/drain",
		"POST /api/v1/agent-runtime/sessions/:id/close",
		"POST /api/v1/agent-runtime/runs/claim",
		"POST /api/v1/agent-runtime/runs/:id/assignment-ack",
		"POST /api/v1/agent-runtime/runs/:id/assignment-reject",
		"POST /api/v1/agent-runtime/runs/:id/lease-renew",
		"POST /api/v1/agent-runtime/runs/:id/events",
		"POST /api/v1/agent-runtime/runs/:id/result",
		"POST /api/v1/agent-runtime/runs/resume",
		"POST /api/v1/agent-runtime/runs/:id/cancel-ack",
		"GET /api/v1/agent-runtime/commands",
		"POST /api/v1/agent-runtime/call-agent",
		"GET /api/v1/agent-runtime/ws",
	} {
		require.True(t, routes[route], route)
	}
}

func TestRuntimeHTTPTransportPolicyAdmissionUsesWireCompatibleSignals(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	controller := fixture.controller()
	controller.dependencies.TransportPolicy = func() RuntimeTransportPolicy {
		policy := CurrentRuntimeTransportPolicy()
		policy.OrderedTransports = []RuntimeTransport{RuntimeTransportWebSocket}
		return policy
	}

	initial := serveRuntimeRaw(
		t, controller, http.MethodPost, "/api/v1/agent-runtime/sessions", `{}`,
	)
	require.Equal(t, http.StatusForbidden, initial.Code)
	var initialError RuntimeError
	require.NoError(t, json.Unmarshal(initial.Body.Bytes(), &initialError))
	require.Equal(t, RuntimeErrorForbidden, initialError.Error.Code)
	require.Equal(t, RuntimeTransportForbiddenSignal, initialError.Error.Message)
	require.Equal(t, 0, fixture.sessions.createCalls)

	for _, endpoint := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1/agent-runtime/sessions/00000000-0000-4000-8000-000000000001/heartbeat"},
		{http.MethodPost, "/api/v1/agent-runtime/sessions/00000000-0000-4000-8000-000000000001/drain"},
		{http.MethodPost, "/api/v1/agent-runtime/sessions/00000000-0000-4000-8000-000000000001/close"},
		{http.MethodPost, "/api/v1/agent-runtime/runs/claim"},
		{http.MethodPost, "/api/v1/agent-runtime/runs/00000000-0000-4000-8000-000000000001/assignment-ack"},
		{http.MethodPost, "/api/v1/agent-runtime/runs/00000000-0000-4000-8000-000000000001/assignment-reject"},
		{http.MethodPost, "/api/v1/agent-runtime/runs/00000000-0000-4000-8000-000000000001/lease-renew"},
		{http.MethodPost, "/api/v1/agent-runtime/runs/00000000-0000-4000-8000-000000000001/events"},
		{http.MethodPost, "/api/v1/agent-runtime/runs/00000000-0000-4000-8000-000000000001/result"},
		{http.MethodPost, "/api/v1/agent-runtime/runs/resume"},
		{http.MethodPost, "/api/v1/agent-runtime/runs/00000000-0000-4000-8000-000000000001/cancel-ack"},
		{http.MethodGet, "/api/v1/agent-runtime/commands"},
	} {
		t.Run(endpoint.method+" "+endpoint.path, func(t *testing.T) {
			established := serveRuntimeRaw(t, controller, endpoint.method, endpoint.path, `{}`)
			require.Equal(t, http.StatusForbidden, established.Code)
			var establishedError RuntimeError
			require.NoError(t, json.Unmarshal(established.Body.Bytes(), &establishedError))
			require.Equal(t, RuntimeErrorForbidden, establishedError.Error.Code)
			require.Equal(t, RuntimePolicyChangedSignal, establishedError.Error.Message)
		})
	}
	require.Equal(t, 0, fixture.leases.claimCalls)
}

func TestRuntimeHTTPTransportPolicyAdmissionPreservesAuthenticationPrecedence(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	fixture.tokens.err = errors.New("invalid token")
	controller := fixture.controller()
	controller.dependencies.TransportPolicy = func() RuntimeTransportPolicy {
		policy := CurrentRuntimeTransportPolicy()
		policy.OrderedTransports = []RuntimeTransport{RuntimeTransportWebSocket}
		return policy
	}

	response := serveRuntimeRaw(t, controller, http.MethodPost, "/api/v1/agent-runtime/sessions", `{}`)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	requireRuntimeResponseCode(t, response, RuntimeErrorUnauthorized)
	require.Equal(t, 0, fixture.sessions.createCalls)
}

func TestRuntimePullClaimWakeDoesNotWaitForDatabasePollTick(t *testing.T) {
	agentID := uuid.New()
	principal := RuntimeSessionPrincipal{AgentID: agentID}
	wakeHub := NewRuntimeWakeHub()
	firstPoll := make(chan struct{})
	assignment := &RunAssignedPayload{}
	var calls int
	leases := &runtimeLeaseServiceFake{
		claim: func(context.Context, RuntimeSessionPrincipal) (*RunAssignedPayload, error) {
			calls++
			if calls == 1 {
				close(firstPoll)
				return nil, nil
			}
			return assignment, nil
		},
	}
	controller := NewRuntimeHTTPController(RuntimeHTTPDependencies{
		Leases: leases, WakeHub: wakeHub,
	})
	type result struct {
		assignment *RunAssignedPayload
		err        error
	}
	done := make(chan result, 1)
	go func() {
		got, err := controller.claimWithWait(context.Background(), principal, time.Second)
		done <- result{assignment: got, err: err}
	}()
	<-firstPoll
	started := time.Now()
	wakeHub.Wake(agentID)
	got := <-done
	require.NoError(t, got.err)
	require.Same(t, assignment, got.assignment)
	require.Less(t, time.Since(started), 190*time.Millisecond)
}

func TestRuntimeCreateSessionAuthenticatesThenMapsFormalHello(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	var created RuntimeSessionRequest
	fixture.sessions.create = func(_ context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionRequest) (RuntimeSessionState, error) {
		require.Equal(t, fixture.authenticated, principal)
		created = request
		return fixture.sessionState(), nil
	}
	controller := fixture.controller()
	hello := fixture.hello()

	recorder := serveRuntime(t, controller, http.MethodPost, "/api/v1/agent-runtime/sessions", hello)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "runtime-secret", fixture.tokens.plaintext)
	require.Equal(t, []string{runtimeTokenScope}, fixture.tokens.scopes)
	require.Equal(t, 1, fixture.devices.calls)
	require.Equal(t, hello.RuntimeSessionID, created.RuntimeSessionID)
	require.Equal(t, hello.NodeID, created.NodeID)
	require.Equal(t, hello.AgentID, created.AgentID)
	require.Equal(t, hello.WorkerID, created.WorkerID)
	require.Equal(t, hello.SessionEpoch, created.SessionEpoch)
	require.Equal(t, hello.NodeVersion, created.NodeVersion)
	require.Equal(t, int32(hello.Capacity), created.Capacity)
	require.Equal(t, int32(RuntimeProtocolVersion), created.ProtocolVersion)
	require.Equal(t, RuntimeContractID, created.RuntimeContractID)
	require.Equal(t, RuntimeContractDigest, created.RuntimeContractDigest)
	require.Equal(t, hello.Features, created.Features)
	require.Equal(t, RuntimeTransportLongPoll, created.Transport)

	var ready RuntimeReadyPayload
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &ready))
	require.Equal(t, fixture.acting.CoreInstanceID.String(), ready.CoreInstanceID)
	require.Equal(t, fixture.acting.AttachmentID, ready.AttachmentID)
	require.Equal(t, RuntimeRequiredFeatures(), ready.Features)
	require.Equal(t, int64(RuntimeOfferTTLSeconds), ready.OfferTTLSeconds)
	require.Equal(t, int64(RuntimeLeaseTTLSeconds), ready.LeaseTTLSeconds)
	require.Equal(t, fixture.now, ready.DatabaseTime)
}

func TestRuntimePullDrainReturnsOnlyCommittedServerReceipt(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	deadline := fixture.now.Add(2 * time.Minute)
	var captured RuntimeSessionDrainRequest
	fixture.sessions.drain = func(
		_ context.Context,
		principal AuthenticatedRuntimePrincipal,
		request RuntimeSessionDrainRequest,
	) (RuntimeDrainPayload, error) {
		require.Equal(t, fixture.authenticated, principal)
		captured = request
		return RuntimeDrainPayload{
			DeadlineAt: deadline,
			ReasonCode: "SDK_SHUTDOWN",
			Capacity:   0,
			Inflight:   2,
		}, nil
	}
	request := RuntimeDrainPayload{
		DeadlineAt: deadline,
		ReasonCode: "SDK_SHUTDOWN",
		Capacity:   0,
		Inflight:   999,
	}
	response := serveRuntime(
		t, fixture.controller(), http.MethodPost,
		"/api/v1/agent-runtime/sessions/"+fixture.acting.RuntimeSessionID.String()+"/drain",
		request,
	)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, fixture.acting.RuntimeSessionID, captured.RuntimeSessionID)
	require.Equal(t, fixture.acting.AttachmentID, captured.AttachmentID)
	require.Equal(t, int64(999), captured.Payload.Inflight)
	var receipt RuntimeDrainPayload
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.Equal(t, int64(2), receipt.Inflight)
	require.Equal(t, int64(0), receipt.Capacity)
}

func TestRuntimeCreateSessionAcceptsOnlyBoundedFallbackReasonHeader(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	fixture.sessions.create = func(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionRequest) (RuntimeSessionState, error) {
		require.Equal(t, RuntimeTransportReasonWebSocketUnavailable, request.ReportedTransportReason)
		require.Equal(t, CurrentRuntimeTransportPolicy(), request.TransportPolicy)
		return fixture.sessionState(), nil
	}
	controller := fixture.controller()
	body, err := json.Marshal(fixture.hello())
	require.NoError(t, err)

	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/sessions", strings.NewReader(string(body)))
	req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set(RuntimeFallbackReasonHeader, string(RuntimeTransportReasonWebSocketUnavailable))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, fixture.sessions.createCalls)

	tracked := &runtimeTrackedReader{reader: strings.NewReader(string(body))}
	bad := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/sessions", tracked)
	bad.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
	bad.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	bad.Header.Set(RuntimeFallbackReasonHeader, "dial tcp 10.0.0.1:443")
	badRecorder := httptest.NewRecorder()
	e.ServeHTTP(badRecorder, bad)
	require.Equal(t, http.StatusUnprocessableEntity, badRecorder.Code, badRecorder.Body.String())
	require.Equal(t, 0, tracked.reads, "invalid header must fail before request body decoding")
	require.Equal(t, 1, fixture.sessions.createCalls)
	requireRuntimeResponseCode(t, badRecorder, RuntimeErrorValidationFailed)
}

func TestRuntimeFallbackReasonHeaderRejectsAmbiguousValues(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/sessions", nil)
	request.Header.Add(RuntimeFallbackReasonHeader, string(RuntimeTransportReasonExplicit))
	request.Header.Add(RuntimeFallbackReasonHeader, string(RuntimeTransportReasonRecovery))
	reason, err := runtimeFallbackReasonFromRequest(request)
	require.Empty(t, reason)
	require.NotNil(t, err)
	require.Equal(t, RuntimeErrorValidationFailed, err.Body.Code)

	request = httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/sessions", nil)
	request.Header.Set(RuntimeFallbackReasonHeader, " explicit")
	reason, err = runtimeFallbackReasonFromRequest(request)
	require.Empty(t, reason)
	require.NotNil(t, err)
}

func TestRuntimePreviousGenerationUsesCanonicalHTTPAndAttachmentFence(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	fixture.acting.RuntimeContractDigest = runtimePreviousContractDigest
	fixture.sessions.resolveResponse = fixture.acting
	fixture.sessions.workerResolveResponse = fixture.acting
	fixture.sessions.create = func(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionRequest) (RuntimeSessionState, error) {
		require.Equal(t, runtimePreviousContractDigest, request.RuntimeContractDigest)
		return fixture.sessionState(), nil
	}
	fixture.sessions.heartbeat = func(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error) {
		require.Equal(t, fixture.acting.AttachmentID, request.AttachmentID, "Server must resolve the previous attachment from database truth")
		return fixture.sessionState(), nil
	}
	hello := fixture.hello()
	hello.ContractDigest = runtimePreviousContractDigest
	hello.Features = runtimeRequiredFeaturesForDigest(runtimePreviousContractDigest)
	controller := fixture.controller()

	created := serveRuntime(t, controller, http.MethodPost, "/api/v1/agent-runtime/sessions", hello)
	require.Equal(t, http.StatusOK, created.Code, created.Body.String())
	var ready map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &ready))
	require.Contains(t, ready, "attachment_id")
	require.ElementsMatch(t, []string{"core_instance_id", "attachment_id", "features", "offer_ttl_seconds", "lease_ttl_seconds", "database_time"}, mapKeys(ready))
	var readyFeatures []string
	require.NoError(t, json.Unmarshal(ready["features"], &readyFeatures))
	require.ElementsMatch(t, runtimeRequiredFeaturesForDigest(runtimePreviousContractDigest), readyFeatures)
	require.NotContains(t, readyFeatures, "session_drain")

	heartbeat := serveRuntimeWithoutAttachment(t, controller, http.MethodPost,
		"/api/v1/agent-runtime/sessions/"+hello.RuntimeSessionID.String()+"/heartbeat", hello)
	require.Equal(t, http.StatusUnprocessableEntity, heartbeat.Code, heartbeat.Body.String())
	require.Equal(t, hello.RuntimeSessionID, fixture.sessions.resolvedSessionID)

	current := newRuntimeHandlerFixture()
	current.sessions.heartbeat = func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error) {
		return current.sessionState(), nil
	}
	currentMissing := serveRuntimeWithoutAttachment(t, current.controller(), http.MethodPost,
		"/api/v1/agent-runtime/sessions/"+current.acting.RuntimeSessionID.String()+"/heartbeat", current.hello())
	require.Equal(t, http.StatusUnprocessableEntity, currentMissing.Code, currentMissing.Body.String())
}

func TestRuntimePreviousGenerationStaleReplayConflictIsStableOverHTTP(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	fixture.acting.RuntimeContractDigest = runtimePreviousContractDigest
	fixture.sessions.create = func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionRequest) (RuntimeSessionState, error) {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	hello := fixture.hello()
	hello.ContractDigest = runtimePreviousContractDigest
	hello.Features = runtimeRequiredFeaturesForDigest(runtimePreviousContractDigest)

	response := serveRuntime(t, fixture.controller(), http.MethodPost, "/api/v1/agent-runtime/sessions", hello)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	requireRuntimeResponseCode(t, response, RuntimeErrorSessionConflict)
}

func TestRuntimeAuthenticationRunsBeforeBodyDecode(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	fixture.tokens.err = errors.New("revoked")
	tracked := &runtimeTrackedReader{reader: strings.NewReader(`{"unknown":true}`)}
	e := echo.New()
	fixture.controller().Register(e.Group("/api/v1"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/sessions", tracked)
	req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Equal(t, 0, tracked.reads)
	require.Equal(t, 0, fixture.devices.calls)
	require.Equal(t, 0, fixture.sessions.createCalls)
	requireRuntimeResponseCode(t, recorder, RuntimeErrorUnauthorized)
}

func TestRuntimeSessionHeartbeatAndClose(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	var heartbeat RuntimeSessionHeartbeatRequest
	fixture.sessions.heartbeat = func(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error) {
		heartbeat = request
		return fixture.sessionState(), nil
	}
	var closeRequest RuntimeSessionCloseRequest
	fixture.sessions.close = func(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionCloseRequest) (RuntimeSessionState, error) {
		closeRequest = request
		return fixture.sessionState(), nil
	}
	controller := fixture.controller()
	hello := fixture.hello()

	heartbeatRecorder := serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/sessions/"+hello.RuntimeSessionID.String()+"/heartbeat", hello,
	)
	require.Equal(t, http.StatusOK, heartbeatRecorder.Code, heartbeatRecorder.Body.String())
	require.Equal(t, hello.RuntimeSessionID, heartbeat.RuntimeSessionID)
	require.Equal(t, int32(RuntimeProtocolVersion), heartbeat.ProtocolVersion)
	require.Equal(t, RuntimeTransportLongPoll, heartbeat.Transport)

	closePayload := runtimeSessionClosePayload{
		NodeID:           hello.NodeID,
		AgentID:          hello.AgentID,
		WorkerID:         hello.WorkerID,
		RuntimeSessionID: hello.RuntimeSessionID,
		SessionEpoch:     hello.SessionEpoch,
		Status:           "offline",
		Reason:           "normal shutdown",
	}
	closeRecorder := serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/sessions/"+hello.RuntimeSessionID.String()+"/close", closePayload,
	)
	require.Equal(t, http.StatusNoContent, closeRecorder.Code, closeRecorder.Body.String())
	require.Empty(t, closeRecorder.Body.String())
	require.Equal(t, "offline", closeRequest.Status)
	require.Equal(t, "normal shutdown", closeRequest.Reason)
}

func TestRuntimeLeaseRoutesUseResolvedSessionAndStableResponses(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	identity := fixture.attemptIdentity()
	now := fixture.now
	fixture.leases.ackResponse = RunAssignmentConfirmedPayload{
		AttemptIdentity: identity,
		AttemptNo:       1,
		LeaseExpiresAt:  now.Add(time.Minute),
	}
	fixture.leases.rejectResponse = RunAssignmentRejectedPayload{
		AttemptIdentity: identity,
		Outcome:         RuntimeOfferRejected,
		DispatchState:   RuntimeDispatchPending,
	}
	fixture.leases.renewResponse = RunLeaseRenewedPayload{
		AttemptIdentity: identity,
		LeaseExpiresAt:  now.Add(time.Minute),
	}
	controller := fixture.controller()

	claim := RuntimeClaimRequest{RuntimeSessionID: fixture.acting.RuntimeSessionID, Capacity: 2, Inflight: 1}
	claimRecorder := serveRuntime(t, controller, http.MethodPost, "/api/v1/agent-runtime/runs/claim?wait=0", claim)
	require.Equal(t, http.StatusNoContent, claimRecorder.Code, claimRecorder.Body.String())
	require.Empty(t, claimRecorder.Body.String())
	require.Equal(t, 1, fixture.leases.claimCalls)
	fixture.leases.claimResponse = &RunAssignedPayload{
		AttemptIdentity:      identity,
		OfferNo:              1,
		OfferExpiresAt:       now.Add(30 * time.Second),
		AttemptDeadlineAt:    now.Add(10 * time.Minute),
		RunDeadlineAt:        now.Add(20 * time.Minute),
		Input:                map[string]any{"task": "test"},
		NodeEnvelope:         "node-envelope",
		AgentInvocationToken: "invocation-token",
	}
	claimRecorder = serveRuntime(t, controller, http.MethodPost, "/api/v1/agent-runtime/runs/claim", claim)
	require.Equal(t, http.StatusOK, claimRecorder.Code, claimRecorder.Body.String())
	require.Equal(t, 2, fixture.leases.claimCalls)

	ackRecorder := serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/runs/"+identity.RunID.String()+"/assignment-ack",
		RunAssignmentAckPayload{AttemptIdentity: identity},
	)
	require.Equal(t, http.StatusOK, ackRecorder.Code, ackRecorder.Body.String())
	require.Equal(t, 1, fixture.leases.ackCalls)

	rejectRecorder := serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/runs/"+identity.RunID.String()+"/assignment-reject",
		RunAssignmentRejectPayload{
			AttemptIdentity: identity,
			ReasonCode:      RuntimeRejectNodeAtCapacity,
			Capacity:        2,
			Inflight:        2,
		},
	)
	require.Equal(t, http.StatusOK, rejectRecorder.Code, rejectRecorder.Body.String())
	require.Equal(t, 1, fixture.leases.rejectCalls)

	renewRecorder := serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/runs/"+identity.RunID.String()+"/lease-renew",
		RunLeaseRenewPayload{
			AttemptIdentity:    identity,
			LastClientEventSeq: 0,
			Capacity:           2,
			Inflight:           1,
		},
	)
	require.Equal(t, http.StatusOK, renewRecorder.Code, renewRecorder.Body.String())
	require.Equal(t, 1, fixture.leases.renewCalls)
	require.Equal(t, fixture.acting.RuntimeSessionID, fixture.sessions.resolvedSessionID)
	require.Equal(t, fixture.acting.RuntimeSessionID, fixture.leases.lastPrincipal.RuntimeSessionID)
}

func TestRuntimeEventAndResultResolveActingSessionByWorker(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	identity := fixture.attemptIdentity()
	sourceSessionID := uuid.New()
	require.NotEqual(t, fixture.acting.RuntimeSessionID, sourceSessionID)
	identity.RuntimeSessionID = sourceSessionID
	controller := fixture.controller()

	eventID := uuid.New()
	fixture.events.ack = RuntimeEventAck{
		ClientEventID:  eventID,
		ClientEventSeq: 1,
		Sequence:       7,
		Replayed:       true,
	}
	eventRecorder := serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/runs/"+identity.RunID.String()+"/events",
		RunEventPayload{
			AttemptIdentity: identity,
			ClientEventID:   eventID,
			ClientEventSeq:  1,
			EventType:       "progress.updated",
			Payload:         map[string]any{"percent": 50},
		},
	)
	require.Equal(t, http.StatusOK, eventRecorder.Code, eventRecorder.Body.String())
	require.Equal(t, identity.WorkerID, fixture.sessions.resolvedWorkerID)
	require.NotNil(t, fixture.events.principal.RuntimeSessionID)
	require.Equal(t, fixture.acting.RuntimeSessionID, *fixture.events.principal.RuntimeSessionID)
	require.NotNil(t, fixture.events.identity.RuntimeSessionID)
	require.Equal(t, sourceSessionID, *fixture.events.identity.RuntimeSessionID)
	var eventAck RunEventAckPayload
	require.NoError(t, json.Unmarshal(eventRecorder.Body.Bytes(), &eventAck))
	require.Equal(t, int64(7), eventAck.Sequence)
	require.True(t, eventAck.Replayed)

	resultID := uuid.New()
	fixture.finalizer.ack = RuntimeResultAck{
		ResultID:       resultID,
		Classification: RuntimeResultClassificationSuccess,
		RunStatus:      string(RuntimeRunSuccess),
		DispatchState:  string(RuntimeDispatchTerminal),
		Replayed:       true,
	}
	resultRecorder := serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/runs/"+identity.RunID.String()+"/result",
		RunResultPayload{
			AttemptIdentity:     identity,
			ResultID:            resultID,
			Status:              "success",
			Output:              map[string]any{"answer": "ok"},
			DurationMS:          12,
			FinalClientEventSeq: 1,
		},
	)
	require.Equal(t, http.StatusOK, resultRecorder.Code, resultRecorder.Body.String())
	require.Equal(t, identity.WorkerID, fixture.sessions.resolvedWorkerID)
	require.NotNil(t, fixture.finalizer.principal.RuntimeSessionID)
	require.Equal(t, fixture.acting.RuntimeSessionID, *fixture.finalizer.principal.RuntimeSessionID)
	require.NotNil(t, fixture.finalizer.request.AttemptIdentity.RuntimeSessionID)
	require.Equal(t, sourceSessionID, *fixture.finalizer.request.AttemptIdentity.RuntimeSessionID)
	var resultAck RunResultAckPayload
	require.NoError(t, json.Unmarshal(resultRecorder.Body.Bytes(), &resultAck))
	require.Equal(t, resultID, resultAck.ResultID)
	require.True(t, resultAck.Replayed)
}

func TestRuntimeResumeUsesAuthenticatedTargetSession(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	identity := fixture.attemptIdentity()
	sourceSessionID := uuid.New()
	identity.RuntimeSessionID = sourceSessionID
	request := RuntimeResumePayload{
		NodeID:           fixture.acting.NodeID,
		AgentID:          fixture.acting.AgentID,
		WorkerID:         fixture.acting.WorkerID,
		RuntimeSessionID: fixture.acting.RuntimeSessionID,
		Attempts: []ResumeAttempt{{
			AttemptIdentity:          identity,
			LastAckedClientEventSeq:  0,
			PendingClientEventRanges: []EventRange{{Start: 1, End: 1}},
		}},
	}
	fixture.resume.response = RuntimeResumeResponse{Decisions: []RunResumeAcceptedPayload{{
		AttemptIdentity: identity,
		Decision:        RuntimeResumeUploadSpoolOnly,
		AllowedActions:  []RuntimeResumeAction{RuntimeActionUploadEvents},
	}}}

	recorder := serveRuntime(
		t, fixture.controller(), http.MethodPost,
		"/api/v1/agent-runtime/runs/resume", request,
	)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, fixture.acting.RuntimeSessionID, fixture.sessions.resolvedSessionID)
	require.Equal(t, fixture.acting.RuntimeSessionID, fixture.resume.principal.RuntimeSessionID)
	require.Equal(t, sourceSessionID, fixture.resume.payload.Attempts[0].AttemptIdentity.RuntimeSessionID)
	var response RuntimeResumeResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, fixture.resume.response, response)

	request.NodeID = uuid.New()
	request.Attempts[0].AttemptIdentity.NodeID = request.NodeID
	recorder = serveRuntime(
		t, fixture.controller(), http.MethodPost,
		"/api/v1/agent-runtime/runs/resume", request,
	)
	require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
	requireRuntimeResponseCode(t, recorder, RuntimeErrorPermissionDenied)
	require.Equal(t, 1, fixture.resume.calls)
}

func TestRuntimeCallAgentPreservesProofBodyAndAuthenticatesDeviceBeforeService(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	childRunID := uuid.New()
	fixture.delegation.summary = RunSummary{
		RunID: childRunID, Status: RuntimeRunRunning, DispatchState: RuntimeDispatchPending,
	}
	body := `{"target_agent_id":"` + uuid.NewString() + `","input":{"q":"delegate"}}`
	e := echo.New()
	fixture.controller().Register(e.Group("/api/v1"))
	req := httptest.NewRequest(http.MethodPost, runtimeCallAgentPath, strings.NewReader(body))
	req.Header.Set(echo.HeaderAuthorization, "Bearer invocation-token")
	req.Header.Set("Idempotency-Key", "delegate-once")
	req.Header.Set("OpenLinker-Invocation-Context", "node-envelope")
	req.Header.Set("OpenLinker-Invocation-Proof", "request-proof")
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, fixture.devices.calls)
	require.Equal(t, 1, fixture.delegation.calls)
	authorization := fixture.delegation.authorization
	require.Equal(t, fixture.authenticated.Device, authorization.Device)
	require.Equal(t, "invocation-token", authorization.InvocationToken)
	require.Equal(t, "node-envelope", authorization.InvocationContext)
	require.Equal(t, "request-proof", authorization.InvocationProof)
	require.Equal(t, "delegate-once", authorization.IdempotencyKey)
	require.Equal(t, http.MethodPost, authorization.ProofRequest.Method)
	require.Equal(t, runtimeCallAgentPath, authorization.ProofRequest.Path)
	require.Equal(t, []byte(body), authorization.ProofRequest.Body)
	var summary RunSummary
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &summary))
	require.Equal(t, childRunID, summary.RunID)

	fixture.devices.err = errors.New("revoked device")
	tracked := &runtimeTrackedReader{reader: strings.NewReader(body)}
	req = httptest.NewRequest(http.MethodPost, runtimeCallAgentPath, tracked)
	req.Header.Set(echo.HeaderAuthorization, "Bearer invocation-token")
	recorder = httptest.NewRecorder()
	e.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Equal(t, 0, tracked.reads)
	require.Equal(t, 1, fixture.delegation.calls)
}

func TestRuntimeCommandsBindExplicitSessionAndCancelAck(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	identity := fixture.attemptIdentity()
	cancellationID := uuid.New()
	cancel := RunCancelPayload{
		CancellationID:  cancellationID,
		AttemptIdentity: identity,
		ReasonCode:      runtimeCancellationReasonCode,
		DeadlineAt:      fixture.now.Add(30 * time.Second),
	}
	rawCancel, err := json.Marshal(cancel)
	require.NoError(t, err)
	fixture.cancellations.response = RuntimeCommandsResponse{
		Commands:     []PendingCommand{{Type: RuntimeMessageRunCancel, Payload: rawCancel}},
		DatabaseTime: fixture.now,
	}

	target := "/api/v1/agent-runtime/commands?runtime_session_id=" +
		fixture.acting.RuntimeSessionID.String() + "&wait=0"
	recorder := serveRuntimeRaw(t, fixture.controller(), http.MethodGet, target, "")
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, fixture.acting.RuntimeSessionID, fixture.sessions.resolvedSessionID)
	require.Equal(t, fixture.acting.RuntimeSessionID, fixture.cancellations.principal.RuntimeSessionID)
	require.Equal(t, 1, fixture.cancellations.pollCalls)
	var commands RuntimeCommandsResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &commands))
	require.Equal(t, fixture.cancellations.response, commands)

	fixture.cancellations.state = RunCancellationState{
		CancellationID: cancellationID,
		CancelState:    RuntimeCancelStopped,
		UpdatedAt:      fixture.now,
	}
	ack := RunCancelAckPayload{
		CancellationID:  cancellationID,
		AttemptIdentity: identity,
		CancelState:     RuntimeCancelStopped,
	}
	recorder = serveRuntime(
		t, fixture.controller(), http.MethodPost,
		"/api/v1/agent-runtime/runs/"+identity.RunID.String()+"/cancel-ack", ack,
	)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, ack, fixture.cancellations.ackPayload)
	require.Equal(t, fixture.acting.RuntimeSessionID, fixture.cancellations.principal.RuntimeSessionID)

	for _, invalidTarget := range []string{
		"/api/v1/agent-runtime/commands",
		"/api/v1/agent-runtime/commands?runtime_session_id=" + strings.ToUpper(fixture.acting.RuntimeSessionID.String()),
		"/api/v1/agent-runtime/commands?runtime_session_id=" + fixture.acting.RuntimeSessionID.String() + "&unknown=1",
		"/api/v1/agent-runtime/commands?runtime_session_id=" + fixture.acting.RuntimeSessionID.String() + "&wait=0&wait=1",
	} {
		recorder = serveRuntimeRaw(t, fixture.controller(), http.MethodGet, invalidTarget, "")
		require.Equal(t, http.StatusUnprocessableEntity, recorder.Code, invalidTarget)
		requireRuntimeResponseCode(t, recorder, RuntimeErrorValidationFailed)
	}
	require.Equal(t, 1, fixture.cancellations.pollCalls)
}

func TestRuntimeRejectsNonCanonicalAndConflictingIDsBeforeMutation(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	identity := fixture.attemptIdentity()
	controller := fixture.controller()

	nonCanonical := strings.ToUpper(identity.RunID.String())
	recorder := serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/runs/"+nonCanonical+"/assignment-ack",
		RunAssignmentAckPayload{AttemptIdentity: identity},
	)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	requireRuntimeResponseCode(t, recorder, RuntimeErrorValidationFailed)
	require.Equal(t, 0, fixture.leases.ackCalls)

	conflicting := identity
	conflicting.RunID = uuid.New()
	recorder = serveRuntime(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/runs/"+identity.RunID.String()+"/assignment-ack",
		RunAssignmentAckPayload{AttemptIdentity: conflicting},
	)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	requireRuntimeResponseCode(t, recorder, RuntimeErrorValidationFailed)
	require.Equal(t, 0, fixture.leases.ackCalls)

	body, err := json.Marshal(RunAssignmentAckPayload{AttemptIdentity: identity})
	require.NoError(t, err)
	unknown := strings.TrimSuffix(string(body), "}") + `,"extra":true}`
	recorder = serveRuntimeRaw(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/runs/"+identity.RunID.String()+"/assignment-ack", unknown,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	requireRuntimeResponseCode(t, recorder, RuntimeErrorValidationFailed)
	require.Equal(t, 0, fixture.leases.ackCalls)
}

func TestRuntimePullAttachmentHeaderIsCanonicalAndGenerationBound(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	controller := fixture.controller()
	claim := RuntimeClaimRequest{RuntimeSessionID: fixture.acting.RuntimeSessionID, Capacity: 1, Inflight: 0}
	body, err := json.Marshal(claim)
	require.NoError(t, err)

	serve := func(headers ...string) *httptest.ResponseRecorder {
		e := echo.New()
		controller.Register(e.Group("/api/v1"))
		req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/runs/claim", strings.NewReader(string(body)))
		req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		for _, header := range headers {
			req.Header.Add(RuntimeAttachmentIDHeader, header)
		}
		recorder := httptest.NewRecorder()
		e.ServeHTTP(recorder, req)
		return recorder
	}

	for _, header := range []string{"", " " + runtimeTestAttachmentID, strings.ToUpper(runtimeTestAttachmentID)} {
		recorder := serve(header)
		require.Equal(t, http.StatusUnprocessableEntity, recorder.Code, recorder.Body.String())
		requireRuntimeResponseCode(t, recorder, RuntimeErrorValidationFailed)
	}
	require.Zero(t, fixture.leases.claimCalls)

	recorder := serve(uuid.NewString())
	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
	requireRuntimeResponseCode(t, recorder, RuntimeErrorSessionConflict)
	require.Zero(t, fixture.leases.claimCalls)

	recorder = serve(runtimeTestAttachmentID, uuid.NewString())
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code, recorder.Body.String())
	requireRuntimeResponseCode(t, recorder, RuntimeErrorValidationFailed)
	require.Zero(t, fixture.leases.claimCalls)

	recorder = serve(runtimeTestAttachmentID)
	require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, fixture.leases.claimCalls)
}

func TestRuntimeWaitAndMissingDependenciesFailClosed(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	claim := RuntimeClaimRequest{RuntimeSessionID: fixture.acting.RuntimeSessionID, Capacity: 1, Inflight: 0}
	recorder := serveRuntime(
		t, fixture.controller(), http.MethodPost,
		"/api/v1/agent-runtime/runs/claim?wait=31", claim,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	require.Equal(t, 0, fixture.leases.claimCalls)

	controller := NewRuntimeHTTPController(RuntimeHTTPDependencies{
		TokenValidator:      fixture.tokens,
		DeviceAuthenticator: fixture.devices,
	})
	recorder = serveRuntime(t, controller, http.MethodPost, "/api/v1/agent-runtime/runs/claim", claim)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	requireRuntimeResponseCode(t, recorder, RuntimeErrorServiceUnavailable)
}

func TestMapRuntimeHTTPErrorUsesStableSessionLeaseAndStoreCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code RuntimeErrorCode
	}{
		{name: "inactive principal", err: newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil), code: RuntimeErrorUnauthorized},
		{name: "session conflict", err: newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil), code: RuntimeErrorSessionConflict},
		{name: "contract", err: newRuntimeSessionError(RuntimeSessionErrorContractMismatch, nil), code: RuntimeErrorClientUpgradeRequired},
		{name: "lease identity", err: newRuntimeLeaseError(RuntimeLeaseErrorIdentityMismatch, nil), code: RuntimeErrorLeaseIdentityMismatch},
		{name: "lease expired", err: newRuntimeLeaseError(RuntimeLeaseErrorLeaseExpired, nil), code: RuntimeErrorLeaseExpired},
		{name: "event conflict", err: newRuntimeEventError(RuntimeEventErrorIDConflict, nil), code: RuntimeErrorEventIDConflict},
		{name: "idempotency reuse", err: httpx.NewError(http.StatusConflict, httpx.ErrorCode(RuntimeErrorIdempotencyKeyReused), "hidden"), code: RuntimeErrorIdempotencyKeyReused},
		{name: "cancel not found", err: ErrRuntimeCancellationNotFound, code: RuntimeErrorNotFound},
		{name: "cancel ended", err: ErrRuntimeCancellationRunEnded, code: RuntimeErrorRunAlreadyTerminal},
		{name: "unknown", err: errors.New("database detail must stay hidden"), code: RuntimeErrorInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapped := mapRuntimeHTTPError(test.err)
			require.Equal(t, test.code, mapped.Body.Code)
			require.Equal(t, runtimeErrorDefaultMessage(test.code), mapped.Body.Message)
		})
	}
}

type runtimeHandlerFixture struct {
	now           time.Time
	authenticated AuthenticatedRuntimePrincipal
	acting        RuntimeSessionPrincipal
	tokens        *runtimeTokenValidatorFake
	devices       *runtimeDeviceAuthenticatorFake
	sessions      *runtimeSessionServiceFake
	leases        *runtimeLeaseServiceFake
	events        *runtimeEventStoreFake
	finalizer     *runtimeResultFinalizerFake
	resume        *runtimeResumeServiceFake
	delegation    *runtimeDelegationServiceFake
	cancellations *runtimeCancellationServiceFake
}

func newRuntimeHandlerFixture() *runtimeHandlerFixture {
	now := time.Date(2026, 7, 11, 8, 9, 10, 0, time.UTC)
	authenticated := AuthenticatedRuntimePrincipal{
		AgentID:      uuid.New(),
		CredentialID: uuid.New(),
		Device: RuntimeDeviceIdentity{
			NodeID:                       uuid.New(),
			CertificateSerial:            "abc",
			CertificateFingerprintSHA256: strings.Repeat("a", 64),
			PublicKeyThumbprintSHA256:    strings.Repeat("b", 64),
		},
	}
	acting := RuntimeSessionPrincipal{
		RuntimeSessionID:                uuid.New(),
		NodeID:                          authenticated.Device.NodeID,
		AgentID:                         authenticated.AgentID,
		CredentialID:                    authenticated.CredentialID,
		WorkerID:                        "worker-installation-1",
		SessionEpoch:                    7,
		RuntimeContractDigest:           RuntimeContractDigest,
		CoreInstanceID:                  uuid.New(),
		AttachmentID:                    uuid.MustParse(runtimeTestAttachmentID),
		DeviceCertificateSerial:         authenticated.Device.CertificateSerial,
		DevicePublicKeyThumbprintSHA256: authenticated.Device.PublicKeyThumbprintSHA256,
		Status:                          "active",
		DatabaseTime:                    now,
	}
	fixture := &runtimeHandlerFixture{
		now:           now,
		authenticated: authenticated,
		acting:        acting,
		tokens: &runtimeTokenValidatorFake{token: db.AgentRuntimeToken{
			ID: authenticated.CredentialID, AgentID: authenticated.AgentID,
		}},
		devices:    &runtimeDeviceAuthenticatorFake{device: authenticated.Device},
		sessions:   &runtimeSessionServiceFake{},
		leases:     &runtimeLeaseServiceFake{},
		events:     &runtimeEventStoreFake{},
		finalizer:  &runtimeResultFinalizerFake{},
		resume:     &runtimeResumeServiceFake{},
		delegation: &runtimeDelegationServiceFake{},
		cancellations: &runtimeCancellationServiceFake{
			databaseTime: now,
		},
	}
	fixture.sessions.resolveResponse = acting
	fixture.sessions.workerResolveResponse = acting
	return fixture
}

func (f *runtimeHandlerFixture) controller() *RuntimeHTTPController {
	return NewRuntimeHTTPController(RuntimeHTTPDependencies{
		TokenValidator:      f.tokens,
		DeviceAuthenticator: f.devices,
		Sessions:            f.sessions,
		Leases:              f.leases,
		EventProjector:      f.events,
		Finalizer:           f.finalizer,
		Resume:              f.resume,
		Delegation:          f.delegation,
		Cancellations:       f.cancellations,
	})
}

func (f *runtimeHandlerFixture) hello() RuntimeHelloPayload {
	return RuntimeHelloPayload{
		NodeID:           f.authenticated.Device.NodeID,
		AgentID:          f.authenticated.AgentID,
		WorkerID:         f.acting.WorkerID,
		RuntimeSessionID: f.acting.RuntimeSessionID,
		SessionEpoch:     f.acting.SessionEpoch,
		NodeVersion:      "2.0.0",
		Capacity:         2,
		Features:         RuntimeRequiredFeatures(),
		ContractDigest:   RuntimeContractDigest,
	}
}

func (f *runtimeHandlerFixture) sessionState() RuntimeSessionState {
	coreID := f.acting.CoreInstanceID
	attachment := db.RuntimeSessionAttachment{
		ID:               f.acting.AttachmentID,
		RuntimeSessionID: f.acting.RuntimeSessionID,
		CoreInstanceID:   coreID,
		AttachedAt:       f.now,
	}
	return RuntimeSessionState{
		Session: db.RuntimeSession{
			RuntimeSessionID:       f.acting.RuntimeSessionID,
			NodeID:                 f.acting.NodeID,
			AgentID:                f.acting.AgentID,
			CredentialID:           f.acting.CredentialID,
			WorkerID:               f.acting.WorkerID,
			SessionEpoch:           f.acting.SessionEpoch,
			RuntimeContractDigest:  f.acting.RuntimeContractDigest,
			Features:               runtimeRequiredFeaturesForDigest(f.acting.RuntimeContractDigest),
			Status:                 "active",
			AttachedCoreInstanceID: &coreID,
			HeartbeatAt:            f.now,
		},
		Attachment:   &attachment,
		DatabaseTime: f.now,
	}
}

func (f *runtimeHandlerFixture) attemptIdentity() AttemptIdentity {
	return AttemptIdentity{
		RunID:            uuid.New(),
		AttemptID:        uuid.New(),
		LeaseID:          uuid.New(),
		FencingToken:     3,
		NodeID:           f.acting.NodeID,
		AgentID:          f.acting.AgentID,
		WorkerID:         f.acting.WorkerID,
		RuntimeSessionID: f.acting.RuntimeSessionID,
	}
}

func serveRuntime(t *testing.T, controller *RuntimeHTTPController, method, target string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	return serveRuntimeRaw(t, controller, method, target, string(body))
}

func serveRuntimeRaw(t *testing.T, controller *RuntimeHTTPController, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	if strings.SplitN(target, "?", 2)[0] != "/api/v1/agent-runtime/sessions" &&
		strings.SplitN(target, "?", 2)[0] != runtimeCallAgentPath {
		req.Header.Set(RuntimeAttachmentIDHeader, runtimeTestAttachmentID)
	}
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)
	return recorder
}

func serveRuntimeWithoutAttachment(t *testing.T, controller *RuntimeHTTPController, method, target string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	req := httptest.NewRequest(method, target, strings.NewReader(string(body)))
	req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)
	return recorder
}

func mapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func requireRuntimeResponseCode(t *testing.T, recorder *httptest.ResponseRecorder, code RuntimeErrorCode) {
	t.Helper()
	var response RuntimeError
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
	require.Equal(t, code, response.Error.Code)
}

type runtimeTrackedReader struct {
	reader io.Reader
	reads  int
}

func (r *runtimeTrackedReader) Read(target []byte) (int, error) {
	r.reads++
	return r.reader.Read(target)
}

type runtimeTokenValidatorFake struct {
	token     db.AgentRuntimeToken
	err       error
	plaintext string
	scopes    []string
}

func (f *runtimeTokenValidatorFake) ValidateRuntimeToken(_ context.Context, plaintext string, scopes ...string) (db.AgentRuntimeToken, error) {
	f.plaintext = plaintext
	f.scopes = append([]string(nil), scopes...)
	return f.token, f.err
}

type runtimeDeviceAuthenticatorFake struct {
	device RuntimeDeviceIdentity
	err    error
	calls  int
}

func (f *runtimeDeviceAuthenticatorFake) AuthenticateHTTP(context.Context, *http.Request) (RuntimeDeviceIdentity, error) {
	f.calls++
	return f.device, f.err
}

type runtimeSessionServiceFake struct {
	create                func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionRequest) (RuntimeSessionState, error)
	heartbeat             func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error)
	drain                 func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionDrainRequest) (RuntimeDrainPayload, error)
	close                 func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionCloseRequest) (RuntimeSessionState, error)
	createCalls           int
	resolveResponse       RuntimeSessionPrincipal
	resolveErr            error
	workerResolveResponse RuntimeSessionPrincipal
	workerResolveErr      error
	resolvedSessionID     uuid.UUID
	resolvedWorkerID      string
}

func (f *runtimeSessionServiceFake) CreateOrAttachSession(ctx context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionRequest) (RuntimeSessionState, error) {
	f.createCalls++
	if f.create == nil {
		return RuntimeSessionState{}, nil
	}
	return f.create(ctx, principal, request)
}

func (f *runtimeSessionServiceFake) HeartbeatSession(ctx context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error) {
	if f.heartbeat == nil {
		return RuntimeSessionState{}, nil
	}
	return f.heartbeat(ctx, principal, request)
}

func (f *runtimeSessionServiceFake) DrainSession(ctx context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionDrainRequest) (RuntimeDrainPayload, error) {
	if f.drain == nil {
		return request.Payload, nil
	}
	return f.drain(ctx, principal, request)
}

func (f *runtimeSessionServiceFake) CloseSession(ctx context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionCloseRequest) (RuntimeSessionState, error) {
	if f.close == nil {
		return RuntimeSessionState{}, nil
	}
	return f.close(ctx, principal, request)
}

func (f *runtimeSessionServiceFake) ResolveSessionPrincipal(_ context.Context, _ AuthenticatedRuntimePrincipal, sessionID uuid.UUID) (RuntimeSessionPrincipal, error) {
	f.resolvedSessionID = sessionID
	return f.resolveResponse, f.resolveErr
}

func (f *runtimeSessionServiceFake) ResolveWorkerSessionPrincipal(_ context.Context, _ AuthenticatedRuntimePrincipal, workerID string) (RuntimeSessionPrincipal, error) {
	f.resolvedWorkerID = workerID
	return f.workerResolveResponse, f.workerResolveErr
}

type runtimeLeaseServiceFake struct {
	claim          func(context.Context, RuntimeSessionPrincipal) (*RunAssignedPayload, error)
	claimResponse  *RunAssignedPayload
	claimErr       error
	ackResponse    RunAssignmentConfirmedPayload
	ackErr         error
	rejectResponse RunAssignmentRejectedPayload
	rejectErr      error
	renewResponse  RunLeaseRenewedPayload
	renewErr       error
	claimCalls     int
	ackCalls       int
	rejectCalls    int
	renewCalls     int
	lastPrincipal  RuntimeSessionPrincipal
}

func (f *runtimeLeaseServiceFake) ClaimOffer(ctx context.Context, principal RuntimeSessionPrincipal) (*RunAssignedPayload, error) {
	f.claimCalls++
	f.lastPrincipal = principal
	if f.claim != nil {
		return f.claim(ctx, principal)
	}
	return f.claimResponse, f.claimErr
}

func (f *runtimeLeaseServiceFake) AckAssignment(_ context.Context, principal RuntimeSessionPrincipal, _ RunAssignmentAckPayload) (RunAssignmentConfirmedPayload, error) {
	f.ackCalls++
	f.lastPrincipal = principal
	return f.ackResponse, f.ackErr
}

func (f *runtimeLeaseServiceFake) RejectAssignment(_ context.Context, principal RuntimeSessionPrincipal, _ RunAssignmentRejectPayload) (RunAssignmentRejectedPayload, error) {
	f.rejectCalls++
	f.lastPrincipal = principal
	return f.rejectResponse, f.rejectErr
}

func (f *runtimeLeaseServiceFake) RenewLease(_ context.Context, principal RuntimeSessionPrincipal, _ RunLeaseRenewPayload) (RunLeaseRenewedPayload, error) {
	f.renewCalls++
	f.lastPrincipal = principal
	return f.renewResponse, f.renewErr
}

func (f *runtimeLeaseServiceFake) ReleaseUnackedOffer(_ context.Context, principal RuntimeSessionPrincipal, _ ...string) error {
	f.lastPrincipal = principal
	return nil
}

type runtimeEventStoreFake struct {
	ack       RuntimeEventAck
	err       error
	principal RuntimeEventPrincipal
	identity  RuntimeAttemptIdentity
	request   RuntimeEventRequest
}

func (f *runtimeEventStoreFake) AppendRuntimeEvent(_ context.Context, principal RuntimeEventPrincipal, identity RuntimeAttemptIdentity, request RuntimeEventRequest) (RuntimeEventAck, error) {
	f.principal = principal
	f.identity = identity
	f.request = request
	return f.ack, f.err
}

type runtimeResultFinalizerFake struct {
	ack       RuntimeResultAck
	err       error
	principal RuntimeResultPrincipal
	request   RuntimeResultRequest
}

type runtimeResumeServiceFake struct {
	response  RuntimeResumeResponse
	err       error
	principal RuntimeSessionPrincipal
	payload   RuntimeResumePayload
	calls     int
}

type runtimeDelegationServiceFake struct {
	summary       RunSummary
	err           error
	authorization RuntimeDelegationAuthorization
	calls         int
}

type runtimeCancellationServiceFake struct {
	response     RuntimeCommandsResponse
	databaseTime time.Time
	err          error
	state        RunCancellationState
	principal    RuntimeSessionPrincipal
	ackPayload   RunCancelAckPayload
	pollCalls    int
	ackCalls     int
}

func (f *runtimeCancellationServiceFake) NextCommand(
	_ context.Context,
	principal RuntimeSessionPrincipal,
) (*PendingCommand, time.Time, error) {
	f.principal = principal
	if len(f.response.Commands) == 0 {
		return nil, f.databaseTime, f.err
	}
	command := f.response.Commands[0]
	return &command, f.response.DatabaseTime, f.err
}

func (f *runtimeCancellationServiceFake) PollCommands(
	_ context.Context,
	principal RuntimeSessionPrincipal,
) (RuntimeCommandsResponse, error) {
	f.pollCalls++
	f.principal = principal
	return f.response, f.err
}

func (f *runtimeCancellationServiceFake) AckCancel(
	_ context.Context,
	principal RuntimeSessionPrincipal,
	payload RunCancelAckPayload,
) (RunCancellationState, error) {
	f.ackCalls++
	f.principal = principal
	f.ackPayload = payload
	return f.state, f.err
}

func (f *runtimeDelegationServiceFake) CallAgent(
	_ context.Context,
	authorization RuntimeDelegationAuthorization,
) (RunSummary, error) {
	f.calls++
	f.authorization = authorization
	return f.summary, f.err
}

func (f *runtimeResumeServiceFake) Resume(
	_ context.Context,
	principal RuntimeSessionPrincipal,
	payload RuntimeResumePayload,
) (RuntimeResumeResponse, error) {
	f.calls++
	f.principal = principal
	f.payload = payload
	return f.response, f.err
}

func (f *runtimeResultFinalizerFake) Finalize(_ context.Context, principal RuntimeResultPrincipal, request RuntimeResultRequest) (RuntimeResultAck, error) {
	f.principal = principal
	f.request = request
	return f.ack, f.err
}

var (
	_ RuntimeTokenValidator      = (*runtimeTokenValidatorFake)(nil)
	_ RuntimeDeviceAuthenticator = (*runtimeDeviceAuthenticatorFake)(nil)
	_ RuntimeSessionAPI          = (*runtimeSessionServiceFake)(nil)
	_ RuntimeLeaseAPI            = (*runtimeLeaseServiceFake)(nil)
	_ RuntimeEventProjector      = (*runtimeEventStoreFake)(nil)
	_ RuntimeResultFinalizer     = (*runtimeResultFinalizerFake)(nil)
	_ RuntimeResumeAPI           = (*runtimeResumeServiceFake)(nil)
	_ RuntimeDelegationAPI       = (*runtimeDelegationServiceFake)(nil)
	_ RuntimeCancellationAPI     = (*runtimeCancellationServiceFake)(nil)
)
