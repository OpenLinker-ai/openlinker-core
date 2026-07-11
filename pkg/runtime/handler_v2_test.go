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
)

func TestRuntimeV2ControllerRegistersLifecycleAndExecutionRoutes(t *testing.T) {
	e := echo.New()
	NewRuntimeV2HTTPController(RuntimeV2HTTPDependencies{}).Register(e.Group("/api/v1"))

	routes := make(map[string]bool)
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/agent-runtime/v2/sessions",
		"POST /api/v1/agent-runtime/v2/sessions/:id/heartbeat",
		"POST /api/v1/agent-runtime/v2/sessions/:id/close",
		"POST /api/v1/agent-runtime/v2/runs/claim",
		"POST /api/v1/agent-runtime/v2/runs/:id/assignment-ack",
		"POST /api/v1/agent-runtime/v2/runs/:id/assignment-reject",
		"POST /api/v1/agent-runtime/v2/runs/:id/lease-renew",
		"POST /api/v1/agent-runtime/v2/runs/:id/events",
		"POST /api/v1/agent-runtime/v2/runs/:id/result",
		"GET /api/v1/agent-runtime/v2/ws",
	} {
		require.True(t, routes[route], route)
	}
}

func TestRuntimeV2CreateSessionAuthenticatesThenMapsFormalHello(t *testing.T) {
	fixture := newRuntimeV2HandlerFixture()
	var created RuntimeSessionRequest
	fixture.sessions.create = func(_ context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionRequest) (RuntimeSessionState, error) {
		require.Equal(t, fixture.authenticated, principal)
		created = request
		return fixture.sessionState(), nil
	}
	controller := fixture.controller()
	hello := fixture.hello()

	recorder := serveRuntimeV2(t, controller, http.MethodPost, "/api/v1/agent-runtime/v2/sessions", hello)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "runtime-secret", fixture.tokens.plaintext)
	require.Equal(t, []string{runtimeV2TokenScope}, fixture.tokens.scopes)
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

	var ready RuntimeReadyPayload
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &ready))
	require.Equal(t, fixture.acting.CoreInstanceID.String(), ready.CoreInstanceID)
	require.Equal(t, RuntimeRequiredFeatures(), ready.Features)
	require.Equal(t, int64(RuntimeOfferTTLSeconds), ready.OfferTTLSeconds)
	require.Equal(t, int64(RuntimeLeaseTTLSeconds), ready.LeaseTTLSeconds)
	require.Equal(t, fixture.now, ready.DatabaseTime)
}

func TestRuntimeV2AuthenticationRunsBeforeBodyDecode(t *testing.T) {
	fixture := newRuntimeV2HandlerFixture()
	fixture.tokens.err = errors.New("revoked")
	tracked := &runtimeV2TrackedReader{reader: strings.NewReader(`{"unknown":true}`)}
	e := echo.New()
	fixture.controller().Register(e.Group("/api/v1"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/v2/sessions", tracked)
	req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Equal(t, 0, tracked.reads)
	require.Equal(t, 0, fixture.devices.calls)
	require.Equal(t, 0, fixture.sessions.createCalls)
	requireRuntimeV2ResponseCode(t, recorder, RuntimeErrorUnauthorized)
}

func TestRuntimeV2SessionHeartbeatAndClose(t *testing.T) {
	fixture := newRuntimeV2HandlerFixture()
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

	heartbeatRecorder := serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/sessions/"+hello.RuntimeSessionID.String()+"/heartbeat", hello,
	)
	require.Equal(t, http.StatusOK, heartbeatRecorder.Code, heartbeatRecorder.Body.String())
	require.Equal(t, hello.RuntimeSessionID, heartbeat.RuntimeSessionID)
	require.Equal(t, int32(RuntimeProtocolVersion), heartbeat.ProtocolVersion)

	closePayload := runtimeSessionClosePayload{
		NodeID:           hello.NodeID,
		AgentID:          hello.AgentID,
		WorkerID:         hello.WorkerID,
		RuntimeSessionID: hello.RuntimeSessionID,
		SessionEpoch:     hello.SessionEpoch,
		Status:           "offline",
		Reason:           "normal shutdown",
	}
	closeRecorder := serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/sessions/"+hello.RuntimeSessionID.String()+"/close", closePayload,
	)
	require.Equal(t, http.StatusNoContent, closeRecorder.Code, closeRecorder.Body.String())
	require.Empty(t, closeRecorder.Body.String())
	require.Equal(t, "offline", closeRequest.Status)
	require.Equal(t, "normal shutdown", closeRequest.Reason)
}

func TestRuntimeV2LeaseRoutesUseResolvedSessionAndStableResponses(t *testing.T) {
	fixture := newRuntimeV2HandlerFixture()
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
	claimRecorder := serveRuntimeV2(t, controller, http.MethodPost, "/api/v1/agent-runtime/v2/runs/claim?wait=0", claim)
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
	claimRecorder = serveRuntimeV2(t, controller, http.MethodPost, "/api/v1/agent-runtime/v2/runs/claim", claim)
	require.Equal(t, http.StatusOK, claimRecorder.Code, claimRecorder.Body.String())
	require.Equal(t, 2, fixture.leases.claimCalls)

	ackRecorder := serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/"+identity.RunID.String()+"/assignment-ack",
		RunAssignmentAckPayload{AttemptIdentity: identity},
	)
	require.Equal(t, http.StatusOK, ackRecorder.Code, ackRecorder.Body.String())
	require.Equal(t, 1, fixture.leases.ackCalls)

	rejectRecorder := serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/"+identity.RunID.String()+"/assignment-reject",
		RunAssignmentRejectPayload{
			AttemptIdentity: identity,
			ReasonCode:      RuntimeRejectNodeAtCapacity,
			Capacity:        2,
			Inflight:        2,
		},
	)
	require.Equal(t, http.StatusOK, rejectRecorder.Code, rejectRecorder.Body.String())
	require.Equal(t, 1, fixture.leases.rejectCalls)

	renewRecorder := serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/"+identity.RunID.String()+"/lease-renew",
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

func TestRuntimeV2EventAndResultResolveActingSessionByWorker(t *testing.T) {
	fixture := newRuntimeV2HandlerFixture()
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
	eventRecorder := serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/"+identity.RunID.String()+"/events",
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
	resultRecorder := serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/"+identity.RunID.String()+"/result",
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

func TestRuntimeV2RejectsNonCanonicalAndConflictingIDsBeforeMutation(t *testing.T) {
	fixture := newRuntimeV2HandlerFixture()
	identity := fixture.attemptIdentity()
	controller := fixture.controller()

	nonCanonical := strings.ToUpper(identity.RunID.String())
	recorder := serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/"+nonCanonical+"/assignment-ack",
		RunAssignmentAckPayload{AttemptIdentity: identity},
	)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	requireRuntimeV2ResponseCode(t, recorder, RuntimeErrorValidationFailed)
	require.Equal(t, 0, fixture.leases.ackCalls)

	conflicting := identity
	conflicting.RunID = uuid.New()
	recorder = serveRuntimeV2(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/"+identity.RunID.String()+"/assignment-ack",
		RunAssignmentAckPayload{AttemptIdentity: conflicting},
	)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	requireRuntimeV2ResponseCode(t, recorder, RuntimeErrorValidationFailed)
	require.Equal(t, 0, fixture.leases.ackCalls)

	body, err := json.Marshal(RunAssignmentAckPayload{AttemptIdentity: identity})
	require.NoError(t, err)
	unknown := strings.TrimSuffix(string(body), "}") + `,"extra":true}`
	recorder = serveRuntimeV2Raw(
		t, controller, http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/"+identity.RunID.String()+"/assignment-ack", unknown,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	requireRuntimeV2ResponseCode(t, recorder, RuntimeErrorValidationFailed)
	require.Equal(t, 0, fixture.leases.ackCalls)
}

func TestRuntimeV2WaitAndMissingDependenciesFailClosed(t *testing.T) {
	fixture := newRuntimeV2HandlerFixture()
	claim := RuntimeClaimRequest{RuntimeSessionID: fixture.acting.RuntimeSessionID, Capacity: 1, Inflight: 0}
	recorder := serveRuntimeV2(
		t, fixture.controller(), http.MethodPost,
		"/api/v1/agent-runtime/v2/runs/claim?wait=31", claim,
	)
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	require.Equal(t, 0, fixture.leases.claimCalls)

	controller := NewRuntimeV2HTTPController(RuntimeV2HTTPDependencies{
		TokenValidator:      fixture.tokens,
		DeviceAuthenticator: fixture.devices,
	})
	recorder = serveRuntimeV2(t, controller, http.MethodPost, "/api/v1/agent-runtime/v2/runs/claim", claim)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	requireRuntimeV2ResponseCode(t, recorder, RuntimeErrorServiceUnavailable)
}

func TestMapRuntimeV2HTTPErrorUsesStableSessionLeaseAndStoreCodes(t *testing.T) {
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
		{name: "unknown", err: errors.New("database detail must stay hidden"), code: RuntimeErrorInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapped := mapRuntimeV2HTTPError(test.err)
			require.Equal(t, test.code, mapped.Body.Code)
			require.Equal(t, runtimeErrorDefaultMessage(test.code), mapped.Body.Message)
		})
	}
}

type runtimeV2HandlerFixture struct {
	now           time.Time
	authenticated AuthenticatedRuntimePrincipal
	acting        RuntimeSessionPrincipal
	tokens        *runtimeV2TokenValidatorFake
	devices       *runtimeV2DeviceAuthenticatorFake
	sessions      *runtimeV2SessionServiceFake
	leases        *runtimeV2LeaseServiceFake
	events        *runtimeV2EventStoreFake
	finalizer     *runtimeV2ResultFinalizerFake
}

func newRuntimeV2HandlerFixture() *runtimeV2HandlerFixture {
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
		CoreInstanceID:                  uuid.New(),
		DeviceCertificateSerial:         authenticated.Device.CertificateSerial,
		DevicePublicKeyThumbprintSHA256: authenticated.Device.PublicKeyThumbprintSHA256,
		Status:                          "active",
		DatabaseTime:                    now,
	}
	fixture := &runtimeV2HandlerFixture{
		now:           now,
		authenticated: authenticated,
		acting:        acting,
		tokens: &runtimeV2TokenValidatorFake{token: db.AgentRuntimeToken{
			ID: authenticated.CredentialID, AgentID: authenticated.AgentID,
		}},
		devices:   &runtimeV2DeviceAuthenticatorFake{device: authenticated.Device},
		sessions:  &runtimeV2SessionServiceFake{},
		leases:    &runtimeV2LeaseServiceFake{},
		events:    &runtimeV2EventStoreFake{},
		finalizer: &runtimeV2ResultFinalizerFake{},
	}
	fixture.sessions.resolveResponse = acting
	fixture.sessions.workerResolveResponse = acting
	return fixture
}

func (f *runtimeV2HandlerFixture) controller() *RuntimeV2HTTPController {
	return NewRuntimeV2HTTPController(RuntimeV2HTTPDependencies{
		TokenValidator:      f.tokens,
		DeviceAuthenticator: f.devices,
		Sessions:            f.sessions,
		Leases:              f.leases,
		EventStore:          f.events,
		Finalizer:           f.finalizer,
	})
}

func (f *runtimeV2HandlerFixture) hello() RuntimeHelloPayload {
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

func (f *runtimeV2HandlerFixture) sessionState() RuntimeSessionState {
	coreID := f.acting.CoreInstanceID
	return RuntimeSessionState{
		Session: db.RuntimeSession{
			RuntimeSessionID:       f.acting.RuntimeSessionID,
			NodeID:                 f.acting.NodeID,
			AgentID:                f.acting.AgentID,
			CredentialID:           f.acting.CredentialID,
			WorkerID:               f.acting.WorkerID,
			SessionEpoch:           f.acting.SessionEpoch,
			Features:               RuntimeRequiredFeatures(),
			Status:                 "active",
			AttachedCoreInstanceID: &coreID,
			HeartbeatAt:            f.now,
		},
		DatabaseTime: f.now,
	}
}

func (f *runtimeV2HandlerFixture) attemptIdentity() AttemptIdentity {
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

func serveRuntimeV2(t *testing.T, controller *RuntimeV2HTTPController, method, target string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	return serveRuntimeV2Raw(t, controller, method, target, string(body))
}

func serveRuntimeV2Raw(t *testing.T, controller *RuntimeV2HTTPController, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)
	return recorder
}

func requireRuntimeV2ResponseCode(t *testing.T, recorder *httptest.ResponseRecorder, code RuntimeErrorCode) {
	t.Helper()
	var response RuntimeError
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
	require.Equal(t, code, response.Error.Code)
}

type runtimeV2TrackedReader struct {
	reader io.Reader
	reads  int
}

func (r *runtimeV2TrackedReader) Read(target []byte) (int, error) {
	r.reads++
	return r.reader.Read(target)
}

type runtimeV2TokenValidatorFake struct {
	token     db.AgentRuntimeToken
	err       error
	plaintext string
	scopes    []string
}

func (f *runtimeV2TokenValidatorFake) ValidateRuntimeToken(_ context.Context, plaintext string, scopes ...string) (db.AgentRuntimeToken, error) {
	f.plaintext = plaintext
	f.scopes = append([]string(nil), scopes...)
	return f.token, f.err
}

type runtimeV2DeviceAuthenticatorFake struct {
	device RuntimeDeviceIdentity
	err    error
	calls  int
}

func (f *runtimeV2DeviceAuthenticatorFake) AuthenticateHTTP(context.Context, *http.Request) (RuntimeDeviceIdentity, error) {
	f.calls++
	return f.device, f.err
}

type runtimeV2SessionServiceFake struct {
	create                func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionRequest) (RuntimeSessionState, error)
	heartbeat             func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error)
	close                 func(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionCloseRequest) (RuntimeSessionState, error)
	createCalls           int
	resolveResponse       RuntimeSessionPrincipal
	resolveErr            error
	workerResolveResponse RuntimeSessionPrincipal
	workerResolveErr      error
	resolvedSessionID     uuid.UUID
	resolvedWorkerID      string
}

func (f *runtimeV2SessionServiceFake) CreateOrAttachSession(ctx context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionRequest) (RuntimeSessionState, error) {
	f.createCalls++
	if f.create == nil {
		return RuntimeSessionState{}, nil
	}
	return f.create(ctx, principal, request)
}

func (f *runtimeV2SessionServiceFake) HeartbeatSession(ctx context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error) {
	if f.heartbeat == nil {
		return RuntimeSessionState{}, nil
	}
	return f.heartbeat(ctx, principal, request)
}

func (f *runtimeV2SessionServiceFake) CloseSession(ctx context.Context, principal AuthenticatedRuntimePrincipal, request RuntimeSessionCloseRequest) (RuntimeSessionState, error) {
	if f.close == nil {
		return RuntimeSessionState{}, nil
	}
	return f.close(ctx, principal, request)
}

func (f *runtimeV2SessionServiceFake) ResolveSessionPrincipal(_ context.Context, _ AuthenticatedRuntimePrincipal, sessionID uuid.UUID) (RuntimeSessionPrincipal, error) {
	f.resolvedSessionID = sessionID
	return f.resolveResponse, f.resolveErr
}

func (f *runtimeV2SessionServiceFake) ResolveWorkerSessionPrincipal(_ context.Context, _ AuthenticatedRuntimePrincipal, workerID string) (RuntimeSessionPrincipal, error) {
	f.resolvedWorkerID = workerID
	return f.workerResolveResponse, f.workerResolveErr
}

type runtimeV2LeaseServiceFake struct {
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

func (f *runtimeV2LeaseServiceFake) ClaimOffer(_ context.Context, principal RuntimeSessionPrincipal) (*RunAssignedPayload, error) {
	f.claimCalls++
	f.lastPrincipal = principal
	return f.claimResponse, f.claimErr
}

func (f *runtimeV2LeaseServiceFake) AckAssignment(_ context.Context, principal RuntimeSessionPrincipal, _ RunAssignmentAckPayload) (RunAssignmentConfirmedPayload, error) {
	f.ackCalls++
	f.lastPrincipal = principal
	return f.ackResponse, f.ackErr
}

func (f *runtimeV2LeaseServiceFake) RejectAssignment(_ context.Context, principal RuntimeSessionPrincipal, _ RunAssignmentRejectPayload) (RunAssignmentRejectedPayload, error) {
	f.rejectCalls++
	f.lastPrincipal = principal
	return f.rejectResponse, f.rejectErr
}

func (f *runtimeV2LeaseServiceFake) RenewLease(_ context.Context, principal RuntimeSessionPrincipal, _ RunLeaseRenewPayload) (RunLeaseRenewedPayload, error) {
	f.renewCalls++
	f.lastPrincipal = principal
	return f.renewResponse, f.renewErr
}

func (f *runtimeV2LeaseServiceFake) ReleaseUnackedOffer(_ context.Context, principal RuntimeSessionPrincipal, _ ...string) error {
	f.lastPrincipal = principal
	return nil
}

type runtimeV2EventStoreFake struct {
	ack       RuntimeEventAck
	err       error
	principal RuntimeEventPrincipal
	identity  RuntimeAttemptIdentity
	request   RuntimeEventRequest
}

func (f *runtimeV2EventStoreFake) Append(_ context.Context, principal RuntimeEventPrincipal, identity RuntimeAttemptIdentity, request RuntimeEventRequest) (RuntimeEventAck, error) {
	f.principal = principal
	f.identity = identity
	f.request = request
	return f.ack, f.err
}

type runtimeV2ResultFinalizerFake struct {
	ack       RuntimeResultAck
	err       error
	principal RuntimeResultPrincipal
	request   RuntimeResultRequest
}

func (f *runtimeV2ResultFinalizerFake) Finalize(_ context.Context, principal RuntimeResultPrincipal, request RuntimeResultRequest) (RuntimeResultAck, error) {
	f.principal = principal
	f.request = request
	return f.ack, f.err
}

var (
	_ RuntimeV2TokenValidator      = (*runtimeV2TokenValidatorFake)(nil)
	_ RuntimeV2DeviceAuthenticator = (*runtimeV2DeviceAuthenticatorFake)(nil)
	_ RuntimeV2SessionService      = (*runtimeV2SessionServiceFake)(nil)
	_ RuntimeV2LeaseService        = (*runtimeV2LeaseServiceFake)(nil)
	_ RuntimeV2EventStore          = (*runtimeV2EventStoreFake)(nil)
	_ RuntimeV2ResultFinalizer     = (*runtimeV2ResultFinalizerFake)(nil)
)
