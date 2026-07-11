package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeV2WebSocketAuthenticatesBeforeUpgrade(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	fixture.tokens.err = errors.New("revoked token")
	server, target := fixture.server(t)
	defer server.Close()

	conn, response, err := websocket.DefaultDialer.Dial(target, http.Header{
		echo.HeaderAuthorization: []string{"Bearer runtime-secret"},
	})
	require.Error(t, err)
	require.Nil(t, conn)
	require.NotNil(t, response)
	defer response.Body.Close()
	require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	require.Equal(t, 0, fixture.devices.calls)
	require.Equal(t, 0, fixture.sessions.createCalls())
}

func TestRuntimeV2WebSocketHelloReadyAndDisconnectOrder(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeV2WS(t, target)

	helloEnvelope := writeRuntimeV2WSHello(t, conn, fixture.hello)
	readyEnvelope := readRuntimeV2WSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageReady, readyEnvelope.Type)
	require.NoError(t, ValidateRuntimeReplyCorrelation(helloEnvelope, readyEnvelope))
	ready, err := DecodeRuntimeMessagePayload[RuntimeReadyPayload](readyEnvelope, RuntimeMessageReady)
	require.NoError(t, err)
	require.Equal(t, fixture.principal.CoreInstanceID.String(), ready.CoreInstanceID)
	require.Equal(t, fixture.now, ready.DatabaseTime)

	created := fixture.sessions.createdRequest()
	require.Equal(t, fixture.hello.RuntimeSessionID, created.RuntimeSessionID)
	require.Equal(t, fixture.hello.NodeID, created.NodeID)
	require.Equal(t, fixture.hello.AgentID, created.AgentID)
	require.Equal(t, fixture.hello.WorkerID, created.WorkerID)
	require.Equal(t, int32(RuntimeProtocolVersion), created.ProtocolVersion)
	require.Equal(t, RuntimeContractID, created.RuntimeContractID)
	require.Equal(t, RuntimeContractDigest, created.RuntimeContractDigest)

	require.NoError(t, conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"),
		time.Now().Add(time.Second),
	))
	require.Eventually(t, func() bool {
		return len(fixture.operations.snapshot()) >= 2
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"close_session", "release_unacked_offer"}, fixture.operations.snapshot()[:2])
	require.Equal(t, "offline", fixture.sessions.closedStatus())
	require.Equal(t, "SESSION_DISCONNECTED", fixture.leases.releaseReason())
}

func TestRuntimeV2WebSocketHandshakeCloseCodes(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*runtimeV2WSTestFixture)
		write     func(*testing.T, *websocket.Conn, RuntimeHelloPayload) RuntimeEnvelope
		errorCode RuntimeErrorCode
		closeCode int
		hasError  bool
	}{
		{
			name: "authentication",
			configure: func(f *runtimeV2WSTestFixture) {
				f.sessions.createErr = newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
			},
			write:     writeRuntimeV2WSHello,
			errorCode: RuntimeErrorUnauthorized,
			closeCode: RuntimeWSCloseAuthenticationFailed,
			hasError:  true,
		},
		{
			name: "protocol",
			write: func(t *testing.T, conn *websocket.Conn, hello RuntimeHelloPayload) RuntimeEnvelope {
				message, err := NewRuntimeTypedMessage(RuntimeMessageHello, nil, hello)
				require.NoError(t, err)
				message.ProtocolVersion = 1
				require.NoError(t, conn.WriteJSON(message))
				return runtimeV2EnvelopeFromTyped(t, message)
			},
			errorCode: RuntimeErrorClientUpgradeRequired,
			closeCode: RuntimeWSCloseClientUpgradeRequired,
		},
		{
			name: "session conflict",
			configure: func(f *runtimeV2WSTestFixture) {
				f.sessions.createErr = newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
			},
			write:     writeRuntimeV2WSHello,
			errorCode: RuntimeErrorSessionConflict,
			closeCode: RuntimeWSCloseSessionConflict,
			hasError:  true,
		},
		{
			name: "required feature",
			write: func(t *testing.T, conn *websocket.Conn, hello RuntimeHelloPayload) RuntimeEnvelope {
				message, err := NewRuntimeTypedMessage(RuntimeMessageHello, nil, hello)
				require.NoError(t, err)
				message.Payload.Features = message.Payload.Features[:len(message.Payload.Features)-1]
				require.NoError(t, conn.WriteJSON(message))
				return runtimeV2EnvelopeFromTyped(t, message)
			},
			errorCode: RuntimeErrorRequiredFeatureMissing,
			closeCode: RuntimeWSCloseRequiredFeatureMissing,
			hasError:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRuntimeV2WSTestFixture()
			if test.configure != nil {
				test.configure(fixture)
			}
			server, target := fixture.server(t)
			defer server.Close()
			conn := dialRuntimeV2WS(t, target)
			defer conn.Close()
			request := test.write(t, conn, fixture.hello)

			if test.hasError {
				errorEnvelope := readRuntimeV2WSEnvelope(t, conn)
				require.Equal(t, RuntimeMessageError, errorEnvelope.Type)
				require.Equal(t, request.MessageID, *errorEnvelope.ReplyToMessageID)
				body, decodeErr := DecodeRuntimeMessagePayload[RuntimeErrorBody](errorEnvelope, RuntimeMessageError)
				require.NoError(t, decodeErr)
				require.Equal(t, test.errorCode, body.Code)
			}
			requireRuntimeV2WSCloseCode(t, conn, test.closeCode)
		})
	}
}

func TestRuntimeV2WebSocketStrictUnknownPayloadClosesProtocol(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeV2WS(t, target)
	defer conn.Close()

	message, err := NewRuntimeTypedMessage(RuntimeMessageHello, nil, fixture.hello)
	require.NoError(t, err)
	raw, err := json.Marshal(message)
	require.NoError(t, err)
	var object map[string]any
	require.NoError(t, json.Unmarshal(raw, &object))
	payload := object["payload"].(map[string]any)
	payload["unexpected"] = true
	require.NoError(t, conn.WriteJSON(object))

	errorEnvelope := readRuntimeV2WSEnvelope(t, conn)
	body, err := DecodeRuntimeMessagePayload[RuntimeErrorBody](errorEnvelope, RuntimeMessageError)
	require.NoError(t, err)
	require.Equal(t, RuntimeErrorValidationFailed, body.Code)
	requireRuntimeV2WSCloseCode(t, conn, RuntimeWSCloseProtocolError)
}

func TestRuntimeV2WebSocketRejectsMessageAboveFourMiB(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeV2WS(t, target)
	defer conn.Close()

	oversized := make([]byte, MaxRuntimeV2MessageBytes+1)
	for index := range oversized {
		oversized[index] = ' '
	}
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, oversized))
	requireRuntimeV2WSCloseCode(t, conn, RuntimeWSCloseProtocolError)
	require.Equal(t, 0, fixture.sessions.createCalls())
}

func TestRuntimeV2WebSocketAssignmentRequiresExplicitCorrelatedAck(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	fixture.leases.ackResponse = RunAssignmentConfirmedPayload{
		AttemptIdentity: assignment.AttemptIdentity,
		AttemptNo:       1,
		LeaseExpiresAt:  fixture.now.Add(time.Minute),
	}
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeV2WS(t, target)
	defer conn.Close()

	writeRuntimeV2WSHello(t, conn, fixture.hello)
	ready := readRuntimeV2WSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageReady, ready.Type)
	assignedEnvelope := readRuntimeV2WSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageRunAssigned, assignedEnvelope.Type)
	require.Nil(t, assignedEnvelope.ReplyToMessageID)
	assigned, err := DecodeRuntimeMessagePayload[RunAssignedPayload](assignedEnvelope, RuntimeMessageRunAssigned)
	require.NoError(t, err)
	require.Equal(t, assignment.AttemptIdentity, assigned.AttemptIdentity)
	require.Equal(t, 0, fixture.leases.ackCallCount(), "sending an offer must not ACK it")

	ackMessage, err := NewRuntimeTypedMessage(
		RuntimeMessageAssignmentAck,
		&assignedEnvelope.MessageID,
		RunAssignmentAckPayload{AttemptIdentity: assigned.AttemptIdentity},
	)
	require.NoError(t, err)
	ackEnvelope := runtimeV2EnvelopeFromTyped(t, ackMessage)
	require.NoError(t, conn.WriteJSON(ackMessage))
	confirmedEnvelope := readRuntimeV2WSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageAssignmentConfirmed, confirmedEnvelope.Type)
	require.NoError(t, ValidateRuntimeReplyCorrelation(ackEnvelope, confirmedEnvelope))
	confirmed, err := DecodeRuntimeMessagePayload[RunAssignmentConfirmedPayload](
		confirmedEnvelope, RuntimeMessageAssignmentConfirmed,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), confirmed.AttemptNo)
	require.Equal(t, 1, fixture.leases.ackCallCount())

	// A lost confirmed frame is recovered by replaying the same correlated ACK;
	// transport correlation must not defeat the Lease service's idempotency.
	require.NoError(t, conn.WriteJSON(ackMessage))
	replayedConfirmed := readRuntimeV2WSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(ackEnvelope, replayedConfirmed))
	require.Equal(t, 2, fixture.leases.ackCallCount())
}

func TestRuntimeV2WebSocketRejectRenewEventAndResultPersistBeforeAck(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	fixture.leases.rejectResponse = RunAssignmentRejectedPayload{
		AttemptIdentity: assignment.AttemptIdentity,
		Outcome:         RuntimeOfferRejected,
		DispatchState:   RuntimeDispatchPending,
	}
	fixture.leases.renewResponse = RunLeaseRenewedPayload{
		AttemptIdentity: assignment.AttemptIdentity,
		LeaseExpiresAt:  fixture.now.Add(time.Minute),
	}
	eventID := uuid.New()
	fixture.events.ack = RuntimeEventAck{ClientEventID: eventID, ClientEventSeq: 1, Sequence: 9}
	resultID := uuid.New()
	fixture.finalizer.ack = RuntimeResultAck{
		ResultID:       resultID,
		Classification: RuntimeResultClassificationSuccess,
		RunStatus:      string(RuntimeRunSuccess),
		DispatchState:  string(RuntimeDispatchTerminal),
	}
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeV2WS(t, target)
	defer conn.Close()

	writeRuntimeV2WSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeV2WSEnvelope(t, conn).Type)
	assignedEnvelope := readRuntimeV2WSEnvelope(t, conn)

	rejectMessage, err := NewRuntimeTypedMessage(
		RuntimeMessageAssignmentReject,
		&assignedEnvelope.MessageID,
		RunAssignmentRejectPayload{
			AttemptIdentity: assignment.AttemptIdentity,
			ReasonCode:      RuntimeRejectNodeAtCapacity,
			Capacity:        1,
			Inflight:        1,
		},
	)
	require.NoError(t, err)
	rejectEnvelope := runtimeV2EnvelopeFromTyped(t, rejectMessage)
	require.NoError(t, conn.WriteJSON(rejectMessage))
	rejectedEnvelope := readRuntimeV2WSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(rejectEnvelope, rejectedEnvelope))
	require.Equal(t, 1, fixture.leases.rejectCallCount())

	renewMessage, err := NewRuntimeTypedMessage(
		RuntimeMessageLeaseRenew,
		nil,
		RunLeaseRenewPayload{
			AttemptIdentity:    assignment.AttemptIdentity,
			LastClientEventSeq: 0,
			Capacity:           1,
			Inflight:           1,
		},
	)
	require.NoError(t, err)
	renewEnvelope := runtimeV2EnvelopeFromTyped(t, renewMessage)
	require.NoError(t, conn.WriteJSON(renewMessage))
	renewedEnvelope := readRuntimeV2WSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(renewEnvelope, renewedEnvelope))
	require.Equal(t, 1, fixture.leases.renewCallCount())

	eventMessage, err := NewRuntimeTypedMessage(
		RuntimeMessageRunEvent,
		nil,
		RunEventPayload{
			AttemptIdentity: assignment.AttemptIdentity,
			ClientEventID:   eventID,
			ClientEventSeq:  1,
			EventType:       "progress.updated",
			Payload:         map[string]any{"percent": 100},
		},
	)
	require.NoError(t, err)
	eventEnvelope := runtimeV2EnvelopeFromTyped(t, eventMessage)
	require.NoError(t, conn.WriteJSON(eventMessage))
	eventAckEnvelope := readRuntimeV2WSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(eventEnvelope, eventAckEnvelope))
	require.Equal(t, 1, fixture.events.callCount())

	resultMessage, err := NewRuntimeTypedMessage(
		RuntimeMessageRunResult,
		nil,
		RunResultPayload{
			AttemptIdentity:     assignment.AttemptIdentity,
			ResultID:            resultID,
			Status:              "success",
			Output:              map[string]any{"answer": "ok"},
			DurationMS:          25,
			FinalClientEventSeq: 1,
		},
	)
	require.NoError(t, err)
	resultEnvelope := runtimeV2EnvelopeFromTyped(t, resultMessage)
	require.NoError(t, conn.WriteJSON(resultMessage))
	resultAckEnvelope := readRuntimeV2WSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(resultEnvelope, resultAckEnvelope))
	require.Equal(t, 1, fixture.finalizer.callCount())

	eventPrincipal := fixture.events.lastPrincipal()
	resultPrincipal := fixture.finalizer.lastPrincipal()
	require.NotNil(t, eventPrincipal.RuntimeSessionID)
	require.NotNil(t, resultPrincipal.RuntimeSessionID)
	require.Equal(t, fixture.principal.RuntimeSessionID, *eventPrincipal.RuntimeSessionID)
	require.Equal(t, fixture.principal.RuntimeSessionID, *resultPrincipal.RuntimeSessionID)
}

func TestRuntimeV2WebSocketRejectsWrongAssignmentCorrelationBeforeLeaseMutation(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeV2WS(t, target)
	defer conn.Close()

	writeRuntimeV2WSHello(t, conn, fixture.hello)
	readRuntimeV2WSEnvelope(t, conn) // ready
	readRuntimeV2WSEnvelope(t, conn) // assigned
	wrongReply := uuid.New()
	ack, err := NewRuntimeTypedMessage(
		RuntimeMessageAssignmentAck,
		&wrongReply,
		RunAssignmentAckPayload{AttemptIdentity: assignment.AttemptIdentity},
	)
	require.NoError(t, err)
	require.NoError(t, conn.WriteJSON(ack))
	errorEnvelope := readRuntimeV2WSEnvelope(t, conn)
	errorBody, err := DecodeRuntimeMessagePayload[RuntimeErrorBody](errorEnvelope, RuntimeMessageError)
	require.NoError(t, err)
	require.Equal(t, RuntimeErrorValidationFailed, errorBody.Code)
	require.Equal(t, 0, fixture.leases.ackCallCount())
	requireRuntimeV2WSCloseCode(t, conn, RuntimeWSCloseProtocolError)
}

func TestRuntimeV2WebSocketBusinessErrorIsCorrelatedAndConnectionStaysOpen(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	fixture.leases.setAckResult(RunAssignmentConfirmedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil))
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeV2WS(t, target)
	defer conn.Close()

	writeRuntimeV2WSHello(t, conn, fixture.hello)
	readRuntimeV2WSEnvelope(t, conn) // ready
	assignedEnvelope := readRuntimeV2WSEnvelope(t, conn)
	ackMessage, err := NewRuntimeTypedMessage(
		RuntimeMessageAssignmentAck,
		&assignedEnvelope.MessageID,
		RunAssignmentAckPayload{AttemptIdentity: assignment.AttemptIdentity},
	)
	require.NoError(t, err)
	ackEnvelope := runtimeV2EnvelopeFromTyped(t, ackMessage)
	require.NoError(t, conn.WriteJSON(ackMessage))
	errorEnvelope := readRuntimeV2WSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(ackEnvelope, errorEnvelope))
	errorBody, err := DecodeRuntimeMessagePayload[RuntimeErrorBody](errorEnvelope, RuntimeMessageError)
	require.NoError(t, err)
	require.Equal(t, RuntimeErrorStaleLease, errorBody.Code)

	// STALE_LEASE is a durable business conflict, not a connection-fatal
	// protocol error. A subsequent idempotent retry can still receive its ACK.
	fixture.leases.setAckResult(RunAssignmentConfirmedPayload{
		AttemptIdentity: assignment.AttemptIdentity,
		AttemptNo:       1,
		LeaseExpiresAt:  fixture.now.Add(time.Minute),
	}, nil)
	require.NoError(t, conn.WriteJSON(ackMessage))
	confirmed := readRuntimeV2WSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageAssignmentConfirmed, confirmed.Type)
	require.NoError(t, ValidateRuntimeReplyCorrelation(ackEnvelope, confirmed))
}

func TestRuntimeV2WebSocketResumeEmitsOneCorrelatedDecisionPerAttempt(t *testing.T) {
	fixture := newRuntimeV2WSTestFixture()
	first := fixture.assignment().AttemptIdentity
	second := fixture.assignment().AttemptIdentity
	resume := &runtimeV2WSResumeServiceFake{response: RuntimeResumeResponse{Decisions: []RunResumeAcceptedPayload{
		{
			AttemptIdentity: first,
			Decision:        RuntimeResumeLeaseRevoked,
			AllowedActions:  []RuntimeResumeAction{RuntimeActionStopExecution, RuntimeActionClearSpool},
		},
		{
			AttemptIdentity: second,
			Decision:        RuntimeResumeResultAcked,
			AllowedActions:  []RuntimeResumeAction{RuntimeActionClearSpool},
		},
	}}}
	fixture.resume = resume
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeV2WS(t, target)
	defer conn.Close()

	writeRuntimeV2WSHello(t, conn, fixture.hello)
	readRuntimeV2WSEnvelope(t, conn) // ready
	requestMessage, err := NewRuntimeTypedMessage(RuntimeMessageResume, nil, RuntimeResumePayload{
		NodeID:           fixture.principal.NodeID,
		AgentID:          fixture.principal.AgentID,
		WorkerID:         fixture.principal.WorkerID,
		RuntimeSessionID: fixture.principal.RuntimeSessionID,
		Attempts: []ResumeAttempt{
			{AttemptIdentity: first, PendingClientEventRanges: []EventRange{}},
			{AttemptIdentity: second, PendingClientEventRanges: []EventRange{}},
		},
	})
	require.NoError(t, err)
	requestEnvelope := runtimeV2EnvelopeFromTyped(t, requestMessage)
	require.NoError(t, conn.WriteJSON(requestMessage))

	firstReply := readRuntimeV2WSEnvelope(t, conn)
	secondReply := readRuntimeV2WSEnvelope(t, conn)
	for _, reply := range []RuntimeEnvelope{firstReply, secondReply} {
		require.Equal(t, RuntimeMessageResumeAccepted, reply.Type)
		require.NoError(t, ValidateRuntimeReplyCorrelation(requestEnvelope, reply))
	}
	firstDecision, err := DecodeRuntimeMessagePayload[RunResumeAcceptedPayload](firstReply, RuntimeMessageResumeAccepted)
	require.NoError(t, err)
	secondDecision, err := DecodeRuntimeMessagePayload[RunResumeAcceptedPayload](secondReply, RuntimeMessageResumeAccepted)
	require.NoError(t, err)
	require.Equal(t, first, firstDecision.AttemptIdentity)
	require.Equal(t, second, secondDecision.AttemptIdentity)
	require.Equal(t, 1, resume.callCount())
}

type runtimeV2WSTestFixture struct {
	now           time.Time
	authenticated AuthenticatedRuntimePrincipal
	hello         RuntimeHelloPayload
	principal     RuntimeSessionPrincipal
	operations    *runtimeV2WSOperations
	tokens        *runtimeV2WSTokenValidatorFake
	devices       *runtimeV2WSDeviceAuthenticatorFake
	sessions      *runtimeV2WSSessionServiceFake
	leases        *runtimeV2WSLeaseServiceFake
	events        *runtimeV2WSEventStoreFake
	finalizer     *runtimeV2WSFinalizerFake
	resume        RuntimeV2ResumeService
}

func newRuntimeV2WSTestFixture() *runtimeV2WSTestFixture {
	now := time.Date(2026, 7, 11, 9, 10, 11, 0, time.UTC)
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
	principal := RuntimeSessionPrincipal{
		RuntimeSessionID:                uuid.New(),
		NodeID:                          authenticated.Device.NodeID,
		AgentID:                         authenticated.AgentID,
		CredentialID:                    authenticated.CredentialID,
		WorkerID:                        "worker-ws-1",
		SessionEpoch:                    3,
		CoreInstanceID:                  uuid.New(),
		DeviceCertificateSerial:         authenticated.Device.CertificateSerial,
		DevicePublicKeyThumbprintSHA256: authenticated.Device.PublicKeyThumbprintSHA256,
		Status:                          "active",
		DatabaseTime:                    now,
	}
	hello := RuntimeHelloPayload{
		NodeID:           principal.NodeID,
		AgentID:          principal.AgentID,
		WorkerID:         principal.WorkerID,
		RuntimeSessionID: principal.RuntimeSessionID,
		SessionEpoch:     principal.SessionEpoch,
		NodeVersion:      "2.0.0",
		Capacity:         1,
		Features:         RuntimeRequiredFeatures(),
		ContractDigest:   RuntimeContractDigest,
	}
	coreID := principal.CoreInstanceID
	state := RuntimeSessionState{
		Session: db.RuntimeSession{
			RuntimeSessionID:       principal.RuntimeSessionID,
			NodeID:                 principal.NodeID,
			AgentID:                principal.AgentID,
			CredentialID:           principal.CredentialID,
			WorkerID:               principal.WorkerID,
			SessionEpoch:           principal.SessionEpoch,
			Status:                 "active",
			AttachedCoreInstanceID: &coreID,
			HeartbeatAt:            now,
		},
		DatabaseTime: now,
	}
	operations := &runtimeV2WSOperations{}
	return &runtimeV2WSTestFixture{
		now:           now,
		authenticated: authenticated,
		hello:         hello,
		principal:     principal,
		operations:    operations,
		tokens: &runtimeV2WSTokenValidatorFake{token: db.AgentRuntimeToken{
			ID: principal.CredentialID, AgentID: principal.AgentID,
		}},
		devices:   &runtimeV2WSDeviceAuthenticatorFake{device: authenticated.Device},
		sessions:  &runtimeV2WSSessionServiceFake{state: state, principal: principal, operations: operations},
		leases:    &runtimeV2WSLeaseServiceFake{operations: operations},
		events:    &runtimeV2WSEventStoreFake{},
		finalizer: &runtimeV2WSFinalizerFake{},
	}
}

func (f *runtimeV2WSTestFixture) controller() *RuntimeV2HTTPController {
	return NewRuntimeV2HTTPController(RuntimeV2HTTPDependencies{
		TokenValidator:      f.tokens,
		DeviceAuthenticator: f.devices,
		Sessions:            f.sessions,
		Leases:              f.leases,
		EventStore:          f.events,
		Finalizer:           f.finalizer,
		Resume:              f.resume,
	})
}

func (f *runtimeV2WSTestFixture) server(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	e := echo.New()
	f.controller().Register(e.Group("/api/v1"))
	server := httptest.NewServer(e)
	target := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/agent-runtime/v2/ws"
	return server, target
}

func (f *runtimeV2WSTestFixture) assignment() RunAssignedPayload {
	return RunAssignedPayload{
		AttemptIdentity: AttemptIdentity{
			RunID:            uuid.New(),
			AttemptID:        uuid.New(),
			LeaseID:          uuid.New(),
			FencingToken:     4,
			NodeID:           f.principal.NodeID,
			AgentID:          f.principal.AgentID,
			WorkerID:         f.principal.WorkerID,
			RuntimeSessionID: f.principal.RuntimeSessionID,
		},
		OfferNo:              1,
		OfferExpiresAt:       f.now.Add(30 * time.Second),
		AttemptDeadlineAt:    f.now.Add(10 * time.Minute),
		RunDeadlineAt:        f.now.Add(20 * time.Minute),
		Input:                map[string]any{"task": "test"},
		NodeEnvelope:         "node-envelope",
		AgentInvocationToken: "invocation-token",
	}
}

func dialRuntimeV2WS(t *testing.T, target string) *websocket.Conn {
	t.Helper()
	conn, response, err := websocket.DefaultDialer.Dial(target, http.Header{
		echo.HeaderAuthorization: []string{"Bearer runtime-secret"},
	})
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	require.NotNil(t, conn)
	return conn
}

func writeRuntimeV2WSHello(t *testing.T, conn *websocket.Conn, hello RuntimeHelloPayload) RuntimeEnvelope {
	t.Helper()
	message, err := NewRuntimeTypedMessage(RuntimeMessageHello, nil, hello)
	require.NoError(t, err)
	require.NoError(t, conn.WriteJSON(message))
	return runtimeV2EnvelopeFromTyped(t, message)
}

func readRuntimeV2WSEnvelope(t *testing.T, conn *websocket.Conn) RuntimeEnvelope {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	messageType, frame, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, messageType)
	envelope, err := ParseRuntimeEnvelope(frame)
	require.NoError(t, err, string(frame))
	return envelope
}

func runtimeV2EnvelopeFromTyped[P any](t *testing.T, message RuntimeTypedEnvelope[P]) RuntimeEnvelope {
	t.Helper()
	payload, err := json.Marshal(message.Payload)
	require.NoError(t, err)
	return RuntimeEnvelope{RuntimeEnvelopeFields: message.RuntimeEnvelopeFields, Payload: payload}
}

func requireRuntimeV2WSCloseCode(t *testing.T, conn *websocket.Conn, expected int) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, expected, closeErr.Code, closeErr.Error())
}

type runtimeV2WSOperations struct {
	mu    sync.Mutex
	items []string
}

func (o *runtimeV2WSOperations) append(value string) {
	o.mu.Lock()
	o.items = append(o.items, value)
	o.mu.Unlock()
}

func (o *runtimeV2WSOperations) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.items...)
}

type runtimeV2WSTokenValidatorFake struct {
	mu        sync.Mutex
	token     db.AgentRuntimeToken
	err       error
	plaintext string
	scopes    []string
}

func (f *runtimeV2WSTokenValidatorFake) ValidateRuntimeToken(_ context.Context, plaintext string, scopes ...string) (db.AgentRuntimeToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plaintext = plaintext
	f.scopes = append([]string(nil), scopes...)
	return f.token, f.err
}

type runtimeV2WSDeviceAuthenticatorFake struct {
	mu     sync.Mutex
	device RuntimeDeviceIdentity
	err    error
	calls  int
}

func (f *runtimeV2WSDeviceAuthenticatorFake) AuthenticateHTTP(context.Context, *http.Request) (RuntimeDeviceIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.device, f.err
}

type runtimeV2WSSessionServiceFake struct {
	mu           sync.Mutex
	state        RuntimeSessionState
	principal    RuntimeSessionPrincipal
	operations   *runtimeV2WSOperations
	createErr    error
	resolveErr   error
	heartbeatErr error
	closeErr     error
	createCount  int
	created      RuntimeSessionRequest
	closeRequest RuntimeSessionCloseRequest
}

func (f *runtimeV2WSSessionServiceFake) CreateOrAttachSession(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionRequest) (RuntimeSessionState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCount++
	f.created = request
	return f.state, f.createErr
}

func (f *runtimeV2WSSessionServiceFake) HeartbeatSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state, f.heartbeatErr
}

func (f *runtimeV2WSSessionServiceFake) CloseSession(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionCloseRequest) (RuntimeSessionState, error) {
	f.mu.Lock()
	f.closeRequest = request
	err := f.closeErr
	state := f.state
	f.mu.Unlock()
	if f.operations != nil {
		f.operations.append("close_session")
	}
	return state, err
}

func (f *runtimeV2WSSessionServiceFake) ResolveSessionPrincipal(context.Context, AuthenticatedRuntimePrincipal, uuid.UUID) (RuntimeSessionPrincipal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.principal, f.resolveErr
}

func (f *runtimeV2WSSessionServiceFake) ResolveWorkerSessionPrincipal(context.Context, AuthenticatedRuntimePrincipal, string) (RuntimeSessionPrincipal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.principal, f.resolveErr
}

func (f *runtimeV2WSSessionServiceFake) createCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createCount
}

func (f *runtimeV2WSSessionServiceFake) createdRequest() RuntimeSessionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created
}

func (f *runtimeV2WSSessionServiceFake) closedStatus() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeRequest.Status
}

type runtimeV2WSLeaseServiceFake struct {
	mu             sync.Mutex
	operations     *runtimeV2WSOperations
	assignment     *RunAssignedPayload
	claimErr       error
	ackResponse    RunAssignmentConfirmedPayload
	ackErr         error
	rejectResponse RunAssignmentRejectedPayload
	rejectErr      error
	renewResponse  RunLeaseRenewedPayload
	renewErr       error
	releaseErr     error
	releaseCode    string
	ackCalls       int
	rejectCalls    int
	renewCalls     int
}

func (f *runtimeV2WSLeaseServiceFake) ClaimOffer(context.Context, RuntimeSessionPrincipal) (*RunAssignedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.assignment == nil {
		return nil, f.claimErr
	}
	copy := *f.assignment
	return &copy, f.claimErr
}

func (f *runtimeV2WSLeaseServiceFake) AckAssignment(_ context.Context, _ RuntimeSessionPrincipal, _ RunAssignmentAckPayload) (RunAssignmentConfirmedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackCalls++
	if f.ackErr == nil {
		f.assignment = nil
	}
	return f.ackResponse, f.ackErr
}

func (f *runtimeV2WSLeaseServiceFake) RejectAssignment(_ context.Context, _ RuntimeSessionPrincipal, _ RunAssignmentRejectPayload) (RunAssignmentRejectedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejectCalls++
	if f.rejectErr == nil {
		f.assignment = nil
	}
	return f.rejectResponse, f.rejectErr
}

func (f *runtimeV2WSLeaseServiceFake) RenewLease(_ context.Context, _ RuntimeSessionPrincipal, _ RunLeaseRenewPayload) (RunLeaseRenewedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls++
	return f.renewResponse, f.renewErr
}

func (f *runtimeV2WSLeaseServiceFake) ReleaseUnackedOffer(_ context.Context, _ RuntimeSessionPrincipal, reason ...string) error {
	f.mu.Lock()
	if len(reason) > 0 {
		f.releaseCode = reason[0]
	}
	err := f.releaseErr
	f.mu.Unlock()
	if f.operations != nil {
		f.operations.append("release_unacked_offer")
	}
	return err
}

func (f *runtimeV2WSLeaseServiceFake) setAssignment(assignment *RunAssignedPayload) {
	f.mu.Lock()
	f.assignment = assignment
	f.mu.Unlock()
}

func (f *runtimeV2WSLeaseServiceFake) setAckResult(response RunAssignmentConfirmedPayload, err error) {
	f.mu.Lock()
	f.ackResponse = response
	f.ackErr = err
	f.mu.Unlock()
}

func (f *runtimeV2WSLeaseServiceFake) ackCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ackCalls
}

func (f *runtimeV2WSLeaseServiceFake) rejectCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rejectCalls
}

func (f *runtimeV2WSLeaseServiceFake) renewCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewCalls
}

func (f *runtimeV2WSLeaseServiceFake) releaseReason() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releaseCode
}

type runtimeV2WSEventStoreFake struct {
	mu        sync.Mutex
	ack       RuntimeEventAck
	err       error
	calls     int
	principal RuntimeEventPrincipal
	identity  RuntimeAttemptIdentity
	request   RuntimeEventRequest
}

func (f *runtimeV2WSEventStoreFake) Append(_ context.Context, principal RuntimeEventPrincipal, identity RuntimeAttemptIdentity, request RuntimeEventRequest) (RuntimeEventAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.principal = principal
	f.identity = identity
	f.request = request
	return f.ack, f.err
}

func (f *runtimeV2WSEventStoreFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *runtimeV2WSEventStoreFake) lastPrincipal() RuntimeEventPrincipal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.principal
}

type runtimeV2WSFinalizerFake struct {
	mu        sync.Mutex
	ack       RuntimeResultAck
	err       error
	calls     int
	principal RuntimeResultPrincipal
	request   RuntimeResultRequest
}

type runtimeV2WSResumeServiceFake struct {
	mu       sync.Mutex
	response RuntimeResumeResponse
	err      error
	calls    int
}

func (f *runtimeV2WSResumeServiceFake) Resume(context.Context, RuntimeSessionPrincipal, RuntimeResumePayload) (RuntimeResumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.response, f.err
}

func (f *runtimeV2WSResumeServiceFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *runtimeV2WSFinalizerFake) Finalize(_ context.Context, principal RuntimeResultPrincipal, request RuntimeResultRequest) (RuntimeResultAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.principal = principal
	f.request = request
	return f.ack, f.err
}

func (f *runtimeV2WSFinalizerFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *runtimeV2WSFinalizerFake) lastPrincipal() RuntimeResultPrincipal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.principal
}

var (
	_ RuntimeV2TokenValidator      = (*runtimeV2WSTokenValidatorFake)(nil)
	_ RuntimeV2DeviceAuthenticator = (*runtimeV2WSDeviceAuthenticatorFake)(nil)
	_ RuntimeV2SessionService      = (*runtimeV2WSSessionServiceFake)(nil)
	_ RuntimeV2LeaseService        = (*runtimeV2WSLeaseServiceFake)(nil)
	_ RuntimeV2EventStore          = (*runtimeV2WSEventStoreFake)(nil)
	_ RuntimeV2ResultFinalizer     = (*runtimeV2WSFinalizerFake)(nil)
	_ RuntimeV2ResumeService       = (*runtimeV2WSResumeServiceFake)(nil)
)
