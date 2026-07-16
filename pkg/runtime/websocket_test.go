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

func TestRuntimeWebSocketAuthenticatesBeforeUpgrade(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
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

func TestRuntimeWebSocketTransportPolicyRejectsForbiddenEndpointBeforeUpgrade(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	fixture.transportPolicy = func() RuntimeTransportPolicy {
		policy := CurrentRuntimeTransportPolicy()
		policy.OrderedTransports = []RuntimeTransport{RuntimeTransportLongPoll}
		return policy
	}
	server, target := fixture.server(t)
	defer server.Close()

	conn, response, err := websocket.DefaultDialer.Dial(target, http.Header{
		echo.HeaderAuthorization: []string{"Bearer runtime-secret"},
	})
	require.Error(t, err)
	require.Nil(t, conn)
	require.NotNil(t, response)
	defer response.Body.Close()
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	var envelope RuntimeError
	require.NoError(t, json.NewDecoder(response.Body).Decode(&envelope))
	require.Equal(t, RuntimeErrorForbidden, envelope.Error.Code)
	require.Equal(t, RuntimeTransportForbiddenSignal, envelope.Error.Message)
	require.Equal(t, 0, fixture.sessions.createCalls())
}

func TestRuntimeWebSocketPolicyChangeClosesEstablishedTransportWithCanonicalSignal(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	var policyMu sync.RWMutex
	policy := CurrentRuntimeTransportPolicy()
	fixture.transportPolicy = func() RuntimeTransportPolicy {
		policyMu.RLock()
		defer policyMu.RUnlock()
		copy := policy
		copy.OrderedTransports = append([]RuntimeTransport(nil), policy.OrderedTransports...)
		return copy
	}
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()
	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)

	policyMu.Lock()
	policy.OrderedTransports = []RuntimeTransport{RuntimeTransportLongPoll}
	policyMu.Unlock()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, websocket.ClosePolicyViolation, closeErr.Code)
	require.Equal(t, RuntimePolicyChangedSignal, closeErr.Text)
}

func TestRuntimeWebSocketHelloReadyAndDisconnectOrder(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)

	helloEnvelope := writeRuntimeWSHello(t, conn, fixture.hello)
	readyEnvelope := readRuntimeWSEnvelope(t, conn)
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
	require.Equal(t, RuntimeTransportWebSocket, created.Transport)

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

func TestRuntimeWebSocketDrainWaitsForCommittedCorrelatedReceipt(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	deadline := fixture.now.Add(2 * time.Minute)
	fixture.sessions.drainReceipt = RuntimeDrainPayload{
		DeadlineAt: deadline,
		ReasonCode: "SDK_SHUTDOWN",
		Capacity:   0,
		Inflight:   2,
	}
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()
	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)

	request, requestEnvelope, err := newRuntimeWSTypedMessage(
		RuntimeMessageDrain, nil, RuntimeDrainPayload{
			DeadlineAt: deadline,
			ReasonCode: "SDK_SHUTDOWN",
			Capacity:   0,
			Inflight:   999,
		},
	)
	require.NoError(t, err)
	require.NoError(t, conn.WriteJSON(request))
	reply := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageDrain, reply.Type)
	require.NoError(t, ValidateRuntimeReplyCorrelation(requestEnvelope, reply))
	receipt, err := DecodeRuntimeMessagePayload[RuntimeDrainPayload](reply, RuntimeMessageDrain)
	require.NoError(t, err)
	require.Equal(t, int64(2), receipt.Inflight)
	require.Equal(t, int64(0), receipt.Capacity)

	fixture.sessions.mu.Lock()
	captured := fixture.sessions.drainRequest
	fixture.sessions.mu.Unlock()
	require.Equal(t, fixture.principal.RuntimeSessionID, captured.RuntimeSessionID)
	require.Equal(t, fixture.principal.AttachmentID, captured.AttachmentID)
	require.Equal(t, int64(999), captured.Payload.Inflight)
}

func TestRuntimeWebSocketPassesBoundedRecoveryReasonIntoServerValidation(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()

	conn, response, err := websocket.DefaultDialer.Dial(target, http.Header{
		echo.HeaderAuthorization:    []string{"Bearer runtime-secret"},
		RuntimeFallbackReasonHeader: []string{string(RuntimeTransportReasonRecovery)},
	})
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	require.NoError(t, err)
	defer conn.Close()
	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)

	created := fixture.sessions.createdRequest()
	require.Equal(t, RuntimeTransportReasonRecovery, created.ReportedTransportReason)
	require.Equal(t, CurrentRuntimeTransportPolicy(), created.TransportPolicy)
}

func TestRuntimePreviousGenerationUsesCanonicalWebSocketAndAttachmentFence(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	fixture.hello.ContractDigest = runtimePreviousContractDigest
	fixture.hello.Features = runtimeRequiredFeaturesForDigest(runtimePreviousContractDigest)
	fixture.principal.RuntimeContractDigest = runtimePreviousContractDigest
	fixture.sessions.principal.RuntimeContractDigest = runtimePreviousContractDigest
	fixture.sessions.state.Session.RuntimeContractDigest = runtimePreviousContractDigest
	fixture.sessions.state.Session.Features = runtimeRequiredFeaturesForDigest(runtimePreviousContractDigest)
	server, target := fixture.server(t)
	defer server.Close()
	require.Contains(t, target, "/api/v1/agent-runtime/ws")
	require.NotContains(t, target, runtimePreviousContractDigest)

	conn := dialRuntimeWS(t, target)
	defer conn.Close()
	writeRuntimeWSHello(t, conn, fixture.hello)
	readyEnvelope := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageReady, readyEnvelope.Type)
	var ready map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(readyEnvelope.Payload, &ready))
	require.Contains(t, ready, "attachment_id")
	require.ElementsMatch(t, []string{"core_instance_id", "attachment_id", "features", "offer_ttl_seconds", "lease_ttl_seconds", "database_time"}, mapKeys(ready))
	var readyFeatures []string
	require.NoError(t, json.Unmarshal(ready["features"], &readyFeatures))
	require.ElementsMatch(t, runtimeRequiredFeaturesForDigest(runtimePreviousContractDigest), readyFeatures)
	require.NotContains(t, readyFeatures, "session_drain")
	require.Equal(t, runtimePreviousContractDigest, fixture.sessions.createdRequest().RuntimeContractDigest)
}

func TestRuntimePreviousGenerationStaleReplayConflictIsStableOverWebSocket(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	fixture.hello.ContractDigest = runtimePreviousContractDigest
	fixture.hello.Features = runtimeRequiredFeaturesForDigest(runtimePreviousContractDigest)
	fixture.sessions.createErr = newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	server, target := fixture.server(t)
	defer server.Close()

	conn := dialRuntimeWS(t, target)
	defer conn.Close()
	request := writeRuntimeWSHello(t, conn, fixture.hello)
	errorEnvelope := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageError, errorEnvelope.Type)
	require.Equal(t, request.MessageID, *errorEnvelope.ReplyToMessageID)
	body, err := DecodeRuntimeMessagePayload[RuntimeErrorBody](errorEnvelope, RuntimeMessageError)
	require.NoError(t, err)
	require.Equal(t, RuntimeErrorSessionConflict, body.Code)
	requireRuntimeWSCloseCode(t, conn, RuntimeWSCloseSessionConflict)
}

func TestRuntimeWebSocketControllerShutdownDrainsHijackedConnections(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	controller := fixture.controller()
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	server := httptest.NewServer(e)
	defer server.Close()
	target := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/agent-runtime/ws"

	conn := dialRuntimeWS(t, target)
	defer conn.Close()
	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	require.NoError(t, controller.Shutdown(shutdownCtx))

	// Shutdown returns only after the WebSocket handler's durable cleanup has
	// detached the Session and released any unacknowledged offer.
	require.Equal(t, []string{"close_session", "release_unacked_offer"}, fixture.operations.snapshot())
	require.Equal(t, "offline", fixture.sessions.closedStatus())
	_, _, err := conn.ReadMessage()
	require.Error(t, err)

	replacement, response, err := websocket.DefaultDialer.Dial(target, http.Header{
		echo.HeaderAuthorization: []string{"Bearer runtime-secret"},
	})
	require.Error(t, err)
	require.Nil(t, replacement)
	require.NotNil(t, response)
	defer response.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	require.Equal(t, 1, fixture.sessions.createCalls())
}

func TestRuntimeWebSocketRegistryShutdownCoversLateRegistration(t *testing.T) {
	registry := newRuntimeWSRegistry()
	require.True(t, registry.admit())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	shutdownResult := make(chan error, 1)
	go func() {
		shutdownResult <- registry.shutdown(shutdownCtx)
	}()
	require.Eventually(t, func() bool {
		registry.mu.Lock()
		defer registry.mu.Unlock()
		return registry.stopping
	}, time.Second, time.Millisecond)

	late := &runtimeWSManagedConnectionFake{shutdownCalled: make(chan struct{})}
	if registry.register(late) {
		t.Fatal("late WebSocket registration was accepted after shutdown started")
	}
	select {
	case <-late.shutdownCalled:
	case <-time.After(time.Second):
		t.Fatal("late WebSocket registration was not closed")
	}
	select {
	case err := <-shutdownResult:
		t.Fatalf("shutdown returned before the admitted request finished: %v", err)
	default:
	}

	registry.finish(late)
	require.NoError(t, <-shutdownResult)
	require.False(t, registry.admit())
}

func TestRuntimeWebSocketMaintenanceStopsBeforeUsingReplacementAttachment(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()

	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)
	fixture.sessions.replaceAttachment()
	fixture.wakeHub.Wake(fixture.principal.AgentID)

	requireRuntimeWSCloseCode(t, conn, RuntimeWSCloseSessionConflict)
	require.Eventually(t, func() bool {
		return len(fixture.operations.snapshot()) >= 1
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"stale_close_session"}, fixture.operations.snapshot())
	require.Empty(t, fixture.leases.releaseReason())
}

func TestRuntimeWebSocketWakeDeliversAssignmentBeforePollTick(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()

	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	started := time.Now()
	fixture.wakeHub.Wake(fixture.principal.AgentID)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(350*time.Millisecond)))
	assigned := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageRunAssigned, assigned.Type)
	require.Less(t, time.Since(started), 350*time.Millisecond)
}

func TestRuntimeWebSocketHandshakeCloseCodes(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*runtimeWSTestFixture)
		write     func(*testing.T, *websocket.Conn, RuntimeHelloPayload) RuntimeEnvelope
		errorCode RuntimeErrorCode
		closeCode int
		hasError  bool
	}{
		{
			name: "authentication",
			configure: func(f *runtimeWSTestFixture) {
				f.sessions.createErr = newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
			},
			write:     writeRuntimeWSHello,
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
				return runtimeEnvelopeFromTyped(t, message)
			},
			errorCode: RuntimeErrorClientUpgradeRequired,
			closeCode: RuntimeWSCloseClientUpgradeRequired,
		},
		{
			name: "session conflict",
			configure: func(f *runtimeWSTestFixture) {
				f.sessions.createErr = newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
			},
			write:     writeRuntimeWSHello,
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
				return runtimeEnvelopeFromTyped(t, message)
			},
			errorCode: RuntimeErrorRequiredFeatureMissing,
			closeCode: RuntimeWSCloseRequiredFeatureMissing,
			hasError:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRuntimeWSTestFixture()
			if test.configure != nil {
				test.configure(fixture)
			}
			server, target := fixture.server(t)
			defer server.Close()
			conn := dialRuntimeWS(t, target)
			defer conn.Close()
			request := test.write(t, conn, fixture.hello)

			if test.hasError {
				errorEnvelope := readRuntimeWSEnvelope(t, conn)
				require.Equal(t, RuntimeMessageError, errorEnvelope.Type)
				require.Equal(t, request.MessageID, *errorEnvelope.ReplyToMessageID)
				body, decodeErr := DecodeRuntimeMessagePayload[RuntimeErrorBody](errorEnvelope, RuntimeMessageError)
				require.NoError(t, decodeErr)
				require.Equal(t, test.errorCode, body.Code)
			}
			requireRuntimeWSCloseCode(t, conn, test.closeCode)
		})
	}
}

func TestRuntimeWebSocketStrictUnknownPayloadClosesProtocol(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
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

	errorEnvelope := readRuntimeWSEnvelope(t, conn)
	body, err := DecodeRuntimeMessagePayload[RuntimeErrorBody](errorEnvelope, RuntimeMessageError)
	require.NoError(t, err)
	require.Equal(t, RuntimeErrorValidationFailed, body.Code)
	requireRuntimeWSCloseCode(t, conn, RuntimeWSCloseProtocolError)
}

func TestRuntimeWebSocketRejectsMessageAboveFourMiB(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()

	oversized := make([]byte, MaxRuntimeMessageBytes+1)
	for index := range oversized {
		oversized[index] = ' '
	}
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, oversized))
	requireRuntimeWSCloseCode(t, conn, RuntimeWSCloseProtocolError)
	require.Equal(t, 0, fixture.sessions.createCalls())
}

func TestRuntimeWebSocketAssignmentRequiresExplicitCorrelatedAck(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	fixture.leases.ackResponse = RunAssignmentConfirmedPayload{
		AttemptIdentity: assignment.AttemptIdentity,
		AttemptNo:       1,
		LeaseExpiresAt:  fixture.now.Add(time.Minute),
	}
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()

	writeRuntimeWSHello(t, conn, fixture.hello)
	ready := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageReady, ready.Type)
	assignedEnvelope := readRuntimeWSEnvelope(t, conn)
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
	ackEnvelope := runtimeEnvelopeFromTyped(t, ackMessage)
	require.NoError(t, conn.WriteJSON(ackMessage))
	confirmedEnvelope := readRuntimeWSEnvelope(t, conn)
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
	replayedConfirmed := readRuntimeWSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(ackEnvelope, replayedConfirmed))
	require.Equal(t, 2, fixture.leases.ackCallCount())
}

func TestRuntimeWebSocketRejectRenewEventAndResultPersistBeforeAck(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
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
	conn := dialRuntimeWS(t, target)
	defer conn.Close()

	writeRuntimeWSHello(t, conn, fixture.hello)
	require.Equal(t, RuntimeMessageReady, readRuntimeWSEnvelope(t, conn).Type)
	assignedEnvelope := readRuntimeWSEnvelope(t, conn)

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
	rejectEnvelope := runtimeEnvelopeFromTyped(t, rejectMessage)
	require.NoError(t, conn.WriteJSON(rejectMessage))
	rejectedEnvelope := readRuntimeWSEnvelope(t, conn)
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
	renewEnvelope := runtimeEnvelopeFromTyped(t, renewMessage)
	require.NoError(t, conn.WriteJSON(renewMessage))
	renewedEnvelope := readRuntimeWSEnvelope(t, conn)
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
	eventEnvelope := runtimeEnvelopeFromTyped(t, eventMessage)
	require.NoError(t, conn.WriteJSON(eventMessage))
	eventAckEnvelope := readRuntimeWSEnvelope(t, conn)
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
	resultEnvelope := runtimeEnvelopeFromTyped(t, resultMessage)
	require.NoError(t, conn.WriteJSON(resultMessage))
	resultAckEnvelope := readRuntimeWSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(resultEnvelope, resultAckEnvelope))
	require.Equal(t, 1, fixture.finalizer.callCount())

	eventPrincipal := fixture.events.lastPrincipal()
	resultPrincipal := fixture.finalizer.lastPrincipal()
	require.NotNil(t, eventPrincipal.RuntimeSessionID)
	require.NotNil(t, resultPrincipal.RuntimeSessionID)
	require.Equal(t, fixture.principal.RuntimeSessionID, *eventPrincipal.RuntimeSessionID)
	require.Equal(t, fixture.principal.RuntimeSessionID, *resultPrincipal.RuntimeSessionID)
}

func TestRuntimeWebSocketRejectsWrongAssignmentCorrelationBeforeLeaseMutation(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()

	writeRuntimeWSHello(t, conn, fixture.hello)
	readRuntimeWSEnvelope(t, conn) // ready
	readRuntimeWSEnvelope(t, conn) // assigned
	wrongReply := uuid.New()
	ack, err := NewRuntimeTypedMessage(
		RuntimeMessageAssignmentAck,
		&wrongReply,
		RunAssignmentAckPayload{AttemptIdentity: assignment.AttemptIdentity},
	)
	require.NoError(t, err)
	require.NoError(t, conn.WriteJSON(ack))
	errorEnvelope := readRuntimeWSEnvelope(t, conn)
	errorBody, err := DecodeRuntimeMessagePayload[RuntimeErrorBody](errorEnvelope, RuntimeMessageError)
	require.NoError(t, err)
	require.Equal(t, RuntimeErrorValidationFailed, errorBody.Code)
	require.Equal(t, 0, fixture.leases.ackCallCount())
	requireRuntimeWSCloseCode(t, conn, RuntimeWSCloseProtocolError)
}

func TestRuntimeWebSocketBusinessErrorIsCorrelatedAndConnectionStaysOpen(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	assignment := fixture.assignment()
	fixture.leases.setAssignment(&assignment)
	fixture.leases.setAckResult(RunAssignmentConfirmedPayload{}, newRuntimeLeaseError(RuntimeLeaseErrorStaleLease, nil))
	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()

	writeRuntimeWSHello(t, conn, fixture.hello)
	readRuntimeWSEnvelope(t, conn) // ready
	assignedEnvelope := readRuntimeWSEnvelope(t, conn)
	ackMessage, err := NewRuntimeTypedMessage(
		RuntimeMessageAssignmentAck,
		&assignedEnvelope.MessageID,
		RunAssignmentAckPayload{AttemptIdentity: assignment.AttemptIdentity},
	)
	require.NoError(t, err)
	ackEnvelope := runtimeEnvelopeFromTyped(t, ackMessage)
	require.NoError(t, conn.WriteJSON(ackMessage))
	errorEnvelope := readRuntimeWSEnvelope(t, conn)
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
	confirmed := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageAssignmentConfirmed, confirmed.Type)
	require.NoError(t, ValidateRuntimeReplyCorrelation(ackEnvelope, confirmed))
}

func TestRuntimeWebSocketResumeEmitsOneCorrelatedDecisionPerAttempt(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	first := fixture.assignment().AttemptIdentity
	second := fixture.assignment().AttemptIdentity
	resume := &runtimeWSResumeServiceFake{response: RuntimeResumeResponse{Decisions: []RunResumeAcceptedPayload{
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
	conn := dialRuntimeWS(t, target)
	defer conn.Close()

	writeRuntimeWSHello(t, conn, fixture.hello)
	readRuntimeWSEnvelope(t, conn) // ready
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
	requestEnvelope := runtimeEnvelopeFromTyped(t, requestMessage)
	require.NoError(t, conn.WriteJSON(requestMessage))

	firstReply := readRuntimeWSEnvelope(t, conn)
	secondReply := readRuntimeWSEnvelope(t, conn)
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

func TestRuntimeWebSocketCancelCommandRequiresCorrelatedDurableAck(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	identity := fixture.assignment().AttemptIdentity
	cancellationID := uuid.New()
	cancel := RunCancelPayload{
		CancellationID:  cancellationID,
		AttemptIdentity: identity,
		ReasonCode:      runtimeCancellationReasonCode,
		DeadlineAt:      fixture.now.Add(30 * time.Second),
	}
	raw, err := json.Marshal(cancel)
	require.NoError(t, err)
	fixture.cancellations.setCommand(PendingCommand{Type: RuntimeMessageRunCancel, Payload: raw})

	server, target := fixture.server(t)
	defer server.Close()
	conn := dialRuntimeWS(t, target)
	defer conn.Close()
	hello := writeRuntimeWSHello(t, conn, fixture.hello)
	ready := readRuntimeWSEnvelope(t, conn)
	require.NoError(t, ValidateRuntimeReplyCorrelation(hello, ready))

	command := readRuntimeWSEnvelope(t, conn)
	require.Equal(t, RuntimeMessageRunCancel, command.Type)
	require.Nil(t, command.ReplyToMessageID)
	decoded, err := DecodeRuntimeMessagePayload[RunCancelPayload](command, RuntimeMessageRunCancel)
	require.NoError(t, err)
	require.Equal(t, cancel, decoded)

	for _, state := range []RuntimeCancelState{RuntimeCancelStopping, RuntimeCancelStopped} {
		ack, err := NewRuntimeTypedMessage(RuntimeMessageRunCancelAck, &command.MessageID, RunCancelAckPayload{
			CancellationID:  cancellationID,
			AttemptIdentity: identity,
			CancelState:     state,
		})
		require.NoError(t, err)
		require.NoError(t, conn.WriteJSON(ack))
	}
	require.Eventually(t, func() bool {
		return fixture.cancellations.ackCallCount() == 2
	}, 3*time.Second, 10*time.Millisecond)
}

func TestRuntimeWebSocketTerminalCancelAckReleasesCorrelation(t *testing.T) {
	fixture := newRuntimeWSTestFixture()
	identity := fixture.assignment().AttemptIdentity
	cancel := RunCancelPayload{
		CancellationID:  uuid.New(),
		AttemptIdentity: identity,
		ReasonCode:      runtimeCancellationReasonCode,
		DeadlineAt:      fixture.now.Add(30 * time.Second),
	}
	_, commandEnvelope, err := newRuntimeWSTypedMessage(RuntimeMessageRunCancel, nil, cancel)
	require.NoError(t, err)
	connection := &runtimeWSConnection{
		controller:       fixture.controller(),
		ctx:              context.Background(),
		sessionPrincipal: fixture.principal,
		cancellations:    make(map[uuid.UUID]runtimeWSCancellationCorrelation),
	}
	connection.recordCancellation(commandEnvelope, cancel)

	for _, state := range []RuntimeCancelState{RuntimeCancelStopping, RuntimeCancelStopped} {
		_, ackEnvelope, ackErr := newRuntimeWSTypedMessage(RuntimeMessageRunCancelAck, &commandEnvelope.MessageID, RunCancelAckPayload{
			CancellationID:  cancel.CancellationID,
			AttemptIdentity: identity,
			CancelState:     state,
		})
		require.NoError(t, ackErr)
		require.NoError(t, connection.handleRunCancelAck(ackEnvelope))
		connection.correlationMu.Lock()
		correlationCount := len(connection.cancellations)
		connection.correlationMu.Unlock()
		if state == RuntimeCancelStopping {
			require.Equal(t, 1, correlationCount)
		} else {
			require.Zero(t, correlationCount)
		}
	}
}

type runtimeWSTestFixture struct {
	now             time.Time
	authenticated   AuthenticatedRuntimePrincipal
	hello           RuntimeHelloPayload
	principal       RuntimeSessionPrincipal
	operations      *runtimeWSOperations
	tokens          *runtimeWSTokenValidatorFake
	devices         *runtimeWSDeviceAuthenticatorFake
	sessions        *runtimeWSSessionServiceFake
	leases          *runtimeWSLeaseServiceFake
	events          *runtimeWSEventStoreFake
	finalizer       *runtimeWSFinalizerFake
	resume          RuntimeResumeAPI
	cancellations   *runtimeWSCancellationServiceFake
	wakeHub         *RuntimeWakeHub
	transportPolicy RuntimeTransportPolicyProvider
}

func newRuntimeWSTestFixture() *runtimeWSTestFixture {
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
	attachmentID := uuid.New()
	principal := RuntimeSessionPrincipal{
		RuntimeSessionID:                uuid.New(),
		NodeID:                          authenticated.Device.NodeID,
		AgentID:                         authenticated.AgentID,
		CredentialID:                    authenticated.CredentialID,
		WorkerID:                        "worker-ws-1",
		SessionEpoch:                    3,
		RuntimeContractDigest:           RuntimeContractDigest,
		CoreInstanceID:                  uuid.New(),
		AttachmentID:                    attachmentID,
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
			RuntimeContractDigest:  principal.RuntimeContractDigest,
			Status:                 "active",
			AttachedCoreInstanceID: &coreID,
			HeartbeatAt:            now,
		},
		Attachment: &db.RuntimeSessionAttachment{
			ID:               attachmentID,
			RuntimeSessionID: principal.RuntimeSessionID,
			CoreInstanceID:   principal.CoreInstanceID,
			AttachmentKind:   "connected",
			AttachedAt:       now,
		},
		DatabaseTime: now,
	}
	operations := &runtimeWSOperations{}
	return &runtimeWSTestFixture{
		now:           now,
		authenticated: authenticated,
		hello:         hello,
		principal:     principal,
		operations:    operations,
		tokens: &runtimeWSTokenValidatorFake{token: db.AgentRuntimeToken{
			ID: principal.CredentialID, AgentID: principal.AgentID,
		}},
		devices:   &runtimeWSDeviceAuthenticatorFake{device: authenticated.Device},
		sessions:  &runtimeWSSessionServiceFake{state: state, principal: principal, operations: operations},
		leases:    &runtimeWSLeaseServiceFake{operations: operations},
		events:    &runtimeWSEventStoreFake{},
		finalizer: &runtimeWSFinalizerFake{},
		wakeHub:   NewRuntimeWakeHub(),
		cancellations: &runtimeWSCancellationServiceFake{
			databaseTime: now,
		},
	}
}

func (f *runtimeWSTestFixture) controller() *RuntimeHTTPController {
	return NewRuntimeHTTPController(RuntimeHTTPDependencies{
		TokenValidator:      f.tokens,
		DeviceAuthenticator: f.devices,
		TransportPolicy:     f.transportPolicy,
		Sessions:            f.sessions,
		Leases:              f.leases,
		EventProjector:      f.events,
		Finalizer:           f.finalizer,
		Resume:              f.resume,
		Cancellations:       f.cancellations,
		WakeHub:             f.wakeHub,
	})
}

func (f *runtimeWSTestFixture) server(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	e := echo.New()
	f.controller().Register(e.Group("/api/v1"))
	server := httptest.NewServer(e)
	target := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/agent-runtime/ws"
	return server, target
}

func (f *runtimeWSTestFixture) assignment() RunAssignedPayload {
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

func dialRuntimeWS(t *testing.T, target string) *websocket.Conn {
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

func writeRuntimeWSHello(t *testing.T, conn *websocket.Conn, hello RuntimeHelloPayload) RuntimeEnvelope {
	t.Helper()
	message, err := NewRuntimeTypedMessage(RuntimeMessageHello, nil, hello)
	require.NoError(t, err)
	require.NoError(t, conn.WriteJSON(message))
	return runtimeEnvelopeFromTyped(t, message)
}

func readRuntimeWSEnvelope(t *testing.T, conn *websocket.Conn) RuntimeEnvelope {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	messageType, frame, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, messageType)
	envelope, err := ParseRuntimeEnvelope(frame)
	require.NoError(t, err, string(frame))
	return envelope
}

func runtimeEnvelopeFromTyped[P any](t *testing.T, message RuntimeTypedEnvelope[P]) RuntimeEnvelope {
	t.Helper()
	payload, err := json.Marshal(message.Payload)
	require.NoError(t, err)
	return RuntimeEnvelope{RuntimeEnvelopeFields: message.RuntimeEnvelopeFields, Payload: payload}
}

func requireRuntimeWSCloseCode(t *testing.T, conn *websocket.Conn, expected int) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, expected, closeErr.Code, closeErr.Error())
}

type runtimeWSOperations struct {
	mu    sync.Mutex
	items []string
}

type runtimeWSManagedConnectionFake struct {
	shutdownOnce   sync.Once
	shutdownCalled chan struct{}
}

func (f *runtimeWSManagedConnectionFake) shutdownConnection() {
	f.shutdownOnce.Do(func() { close(f.shutdownCalled) })
}

func (f *runtimeWSManagedConnectionFake) shutdown() {
	f.shutdownConnection()
}

func (o *runtimeWSOperations) append(value string) {
	o.mu.Lock()
	o.items = append(o.items, value)
	o.mu.Unlock()
}

func (o *runtimeWSOperations) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.items...)
}

type runtimeWSTokenValidatorFake struct {
	mu        sync.Mutex
	token     db.AgentRuntimeToken
	err       error
	plaintext string
	scopes    []string
}

func (f *runtimeWSTokenValidatorFake) ValidateRuntimeToken(_ context.Context, plaintext string, scopes ...string) (db.AgentRuntimeToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plaintext = plaintext
	f.scopes = append([]string(nil), scopes...)
	return f.token, f.err
}

type runtimeWSDeviceAuthenticatorFake struct {
	mu     sync.Mutex
	device RuntimeDeviceIdentity
	err    error
	calls  int
}

func (f *runtimeWSDeviceAuthenticatorFake) AuthenticateHTTP(context.Context, *http.Request) (RuntimeDeviceIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.device, f.err
}

type runtimeWSSessionServiceFake struct {
	mu           sync.Mutex
	state        RuntimeSessionState
	principal    RuntimeSessionPrincipal
	operations   *runtimeWSOperations
	createErr    error
	resolveErr   error
	heartbeatErr error
	closeErr     error
	drainErr     error
	drainReceipt RuntimeDrainPayload
	drainRequest RuntimeSessionDrainRequest
	createCount  int
	created      RuntimeSessionRequest
	closeRequest RuntimeSessionCloseRequest
}

func (f *runtimeWSSessionServiceFake) CreateOrAttachSession(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionRequest) (RuntimeSessionState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCount++
	f.created = request
	return f.state, f.createErr
}

func (f *runtimeWSSessionServiceFake) HeartbeatSession(context.Context, AuthenticatedRuntimePrincipal, RuntimeSessionHeartbeatRequest) (RuntimeSessionState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state, f.heartbeatErr
}

func (f *runtimeWSSessionServiceFake) DrainSession(
	_ context.Context,
	_ AuthenticatedRuntimePrincipal,
	request RuntimeSessionDrainRequest,
) (RuntimeDrainPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drainRequest = request
	if f.drainReceipt.DeadlineAt.IsZero() {
		return request.Payload, f.drainErr
	}
	return f.drainReceipt, f.drainErr
}

func (f *runtimeWSSessionServiceFake) CloseSession(_ context.Context, _ AuthenticatedRuntimePrincipal, request RuntimeSessionCloseRequest) (RuntimeSessionState, error) {
	f.mu.Lock()
	f.closeRequest = request
	err := f.closeErr
	state := f.state
	stale := state.Attachment != nil && request.AttachmentID != state.Attachment.ID
	f.mu.Unlock()
	if f.operations != nil {
		if stale {
			f.operations.append("stale_close_session")
		} else {
			f.operations.append("close_session")
		}
	}
	if stale {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
	}
	return state, err
}

func (f *runtimeWSSessionServiceFake) replaceAttachment() uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	replacement := uuid.New()
	f.principal.AttachmentID = replacement
	if f.state.Attachment == nil {
		f.state.Attachment = &db.RuntimeSessionAttachment{}
	}
	f.state.Attachment.ID = replacement
	return replacement
}

func (f *runtimeWSSessionServiceFake) ResolveSessionPrincipal(context.Context, AuthenticatedRuntimePrincipal, uuid.UUID) (RuntimeSessionPrincipal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.principal, f.resolveErr
}

func (f *runtimeWSSessionServiceFake) ResolveWorkerSessionPrincipal(context.Context, AuthenticatedRuntimePrincipal, string) (RuntimeSessionPrincipal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.principal, f.resolveErr
}

func (f *runtimeWSSessionServiceFake) createCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createCount
}

func (f *runtimeWSSessionServiceFake) createdRequest() RuntimeSessionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created
}

func (f *runtimeWSSessionServiceFake) closedStatus() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeRequest.Status
}

type runtimeWSLeaseServiceFake struct {
	mu             sync.Mutex
	operations     *runtimeWSOperations
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

func (f *runtimeWSLeaseServiceFake) ClaimOffer(context.Context, RuntimeSessionPrincipal) (*RunAssignedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.assignment == nil {
		return nil, f.claimErr
	}
	copy := *f.assignment
	return &copy, f.claimErr
}

func (f *runtimeWSLeaseServiceFake) AckAssignment(_ context.Context, _ RuntimeSessionPrincipal, _ RunAssignmentAckPayload) (RunAssignmentConfirmedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackCalls++
	if f.ackErr == nil {
		f.assignment = nil
	}
	return f.ackResponse, f.ackErr
}

func (f *runtimeWSLeaseServiceFake) RejectAssignment(_ context.Context, _ RuntimeSessionPrincipal, _ RunAssignmentRejectPayload) (RunAssignmentRejectedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejectCalls++
	if f.rejectErr == nil {
		f.assignment = nil
	}
	return f.rejectResponse, f.rejectErr
}

func (f *runtimeWSLeaseServiceFake) RenewLease(_ context.Context, _ RuntimeSessionPrincipal, _ RunLeaseRenewPayload) (RunLeaseRenewedPayload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls++
	return f.renewResponse, f.renewErr
}

func (f *runtimeWSLeaseServiceFake) ReleaseUnackedOffer(_ context.Context, _ RuntimeSessionPrincipal, reason ...string) error {
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

func (f *runtimeWSLeaseServiceFake) setAssignment(assignment *RunAssignedPayload) {
	f.mu.Lock()
	f.assignment = assignment
	f.mu.Unlock()
}

func (f *runtimeWSLeaseServiceFake) setAckResult(response RunAssignmentConfirmedPayload, err error) {
	f.mu.Lock()
	f.ackResponse = response
	f.ackErr = err
	f.mu.Unlock()
}

func (f *runtimeWSLeaseServiceFake) ackCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ackCalls
}

func (f *runtimeWSLeaseServiceFake) rejectCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rejectCalls
}

func (f *runtimeWSLeaseServiceFake) renewCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewCalls
}

func (f *runtimeWSLeaseServiceFake) releaseReason() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releaseCode
}

type runtimeWSEventStoreFake struct {
	mu        sync.Mutex
	ack       RuntimeEventAck
	err       error
	calls     int
	principal RuntimeEventPrincipal
	identity  RuntimeAttemptIdentity
	request   RuntimeEventRequest
}

func (f *runtimeWSEventStoreFake) AppendRuntimeEvent(_ context.Context, principal RuntimeEventPrincipal, identity RuntimeAttemptIdentity, request RuntimeEventRequest) (RuntimeEventAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.principal = principal
	f.identity = identity
	f.request = request
	return f.ack, f.err
}

func (f *runtimeWSEventStoreFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *runtimeWSEventStoreFake) lastPrincipal() RuntimeEventPrincipal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.principal
}

type runtimeWSFinalizerFake struct {
	mu        sync.Mutex
	ack       RuntimeResultAck
	err       error
	calls     int
	principal RuntimeResultPrincipal
	request   RuntimeResultRequest
}

type runtimeWSResumeServiceFake struct {
	mu       sync.Mutex
	response RuntimeResumeResponse
	err      error
	calls    int
}

type runtimeWSCancellationServiceFake struct {
	mu           sync.Mutex
	command      *PendingCommand
	databaseTime time.Time
	nextErr      error
	ackState     RunCancellationState
	ackErr       error
	ackPayloads  []RunCancelAckPayload
}

func (f *runtimeWSCancellationServiceFake) NextCommand(
	context.Context,
	RuntimeSessionPrincipal,
) (*PendingCommand, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.command == nil {
		return nil, f.databaseTime, f.nextErr
	}
	command := *f.command
	command.Payload = append([]byte(nil), f.command.Payload...)
	return &command, f.databaseTime, f.nextErr
}

func (f *runtimeWSCancellationServiceFake) PollCommands(
	context.Context,
	RuntimeSessionPrincipal,
) (RuntimeCommandsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	commands := make([]PendingCommand, 0, 1)
	if f.command != nil {
		command := *f.command
		command.Payload = append([]byte(nil), f.command.Payload...)
		commands = append(commands, command)
	}
	return RuntimeCommandsResponse{Commands: commands, DatabaseTime: f.databaseTime}, f.nextErr
}

func (f *runtimeWSCancellationServiceFake) AckCancel(
	_ context.Context,
	_ RuntimeSessionPrincipal,
	payload RunCancelAckPayload,
) (RunCancellationState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackPayloads = append(f.ackPayloads, payload)
	return f.ackState, f.ackErr
}

func (f *runtimeWSCancellationServiceFake) setCommand(command PendingCommand) {
	f.mu.Lock()
	f.command = &command
	f.mu.Unlock()
}

func (f *runtimeWSCancellationServiceFake) ackCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ackPayloads)
}

func (f *runtimeWSResumeServiceFake) Resume(context.Context, RuntimeSessionPrincipal, RuntimeResumePayload) (RuntimeResumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.response, f.err
}

func (f *runtimeWSResumeServiceFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *runtimeWSFinalizerFake) Finalize(_ context.Context, principal RuntimeResultPrincipal, request RuntimeResultRequest) (RuntimeResultAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.principal = principal
	f.request = request
	return f.ack, f.err
}

func (f *runtimeWSFinalizerFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *runtimeWSFinalizerFake) lastPrincipal() RuntimeResultPrincipal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.principal
}

var (
	_ RuntimeTokenValidator      = (*runtimeWSTokenValidatorFake)(nil)
	_ RuntimeDeviceAuthenticator = (*runtimeWSDeviceAuthenticatorFake)(nil)
	_ RuntimeSessionAPI          = (*runtimeWSSessionServiceFake)(nil)
	_ RuntimeLeaseAPI            = (*runtimeWSLeaseServiceFake)(nil)
	_ RuntimeEventProjector      = (*runtimeWSEventStoreFake)(nil)
	_ RuntimeResultFinalizer     = (*runtimeWSFinalizerFake)(nil)
	_ RuntimeResumeAPI           = (*runtimeWSResumeServiceFake)(nil)
	_ RuntimeCancellationAPI     = (*runtimeWSCancellationServiceFake)(nil)
)
