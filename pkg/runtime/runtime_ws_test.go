package runtime

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestRuntimeWSHubTracksConnectionsByAgent(t *testing.T) {
	hub := newRuntimeWSHub()
	agentA := uuid.New()
	agentB := uuid.New()
	connA1 := &runtimeWSConn{token: db.AgentRuntimeToken{AgentID: agentA}}
	connA2 := &runtimeWSConn{token: db.AgentRuntimeToken{AgentID: agentA}}
	connB := &runtimeWSConn{token: db.AgentRuntimeToken{AgentID: agentB}}

	hub.unregister(connA1)
	hub.register(connA1)
	hub.register(connA2)
	hub.register(connB)

	if got := len(hub.connections(agentA)); got != 2 {
		t.Fatalf("agentA connections = %d, want 2", got)
	}
	if got := len(hub.connections(agentB)); got != 1 {
		t.Fatalf("agentB connections = %d, want 1", got)
	}

	hub.unregister(connA1)
	if got := len(hub.connections(agentA)); got != 1 {
		t.Fatalf("agentA connections after unregister = %d, want 1", got)
	}
	hub.unregister(connA2)
	if got := len(hub.connections(agentA)); got != 0 {
		t.Fatalf("agentA connections after final unregister = %d, want 0", got)
	}
}

func TestRuntimeQueuedModeAndAssignmentMessage(t *testing.T) {
	if !isQueuedRuntimeMode(connectionModeRuntimePull) || !isQueuedRuntimeMode(connectionModeRuntimeWS) || isQueuedRuntimeMode(connectionModeDirectHTTP) {
		t.Fatal("queued runtime mode detection failed")
	}
	if (&Service{}).isRuntimePull(nil) {
		t.Fatal("nil invocation should not be runtime pull")
	}
	if !(&Service{}).isRuntimePull(&runInvocation{agent: db.Agent{ConnectionMode: connectionModeRuntimeWS}}) {
		t.Fatal("runtime_ws invocation should be queued runtime")
	}

	runID := uuid.NewString()
	agentID := uuid.NewString()
	a2a := &AgentA2AContext{
		CurrentRunID:      runID,
		CallAgentEndpoint: "https://api.example.com/api/v1/agent-runtime/call-agent",
		CallAgentMethod:   http.MethodPost,
		RuntimeTokenType:  "ol_live",
		RuntimeScopes:     []string{"agent:call"},
	}
	msg := runtimeWSAssignmentMessage(&RuntimePullRunResponse{
		RunID:          runID,
		AgentID:        agentID,
		Input:          map[string]interface{}{"q": "hello"},
		Metadata:       map[string]interface{}{"trace_id": "trace-1"},
		Source:         "api",
		ResultEndpoint: "/api/v1/agent-runtime/runs/" + runID + "/result",
		ResultMethod:   http.MethodPost,
		ResultRequired: true,
		A2A:            a2a,
	})
	if msg.Type != "run.assigned" || msg.RunID != runID || msg.AgentID != agentID || msg.Source != "api" {
		t.Fatalf("assignment message basic fields = %#v", msg)
	}
	if _, err := uuid.Parse(msg.ID); err != nil {
		t.Fatalf("assignment message id = %q, want uuid: %v", msg.ID, err)
	}
	if msg.Input["q"] != "hello" || msg.Metadata["trace_id"] != "trace-1" || msg.ResultMethod != http.MethodPost || !msg.ResultRequired {
		t.Fatalf("assignment message payload = %#v", msg)
	}
	if msg.A2A != a2a || msg.A2A.CallAgentEndpoint == "" {
		t.Fatalf("assignment message a2a context = %#v", msg.A2A)
	}
}

func TestRuntimeWSConnSendServerMessageDoesNotBlockWhenBufferFull(t *testing.T) {
	conn := &runtimeWSConn{
		send:   make(chan RuntimeWSServerMessage, 1),
		closed: make(chan struct{}),
	}
	conn.send <- RuntimeWSServerMessage{Type: "queued"}

	err := conn.sendServerMessage(context.Background(), RuntimeWSServerMessage{Type: "overflow"})
	if err == nil || !strings.Contains(err.Error(), "send buffer full") {
		t.Fatalf("sendServerMessage() error = %v, want send buffer full", err)
	}
}

func TestRuntimeWSConnSendServerMessageSuccessAndDispatchNoHub(t *testing.T) {
	conn := &runtimeWSConn{
		send:   make(chan RuntimeWSServerMessage, 1),
		closed: make(chan struct{}),
	}
	want := RuntimeWSServerMessage{Type: "runtime.ready", AgentID: uuid.NewString()}
	if err := conn.sendServerMessage(context.Background(), want); err != nil {
		t.Fatalf("sendServerMessage() error = %v", err)
	}
	if got := <-conn.send; got.Type != want.Type || got.AgentID != want.AgentID {
		t.Fatalf("sent message = %#v, want %#v", got, want)
	}

	(&Service{}).dispatchRuntimeWSRunBestEffort(context.Background(), uuid.New())
	(&Service{wsHub: newRuntimeWSHub()}).dispatchRuntimeWSRunBestEffort(context.Background(), uuid.New())
}

func TestRuntimeWSConnSendServerMessageClosedAndCanceled(t *testing.T) {
	conn := &runtimeWSConn{
		send:   make(chan RuntimeWSServerMessage, 1),
		closed: make(chan struct{}),
	}
	conn.close()
	if err := conn.sendServerMessage(context.Background(), RuntimeWSServerMessage{Type: "late"}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("sendServerMessage() error = %v, want closed", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	conn = &runtimeWSConn{
		send:   make(chan RuntimeWSServerMessage, 1),
		closed: make(chan struct{}),
	}
	conn.send <- RuntimeWSServerMessage{Type: "queued"}
	if err := conn.sendServerMessage(canceled, RuntimeWSServerMessage{Type: "late"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("sendServerMessage() error = %v, want context.Canceled", err)
	}
}

func TestRuntimeWSErrorMessageMapsHTTPAndGenericErrors(t *testing.T) {
	msg := runtimeWSErrorMessage("req-1", httpx.BadRequest("bad runtime ws message"))
	if msg.Type != "error" || msg.ID != "req-1" || msg.Error == nil {
		t.Fatalf("runtimeWSErrorMessage() = %#v, want error with id", msg)
	}
	if msg.Error.Code != string(httpx.CodeBadRequest) || msg.Error.Message != "bad runtime ws message" {
		t.Fatalf("runtimeWSErrorMessage() error = %#v, want mapped HTTP error", msg.Error)
	}

	msg = runtimeWSErrorMessage("req-2", errors.New("plain failure"))
	if msg.Error == nil || msg.Error.Code != "RUNTIME_WS_ERROR" || msg.Error.Message != "plain failure" {
		t.Fatalf("runtimeWSErrorMessage() generic error = %#v", msg.Error)
	}
}

func TestRuntimeWSClientMessageProtocolErrors(t *testing.T) {
	svc := &Service{}
	conn := &runtimeWSConn{
		send:   make(chan RuntimeWSServerMessage, 4),
		closed: make(chan struct{}),
	}

	svc.handleRuntimeWSClientMessage(context.Background(), conn, &RuntimeWSClientMessage{
		Type: "unknown",
		ID:   "unknown-1",
	})
	got := <-conn.send
	if got.Type != "error" || got.ID != "unknown-1" || got.Error == nil || got.Error.Code != "UNKNOWN_WS_MESSAGE" {
		t.Fatalf("unknown message response = %#v", got)
	}

	svc.handleRuntimeWSClientMessage(context.Background(), conn, &RuntimeWSClientMessage{
		Type:  "run.event",
		ID:    "event-1",
		RunID: "not-a-uuid",
	})
	got = <-conn.send
	if got.Type != "error" || got.ID != "event-1" || got.Error == nil || got.Error.Code != string(httpx.CodeBadRequest) {
		t.Fatalf("run.event invalid uuid response = %#v", got)
	}
	if !strings.Contains(got.Error.Message, "run_id") {
		t.Fatalf("run.event invalid uuid message = %q, want run_id context", got.Error.Message)
	}

	svc.handleRuntimeWSClientMessage(context.Background(), conn, &RuntimeWSClientMessage{
		Type:  "run.result",
		ID:    "result-1",
		RunID: "not-a-uuid",
	})
	got = <-conn.send
	if got.Type != "error" || got.ID != "result-1" || got.Error == nil || got.Error.Code != string(httpx.CodeBadRequest) {
		t.Fatalf("run.result invalid uuid response = %#v", got)
	}
}

func TestRuntimeWSHandlerRejectsMissingBearerToken(t *testing.T) {
	h := NewHandler(&mockRuntimeService{})
	c, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodGet,
		target: "/api/v1/agent-runtime/ws",
	})

	err := h.RuntimeWebSocket(c)
	if err == nil {
		t.Fatal("RuntimeWebSocket() error = nil, want unauthorized")
	}
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusUnauthorized {
		t.Fatalf("RuntimeWebSocket() error = %T %v, want unauthorized HTTPError", err, err)
	}
}
