package runtime

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
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

func TestRuntimeWSOriginPolicy(t *testing.T) {
	prodSvc := &Service{cfg: &config.Config{
		Env:         "production",
		FrontendURL: "https://app.example",
		APIURL:      "https://api.example",
	}}
	if !prodSvc.checkRuntimeWSOrigin(&http.Request{Header: http.Header{}}) {
		t.Fatal("non-browser runtime client without Origin should be allowed")
	}
	if !prodSvc.checkRuntimeWSOrigin(&http.Request{Header: http.Header{"Origin": {"https://app.example"}}}) {
		t.Fatal("configured frontend origin should be allowed")
	}
	if prodSvc.checkRuntimeWSOrigin(&http.Request{Header: http.Header{"Origin": {"https://evil.example"}}}) {
		t.Fatal("unexpected production origin should be denied")
	}

	devSvc := &Service{cfg: &config.Config{Env: "development"}}
	if !devSvc.checkRuntimeWSOrigin(&http.Request{Header: http.Header{"Origin": {"http://localhost:3000"}}}) {
		t.Fatal("local development origin should be allowed")
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
		AgentTokenType:    "ol_agent",
		AgentScopes:       []string{"agent:call"},
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
		Conversation: &ConversationContext{
			ID:           "conv-1",
			SessionKey:   "conv-1",
			CurrentRunID: runID,
			Source:       "core",
			HistoryBeforeCurrent: []ConversationMessage{{
				RunID:   uuid.NewString(),
				Role:    "user",
				Content: "previous",
			}},
		},
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
	if msg.Conversation == nil || msg.Conversation.SessionKey != "conv-1" || msg.Conversation.HistoryBeforeCurrent[0].Content != "previous" {
		t.Fatalf("assignment message conversation = %#v", msg.Conversation)
	}
}

func TestRuntimeWSHeartbeatDispatchDecision(t *testing.T) {
	cases := []struct {
		name      string
		heartbeat *AgentHeartbeatResponse
		want      bool
	}{
		{name: "nil", heartbeat: nil, want: false},
		{name: "empty", heartbeat: &AgentHeartbeatResponse{}, want: false},
		{name: "claim now without pending count", heartbeat: &AgentHeartbeatResponse{ClaimNow: true}, want: false},
		{name: "pending count without claim now", heartbeat: &AgentHeartbeatResponse{PendingRunCount: 1}, want: false},
		{name: "claimable pending run", heartbeat: &AgentHeartbeatResponse{ClaimNow: true, PendingRunCount: 1}, want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := runtimeWSHeartbeatShouldDispatch(tc.heartbeat); got != tc.want {
				t.Fatalf("runtimeWSHeartbeatShouldDispatch() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRuntimeWSConnSendServerMessageDoesNotBlockWhenBufferFull(t *testing.T) {
	conn := &runtimeWSConn{
		send:   make(chan runtimeWSSendRequest, 1),
		closed: make(chan struct{}),
	}
	conn.send <- runtimeWSSendRequest{msg: RuntimeWSServerMessage{Type: "queued"}, result: make(chan error, 1)}

	err := conn.sendServerMessage(context.Background(), RuntimeWSServerMessage{Type: "overflow"})
	if err == nil || !strings.Contains(err.Error(), "send buffer full") {
		t.Fatalf("sendServerMessage() error = %v, want send buffer full", err)
	}
}

func TestRuntimeWSConnSendServerMessageSuccessAndDispatchNoHub(t *testing.T) {
	conn := &runtimeWSConn{
		send:   make(chan runtimeWSSendRequest, 1),
		closed: make(chan struct{}),
	}
	want := RuntimeWSServerMessage{Type: "runtime.ready", AgentID: uuid.NewString()}
	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.sendServerMessage(context.Background(), want)
	}()
	got := readSentWSMessage(t, conn)
	if got.Type != want.Type || got.AgentID != want.AgentID {
		t.Fatalf("sent message = %#v, want %#v", got, want)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("sendServerMessage() error = %v", err)
	}

	(&Service{}).dispatchRuntimeWSRunBestEffort(context.Background(), uuid.New())
	(&Service{wsHub: newRuntimeWSHub()}).dispatchRuntimeWSRunBestEffort(context.Background(), uuid.New())
}

func TestRuntimeWSConnSendServerMessageClosedAndCanceled(t *testing.T) {
	conn := &runtimeWSConn{
		send:   make(chan runtimeWSSendRequest, 1),
		closed: make(chan struct{}),
	}
	conn.close()
	if err := conn.sendServerMessage(context.Background(), RuntimeWSServerMessage{Type: "late"}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("sendServerMessage() error = %v, want closed", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	conn = &runtimeWSConn{
		send:   make(chan runtimeWSSendRequest, 1),
		closed: make(chan struct{}),
	}
	conn.send <- runtimeWSSendRequest{msg: RuntimeWSServerMessage{Type: "queued"}, result: make(chan error, 1)}
	if err := conn.sendServerMessage(canceled, RuntimeWSServerMessage{Type: "late"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("sendServerMessage() error = %v, want context.Canceled", err)
	}
}

func TestRuntimeWSConnRunSlotReservationSkipsClosedConnection(t *testing.T) {
	conn := &runtimeWSConn{closed: make(chan struct{})}
	close(conn.closed)

	if conn.tryReserveRunSlot(uuid.Nil) {
		t.Fatal("tryReserveRunSlot() = true, want false for closed connection")
	}
	if len(conn.inFlightRuns) != 0 {
		t.Fatalf("inFlightRuns len = %d, want 0", len(conn.inFlightRuns))
	}
}

func TestRuntimeWSConnRunAssignmentStateDistinguishesReleasedSlot(t *testing.T) {
	runID := uuid.New()
	conn := &runtimeWSConn{closed: make(chan struct{})}

	if present, acked := conn.runAssignmentState(runID); present || acked {
		t.Fatalf("empty runAssignmentState() = present %v acked %v, want false false", present, acked)
	}
	if !conn.tryReserveRunSlot(runID) {
		t.Fatal("tryReserveRunSlot() = false, want true")
	}
	if present, acked := conn.runAssignmentState(runID); !present || acked {
		t.Fatalf("reserved runAssignmentState() = present %v acked %v, want true false", present, acked)
	}
	if !conn.markRunAssignmentAcked(runID) {
		t.Fatal("markRunAssignmentAcked() = false, want true")
	}
	if present, acked := conn.runAssignmentState(runID); !present || !acked {
		t.Fatalf("acked runAssignmentState() = present %v acked %v, want true true", present, acked)
	}
	conn.releaseRunSlot(runID)
	if present, acked := conn.runAssignmentState(runID); present || acked {
		t.Fatalf("released runAssignmentState() = present %v acked %v, want false false", present, acked)
	}
}

func TestRuntimeWSConnReplaceReservedRunSlotAfterCloseDoesNotPanic(t *testing.T) {
	conn := &runtimeWSConn{closed: make(chan struct{})}
	if !conn.tryReserveRunSlot(uuid.Nil) {
		t.Fatal("tryReserveRunSlot() = false, want true")
	}

	conn.releaseAllRunClaims(context.Background(), "test")
	close(conn.closed)

	if conn.replaceReservedRunSlot(uuid.Nil, uuid.New()) {
		t.Fatal("replaceReservedRunSlot() = true, want false for closed connection")
	}
	if len(conn.inFlightRuns) != 0 {
		t.Fatalf("inFlightRuns len = %d, want 0", len(conn.inFlightRuns))
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
		send:   make(chan runtimeWSSendRequest, 4),
		closed: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		svc.handleRuntimeWSClientMessage(context.Background(), conn, &RuntimeWSClientMessage{
			Type: "unknown",
			ID:   "unknown-1",
		})
		close(done)
	}()
	got := readSentWSMessage(t, conn)
	<-done
	if got.Type != "error" || got.ID != "unknown-1" || got.Error == nil || got.Error.Code != "UNKNOWN_WS_MESSAGE" {
		t.Fatalf("unknown message response = %#v", got)
	}

	done = make(chan struct{})
	go func() {
		svc.handleRuntimeWSClientMessage(context.Background(), conn, &RuntimeWSClientMessage{
			Type:  "run.event",
			ID:    "event-1",
			RunID: "not-a-uuid",
		})
		close(done)
	}()
	got = readSentWSMessage(t, conn)
	<-done
	if got.Type != "error" || got.ID != "event-1" || got.Error == nil || got.Error.Code != string(httpx.CodeBadRequest) {
		t.Fatalf("run.event invalid uuid response = %#v", got)
	}
	if !strings.Contains(got.Error.Message, "run_id") {
		t.Fatalf("run.event invalid uuid message = %q, want run_id context", got.Error.Message)
	}

	done = make(chan struct{})
	go func() {
		svc.handleRuntimeWSClientMessage(context.Background(), conn, &RuntimeWSClientMessage{
			Type:  "run.result",
			ID:    "result-1",
			RunID: "not-a-uuid",
		})
		close(done)
	}()
	got = readSentWSMessage(t, conn)
	<-done
	if got.Type != "error" || got.ID != "result-1" || got.Error == nil || got.Error.Code != string(httpx.CodeBadRequest) {
		t.Fatalf("run.result invalid uuid response = %#v", got)
	}
}

func readSentWSMessage(t *testing.T, conn *runtimeWSConn) RuntimeWSServerMessage {
	t.Helper()
	select {
	case req := <-conn.send:
		req.result <- nil
		return req.msg
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for websocket send request")
		return RuntimeWSServerMessage{}
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
