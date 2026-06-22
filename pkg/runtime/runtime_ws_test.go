package runtime

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

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
