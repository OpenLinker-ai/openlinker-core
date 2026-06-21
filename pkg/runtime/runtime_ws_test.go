package runtime

import (
	"context"
	"strings"
	"testing"
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
