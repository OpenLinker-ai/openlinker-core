package a2a

import (
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Each heartbeat must land inside the rolling write window, or an idle stream
// would hit its write deadline between heartbeats.
func TestA2ASSEHeartbeatFitsStreamWriteWindow(t *testing.T) {
	if a2aSSEHeartbeat >= httpx.StreamWriteWindow {
		t.Fatalf("a2aSSEHeartbeat %s must be shorter than httpx.StreamWriteWindow %s", a2aSSEHeartbeat, httpx.StreamWriteWindow)
	}
}
