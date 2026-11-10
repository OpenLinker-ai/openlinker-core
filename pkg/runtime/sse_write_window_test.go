package runtime

import (
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Each heartbeat must land inside the rolling write window, or an idle stream
// would hit its write deadline between heartbeats.
func TestSSEHeartbeatFitsStreamWriteWindow(t *testing.T) {
	if sseHeartbeatInterval >= httpx.StreamWriteWindow {
		t.Fatalf("sseHeartbeatInterval %s must be shorter than httpx.StreamWriteWindow %s", sseHeartbeatInterval, httpx.StreamWriteWindow)
	}
}
