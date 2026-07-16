package runtime

import "testing"

func TestNormalizePhysicalCancellationEvidenceNeverTreatsNegativeAckAsStopped(t *testing.T) {
	for _, state := range []string{"unconfirmed", "unsupported", "failed", "unknown"} {
		mapped, _ := normalizePhysicalCancellationEvidence(state, nil)
		if mapped != "unconfirmed" {
			t.Fatalf("state %q mapped to %q, want unconfirmed", state, mapped)
		}
	}
	for _, state := range []string{"requested", "delivered", "stopping"} {
		mapped, _ := normalizePhysicalCancellationEvidence(state, nil)
		if mapped != "stopping" {
			t.Fatalf("state %q mapped to %q, want stopping", state, mapped)
		}
	}
	mapped, _ := normalizePhysicalCancellationEvidence("stopped", nil)
	if mapped != "stopped" {
		t.Fatalf("stopped mapped to %q", mapped)
	}
}
