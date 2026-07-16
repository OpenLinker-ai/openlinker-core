package workflow

import (
	"testing"

	"github.com/google/uuid"
)

func TestWorkflowCancellationIDIsDeterministic(t *testing.T) {
	runID := uuid.New()
	first := deterministicWorkflowCancellationID(runID)
	if first == uuid.Nil || first != deterministicWorkflowCancellationID(runID) {
		t.Fatal("workflow cancellation ID is not deterministic")
	}
	if first == deterministicWorkflowCancellationID(uuid.New()) {
		t.Fatal("workflow cancellation ID is not run isolated")
	}
}
