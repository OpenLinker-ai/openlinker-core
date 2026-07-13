package servicebridge

import (
	"encoding/json"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestExecutionStatusResponseUsesCloudFlatContract(t *testing.T) {
	payload, err := json.Marshal(ExecutionStatusResponse{
		ExternalOrderID: "order-id",
		ExecutionID:     "execution-id",
		TargetType:      TargetTypeAgent,
		Status:          "failed",
		Artifacts:       []runtime.RunArtifactResponse{},
		ErrorCode:       "EXECUTION_FAILED",
		ErrorMessage:    "Execution failed.",
		StartedAt:       "2026-07-13T00:00:00Z",
		FinishedAt:      "2026-07-13T00:01:00Z",
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	for _, key := range []string{"execution_id", "status", "artifacts", "error_code", "error_message", "started_at", "finished_at"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("flat Cloud contract missing %q: %s", key, payload)
		}
	}
	if _, nested := decoded["error"]; nested {
		t.Fatalf("legacy nested error must not be emitted: %s", payload)
	}
	if _, nested := decoded["timestamps"]; nested {
		t.Fatalf("legacy nested timestamps must not be emitted: %s", payload)
	}
}
