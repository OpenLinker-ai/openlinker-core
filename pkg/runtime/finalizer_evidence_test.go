package runtime

import (
	"testing"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeResultEvidenceAttribution(t *testing.T) {
	mode := func(value string) *string { return &value }
	coreTarget := &db.AgentEvidenceTarget{ConnectionMode: connectionModeDirectHTTP, EndpointURL: "https://a.example/x"}

	legacy := runtimeResultEvidenceFor(runtimeResultRun{}, nil)
	if !legacy.legacy || legacy.target != nil {
		t.Fatalf("pre-v2 Runs keep the unconditional write: %#v", legacy)
	}

	queued := runtimeResultEvidenceFor(runtimeResultRun{connectionMode: mode(connectionModeRuntime)}, coreTarget)
	if queued.legacy || queued.target == nil || queued.target.ConnectionMode != connectionModeRuntime ||
		queued.target.EndpointURL != "" {
		t.Fatalf("Runtime Runs are attributed to the Agent's Runtime queue: %#v", queued)
	}

	core := runtimeResultEvidenceFor(runtimeResultRun{connectionMode: mode(connectionModeDirectHTTP)}, coreTarget)
	if core.target == nil || *core.target != *coreTarget {
		t.Fatalf("in-process Core Results carry the executed target: %#v", core)
	}
	if core.target == coreTarget {
		t.Fatal("the attributed target must be a copy")
	}

	for name, evidence := range map[string]runtimeResultEvidence{
		"reconciled Core Attempt": runtimeResultEvidenceFor(runtimeResultRun{connectionMode: mode(connectionModeDirectHTTP)}, nil),
		"mode mismatch":           runtimeResultEvidenceFor(runtimeResultRun{connectionMode: mode(connectionModeMCPServer)}, coreTarget),
	} {
		if evidence.legacy || evidence.target != nil {
			t.Fatalf("%s must not record availability: %#v", name, evidence)
		}
	}
}
