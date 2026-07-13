package workflow

import (
	"testing"

	"github.com/google/uuid"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestHostedWorkflowRunMatchesSemanticIdentity(t *testing.T) {
	workflowID, buyerID := uuid.New(), uuid.New()
	run := db.WorkflowRun{
		WorkflowID:  workflowID,
		UserID:      buyerID,
		Input:       []byte(`{"count":2,"topic":"Go"}`),
		MaxAttempts: defaultWorkflowRunMaxAttempts,
	}
	if !hostedWorkflowRunMatches(run, workflowID, buyerID, map[string]interface{}{"topic": "Go", "count": 2}, defaultWorkflowRunMaxAttempts) {
		t.Fatalf("same semantic input should match regardless of key order or Go number type")
	}
	if hostedWorkflowRunMatches(run, workflowID, uuid.New(), map[string]interface{}{"topic": "Go", "count": 2}, defaultWorkflowRunMaxAttempts) {
		t.Fatalf("different buyer must not reuse hosted workflow run")
	}
	if hostedWorkflowRunMatches(run, workflowID, buyerID, map[string]interface{}{"topic": "Rust", "count": 2}, defaultWorkflowRunMaxAttempts) {
		t.Fatalf("different input must not reuse hosted workflow run")
	}
}
