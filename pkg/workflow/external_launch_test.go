package workflow

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestExternalWorkflowLaunchFenceRejectsIncompleteIdentity(t *testing.T) {
	svc := &Service{}
	_, err := svc.StartExternalExecutionWorkflowRunWithFence(
		context.Background(), uuid.New(), uuid.New(), uuid.New(), nil,
		ExternalExecutionLaunchFence{},
	)
	if err == nil {
		t.Fatal("incomplete external workflow launch fence was accepted")
	}
}
