package workflow

import (
	"context"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
)

func TestClaimWorkflowChildLaunchRejectsIncompleteEvidence(t *testing.T) {
	svc := &Service{}
	_, err := svc.claimWorkflowChildLaunch(
		context.Background(), dbWorkflowRunIdentity{ID: uuid.New(), UserID: uuid.New()},
		uuid.New(), uuid.New(), "node", runtime.RunCreationIdentity{},
	)
	if err == nil {
		t.Fatal("incomplete child creation evidence was accepted")
	}
}
