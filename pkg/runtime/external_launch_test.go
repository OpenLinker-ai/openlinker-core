package runtime

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestLookupRunByCreationIdentityRejectsMalformedEvidenceBeforeDatabase(t *testing.T) {
	svc := &Service{}
	for _, testCase := range []struct {
		name        string
		userID      uuid.UUID
		keyHash     []byte
		fingerprint []byte
	}{
		{name: "missing actor", keyHash: make([]byte, 32), fingerprint: make([]byte, 32)},
		{name: "short key", userID: uuid.New(), keyHash: make([]byte, 31), fingerprint: make([]byte, 32)},
		{name: "short fingerprint", userID: uuid.New(), keyHash: make([]byte, 32), fingerprint: make([]byte, 31)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, _, err := svc.LookupRunByCreationIdentity(
				context.Background(), testCase.userID, testCase.keyHash, testCase.fingerprint,
			); err == nil {
				t.Fatal("malformed durable identity was accepted")
			}
		})
	}
}

func TestExternalLaunchFenceRejectsIncompleteIdentity(t *testing.T) {
	svc := &Service{}
	_, err := svc.StartExternalRun(context.Background(), uuid.New(), &RunRequest{}, "api", ExternalExecutionLaunchFence{})
	if err == nil {
		t.Fatal("incomplete external launch fence was accepted")
	}
}

func TestWorkflowChildLaunchFenceRejectsIncompleteIdentity(t *testing.T) {
	svc := &Service{}
	_, err := svc.RunWorkflowChild(context.Background(), uuid.New(), &RunRequest{}, "api", WorkflowChildLaunchFence{})
	if err == nil {
		t.Fatal("incomplete workflow child launch fence was accepted")
	}
}
