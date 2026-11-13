package workflow

import (
	"context"

	"github.com/google/uuid"
)

// Test-only access to the attempt transitions for integration tests in
// package workflow_test.

var ErrWorkflowRunLeaseLostForTest = errWorkflowRunLeaseLost

func (s *Service) InstanceIDForTest() uuid.UUID { return s.instanceID }

func WithWorkflowRunLeaseForTest(ctx context.Context, runID, owner uuid.UUID, epoch int64) context.Context {
	return withWorkflowRunLease(ctx, workflowRunLease{runID: runID, owner: owner, epoch: epoch})
}

func (s *Service) FailAttemptLocalForTest(ctx context.Context, runID, nodeID uuid.UUID, attempt int32, message string) error {
	return s.failAttemptLocal(ctx, runID, nodeID, attempt, message)
}
