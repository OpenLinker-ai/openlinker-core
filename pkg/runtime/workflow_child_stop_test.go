package runtime

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClassifyWorkflowChildStop(t *testing.T) {
	natural := func(outcome, code string) workflowChildStopAttempt {
		return workflowChildStopAttempt{id: uuid.New(), accepted: true, finished: true, outcome: outcome, hasResult: true, errorCode: code}
	}
	neverStarted := func(outcome string) workflowChildStopAttempt {
		return workflowChildStopAttempt{id: uuid.New(), finished: true, outcome: outcome}
	}
	cases := []struct {
		name         string
		status       string
		attempts     []workflowChildStopAttempt
		cancellation string
		want         WorkflowChildStopState
	}{
		{"running run", "running", nil, "", WorkflowChildStopInProgress},
		{"canceled run with unfinished attempt", "canceled",
			[]workflowChildStopAttempt{{id: uuid.New(), accepted: true}}, "delivered", WorkflowChildStopInProgress},
		{"cancellation requested", "canceled", []workflowChildStopAttempt{neverStarted("canceled")}, "requested", WorkflowChildStopInProgress},
		{"cancellation delivered", "canceled", []workflowChildStopAttempt{neverStarted("canceled")}, "delivered", WorkflowChildStopInProgress},
		{"cancellation stopping", "canceled", []workflowChildStopAttempt{neverStarted("canceled")}, "stopping", WorkflowChildStopInProgress},
		{"natural success", "success", []workflowChildStopAttempt{natural("success", "")}, "", WorkflowChildStopConfirmed},
		{"natural failure", "failed", []workflowChildStopAttempt{natural("non_retryable_failure", "AGENT_FAILED")}, "", WorkflowChildStopConfirmed},
		{"offer expired then success", "success",
			[]workflowChildStopAttempt{neverStarted("offer_expired"), natural("success", "")}, "", WorkflowChildStopConfirmed},
		{"offer rejected only", "timeout", []workflowChildStopAttempt{neverStarted("offer_rejected")}, "", WorkflowChildStopConfirmed},
		{"canceled before acceptance", "canceled", []workflowChildStopAttempt{neverStarted("canceled")}, "stopped", WorkflowChildStopConfirmed},
		{"pending run canceled, no attempts", "canceled", nil, "stopped", WorkflowChildStopConfirmed},
		{"accepted then canceled and stopped", "canceled",
			[]workflowChildStopAttempt{{id: uuid.New(), accepted: true, finished: true, outcome: "canceled"}}, "stopped", WorkflowChildStopUnconfirmed},
		{"lease expired", "failed",
			[]workflowChildStopAttempt{{id: uuid.New(), accepted: true, finished: true, outcome: "lease_expired"}}, "", WorkflowChildStopUnconfirmed},
		{"result unknown", "failed",
			[]workflowChildStopAttempt{{id: uuid.New(), accepted: true, finished: true, outcome: "result_unknown", errorCode: "ENDPOINT_RESULT_UNKNOWN"}}, "", WorkflowChildStopUnconfirmed},
		{"presumed timeout", "timeout",
			[]workflowChildStopAttempt{{id: uuid.New(), accepted: true, finished: true, outcome: "timeout"}}, "", WorkflowChildStopUnconfirmed},
		{"natural result with unknown code", "failed", []workflowChildStopAttempt{natural("non_retryable_failure", "ENDPOINT_RESULT_UNKNOWN")}, "", WorkflowChildStopUnconfirmed},
		{"natural result naming itself unknown", "failed", []workflowChildStopAttempt{natural("non_retryable_failure", "UPSTREAM_STATE_UNKNOWN")}, "", WorkflowChildStopUnconfirmed},
		{"cancellation unconfirmed", "canceled",
			[]workflowChildStopAttempt{{id: uuid.New(), accepted: true, finished: true, outcome: "canceled"}}, "unconfirmed", WorkflowChildStopUnconfirmed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyWorkflowChildStop(tc.status, tc.attempts, tc.cancellation).State)
		})
	}

	// A natural result is not natural when a cancellation targeted it.
	targeted := natural("success", "")
	targeted.canceledBy = true
	require.Equal(t, WorkflowChildStopUnconfirmed,
		classifyWorkflowChildStop("canceled", []workflowChildStopAttempt{targeted}, "stopped").State)

	evidence := classifyWorkflowChildStop("canceled",
		[]workflowChildStopAttempt{{id: uuid.New(), accepted: true, finished: true, outcome: "canceled"}}, "unsupported")
	require.True(t, evidence.CancellationUnconfirmed)
	require.True(t, evidence.RunTerminal)
}

func TestRuntimeResultUnknownCodesAreNotDefinite(t *testing.T) {
	for code := range runtimeResultUnknownErrorCodes {
		require.False(t, runtimeResultCodeIsDefinite(code), code)
	}
	require.True(t, runtimeResultCodeIsDefinite(""))
	require.True(t, runtimeResultCodeIsDefinite("VALIDATION_FAILED"))
	require.True(t, runtimeResultCodeIsDefinite("AGENT_REPORTED_ERROR"))
}
