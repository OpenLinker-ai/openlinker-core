package runtime

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeResultFingerprintCanonicalContract(t *testing.T) {
	identity := runtimeResultTestIdentity()
	resultID := uuid.New()
	request := RuntimeResultRequest{
		AttemptIdentity:     identity,
		ResultID:            resultID,
		Status:              "success",
		Output:              map[string]any{"z": float64(1), "a": []any{"ok", true}},
		DurationMS:          125,
		FinalClientEventSeq: 3,
	}

	first, err := RuntimeResultFingerprint(request)
	require.NoError(t, err)
	reordered := request
	reordered.Output = map[string]any{"a": []any{"ok", true}, "z": float64(1)}
	second, err := RuntimeResultFingerprint(reordered)
	require.NoError(t, err)
	require.Equal(t, first, second)

	// Result identity, immutable Attempt identity, and transport envelopes are
	// deliberately compared outside the semantic payload digest.
	excluded := reordered
	excluded.ResultID = uuid.New()
	excluded.AttemptIdentity.AttemptID = uuid.New()
	excluded.AttemptIdentity.LeaseID = uuid.New()
	excluded.AttemptIdentity.FencingToken++
	third, err := RuntimeResultFingerprint(excluded)
	require.NoError(t, err)
	require.Equal(t, first, third)

	mutations := []func(*RuntimeResultRequest){
		func(v *RuntimeResultRequest) { v.Output["z"] = float64(2) },
		func(v *RuntimeResultRequest) { v.DurationMS++ },
		func(v *RuntimeResultRequest) { v.FinalClientEventSeq++ },
	}
	for index, mutate := range mutations {
		candidate := request
		candidate.Output = map[string]any{"z": float64(1), "a": []any{"ok", true}}
		mutate(&candidate)
		got, err := RuntimeResultFingerprint(candidate)
		require.NoError(t, err, "mutation %d", index)
		require.NotEqual(t, first, got, "mutation %d", index)
	}
}

func TestRuntimeResultFingerprintNormalizesRetryableHintDefault(t *testing.T) {
	request := RuntimeResultRequest{
		AttemptIdentity: runtimeResultTestIdentity(),
		ResultID:        uuid.New(),
		Status:          "failed",
		Error: &RuntimeResultFailure{
			ErrorCode: "TEMPORARY_UNAVAILABLE",
			Message:   "try again",
		},
		DurationMS:          9,
		FinalClientEventSeq: 0,
	}
	implicit, err := RuntimeResultFingerprint(request)
	require.NoError(t, err)
	request.Error.RetryableHint = false
	explicit, err := RuntimeResultFingerprint(request)
	require.NoError(t, err)
	require.Equal(t, implicit, explicit)

	request.Error.RetryableHint = true
	retryable, err := RuntimeResultFingerprint(request)
	require.NoError(t, err)
	require.NotEqual(t, implicit, retryable)
}

func TestRuntimeResultRequestStrictValidation(t *testing.T) {
	validSuccess := RuntimeResultRequest{
		AttemptIdentity: runtimeResultTestIdentity(),
		ResultID:        uuid.New(),
		Status:          "success",
		Output:          map[string]any{},
	}
	validFailure := RuntimeResultRequest{
		AttemptIdentity: runtimeResultTestIdentity(),
		ResultID:        uuid.New(),
		Status:          "failed",
		Error: &RuntimeResultFailure{
			ErrorCode: "POLICY_REJECTED",
			Message:   "request rejected",
		},
	}
	require.NoError(t, validateRuntimeResultRequest(validSuccess))
	require.NoError(t, validateRuntimeResultRequest(validFailure))

	cases := map[string]RuntimeResultRequest{
		"nil result ID":        func() RuntimeResultRequest { v := validSuccess; v.ResultID = uuid.Nil; return v }(),
		"missing output":       func() RuntimeResultRequest { v := validSuccess; v.Output = nil; return v }(),
		"success with error":   func() RuntimeResultRequest { v := validSuccess; v.Error = validFailure.Error; return v }(),
		"failed with output":   func() RuntimeResultRequest { v := validFailure; v.Output = map[string]any{}; return v }(),
		"failed without error": func() RuntimeResultRequest { v := validFailure; v.Error = nil; return v }(),
		"blank error code": func() RuntimeResultRequest {
			v := validFailure
			copy := *v.Error
			copy.ErrorCode = "  "
			v.Error = &copy
			return v
		}(),
		"blank error message": func() RuntimeResultRequest {
			v := validFailure
			copy := *v.Error
			copy.Message = "\n"
			v.Error = &copy
			return v
		}(),
		"negative duration":        func() RuntimeResultRequest { v := validSuccess; v.DurationMS = -1; return v }(),
		"negative final sequence":  func() RuntimeResultRequest { v := validSuccess; v.FinalClientEventSeq = -1; return v }(),
		"overflow final sequence":  func() RuntimeResultRequest { v := validSuccess; v.FinalClientEventSeq = math.MaxInt64; return v }(),
		"invalid Attempt identity": func() RuntimeResultRequest { v := validSuccess; v.AttemptIdentity.LeaseID = uuid.Nil; return v }(),
		"unsupported status":       func() RuntimeResultRequest { v := validSuccess; v.Status = "timeout"; return v }(),
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateRuntimeResultRequest(request)
			require.True(t, IsRuntimeResultError(err, RuntimeResultErrorValidationFailed), "%v", err)
		})
	}

	nonIJSON := validSuccess
	nonIJSON.Output = map[string]any{"not_finite": math.Inf(1)}
	_, err := RuntimeResultFingerprint(nonIJSON)
	require.True(t, IsRuntimeResultError(err, RuntimeResultErrorValidationFailed), "%v", err)
}

func TestDefaultResultClassifierUsesServerPolicy(t *testing.T) {
	classifier := defaultResultClassifier{}
	success := RuntimeResultRequest{Status: "success"}
	retryable := RuntimeResultRequest{
		Status: "failed",
		Error:  &RuntimeResultFailure{RetryableHint: true},
	}
	permanent := RuntimeResultRequest{
		Status: "failed",
		Error:  &RuntimeResultFailure{RetryableHint: false},
	}

	require.Equal(t, RuntimeResultClassificationSuccess, classifier.ClassifyResult(ResultClassificationInput{
		ExecutorType: "runtime", Request: success,
	}))
	require.Equal(t, RuntimeResultClassificationRetryable, classifier.ClassifyResult(ResultClassificationInput{
		ExecutorType: "runtime", Request: retryable,
	}))
	require.Equal(t, RuntimeResultClassificationNonRetryable, classifier.ClassifyResult(ResultClassificationInput{
		ExecutorType: "runtime", Request: permanent,
	}))
	require.Equal(t, RuntimeResultClassificationNonRetryable, classifier.ClassifyResult(ResultClassificationInput{
		ExecutorType: "core_http", EndpointIdempotency: false, Request: retryable,
	}))
	require.Equal(t, RuntimeResultClassificationRetryable, classifier.ClassifyResult(ResultClassificationInput{
		ExecutorType: "core_http", EndpointIdempotency: true, Request: retryable,
	}))
	require.Equal(t, RuntimeResultClassificationRetryable, classifier.ClassifyResult(ResultClassificationInput{
		ExecutorType: "core_mcp", EndpointIdempotency: true, Request: retryable,
	}))
}

func TestFixedResultRetryPlannerAndTerminalEventIdentity(t *testing.T) {
	planner := fixedResultRetryPlanner{}
	require.Equal(t, time.Second, planner.NextRetryDelay(1))
	require.Equal(t, 2*time.Second, planner.NextRetryDelay(2))
	require.Equal(t, 32*time.Second, planner.NextRetryDelay(6))
	require.Equal(t, 60*time.Second, planner.NextRetryDelay(7))
	require.Equal(t, 60*time.Second, planner.NextRetryDelay(20))

	runID := uuid.New()
	first := deterministicTerminalEventID(runID, "success")
	require.Equal(t, first, deterministicTerminalEventID(runID, "success"))
	require.NotEqual(t, first, deterministicTerminalEventID(runID, "failed"))
	require.NotEqual(t, first, deterministicTerminalEventID(uuid.New(), "success"))
	require.Equal(t, byte(5), first[6]>>4)
}

func TestRuntimeResultErrorHelpers(t *testing.T) {
	cause := errors.New("database rejected write")
	err := newRuntimeResultError(RuntimeResultErrorResultIDConflict, cause)
	require.True(t, IsRuntimeResultError(err, RuntimeResultErrorResultIDConflict))
	require.ErrorIs(t, err, cause)

	ranges := []EventRange{{Start: 2, End: 4}}
	err = missingRuntimeResultEvents(ranges)
	var resultErr *RuntimeResultError
	require.ErrorAs(t, err, &resultErr)
	require.Equal(t, ranges, resultErr.MissingRanges)
	ranges[0].Start = 99
	require.Equal(t, int64(2), resultErr.MissingRanges[0].Start)
}

func TestRuntimeResultReplayAckUsesCurrentPublicTerminalState(t *testing.T) {
	resultID := uuid.New()
	attemptID := uuid.New()
	classification := string(RuntimeResultClassificationRetryable)
	attempt := db.RunAttempt{
		ID:                   attemptID,
		ResultID:             &resultID,
		ResultClassification: &classification,
	}

	cases := []struct {
		name     string
		run      runtimeResultRun
		expected RuntimeResultClassification
	}{
		{name: "retry state", run: runtimeResultRun{status: "running", dispatchState: "retry_wait"}, expected: RuntimeResultClassificationRetryable},
		{name: "later success", run: runtimeResultRun{status: "success", dispatchState: "terminal"}, expected: RuntimeResultClassificationSuccess},
		{name: "later permanent failure", run: runtimeResultRun{status: "failed", dispatchState: "terminal"}, expected: RuntimeResultClassificationNonRetryable},
		{name: "later timeout", run: runtimeResultRun{status: "timeout", dispatchState: "terminal"}, expected: RuntimeResultClassificationTimeout},
		{name: "later cancel", run: runtimeResultRun{status: "canceled", dispatchState: "terminal"}, expected: RuntimeResultClassificationCanceled},
		{name: "retry exhausted", run: runtimeResultRun{status: "failed", dispatchState: "dead_letter", latestAttemptID: &attemptID}, expected: RuntimeResultClassificationDeadLetter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ack, err := runtimeResultAckFromStored(tc.run, attempt, true)
			require.NoError(t, err)
			require.True(t, ack.Replayed)
			require.Equal(t, tc.expected, ack.Classification)
		})
	}
}

func TestTerminalRuntimeResultPayloadPreservesProductResult(t *testing.T) {
	success := RuntimeResultRequest{
		ResultID:   uuid.New(),
		DurationMS: 12,
		Output:     map[string]any{"answer": "visible to callback"},
	}
	payload := terminalRuntimeResultPayload(success, "success", RuntimeResultClassificationSuccess, "")
	require.Equal(t, success.Output, payload["output"])
	require.Equal(t, true, payload["terminal"])
	require.NotEmpty(t, payload["result_id"])

	failure := RuntimeResultRequest{
		ResultID:   uuid.New(),
		DurationMS: 15,
		Error: &RuntimeResultFailure{
			ErrorCode: "POLICY_REJECTED",
			Message:   "public safe failure",
		},
	}
	payload = terminalRuntimeResultPayload(
		failure, "failed", RuntimeResultClassificationNonRetryable, "POLICY_REJECTED",
	)
	require.Equal(t, "POLICY_REJECTED", payload["error_code"])
	require.Equal(t, "public safe failure", payload["error_message"])
	require.NotContains(t, payload, "output")

	payload = terminalRuntimeResultPayload(
		failure, "timeout", RuntimeResultClassificationTimeout, "RUN_DEADLINE_EXCEEDED",
	)
	require.Equal(t, "RUN_DEADLINE_EXCEEDED", payload["error_code"])
	require.Equal(t, "Run deadline exceeded", payload["error_message"])
	require.NotContains(t, fmt.Sprint(payload), failure.Error.ErrorCode)
	require.NotContains(t, fmt.Sprint(payload), failure.Error.Message)
}

func runtimeResultTestIdentity() RuntimeAttemptIdentity {
	workerID := "worker-finalizer-test"
	nodeID := uuid.New()
	sessionID := uuid.New()
	return RuntimeAttemptIdentity{
		RunID:            uuid.New(),
		AttemptID:        uuid.New(),
		LeaseID:          uuid.New(),
		FencingToken:     1,
		NodeID:           &nodeID,
		AgentID:          uuid.New(),
		WorkerID:         &workerID,
		RuntimeSessionID: &sessionID,
	}
}
