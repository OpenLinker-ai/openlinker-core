package runtime

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeEventFingerprintCanonicalEnvelope(t *testing.T) {
	firstID := uuid.New()
	secondID := uuid.New()
	left := RuntimeEventRequest{
		ClientEventID:  firstID,
		ClientEventSeq: 7,
		EventType:      "run.message.delta",
		Payload: map[string]any{
			"text": "你好",
			"meta": map[string]any{"z": 2, "a": 1},
		},
	}
	right := RuntimeEventRequest{
		ClientEventID:  secondID,
		ClientEventSeq: 7,
		EventType:      "run.message.delta",
		Payload: map[string]any{
			"meta": map[string]any{"a": 1, "z": 2},
			"text": "你好",
		},
	}

	leftFingerprint, err := RuntimeEventFingerprint(left)
	require.NoError(t, err)
	rightFingerprint, err := RuntimeEventFingerprint(right)
	require.NoError(t, err)
	require.Equal(t, leftFingerprint, rightFingerprint, "client_event_id and map iteration order are outside the fingerprint")
	require.Len(t, leftFingerprint, sha256DigestSize)

	mutations := []RuntimeEventRequest{
		{
			ClientEventID:  secondID,
			ClientEventSeq: 8,
			EventType:      left.EventType,
			Payload:        left.Payload,
		},
		{
			ClientEventID:  secondID,
			ClientEventSeq: left.ClientEventSeq,
			EventType:      "run.tool.delta",
			Payload:        left.Payload,
		},
		{
			ClientEventID:  secondID,
			ClientEventSeq: left.ClientEventSeq,
			EventType:      left.EventType,
			Payload:        map[string]any{"text": "different"},
		},
	}
	for _, mutation := range mutations {
		got, err := RuntimeEventFingerprint(mutation)
		require.NoError(t, err)
		assert.NotEqual(t, leftFingerprint, got)
	}
}

func TestRuntimeEventFingerprintRejectsNonIJSONAndMalformedRequests(t *testing.T) {
	valid := RuntimeEventRequest{
		ClientEventID:  uuid.New(),
		ClientEventSeq: 1,
		EventType:      "run.progress.changed",
		Payload:        map[string]any{},
	}

	tests := []struct {
		name    string
		mutate  func(*RuntimeEventRequest)
		wantErr error
	}{
		{name: "missing event id", mutate: func(req *RuntimeEventRequest) { req.ClientEventID = uuid.Nil }, wantErr: ErrInvalidRuntimeEvent},
		{name: "sequence starts at one", mutate: func(req *RuntimeEventRequest) { req.ClientEventSeq = 0 }, wantErr: ErrInvalidRuntimeEvent},
		{name: "invalid event type", mutate: func(req *RuntimeEventRequest) { req.EventType = "progress" }, wantErr: ErrInvalidRuntimeEvent},
		{name: "completed is Core owned", mutate: func(req *RuntimeEventRequest) { req.EventType = "run.completed" }, wantErr: ErrInvalidRuntimeEvent},
		{name: "failed is Core owned", mutate: func(req *RuntimeEventRequest) { req.EventType = "run.failed" }, wantErr: ErrInvalidRuntimeEvent},
		{name: "canceled is Core owned", mutate: func(req *RuntimeEventRequest) { req.EventType = "run.canceled" }, wantErr: ErrInvalidRuntimeEvent},
		{name: "stream gap is synthetic", mutate: func(req *RuntimeEventRequest) { req.EventType = "run.stream.gap" }, wantErr: ErrInvalidRuntimeEvent},
		{name: "payload must be object", mutate: func(req *RuntimeEventRequest) { req.Payload = nil }, wantErr: ErrInvalidRuntimeEvent},
		{name: "non finite number", mutate: func(req *RuntimeEventRequest) { req.Payload = map[string]any{"value": math.Inf(1)} }, wantErr: ErrInvalidRuntimeEvent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := valid
			test.mutate(&req)
			_, err := RuntimeEventFingerprint(req)
			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

func TestRuntimeEventPayloadLimitUsesCanonicalBytes(t *testing.T) {
	request := RuntimeEventRequest{
		ClientEventID:  uuid.New(),
		ClientEventSeq: 1,
		EventType:      "run.progress.changed",
		// Canonical {"x":""} is eight bytes before string content.
		Payload: map[string]any{"x": strings.Repeat("a", MaxRuntimeEventPayloadBytes-8)},
	}
	canonical, err := canonicalRuntimeEventPayload(request)
	require.NoError(t, err)
	require.Len(t, canonical, MaxRuntimeEventPayloadBytes)
	_, err = RuntimeEventFingerprint(request)
	require.NoError(t, err)

	request.Payload["x"] = strings.Repeat("a", MaxRuntimeEventPayloadBytes-7)
	_, err = canonicalRuntimeEventPayload(request)
	require.ErrorIs(t, err, ErrInvalidRuntimeEvent)
	_, err = RuntimeEventFingerprint(request)
	require.ErrorIs(t, err, ErrInvalidRuntimeEvent)
}

func TestRuntimeAttemptIdentityWireShapeDoesNotInventAttemptNumber(t *testing.T) {
	nodeID := uuid.New()
	sessionID := uuid.New()
	workerID := "worker-a"
	identity := RuntimeAttemptIdentity{
		RunID:            uuid.New(),
		AttemptID:        uuid.New(),
		LeaseID:          uuid.New(),
		FencingToken:     3,
		NodeID:           &nodeID,
		AgentID:          uuid.New(),
		WorkerID:         &workerID,
		RuntimeSessionID: &sessionID,
	}

	encoded, err := json.Marshal(identity)
	require.NoError(t, err)
	var object map[string]any
	require.NoError(t, json.Unmarshal(encoded, &object))
	assert.NotContains(t, object, "attempt_no")
	require.NoError(t, validateRuntimeAttemptIdentity(identity))
}

func TestRuntimeExecutorIdentityShapeSupportsNodeAndCoreAttempts(t *testing.T) {
	nodeID := uuid.New()
	sessionID := uuid.New()
	workerID := "worker-a"

	require.True(t, runtimeExecutorIdentityShapeValid("agent_node", &nodeID, &sessionID, &workerID))
	require.False(t, runtimeExecutorIdentityShapeValid("agent_node", nil, nil, nil))
	require.True(t, runtimeExecutorIdentityShapeValid("core_http", nil, nil, nil))
	require.True(t, runtimeExecutorIdentityShapeValid("core_mcp", nil, nil, nil))
	require.False(t, runtimeExecutorIdentityShapeValid("core_http", &nodeID, &sessionID, &workerID))
	require.False(t, runtimeExecutorIdentityShapeValid("unknown", nil, nil, nil))

	identity := RuntimeAttemptIdentity{
		RunID:        uuid.New(),
		AttemptID:    uuid.New(),
		LeaseID:      uuid.New(),
		FencingToken: 1,
		AgentID:      uuid.New(),
	}
	require.NoError(t, validateRuntimeAttemptIdentity(identity), "Core executor identity may omit node/session/worker")

	identity.NodeID = &nodeID
	require.ErrorIs(t, validateRuntimeAttemptIdentity(identity), ErrInvalidRuntimeEvent, "partial Node identity is never accepted")
}

func TestStoredRuntimeEventMatchCoversFingerprintSequenceAndAttemptIdentity(t *testing.T) {
	nodeID := uuid.New()
	sessionID := uuid.New()
	workerID := "worker-a"
	attemptNo := int32(2)
	identity := RuntimeAttemptIdentity{
		RunID:            uuid.New(),
		AttemptID:        uuid.New(),
		LeaseID:          uuid.New(),
		FencingToken:     9,
		NodeID:           &nodeID,
		AgentID:          uuid.New(),
		WorkerID:         &workerID,
		RuntimeSessionID: &sessionID,
	}
	fingerprint := make([]byte, sha256DigestSize)
	for i := range fingerprint {
		fingerprint[i] = byte(i + 1)
	}
	stored := runtimeStoredEvent{
		clientEventSeq:     4,
		payloadFingerprint: append([]byte(nil), fingerprint...),
		attemptID:          identity.AttemptID,
		attemptNo:          attemptNo,
		fencingToken:       identity.FencingToken,
		attempt: runtimeEventAttempt{
			id:               identity.AttemptID,
			runID:            identity.RunID,
			agentID:          identity.AgentID,
			attemptNo:        &attemptNo,
			leaseID:          identity.LeaseID,
			fencingToken:     identity.FencingToken,
			workerID:         &workerID,
			runtimeSessionID: &sessionID,
			nodeID:           &nodeID,
		},
	}

	require.True(t, storedRuntimeEventMatches(stored, identity, 4, fingerprint))

	changedFingerprint := append([]byte(nil), fingerprint...)
	changedFingerprint[0] ^= 0xff
	require.False(t, storedRuntimeEventMatches(stored, identity, 4, changedFingerprint))
	require.False(t, storedRuntimeEventMatches(stored, identity, 5, fingerprint))
	changedIdentity := identity
	changedIdentity.LeaseID = uuid.New()
	require.False(t, storedRuntimeEventMatches(stored, changedIdentity, 4, fingerprint))
}

func TestRuntimeEventDeadlineBoundaryUsesDatabaseClock(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Second)
	require.True(t, runtimeEventDeadlinesValid(now, future, future, future))
	require.False(t, runtimeEventDeadlinesValid(now, now, future, future))
	require.False(t, runtimeEventDeadlinesValid(now, future, now, future))
	require.False(t, runtimeEventDeadlinesValid(now, future, future, now))
}

func TestRuntimeEventStableErrorsAreMachineReadable(t *testing.T) {
	err := newRuntimeEventError(RuntimeEventErrorIDConflict, errors.New("detail"))
	require.True(t, IsRuntimeEventError(err, RuntimeEventErrorIDConflict))
	require.False(t, IsRuntimeEventError(err, RuntimeEventErrorStaleLease))
	require.NotContains(t, err.Error(), "detail")
	require.Error(t, errors.Unwrap(err))

	missing := &RuntimeEventError{
		Code: RuntimeEventErrorEventsMissing,
		MissingRanges: []EventRange{
			{Start: 2, End: 3},
		},
	}
	require.True(t, IsRuntimeEventError(missing, RuntimeEventErrorEventsMissing))
	require.Equal(t, []EventRange{{Start: 2, End: 3}}, missing.MissingRanges)
}

const sha256DigestSize = 32
