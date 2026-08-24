package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDecodeRuntimeEnvelopeStrict(t *testing.T) {
	t.Parallel()

	messageID := uuid.New()
	sentAt := time.Now().UTC().Format(time.RFC3339Nano)
	valid := `{
		"protocol_version":2,
		"runtime_contract_id":"openlinker.runtime.v2",
		"message_id":"` + messageID.String() + `",
		"type":"runtime.hello",
		"sent_at":"` + sentAt + `",
		"payload":{}
	}`

	tests := []struct {
		name string
		body string
		code RuntimeErrorCode
	}{
		{name: "valid", body: valid},
		{name: "empty body", body: "", code: RuntimeErrorValidationFailed},
		{name: "unknown envelope field", body: strings.Replace(valid, `"payload":{}`, `"unexpected":true,"payload":{}`, 1), code: RuntimeErrorValidationFailed},
		{name: "bad message uuid", body: strings.Replace(valid, messageID.String(), "not-a-uuid", 1), code: RuntimeErrorValidationFailed},
		{name: "zero message uuid", body: strings.Replace(valid, messageID.String(), uuid.Nil.String(), 1), code: RuntimeErrorValidationFailed},
		{name: "wrong protocol", body: strings.Replace(valid, `"protocol_version":2`, `"protocol_version":1`, 1), code: RuntimeErrorClientUpgradeRequired},
		{name: "wrong contract id", body: strings.Replace(valid, RuntimeContractID, "openlinker.runtime.v3", 1), code: RuntimeErrorClientUpgradeRequired},
		{name: "unknown message type", body: strings.Replace(valid, string(RuntimeMessageHello), "runtime.future", 1), code: RuntimeErrorValidationFailed},
		{name: "payload is null", body: strings.Replace(valid, `"payload":{}`, `"payload":null`, 1), code: RuntimeErrorValidationFailed},
		{name: "second json value", body: valid + `{}`, code: RuntimeErrorValidationFailed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope, err := DecodeRuntimeEnvelope(strings.NewReader(test.body))
			if test.code == "" {
				require.NoError(t, err)
				require.Equal(t, messageID, envelope.MessageID)
				return
			}
			requireRuntimeTransportCode(t, err, test.code)
		})
	}
}

func TestRuntimeDecoderRejectsOversizeCompleteBody(t *testing.T) {
	t.Parallel()

	body := bytes.NewReader(bytes.Repeat([]byte{' '}, int(MaxRuntimeMessageBytes)+1))
	_, err := DecodeRuntimeBody[RuntimeClaimRequest](body)
	requireRuntimeTransportCode(t, err, RuntimeErrorBadRequest)
}

func TestDecodeRuntimeBodyStrictPayload(t *testing.T) {
	t.Parallel()

	sessionID := uuid.New()
	tests := []struct {
		name string
		body string
	}{
		{
			name: "unknown field",
			body: `{"runtime_session_id":"` + sessionID.String() + `","capacity":0,"inflight":0,"extra":true}`,
		},
		{
			name: "required zero field omitted",
			body: `{"runtime_session_id":"` + sessionID.String() + `","inflight":0}`,
		},
		{
			name: "second payload",
			body: `{"runtime_session_id":"` + sessionID.String() + `","capacity":0,"inflight":0}{}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeRuntimeBody[RuntimeClaimRequest](strings.NewReader(test.body))
			requireRuntimeTransportCode(t, err, RuntimeErrorValidationFailed)
		})
	}
}

func TestRuntimeFramePayloadsAcceptCanonicalBase64ByteSlices(t *testing.T) {
	t.Parallel()
	captured := time.Now().UTC()
	observer := observerIdentity()
	tests := []struct {
		name        string
		messageType RuntimeMessageType
		payload     any
		decode      func(RuntimeEnvelope) error
	}{
		{
			name:        "authenticated observation",
			messageType: RuntimeMessageBrowserObserverEvent,
			payload: BrowserObserverEventPayload{
				AttemptIdentity:      observer.RuntimeIdentity(),
				SessionEpoch:         observer.SessionEpoch,
				BrowserSessionSHA256: observer.BrowserSessionSHA256,
				AttachmentSHA256:     observer.AttachmentSHA256,
				CommandID:            uuid.New(),
				LeaseID:              uuid.New(),
				EventSeq:             2,
				Kind:                 BrowserObserverFrame,
				CapturedAt:           &captured,
				Frame: &BrowserObserverFramePayload{
					MIMEType: "image/jpeg",
					Data:     []byte{0xff, 0xd8, 0xff, 0xd9},
					Width:    1280,
					Height:   720,
				},
			},
			decode: func(envelope RuntimeEnvelope) error {
				_, err := DecodeRuntimeMessagePayload[BrowserObserverEventPayload](
					envelope,
					RuntimeMessageBrowserObserverEvent,
				)
				return err
			},
		},
		{
			name:        "human-control Viewer",
			messageType: RuntimeMessageBrowserViewerFrame,
			payload: BrowserViewerFramePayload{
				AttemptIdentity:  runtimeTestAttemptIdentity(),
				BrowserSessionID: uuid.New(),
				SessionEpoch:     1,
				AttachmentID:     uuid.New(),
				ControlEpoch:     1,
				FrameSeq:         1,
				MIMEType:         "image/jpeg",
				Data:             []byte{0xff, 0xd8, 0xff, 0xd9},
				Width:            1280,
				Height:           720,
			},
			decode: func(envelope RuntimeEnvelope) error {
				_, err := DecodeRuntimeMessagePayload[BrowserViewerFramePayload](
					envelope,
					RuntimeMessageBrowserViewerFrame,
				)
				return err
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(test.payload)
			require.NoError(t, err)
			require.Contains(t, string(raw), `"data":"/9j/2Q=="`)
			envelope := runtimeTestEnvelope(test.messageType, nil)
			envelope.Payload = raw
			require.NoError(t, test.decode(envelope))

			// Arrays are accepted by encoding/json for []byte, but they are not
			// the canonical Go JSON wire shape and must not become a second
			// representation that contract clients have to support.
			envelope.Payload = bytes.Replace(
				raw,
				[]byte(`"data":"/9j/2Q=="`),
				[]byte(`"data":[255,216,255,217]`),
				1,
			)
			requireRuntimeTransportCode(
				t,
				test.decode(envelope),
				RuntimeErrorValidationFailed,
			)
		})
	}
}

func TestDecodeRuntimeTypedMessageRejectsUnknownPayloadField(t *testing.T) {
	t.Parallel()

	hello := validRuntimeHelloPayload()
	payload, err := json.Marshal(hello)
	require.NoError(t, err)
	payload = bytes.Replace(payload, []byte(`}`), []byte(`,"unexpected":true}`), 1)
	body := `{
		"protocol_version":2,
		"runtime_contract_id":"` + RuntimeContractID + `",
		"message_id":"` + uuid.NewString() + `",
		"type":"runtime.hello",
		"sent_at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `",
		"payload":` + string(payload) + `
	}`

	_, err = DecodeRuntimeTypedMessage[RuntimeHelloPayload](strings.NewReader(body), RuntimeMessageHello)
	requireRuntimeTransportCode(t, err, RuntimeErrorValidationFailed)
}

func TestValidateRuntimeHelloContractNegotiation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*RuntimeHelloPayload)
		code   RuntimeErrorCode
	}{
		{name: "valid"},
		{
			name: "wrong digest",
			mutate: func(payload *RuntimeHelloPayload) {
				payload.ContractDigest = strings.Repeat("0", 64)
			},
			code: RuntimeErrorClientUpgradeRequired,
		},
		{
			name: "required feature missing",
			mutate: func(payload *RuntimeHelloPayload) {
				payload.Features = payload.Features[:len(payload.Features)-1]
			},
			code: RuntimeErrorRequiredFeatureMissing,
		},
		{
			name: "duplicate feature",
			mutate: func(payload *RuntimeHelloPayload) {
				payload.Features = append(payload.Features, payload.Features[0])
			},
			code: RuntimeErrorValidationFailed,
		},
		{
			name: "zero node id",
			mutate: func(payload *RuntimeHelloPayload) {
				payload.NodeID = uuid.Nil
			},
			code: RuntimeErrorValidationFailed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := validRuntimeHelloPayload()
			if test.mutate != nil {
				test.mutate(&payload)
			}
			err := ValidateRuntimePayload(payload)
			if test.code == "" {
				require.NoError(t, err)
				return
			}
			requireRuntimeTransportCode(t, err, test.code)
		})
	}
}

func TestValidateRuntimeReplyCorrelation(t *testing.T) {
	t.Parallel()

	request := runtimeTestEnvelope(RuntimeMessageRunAssigned, nil)
	requestID := request.MessageID
	otherID := uuid.New()

	tests := []struct {
		name  string
		reply RuntimeEnvelope
		code  RuntimeErrorCode
	}{
		{name: "assignment ack", reply: runtimeTestEnvelope(RuntimeMessageAssignmentAck, &requestID)},
		{name: "assignment reject", reply: runtimeTestEnvelope(RuntimeMessageAssignmentReject, &requestID)},
		{name: "business error", reply: runtimeTestEnvelope(RuntimeMessageError, &requestID)},
		{name: "wrong reply id", reply: runtimeTestEnvelope(RuntimeMessageAssignmentAck, &otherID), code: RuntimeErrorValidationFailed},
		{name: "wrong reply type", reply: runtimeTestEnvelope(RuntimeMessageRunEventAck, &requestID), code: RuntimeErrorValidationFailed},
		{name: "missing reply id", reply: runtimeTestEnvelope(RuntimeMessageAssignmentAck, nil), code: RuntimeErrorValidationFailed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateRuntimeReplyCorrelation(request, test.reply)
			if test.code == "" {
				require.NoError(t, err)
				return
			}
			requireRuntimeTransportCode(t, err, test.code)
		})
	}
}

func TestRuntimeResumePreservesSourceAttemptSessionDuringTakeover(t *testing.T) {
	identity := runtimeTestAttemptIdentity()
	payload := RuntimeResumePayload{
		NodeID:           identity.NodeID,
		AgentID:          identity.AgentID,
		WorkerID:         identity.WorkerID,
		RuntimeSessionID: uuid.New(),
		Attempts: []ResumeAttempt{{
			AttemptIdentity:          identity,
			PendingClientEventRanges: []EventRange{},
		}},
	}
	require.NoError(t, ValidateRuntimePayload(payload))

	payload.AgentID = uuid.New()
	requireRuntimeTransportCode(t, ValidateRuntimePayload(payload), RuntimeErrorValidationFailed)
}

func TestRuntimeResumeRejectsAmbiguousSpoolState(t *testing.T) {
	t.Parallel()

	identity := runtimeTestAttemptIdentity()
	resultID := uuid.New()
	finalSequence := int64(5)
	tests := []struct {
		name    string
		attempt ResumeAttempt
	}{
		{
			name: "range overlaps acknowledged prefix",
			attempt: ResumeAttempt{AttemptIdentity: identity, LastAckedClientEventSeq: 2,
				PendingClientEventRanges: []EventRange{{Start: 2, End: 3}}},
		},
		{
			name: "ranges overlap",
			attempt: ResumeAttempt{AttemptIdentity: identity,
				PendingClientEventRanges: []EventRange{{Start: 2, End: 4}, {Start: 4, End: 5}}},
		},
		{
			name: "ranges are unsorted",
			attempt: ResumeAttempt{AttemptIdentity: identity,
				PendingClientEventRanges: []EventRange{{Start: 5, End: 6}, {Start: 2, End: 3}}},
		},
		{
			name: "result missing final sequence",
			attempt: ResumeAttempt{AttemptIdentity: identity, PendingClientEventRanges: []EventRange{},
				PendingResultID: &resultID},
		},
		{
			name: "final sequence missing result",
			attempt: ResumeAttempt{AttemptIdentity: identity, PendingClientEventRanges: []EventRange{},
				FinalClientEventSeq: &finalSequence},
		},
		{
			name: "final sequence precedes pending events",
			attempt: ResumeAttempt{AttemptIdentity: identity,
				PendingClientEventRanges: []EventRange{{Start: 4, End: 6}}, PendingResultID: &resultID,
				FinalClientEventSeq: &finalSequence},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireRuntimeTransportCode(t, ValidateRuntimePayload(test.attempt), RuntimeErrorValidationFailed)
		})
	}
}

func TestRuntimeResumeRejectsDuplicateAttemptIdentity(t *testing.T) {
	t.Parallel()

	identity := runtimeTestAttemptIdentity()
	attempt := ResumeAttempt{AttemptIdentity: identity, PendingClientEventRanges: []EventRange{}}
	payload := RuntimeResumePayload{
		NodeID: identity.NodeID, AgentID: identity.AgentID, WorkerID: identity.WorkerID,
		RuntimeSessionID: uuid.New(), Attempts: []ResumeAttempt{attempt, attempt},
	}
	requireRuntimeTransportCode(t, ValidateRuntimePayload(payload), RuntimeErrorValidationFailed)
}

func TestRuntimeResumeDecisionActionsAreCoherent(t *testing.T) {
	t.Parallel()

	identity := runtimeTestAttemptIdentity()
	expiresAt := time.Now().UTC().Add(time.Minute)
	valid := []RunResumeAcceptedPayload{
		{AttemptIdentity: identity, Decision: RuntimeResumeContinueExecution, LeaseExpiresAt: &expiresAt,
			AllowedActions: []RuntimeResumeAction{RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult}},
		{AttemptIdentity: identity, Decision: RuntimeResumeUploadSpoolOnly,
			AllowedActions: []RuntimeResumeAction{RuntimeActionUploadResult}},
		{AttemptIdentity: identity, Decision: RuntimeResumeResultAcked,
			AllowedActions: []RuntimeResumeAction{RuntimeActionClearSpool}},
		{AttemptIdentity: identity, Decision: RuntimeResumeLeaseRevoked,
			AllowedActions: []RuntimeResumeAction{RuntimeActionStopExecution, RuntimeActionClearSpool}},
	}
	for _, decision := range valid {
		require.NoError(t, ValidateRuntimePayload(decision))
	}

	invalid := []RunResumeAcceptedPayload{
		{AttemptIdentity: identity, Decision: RuntimeResumeContinueExecution,
			AllowedActions: []RuntimeResumeAction{RuntimeActionContinueExecution}},
		{AttemptIdentity: identity, Decision: RuntimeResumeUploadSpoolOnly, LeaseExpiresAt: &expiresAt,
			AllowedActions: []RuntimeResumeAction{RuntimeActionUploadResult}},
		{AttemptIdentity: identity, Decision: RuntimeResumeResultAcked,
			AllowedActions: []RuntimeResumeAction{RuntimeActionUploadResult}},
		{AttemptIdentity: identity, Decision: RuntimeResumeLeaseRevoked,
			AllowedActions: []RuntimeResumeAction{RuntimeActionClearSpool}},
	}
	for _, decision := range invalid {
		requireRuntimeTransportCode(t, ValidateRuntimePayload(decision), RuntimeErrorValidationFailed)
	}
}

func TestRuntimeTransportErrorMappings(t *testing.T) {
	t.Parallel()

	ranges := []EventRange{{Start: 2, End: 3}}
	tests := []struct {
		name   string
		err    error
		code   RuntimeErrorCode
		ranges []EventRange
	}{
		{
			name:   "event store",
			err:    &RuntimeEventError{Code: RuntimeEventErrorEventsMissing, MissingRanges: ranges},
			code:   RuntimeErrorEventsMissing,
			ranges: ranges,
		},
		{
			name: "result finalizer",
			err:  &RuntimeResultError{Code: RuntimeResultErrorResultIDConflict},
			code: RuntimeErrorResultIDConflict,
		},
		{
			name: "invalid event",
			err:  ErrInvalidRuntimeEvent,
			code: RuntimeErrorValidationFailed,
		},
		{
			name: "private internal error",
			err:  errors.New("postgres password must not leak"),
			code: RuntimeErrorInternal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapped := MapRuntimeTransportError(test.err)
			require.Equal(t, test.code, mapped.Body.Code)
			require.Equal(t, test.ranges, mapped.Body.MissingEventRanges)
			require.NotContains(t, mapped.Body.Message, "password")
			require.Equal(t, mapped.Body, mapped.Envelope().Error)
		})
	}
}

func TestRuntimeHTTPAndWebSocketMappings(t *testing.T) {
	t.Parallel()

	require.Equal(t, http.StatusUpgradeRequired, RuntimeHTTPStatus(RuntimeErrorClientUpgradeRequired))
	require.Equal(t, http.StatusUnprocessableEntity, RuntimeHTTPStatus(RuntimeErrorValidationFailed))
	require.Equal(t, http.StatusConflict, RuntimeHTTPStatus(RuntimeErrorEventsMissing))
	require.Equal(t, http.StatusServiceUnavailable, RuntimeHTTPStatus(RuntimeErrorServiceUnavailable))

	tests := []struct {
		code      RuntimeErrorCode
		closeCode int
		fatal     bool
	}{
		{code: RuntimeErrorUnauthorized, closeCode: 4401, fatal: true},
		{code: RuntimeErrorClientUpgradeRequired, closeCode: 4406, fatal: true},
		{code: RuntimeErrorSessionConflict, closeCode: 4409, fatal: true},
		{code: RuntimeErrorRequiredFeatureMissing, closeCode: 4412, fatal: true},
		{code: RuntimeErrorValidationFailed, closeCode: 1002, fatal: true},
		{code: RuntimeErrorEventsMissing},
	}
	for _, test := range tests {
		closeCode, fatal := RuntimeWebSocketCloseCode(test.code)
		require.Equal(t, test.closeCode, closeCode)
		require.Equal(t, test.fatal, fatal)
	}
}

func validRuntimeHelloPayload() RuntimeHelloPayload {
	return RuntimeHelloPayload{
		NodeID:           uuid.New(),
		AgentID:          uuid.New(),
		WorkerID:         "worker-1",
		RuntimeSessionID: uuid.New(),
		SessionEpoch:     1,
		NodeVersion:      "2.0.0",
		Capacity:         1,
		Features:         RuntimeRequiredFeatures(),
		ContractDigest:   RuntimeContractDigest,
	}
}

func runtimeTestEnvelope(messageType RuntimeMessageType, replyTo *uuid.UUID) RuntimeEnvelope {
	return RuntimeEnvelope{
		RuntimeEnvelopeFields: RuntimeEnvelopeFields{
			ProtocolVersion:   RuntimeProtocolVersion,
			RuntimeContractID: RuntimeContractID,
			MessageID:         uuid.New(),
			ReplyToMessageID:  replyTo,
			Type:              messageType,
			SentAt:            time.Now().UTC(),
		},
		Payload: json.RawMessage(`{}`),
	}
}

func runtimeTestAttemptIdentity() AttemptIdentity {
	return AttemptIdentity{
		RunID:            uuid.New(),
		AttemptID:        uuid.New(),
		LeaseID:          uuid.New(),
		FencingToken:     3,
		NodeID:           uuid.New(),
		AgentID:          uuid.New(),
		WorkerID:         "worker-a",
		RuntimeSessionID: uuid.New(),
	}
}

func requireRuntimeTransportCode(t *testing.T, err error, code RuntimeErrorCode) {
	t.Helper()
	require.Error(t, err)
	var transportErr *RuntimeTransportError
	require.ErrorAs(t, err, &transportErr)
	require.Equal(t, code, transportErr.Body.Code)
}
