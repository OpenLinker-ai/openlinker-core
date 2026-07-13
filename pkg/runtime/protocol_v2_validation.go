package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

var (
	runtimeUUIDType       = reflect.TypeFor[uuid.UUID]()
	runtimeTimeType       = reflect.TypeFor[time.Time]()
	runtimeRawMessageType = reflect.TypeFor[json.RawMessage]()
)

// DecodeRuntimeEnvelope decodes one complete WebSocket message. It validates
// only the common envelope; callers then use DecodeRuntimeMessagePayload after
// dispatching on Type so unknown payload fields cannot slip through.
func DecodeRuntimeEnvelope(reader io.Reader) (RuntimeEnvelope, error) {
	raw, err := readRuntimeJSON(reader)
	if err != nil {
		return RuntimeEnvelope{}, err
	}
	return parseRuntimeEnvelope(raw)
}

// ParseRuntimeEnvelope is the byte-slice form used by WebSocket readers.
func ParseRuntimeEnvelope(frame []byte) (RuntimeEnvelope, error) {
	if int64(len(frame)) > MaxRuntimeMessageBytes {
		return RuntimeEnvelope{}, runtimeMessageTooLargeError()
	}
	return parseRuntimeEnvelope(frame)
}

func parseRuntimeEnvelope(raw []byte) (RuntimeEnvelope, error) {
	var envelope RuntimeEnvelope
	if err := decodeRuntimeJSON(raw, &envelope); err != nil {
		return RuntimeEnvelope{}, err
	}
	if err := ValidateRuntimeEnvelope(envelope); err != nil {
		return RuntimeEnvelope{}, err
	}
	return envelope, nil
}

// DecodeRuntimeMessagePayload strictly decodes the routed envelope payload.
func DecodeRuntimeMessagePayload[P any](envelope RuntimeEnvelope, expected RuntimeMessageType) (P, error) {
	var payload P
	if err := ValidateRuntimeEnvelope(envelope); err != nil {
		return payload, err
	}
	if envelope.Type != expected {
		return payload, runtimeValidationError("runtime message type does not match payload", nil)
	}
	if err := decodeRuntimeJSON(envelope.Payload, &payload); err != nil {
		return payload, err
	}
	if err := ValidateRuntimePayload(payload); err != nil {
		return payload, err
	}
	return payload, nil
}

// DecodeRuntimeTypedMessage performs strict one-pass decoding when the caller
// already knows the only permitted message type.
func DecodeRuntimeTypedMessage[P any](reader io.Reader, expected RuntimeMessageType) (RuntimeTypedEnvelope[P], error) {
	var message RuntimeTypedEnvelope[P]
	raw, err := readRuntimeJSON(reader)
	if err != nil {
		return message, err
	}
	if err := decodeRuntimeJSON(raw, &message); err != nil {
		return message, err
	}
	rawPayload, err := json.Marshal(message.Payload)
	if err != nil {
		return message, runtimeValidationError("runtime payload cannot be encoded", err)
	}
	envelope := RuntimeEnvelope{RuntimeEnvelopeFields: message.RuntimeEnvelopeFields, Payload: rawPayload}
	if err := ValidateRuntimeEnvelope(envelope); err != nil {
		return message, err
	}
	if message.Type != expected {
		return message, runtimeValidationError("runtime message type does not match endpoint", nil)
	}
	if err := ValidateRuntimePayload(message.Payload); err != nil {
		return message, err
	}
	return message, nil
}

// DecodeRuntimeBody decodes one strict HTTP Runtime request body.
func DecodeRuntimeBody[P any](reader io.Reader) (P, error) {
	var payload P
	raw, err := readRuntimeJSON(reader)
	if err != nil {
		return payload, err
	}
	if err := decodeRuntimeJSON(raw, &payload); err != nil {
		return payload, err
	}
	if err := ValidateRuntimePayload(payload); err != nil {
		return payload, err
	}
	return payload, nil
}

// NewRuntimeTypedMessage creates an internally valid outbound envelope.
func NewRuntimeTypedMessage[P any](messageType RuntimeMessageType, replyTo *uuid.UUID, payload P) (RuntimeTypedEnvelope[P], error) {
	message := RuntimeTypedEnvelope[P]{
		RuntimeEnvelopeFields: RuntimeEnvelopeFields{
			ProtocolVersion:   RuntimeProtocolVersion,
			RuntimeContractID: RuntimeContractID,
			MessageID:         uuid.New(),
			ReplyToMessageID:  replyTo,
			Type:              messageType,
			SentAt:            time.Now().UTC(),
		},
		Payload: payload,
	}
	if err := ValidateRuntimePayload(payload); err != nil {
		return RuntimeTypedEnvelope[P]{}, err
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return RuntimeTypedEnvelope[P]{}, runtimeValidationError("runtime payload cannot be encoded", err)
	}
	if err := ValidateRuntimeEnvelope(RuntimeEnvelope{
		RuntimeEnvelopeFields: message.RuntimeEnvelopeFields,
		Payload:               rawPayload,
	}); err != nil {
		return RuntimeTypedEnvelope[P]{}, err
	}
	return message, nil
}

// ValidateRuntimeEnvelope enforces the version, contract, identity, message
// set, payload-object, and reply-presence rules in core-runtime.json.
func ValidateRuntimeEnvelope(envelope RuntimeEnvelope) error {
	if envelope.ProtocolVersion != RuntimeProtocolVersion || envelope.RuntimeContractID != RuntimeContractID {
		return newRuntimeTransportError(
			RuntimeErrorClientUpgradeRequired,
			runtimeErrorDefaultMessage(RuntimeErrorClientUpgradeRequired),
			nil,
		)
	}
	if envelope.MessageID == uuid.Nil {
		return runtimeValidationError("runtime message_id must be a non-zero UUID", nil)
	}
	if envelope.ReplyToMessageID != nil {
		if *envelope.ReplyToMessageID == uuid.Nil {
			return runtimeValidationError("runtime reply_to_message_id must be a non-zero UUID", nil)
		}
		if *envelope.ReplyToMessageID == envelope.MessageID {
			return runtimeValidationError("runtime message cannot reply to itself", nil)
		}
	}
	if envelope.SentAt.IsZero() {
		return runtimeValidationError("runtime sent_at is required", nil)
	}
	if !knownRuntimeMessageType(envelope.Type) {
		return runtimeValidationError("unknown runtime message type", nil)
	}
	if runtimeMessageRequiresReplyTo(envelope.Type) && envelope.ReplyToMessageID == nil {
		return runtimeValidationError("runtime reply_to_message_id is required", nil)
	}
	var object map[string]json.RawMessage
	if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &object) != nil || object == nil {
		return runtimeValidationError("runtime payload must be a JSON object", nil)
	}
	return nil
}

// ValidateRuntimeReplyCorrelation proves that a business ACK/error belongs to
// a concrete request. Socket write order is not accepted as correlation.
func ValidateRuntimeReplyCorrelation(request, reply RuntimeEnvelope) error {
	if err := ValidateRuntimeEnvelope(request); err != nil {
		return err
	}
	if err := ValidateRuntimeEnvelope(reply); err != nil {
		return err
	}
	if reply.ReplyToMessageID == nil || *reply.ReplyToMessageID != request.MessageID {
		return runtimeValidationError("runtime reply correlation does not match request message_id", nil)
	}
	if reply.Type == RuntimeMessageError {
		if runtimeMessageExpectsReply(request.Type) {
			return nil
		}
		return runtimeValidationError("runtime.error cannot reply to this message type", nil)
	}
	for _, allowed := range runtimeReplyTypes[request.Type] {
		if reply.Type == allowed {
			return nil
		}
	}
	return runtimeValidationError("runtime reply type does not match request type", nil)
}

var runtimeReplyTypes = map[RuntimeMessageType][]RuntimeMessageType{
	RuntimeMessageHello:            {RuntimeMessageReady},
	RuntimeMessageRunAssigned:      {RuntimeMessageAssignmentAck, RuntimeMessageAssignmentReject},
	RuntimeMessageAssignmentAck:    {RuntimeMessageAssignmentConfirmed},
	RuntimeMessageAssignmentReject: {RuntimeMessageAssignmentRejected},
	RuntimeMessageLeaseRenew:       {RuntimeMessageLeaseRenewed},
	RuntimeMessageRunEvent:         {RuntimeMessageRunEventAck},
	RuntimeMessageRunResult:        {RuntimeMessageRunResultAck},
	RuntimeMessageRunCancel:        {RuntimeMessageRunCancelAck},
	RuntimeMessageResume:           {RuntimeMessageResumeAccepted},
}

func runtimeMessageExpectsReply(messageType RuntimeMessageType) bool {
	_, ok := runtimeReplyTypes[messageType]
	return ok
}

func runtimeMessageRequiresReplyTo(messageType RuntimeMessageType) bool {
	switch messageType {
	case RuntimeMessageReady,
		RuntimeMessageAssignmentAck,
		RuntimeMessageAssignmentConfirmed,
		RuntimeMessageAssignmentReject,
		RuntimeMessageAssignmentRejected,
		RuntimeMessageLeaseRenewed,
		RuntimeMessageRunEventAck,
		RuntimeMessageRunResultAck,
		RuntimeMessageRunCancelAck,
		RuntimeMessageResumeAccepted,
		RuntimeMessageError:
		return true
	default:
		return false
	}
}

func knownRuntimeMessageType(messageType RuntimeMessageType) bool {
	switch messageType {
	case RuntimeMessageHello,
		RuntimeMessageReady,
		RuntimeMessageRunAssigned,
		RuntimeMessageAssignmentAck,
		RuntimeMessageAssignmentConfirmed,
		RuntimeMessageAssignmentReject,
		RuntimeMessageAssignmentRejected,
		RuntimeMessageLeaseRenew,
		RuntimeMessageLeaseRenewed,
		RuntimeMessageRunEvent,
		RuntimeMessageRunEventAck,
		RuntimeMessageRunResult,
		RuntimeMessageRunResultAck,
		RuntimeMessageRunCancel,
		RuntimeMessageRunCancelAck,
		RuntimeMessageResume,
		RuntimeMessageResumeAccepted,
		RuntimeMessageLeaseRevoked,
		RuntimeMessageDrain,
		RuntimeMessageError:
		return true
	default:
		return false
	}
}

// ValidateRuntimePayload applies the semantic constraints that encoding/json
// cannot express. Required-field presence and unknown fields are handled by
// the strict decoders above.
func ValidateRuntimePayload(payload any) error {
	switch value := payload.(type) {
	case RuntimeHelloPayload:
		return validateRuntimeHello(value)
	case RuntimeReadyPayload:
		return validateRuntimeReady(value)
	case RunAssignedPayload:
		return validateRunAssigned(value)
	case RunAssignmentAckPayload:
		return validateAttemptIdentity(value.AttemptIdentity)
	case RunAssignmentConfirmedPayload:
		if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
			return err
		}
		if value.AttemptNo < 1 || value.LeaseExpiresAt.IsZero() {
			return runtimeValidationError("invalid assignment confirmation", nil)
		}
	case RunAssignmentRejectPayload:
		return validateAssignmentReject(value)
	case RunAssignmentRejectedPayload:
		if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
			return err
		}
		if !validAssignmentRejectOutcome(value.Outcome) || !validRuntimeDispatchState(value.DispatchState) {
			return runtimeValidationError("invalid assignment rejection", nil)
		}
	case RunLeaseRenewPayload:
		if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
			return err
		}
		if value.LastClientEventSeq < 0 || !validRuntimeCapacitySnapshot(value.Capacity, value.Inflight) {
			return runtimeValidationError("invalid lease renewal", nil)
		}
	case RunLeaseRenewedPayload:
		if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
			return err
		}
		if value.LeaseExpiresAt.IsZero() {
			return runtimeValidationError("lease_expires_at is required", nil)
		}
		if value.PendingCommand != nil {
			_, err := DecodePendingCommand(*value.PendingCommand)
			return err
		}
	case RunEventPayload:
		return validateRunEvent(value)
	case RunEventAckPayload:
		if value.ClientEventID == uuid.Nil || value.ClientEventSeq < 1 || value.Sequence < 1 {
			return runtimeValidationError("invalid runtime event acknowledgement", nil)
		}
	case RunErrorPayload:
		return validateRunError(value)
	case RunResultPayload:
		return validateRunResult(value)
	case RunResultAckPayload:
		return validateRunResultAck(value)
	case RunCancelPayload:
		return validateRunCancel(value)
	case RunCancelAckPayload:
		return validateRunCancelAck(value)
	case RunCancellationState:
		if value.CancellationID == uuid.Nil || !validRuntimeCancelState(value.CancelState) || value.UpdatedAt.IsZero() || !validOptionalString(value.ErrorCode, 120) {
			return runtimeValidationError("invalid cancellation state", nil)
		}
	case ResumeAttempt:
		return validateResumeAttempt(value)
	case RuntimeResumePayload:
		return validateRuntimeResume(value)
	case RunResumeAcceptedPayload:
		return validateResumeAccepted(value)
	case RuntimeResumeResponse:
		seen := make(map[AttemptIdentity]struct{}, len(value.Decisions))
		for _, decision := range value.Decisions {
			if err := validateResumeAccepted(decision); err != nil {
				return err
			}
			if _, duplicate := seen[decision.AttemptIdentity]; duplicate {
				return runtimeValidationError("runtime resume decisions must be unique per Attempt", nil)
			}
			seen[decision.AttemptIdentity] = struct{}{}
		}
	case RunLeaseRevokedPayload:
		if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
			return err
		}
		if !validRequiredString(value.ReasonCode, 120) || !validRuntimeDispatchState(value.DispatchState) || !validRuntimeRunStatus(value.RunStatus) {
			return runtimeValidationError("invalid lease revocation", nil)
		}
	case RuntimeDrainPayload:
		if value.DeadlineAt.IsZero() || !validRequiredString(value.ReasonCode, 120) || value.Capacity < 0 || value.Inflight < 0 {
			return runtimeValidationError("invalid runtime drain command", nil)
		}
	case PendingCommand:
		_, err := DecodePendingCommand(value)
		return err
	case RuntimeCommandsResponse:
		if value.DatabaseTime.IsZero() {
			return runtimeValidationError("database_time is required", nil)
		}
		for _, command := range value.Commands {
			if _, err := DecodePendingCommand(command); err != nil {
				return err
			}
		}
	case RuntimeClaimRequest:
		if value.RuntimeSessionID == uuid.Nil || value.Capacity < 0 || value.Inflight < 0 {
			return runtimeValidationError("invalid runtime claim request", nil)
		}
	case CallAgentRequest:
		if value.TargetAgentID == uuid.Nil || value.Input == nil || !validOptionalString(value.Reason, 500) {
			return runtimeValidationError("invalid delegated agent request", nil)
		}
	case RunSummary:
		if value.RunID == uuid.Nil || !validRuntimeRunStatus(value.Status) || !validRuntimeDispatchState(value.DispatchState) {
			return runtimeValidationError("invalid runtime run summary", nil)
		}
	case RuntimeErrorBody:
		return validateRuntimeErrorBody(value)
	case RuntimeError:
		return validateRuntimeErrorBody(value.Error)
	default:
		return runtimeValidationError("unsupported Runtime payload type", nil)
	}
	return nil
}

func validateRuntimeHello(value RuntimeHelloPayload) error {
	if value.NodeID == uuid.Nil || value.AgentID == uuid.Nil || value.RuntimeSessionID == uuid.Nil ||
		!validRequiredString(value.WorkerID, 200) || value.SessionEpoch < 1 ||
		!validRequiredString(value.NodeVersion, 100) || value.Capacity < 0 || value.Capacity > RuntimeMaximumNodeCapacity {
		return runtimeValidationError("invalid runtime hello identity or capacity", nil)
	}
	if value.ContractDigest != RuntimeContractDigest {
		return newRuntimeTransportError(RuntimeErrorClientUpgradeRequired, runtimeErrorDefaultMessage(RuntimeErrorClientUpgradeRequired), nil)
	}
	seen := make(map[string]struct{}, len(value.Features))
	for _, feature := range value.Features {
		if !validRequiredString(feature, 100) {
			return runtimeValidationError("invalid runtime feature", nil)
		}
		if _, duplicate := seen[feature]; duplicate {
			return runtimeValidationError("runtime features must be unique", nil)
		}
		seen[feature] = struct{}{}
	}
	for _, required := range RuntimeRequiredFeatures() {
		if _, ok := seen[required]; !ok {
			return newRuntimeTransportError(RuntimeErrorRequiredFeatureMissing, runtimeErrorDefaultMessage(RuntimeErrorRequiredFeatureMissing), nil)
		}
	}
	return nil
}

func validateRuntimeReady(value RuntimeReadyPayload) error {
	if !validRequiredString(value.CoreInstanceID, 200) || value.AttachmentID == uuid.Nil ||
		value.OfferTTLSeconds < 1 || value.LeaseTTLSeconds < 1 || value.DatabaseTime.IsZero() {
		return runtimeValidationError("invalid runtime ready payload", nil)
	}
	seen := make(map[string]struct{}, len(value.Features))
	for _, feature := range value.Features {
		if feature == "" {
			return runtimeValidationError("invalid runtime ready feature", nil)
		}
		if _, duplicate := seen[feature]; duplicate {
			return runtimeValidationError("runtime ready features must be unique", nil)
		}
		seen[feature] = struct{}{}
	}
	return nil
}

func validateRunAssigned(value RunAssignedPayload) error {
	if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if value.OfferNo < 1 || value.OfferExpiresAt.IsZero() || value.AttemptDeadlineAt.IsZero() || value.RunDeadlineAt.IsZero() ||
		value.Input == nil || value.NodeEnvelope == "" || value.AgentInvocationToken == "" {
		return runtimeValidationError("invalid run assignment", nil)
	}
	return nil
}

func validateAssignmentReject(value RunAssignmentRejectPayload) error {
	if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if !validRuntimeCapacitySnapshot(value.Capacity, value.Inflight) {
		return runtimeValidationError("invalid assignment rejection capacity", nil)
	}
	switch value.ReasonCode {
	case RuntimeRejectNodeAtCapacity, RuntimeRejectNodeDraining, RuntimeRejectClientUpgradeRequired, RuntimeRejectRequiredFeatureMissing:
		return nil
	default:
		return runtimeValidationError("invalid assignment rejection reason", nil)
	}
}

func validRuntimeCapacitySnapshot(capacity, inflight int64) bool {
	return capacity >= 0 && capacity <= RuntimeMaximumNodeCapacity &&
		inflight >= 0 && inflight <= RuntimeMaximumNodeCapacity
}

func validateRunEvent(value RunEventPayload) error {
	if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if value.ClientEventID == uuid.Nil || value.ClientEventSeq < 1 || !runtimeEventTypePattern.MatchString(value.EventType) || value.Payload == nil {
		return runtimeValidationError("invalid runtime event", nil)
	}
	if _, reserved := coreOwnedRuntimeEventTypes[value.EventType]; reserved {
		return runtimeValidationError("runtime event type is reserved by Core", nil)
	}
	return nil
}

func validateRunError(value RunErrorPayload) error {
	if !validRequiredString(value.ErrorCode, 120) || !validRequiredString(value.Message, 500) {
		return runtimeValidationError("invalid runtime result error", nil)
	}
	return nil
}

func validateRunResult(value RunResultPayload) error {
	if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if value.ResultID == uuid.Nil || value.DurationMS < 0 || value.DurationMS > math.MaxInt32 || value.FinalClientEventSeq < 0 {
		return runtimeValidationError("invalid runtime result identity or counters", nil)
	}
	switch value.Status {
	case "success":
		if value.Output == nil || value.Error != nil {
			return runtimeValidationError("successful runtime result requires output only", nil)
		}
	case "failed":
		if value.Output != nil || value.Error == nil {
			return runtimeValidationError("failed runtime result requires error only", nil)
		}
		if err := validateRunError(*value.Error); err != nil {
			return err
		}
	default:
		return runtimeValidationError("invalid runtime result status", nil)
	}
	return nil
}

// FinalizerRequest converts a validated wire Result into the transport-neutral
// Finalizer request without trusting a lossy integer conversion.
func (p RunResultPayload) FinalizerRequest() (RuntimeResultRequest, error) {
	if err := validateRunResult(p); err != nil {
		return RuntimeResultRequest{}, err
	}
	request := RuntimeResultRequest{
		AttemptIdentity:     p.AttemptIdentity.RuntimeIdentity(),
		ResultID:            p.ResultID,
		Status:              p.Status,
		Output:              p.Output,
		DurationMS:          int32(p.DurationMS),
		FinalClientEventSeq: p.FinalClientEventSeq,
	}
	if p.Error != nil {
		request.Error = &RuntimeResultFailure{
			ErrorCode:     p.Error.ErrorCode,
			Message:       p.Error.Message,
			RetryableHint: p.Error.RetryableHint,
		}
	}
	return request, nil
}

func validateRunResultAck(value RunResultAckPayload) error {
	if value.ResultID == uuid.Nil || !validRuntimeResultClassification(value.Classification) ||
		!validRuntimeRunStatus(value.RunStatus) || !validRuntimeDispatchState(value.DispatchState) {
		return runtimeValidationError("invalid runtime result acknowledgement", nil)
	}
	if value.NextAttemptAt != nil && value.NextAttemptAt.IsZero() {
		return runtimeValidationError("invalid next_attempt_at", nil)
	}
	return nil
}

func validateRunCancel(value RunCancelPayload) error {
	if value.CancellationID == uuid.Nil || !validRequiredString(value.ReasonCode, 120) || value.DeadlineAt.IsZero() {
		return runtimeValidationError("invalid runtime cancellation", nil)
	}
	return validateAttemptIdentity(value.AttemptIdentity)
}

func validateRunCancelAck(value RunCancelAckPayload) error {
	if value.CancellationID == uuid.Nil || !validRuntimeCancelState(value.CancelState) || !validOptionalString(value.ErrorCode, 120) {
		return runtimeValidationError("invalid runtime cancellation acknowledgement", nil)
	}
	return validateAttemptIdentity(value.AttemptIdentity)
}

func validateResumeAttempt(value ResumeAttempt) error {
	if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if value.LastAckedClientEventSeq < 0 {
		return runtimeValidationError("invalid resume event sequence", nil)
	}
	previousEnd := value.LastAckedClientEventSeq
	for _, eventRange := range value.PendingClientEventRanges {
		if eventRange.Start < 1 || eventRange.End < eventRange.Start {
			return runtimeValidationError("invalid pending event range", nil)
		}
		if eventRange.Start <= previousEnd {
			return runtimeValidationError("pending event ranges must be ordered, non-overlapping, and newer than the acknowledged sequence", nil)
		}
		previousEnd = eventRange.End
	}
	if value.PendingResultID != nil && *value.PendingResultID == uuid.Nil {
		return runtimeValidationError("invalid pending result ID", nil)
	}
	if value.FinalClientEventSeq != nil && *value.FinalClientEventSeq < 0 {
		return runtimeValidationError("invalid final event sequence", nil)
	}
	if (value.PendingResultID == nil) != (value.FinalClientEventSeq == nil) {
		return runtimeValidationError("pending result ID and final event sequence must be provided together", nil)
	}
	if value.FinalClientEventSeq != nil && *value.FinalClientEventSeq < previousEnd {
		return runtimeValidationError("final event sequence precedes pending events", nil)
	}
	return nil
}

func validateRuntimeResume(value RuntimeResumePayload) error {
	if value.NodeID == uuid.Nil || value.AgentID == uuid.Nil || value.RuntimeSessionID == uuid.Nil || !validRequiredString(value.WorkerID, 200) || len(value.Attempts) > RuntimeMaximumResumeAttempts {
		return runtimeValidationError("invalid runtime resume identity", nil)
	}
	seen := make(map[AttemptIdentity]struct{}, len(value.Attempts))
	for _, attempt := range value.Attempts {
		if err := validateResumeAttempt(attempt); err != nil {
			return err
		}
		identity := attempt.AttemptIdentity
		// AttemptIdentity remains immutable and therefore names the source
		// Session. RuntimeResumePayload names the currently authenticated target
		// Session, which may differ during an authorized process takeover.
		if identity.NodeID != value.NodeID || identity.AgentID != value.AgentID || identity.WorkerID != value.WorkerID {
			return runtimeValidationError("resume attempt identity does not match session", nil)
		}
		if _, duplicate := seen[identity]; duplicate {
			return runtimeValidationError("runtime resume attempts must be unique", nil)
		}
		seen[identity] = struct{}{}
	}
	return nil
}

func validateResumeAccepted(value RunResumeAcceptedPayload) error {
	if err := validateAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	switch value.Decision {
	case RuntimeResumeContinueExecution, RuntimeResumeUploadSpoolOnly, RuntimeResumeResultAcked, RuntimeResumeLeaseRevoked:
	default:
		return runtimeValidationError("invalid runtime resume decision", nil)
	}
	if value.LeaseExpiresAt != nil && value.LeaseExpiresAt.IsZero() {
		return runtimeValidationError("invalid resumed lease expiry", nil)
	}
	seen := make(map[RuntimeResumeAction]struct{}, len(value.AllowedActions))
	for _, action := range value.AllowedActions {
		switch action {
		case RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult, RuntimeActionStopExecution, RuntimeActionClearSpool:
		default:
			return runtimeValidationError("invalid runtime resume action", nil)
		}
		if _, duplicate := seen[action]; duplicate {
			return runtimeValidationError("runtime resume actions must be unique", nil)
		}
		seen[action] = struct{}{}
	}

	has := func(action RuntimeResumeAction) bool {
		_, ok := seen[action]
		return ok
	}
	switch value.Decision {
	case RuntimeResumeContinueExecution:
		if value.LeaseExpiresAt == nil || len(seen) != 3 ||
			!has(RuntimeActionContinueExecution) || !has(RuntimeActionUploadEvents) || !has(RuntimeActionUploadResult) {
			return runtimeValidationError("continue_execution requires a lease expiry and execution/event/result actions", nil)
		}
	case RuntimeResumeUploadSpoolOnly:
		if value.LeaseExpiresAt != nil || len(seen) == 0 || len(seen) > 2 ||
			(has(RuntimeActionContinueExecution) || has(RuntimeActionStopExecution) || has(RuntimeActionClearSpool)) {
			return runtimeValidationError("upload_spool_only permits only event and result upload actions", nil)
		}
	case RuntimeResumeResultAcked:
		if value.LeaseExpiresAt != nil || len(seen) != 1 || !has(RuntimeActionClearSpool) {
			return runtimeValidationError("result_already_acked permits only clearing the spool", nil)
		}
	case RuntimeResumeLeaseRevoked:
		if value.LeaseExpiresAt != nil || len(seen) != 2 || !has(RuntimeActionStopExecution) || !has(RuntimeActionClearSpool) {
			return runtimeValidationError("lease_revoked requires stop_execution and clear_spool actions", nil)
		}
	}
	return nil
}

type DecodedPendingCommand struct {
	Type   RuntimeMessageType
	Cancel *RunCancelPayload
	Drain  *RuntimeDrainPayload
	Revoke *RunLeaseRevokedPayload
}

// DecodePendingCommand strictly decodes the command union and rejects unknown
// fields inside its raw payload.
func DecodePendingCommand(command PendingCommand) (DecodedPendingCommand, error) {
	decoded := DecodedPendingCommand{Type: command.Type}
	switch command.Type {
	case RuntimeMessageRunCancel:
		var payload RunCancelPayload
		if err := decodeRuntimeJSON(command.Payload, &payload); err != nil {
			return DecodedPendingCommand{}, err
		}
		if err := validateRunCancel(payload); err != nil {
			return DecodedPendingCommand{}, err
		}
		decoded.Cancel = &payload
	case RuntimeMessageDrain:
		var payload RuntimeDrainPayload
		if err := decodeRuntimeJSON(command.Payload, &payload); err != nil {
			return DecodedPendingCommand{}, err
		}
		if err := ValidateRuntimePayload(payload); err != nil {
			return DecodedPendingCommand{}, err
		}
		decoded.Drain = &payload
	case RuntimeMessageLeaseRevoked:
		var payload RunLeaseRevokedPayload
		if err := decodeRuntimeJSON(command.Payload, &payload); err != nil {
			return DecodedPendingCommand{}, err
		}
		if err := ValidateRuntimePayload(payload); err != nil {
			return DecodedPendingCommand{}, err
		}
		decoded.Revoke = &payload
	default:
		return DecodedPendingCommand{}, runtimeValidationError("unknown runtime command type", nil)
	}
	return decoded, nil
}

func validateAttemptIdentity(value AttemptIdentity) error {
	if value.RunID == uuid.Nil || value.AttemptID == uuid.Nil || value.LeaseID == uuid.Nil || value.FencingToken < 1 ||
		value.NodeID == uuid.Nil || value.AgentID == uuid.Nil || !validRequiredString(value.WorkerID, 200) || value.RuntimeSessionID == uuid.Nil {
		return runtimeValidationError("invalid runtime attempt identity", nil)
	}
	return nil
}

func validateRuntimeErrorBody(value RuntimeErrorBody) error {
	if !validRuntimeErrorCode(value.Code) || !validRequiredString(value.Message, 500) ||
		(value.CurrentRunStatus != "" && !validRuntimeRunStatus(value.CurrentRunStatus)) ||
		(value.CurrentDispatchState != "" && !validRuntimeDispatchState(value.CurrentDispatchState)) {
		return runtimeValidationError("invalid runtime error body", nil)
	}
	for _, eventRange := range value.MissingEventRanges {
		if eventRange.Start < 1 || eventRange.End < eventRange.Start {
			return runtimeValidationError("invalid missing event range", nil)
		}
	}
	return nil
}

func validRuntimeResultClassification(value RuntimeResultClassification) bool {
	switch value {
	case RuntimeResultClassificationSuccess,
		RuntimeResultClassificationRetryable,
		RuntimeResultClassificationNonRetryable,
		RuntimeResultClassificationTimeout,
		RuntimeResultClassificationCanceled,
		RuntimeResultClassificationDeadLetter:
		return true
	default:
		return false
	}
}

func validRuntimeRunStatus(value RuntimeRunStatus) bool {
	switch value {
	case RuntimeRunRunning, RuntimeRunSuccess, RuntimeRunFailed, RuntimeRunTimeout, RuntimeRunCanceled:
		return true
	default:
		return false
	}
}

func validRuntimeDispatchState(value RuntimeDispatchState) bool {
	switch value {
	case RuntimeDispatchPending, RuntimeDispatchOffered, RuntimeDispatchExecuting, RuntimeDispatchRetryWait, RuntimeDispatchTerminal, RuntimeDispatchDeadLetter:
		return true
	default:
		return false
	}
}

func validRuntimeCancelState(value RuntimeCancelState) bool {
	switch value {
	case RuntimeCancelRequested, RuntimeCancelDelivered, RuntimeCancelStopping, RuntimeCancelStopped, RuntimeCancelUnsupported, RuntimeCancelFailed, RuntimeCancelUnconfirmed:
		return true
	default:
		return false
	}
}

func validAssignmentRejectOutcome(value RuntimeAssignmentRejectOutcome) bool {
	return value == RuntimeOfferRejected || value == RuntimeLeaseRevoked
}

func validRequiredString(value string, maximum int) bool {
	return utf8.ValidString(value) && utf8.RuneCountInString(value) >= 1 && utf8.RuneCountInString(value) <= maximum
}

func validOptionalString(value string, maximum int) bool {
	return value == "" || validRequiredString(value, maximum)
}

func readRuntimeJSON(reader io.Reader) ([]byte, error) {
	if reader == nil {
		return nil, runtimeValidationError("runtime request body is required", nil)
	}
	raw, err := io.ReadAll(io.LimitReader(reader, MaxRuntimeMessageBytes+1))
	if err != nil {
		return nil, newRuntimeTransportError(RuntimeErrorBadRequest, runtimeErrorDefaultMessage(RuntimeErrorBadRequest), err)
	}
	if int64(len(raw)) > MaxRuntimeMessageBytes {
		return nil, runtimeMessageTooLargeError()
	}
	return raw, nil
}

func runtimeMessageTooLargeError() error {
	return newRuntimeTransportError(RuntimeErrorBadRequest, "Runtime message exceeds 4194304 bytes", nil)
}

func decodeRuntimeJSON(raw []byte, target any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return runtimeValidationError("runtime JSON body is required", nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return runtimeValidationError("invalid runtime JSON body", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return runtimeValidationError("runtime body must contain exactly one JSON value", err)
	}
	if err := validateRuntimeRequiredFields(raw, reflect.TypeOf(target)); err != nil {
		return runtimeValidationError(err.Error(), err)
	}
	return nil
}

func runtimeValidationError(message string, cause error) error {
	return newRuntimeTransportError(RuntimeErrorValidationFailed, message, cause)
}

// validateRuntimeRequiredFields uses the runtime:"required" marker to retain
// JSON Schema presence semantics even when a required zero/false value is
// valid and therefore indistinguishable after ordinary unmarshalling.
func validateRuntimeRequiredFields(raw json.RawMessage, valueType reflect.Type) error {
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	if valueType == runtimeUUIDType || valueType == runtimeTimeType || valueType == runtimeRawMessageType {
		return nil
	}
	switch valueType.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil || object == nil {
			return errors.New("runtime value must be a JSON object")
		}
		for index := 0; index < valueType.NumField(); index++ {
			field := valueType.Field(index)
			if field.PkgPath != "" {
				continue
			}
			jsonName, skip, embedded := runtimeJSONFieldName(field)
			if skip {
				continue
			}
			if embedded {
				if err := validateRuntimeRequiredFields(raw, field.Type); err != nil {
					return err
				}
				continue
			}
			fieldRaw, present := object[jsonName]
			runtimeTag := field.Tag.Get("runtime")
			isNull := present && bytes.Equal(bytes.TrimSpace(fieldRaw), []byte("null"))
			if runtimeTag == "required" && (!present || isNull) {
				return fmt.Errorf("runtime field %q is required", jsonName)
			}
			if isNull && runtimeTag != "nullable" {
				return fmt.Errorf("runtime field %q cannot be null", jsonName)
			}
			if present && !isNull {
				if err := validateRuntimeRequiredFields(fieldRaw, field.Type); err != nil {
					return fmt.Errorf("runtime field %q: %w", jsonName, err)
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if valueType == runtimeRawMessageType || valueType == runtimeUUIDType {
			return nil
		}
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return errors.New("runtime value must be a JSON array")
		}
		for index, item := range items {
			if err := validateRuntimeRequiredFields(item, valueType.Elem()); err != nil {
				return fmt.Errorf("runtime array item %d: %w", index, err)
			}
		}
	}
	return nil
}

func runtimeJSONFieldName(field reflect.StructField) (name string, skip bool, embedded bool) {
	tag := field.Tag.Get("json")
	parts := strings.Split(tag, ",")
	if len(parts) > 0 && parts[0] == "-" {
		return "", true, false
	}
	if field.Anonymous && (tag == "" || parts[0] == "") {
		return "", false, true
	}
	if len(parts) > 0 && parts[0] != "" {
		return parts[0], false, false
	}
	return field.Name, false, false
}
