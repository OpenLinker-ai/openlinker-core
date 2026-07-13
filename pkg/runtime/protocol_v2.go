package runtime

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	// MaxRuntimeMessageBytes is the contract's limit for every non-artifact
	// HTTP body and WebSocket message. The limit applies to the complete wire
	// message, not only to its business payload.
	MaxRuntimeMessageBytes int64 = 4 * 1024 * 1024

	RuntimeOfferTTLSeconds       = 30
	RuntimeLeaseTTLSeconds       = 60
	RuntimeHelloTimeoutSeconds   = 5
	RuntimeMaxPullWaitSeconds    = 30
	RuntimeMaximumNodeCapacity   = 1024
	RuntimeMaximumResumeAttempts = 1024
)

// RuntimeMessageType is the closed set of Runtime WebSocket message types.
type RuntimeMessageType string

const (
	RuntimeMessageHello               RuntimeMessageType = "runtime.hello"
	RuntimeMessageReady               RuntimeMessageType = "runtime.ready"
	RuntimeMessageRunAssigned         RuntimeMessageType = "run.assigned"
	RuntimeMessageAssignmentAck       RuntimeMessageType = "run.assignment.ack"
	RuntimeMessageAssignmentConfirmed RuntimeMessageType = "run.assignment.confirmed"
	RuntimeMessageAssignmentReject    RuntimeMessageType = "run.assignment.reject"
	RuntimeMessageAssignmentRejected  RuntimeMessageType = "run.assignment.rejected"
	RuntimeMessageLeaseRenew          RuntimeMessageType = "run.lease.renew"
	RuntimeMessageLeaseRenewed        RuntimeMessageType = "run.lease.renewed"
	RuntimeMessageRunEvent            RuntimeMessageType = "run.event"
	RuntimeMessageRunEventAck         RuntimeMessageType = "run.event.ack"
	RuntimeMessageRunResult           RuntimeMessageType = "run.result"
	RuntimeMessageRunResultAck        RuntimeMessageType = "run.result.ack"
	RuntimeMessageRunCancel           RuntimeMessageType = "run.cancel"
	RuntimeMessageRunCancelAck        RuntimeMessageType = "run.cancel.ack"
	RuntimeMessageResume              RuntimeMessageType = "runtime.resume"
	RuntimeMessageResumeAccepted      RuntimeMessageType = "run.resume.accepted"
	RuntimeMessageLeaseRevoked        RuntimeMessageType = "run.lease.revoked"
	RuntimeMessageDrain               RuntimeMessageType = "runtime.drain"
	RuntimeMessageError               RuntimeMessageType = "runtime.error"
)

// RuntimeEnvelopeFields are common to every Runtime WebSocket message.
// Payload is deliberately not part of this struct so concrete messages keep a
// strongly typed payload while the router can use RuntimeEnvelope below.
type RuntimeEnvelopeFields struct {
	ProtocolVersion   int                `json:"protocol_version" runtime:"required"`
	RuntimeContractID string             `json:"runtime_contract_id" runtime:"required"`
	MessageID         uuid.UUID          `json:"message_id" runtime:"required"`
	ReplyToMessageID  *uuid.UUID         `json:"reply_to_message_id,omitempty"`
	Type              RuntimeMessageType `json:"type" runtime:"required"`
	SentAt            time.Time          `json:"sent_at" runtime:"required"`
}

// RuntimeEnvelope is the routing form of a WebSocket message. Payload remains
// raw until Type has selected the one permitted concrete payload schema.
type RuntimeEnvelope struct {
	RuntimeEnvelopeFields
	Payload json.RawMessage `json:"payload" runtime:"required"`
}

// RuntimeTypedEnvelope is used by concrete messages after routing.
type RuntimeTypedEnvelope[P any] struct {
	RuntimeEnvelopeFields
	Payload P `json:"payload" runtime:"required"`
}

type RuntimeHelloMessage = RuntimeTypedEnvelope[RuntimeHelloPayload]
type RuntimeReadyMessage = RuntimeTypedEnvelope[RuntimeReadyPayload]
type RunAssignedMessage = RuntimeTypedEnvelope[RunAssignedPayload]
type RunAssignmentAckMessage = RuntimeTypedEnvelope[RunAssignmentAckPayload]
type RunAssignmentConfirmedMessage = RuntimeTypedEnvelope[RunAssignmentConfirmedPayload]
type RunAssignmentRejectMessage = RuntimeTypedEnvelope[RunAssignmentRejectPayload]
type RunAssignmentRejectedMessage = RuntimeTypedEnvelope[RunAssignmentRejectedPayload]
type RunLeaseRenewMessage = RuntimeTypedEnvelope[RunLeaseRenewPayload]
type RunLeaseRenewedMessage = RuntimeTypedEnvelope[RunLeaseRenewedPayload]
type RunEventMessage = RuntimeTypedEnvelope[RunEventPayload]
type RunEventAckMessage = RuntimeTypedEnvelope[RunEventAckPayload]
type RunResultMessage = RuntimeTypedEnvelope[RunResultPayload]
type RunResultAckMessage = RuntimeTypedEnvelope[RunResultAckPayload]
type RunCancelMessage = RuntimeTypedEnvelope[RunCancelPayload]
type RunCancelAckMessage = RuntimeTypedEnvelope[RunCancelAckPayload]
type RuntimeResumeMessage = RuntimeTypedEnvelope[RuntimeResumePayload]
type RunResumeAcceptedMessage = RuntimeTypedEnvelope[RunResumeAcceptedPayload]
type RunLeaseRevokedMessage = RuntimeTypedEnvelope[RunLeaseRevokedPayload]
type RuntimeDrainMessage = RuntimeTypedEnvelope[RuntimeDrainPayload]
type RuntimeErrorMessage = RuntimeTypedEnvelope[RuntimeErrorBody]

// AttemptIdentity is the exact runtime wire identity. Unlike the
// transport-neutral RuntimeAttemptIdentity, every Node/session field is
// mandatory here.
type AttemptIdentity struct {
	RunID            uuid.UUID `json:"run_id" runtime:"required"`
	AttemptID        uuid.UUID `json:"attempt_id" runtime:"required"`
	LeaseID          uuid.UUID `json:"lease_id" runtime:"required"`
	FencingToken     int64     `json:"fencing_token" runtime:"required"`
	NodeID           uuid.UUID `json:"node_id" runtime:"required"`
	AgentID          uuid.UUID `json:"agent_id" runtime:"required"`
	WorkerID         string    `json:"worker_id" runtime:"required"`
	RuntimeSessionID uuid.UUID `json:"runtime_session_id" runtime:"required"`
}

func (i AttemptIdentity) RuntimeIdentity() RuntimeAttemptIdentity {
	workerID := i.WorkerID
	nodeID := i.NodeID
	sessionID := i.RuntimeSessionID
	return RuntimeAttemptIdentity{
		RunID:            i.RunID,
		AttemptID:        i.AttemptID,
		LeaseID:          i.LeaseID,
		FencingToken:     i.FencingToken,
		NodeID:           &nodeID,
		AgentID:          i.AgentID,
		WorkerID:         &workerID,
		RuntimeSessionID: &sessionID,
	}
}

type RuntimeHelloPayload struct {
	NodeID           uuid.UUID `json:"node_id" runtime:"required"`
	AgentID          uuid.UUID `json:"agent_id" runtime:"required"`
	WorkerID         string    `json:"worker_id" runtime:"required"`
	RuntimeSessionID uuid.UUID `json:"runtime_session_id" runtime:"required"`
	SessionEpoch     int64     `json:"session_epoch" runtime:"required"`
	NodeVersion      string    `json:"node_version" runtime:"required"`
	Capacity         int64     `json:"capacity" runtime:"required"`
	Features         []string  `json:"features" runtime:"required"`
	ContractDigest   string    `json:"contract_digest" runtime:"required"`
}

type RuntimeReadyPayload struct {
	CoreInstanceID  string    `json:"core_instance_id" runtime:"required"`
	AttachmentID    uuid.UUID `json:"attachment_id" runtime:"required"`
	Features        []string  `json:"features" runtime:"required"`
	OfferTTLSeconds int64     `json:"offer_ttl_seconds" runtime:"required"`
	LeaseTTLSeconds int64     `json:"lease_ttl_seconds" runtime:"required"`
	DatabaseTime    time.Time `json:"database_time" runtime:"required"`
}

type RunAssignedPayload struct {
	AttemptIdentity      AttemptIdentity `json:"attempt_identity" runtime:"required"`
	OfferNo              int64           `json:"offer_no" runtime:"required"`
	OfferExpiresAt       time.Time       `json:"offer_expires_at" runtime:"required"`
	AttemptDeadlineAt    time.Time       `json:"attempt_deadline_at" runtime:"required"`
	RunDeadlineAt        time.Time       `json:"run_deadline_at" runtime:"required"`
	Input                map[string]any  `json:"input" runtime:"required"`
	Metadata             map[string]any  `json:"metadata,omitempty"`
	NodeEnvelope         string          `json:"node_envelope" runtime:"required"`
	AgentInvocationToken string          `json:"agent_invocation_token" runtime:"required"`
}

type RunAssignmentAckPayload struct {
	AttemptIdentity AttemptIdentity `json:"attempt_identity" runtime:"required"`
}

type RunAssignmentConfirmedPayload struct {
	AttemptIdentity AttemptIdentity `json:"attempt_identity" runtime:"required"`
	AttemptNo       int64           `json:"attempt_no" runtime:"required"`
	LeaseExpiresAt  time.Time       `json:"lease_expires_at" runtime:"required"`
}

type RuntimeAssignmentRejectReason string

const (
	RuntimeRejectNodeAtCapacity         RuntimeAssignmentRejectReason = "NODE_AT_CAPACITY"
	RuntimeRejectNodeDraining           RuntimeAssignmentRejectReason = "NODE_DRAINING"
	RuntimeRejectClientUpgradeRequired  RuntimeAssignmentRejectReason = "RUNTIME_CLIENT_UPGRADE_REQUIRED"
	RuntimeRejectRequiredFeatureMissing RuntimeAssignmentRejectReason = "RUNTIME_REQUIRED_FEATURE_MISSING"
)

type RunAssignmentRejectPayload struct {
	AttemptIdentity AttemptIdentity               `json:"attempt_identity" runtime:"required"`
	ReasonCode      RuntimeAssignmentRejectReason `json:"reason_code" runtime:"required"`
	Capacity        int64                         `json:"capacity" runtime:"required"`
	Inflight        int64                         `json:"inflight" runtime:"required"`
}

type RuntimeAssignmentRejectOutcome string

const (
	RuntimeOfferRejected RuntimeAssignmentRejectOutcome = "offer_rejected"
	RuntimeLeaseRevoked  RuntimeAssignmentRejectOutcome = "lease_revoked"
)

type RunAssignmentRejectedPayload struct {
	AttemptIdentity AttemptIdentity                `json:"attempt_identity" runtime:"required"`
	Outcome         RuntimeAssignmentRejectOutcome `json:"outcome" runtime:"required"`
	DispatchState   RuntimeDispatchState           `json:"dispatch_state" runtime:"required"`
}

type RunLeaseRenewPayload struct {
	AttemptIdentity    AttemptIdentity `json:"attempt_identity" runtime:"required"`
	LastClientEventSeq int64           `json:"last_client_event_seq" runtime:"required"`
	Capacity           int64           `json:"capacity" runtime:"required"`
	Inflight           int64           `json:"inflight" runtime:"required"`
}

type RunLeaseRenewedPayload struct {
	AttemptIdentity AttemptIdentity `json:"attempt_identity" runtime:"required"`
	LeaseExpiresAt  time.Time       `json:"lease_expires_at" runtime:"required"`
	PendingCommand  *PendingCommand `json:"pending_command,omitempty" runtime:"nullable"`
}

type RunEventPayload struct {
	AttemptIdentity AttemptIdentity `json:"attempt_identity" runtime:"required"`
	ClientEventID   uuid.UUID       `json:"client_event_id" runtime:"required"`
	ClientEventSeq  int64           `json:"client_event_seq" runtime:"required"`
	EventType       string          `json:"event_type" runtime:"required"`
	Payload         map[string]any  `json:"payload" runtime:"required"`
}

func (p RunEventPayload) StoreRequest() RuntimeEventRequest {
	return RuntimeEventRequest{
		ClientEventID:  p.ClientEventID,
		ClientEventSeq: p.ClientEventSeq,
		EventType:      p.EventType,
		Payload:        p.Payload,
	}
}

type RunEventAckPayload struct {
	ClientEventID  uuid.UUID `json:"client_event_id" runtime:"required"`
	ClientEventSeq int64     `json:"client_event_seq" runtime:"required"`
	Sequence       int64     `json:"sequence" runtime:"required"`
	Replayed       bool      `json:"replayed" runtime:"required"`
}

type RunErrorPayload struct {
	ErrorCode     string `json:"error_code" runtime:"required"`
	Message       string `json:"message" runtime:"required"`
	RetryableHint bool   `json:"retryable_hint,omitempty"`
}

type RunResultPayload struct {
	AttemptIdentity     AttemptIdentity  `json:"attempt_identity" runtime:"required"`
	ResultID            uuid.UUID        `json:"result_id" runtime:"required"`
	Status              string           `json:"status" runtime:"required"`
	Output              map[string]any   `json:"output,omitempty"`
	Error               *RunErrorPayload `json:"error,omitempty"`
	DurationMS          int64            `json:"duration_ms" runtime:"required"`
	FinalClientEventSeq int64            `json:"final_client_event_seq" runtime:"required"`
}

type RunResultAckPayload struct {
	ResultID       uuid.UUID                   `json:"result_id" runtime:"required"`
	Classification RuntimeResultClassification `json:"classification" runtime:"required"`
	RunStatus      RuntimeRunStatus            `json:"run_status" runtime:"required"`
	DispatchState  RuntimeDispatchState        `json:"dispatch_state" runtime:"required"`
	Replayed       bool                        `json:"replayed" runtime:"required"`
	NextAttemptAt  *time.Time                  `json:"next_attempt_at,omitempty"`
}

type RunCancelPayload struct {
	CancellationID  uuid.UUID       `json:"cancellation_id" runtime:"required"`
	AttemptIdentity AttemptIdentity `json:"attempt_identity" runtime:"required"`
	ReasonCode      string          `json:"reason_code" runtime:"required"`
	DeadlineAt      time.Time       `json:"deadline_at" runtime:"required"`
}

type RuntimeCancelState string

const (
	RuntimeCancelRequested   RuntimeCancelState = "requested"
	RuntimeCancelDelivered   RuntimeCancelState = "delivered"
	RuntimeCancelStopping    RuntimeCancelState = "stopping"
	RuntimeCancelStopped     RuntimeCancelState = "stopped"
	RuntimeCancelUnsupported RuntimeCancelState = "unsupported"
	RuntimeCancelFailed      RuntimeCancelState = "failed"
	RuntimeCancelUnconfirmed RuntimeCancelState = "unconfirmed"
)

type RunCancelAckPayload struct {
	CancellationID  uuid.UUID          `json:"cancellation_id" runtime:"required"`
	AttemptIdentity AttemptIdentity    `json:"attempt_identity" runtime:"required"`
	CancelState     RuntimeCancelState `json:"cancel_state" runtime:"required"`
	ErrorCode       string             `json:"error_code,omitempty"`
}

type RunCancellationState struct {
	CancellationID uuid.UUID          `json:"cancellation_id" runtime:"required"`
	CancelState    RuntimeCancelState `json:"cancel_state" runtime:"required"`
	UpdatedAt      time.Time          `json:"updated_at" runtime:"required"`
	ErrorCode      string             `json:"error_code,omitempty"`
}

type ResumeAttempt struct {
	AttemptIdentity          AttemptIdentity `json:"attempt_identity" runtime:"required"`
	LastAckedClientEventSeq  int64           `json:"last_acked_client_event_seq" runtime:"required"`
	PendingClientEventRanges []EventRange    `json:"pending_client_event_ranges" runtime:"required"`
	PendingResultID          *uuid.UUID      `json:"pending_result_id,omitempty"`
	FinalClientEventSeq      *int64          `json:"final_client_event_seq,omitempty"`
}

type RuntimeResumePayload struct {
	NodeID           uuid.UUID       `json:"node_id" runtime:"required"`
	AgentID          uuid.UUID       `json:"agent_id" runtime:"required"`
	WorkerID         string          `json:"worker_id" runtime:"required"`
	RuntimeSessionID uuid.UUID       `json:"runtime_session_id" runtime:"required"`
	Attempts         []ResumeAttempt `json:"attempts" runtime:"required"`
}

type RuntimeResumeDecision string

const (
	RuntimeResumeContinueExecution RuntimeResumeDecision = "continue_execution"
	RuntimeResumeUploadSpoolOnly   RuntimeResumeDecision = "upload_spool_only"
	RuntimeResumeResultAcked       RuntimeResumeDecision = "result_already_acked"
	RuntimeResumeLeaseRevoked      RuntimeResumeDecision = "lease_revoked"
)

type RuntimeResumeAction string

const (
	RuntimeActionContinueExecution RuntimeResumeAction = "continue_execution"
	RuntimeActionUploadEvents      RuntimeResumeAction = "upload_events"
	RuntimeActionUploadResult      RuntimeResumeAction = "upload_result"
	RuntimeActionStopExecution     RuntimeResumeAction = "stop_execution"
	RuntimeActionClearSpool        RuntimeResumeAction = "clear_spool"
)

type RunResumeAcceptedPayload struct {
	AttemptIdentity AttemptIdentity       `json:"attempt_identity" runtime:"required"`
	Decision        RuntimeResumeDecision `json:"decision" runtime:"required"`
	LeaseExpiresAt  *time.Time            `json:"lease_expires_at,omitempty"`
	AllowedActions  []RuntimeResumeAction `json:"allowed_actions" runtime:"required"`
}

type RuntimeResumeResponse struct {
	Decisions []RunResumeAcceptedPayload `json:"decisions" runtime:"required"`
}

type RunLeaseRevokedPayload struct {
	AttemptIdentity AttemptIdentity      `json:"attempt_identity" runtime:"required"`
	ReasonCode      string               `json:"reason_code" runtime:"required"`
	DispatchState   RuntimeDispatchState `json:"dispatch_state" runtime:"required"`
	RunStatus       RuntimeRunStatus     `json:"run_status" runtime:"required"`
}

type RuntimeDrainPayload struct {
	DeadlineAt time.Time `json:"deadline_at" runtime:"required"`
	ReasonCode string    `json:"reason_code" runtime:"required"`
	Capacity   int64     `json:"capacity" runtime:"required"`
	Inflight   int64     `json:"inflight" runtime:"required"`
}

// PendingCommand is a discriminated union. Payload is decoded strictly by
// DecodePendingCommand after Type has selected the only legal schema.
type PendingCommand struct {
	Type    RuntimeMessageType `json:"type" runtime:"required"`
	Payload json.RawMessage    `json:"payload" runtime:"required"`
}

type CancelCommand = RuntimeCommand[RunCancelPayload]
type DrainCommand = RuntimeCommand[RuntimeDrainPayload]
type RevokeCommand = RuntimeCommand[RunLeaseRevokedPayload]

type RuntimeCommand[P any] struct {
	Type    RuntimeMessageType `json:"type" runtime:"required"`
	Payload P                  `json:"payload" runtime:"required"`
}

type RuntimeCommandsResponse struct {
	Commands     []PendingCommand `json:"commands" runtime:"required"`
	DatabaseTime time.Time        `json:"database_time" runtime:"required"`
}

type RuntimeClaimRequest struct {
	RuntimeSessionID uuid.UUID `json:"runtime_session_id" runtime:"required"`
	Capacity         int64     `json:"capacity" runtime:"required"`
	Inflight         int64     `json:"inflight" runtime:"required"`
}

type CallAgentRequest struct {
	TargetAgentID uuid.UUID      `json:"target_agent_id" runtime:"required"`
	Input         map[string]any `json:"input" runtime:"required"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Reason        string         `json:"reason,omitempty"`
}

type RuntimeRunStatus string

const (
	RuntimeRunRunning  RuntimeRunStatus = "running"
	RuntimeRunSuccess  RuntimeRunStatus = "success"
	RuntimeRunFailed   RuntimeRunStatus = "failed"
	RuntimeRunTimeout  RuntimeRunStatus = "timeout"
	RuntimeRunCanceled RuntimeRunStatus = "canceled"
)

type RuntimeDispatchState string

const (
	RuntimeDispatchPending    RuntimeDispatchState = "pending"
	RuntimeDispatchOffered    RuntimeDispatchState = "offered"
	RuntimeDispatchExecuting  RuntimeDispatchState = "executing"
	RuntimeDispatchRetryWait  RuntimeDispatchState = "retry_wait"
	RuntimeDispatchTerminal   RuntimeDispatchState = "terminal"
	RuntimeDispatchDeadLetter RuntimeDispatchState = "dead_letter"
)

type RunSummary struct {
	RunID         uuid.UUID            `json:"run_id" runtime:"required"`
	Status        RuntimeRunStatus     `json:"status" runtime:"required"`
	DispatchState RuntimeDispatchState `json:"dispatch_state" runtime:"required"`
}
