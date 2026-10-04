package runtime

import (
	"errors"
	"net/http"
	"unicode/utf8"
)

// RuntimeErrorCode is the stable error vocabulary shared by HTTP and
// WebSocket Runtime transports.
type RuntimeErrorCode string

const (
	RuntimeErrorBadRequest             RuntimeErrorCode = "BAD_REQUEST"
	RuntimeErrorUnauthorized           RuntimeErrorCode = "UNAUTHORIZED"
	RuntimeErrorForbidden              RuntimeErrorCode = "FORBIDDEN"
	RuntimeErrorPermissionDenied       RuntimeErrorCode = "PERMISSION_DENIED"
	RuntimeErrorNotFound               RuntimeErrorCode = "NOT_FOUND"
	RuntimeErrorConflict               RuntimeErrorCode = "CONFLICT"
	RuntimeErrorValidationFailed       RuntimeErrorCode = "VALIDATION_FAILED"
	RuntimeErrorRateLimited            RuntimeErrorCode = "RATE_LIMITED"
	RuntimeErrorInternal               RuntimeErrorCode = "INTERNAL_ERROR"
	RuntimeErrorServiceUnavailable     RuntimeErrorCode = "SERVICE_UNAVAILABLE"
	RuntimeErrorIdempotencyKeyReused   RuntimeErrorCode = "IDEMPOTENCY_KEY_REUSED"
	RuntimeErrorRunAlreadyTerminal     RuntimeErrorCode = "RUN_ALREADY_TERMINAL"
	RuntimeErrorStaleLease             RuntimeErrorCode = "STALE_LEASE"
	RuntimeErrorLeaseExpired           RuntimeErrorCode = "LEASE_EXPIRED"
	RuntimeErrorLeaseIdentityMismatch  RuntimeErrorCode = "LEASE_IDENTITY_MISMATCH"
	RuntimeErrorResultIDConflict       RuntimeErrorCode = "RESULT_ID_CONFLICT"
	RuntimeErrorEventIDConflict        RuntimeErrorCode = "EVENT_ID_CONFLICT"
	RuntimeErrorNodeAtCapacity         RuntimeErrorCode = "NODE_AT_CAPACITY"
	RuntimeErrorClientUpgradeRequired  RuntimeErrorCode = "RUNTIME_CLIENT_UPGRADE_REQUIRED"
	RuntimeErrorRequiredFeatureMissing RuntimeErrorCode = "RUNTIME_REQUIRED_FEATURE_MISSING"
	RuntimeErrorRunCancelRequested     RuntimeErrorCode = "RUN_CANCEL_REQUESTED"
	RuntimeErrorRunCancelUnconfirmed   RuntimeErrorCode = "RUN_CANCEL_UNCONFIRMED"
	RuntimeErrorRetryExhausted         RuntimeErrorCode = "RUNTIME_RETRY_EXHAUSTED"
	RuntimeErrorDispatchTimeout        RuntimeErrorCode = "RUNTIME_DISPATCH_TIMEOUT"
	RuntimeErrorRunDeadlineExceeded    RuntimeErrorCode = "RUN_DEADLINE_EXCEEDED"
	RuntimeErrorEventsMissing          RuntimeErrorCode = "EVENTS_MISSING"
	RuntimeErrorReplayInputUnavailable RuntimeErrorCode = "REPLAY_INPUT_UNAVAILABLE"
	RuntimeErrorEndpointResultUnknown  RuntimeErrorCode = "ENDPOINT_RESULT_UNKNOWN"
	RuntimeErrorSessionConflict        RuntimeErrorCode = "RUNTIME_SESSION_CONFLICT"
	RuntimeErrorSpoolCorrupt           RuntimeErrorCode = "RUNTIME_SPOOL_CORRUPT"
)

type RuntimeErrorBody struct {
	Code                 RuntimeErrorCode     `json:"code" runtime:"required"`
	Message              string               `json:"message" runtime:"required"`
	Retryable            bool                 `json:"retryable,omitempty"`
	MissingEventRanges   []EventRange         `json:"missing_event_ranges,omitempty"`
	CurrentRunStatus     RuntimeRunStatus     `json:"current_run_status,omitempty"`
	CurrentDispatchState RuntimeDispatchState `json:"current_dispatch_state,omitempty"`
}

// RuntimeError is the stable HTTP error envelope from the canonical contract.
type RuntimeError struct {
	Error RuntimeErrorBody `json:"error" runtime:"required"`
}

// RuntimeTransportError preserves a stable wire error without exposing an
// internal cause in its JSON representation.
type RuntimeTransportError struct {
	Body  RuntimeErrorBody
	cause error
}

func (e *RuntimeTransportError) Error() string {
	if e == nil {
		return ""
	}
	return e.Body.Message
}

func (e *RuntimeTransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *RuntimeTransportError) Envelope() RuntimeError {
	if e == nil {
		return RuntimeError{Error: RuntimeErrorBody{
			Code:    RuntimeErrorInternal,
			Message: runtimeErrorDefaultMessage(RuntimeErrorInternal),
		}}
	}
	return RuntimeError{Error: e.Body}
}

func NewRuntimeTransportError(code RuntimeErrorCode, message string) *RuntimeTransportError {
	if !validRuntimeErrorCode(code) {
		code = RuntimeErrorInternal
	}
	if message == "" || !utf8.ValidString(message) || utf8.RuneCountInString(message) > 500 {
		message = runtimeErrorDefaultMessage(code)
	}
	return &RuntimeTransportError{Body: RuntimeErrorBody{Code: code, Message: message}}
}

func newRuntimeTransportError(code RuntimeErrorCode, message string, cause error) *RuntimeTransportError {
	err := NewRuntimeTransportError(code, message)
	err.cause = cause
	return err
}

// NewRuntimeAuthenticationUnavailableError reports that a credential store
// could not be consulted. It fails closed without treating an infrastructure
// failure as permanent credential revocation, and never exposes the cause on
// the wire. Missing, revoked, expired and mismatched credentials must not use it.
func NewRuntimeAuthenticationUnavailableError(cause error) *RuntimeTransportError {
	err := newRuntimeTransportError(RuntimeErrorServiceUnavailable, runtimeErrorDefaultMessage(RuntimeErrorServiceUnavailable), cause)
	err.Body.Retryable = true
	return err
}

// MapRuntimeTransportError converts EventStore and ResultFinalizer errors into
// the shared transport vocabulary. Unknown causes are intentionally hidden.
func MapRuntimeTransportError(err error) *RuntimeTransportError {
	if err == nil {
		return nil
	}
	var transportErr *RuntimeTransportError
	if errors.As(err, &transportErr) {
		return transportErr
	}

	var eventErr *RuntimeEventError
	if errors.As(err, &eventErr) {
		mapped := newRuntimeTransportError(
			RuntimeErrorCode(eventErr.Code),
			runtimeErrorDefaultMessage(RuntimeErrorCode(eventErr.Code)),
			err,
		)
		mapped.Body.MissingEventRanges = append([]EventRange(nil), eventErr.MissingRanges...)
		return mapped
	}
	if errors.Is(err, ErrInvalidRuntimeEvent) {
		return newRuntimeTransportError(RuntimeErrorValidationFailed, runtimeErrorDefaultMessage(RuntimeErrorValidationFailed), err)
	}

	var resultErr *RuntimeResultError
	if errors.As(err, &resultErr) {
		mapped := newRuntimeTransportError(
			RuntimeErrorCode(resultErr.Code),
			runtimeErrorDefaultMessage(RuntimeErrorCode(resultErr.Code)),
			err,
		)
		mapped.Body.MissingEventRanges = append([]EventRange(nil), resultErr.MissingRanges...)
		return mapped
	}

	return newRuntimeTransportError(RuntimeErrorInternal, runtimeErrorDefaultMessage(RuntimeErrorInternal), err)
}

// RuntimeHTTPStatus maps every stable runtime error to a deterministic status.
func RuntimeHTTPStatus(code RuntimeErrorCode) int {
	switch code {
	case RuntimeErrorBadRequest:
		return http.StatusBadRequest
	case RuntimeErrorUnauthorized:
		return http.StatusUnauthorized
	case RuntimeErrorForbidden, RuntimeErrorPermissionDenied:
		return http.StatusForbidden
	case RuntimeErrorNotFound:
		return http.StatusNotFound
	case RuntimeErrorValidationFailed:
		return http.StatusUnprocessableEntity
	case RuntimeErrorRateLimited:
		return http.StatusTooManyRequests
	case RuntimeErrorClientUpgradeRequired, RuntimeErrorRequiredFeatureMissing:
		return http.StatusUpgradeRequired
	case RuntimeErrorReplayInputUnavailable:
		return http.StatusGone
	case RuntimeErrorEndpointResultUnknown:
		return http.StatusBadGateway
	case RuntimeErrorInternal:
		return http.StatusInternalServerError
	case RuntimeErrorServiceUnavailable:
		return http.StatusServiceUnavailable
	default:
		// Runtime state, identity, idempotency, cancellation, deadline, and
		// capacity errors all describe a conflict with current durable state.
		return http.StatusConflict
	}
}

const (
	RuntimeWSCloseAuthenticationFailed   = 4401
	RuntimeWSCloseClientUpgradeRequired  = 4406
	RuntimeWSCloseSessionConflict        = 4409
	RuntimeWSCloseRequiredFeatureMissing = 4412
	RuntimeWSCloseProtocolError          = 1002
	RuntimeWSCloseInternalError          = 1011
)

// RuntimeWebSocketCloseCode returns a close code only for connection-fatal
// errors. Durable Run errors are sent as runtime.error and keep the session
// alive, so their second return value is false.
func RuntimeWebSocketCloseCode(code RuntimeErrorCode) (int, bool) {
	switch code {
	case RuntimeErrorUnauthorized:
		return RuntimeWSCloseAuthenticationFailed, true
	case RuntimeErrorClientUpgradeRequired:
		return RuntimeWSCloseClientUpgradeRequired, true
	case RuntimeErrorSessionConflict:
		return RuntimeWSCloseSessionConflict, true
	case RuntimeErrorRequiredFeatureMissing:
		return RuntimeWSCloseRequiredFeatureMissing, true
	case RuntimeErrorBadRequest, RuntimeErrorValidationFailed:
		return RuntimeWSCloseProtocolError, true
	case RuntimeErrorInternal, RuntimeErrorServiceUnavailable:
		return RuntimeWSCloseInternalError, true
	default:
		return 0, false
	}
}

func runtimeErrorDefaultMessage(code RuntimeErrorCode) string {
	switch code {
	case RuntimeErrorBadRequest:
		return "Invalid runtime request"
	case RuntimeErrorUnauthorized:
		return "Runtime authentication failed"
	case RuntimeErrorForbidden, RuntimeErrorPermissionDenied:
		return "Runtime permission denied"
	case RuntimeErrorNotFound:
		return "Runtime resource not found"
	case RuntimeErrorConflict:
		return "Runtime state conflict"
	case RuntimeErrorValidationFailed:
		return "Runtime message validation failed"
	case RuntimeErrorRateLimited:
		return "Runtime request rate limited"
	case RuntimeErrorServiceUnavailable:
		return "Runtime service unavailable"
	case RuntimeErrorIdempotencyKeyReused:
		return "Idempotency key was reused with different input"
	case RuntimeErrorRunAlreadyTerminal:
		return "Run is already terminal"
	case RuntimeErrorStaleLease:
		return "Runtime lease is stale"
	case RuntimeErrorLeaseExpired:
		return "Runtime lease has expired"
	case RuntimeErrorLeaseIdentityMismatch:
		return "Runtime lease identity does not match"
	case RuntimeErrorResultIDConflict:
		return "Result identity conflicts with the stored result"
	case RuntimeErrorEventIDConflict:
		return "Event identity conflicts with a stored event"
	case RuntimeErrorNodeAtCapacity:
		return "Runtime node is at capacity"
	case RuntimeErrorClientUpgradeRequired:
		return "Runtime client upgrade required"
	case RuntimeErrorRequiredFeatureMissing:
		return "Runtime client is missing a required feature"
	case RuntimeErrorRunCancelRequested:
		return "Run cancellation has been requested"
	case RuntimeErrorRunCancelUnconfirmed:
		return "Run cancellation was not confirmed"
	case RuntimeErrorRetryExhausted:
		return "Runtime retry limit exhausted"
	case RuntimeErrorDispatchTimeout:
		return "Runtime dispatch deadline exceeded"
	case RuntimeErrorRunDeadlineExceeded:
		return "Run deadline exceeded"
	case RuntimeErrorEventsMissing:
		return "Runtime events are missing"
	case RuntimeErrorReplayInputUnavailable:
		return "Replay input is unavailable"
	case RuntimeErrorEndpointResultUnknown:
		return "Endpoint result is unknown"
	case RuntimeErrorSessionConflict:
		return "Runtime session conflicts with an active session"
	case RuntimeErrorSpoolCorrupt:
		return "Runtime spool is corrupt"
	default:
		return "Internal runtime error"
	}
}

func validRuntimeErrorCode(code RuntimeErrorCode) bool {
	switch code {
	case RuntimeErrorBadRequest,
		RuntimeErrorUnauthorized,
		RuntimeErrorForbidden,
		RuntimeErrorPermissionDenied,
		RuntimeErrorNotFound,
		RuntimeErrorConflict,
		RuntimeErrorValidationFailed,
		RuntimeErrorRateLimited,
		RuntimeErrorInternal,
		RuntimeErrorServiceUnavailable,
		RuntimeErrorIdempotencyKeyReused,
		RuntimeErrorRunAlreadyTerminal,
		RuntimeErrorStaleLease,
		RuntimeErrorLeaseExpired,
		RuntimeErrorLeaseIdentityMismatch,
		RuntimeErrorResultIDConflict,
		RuntimeErrorEventIDConflict,
		RuntimeErrorNodeAtCapacity,
		RuntimeErrorClientUpgradeRequired,
		RuntimeErrorRequiredFeatureMissing,
		RuntimeErrorRunCancelRequested,
		RuntimeErrorRunCancelUnconfirmed,
		RuntimeErrorRetryExhausted,
		RuntimeErrorDispatchTimeout,
		RuntimeErrorRunDeadlineExceeded,
		RuntimeErrorEventsMissing,
		RuntimeErrorReplayInputUnavailable,
		RuntimeErrorEndpointResultUnknown,
		RuntimeErrorSessionConflict,
		RuntimeErrorSpoolCorrupt:
		return true
	default:
		return false
	}
}
