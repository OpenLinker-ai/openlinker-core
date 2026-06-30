package a2a

import (
	"net/http"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	a2aErrorTaskNotFound                   = "TaskNotFoundError"
	a2aErrorTaskNotCancelable              = "TaskNotCancelableError"
	a2aErrorPushNotificationNotSupported   = "PushNotificationNotSupportedError"
	a2aErrorUnsupportedOperation           = "UnsupportedOperationError"
	a2aErrorContentTypeNotSupported        = "ContentTypeNotSupportedError"
	a2aErrorInvalidAgentResponse           = "InvalidAgentResponseError"
	a2aErrorExtendedAgentCardNotConfigured = "ExtendedAgentCardNotConfiguredError"
	a2aErrorExtensionSupportRequired       = "ExtensionSupportRequiredError"
	a2aErrorVersionNotSupported            = "VersionNotSupportedError"
)

func a2aProtocolError(errorType string, status int, message string, metadata map[string]string) *httpx.HTTPError {
	err := httpx.NewError(status, httpx.ErrorCode(errorType), message)
	err.Details = a2aErrorDetails(errorType, metadata)
	return err
}

func a2aTaskNotFound(message string) *httpx.HTTPError {
	if message == "" {
		message = "任务不存在"
	}
	return a2aProtocolError(a2aErrorTaskNotFound, http.StatusNotFound, message, nil)
}

func a2aTaskNotCancelable(message string) *httpx.HTTPError {
	if message == "" {
		message = "任务不可取消"
	}
	return a2aProtocolError(a2aErrorTaskNotCancelable, http.StatusBadRequest, message, nil)
}

func a2aContentTypeNotSupported(message string, metadata map[string]string) *httpx.HTTPError {
	if message == "" {
		message = "A2A content type 不受支持"
	}
	return a2aProtocolError(a2aErrorContentTypeNotSupported, http.StatusBadRequest, message, metadata)
}

func a2aExtensionSupportRequired(missing []string) *httpx.HTTPError {
	metadata := map[string]string{}
	if len(missing) > 0 {
		metadata["missing_extensions"] = stringsJoinComma(missing)
	}
	return a2aProtocolError(a2aErrorExtensionSupportRequired, http.StatusBadRequest, "缺少必需的 A2A-Extensions 声明", metadata)
}

func a2aVersionNotSupported(raw string) *httpx.HTTPError {
	return a2aProtocolError(a2aErrorVersionNotSupported, http.StatusBadRequest, "不支持的 A2A-Version: "+raw, map[string]string{
		"supported_versions": stringsJoinComma(a2aSupportedProtocolVersions),
	})
}

func a2aErrorDetails(errorType string, metadata map[string]string) []map[string]interface{} {
	if metadata == nil {
		metadata = map[string]string{}
	}
	return []map[string]interface{}{{
		"@type":    "type.googleapis.com/google.rpc.ErrorInfo",
		"reason":   errorType,
		"domain":   "a2a-protocol.org",
		"metadata": metadata,
	}}
}

func a2aJSONRPCCode(errorType string) int {
	switch errorType {
	case a2aErrorTaskNotFound:
		return -32001
	case a2aErrorTaskNotCancelable:
		return -32002
	case a2aErrorPushNotificationNotSupported:
		return -32003
	case a2aErrorUnsupportedOperation:
		return -32004
	case a2aErrorContentTypeNotSupported:
		return -32005
	case a2aErrorInvalidAgentResponse:
		return -32006
	case a2aErrorExtendedAgentCardNotConfigured:
		return -32007
	case a2aErrorExtensionSupportRequired:
		return -32008
	case a2aErrorVersionNotSupported:
		return -32009
	default:
		return 0
	}
}

func a2aErrorTypeFromHTTPError(err *httpx.HTTPError) string {
	if err == nil {
		return ""
	}
	errorType := string(err.Code)
	if a2aJSONRPCCode(errorType) != 0 {
		return errorType
	}
	return ""
}

func stringsJoinComma(values []string) string {
	return strings.Join(values, ",")
}
