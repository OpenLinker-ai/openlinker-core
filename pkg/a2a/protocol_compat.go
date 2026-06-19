package a2a

import (
	"encoding/json"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const (
	a2aProtocolVersionLegacy  = "0.3"
	a2aProtocolVersionCurrent = "1.0"
	a2aVersionHeader          = "A2A-Version"
)

var a2aSupportedProtocolVersions = []string{a2aProtocolVersionLegacy, a2aProtocolVersionCurrent}

func a2aVersionFromRequest(c echo.Context) (string, error) {
	raw := strings.TrimSpace(c.Request().Header.Get(a2aVersionHeader))
	if raw == "" {
		raw = strings.TrimSpace(c.QueryParam(a2aVersionHeader))
	}
	if raw == "" {
		raw = strings.TrimSpace(c.QueryParam("a2a_version"))
	}
	if raw == "" {
		raw = strings.TrimSpace(c.QueryParam("version"))
	}
	if raw == "" {
		return a2aProtocolVersionLegacy, nil
	}
	normalized := normalizeA2AVersion(raw)
	switch normalized {
	case a2aProtocolVersionLegacy, a2aProtocolVersionCurrent:
		return normalized, nil
	default:
		return "", httpx.BadRequest("不支持的 A2A-Version: " + raw)
	}
}

func normalizeA2AVersion(raw string) string {
	value := strings.TrimSpace(strings.TrimPrefix(strings.ToLower(raw), "v"))
	switch value {
	case "0.3", "0.3.0":
		return a2aProtocolVersionLegacy
	case "1", "1.0", "1.0.0":
		return a2aProtocolVersionCurrent
	default:
		return value
	}
}

func setA2AVersionHeader(c echo.Context, version string) {
	if version != "" {
		c.Response().Header().Set(a2aVersionHeader, version)
	}
}

func a2aUnsupportedVersionJSONRPCError(id json.RawMessage, err error) JSONRPCResponse {
	return jsonRPCError(id, -32009, err.Error(), map[string]interface{}{
		"code":               "VERSION_NOT_SUPPORTED",
		"supported_versions": a2aSupportedProtocolVersions,
	})
}

func jsonRPCResultWithVersion(id json.RawMessage, result interface{}, version string) JSONRPCResponse {
	return JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      normalizeJSONRPCID(id),
		Result:  normalizeA2AResultForVersion(result, version),
	}
}

func normalizeA2AResultForVersion(result interface{}, version string) interface{} {
	if version != a2aProtocolVersionCurrent || result == nil {
		return result
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return result
	}
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return result
	}
	return normalizeA2AValueForCurrent(value)
}

func normalizeA2AValueForCurrent(value interface{}) interface{} {
	switch typed := value.(type) {
	case []interface{}:
		for i := range typed {
			typed[i] = normalizeA2AValueForCurrent(typed[i])
		}
		return typed
	case map[string]interface{}:
		if parts, ok := typed["parts"].([]interface{}); ok {
			normalized := make([]interface{}, 0, len(parts))
			for _, rawPart := range parts {
				part, ok := rawPart.(map[string]interface{})
				if !ok {
					normalized = append(normalized, normalizeA2AValueForCurrent(rawPart))
					continue
				}
				normalized = append(normalized, normalizeA2APartForCurrent(part))
			}
			typed["parts"] = normalized
		}
		for key, raw := range typed {
			if key == "parts" || key == "data" || key == "metadata" || key == "openlinker" || key == "payload" {
				continue
			}
			typed[key] = normalizeA2AValueForCurrent(raw)
		}
		if state, ok := typed["state"].(string); ok {
			typed["state"] = normalizeA2ATaskStateForCurrent(state)
		}
		if shouldDropA2AKind(typed) {
			delete(typed, "kind")
		}
		return typed
	default:
		return value
	}
}

func normalizeA2ATaskStateForCurrent(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "submitted", "task_state_submitted":
		return "TASK_STATE_SUBMITTED"
	case "working", "task_state_working":
		return "TASK_STATE_WORKING"
	case "completed", "task_state_completed":
		return "TASK_STATE_COMPLETED"
	case "canceled", "cancelled", "task_state_canceled", "task_state_cancelled":
		return "TASK_STATE_CANCELED"
	case "failed", "task_state_failed":
		return "TASK_STATE_FAILED"
	case "rejected", "task_state_rejected":
		return "TASK_STATE_REJECTED"
	case "input-required", "input_required", "task_state_input_required":
		return "TASK_STATE_INPUT_REQUIRED"
	case "auth-required", "auth_required", "task_state_auth_required":
		return "TASK_STATE_AUTH_REQUIRED"
	case "unknown", "unspecified", "task_state_unspecified":
		return "TASK_STATE_UNSPECIFIED"
	default:
		return state
	}
}

func normalizeA2APartForCurrent(part map[string]interface{}) map[string]interface{} {
	kind := partKind(part)
	switch kind {
	case "text":
		out := copyMapWithoutKeys(part, "kind", "type")
		if text, ok := part["text"]; ok {
			out["text"] = text
		}
		return out
	case "data":
		out := copyMapWithoutKeys(part, "kind", "type")
		if data, ok := part["data"]; ok {
			out["data"] = data
		}
		return out
	case "file":
		if legacyFile, ok := part["file"].(map[string]interface{}); ok {
			return normalizeA2AFilePartForCurrent(legacyFile)
		}
		return normalizeA2AFilePartForCurrent(part)
	default:
		return copyMapWithoutKeys(part, "kind", "type")
	}
}

func normalizeA2AFilePartForCurrent(source map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	if value := firstPartString(source, "url", "uri"); value != "" {
		out["url"] = value
	}
	if value, ok := source["raw"]; ok {
		out["raw"] = value
	} else if value, ok := source["fileWithBytes"]; ok {
		out["raw"] = value
	} else if value, ok := source["bytes"]; ok {
		out["raw"] = value
	}
	if value := firstPartString(source, "filename", "fileName", "name"); value != "" {
		out["filename"] = value
	}
	if value := firstPartString(source, "mediaType", "mimeType"); value != "" {
		out["mediaType"] = value
	}
	metadata := map[string]interface{}{}
	if raw, ok := source["metadata"].(map[string]interface{}); ok {
		for key, value := range raw {
			metadata[key] = value
		}
	}
	for _, key := range []string{"sha256", "sizeBytes"} {
		if value, ok := source[key]; ok {
			metadata[key] = value
		}
	}
	if len(metadata) > 0 {
		out["metadata"] = metadata
	}
	return out
}

func shouldDropA2AKind(value map[string]interface{}) bool {
	if _, ok := value["kind"]; !ok {
		return false
	}
	if _, ok := value["parts"]; ok {
		return true
	}
	if _, hasTaskID := value["taskId"]; hasTaskID {
		if _, ok := value["status"]; ok {
			return true
		}
		if _, ok := value["artifact"]; ok {
			return true
		}
	}
	if _, ok := value["status"]; ok {
		if _, hasID := value["id"]; hasID {
			return true
		}
	}
	return false
}

func copyMapWithoutKeys(in map[string]interface{}, keys ...string) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	skip := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		skip[key] = struct{}{}
	}
	for key, value := range in {
		if _, ok := skip[key]; ok {
			continue
		}
		out[key] = value
	}
	return out
}

func firstPartString(source map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := source[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
