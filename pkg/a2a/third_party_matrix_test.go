package a2a

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type thirdPartyMatrix struct {
	Messages       []thirdPartyMessageCase       `json:"messages"`
	Normalizations []thirdPartyNormalizationCase `json:"normalizations"`
	PushConfigs    []thirdPartyPushConfigCase    `json:"push_configs"`
	Errors         []thirdPartyErrorCase         `json:"errors"`
}

type thirdPartyMessageCase struct {
	Name          string                 `json:"name"`
	Message       A2AMessage             `json:"message"`
	ExpectedInput map[string]interface{} `json:"expected_input"`
}

type thirdPartyNormalizationCase struct {
	Name                string                 `json:"name"`
	Value               map[string]interface{} `json:"value"`
	ExpectedState       string                 `json:"expected_state"`
	ExpectedRole        string                 `json:"expected_role"`
	ExpectedKindDropped bool                   `json:"expected_kind_dropped"`
}

type thirdPartyPushConfigCase struct {
	Name             string                  `json:"name"`
	Params           A2ATaskPushConfigParams `json:"params"`
	ExpectedTaskID   string                  `json:"expected_task_id"`
	ExpectedConfigID string                  `json:"expected_config_id"`
}

type thirdPartyErrorCase struct {
	Name        string `json:"name"`
	Reason      string `json:"reason"`
	JSONRPCCode int    `json:"jsonrpc_code"`
	HTTPStatus  int    `json:"http_status"`
}

func TestA2AThirdPartyFixtureMatrix(t *testing.T) {
	matrix := loadThirdPartyMatrix(t)

	for _, tc := range matrix.Messages {
		t.Run("message/"+tc.Name, func(t *testing.T) {
			input, err := inputFromA2AMessage(tc.Message)
			if err != nil {
				t.Fatalf("inputFromA2AMessage error = %v", err)
			}
			assertMatrixString(t, input, "message", tc.ExpectedInput["message"])
			assertMatrixString(t, input, "a2a_message_id", tc.ExpectedInput["a2a_message_id"])
			assertMatrixString(t, input, "a2a_context_id", tc.ExpectedInput["a2a_context_id"])
			assertMatrixString(t, input, "a2a_task_id", tc.ExpectedInput["a2a_task_id"])
			if expected, ok := tc.ExpectedInput["file_count"].(float64); ok {
				files, _ := input["files"].([]interface{})
				if len(files) != int(expected) {
					t.Fatalf("files length = %d, want %d", len(files), int(expected))
				}
			}
			if expected, ok := tc.ExpectedInput["data_count"].(float64); ok {
				dataParts, _ := input["data_parts"].([]interface{})
				if len(dataParts) != int(expected) {
					t.Fatalf("data_parts length = %d, want %d", len(dataParts), int(expected))
				}
			}
		})
	}

	for _, tc := range matrix.Normalizations {
		t.Run("normalization/"+tc.Name, func(t *testing.T) {
			normalized, ok := normalizeA2AResultForVersion(tc.Value, a2aProtocolVersionCurrent).(map[string]interface{})
			if !ok {
				t.Fatalf("normalized value is %T", normalized)
			}
			if tc.ExpectedState != "" {
				task := matrixMap(t, normalized, "task")
				status := matrixMap(t, task, "status")
				if got, _ := status["state"].(string); got != tc.ExpectedState {
					t.Fatalf("state = %q, want %q", got, tc.ExpectedState)
				}
				message := matrixMap(t, status, "message")
				if got, _ := message["role"].(string); got != tc.ExpectedRole {
					t.Fatalf("status.message.role = %q, want %q", got, tc.ExpectedRole)
				}
				assertFirstPartKindDropped(t, message, tc.ExpectedKindDropped)
				return
			}
			message := matrixMap(t, normalized, "message")
			if got, _ := message["role"].(string); got != tc.ExpectedRole {
				t.Fatalf("message.role = %q, want %q", got, tc.ExpectedRole)
			}
			assertFirstPartKindDropped(t, message, tc.ExpectedKindDropped)
		})
	}

	for _, tc := range matrix.PushConfigs {
		t.Run("push/"+tc.Name, func(t *testing.T) {
			if got := taskIDFromPushParams(&tc.Params); got != tc.ExpectedTaskID {
				t.Fatalf("taskIDFromPushParams = %q, want %q", got, tc.ExpectedTaskID)
			}
			if got := configIDFromPushParams(&tc.Params); got != tc.ExpectedConfigID {
				t.Fatalf("configIDFromPushParams = %q, want %q", got, tc.ExpectedConfigID)
			}
		})
	}

	for _, tc := range matrix.Errors {
		t.Run("error/"+tc.Name, func(t *testing.T) {
			err := matrixError(tc.Reason)
			var he *httpx.HTTPError
			if !errors.As(err, &he) {
				t.Fatalf("error type = %T", err)
			}
			if he.Status != tc.HTTPStatus {
				t.Fatalf("HTTP status = %d, want %d", he.Status, tc.HTTPStatus)
			}
			resp := jsonRPCErrorFrom(json.RawMessage(`"matrix"`), err)
			if resp.Error == nil {
				t.Fatal("jsonRPCErrorFrom missing error")
			}
			if resp.Error.Code != tc.JSONRPCCode {
				t.Fatalf("jsonrpc code = %d, want %d", resp.Error.Code, tc.JSONRPCCode)
			}
			info := matrixFirstErrorInfo(t, resp.Error.Data)
			if got, _ := info["reason"].(string); got != tc.Reason {
				t.Fatalf("ErrorInfo.reason = %q, want %q", got, tc.Reason)
			}
			if got, _ := info["domain"].(string); got != "a2a-protocol.org" {
				t.Fatalf("ErrorInfo.domain = %q", got)
			}
		})
	}
}

func loadThirdPartyMatrix(t *testing.T) thirdPartyMatrix {
	t.Helper()
	raw, err := os.ReadFile("testdata/third_party_matrix.json")
	if err != nil {
		t.Fatalf("read matrix: %v", err)
	}
	var matrix thirdPartyMatrix
	if err := json.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("decode matrix: %v", err)
	}
	return matrix
}

func assertMatrixString(t *testing.T, source map[string]interface{}, key string, expected interface{}) {
	t.Helper()
	want, _ := expected.(string)
	if want == "" {
		return
	}
	if got, _ := source[key].(string); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}

func matrixMap(t *testing.T, source map[string]interface{}, key string) map[string]interface{} {
	t.Helper()
	value, ok := source[key].(map[string]interface{})
	if !ok {
		t.Fatalf("%s is %T", key, source[key])
	}
	return value
}

func assertFirstPartKindDropped(t *testing.T, message map[string]interface{}, expected bool) {
	t.Helper()
	parts, _ := message["parts"].([]interface{})
	if len(parts) == 0 {
		t.Fatal("message.parts is empty")
	}
	part, _ := parts[0].(map[string]interface{})
	_, hasKind := part["kind"]
	if hasKind == expected {
		t.Fatalf("part kind present = %v, expected dropped = %v", hasKind, expected)
	}
}

func matrixError(reason string) error {
	switch reason {
	case a2aErrorTaskNotFound, a2aErrorReason(a2aErrorTaskNotFound):
		return a2aTaskNotFound("missing task")
	case a2aErrorUnsupportedOperation, a2aErrorReason(a2aErrorUnsupportedOperation):
		return a2aUnsupportedOperation("unsupported")
	default:
		return a2aProtocolError(reason, http.StatusBadRequest, reason, nil)
	}
}

func matrixFirstErrorInfo(t *testing.T, raw interface{}) map[string]interface{} {
	t.Helper()
	items, ok := raw.([]map[string]interface{})
	if ok && len(items) > 0 {
		return items[0]
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal error data: %v", err)
	}
	var decoded []map[string]interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil || len(decoded) == 0 {
		t.Fatalf("decode error data: %v len=%d", err, len(decoded))
	}
	return decoded[0]
}
