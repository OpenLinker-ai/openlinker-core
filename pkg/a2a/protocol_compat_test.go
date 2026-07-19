package a2a

import (
	"encoding/json"
	"math"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeA2AResultForCurrentVersionRemovesLegacyDiscriminators(t *testing.T) {
	task := A2ATask{
		Kind:      "task",
		ID:        "task-1",
		ContextID: "ctx-1",
		Status: A2ATaskStatus{
			State: "completed",
			Message: &A2AMessage{
				Kind: "message",
				Role: "agent",
				Parts: []map[string]interface{}{
					{"kind": "text", "text": "done"},
					{"kind": "data", "data": map[string]interface{}{"ok": true}, "mediaType": "application/json"},
				},
			},
		},
		Artifacts: []A2AArtifact{{
			ArtifactID: "file-1",
			Name:       "report.csv",
			Parts: []map[string]interface{}{{
				"kind": "file",
				"file": map[string]interface{}{
					"uri":       "https://files.example/report.csv",
					"name":      "report.csv",
					"mimeType":  "text/csv",
					"sha256":    "abc",
					"sizeBytes": float64(42),
				},
			}},
		}},
		Metadata: map[string]interface{}{
			"openlinker": map[string]interface{}{"kind": "internal", "run_id": "task-1"},
		},
	}

	normalized := normalizeA2AResultForVersion(&task, a2aProtocolVersionCurrent)
	raw, err := json.Marshal(normalized)
	require.NoError(t, err)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &body))
	assert.NotContains(t, body, "kind")
	require.Contains(t, body, "status")
	status := body["status"].(map[string]interface{})
	assert.Equal(t, "TASK_STATE_COMPLETED", status["state"])
	message := status["message"].(map[string]interface{})
	assert.NotContains(t, message, "kind")
	assert.Equal(t, "ROLE_AGENT", message["role"])
	parts := message["parts"].([]interface{})
	assert.Equal(t, map[string]interface{}{"text": "done"}, parts[0])
	assert.Equal(t, "application/json", parts[1].(map[string]interface{})["mediaType"])
	artifact := body["artifacts"].([]interface{})[0].(map[string]interface{})
	filePart := artifact["parts"].([]interface{})[0].(map[string]interface{})
	assert.Equal(t, "https://files.example/report.csv", filePart["url"])
	assert.Equal(t, "report.csv", filePart["filename"])
	assert.Equal(t, "text/csv", filePart["mediaType"])
	assert.NotContains(t, body, "metadata")
}

func TestNormalizeA2AResultForVersionCompatibilityEdges(t *testing.T) {
	legacy := map[string]interface{}{
		"kind":  "task",
		"state": "completed",
	}
	assert.Equal(t, legacy, normalizeA2AResultForVersion(legacy, a2aProtocolVersionLegacy))
	assert.Nil(t, normalizeA2AResultForVersion(nil, a2aProtocolVersionCurrent))

	unmarshalable := map[string]interface{}{"bad": math.Inf(1)}
	assert.Equal(t, unmarshalable, normalizeA2AResultForVersion(unmarshalable, a2aProtocolVersionCurrent))

	normalized := normalizeA2AResultForVersion([]interface{}{
		map[string]interface{}{
			"kind": "message",
			"parts": []interface{}{
				"loose-part",
				map[string]interface{}{"kind": "file", "url": "https://files.example/raw.bin", "fileWithBytes": "Ymlu", "fileName": "raw.bin"},
			},
			"nested": map[string]interface{}{"state": "input-required"},
			"data":   map[string]interface{}{"state": "custom-data-state"},
		},
		map[string]interface{}{"kind": "task", "taskId": "task-1", "artifact": map[string]interface{}{}},
		map[string]interface{}{"kind": "task-status", "id": "task-2", "status": map[string]interface{}{}},
	}, a2aProtocolVersionCurrent)

	items, ok := normalized.([]interface{})
	require.True(t, ok)
	message := items[0].(map[string]interface{})
	assert.NotContains(t, message, "kind")
	assert.Equal(t, "TASK_STATE_INPUT_REQUIRED", message["nested"].(map[string]interface{})["state"])
	assert.Equal(t, "custom-data-state", message["data"].(map[string]interface{})["state"])
	parts := message["parts"].([]interface{})
	assert.Equal(t, "loose-part", parts[0])
	file := parts[1].(map[string]interface{})
	assert.Equal(t, "https://files.example/raw.bin", file["url"])
	assert.Equal(t, "Ymlu", file["raw"])
	assert.Equal(t, "raw.bin", file["filename"])
	assert.NotContains(t, items[1].(map[string]interface{}), "kind")
	assert.NotContains(t, items[2].(map[string]interface{}), "kind")
}

func TestOfficialA2AAgentCardViewPreservesDeclaredExtensionOnly(t *testing.T) {
	view := officialA2AAgentCardView(map[string]interface{}{
		"name":        "Extended Agent",
		"description": "authenticated card",
		"version":     "v1",
		"capabilities": map[string]interface{}{
			"streaming": true,
			"internal":  "drop-me",
		},
		"openlinker": map[string]interface{}{
			"slug":         "extended-agent",
			"card_variant": "extended",
		},
		"capability":           map[string]interface{}{"input_schema": map[string]interface{}{"type": "object"}},
		"endpoint_auth_header": "Bearer secret",
	}).(map[string]interface{})

	require.Equal(t, "extended", view["openlinker"].(map[string]interface{})["card_variant"])
	require.Equal(t, true, view["capabilities"].(map[string]interface{})["streaming"])
	assert.NotContains(t, view["capabilities"].(map[string]interface{}), "internal")
	assert.NotContains(t, view, "capability")
	assert.NotContains(t, view, "endpoint_auth_header")
}

func TestNormalizeA2AResultForCurrentVersionUsesProtoPushAndEventShapes(t *testing.T) {
	normalized := normalizeA2AResultForVersion(map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"taskId": "task-1",
				"pushNotificationConfig": map[string]interface{}{
					"id":             "cfg-1",
					"url":            "https://hooks.example/a2a",
					"token":          "secret",
					"authentication": map[string]interface{}{"scheme": "Bearer"},
				},
			},
		},
		"statusUpdate": map[string]interface{}{
			"kind":      "status-update",
			"taskId":    "task-1",
			"contextId": "ctx-1",
			"status":    map[string]interface{}{"state": "working"},
			"final":     false,
		},
	}, a2aProtocolVersionCurrent)

	body := normalized.(map[string]interface{})
	assert.NotContains(t, body, "items")
	configs := body["configs"].([]interface{})
	cfg := configs[0].(map[string]interface{})
	assert.Equal(t, "cfg-1", cfg["id"])
	assert.Equal(t, "task-1", cfg["taskId"])
	assert.Equal(t, "https://hooks.example/a2a", cfg["url"])
	assert.NotContains(t, cfg, "pushNotificationConfig")

	statusUpdate := body["statusUpdate"].(map[string]interface{})
	assert.NotContains(t, statusUpdate, "kind")
	assert.NotContains(t, statusUpdate, "final")
	assert.Equal(t, "TASK_STATE_WORKING", statusUpdate["status"].(map[string]interface{})["state"])
}

func TestNormalizeA2ATaskStateForCurrentCoversStandardStates(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
	}{
		{raw: " submitted ", want: "TASK_STATE_SUBMITTED"},
		{raw: "WORKING", want: "TASK_STATE_WORKING"},
		{raw: "task_state_completed", want: "TASK_STATE_COMPLETED"},
		{raw: "task_state_cancelled", want: "TASK_STATE_CANCELED"},
		{raw: "failed", want: "TASK_STATE_FAILED"},
		{raw: "task_state_rejected", want: "TASK_STATE_REJECTED"},
		{raw: "input_required", want: "TASK_STATE_INPUT_REQUIRED"},
		{raw: "task_state_auth_required", want: "TASK_STATE_AUTH_REQUIRED"},
		{raw: "unspecified", want: "TASK_STATE_UNSPECIFIED"},
		{raw: "vendor_state", want: "vendor_state"},
	} {
		assert.Equal(t, tc.want, normalizeA2ATaskStateForCurrent(tc.raw))
	}
}

func TestNormalizeA2ARoleForCurrentCoversStandardRoles(t *testing.T) {
	assert.Equal(t, "ROLE_USER", normalizeA2ARoleForCurrent("user"))
	assert.Equal(t, "ROLE_AGENT", normalizeA2ARoleForCurrent("agent"))
	assert.Equal(t, "ROLE_AGENT", normalizeA2ARoleForCurrent("assistant"))
	assert.Equal(t, "ROLE_UNSPECIFIED", normalizeA2ARoleForCurrent("unspecified"))
	assert.Equal(t, "vendor_role", normalizeA2ARoleForCurrent("vendor_role"))
}

func TestNormalizeA2AJSONRPCMethodAcceptsStandardAliases(t *testing.T) {
	assert.Equal(t, "message/send", normalizeA2AJSONRPCMethod("SendMessage"))
	assert.Equal(t, "message/stream", normalizeA2AJSONRPCMethod("SendStreamingMessage"))
	assert.Equal(t, "tasks/get", normalizeA2AJSONRPCMethod("GetTask"))
	assert.Equal(t, "tasks/list", normalizeA2AJSONRPCMethod("ListTasks"))
	assert.Equal(t, "tasks/cancel", normalizeA2AJSONRPCMethod("CancelTask"))
	assert.Equal(t, "tasks/resubscribe", normalizeA2AJSONRPCMethod("SubscribeToTask"))
	assert.Equal(t, "tasks/pushNotificationConfig/set", normalizeA2AJSONRPCMethod("CreateTaskPushNotificationConfig"))
	assert.Equal(t, "tasks/pushNotificationConfig/get", normalizeA2AJSONRPCMethod("GetTaskPushNotificationConfig"))
	assert.Equal(t, "tasks/pushNotificationConfig/list", normalizeA2AJSONRPCMethod("ListTaskPushNotificationConfigs"))
	assert.Equal(t, "tasks/pushNotificationConfig/delete", normalizeA2AJSONRPCMethod("DeleteTaskPushNotificationConfig"))
	assert.Equal(t, "agent/getExtendedCard", normalizeA2AJSONRPCMethod("GetExtendedAgentCard"))
}

func TestAfterSequenceFromA2ASSEUsesQueryBeforeLastEventID(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest("GET", "/stream?after_sequence=7", nil)
	req.Header.Set("Last-Event-ID", "3")
	ctx := e.NewContext(req, httptest.NewRecorder())

	seq, err := afterSequenceFromA2ASSE(ctx)

	require.NoError(t, err)
	assert.Equal(t, int32(7), seq)
}

func TestAfterSequenceFromA2ASSEUsesLastEventID(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest("GET", "/stream", nil)
	req.Header.Set("Last-Event-ID", "9")
	ctx := e.NewContext(req, httptest.NewRecorder())

	seq, err := afterSequenceFromA2ASSE(ctx)

	require.NoError(t, err)
	assert.Equal(t, int32(9), seq)
}

func TestWriteA2ASSEPayloadWritesResumableEventID(t *testing.T) {
	rec := httptest.NewRecorder()
	event := &A2ATaskStatusUpdateEvent{
		TaskID:    "task-1",
		ContextID: "ctx-1",
		Status: A2ATaskStatus{
			State: "working",
		},
		Metadata: map[string]interface{}{"openlinker_sequence": int32(42)},
	}

	require.NoError(t, writeA2ASSEPayload(rec, 42, nil, event, false, a2aProtocolVersionCurrent))

	body := rec.Body.String()
	assert.Contains(t, body, "id: 42\n")
	assert.Contains(t, body, "event: status-update\n")
	assert.Contains(t, body, "TASK_STATE_WORKING")
}

func TestTaskIDFromActionRequestStripsColonActionFromTaskParam(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest("GET", "/tasks/task-1:subscribe", nil)
	ctx := e.NewContext(req, httptest.NewRecorder())
	ctx.SetParamNames("taskID")
	ctx.SetParamValues("task-1:subscribe")

	taskID, err := taskIDFromSubscribeRequest(ctx)

	require.NoError(t, err)
	assert.Equal(t, "task-1", taskID)
}
