package a2a

import (
	"encoding/json"
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
	parts := message["parts"].([]interface{})
	assert.Equal(t, map[string]interface{}{"text": "done"}, parts[0])
	assert.Equal(t, "application/json", parts[1].(map[string]interface{})["mediaType"])
	artifact := body["artifacts"].([]interface{})[0].(map[string]interface{})
	filePart := artifact["parts"].([]interface{})[0].(map[string]interface{})
	assert.Equal(t, "https://files.example/report.csv", filePart["url"])
	assert.Equal(t, "report.csv", filePart["filename"])
	assert.Equal(t, "text/csv", filePart["mediaType"])
	assert.Equal(t, "internal", body["metadata"].(map[string]interface{})["openlinker"].(map[string]interface{})["kind"])
}

func TestNormalizeA2AJSONRPCMethodAcceptsStandardAliases(t *testing.T) {
	assert.Equal(t, "message/send", normalizeA2AJSONRPCMethod("SendMessage"))
	assert.Equal(t, "message/stream", normalizeA2AJSONRPCMethod("SendStreamingMessage"))
	assert.Equal(t, "tasks/get", normalizeA2AJSONRPCMethod("GetTask"))
	assert.Equal(t, "tasks/list", normalizeA2AJSONRPCMethod("ListTasks"))
	assert.Equal(t, "tasks/cancel", normalizeA2AJSONRPCMethod("CancelTask"))
	assert.Equal(t, "tasks/resubscribe", normalizeA2AJSONRPCMethod("SubscribeToTask"))
	assert.Equal(t, "tasks/pushNotificationConfig/list", normalizeA2AJSONRPCMethod("ListTaskPushNotificationConfigs"))
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
