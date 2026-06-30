package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	runtimepkg "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/webhook"
)

func TestA2AVersionJSONRPCAndQueryHelpers(t *testing.T) {
	c := newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/"})
	version, err := a2aVersionFromRequest(c)
	if err != nil || version != a2aProtocolVersionLegacy {
		t.Fatalf("default version = %q, %v", version, err)
	}

	c = newA2ATestContext(&a2aHandlerRequest{
		method:  http.MethodGet,
		target:  "/",
		headers: map[string]string{a2aVersionHeader: " v1.0.0 "},
	})
	version, err = a2aVersionFromRequest(c)
	if err != nil || version != a2aProtocolVersionCurrent {
		t.Fatalf("header version = %q, %v", version, err)
	}
	setA2AVersionHeader(c, version)
	if got := c.Response().Header().Get(a2aVersionHeader); got != a2aProtocolVersionCurrent {
		t.Fatalf("response version header = %q", got)
	}

	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/?a2a_version=0.3.0"})
	version, err = a2aVersionFromRequest(c)
	if err != nil || version != a2aProtocolVersionLegacy {
		t.Fatalf("query version = %q, %v", version, err)
	}
	c = newA2ATestContext(&a2aHandlerRequest{
		method:  http.MethodGet,
		target:  "/?a2a_extensions=https://example.com/ext/b,https://example.com/ext/a",
		headers: map[string]string{a2aExtensionsHeader: "https://example.com/ext/a, https://example.com/ext/a"},
	})
	extensions := a2aExtensionsFromRequest(c)
	if !reflect.DeepEqual(extensions, []string{"https://example.com/ext/a"}) {
		t.Fatalf("header extensions = %#v", extensions)
	}
	params, err := a2aServiceParametersFromRequest(c, []string{"https://example.com/ext/a"})
	if err != nil || params.Version != a2aProtocolVersionLegacy || len(params.Extensions) != 1 {
		t.Fatalf("service params = %#v, %v", params, err)
	}
	if _, err := a2aServiceParametersFromRequest(c, []string{"https://example.com/ext/missing"}); err == nil {
		t.Fatalf("missing required extension should fail")
	} else {
		got := jsonRPCErrorFrom(nil, err)
		if got.Error == nil || got.Error.Code != -32008 {
			t.Fatalf("required extension JSON-RPC mapping = %#v", got)
		}
	}
	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/?version=2.0"})
	if _, err := a2aVersionFromRequest(c); err == nil {
		t.Fatalf("unsupported version should fail")
	} else {
		requireA2AHTTPStatus(t, err, http.StatusBadRequest)
	}

	for _, tc := range []struct {
		raw  string
		want string
	}{
		{raw: "v1", want: a2aProtocolVersionCurrent},
		{raw: "1.0.0", want: a2aProtocolVersionCurrent},
		{raw: "0.3.0", want: a2aProtocolVersionLegacy},
		{raw: "future", want: "future"},
	} {
		if got := normalizeA2AVersion(tc.raw); got != tc.want {
			t.Fatalf("normalizeA2AVersion(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}

	badVersion := a2aUnsupportedVersionJSONRPCError(json.RawMessage(`"req-1"`), a2aVersionNotSupported("2.0"))
	if badVersion.Error == nil || badVersion.Error.Code != -32009 {
		t.Fatalf("unexpected unsupported version JSON-RPC error: %#v", badVersion)
	}
	data, ok := badVersion.Error.Data.([]map[string]interface{})
	if !ok || len(data) != 1 || data[0]["reason"] != a2aErrorReason(a2aErrorVersionNotSupported) {
		t.Fatalf("unexpected unsupported version data: %#v", badVersion.Error.Data)
	}

	result := jsonRPCResultWithVersion(json.RawMessage(`7`), A2ATask{
		Kind: "task",
		ID:   "run-1",
		Status: A2ATaskStatus{
			State: "completed",
		},
	}, a2aProtocolVersionCurrent)
	resultMap, ok := result.Result.(map[string]interface{})
	if !ok || resultMap["kind"] != nil || resultMap["status"].(map[string]interface{})["state"] != "TASK_STATE_COMPLETED" {
		t.Fatalf("current JSON-RPC result was not normalized: %#v", result.Result)
	}
	if got := string(jsonRPCNullResult(nil).Result.(json.RawMessage)); got != "null" {
		t.Fatalf("jsonRPCNullResult = %q", got)
	}
	if got := string(normalizeJSONRPCID(nil)); got != "null" {
		t.Fatalf("normalizeJSONRPCID(nil) = %q", got)
	}
	if got := jsonRPCResult(json.RawMessage(`"id"`), "ok"); string(got.ID) != `"id"` || got.Result != "ok" {
		t.Fatalf("jsonRPCResult = %#v", got)
	}

	for _, tc := range []struct {
		err      error
		wantCode int
	}{
		{err: httpx.BadRequest("bad"), wantCode: jsonRPCInvalidParams},
		{err: httpx.Unprocessable("invalid"), wantCode: jsonRPCInvalidParams},
		{err: httpx.Unauthorized("auth"), wantCode: -32010},
		{err: httpx.Forbidden("scope"), wantCode: -32011},
		{err: httpx.NotFound("missing"), wantCode: -32001},
		{err: httpx.Conflict("conflict"), wantCode: -32002},
		{err: errors.New("boom"), wantCode: jsonRPCInternalError},
	} {
		got := jsonRPCErrorFrom(nil, tc.err)
		if got.Error == nil || got.Error.Code != tc.wantCode {
			t.Fatalf("jsonRPCErrorFrom(%v) = %#v, want code %d", tc.err, got, tc.wantCode)
		}
	}
}

func TestA2AResultPartNormalizationHelpers(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
	}{
		{raw: "submitted", want: "TASK_STATE_SUBMITTED"},
		{raw: "task_state_working", want: "TASK_STATE_WORKING"},
		{raw: "cancelled", want: "TASK_STATE_CANCELED"},
		{raw: "auth_required", want: "TASK_STATE_AUTH_REQUIRED"},
		{raw: "custom", want: "custom"},
	} {
		if got := normalizeA2ATaskStateForCurrent(tc.raw); got != tc.want {
			t.Fatalf("normalizeA2ATaskStateForCurrent(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}

	textPart := normalizeA2APartForCurrent(map[string]interface{}{"kind": "text", "type": "text", "text": "hello", "metadata": map[string]interface{}{"k": "v"}})
	if _, ok := textPart["kind"]; ok || textPart["text"] != "hello" {
		t.Fatalf("text part not normalized: %#v", textPart)
	}
	dataPart := normalizeA2APartForCurrent(map[string]interface{}{"kind": "data", "data": map[string]interface{}{"ok": true}, "mediaType": "application/json"})
	if _, ok := dataPart["kind"]; ok || dataPart["data"].(map[string]interface{})["ok"] != true {
		t.Fatalf("data part not normalized: %#v", dataPart)
	}
	filePart := normalizeA2APartForCurrent(map[string]interface{}{"kind": "file", "file": map[string]interface{}{
		"uri":       "https://files.example/report.csv",
		"name":      "report.csv",
		"mimeType":  "text/csv",
		"sha256":    "abc",
		"sizeBytes": float64(42),
	}})
	if filePart["url"] != "https://files.example/report.csv" || filePart["filename"] != "report.csv" || filePart["mediaType"] != "text/csv" {
		t.Fatalf("file part not normalized: %#v", filePart)
	}
	metadata := filePart["metadata"].(map[string]interface{})
	if metadata["sha256"] != "abc" || metadata["sizeBytes"] != float64(42) {
		t.Fatalf("file metadata not preserved: %#v", filePart)
	}

	rawFile := normalizeA2AFilePartForCurrent(map[string]interface{}{
		"bytes":     "Zm9v",
		"fileName":  "foo.txt",
		"mediaType": "text/plain",
		"metadata":  map[string]interface{}{"custom": "yes"},
	})
	if rawFile["raw"] != "Zm9v" || rawFile["filename"] != "foo.txt" || rawFile["metadata"].(map[string]interface{})["custom"] != "yes" {
		t.Fatalf("raw file current part = %#v", rawFile)
	}

	if !shouldDropA2AKind(map[string]interface{}{"kind": "message", "parts": []interface{}{}}) {
		t.Fatalf("message kind should be dropped for current version")
	}
	if shouldDropA2AKind(map[string]interface{}{"kind": "internal", "openlinker": true}) {
		t.Fatalf("internal metadata kind should be retained")
	}
	copied := copyMapWithoutKeys(map[string]interface{}{"kind": "x", "keep": 1}, "kind")
	if _, ok := copied["kind"]; ok || copied["keep"] != 1 {
		t.Fatalf("copyMapWithoutKeys = %#v", copied)
	}
	if got := firstPartString(map[string]interface{}{"uri": "  https://example.com/a  "}, "url", "uri"); got != "https://example.com/a" {
		t.Fatalf("firstPartString = %q", got)
	}
}

func TestA2AHandlerUtilityHelpers(t *testing.T) {
	var params A2ATaskQueryParams
	if err := decodeJSONRPCParams(nil, &params); err == nil {
		t.Fatalf("empty params should fail")
	}
	if err := decodeJSONRPCParams(json.RawMessage(`null`), &params); err == nil {
		t.Fatalf("null params should fail")
	}
	if err := decodeJSONRPCParams(json.RawMessage(`{`), &params); err == nil {
		t.Fatalf("malformed params should fail")
	}
	if err := decodeJSONRPCParams(json.RawMessage(`{"id":"task-1"}`), &params); err != nil || params.ID != "task-1" {
		t.Fatalf("decodeJSONRPCParams valid = %#v, %v", params, err)
	}

	c := newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/"})
	if err := requireScope(c, "agents:run"); err != nil {
		t.Fatalf("non-api-key auth should not require scopes: %v", err)
	}
	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/", authMethod: "apikey"})
	requireA2AHTTPStatus(t, requireScope(c, "agents:run"), http.StatusForbidden)
	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/", authMethod: "apikey", scopes: []string{"agents:run"}})
	if err := requireScope(c, "agents:run"); err != nil {
		t.Fatalf("api key with required scope should pass: %v", err)
	}

	if got, err := optionalIntQuery("5"); err != nil || got == nil || *got != 5 {
		t.Fatalf("optionalIntQuery valid = %#v, %v", got, err)
	}
	if got, err := optionalIntQuery(""); err != nil || got != nil {
		t.Fatalf("optionalIntQuery empty = %#v, %v", got, err)
	}
	if _, err := optionalIntQuery("-1"); err == nil {
		t.Fatalf("optionalIntQuery negative should fail")
	}
	for _, raw := range []string{"1", "true", "YES", "y", "on"} {
		got, err := optionalBoolQuery(raw)
		if err != nil || got == nil || !*got {
			t.Fatalf("optionalBoolQuery(%q) = %#v, %v", raw, got, err)
		}
	}
	for _, raw := range []string{"0", "false", "NO", "n", "off"} {
		got, err := optionalBoolQuery(raw)
		if err != nil || got == nil || *got {
			t.Fatalf("optionalBoolQuery(%q) = %#v, %v", raw, got, err)
		}
	}
	if _, err := optionalBoolQuery("maybe"); err == nil {
		t.Fatalf("optionalBoolQuery invalid should fail")
	}

	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/?page_size=12&history_length=3&include_artifacts=yes&context_id=ctx&status=completed&page_token=tok&status_timestamp_after=2026-06-20T00:00:00Z"})
	taskList, err := a2aTaskListParamsFromQuery(c)
	if err != nil || taskList.PageSize == nil || *taskList.PageSize != 12 || taskList.HistoryLength == nil || *taskList.HistoryLength != 3 || taskList.IncludeArtifacts == nil || !*taskList.IncludeArtifacts {
		t.Fatalf("a2aTaskListParamsFromQuery = %#v, %v", taskList, err)
	}
	if taskList.ContextID != "ctx" || taskList.Status != "completed" || taskList.PageToken != "tok" || taskList.StatusTimestampAfter == "" {
		t.Fatalf("unexpected task list params: %#v", taskList)
	}
	if first := firstQueryParam(c, "missing", "context_id"); first != "ctx" {
		t.Fatalf("firstQueryParam = %q", first)
	}
	for _, target := range []string{
		"/?pageSize=-1",
		"/?historyLength=bad",
		"/?includeArtifacts=maybe",
	} {
		c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: target})
		if _, err := a2aTaskListParamsFromQuery(c); err == nil {
			t.Fatalf("a2aTaskListParamsFromQuery(%q) should fail", target)
		}
	}

	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/?after_sequence=-1"})
	if _, err := afterSequenceFromA2ASSE(c); err == nil {
		t.Fatalf("negative after_sequence should fail")
	}
	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/", headers: map[string]string{"Last-Event-ID": "42"}})
	if got, err := afterSequenceFromA2ASSE(c); err != nil || got != 42 {
		t.Fatalf("Last-Event-ID after sequence = %d, %v", got, err)
	}

	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/tasks/task-1/cancel", params: map[string]string{"*": "tasks/task-1/cancel"}})
	taskID, err := taskIDFromActionRequest(c, "cancel")
	if err != nil || taskID != "task-1" {
		t.Fatalf("taskIDFromAction wildcard = %q, %v", taskID, err)
	}
	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodPost, target: "/tasks/subscribe", body: `{"name":"tasks/task-2"}`})
	taskID, err = taskIDFromActionRequest(c, "subscribe")
	if err != nil || taskID != "task-2" {
		t.Fatalf("taskIDFromAction body name = %q, %v", taskID, err)
	}
	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodPost, target: "/tasks/subscribe", body: `{}`})
	if _, err := taskIDFromActionRequest(c, "subscribe"); err == nil {
		t.Fatalf("missing task id should fail")
	}

	statusEvent := &A2ATaskStatusUpdateEvent{Metadata: map[string]interface{}{"openlinker_sequence": float64(8)}}
	if got := sequenceFromStreamItem(statusEvent); got != 8 {
		t.Fatalf("sequenceFromStreamItem(float64) = %d", got)
	}
	statusEvent = &A2ATaskStatusUpdateEvent{Metadata: map[string]interface{}{"openlinker_sequence": int32(10)}}
	if got := sequenceFromStreamItem(statusEvent); got != 10 {
		t.Fatalf("sequenceFromStreamItem(int32) = %d", got)
	}
	statusEvent = &A2ATaskStatusUpdateEvent{Metadata: map[string]interface{}{"openlinker_sequence": "bad"}}
	if got := sequenceFromStreamItem(statusEvent); got != 0 {
		t.Fatalf("sequenceFromStreamItem(unsupported) = %d", got)
	}
	artifactEvent := A2ATaskArtifactUpdateEvent{Metadata: map[string]interface{}{"openlinker_sequence": int(9)}}
	if got := sequenceFromStreamItem(artifactEvent); got != 9 {
		t.Fatalf("sequenceFromStreamItem(int) = %d", got)
	}
	if got := sequenceFromStreamItem("no metadata"); got != 0 {
		t.Fatalf("sequenceFromStreamItem(default) = %d", got)
	}

	if got := streamResponseForResult(A2AMessage{Role: "agent"}); got.Message == nil {
		t.Fatalf("streamResponseForResult message = %#v", got)
	}
	if got := streamResponseForResult(A2ATask{ID: "task-1"}); got.Task == nil {
		t.Fatalf("streamResponseForResult task = %#v", got)
	}
	statusUpdate := &A2ATaskStatusUpdateEvent{Status: A2ATaskStatus{State: a2aTaskStateWorking}}
	if got := streamResponseForResult(statusUpdate); got.StatusUpdate != statusUpdate {
		t.Fatalf("streamResponseForResult status update = %#v", got)
	}
	if got := streamResponseForResult(A2ATaskStatusUpdateEvent{Status: A2ATaskStatus{State: a2aTaskStateCompleted}}); got.StatusUpdate == nil || got.StatusUpdate.Status.State != a2aTaskStateCompleted {
		t.Fatalf("streamResponseForResult status update value = %#v", got)
	}
	artifactUpdate := &A2ATaskArtifactUpdateEvent{Artifact: A2AArtifact{ArtifactID: "artifact-1"}}
	if got := streamResponseForResult(artifactUpdate); got.ArtifactUpdate != artifactUpdate {
		t.Fatalf("streamResponseForResult artifact update = %#v", got)
	}
	if got := streamResponseForResult(A2ATaskArtifactUpdateEvent{Artifact: A2AArtifact{ArtifactID: "artifact-2"}}); got.ArtifactUpdate == nil || got.ArtifactUpdate.Artifact.ArtifactID != "artifact-2" {
		t.Fatalf("streamResponseForResult artifact update value = %#v", got)
	}
	if got := streamResponseForResult(map[string]interface{}{"ok": true}); got.Message == nil || got.Message.Parts[0]["kind"] != "data" {
		t.Fatalf("streamResponseForResult default = %#v", got)
	}
	rec := httptest.NewRecorder()
	if err := writeA2ASSEPayload(rec, 7, json.RawMessage(`"stream"`), *statusUpdate, true, a2aProtocolVersionLegacy); err != nil {
		t.Fatalf("writeA2ASSEPayload legacy JSON-RPC: %v", err)
	}
	if body := rec.Body.String(); !strings.Contains(body, `id: 7`) || !strings.Contains(body, `event: status-update`) {
		t.Fatalf("legacy JSON-RPC SSE payload = %s", body)
	}
	for _, tc := range []struct {
		item interface{}
		want string
	}{
		{item: A2ATask{}, want: "task"},
		{item: A2ATaskArtifactUpdateEvent{}, want: "artifact-update"},
		{item: A2ATaskStatusUpdateEvent{}, want: "status-update"},
		{item: A2AMessage{}, want: "message"},
		{item: "x", want: "message"},
	} {
		if got := a2aSSEEventName(tc.item); got != tc.want {
			t.Fatalf("a2aSSEEventName(%T) = %q, want %q", tc.item, got, tc.want)
		}
	}

	rec = httptest.NewRecorder()
	if err := writeA2ASSEError(rec, json.RawMessage(`"id"`), httpx.NotFound("missing"), true); err != nil {
		t.Fatalf("writeA2ASSEError jsonrpc: %v", err)
	}
	if !strings.Contains(rec.Body.String(), `"code":-32001`) {
		t.Fatalf("JSON-RPC SSE error body = %s", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	if err := writeA2ASSEError(rec, nil, errors.New("plain"), false); err != nil {
		t.Fatalf("writeA2ASSEError plain: %v", err)
	}
	if !strings.Contains(rec.Body.String(), `"error":"plain"`) {
		t.Fatalf("plain SSE error body = %s", rec.Body.String())
	}
}

func TestA2AProtocolServiceInputCursorAndStatusHelpers(t *testing.T) {
	if got, err := normalizeA2AListTasksPageSize(nil); err != nil || got != defaultA2AListTasksPageSize {
		t.Fatalf("default page size = %d, %v", got, err)
	}
	zero := 0
	negative := -1
	large := 200
	if got, err := normalizeA2AListTasksPageSize(&zero); err != nil || got != defaultA2AListTasksPageSize {
		t.Fatalf("zero page size = %d, %v", got, err)
	}
	if _, err := normalizeA2AListTasksPageSize(&negative); err == nil {
		t.Fatalf("negative page size should fail")
	}
	if got, err := normalizeA2AListTasksPageSize(&large); err != nil || got != maxA2AListTasksPageSize {
		t.Fatalf("capped page size = %d, %v", got, err)
	}

	cursorAt := time.Date(2026, 6, 20, 1, 2, 3, 4, time.UTC)
	cursorID := uuid.New()
	token := encodeA2ATaskCursor(cursorAt, cursorID)
	decoded, err := decodeA2ATaskCursor(token)
	if err != nil || !decoded.StartedAt.Equal(cursorAt) || decoded.ID != cursorID {
		t.Fatalf("cursor round trip = %#v, %v", decoded, err)
	}
	for _, bad := range []string{"", "not-base64", "e30"} {
		if _, err := decodeA2ATaskCursor(bad); err == nil {
			t.Fatalf("decodeA2ATaskCursor(%q) should fail", bad)
		}
	}

	for _, tc := range []struct {
		raw  string
		want []string
	}{
		{raw: "", want: nil},
		{raw: "submitted", want: []string{"running"}},
		{raw: "TASK_STATE_COMPLETED", want: []string{"success"}},
		{raw: "failed", want: []string{"failed", "timeout"}},
		{raw: "task_state_canceled", want: []string{"canceled"}},
		{raw: "input_required", want: []string{"__none__"}},
	} {
		got, err := runStatusesFromA2ATaskState(tc.raw)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("runStatusesFromA2ATaskState(%q) = %#v, %v", tc.raw, got, err)
		}
	}
	if _, err := runStatusesFromA2ATaskState("strange"); err == nil {
		t.Fatalf("unsupported task state should fail")
	}
	parsed, noFilter, err := parseA2AStatusTimestampAfter("2026-06-20T01:02:03.000000004Z")
	if err != nil || noFilter || !parsed.Equal(cursorAt) {
		t.Fatalf("parseA2AStatusTimestampAfter = %s, %v, %v", parsed, noFilter, err)
	}
	if _, noFilter, err := parseA2AStatusTimestampAfter(""); err != nil || !noFilter {
		t.Fatalf("empty timestamp should be no-filter")
	}
	if _, _, err := parseA2AStatusTimestampAfter("bad"); err == nil {
		t.Fatalf("invalid timestamp should fail")
	}

	input, err := inputFromA2AMessage(A2AMessage{
		MessageID: "msg-1",
		ContextID: "ctx-1",
		TaskID:    "task-1",
		Parts: []map[string]interface{}{
			{"kind": "text", "text": "hello"},
			{"kind": "data", "data": map[string]interface{}{"rows": float64(2)}},
			{"url": "https://files.example/a.csv", "filename": "a.csv", "mediaType": "text/csv"},
			{"kind": "custom", "value": "kept"},
		},
	})
	if err != nil {
		t.Fatalf("inputFromA2AMessage mixed error: %v", err)
	}
	if input["message"] != "hello" || input["text"] != "hello" || input["a2a_context_id"] != "ctx-1" {
		t.Fatalf("mixed input missing text/ids: %#v", input)
	}
	if len(input["data_parts"].([]interface{})) != 1 || len(input["files"].([]interface{})) != 1 || len(input["parts"].([]map[string]interface{})) != 1 {
		t.Fatalf("mixed input missing parts: %#v", input)
	}

	sourceData := map[string]interface{}{"q": "copy me"}
	dataOnly, err := inputFromA2AMessage(A2AMessage{ContextID: "ctx-2", Parts: []map[string]interface{}{{"kind": "data", "data": sourceData}}})
	if err != nil || dataOnly["q"] != "copy me" || dataOnly["a2a_context_id"] != "ctx-2" {
		t.Fatalf("data-only input = %#v, %v", dataOnly, err)
	}
	dataOnly["q"] = "mutated"
	if sourceData["q"] != "copy me" {
		t.Fatalf("data-only map was not copied")
	}
	scalarData, err := inputFromA2AMessage(A2AMessage{Parts: []map[string]interface{}{{"data": "value"}}})
	if err != nil || scalarData["data"] != "value" {
		t.Fatalf("scalar data input = %#v, %v", scalarData, err)
	}
	for _, msg := range []A2AMessage{
		{},
		{Parts: []map[string]interface{}{{"kind": "text", "text": "  "}}},
		{Parts: []map[string]interface{}{{"kind": "data"}}},
		{Parts: []map[string]interface{}{{"kind": "file"}}},
		{Parts: []map[string]interface{}{{"kind": "file", "url": "ftp://files.example/a.csv"}}},
	} {
		if _, err := inputFromA2AMessage(msg); err == nil {
			t.Fatalf("inputFromA2AMessage(%#v) should fail", msg)
		}
	}

	legacyFile, err := filePartInput(map[string]interface{}{"file": map[string]interface{}{"uri": "http://files.example/a.txt", "fileWithBytes": "Zm9v", "fileName": "a.txt", "mimeType": "text/plain"}})
	if err != nil || legacyFile["uri"] != "http://files.example/a.txt" || legacyFile["bytes"] != "Zm9v" || legacyFile["name"] != "a.txt" {
		t.Fatalf("legacy file input = %#v, %v", legacyFile, err)
	}
	rawFileInput, err := filePartInput(map[string]interface{}{
		"raw":       "Zm9v",
		"filename":  "raw.txt",
		"mediaType": "text/plain",
		"sha256":    "abc",
		"sizeBytes": float64(3),
		"metadata":  map[string]interface{}{"source": "inline"},
	})
	if err != nil || rawFileInput["raw"] != "Zm9v" || rawFileInput["name"] != "raw.txt" || rawFileInput["metadata"].(map[string]interface{})["source"] != "inline" {
		t.Fatalf("raw file input = %#v, %v", rawFileInput, err)
	}
	if partKind(map[string]interface{}{"bytes": "abc"}) != "file" || partKind(map[string]interface{}{"text": "abc"}) != "text" || partKind(map[string]interface{}{"data": "abc"}) != "data" {
		t.Fatalf("partKind inference failed")
	}
	if partKind(map[string]interface{}{"type": "TEXT"}) != "text" || partKind(map[string]interface{}{"file": map[string]interface{}{}}) != "file" || partKind(map[string]interface{}{"mimeType": "text/plain"}) != "file" || partKind(map[string]interface{}{}) != "" {
		t.Fatalf("partKind explicit/current shape inference failed")
	}
	if err := validateA2AFileURI("https://files.example/a.txt"); err != nil {
		t.Fatalf("valid file uri rejected: %v", err)
	}
	if err := validateA2AFileURI("file:///tmp/a.txt"); err == nil {
		t.Fatalf("non-http file uri should fail")
	}
	if err := validateA2AMessageSendParams(&A2AMessageSendParams{
		Message: A2AMessage{Parts: []map[string]interface{}{{"kind": "text", "text": "hi"}}},
		Configuration: &A2ASendConfiguration{
			AcceptedOutputModes: []string{"application/xml"},
		},
	}); err == nil {
		t.Fatalf("unsupported acceptedOutputModes should fail")
	} else if got := jsonRPCErrorFrom(nil, err); got.Error == nil || got.Error.Code != -32005 {
		t.Fatalf("acceptedOutputModes error mapping = %#v", got)
	}
	if err := validateA2AMessageSendParams(&A2AMessageSendParams{
		Message: A2AMessage{Parts: []map[string]interface{}{{"kind": "file", "url": "https://files.example/a.bin", "mediaType": "application/x-unknown"}}},
	}); err == nil {
		t.Fatalf("unsupported part mediaType should fail")
	} else if got := jsonRPCErrorFrom(nil, err); got.Error == nil || got.Error.Code != -32005 {
		t.Fatalf("mediaType error mapping = %#v", got)
	}
	if err := validateA2AMessageSendParams(&A2AMessageSendParams{
		Message: A2AMessage{Parts: []map[string]interface{}{{"kind": "file", "url": "https://files.example/a.pdf", "mediaType": "application/pdf; charset=binary"}}},
		Configuration: &A2ASendConfiguration{
			AcceptedOutputModes: []string{"application/json"},
		},
	}); err != nil {
		t.Fatalf("supported media/output modes rejected: %v", err)
	}

	metadata := protocolMetadata(&A2AMessageSendParams{
		Message:  A2AMessage{MessageID: "m", ContextID: "c", TaskID: "t", Extensions: []string{"https://example.com/ext/a"}, Metadata: map[string]interface{}{"message": "meta"}},
		Metadata: map[string]interface{}{"trace": "root"},
	})
	if metadata["source"] != "a2a" || metadata["trace"] != "root" || metadata["message"] != "meta" {
		t.Fatalf("protocolMetadata merge = %#v", metadata)
	}
	if nested := metadata["a2a"].(map[string]interface{}); nested["message_id"] != "m" || nested["context_id"] != "c" || nested["task_id"] != "t" || len(nested["extensions"].([]string)) != 1 {
		t.Fatalf("protocolMetadata a2a block = %#v", nested)
	}
}

func TestA2ARunTaskArtifactAndEventMapping(t *testing.T) {
	for _, tc := range []struct {
		status string
		want   string
	}{
		{status: "running", want: a2aTaskStateWorking},
		{status: "success", want: a2aTaskStateCompleted},
		{status: "canceled", want: a2aTaskStateCanceled},
		{status: "failed", want: a2aTaskStateFailed},
		{status: "timeout", want: a2aTaskStateFailed},
		{status: "", want: a2aTaskStateSubmitted},
		{status: "custom", want: a2aTaskStateSubmitted},
	} {
		if got := stateFromRunStatus(tc.status); got != tc.want {
			t.Fatalf("stateFromRunStatus(%q) = %q, want %q", tc.status, got, tc.want)
		}
	}

	runID := uuid.NewString()
	task := taskFromRun(&runtimepkg.RunResponse{
		RunID:         runID,
		Status:        "success",
		Output:        map[string]interface{}{"summary": "done", "value": float64(2)},
		CostCents:     11,
		DurationMs:    22,
		Source:        "api",
		ParentRunID:   "parent",
		CallerAgentID: "caller",
		BillingMode:   "free_delegation",
	}, "", []runtimepkg.RunArtifactResponse{{
		ID:           "artifact-1",
		ArtifactType: "text",
		Title:        "Note",
		Content:      map[string]interface{}{"text": "artifact text"},
		Visibility:   "private",
		CreatedAt:    time.Date(2026, 6, 20, 1, 2, 3, 0, time.UTC),
	}}, []runtimepkg.RunMessageResponse{{
		ID:        "message-1",
		RunID:     runID,
		Content:   "history text",
		CreatedAt: time.Date(2026, 6, 20, 1, 2, 4, 0, time.UTC),
	}})
	if task.ID != runID || task.ContextID != runID || task.Status.State != a2aTaskStateCompleted {
		t.Fatalf("taskFromRun basic fields = %#v", task)
	}
	if task.Status.Message == nil || task.Status.Message.Parts[0]["text"] != "done" || len(task.Artifacts) != 2 || len(task.History) != 1 {
		t.Fatalf("taskFromRun messages/artifacts = %#v", task)
	}
	openlinker := task.Metadata["openlinker"].(map[string]interface{})
	if openlinker["parent_run_id"] != "parent" || openlinker["caller_agent_id"] != "caller" || openlinker["billing_mode"] != "free_delegation" {
		t.Fatalf("task metadata = %#v", openlinker)
	}

	for _, resp := range []*runtimepkg.RunResponse{
		{Status: "failed", ErrorMsg: "bad"},
		{Status: "failed", ErrorCode: "ERR"},
		{Status: "timeout"},
		{Status: "canceled"},
		{Status: "running"},
		{Status: "queued"},
	} {
		msg := statusMessageFromRun(resp)
		if resp.Status == "queued" {
			if msg != nil {
				t.Fatalf("queued status should not produce message: %#v", msg)
			}
			continue
		}
		if msg == nil || len(msg.Parts) == 0 {
			t.Fatalf("statusMessageFromRun(%#v) = %#v", resp, msg)
		}
	}
	if summaryText(map[string]interface{}{"answer": "answer text"}) != "answer text" || summaryText(map[string]interface{}{"empty": ""}) != "" {
		t.Fatalf("summaryText did not pick expected fields")
	}
	output := outputArtifact(map[string]interface{}{"message": "hello", "ok": true})
	if output.ArtifactID != "output" || output.Parts[0]["kind"] != "text" || output.Parts[1]["kind"] != "data" {
		t.Fatalf("outputArtifact = %#v", output)
	}
	projected := taskFromRun(&runtimepkg.RunResponse{
		RunID:  uuid.NewString(),
		Status: "success",
		Output: map[string]interface{}{
			"a2a": map[string]interface{}{
				"task_state":     "TASK_STATE_INPUT_REQUIRED",
				"status_message": "Need more input",
				"response_message": map[string]interface{}{
					"role":  "agent",
					"parts": []interface{}{map[string]interface{}{"kind": "text", "text": "Direct message response"}},
				},
				"artifacts": []interface{}{map[string]interface{}{
					"artifactId": "artifact-text",
					"parts":      []interface{}{map[string]interface{}{"kind": "text", "text": "Generated text content"}},
				}},
				"history": []interface{}{map[string]interface{}{
					"role":  "user",
					"parts": []interface{}{map[string]interface{}{"kind": "text", "text": "history"}},
				}},
			},
		},
	}, "ctx-projected", nil, nil)
	if projected.Status.State != a2aTaskStateInputReq || projected.Status.Message == nil || projected.Status.Message.Parts[0]["text"] != "Need more input" {
		t.Fatalf("projected task status = %#v", projected.Status)
	}
	if projected.ResponseMessage == nil || projected.ResponseMessage.Parts[0]["text"] != "Direct message response" {
		t.Fatalf("projected response message = %#v", projected.ResponseMessage)
	}
	if len(projected.Artifacts) != 1 || projected.Artifacts[0].ArtifactID != "artifact-text" || len(projected.History) != 1 {
		t.Fatalf("projected artifacts/history = %#v %#v", projected.Artifacts, projected.History)
	}
	if got := sendMessageResultForVersion(projected, a2aProtocolVersionCurrent).(A2ASendMessageResponse); got.Message == nil || got.Task != nil {
		t.Fatalf("send message projection result = %#v", got)
	}
	if !isTerminalA2ATaskState("TASK_STATE_REJECTED") || isTerminalA2ATaskState("TASK_STATE_INPUT_REQUIRED") {
		t.Fatalf("terminal state helper mismatch")
	}

	size := int64(123)
	fileArtifact := artifactFromRunArtifact(runtimepkg.RunArtifactResponse{
		ID:               "file-1",
		ArtifactType:     "file",
		Title:            "File",
		Content:          map[string]interface{}{"fallback": true},
		Visibility:       "public",
		SourceArtifactID: "source-1",
		MimeType:         "text/csv",
		FileURI:          "https://files.example/file.csv",
		FileName:         "file.csv",
		FileSHA256:       "abc",
		FileSizeBytes:    &size,
		CreatedAt:        time.Date(2026, 6, 20, 1, 2, 5, 0, time.UTC),
	})
	if fileArtifact.Parts[0]["kind"] != "file" || fileArtifact.Metadata["file_size_bytes"] != size {
		t.Fatalf("file artifact = %#v", fileArtifact)
	}
	dataArtifact := artifactFromRunArtifact(runtimepkg.RunArtifactResponse{ID: "data-1", ArtifactType: "json", Content: map[string]interface{}{"ok": true}, CreatedAt: time.Now()})
	if dataArtifact.Parts[0]["kind"] != "data" {
		t.Fatalf("data artifact = %#v", dataArtifact)
	}
	if file := filePartFromRunArtifact(runtimepkg.RunArtifactResponse{Content: map[string]interface{}{"inline": true}}); file["metadata"].(map[string]interface{})["inline"] != true {
		t.Fatalf("empty file artifact fallback = %#v", file)
	}

	seq := int32(4)
	message := messageFromRunMessage(runtimepkg.RunMessageResponse{ID: "msg", RunID: runID, EventSequence: &seq, Content: "content", CreatedAt: time.Date(2026, 6, 20, 1, 2, 6, 0, time.UTC)})
	if message.Role != "agent" || message.Metadata["openlinker"].(map[string]interface{})["event_sequence"] != &seq {
		t.Fatalf("messageFromRunMessage = %#v", message)
	}

	eventAt := time.Date(2026, 6, 20, 2, 0, 0, 0, time.UTC)
	statusUpdate := streamEventFromRunEvent("task", "ctx", runtimepkg.RunEventResponse{EventID: "evt-1", Sequence: 5, EventType: "run.completed", Payload: map[string]interface{}{"text": "done"}, CreatedAt: eventAt})
	status, ok := statusUpdate.(*A2ATaskStatusUpdateEvent)
	if !ok || !status.Final || status.Status.State != a2aTaskStateCompleted || sequenceFromStreamItem(status) != 5 {
		t.Fatalf("status event = %#v", statusUpdate)
	}
	artifactUpdate := streamEventFromRunEvent("task", "", runtimepkg.RunEventResponse{EventID: "evt-2", Sequence: 6, EventType: "run.artifact.delta", Payload: map[string]interface{}{"title": "Report", "text": "chunk", "lastChunk": true}, CreatedAt: eventAt})
	artifact, ok := artifactUpdate.(*A2ATaskArtifactUpdateEvent)
	if !ok || artifact.ContextID != "task" || !artifact.LastChunk || artifact.Artifact.Parts[0]["kind"] != "text" {
		t.Fatalf("artifact event = %#v", artifactUpdate)
	}
	unknownUpdate := streamEventFromRunEvent("task", "", runtimepkg.RunEventResponse{EventID: "evt-unknown", Sequence: 7, EventType: "run.custom", Payload: map[string]interface{}{"text": "custom"}, CreatedAt: eventAt})
	unknownStatus, ok := unknownUpdate.(*A2ATaskStatusUpdateEvent)
	if !ok || unknownStatus.ContextID != "task" || unknownStatus.Status.Message.Parts[0]["text"] != "custom" {
		t.Fatalf("unknown stream event = %#v", unknownUpdate)
	}
	if part := artifactPartFromPayload(map[string]interface{}{"parts": []interface{}{map[string]interface{}{"url": "https://files.example/a.txt"}}}); part["kind"] != "file" {
		t.Fatalf("artifactPartFromPayload file = %#v", part)
	}
	if part := artifactPartFromPayload(map[string]interface{}{"file": map[string]interface{}{"uri": "https://files.example/direct.txt"}}); part["kind"] != "file" {
		t.Fatalf("artifactPartFromPayload direct file = %#v", part)
	}
	if part := artifactPartFromPayload(map[string]interface{}{"parts": []interface{}{
		"skip",
		map[string]interface{}{"kind": "file", "file": map[string]interface{}{"uri": "https://files.example/nested.txt"}},
	}}); part["kind"] != "file" || part["file"].(map[string]interface{})["uri"] != "https://files.example/nested.txt" {
		t.Fatalf("artifactPartFromPayload nested file = %#v", part)
	}
	if part := artifactPartFromPayload(map[string]interface{}{"data": map[string]interface{}{"ok": true}}); part["kind"] != "data" {
		t.Fatalf("artifactPartFromPayload data = %#v", part)
	}
	if part := artifactPartFromPayload(map[string]interface{}{"unknown": "kept"}); part["kind"] != "data" {
		t.Fatalf("artifactPartFromPayload fallback = %#v", part)
	}
	if payloadString(nil, "x") != "" || payloadString(map[string]interface{}{"x": "  y  "}, "x") != "y" {
		t.Fatalf("payloadString failed")
	}
	if !isTerminalRunEvent("run.failed") || isTerminalRunEvent("run.started") {
		t.Fatalf("isTerminalRunEvent failed")
	}

	finishedAt := eventAt.Add(time.Minute)
	duration := int32(88)
	errCode := "ERR"
	errMessage := "failed"
	dbTask := taskFromDBRun(db.Run{
		ID:           uuid.MustParse(runID),
		UserID:       uuid.New(),
		AgentID:      uuid.New(),
		Input:        []byte(`{"contextId":"ctx-db"}`),
		Output:       []byte(`{"summary":"db done"}`),
		Status:       "success",
		ErrorCode:    &errCode,
		ErrorMessage: &errMessage,
		CostCents:    3,
		DurationMs:   &duration,
		StartedAt:    eventAt,
		FinishedAt:   &finishedAt,
		Source:       "api",
	}, true, nil, nil)
	if dbTask.ContextID != "ctx-db" || dbTask.Status.Timestamp != "2026-06-20T02:01:00Z" || len(dbTask.Artifacts) == 0 {
		t.Fatalf("taskFromDBRun = %#v", dbTask)
	}
	if a2aContextIDFromRunInput([]byte(`{"context_id":"ctx-snake"}`)) != "ctx-snake" || a2aContextIDFromRunInput([]byte(`bad`)) != "" {
		t.Fatalf("a2aContextIDFromRunInput failed")
	}
}

func TestA2ARunEventMessageFallbacks(t *testing.T) {
	eventAt := time.Date(2026, 6, 20, 3, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		eventType string
		wantState string
		wantText  string
		wantFinal bool
	}{
		{eventType: "run.created", wantState: a2aTaskStateSubmitted, wantText: "OpenLinker task created"},
		{eventType: "run.started", wantState: a2aTaskStateWorking, wantText: "OpenLinker task started"},
		{eventType: "run.dispatch.pending", wantState: a2aTaskStateWorking, wantText: "OpenLinker task is waiting for a runtime worker"},
		{eventType: "run.dispatch.claimed", wantState: a2aTaskStateWorking, wantText: "OpenLinker task was claimed by a runtime worker"},
		{eventType: "run.completed", wantState: a2aTaskStateCompleted, wantText: "OpenLinker task completed", wantFinal: true},
		{eventType: "run.failed", wantState: a2aTaskStateFailed, wantText: "OpenLinker task failed", wantFinal: true},
		{eventType: "run.canceled", wantState: a2aTaskStateCanceled, wantText: "OpenLinker task canceled", wantFinal: true},
	} {
		t.Run(tc.eventType, func(t *testing.T) {
			event := runtimepkg.RunEventResponse{EventID: "evt", Sequence: 1, EventType: tc.eventType, Payload: map[string]interface{}{}, CreatedAt: eventAt}
			update := statusUpdateFromRunEvent("task", "ctx", event)
			if update.ContextID != "ctx" || update.Status.State != tc.wantState || update.Final != tc.wantFinal {
				t.Fatalf("statusUpdateFromRunEvent = %#v", update)
			}
			if update.Status.Message == nil || update.Status.Message.Parts[0]["text"] != tc.wantText {
				t.Fatalf("status message = %#v, want %q", update.Status.Message, tc.wantText)
			}
		})
	}

	explicit := messageFromRunEvent(runtimepkg.RunEventResponse{EventType: "custom", Payload: map[string]interface{}{"message": "from payload"}})
	if explicit == nil || explicit.Parts[0]["text"] != "from payload" {
		t.Fatalf("messageFromRunEvent payload message = %#v", explicit)
	}
	if got := messageFromRunEvent(runtimepkg.RunEventResponse{EventType: "custom", Payload: map[string]interface{}{}}); got != nil {
		t.Fatalf("unknown event without text should not produce a message: %#v", got)
	}
}

func TestA2ARuntimeWorkbenchTokenAndPushHelpers(t *testing.T) {
	if svc := NewService(nil, nil); svc == nil || svc.queries == nil {
		t.Fatalf("NewService did not initialize queries: %#v", svc)
	}
	if got := runtimeTokenScopesForAgent(db.Agent{ConnectionMode: "direct_http"}); !reflect.DeepEqual(got, []string{"agent:call"}) {
		t.Fatalf("direct token scopes = %#v", got)
	}
	if got := runtimeTokenScopesForAgent(db.Agent{ConnectionMode: "runtime_pull"}); !reflect.DeepEqual(got, []string{"agent:call", "agent:pull"}) {
		t.Fatalf("runtime pull token scopes = %#v", got)
	}
	if got := runtimeTokenScopesForAgent(db.Agent{ConnectionMode: "runtime_ws"}); !reflect.DeepEqual(got, []string{"agent:call", "agent:pull"}) {
		t.Fatalf("runtime ws token scopes = %#v", got)
	}
	if !isQueuedRuntimeConnectionMode("runtime_pull") || !isQueuedRuntimeConnectionMode("runtime_ws") || isQueuedRuntimeConnectionMode("direct_http") {
		t.Fatalf("isQueuedRuntimeConnectionMode failed")
	}

	lastUsed := "2026-06-20T01:02:03Z"
	revokedAt := "2026-06-20T02:02:03Z"
	tokens := []RuntimeTokenResponse{
		{Scopes: []string{"agent:call"}, LastUsedAt: &lastUsed},
		{Scopes: []string{"agent:call", "agent:pull"}, RevokedAt: &revokedAt},
	}
	if activeRuntimeTokenCount(tokens) != 1 || hasActiveRuntimePullToken(tokens) {
		t.Fatalf("active token helpers failed")
	}
	tokens = append(tokens, RuntimeTokenResponse{Scopes: []string{"agent:call", "agent:pull"}, LastUsedAt: &lastUsed})
	if activeRuntimeTokenCount(tokens) != 2 || !hasActiveRuntimePullToken(tokens) {
		t.Fatalf("active runtime pull token helpers failed")
	}

	if got := runtimeWorkbenchAvailability(db.Agent{LifecycleStatus: "disabled"}, tokens, nil); got != "disabled" {
		t.Fatalf("disabled availability = %q", got)
	}
	if got := runtimeWorkbenchAvailability(db.Agent{LifecycleStatus: "active"}, nil, []RuntimeWorkbenchRun{{Status: "success"}}); got != "healthy" {
		t.Fatalf("healthy availability = %q", got)
	}
	if got := runtimeWorkbenchAvailability(db.Agent{LifecycleStatus: "active", ConnectionMode: "runtime_pull"}, tokens, nil); got != "active" {
		t.Fatalf("active availability = %q", got)
	}
	if got := runtimeWorkbenchAvailability(db.Agent{LifecycleStatus: "active"}, nil, nil); got != "unknown" {
		t.Fatalf("unknown availability = %q", got)
	}

	diagnostics := runtimeWorkbenchDiagnostics(db.Agent{ConnectionMode: "direct_http"}, nil, 0, nil, nil)
	if len(diagnostics) != 1 || diagnostics[0].Code != "not_runtime_pull" {
		t.Fatalf("direct diagnostics = %#v", diagnostics)
	}
	pullNotClaimed := "RUNTIME_PULL_NOT_CLAIMED"
	resultTimeout := "RUNTIME_PULL_RESULT_TIMEOUT"
	diagnostics = runtimeWorkbenchDiagnostics(db.Agent{ConnectionMode: "runtime_pull"}, []RuntimeTokenResponse{{Scopes: []string{"agent:call"}}}, 2, []RuntimeWorkbenchRun{{ErrorCode: &pullNotClaimed}, {ErrorCode: &resultTimeout}}, nil)
	codes := diagnosticCodes(diagnostics)
	for _, want := range []string{"scope_missing", "no_recent_runtime_activity", "pending_claimable_runs", "pending_not_claimed", "result_timeout"} {
		if !containsString(codes, want) {
			t.Fatalf("missing diagnostic %q in %#v", want, diagnostics)
		}
	}
	diagnostics = runtimeWorkbenchDiagnostics(db.Agent{ConnectionMode: "runtime_pull"}, []RuntimeTokenResponse{{Scopes: []string{"agent:call", "agent:pull"}, LastUsedAt: &lastUsed}}, 0, nil, &lastUsed)
	if len(diagnostics) != 1 || diagnostics[0].Code != "runtime_ready" {
		t.Fatalf("ready diagnostics = %#v", diagnostics)
	}

	runID := uuid.New()
	if got, err := currentRunIDFromRequest(&CallAgentRequest{CurrentRunID: runID.String()}); err != nil || got != runID {
		t.Fatalf("currentRunID current = %s, %v", got, err)
	}
	if got, err := currentRunIDFromRequest(&CallAgentRequest{ParentRunID: runID.String()}); err != nil || got != runID {
		t.Fatalf("currentRunID parent = %s, %v", got, err)
	}
	for _, req := range []*CallAgentRequest{
		{},
		{CurrentRunID: "bad"},
		{CurrentRunID: runID.String(), ParentRunID: uuid.NewString()},
	} {
		if _, err := currentRunIDFromRequest(req); err == nil {
			t.Fatalf("currentRunIDFromRequest(%#v) should fail", req)
		}
	}

	if !hasScope([]string{"agent:call"}, "agent:call") || hasScope([]string{"agent:call"}, "agent:pull") {
		t.Fatalf("hasScope failed")
	}
	service := &Service{}
	if _, err := service.verifyRuntimeToken(context.Background(), "bad"); err == nil {
		t.Fatalf("invalid token should fail before database lookup")
	} else {
		requireA2AHTTPStatus(t, err, http.StatusUnauthorized)
	}

	now := time.Date(2026, 6, 20, 1, 2, 3, 0, time.UTC)
	last := now.Add(time.Minute)
	revoked := now.Add(2 * time.Minute)
	tokenResp := tokenResponse(db.AgentRuntimeToken{
		ID:         uuid.New(),
		AgentID:    uuid.New(),
		Name:       "worker",
		Prefix:     "ol_live_test",
		Scopes:     []string{"agent:call"},
		LastUsedAt: &last,
		RevokedAt:  &revoked,
		CreatedAt:  now,
	})
	if tokenResp.LastUsedAt == nil || tokenResp.RevokedAt == nil || tokenResp.CreatedAt != "2026-06-20T01:02:03Z" {
		t.Fatalf("tokenResponse = %#v", tokenResp)
	}

	if refs := skillRefs(nil, nil); refs == nil || len(refs) != 0 {
		t.Fatalf("skillRefs nil = %#v", refs)
	}
	refs := skillRefs([]string{"skill/a", "skill/b"}, []string{"Skill A"})
	if refs[0].Name != "Skill A" || refs[1].Name != "skill/b" {
		t.Fatalf("skillRefs fallback = %#v", refs)
	}

	if taskIDFromPushParams(&A2ATaskPushConfigParams{TaskID: " task ", ID: "fallback"}) != "task" || taskIDFromPushParams(&A2ATaskPushConfigParams{ID: " id "}) != "id" || taskIDFromPushParams(nil) != "" {
		t.Fatalf("taskIDFromPushParams failed")
	}
	scheme, credentials := callbackAuthFromA2AConfig(A2APushNotificationConfig{Authentication: &A2APushAuthenticationInfo{Scheme: " HMAC ", Credentials: " secret "}, Token: "token"})
	if scheme != "HMAC" || credentials != "secret" {
		t.Fatalf("callbackAuthFromA2AConfig auth = %q %q", scheme, credentials)
	}
	scheme, credentials = callbackAuthFromA2AConfig(A2APushNotificationConfig{Token: " token "})
	if scheme != "Bearer" || credentials != "token" {
		t.Fatalf("callbackAuthFromA2AConfig token = %q %q", scheme, credentials)
	}
	if got := defaultTaskCallbackEventTypes([]string{"run.completed"}); !reflect.DeepEqual(got, []string{"run.completed"}) {
		t.Fatalf("defaultTaskCallbackEventTypes custom = %#v", got)
	}
	if got := defaultTaskCallbackEventTypes(nil); len(got) < 5 || !containsString(got, "run.completed") {
		t.Fatalf("defaultTaskCallbackEventTypes default = %#v", got)
	}
	authScheme := "Bearer"
	authCredentials := "secret"
	sub := db.TaskCallbackSubscription{
		ID:                  uuid.New(),
		TargetURL:           "https://hooks.example/a2a",
		EventTypes:          []string{"run.completed"},
		AuthScheme:          &authScheme,
		AuthCredentials:     &authCredentials,
		Metadata:            []byte(`{"client":"test"}`),
		Status:              "active",
		ConsecutiveFailures: 2,
	}
	pushCfg := a2aPushConfigFromTaskCallback("task-1", sub, false)
	if pushCfg.TaskID != "task-1" || pushCfg.PushNotificationConfig.Authentication.Credentials != "" || pushCfg.PushNotificationConfig.Metadata["client"] != "test" {
		t.Fatalf("a2aPushConfigFromTaskCallback public = %#v", pushCfg)
	}
	pushCfg = a2aPushConfigFromTaskCallback("task-1", sub, true)
	if pushCfg.PushNotificationConfig.Authentication.Credentials != "secret" {
		t.Fatalf("a2aPushConfigFromTaskCallback credentials = %#v", pushCfg)
	}
	metadata := a2aMetadataFromTaskCallback(sub)
	if metadata["openlinker_subscription_status"] != "active" || metadata["openlinker_consecutive_failures"] != int32(2) || metadata["client"] != "test" {
		t.Fatalf("a2aMetadataFromTaskCallback = %#v", metadata)
	}

	sub.Metadata = []byte(`{`)
	metadata = a2aMetadataFromTaskCallback(sub)
	if metadata["openlinker_subscription_status"] != "active" || metadata["openlinker_consecutive_failures"] != int32(2) {
		t.Fatalf("a2aMetadataFromTaskCallback invalid json = %#v", metadata)
	}
	scheme, credentials = callbackAuthFromA2AConfig(A2APushNotificationConfig{Authentication: &A2APushAuthenticationInfo{Scheme: " HMAC "}})
	if scheme != "" || credentials != "" {
		t.Fatalf("incomplete push auth should be ignored, got %q %q", scheme, credentials)
	}
}

func TestA2APushServiceValidationBranches(t *testing.T) {
	ctx := context.Background()
	userID := uuid.New()
	svc := NewService(nil, nil)

	_, err := svc.SetPushNotificationConfig(ctx, userID, "agent", &A2ATaskPushConfigParams{ID: uuid.NewString()})
	requireA2AHTTPStatus(t, err, http.StatusServiceUnavailable)
	err = svc.DeletePushNotificationConfig(ctx, userID, "agent", &A2ATaskPushConfigParams{ID: uuid.NewString()})
	requireA2AHTTPStatus(t, err, http.StatusServiceUnavailable)

	svc.SetTaskCallbackManager(noopTaskCallbackManager{})
	_, err = svc.SetPushNotificationConfig(ctx, userID, "agent", nil)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)
	err = svc.DeletePushNotificationConfig(ctx, userID, "agent", nil)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.GetPushNotificationConfig(ctx, userID, "agent", nil)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.ListPushNotificationConfigs(ctx, userID, "agent", nil)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)

	if err := svc.createInlinePushConfig(ctx, userID, "agent", "task", nil); err != nil {
		t.Fatalf("nil params should skip inline push config: %v", err)
	}
	if err := svc.createInlinePushConfig(ctx, userID, "agent", "task", &A2AMessageSendParams{}); err != nil {
		t.Fatalf("nil configuration should skip inline push config: %v", err)
	}
	if err := svc.createInlinePushConfig(ctx, userID, "agent", "task", &A2AMessageSendParams{Configuration: &A2ASendConfiguration{}}); err != nil {
		t.Fatalf("empty configuration should skip inline push config: %v", err)
	}

	noPush := NewService(nil, nil)
	err = noPush.createInlinePushConfig(ctx, userID, "agent", "task", &A2AMessageSendParams{
		Configuration: &A2ASendConfiguration{
			TaskPushNotificationConfig: &A2ATaskPushNotificationConfig{
				PushNotificationConfig: A2APushNotificationConfig{URL: "https://hooks.example/a2a"},
			},
		},
	})
	requireA2AHTTPStatus(t, err, http.StatusServiceUnavailable)
}

func TestA2AProtocolServiceValidationAndDBErrorMapping(t *testing.T) {
	ctx := context.Background()
	userID := uuid.New()
	params := &A2AMessageSendParams{Message: A2AMessage{
		Role:  "user",
		Parts: []map[string]interface{}{{"kind": "text", "text": "hello"}},
	}}

	svc := NewService(nil, nil)
	_, err := svc.SendProtocolMessage(ctx, userID, " ", params)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.SendProtocolMessage(ctx, userID, "agent-one", nil)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.StartProtocolMessage(ctx, userID, " ", params)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.StartProtocolMessage(ctx, userID, "agent-one", nil)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)

	missingSvc := &Service{queries: db.New(&a2aErrorDBTX{rowErr: pgx.ErrNoRows})}
	_, err = missingSvc.SendProtocolMessage(ctx, userID, "agent-one", params)
	requireA2AHTTPStatus(t, err, http.StatusNotFound)
	_, err = missingSvc.StartProtocolMessage(ctx, userID, "agent-one", params)
	requireA2AHTTPStatus(t, err, http.StatusNotFound)
	_, err = missingSvc.ListProtocolTasks(ctx, userID, "agent-one", nil)
	requireA2AHTTPStatus(t, err, http.StatusNotFound)
	_, err = missingSvc.GetProtocolTask(ctx, userID, "agent-one", uuid.NewString(), nil)
	requireA2AHTTPStatus(t, err, http.StatusNotFound)

	brokenSvc := &Service{queries: db.New(&a2aErrorDBTX{rowErr: errors.New("database offline")})}
	_, err = brokenSvc.SendProtocolMessage(ctx, userID, "agent-one", params)
	requireA2AHTTPStatus(t, err, http.StatusInternalServerError)
	_, err = brokenSvc.StartProtocolMessage(ctx, userID, "agent-one", params)
	requireA2AHTTPStatus(t, err, http.StatusInternalServerError)
	_, err = brokenSvc.ListProtocolTasks(ctx, userID, "agent-one", nil)
	requireA2AHTTPStatus(t, err, http.StatusInternalServerError)
	_, err = brokenSvc.GetProtocolTask(ctx, userID, "agent-one", uuid.NewString(), nil)
	requireA2AHTTPStatus(t, err, http.StatusInternalServerError)

	_, err = svc.GetProtocolTask(ctx, userID, "", uuid.NewString(), nil)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)
	_, err = svc.GetProtocolTask(ctx, userID, "agent-one", "bad", nil)
	requireA2AHTTPStatus(t, err, http.StatusBadRequest)
}

func TestA2AListTaskCallbackSubscriptionsErrorMapping(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	userID := uuid.New()

	svc := &Service{queries: db.New(&a2aErrorDBTX{queryErr: pgx.ErrNoRows})}
	items, err := svc.listTaskCallbackSubscriptionsForA2A(ctx, runID, userID)
	if err != nil || len(items) != 0 {
		t.Fatalf("pgx.ErrNoRows should map to empty list, got %#v, %v", items, err)
	}

	svc = &Service{queries: db.New(&a2aErrorDBTX{queryErr: errors.New("database offline")})}
	_, err = svc.listTaskCallbackSubscriptionsForA2A(ctx, runID, userID)
	requireA2AHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestA2AHTTPHandlersValidateBeforeServiceDispatch(t *testing.T) {
	h := NewHandler(nil)
	userID := uuid.NewString()
	agentID := uuid.NewString()
	runID := uuid.NewString()

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    *a2aHandlerRequest
		want   int
	}{
		{name: "create token missing user", method: h.CreateRuntimeToken, req: &a2aHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "create token invalid id", method: h.CreateRuntimeToken, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "create token invalid json", method: h.CreateRuntimeToken, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: "{", params: map[string]string{"id": agentID}}, want: http.StatusBadRequest},
		{name: "create token validation", method: h.CreateRuntimeToken, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`, params: map[string]string{"id": agentID}}, want: http.StatusUnprocessableEntity},
		{name: "list tokens invalid id", method: h.ListRuntimeTokens, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "revoke token invalid id", method: h.RevokeRuntimeToken, req: &a2aHandlerRequest{method: http.MethodDelete, target: "/", userID: userID, params: map[string]string{"tokenID": "bad"}}, want: http.StatusBadRequest},
		{name: "workbench invalid id", method: h.GetRuntimeWorkbench, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "get policy invalid id", method: h.GetCallPolicy, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "update policy invalid json", method: h.UpdateCallPolicy, req: &a2aHandlerRequest{method: http.MethodPut, target: "/", userID: userID, body: "{", params: map[string]string{"id": agentID}}, want: http.StatusBadRequest},
		{name: "update policy validation", method: h.UpdateCallPolicy, req: &a2aHandlerRequest{method: http.MethodPut, target: "/", userID: userID, body: `{}`, params: map[string]string{"id": agentID}}, want: http.StatusUnprocessableEntity},
		{name: "call agent missing bearer", method: h.CallAgent, req: &a2aHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "call agent invalid json", method: h.CallAgent, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", body: "{", headers: map[string]string{echo.HeaderAuthorization: "Bearer token"}}, want: http.StatusBadRequest},
		{name: "call agent validation", method: h.CallAgent, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", body: `{}`, headers: map[string]string{echo.HeaderAuthorization: "Bearer token"}}, want: http.StatusUnprocessableEntity},
		{name: "call agent missing current run", method: h.CallAgent, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", body: `{"target_agent_id":"` + agentID + `","input":{"q":"hi"}}`, headers: map[string]string{echo.HeaderAuthorization: "Bearer token"}}, want: http.StatusUnprocessableEntity},
		{name: "list children invalid id", method: h.ListChildren, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "list parents missing user", method: h.ListParentRuns, req: &a2aHandlerRequest{method: http.MethodGet, target: "/"}, want: http.StatusUnauthorized},
		{name: "extended card bad version", method: h.GetExtendedAgentCardHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/?version=2"}, want: http.StatusBadRequest},
		{name: "extended card missing scope", method: h.GetExtendedAgentCardHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", authMethod: "apikey", userID: userID}, want: http.StatusForbidden},
		{name: "send message invalid json", method: h.SendMessageHTTP, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: "{"}, want: http.StatusBadRequest},
		{name: "stream message invalid json", method: h.StreamMessageHTTP, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: "{"}, want: http.StatusBadRequest},
		{name: "list tasks missing user", method: h.ListTasksHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/"}, want: http.StatusUnauthorized},
		{name: "list tasks invalid include artifacts", method: h.ListTasksHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/?includeArtifacts=maybe", userID: userID}, want: http.StatusBadRequest},
		{name: "get task missing scope", method: h.GetTaskHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID, authMethod: "apikey", params: map[string]string{"taskID": runID}}, want: http.StatusForbidden},
		{name: "get task bad history", method: h.GetTaskHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/?historyLength=-1", userID: userID, params: map[string]string{"taskID": runID}}, want: http.StatusBadRequest},
		{name: "subscribe missing scope", method: h.SubscribeTaskHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID, authMethod: "apikey", params: map[string]string{"taskID": runID}}, want: http.StatusForbidden},
		{name: "subscribe missing task", method: h.SubscribeTaskHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID}, want: http.StatusBadRequest},
		{name: "cancel missing scope", method: h.CancelTaskHTTP, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, authMethod: "apikey", params: map[string]string{"taskID": runID}}, want: http.StatusForbidden},
		{name: "cancel missing task", method: h.CancelTaskHTTP, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID}, want: http.StatusBadRequest},
		{name: "set push missing scope", method: h.SetTaskPushNotificationHTTP, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, authMethod: "apikey", body: `{}`, params: map[string]string{"taskID": runID}}, want: http.StatusForbidden},
		{name: "set push invalid json", method: h.SetTaskPushNotificationHTTP, req: &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: "{", params: map[string]string{"taskID": runID}}, want: http.StatusBadRequest},
		{name: "list push missing user", method: h.ListTaskPushNotificationsHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", params: map[string]string{"taskID": runID}}, want: http.StatusUnauthorized},
		{name: "get push missing scope", method: h.GetTaskPushNotificationHTTP, req: &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID, authMethod: "apikey", params: map[string]string{"taskID": runID, "configID": uuid.NewString()}}, want: http.StatusForbidden},
		{name: "delete push missing user", method: h.DeleteTaskPushNotificationHTTP, req: &a2aHandlerRequest{method: http.MethodDelete, target: "/", params: map[string]string{"taskID": runID, "configID": uuid.NewString()}}, want: http.StatusUnauthorized},
		{name: "message unknown action", method: h.MessageHTTP, req: &a2aHandlerRequest{method: http.MethodPost, target: "/message:bad", params: map[string]string{"action": ":bad"}}, want: http.StatusNotFound},
		{name: "task action unknown", method: h.TaskActionHTTP, req: &a2aHandlerRequest{method: http.MethodPost, target: "/tasks/x:bad", params: map[string]string{"*": "x:bad"}}, want: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newA2ATestContext(tc.req)
			requireA2AHTTPStatus(t, tc.method(c), tc.want)
		})
	}

	if got := parseInt32Query("bad", 7); got != 7 {
		t.Fatalf("parseInt32Query fallback = %d", got)
	}
	if got := parseInt32Query("12", 7); got != 12 {
		t.Fatalf("parseInt32Query valid = %d", got)
	}
	if got, err := bearerToken(" Bearer abc "); err != nil || got != "abc" {
		t.Fatalf("bearerToken valid = %q, %v", got, err)
	}
	if _, err := bearerToken("Basic abc"); err == nil {
		t.Fatalf("bearerToken invalid should fail")
	}
	c := newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID})
	if got, err := userIDFromCtx(c); err != nil || got.String() != userID {
		t.Fatalf("userIDFromCtx valid = %s, %v", got, err)
	}
	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/", userID: "bad"})
	requireA2AHTTPStatus(t, userIDFromCtxOnly(c), http.StatusUnauthorized)

	e := echo.New()
	api := e.Group("/api/v1")
	noop := func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	h.Register(api, noop, noop)
	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/creator/agents/:id/runtime-tokens",
		"GET /api/v1/creator/agents/:id/runtime-workbench",
		"POST /api/v1/agent-runtime/call-agent",
		"POST /api/v1/a2a/agents/:slug",
		"GET /api/v1/a2a/agents/:slug/.well-known/agent-card.json",
		"POST /api/v1/a2a/agents/:slug/message:action",
		"GET /api/v1/a2a/agents/:slug/tasks/:taskID/subscribe",
		"DELETE /api/v1/a2a/agents/:slug/tasks/:taskID/pushNotificationConfigs/:configID",
		"GET /api/v1/runs/:id/children",
	} {
		if !routes[route] {
			t.Fatalf("missing route %s; got %#v", route, routes)
		}
	}
}

func TestA2AControlHTTPHandlersDispatchService(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	tokenID := uuid.New()
	parentRunID := uuid.New()
	targetAgentID := uuid.New()

	t.Run("create runtime token", func(t *testing.T) {
		svc := newControlA2AService()
		c := newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodPost,
			target: "/creator/agents/" + agentID.String() + "/runtime-tokens",
			userID: userID.String(),
			body:   `{"name":"worker"}`,
			params: map[string]string{"id": agentID.String()},
		})

		require.NoError(t, NewHandler(svc).CreateRuntimeToken(c))
		assert.Equal(t, http.StatusCreated, c.(*a2ATestContext).rec.Code)
		assert.Equal(t, "create-runtime-token", svc.calls[len(svc.calls)-1])
		assert.Equal(t, userID, svc.userID)
		assert.Equal(t, agentID, svc.agentID)
		assert.Equal(t, "worker", svc.createReq.Name)
		assert.Contains(t, c.(*a2ATestContext).rec.Body.String(), "rt_live_test")
	})

	t.Run("list runtime tokens", func(t *testing.T) {
		svc := newControlA2AService()
		c := newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodGet,
			target: "/creator/agents/" + agentID.String() + "/runtime-tokens",
			userID: userID.String(),
			params: map[string]string{"id": agentID.String()},
		})

		require.NoError(t, NewHandler(svc).ListRuntimeTokens(c))
		assert.Equal(t, http.StatusOK, c.(*a2ATestContext).rec.Code)
		assert.Equal(t, "list-runtime-tokens", svc.calls[len(svc.calls)-1])
		assert.Contains(t, c.(*a2ATestContext).rec.Body.String(), "worker")
	})

	t.Run("revoke runtime token", func(t *testing.T) {
		svc := newControlA2AService()
		c := newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodDelete,
			target: "/creator/runtime-tokens/" + tokenID.String(),
			userID: userID.String(),
			params: map[string]string{"tokenID": tokenID.String()},
		})

		require.NoError(t, NewHandler(svc).RevokeRuntimeToken(c))
		assert.Equal(t, http.StatusNoContent, c.(*a2ATestContext).rec.Code)
		assert.Equal(t, tokenID, svc.tokenID)
	})

	t.Run("runtime workbench", func(t *testing.T) {
		svc := newControlA2AService()
		c := newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodGet,
			target: "/creator/agents/" + agentID.String() + "/runtime-workbench",
			userID: userID.String(),
			params: map[string]string{"id": agentID.String()},
		})

		require.NoError(t, NewHandler(svc).GetRuntimeWorkbench(c))
		assert.Equal(t, http.StatusOK, c.(*a2ATestContext).rec.Code)
		assert.Contains(t, c.(*a2ATestContext).rec.Body.String(), "runtime-agent")
	})

	t.Run("get and update call policy", func(t *testing.T) {
		svc := newControlA2AService()
		h := NewHandler(svc)
		c := newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodGet,
			target: "/creator/agents/" + agentID.String() + "/a2a-policy",
			userID: userID.String(),
			params: map[string]string{"id": agentID.String()},
		})

		require.NoError(t, h.GetCallPolicy(c))
		assert.Equal(t, http.StatusOK, c.(*a2ATestContext).rec.Code)
		assert.Contains(t, c.(*a2ATestContext).rec.Body.String(), "public")

		c = newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodPut,
			target: "/creator/agents/" + agentID.String() + "/a2a-policy",
			userID: userID.String(),
			body:   `{"callable_by":"same_creator"}`,
			params: map[string]string{"id": agentID.String()},
		})

		require.NoError(t, h.UpdateCallPolicy(c))
		assert.Equal(t, http.StatusOK, c.(*a2ATestContext).rec.Code)
		assert.Equal(t, "same_creator", svc.updatePolicyReq.CallableBy)
	})

	t.Run("call agent", func(t *testing.T) {
		svc := newControlA2AService()
		c := newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodPost,
			target: "/agent-runtime/call-agent",
			body:   `{"target_agent_id":"` + targetAgentID.String() + `","reason":"need data","input":{"q":"hi"},"task_callback":{"url":"https://caller.example.com/a2a/events","token":"caller-token","event_types":["run.completed","run.failed"]}}`,
			headers: map[string]string{
				echo.HeaderAuthorization: "Bearer rt_live_test",
				"X-OpenLinker-Run-Id":    parentRunID.String(),
			},
		})

		require.NoError(t, NewHandler(svc).CallAgent(c))
		assert.Equal(t, http.StatusOK, c.(*a2ATestContext).rec.Code)
		assert.Equal(t, "rt_live_test", svc.callToken)
		assert.Equal(t, parentRunID.String(), svc.callReq.CurrentRunID)
		assert.Equal(t, targetAgentID.String(), svc.callReq.TargetAgentID)
		require.NotNil(t, svc.callReq.TaskCallback)
		assert.Equal(t, "https://caller.example.com/a2a/events", svc.callReq.TaskCallback.URL)
		assert.Equal(t, []string{"run.completed", "run.failed"}, svc.callReq.TaskCallback.EventTypesAlias)
	})

	t.Run("list children and parents", func(t *testing.T) {
		svc := newControlA2AService()
		h := NewHandler(svc)
		c := newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodGet,
			target: "/runs/" + parentRunID.String() + "/children",
			userID: userID.String(),
			params: map[string]string{"id": parentRunID.String()},
		})

		require.NoError(t, h.ListChildren(c))
		assert.Equal(t, http.StatusOK, c.(*a2ATestContext).rec.Code)
		assert.Contains(t, c.(*a2ATestContext).rec.Body.String(), parentRunID.String())

		c = newA2ATestContext(&a2aHandlerRequest{
			method: http.MethodGet,
			target: "/a2a/parents?page=3&size=25&q=caller",
			userID: userID.String(),
		})

		require.NoError(t, h.ListParentRuns(c))
		assert.Equal(t, http.StatusOK, c.(*a2ATestContext).rec.Code)
		assert.Equal(t, int32(3), svc.page)
		assert.Equal(t, int32(25), svc.size)
		assert.Equal(t, "caller", svc.search)
		assert.Contains(t, c.(*a2ATestContext).rec.Body.String(), "child_count")
	})
}

func TestA2AControlHTTPHandlersPropagateServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	tokenID := uuid.New()
	parentRunID := uuid.New()
	serviceDown := httpx.ServiceUnavailable("service down")

	for _, tc := range []struct {
		name string
		key  string
		call func(*Handler, echo.Context) error
		req  *a2aHandlerRequest
	}{
		{
			name: "create runtime token",
			key:  "create-runtime-token",
			call: (*Handler).CreateRuntimeToken,
			req:  &a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID.String(), body: `{"name":"worker"}`, params: map[string]string{"id": agentID.String()}},
		},
		{
			name: "list runtime tokens",
			key:  "list-runtime-tokens",
			call: (*Handler).ListRuntimeTokens,
			req:  &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID.String(), params: map[string]string{"id": agentID.String()}},
		},
		{
			name: "revoke runtime token",
			key:  "revoke-runtime-token",
			call: (*Handler).RevokeRuntimeToken,
			req:  &a2aHandlerRequest{method: http.MethodDelete, target: "/", userID: userID.String(), params: map[string]string{"tokenID": tokenID.String()}},
		},
		{
			name: "runtime workbench",
			key:  "runtime-workbench",
			call: (*Handler).GetRuntimeWorkbench,
			req:  &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID.String(), params: map[string]string{"id": agentID.String()}},
		},
		{
			name: "get call policy",
			key:  "get-call-policy",
			call: (*Handler).GetCallPolicy,
			req:  &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID.String(), params: map[string]string{"id": agentID.String()}},
		},
		{
			name: "update call policy",
			key:  "update-call-policy",
			call: (*Handler).UpdateCallPolicy,
			req:  &a2aHandlerRequest{method: http.MethodPut, target: "/", userID: userID.String(), body: `{"callable_by":"private"}`, params: map[string]string{"id": agentID.String()}},
		},
		{
			name: "call agent",
			key:  "call-agent",
			call: (*Handler).CallAgent,
			req: &a2aHandlerRequest{method: http.MethodPost, target: "/", body: `{"target_agent_id":"` + agentID.String() + `","current_run_id":"` + parentRunID.String() + `","input":{"q":"hi"}}`, headers: map[string]string{
				echo.HeaderAuthorization: "Bearer rt_live_test",
			}},
		},
		{
			name: "list children",
			key:  "list-children",
			call: (*Handler).ListChildren,
			req:  &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID.String(), params: map[string]string{"id": parentRunID.String()}},
		},
		{
			name: "list parents",
			key:  "list-parent-runs",
			call: (*Handler).ListParentRuns,
			req:  &a2aHandlerRequest{method: http.MethodGet, target: "/", userID: userID.String()},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newControlA2AService()
			svc.errs[tc.key] = serviceDown
			requireA2AHTTPStatus(t, tc.call(NewHandler(svc), newA2ATestContext(tc.req)), http.StatusServiceUnavailable)
		})
	}
}

func TestA2AJSONRPCHandlerValidationBeforeServiceDispatch(t *testing.T) {
	h := NewHandler(nil)
	userID := uuid.NewString()

	c := newA2ATestContext(&a2aHandlerRequest{method: http.MethodPost, target: "/", body: "{"})
	if err := h.JSONRPC(c); err != nil {
		t.Fatalf("JSONRPC malformed returned error: %v", err)
	}
	if c.Response().Status != http.StatusBadRequest || !strings.Contains(c.(*a2ATestContext).rec.Body.String(), `-32700`) {
		t.Fatalf("malformed JSONRPC status/body = %d %s", c.Response().Status, c.(*a2ATestContext).rec.Body.String())
	}

	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodPost, target: "/", body: `{"jsonrpc":"2.0","id":1}`})
	if err := h.JSONRPC(c); err != nil {
		t.Fatalf("JSONRPC invalid request returned error: %v", err)
	}
	if c.Response().Status != http.StatusOK || !strings.Contains(c.(*a2ATestContext).rec.Body.String(), `-32600`) {
		t.Fatalf("invalid JSONRPC body = %d %s", c.Response().Status, c.(*a2ATestContext).rec.Body.String())
	}

	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodPost, target: "/?version=2", body: `{"jsonrpc":"2.0","id":1,"method":"message/send","params":{}}`})
	if err := h.JSONRPC(c); err != nil {
		t.Fatalf("JSONRPC unsupported version returned error: %v", err)
	}
	if !strings.Contains(c.(*a2ATestContext).rec.Body.String(), a2aErrorReason(a2aErrorVersionNotSupported)) {
		t.Fatalf("unsupported version JSONRPC body = %s", c.(*a2ATestContext).rec.Body.String())
	}

	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, authMethod: "apikey", body: `{"jsonrpc":"2.0","id":1,"method":"message/send","params":{}}`})
	if err := h.JSONRPC(c); err != nil {
		t.Fatalf("JSONRPC missing scope returned error: %v", err)
	}
	if !strings.Contains(c.(*a2ATestContext).rec.Body.String(), `-32011`) {
		t.Fatalf("missing scope JSONRPC body = %s", c.(*a2ATestContext).rec.Body.String())
	}

	c = newA2ATestContext(&a2aHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{"jsonrpc":"2.0","id":1,"method":"UnknownMethod","params":{}}`})
	if err := h.JSONRPC(c); err != nil {
		t.Fatalf("JSONRPC unknown method returned error: %v", err)
	}
	if !strings.Contains(c.(*a2ATestContext).rec.Body.String(), `-32601`) {
		t.Fatalf("unknown method JSONRPC body = %s", c.(*a2ATestContext).rec.Body.String())
	}
}

func TestA2AJSONRPCHandlerAdditionalErrorAndNullParamEdges(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	taskID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567").String()
	const slug = "agent-one"

	svcWithExtensions := newFakeA2AService(taskID)
	hWithExtensions := NewHandler(svcWithExtensions)
	cWithExtensions := newA2ATestContext(&a2aHandlerRequest{
		method:     http.MethodPost,
		target:     "/?version=1.0",
		body:       `{"jsonrpc":"2.0","id":"ext","method":"SendMessage","params":{"message":{"role":"user","parts":[{"text":"hi"}]}}}`,
		userID:     userID.String(),
		authMethod: "apikey",
		scopes:     []string{"agents:run"},
		headers:    map[string]string{a2aExtensionsHeader: "https://example.com/ext/a, https://example.com/ext/b"},
		params:     map[string]string{"slug": slug},
	})
	require.NoError(t, hWithExtensions.JSONRPC(cWithExtensions))
	if got, ok := svcWithExtensions.sendParams.Metadata["a2a_extensions"].([]string); !ok || len(got) != 2 {
		t.Fatalf("A2A extensions metadata = %#v", svcWithExtensions.sendParams.Metadata)
	}

	h := NewHandler(newFakeA2AService(taskID))
	c := newA2ATestContext(&a2aHandlerRequest{
		method: http.MethodPost,
		target: "/",
		body:   `{"jsonrpc":"2.0","id":"no-user","method":"message/send","params":{"message":{"role":"user","parts":[{"kind":"text","text":"hi"}]}}}`,
		params: map[string]string{"slug": slug},
	})
	require.NoError(t, h.JSONRPC(c))
	if body := c.(*a2ATestContext).rec.Body.String(); !strings.Contains(body, `-32010`) {
		t.Fatalf("missing user JSON-RPC body = %s", body)
	}

	svc := newFakeA2AService(taskID)
	h = NewHandler(svc)
	c = newA2ATestContext(&a2aHandlerRequest{
		method:     http.MethodPost,
		target:     "/?version=1.0",
		body:       `{"jsonrpc":"2.0","id":"list-null","method":"tasks/list","params":null}`,
		userID:     userID.String(),
		authMethod: "apikey",
		scopes:     []string{"runs:read"},
		params:     map[string]string{"slug": slug},
	})
	require.NoError(t, h.JSONRPC(c))
	if !svc.called("tasks/list") {
		t.Fatalf("tasks/list with null params did not dispatch: %v", svc.calls)
	}

	h = NewHandler(newFakeA2AService(taskID))
	c = newA2ATestContext(&a2aHandlerRequest{
		method:     http.MethodPost,
		target:     "/",
		body:       `{"jsonrpc":"2.0","id":"card","method":"agent/getExtendedCard","params":{}}`,
		userID:     userID.String(),
		authMethod: "apikey",
		scopes:     []string{"runs:read"},
		params:     map[string]string{"slug": slug},
	})
	require.NoError(t, h.JSONRPC(c))
	if body := c.(*a2ATestContext).rec.Body.String(); !strings.Contains(body, `-32007`) {
		t.Fatalf("missing card provider JSON-RPC body = %s", body)
	}

	for _, tc := range []struct {
		method string
		scope  string
	}{
		{method: "message/stream", scope: "agents:run"},
		{method: "tasks/get", scope: "runs:read"},
		{method: "tasks/list", scope: "runs:read"},
		{method: "tasks/cancel", scope: "agents:run"},
		{method: "tasks/resubscribe", scope: "runs:read"},
		{method: "tasks/pushNotificationConfig/set", scope: "runs:read"},
		{method: "tasks/pushNotificationConfig/get", scope: "runs:read"},
		{method: "tasks/pushNotificationConfig/list", scope: "runs:read"},
		{method: "tasks/pushNotificationConfig/delete", scope: "runs:read"},
	} {
		t.Run(tc.method, func(t *testing.T) {
			c := newA2ATestContext(&a2aHandlerRequest{
				method:     http.MethodPost,
				target:     "/",
				body:       `{"jsonrpc":"2.0","id":"bad-params","method":"` + tc.method + `","params":[]}`,
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{tc.scope},
				params:     map[string]string{"slug": slug},
			})
			require.NoError(t, NewHandler(newFakeA2AService(taskID)).JSONRPC(c))
			if body := c.(*a2ATestContext).rec.Body.String(); !strings.Contains(body, `-32602`) {
				t.Fatalf("%s invalid params body = %s", tc.method, body)
			}
		})
	}
}

func TestA2AJSONRPCHandlerDispatchesStandardMethods(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	taskID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567").String()
	const slug = "agent-one"

	tests := []struct {
		name      string
		body      string
		scopes    []string
		streaming bool
		assert    func(t *testing.T, svc *fakeA2AService, cards *fakeA2ACardProvider)
	}{
		{
			name:   "message send",
			body:   `{"jsonrpc":"2.0","id":"send","method":"SendMessage","params":{"message":{"messageId":"msg-send","contextId":"ctx-jsonrpc","role":"user","parts":[{"text":"hello"}]},"configuration":{"returnImmediately":false,"acceptedOutputModes":["text/plain"]}}}`,
			scopes: []string{"agents:run"},
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("message/send") || svc.slug != slug || svc.userID != userID {
					t.Fatalf("message/send dispatch = calls=%v slug=%q user=%s", svc.calls, svc.slug, svc.userID)
				}
				if svc.sendParams.Metadata["a2a_protocol_version"] != a2aProtocolVersionCurrent {
					t.Fatalf("message/send metadata = %#v", svc.sendParams.Metadata)
				}
				if svc.sendParams.Configuration == nil || svc.sendParams.Configuration.ReturnImmediately == nil || *svc.sendParams.Configuration.ReturnImmediately {
					t.Fatalf("message/send configuration = %#v", svc.sendParams.Configuration)
				}
			},
		},
		{
			name:      "message stream",
			body:      `{"jsonrpc":"2.0","id":"stream","method":"SendStreamingMessage","params":{"message":{"messageId":"msg-stream","role":"user","parts":[{"text":"stream"}]}}}`,
			scopes:    []string{"agents:run"},
			streaming: true,
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("message/stream") || !svc.called("events") {
					t.Fatalf("message/stream dispatch calls = %v", svc.calls)
				}
			},
		},
		{
			name:   "tasks get",
			body:   `{"jsonrpc":"2.0","id":"get","method":"GetTask","params":{"id":"` + taskID + `","historyLength":2}}`,
			scopes: []string{"runs:read"},
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("tasks/get") || svc.taskID != taskID || svc.historyLength == nil || *svc.historyLength != 2 {
					t.Fatalf("tasks/get dispatch = calls=%v task=%q history=%v", svc.calls, svc.taskID, svc.historyLength)
				}
			},
		},
		{
			name:   "tasks list",
			body:   `{"jsonrpc":"2.0","id":"list","method":"ListTasks","params":{"status":"completed","pageSize":7,"contextId":"ctx-jsonrpc","includeArtifacts":true}}`,
			scopes: []string{"runs:read"},
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("tasks/list") || svc.listParams.Status != "completed" || svc.listParams.PageSize == nil || *svc.listParams.PageSize != 7 {
					t.Fatalf("tasks/list dispatch = calls=%v params=%#v", svc.calls, svc.listParams)
				}
			},
		},
		{
			name:   "tasks cancel",
			body:   `{"jsonrpc":"2.0","id":"cancel","method":"CancelTask","params":{"id":"` + taskID + `"}}`,
			scopes: []string{"agents:run"},
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("tasks/cancel") || svc.taskID != taskID {
					t.Fatalf("tasks/cancel dispatch = calls=%v task=%q", svc.calls, svc.taskID)
				}
			},
		},
		{
			name:      "tasks resubscribe",
			body:      `{"jsonrpc":"2.0","id":"resub","method":"SubscribeToTask","params":{"id":"` + taskID + `"}}`,
			scopes:    []string{"runs:read"},
			streaming: true,
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("tasks/get") || !svc.called("events") {
					t.Fatalf("tasks/resubscribe dispatch calls = %v", svc.calls)
				}
			},
		},
		{
			name:   "push set",
			body:   `{"jsonrpc":"2.0","id":"push-set","method":"CreateTaskPushNotificationConfig","params":{"id":"` + taskID + `","pushNotificationConfig":{"url":"https://hooks.example/a2a","token":"secret"}}}`,
			scopes: []string{"runs:read"},
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("push/set") || svc.pushParams.ID != taskID || svc.pushParams.PushNotificationConfig.URL != "https://hooks.example/a2a" {
					t.Fatalf("push set dispatch = calls=%v params=%#v", svc.calls, svc.pushParams)
				}
			},
		},
		{
			name:   "push get",
			body:   `{"jsonrpc":"2.0","id":"push-get","method":"GetTaskPushNotificationConfig","params":{"id":"` + taskID + `","pushNotificationConfigId":"cfg-1"}}`,
			scopes: []string{"runs:read"},
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("push/get") || svc.pushParams.PushNotificationConfigID != "cfg-1" {
					t.Fatalf("push get dispatch = calls=%v params=%#v", svc.calls, svc.pushParams)
				}
			},
		},
		{
			name:   "push list",
			body:   `{"jsonrpc":"2.0","id":"push-list","method":"ListTaskPushNotificationConfigs","params":{"id":"` + taskID + `"}}`,
			scopes: []string{"runs:read"},
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("push/list") || svc.pushParams.ID != taskID {
					t.Fatalf("push list dispatch = calls=%v params=%#v", svc.calls, svc.pushParams)
				}
			},
		},
		{
			name:   "push delete",
			body:   `{"jsonrpc":"2.0","id":"push-delete","method":"DeleteTaskPushNotificationConfig","params":{"id":"` + taskID + `","pushNotificationConfigId":"cfg-1"}}`,
			scopes: []string{"runs:read"},
			assert: func(t *testing.T, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("push/delete") || svc.pushParams.PushNotificationConfigID != "cfg-1" {
					t.Fatalf("push delete dispatch = calls=%v params=%#v", svc.calls, svc.pushParams)
				}
			},
		},
		{
			name:   "extended card",
			body:   `{"jsonrpc":"2.0","id":"card","method":"GetExtendedAgentCard","params":{}}`,
			scopes: []string{"runs:read"},
			assert: func(t *testing.T, _ *fakeA2AService, cards *fakeA2ACardProvider) {
				if cards.extendedSlug != slug {
					t.Fatalf("extended card slug = %q", cards.extendedSlug)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newFakeA2AService(taskID)
			cards := &fakeA2ACardProvider{}
			h := NewHandler(svc)
			h.SetAgentCardProvider(cards)
			c := newA2ATestContext(&a2aHandlerRequest{
				method:     http.MethodPost,
				target:     "/?version=1.0",
				body:       tt.body,
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     tt.scopes,
				params:     map[string]string{"slug": slug},
			})

			if err := h.JSONRPC(c); err != nil {
				t.Fatalf("JSONRPC returned error: %v", err)
			}
			rec := c.(*a2ATestContext).rec
			if rec.Code != http.StatusOK {
				t.Fatalf("JSONRPC status = %d, body = %s", rec.Code, rec.Body.String())
			}
			if got := c.Response().Header().Get(a2aVersionHeader); got != a2aProtocolVersionCurrent {
				t.Fatalf("A2A version header = %q", got)
			}
			if tt.streaming {
				body := rec.Body.String()
				if !strings.Contains(body, "event: task") || !strings.Contains(body, "event: status-update") {
					t.Fatalf("stream body missing expected events: %s", body)
				}
			} else {
				var resp JSONRPCResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("JSONRPC response decode failed: %v", err)
				}
				if resp.Error != nil {
					t.Fatalf("JSONRPC error response = %#v", resp.Error)
				}
			}
			tt.assert(t, svc, cards)
		})
	}
}

func TestA2AJSONRPCCurrentProtocolShapes(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	taskID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567").String()
	const slug = "agent-one"

	call := func(t *testing.T, h *Handler, body string) map[string]interface{} {
		t.Helper()
		c := newA2ATestContext(&a2aHandlerRequest{
			method:     http.MethodPost,
			target:     "/?version=1.0",
			body:       body,
			userID:     userID.String(),
			authMethod: "apikey",
			scopes:     []string{"agents:run", "runs:read"},
			params:     map[string]string{"slug": slug},
		})
		require.NoError(t, h.JSONRPC(c))
		rec := c.(*a2ATestContext).rec
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var resp map[string]interface{}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.NotContains(t, resp, "error")
		result, ok := resp["result"].(map[string]interface{})
		require.True(t, ok, "result body = %s", rec.Body.String())
		return result
	}

	svc := newFakeA2AService(taskID)
	h := NewHandler(svc)
	sendResult := call(t, h, `{"jsonrpc":"2.0","id":"send","method":"SendMessage","params":{"message":{"messageId":"msg-send","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`)
	task := sendResult["task"].(map[string]interface{})
	assert.Equal(t, taskID, task["id"])
	assert.NotContains(t, task, "kind")
	assert.Equal(t, "TASK_STATE_COMPLETED", task["status"].(map[string]interface{})["state"])
	assert.NotContains(t, sendResult, "id")

	svc = newFakeA2AService(taskID)
	h = NewHandler(svc)
	pushSet := call(t, h, `{"jsonrpc":"2.0","id":"push-set","method":"CreateTaskPushNotificationConfig","params":{"taskId":"`+taskID+`","url":"https://hooks.example/a2a","token":"secret"}}`)
	assert.Equal(t, taskID, svc.pushParams.TaskID)
	assert.Equal(t, "https://hooks.example/a2a", svc.pushParams.URL)
	assert.Equal(t, "https://hooks.example/a2a", pushSet["url"])
	assert.NotContains(t, pushSet, "pushNotificationConfig")

	svc = newFakeA2AService(taskID)
	h = NewHandler(svc)
	pushList := call(t, h, `{"jsonrpc":"2.0","id":"push-list","method":"ListTaskPushNotificationConfigs","params":{"taskId":"`+taskID+`"}}`)
	configs := pushList["configs"].([]interface{})
	assert.Equal(t, taskID, configs[0].(map[string]interface{})["taskId"])
	assert.NotContains(t, pushList, "items")
}

func TestA2AHTTPJSONHandlersDispatchStandardEndpoints(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	taskID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567").String()
	configID := uuid.MustParse("2f151345-b29a-463b-90fc-7e20e27fbf20").String()
	const slug = "agent-one"

	tests := []struct {
		name   string
		method func(*Handler, echo.Context) error
		req    *a2aHandlerRequest
		want   int
		assert func(t *testing.T, c echo.Context, svc *fakeA2AService, cards *fakeA2ACardProvider)
	}{
		{
			name: "public agent card",
			method: func(h *Handler, c echo.Context) error {
				return h.GetPublicAgentCardHTTP(c)
			},
			req:  &a2aHandlerRequest{method: http.MethodGet, target: "/", params: map[string]string{"slug": slug}},
			want: http.StatusOK,
			assert: func(t *testing.T, c echo.Context, _ *fakeA2AService, cards *fakeA2ACardProvider) {
				if cards.publicSlug != slug {
					t.Fatalf("public card slug = %q", cards.publicSlug)
				}
				if got := c.Response().Header().Get(echo.HeaderCacheControl); got != "public, max-age=300" {
					t.Fatalf("public card cache header = %q", got)
				}
			},
		},
		{
			name: "extended agent card",
			method: func(h *Handler, c echo.Context) error {
				return h.GetExtendedAgentCardHTTP(c)
			},
			req:  &a2aHandlerRequest{method: http.MethodGet, target: "/?version=v1", userID: userID.String(), authMethod: "apikey", scopes: []string{"runs:read"}, params: map[string]string{"slug": slug}},
			want: http.StatusOK,
			assert: func(t *testing.T, c echo.Context, _ *fakeA2AService, cards *fakeA2ACardProvider) {
				if cards.extendedSlug != slug {
					t.Fatalf("extended card slug = %q", cards.extendedSlug)
				}
				if got := c.Response().Header().Get(a2aVersionHeader); got != a2aProtocolVersionCurrent {
					t.Fatalf("extended card version header = %q", got)
				}
			},
		},
		{
			name: "message send",
			method: func(h *Handler, c echo.Context) error {
				return h.MessageHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodPost,
				target:     "/message:send?version=1.0",
				body:       `{"message":{"messageId":"msg-http","contextId":"ctx-http","role":"user","parts":[{"kind":"text","text":"hello http"}]}}`,
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"agents:run"},
				params:     map[string]string{"slug": slug, "action": ":send"},
			},
			want: http.StatusOK,
			assert: func(t *testing.T, c echo.Context, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("message/send") || svc.slug != slug || svc.userID != userID {
					t.Fatalf("message send dispatch = calls=%v slug=%q user=%s", svc.calls, svc.slug, svc.userID)
				}
				if svc.sendParams.Metadata["a2a_protocol_version"] != a2aProtocolVersionCurrent {
					t.Fatalf("message send metadata = %#v", svc.sendParams.Metadata)
				}
				if got := c.Response().Header().Get(a2aVersionHeader); got != a2aProtocolVersionCurrent {
					t.Fatalf("message send version header = %q", got)
				}
				if got := c.Response().Header().Get(echo.HeaderContentType); !strings.Contains(got, a2aJSONContentType) {
					t.Fatalf("message send content-type = %q", got)
				}
			},
		},
		{
			name: "tasks list",
			method: func(h *Handler, c echo.Context) error {
				return h.ListTasksHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodGet,
				target:     "/tasks?page_size=5&history_length=2&include_artifacts=yes&context_id=ctx-http&status=completed&page_token=next&version=1.0",
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"runs:read"},
				params:     map[string]string{"slug": slug},
			},
			want: http.StatusOK,
			assert: func(t *testing.T, _ echo.Context, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("tasks/list") || svc.listParams.ContextID != "ctx-http" || svc.listParams.Status != "completed" || svc.listParams.PageToken != "next" {
					t.Fatalf("tasks list dispatch = calls=%v params=%#v", svc.calls, svc.listParams)
				}
				if svc.listParams.PageSize == nil || *svc.listParams.PageSize != 5 || svc.listParams.HistoryLength == nil || *svc.listParams.HistoryLength != 2 || svc.listParams.IncludeArtifacts == nil || !*svc.listParams.IncludeArtifacts {
					t.Fatalf("tasks list optional params = %#v", svc.listParams)
				}
			},
		},
		{
			name: "tasks get",
			method: func(h *Handler, c echo.Context) error {
				return h.GetTaskHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodGet,
				target:     "/tasks/" + taskID + "?historyLength=3&version=1.0",
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"runs:read"},
				params:     map[string]string{"slug": slug, "taskID": taskID},
			},
			want: http.StatusOK,
			assert: func(t *testing.T, _ echo.Context, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("tasks/get") || svc.taskID != taskID || svc.historyLength == nil || *svc.historyLength != 3 {
					t.Fatalf("tasks get dispatch = calls=%v task=%q history=%v", svc.calls, svc.taskID, svc.historyLength)
				}
			},
		},
		{
			name: "tasks cancel via action route",
			method: func(h *Handler, c echo.Context) error {
				return h.TaskActionHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodPost,
				target:     "/tasks/" + taskID + ":cancel?version=1.0",
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"agents:run"},
				params:     map[string]string{"slug": slug, "*": "tasks/" + taskID + ":cancel"},
			},
			want: http.StatusOK,
			assert: func(t *testing.T, _ echo.Context, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("tasks/cancel") || svc.taskID != taskID {
					t.Fatalf("tasks cancel dispatch = calls=%v task=%q", svc.calls, svc.taskID)
				}
			},
		},
		{
			name: "push set",
			method: func(h *Handler, c echo.Context) error {
				return h.SetTaskPushNotificationHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodPost,
				target:     "/tasks/" + taskID + "/pushNotificationConfig?version=1.0",
				body:       `{"pushNotificationConfig":{"url":"https://hooks.example/a2a","token":"secret","eventTypes":["run.completed"]}}`,
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"runs:read"},
				params:     map[string]string{"slug": slug, "taskID": taskID},
			},
			want: http.StatusCreated,
			assert: func(t *testing.T, _ echo.Context, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("push/set") || taskIDFromPushParams(&svc.pushParams) != taskID || pushConfigFromPushParams(&svc.pushParams).URL != "https://hooks.example/a2a" {
					t.Fatalf("push set dispatch = calls=%v params=%#v", svc.calls, svc.pushParams)
				}
			},
		},
		{
			name: "push list",
			method: func(h *Handler, c echo.Context) error {
				return h.ListTaskPushNotificationsHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodGet,
				target:     "/tasks/" + taskID + "/pushNotificationConfig?version=1.0",
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"runs:read"},
				params:     map[string]string{"slug": slug, "taskID": taskID},
			},
			want: http.StatusOK,
			assert: func(t *testing.T, _ echo.Context, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("push/list") || svc.pushParams.ID != taskID {
					t.Fatalf("push list dispatch = calls=%v params=%#v", svc.calls, svc.pushParams)
				}
			},
		},
		{
			name: "push get",
			method: func(h *Handler, c echo.Context) error {
				return h.GetTaskPushNotificationHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodGet,
				target:     "/tasks/" + taskID + "/pushNotificationConfig/" + configID + "?version=1.0",
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"runs:read"},
				params:     map[string]string{"slug": slug, "taskID": taskID, "configID": configID},
			},
			want: http.StatusOK,
			assert: func(t *testing.T, _ echo.Context, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("push/get") || svc.pushParams.ID != taskID || svc.pushParams.PushNotificationConfigID != configID {
					t.Fatalf("push get dispatch = calls=%v params=%#v", svc.calls, svc.pushParams)
				}
			},
		},
		{
			name: "push delete",
			method: func(h *Handler, c echo.Context) error {
				return h.DeleteTaskPushNotificationHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodDelete,
				target:     "/tasks/" + taskID + "/pushNotificationConfig/" + configID + "?version=1.0",
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"runs:read"},
				params:     map[string]string{"slug": slug, "taskID": taskID, "configID": configID},
			},
			want: http.StatusNoContent,
			assert: func(t *testing.T, _ echo.Context, svc *fakeA2AService, _ *fakeA2ACardProvider) {
				if !svc.called("push/delete") || svc.pushParams.ID != taskID || svc.pushParams.PushNotificationConfigID != configID {
					t.Fatalf("push delete dispatch = calls=%v params=%#v", svc.calls, svc.pushParams)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newFakeA2AService(taskID)
			cards := &fakeA2ACardProvider{}
			h := NewHandler(svc)
			h.SetAgentCardProvider(cards)
			c := newA2ATestContext(tt.req)

			if err := tt.method(h, c); err != nil {
				t.Fatalf("%s returned error: %v", tt.name, err)
			}
			rec := c.(*a2ATestContext).rec
			if rec.Code != tt.want {
				t.Fatalf("%s status = %d, want %d, body = %s", tt.name, rec.Code, tt.want, rec.Body.String())
			}
			tt.assert(t, c, svc, cards)
		})
	}
}

func TestA2AHTTPJSONStreamingAliasesDispatch(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	taskID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567").String()
	const slug = "agent-one"

	for _, tc := range []struct {
		name   string
		method func(*Handler, echo.Context) error
		req    *a2aHandlerRequest
		calls  []string
	}{
		{
			name: "message stream path fallback",
			method: func(h *Handler, c echo.Context) error {
				return h.MessageHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodPost,
				target:     "/message:stream?version=1.0",
				body:       `{"message":{"messageId":"msg-http-stream","role":"user","parts":[{"kind":"text","text":"stream"}]}}`,
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"agents:run"},
				params:     map[string]string{"slug": slug},
			},
			calls: []string{"message/stream", "events"},
		},
		{
			name: "task get subscribe suffix",
			method: func(h *Handler, c echo.Context) error {
				return h.GetTaskHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodGet,
				target:     "/tasks/" + taskID + ":subscribe?version=1.0",
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"runs:read"},
				params:     map[string]string{"slug": slug, "taskID": taskID + ":subscribe"},
			},
			calls: []string{"tasks/get", "events"},
		},
		{
			name: "task action subscribe slash suffix",
			method: func(h *Handler, c echo.Context) error {
				return h.TaskActionHTTP(c)
			},
			req: &a2aHandlerRequest{
				method:     http.MethodGet,
				target:     "/tasks/" + taskID + "/subscribe?version=1.0",
				userID:     userID.String(),
				authMethod: "apikey",
				scopes:     []string{"runs:read"},
				params:     map[string]string{"slug": slug, "*": "tasks/" + taskID + "/subscribe"},
			},
			calls: []string{"tasks/get", "events"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newFakeA2AService(taskID)
			h := NewHandler(svc)
			c := newA2ATestContext(tc.req)

			if err := tc.method(h, c); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}
			rec := c.(*a2ATestContext).rec
			if rec.Code != http.StatusOK {
				t.Fatalf("%s status = %d body=%s", tc.name, rec.Code, rec.Body.String())
			}
			body := rec.Body.String()
			if !strings.Contains(body, "event: task") || !strings.Contains(body, "event: status-update") {
				t.Fatalf("%s stream body missing expected events: %s", tc.name, body)
			}
			for _, call := range tc.calls {
				if !svc.called(call) {
					t.Fatalf("%s missing call %s in %v", tc.name, call, svc.calls)
				}
			}
			if got := c.Response().Header().Get(a2aVersionHeader); got != a2aProtocolVersionCurrent {
				t.Fatalf("%s A2A version header = %q", tc.name, got)
			}
		})
	}
}

func TestA2AHTTPJSONStreamingWritesSSEErrorsAndResumesAfterLastEventID(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	taskID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567").String()
	const slug = "agent-one"

	svc := newFakeA2AService(taskID)
	svc.errs["events"] = httpx.ServiceUnavailable("events down")
	c := newA2ATestContext(&a2aHandlerRequest{
		method:     http.MethodGet,
		target:     "/tasks/" + taskID + "/subscribe?version=1.0",
		userID:     userID.String(),
		authMethod: "apikey",
		scopes:     []string{"runs:read"},
		params:     map[string]string{"slug": slug, "taskID": taskID},
		headers:    map[string]string{"Last-Event-ID": "41"},
	})

	require.NoError(t, NewHandler(svc).SubscribeTaskHTTP(c))
	assert.Equal(t, int32(41), svc.afterSequence)
	rec := c.(*a2ATestContext).rec
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "event: task")
	assert.Contains(t, body, "event: task.stream.error")
	assert.Contains(t, body, "events down")
}

func TestA2AHTTPJSONHandlersPropagateServiceErrors(t *testing.T) {
	userID := uuid.MustParse("8582c7a4-0f02-4895-8570-7c7cce357e5f")
	taskID := uuid.MustParse("c93dbab2-404f-4460-bcb7-0f17ece85567").String()
	configID := uuid.MustParse("2f151345-b29a-463b-90fc-7e20e27fbf20").String()
	const slug = "agent-one"
	serviceDown := httpx.ServiceUnavailable("service down")

	for _, tc := range []struct {
		name   string
		key    string
		method func(*Handler, echo.Context) error
		req    *a2aHandlerRequest
	}{
		{
			name: "send message",
			key:  "message/send",
			method: func(h *Handler, c echo.Context) error {
				return h.SendMessageHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodPost, target: "/message:send", body: `{"message":{"messageId":"msg","role":"user","parts":[{"kind":"text","text":"hi"}]}}`, userID: userID.String(), authMethod: "apikey", scopes: []string{"agents:run"}, params: map[string]string{"slug": slug}},
		},
		{
			name: "stream message before SSE",
			key:  "message/stream",
			method: func(h *Handler, c echo.Context) error {
				return h.StreamMessageHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodPost, target: "/message:stream", body: `{"message":{"messageId":"msg","role":"user","parts":[{"kind":"text","text":"hi"}]}}`, userID: userID.String(), authMethod: "apikey", scopes: []string{"agents:run"}, params: map[string]string{"slug": slug}},
		},
		{
			name: "list tasks",
			key:  "tasks/list",
			method: func(h *Handler, c echo.Context) error {
				return h.ListTasksHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodGet, target: "/tasks", userID: userID.String(), authMethod: "apikey", scopes: []string{"runs:read"}, params: map[string]string{"slug": slug}},
		},
		{
			name: "get task",
			key:  "tasks/get",
			method: func(h *Handler, c echo.Context) error {
				return h.GetTaskHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodGet, target: "/tasks/" + taskID, userID: userID.String(), authMethod: "apikey", scopes: []string{"runs:read"}, params: map[string]string{"slug": slug, "taskID": taskID}},
		},
		{
			name: "cancel task",
			key:  "tasks/cancel",
			method: func(h *Handler, c echo.Context) error {
				return h.CancelTaskHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodPost, target: "/tasks/" + taskID + ":cancel", userID: userID.String(), authMethod: "apikey", scopes: []string{"agents:run"}, params: map[string]string{"slug": slug, "taskID": taskID}},
		},
		{
			name: "set push",
			key:  "push/set",
			method: func(h *Handler, c echo.Context) error {
				return h.SetTaskPushNotificationHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodPost, target: "/tasks/" + taskID + "/pushNotificationConfig", body: `{"pushNotificationConfig":{"url":"https://hooks.example/a2a"}}`, userID: userID.String(), authMethod: "apikey", scopes: []string{"runs:read"}, params: map[string]string{"slug": slug, "taskID": taskID}},
		},
		{
			name: "list push",
			key:  "push/list",
			method: func(h *Handler, c echo.Context) error {
				return h.ListTaskPushNotificationsHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodGet, target: "/tasks/" + taskID + "/pushNotificationConfig", userID: userID.String(), authMethod: "apikey", scopes: []string{"runs:read"}, params: map[string]string{"slug": slug, "taskID": taskID}},
		},
		{
			name: "get push",
			key:  "push/get",
			method: func(h *Handler, c echo.Context) error {
				return h.GetTaskPushNotificationHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodGet, target: "/tasks/" + taskID + "/pushNotificationConfig/" + configID, userID: userID.String(), authMethod: "apikey", scopes: []string{"runs:read"}, params: map[string]string{"slug": slug, "taskID": taskID, "configID": configID}},
		},
		{
			name: "delete push",
			key:  "push/delete",
			method: func(h *Handler, c echo.Context) error {
				return h.DeleteTaskPushNotificationHTTP(c)
			},
			req: &a2aHandlerRequest{method: http.MethodDelete, target: "/tasks/" + taskID + "/pushNotificationConfig/" + configID, userID: userID.String(), authMethod: "apikey", scopes: []string{"runs:read"}, params: map[string]string{"slug": slug, "taskID": taskID, "configID": configID}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newFakeA2AService(taskID)
			svc.errs[tc.key] = serviceDown
			requireA2AHTTPStatus(t, tc.method(NewHandler(svc), newA2ATestContext(tc.req)), http.StatusServiceUnavailable)
		})
	}
}

func TestA2AAgentCardHandlersUnavailableWithoutProvider(t *testing.T) {
	h := NewHandler(newFakeA2AService(uuid.NewString()))
	requireA2AHTTPStatus(t, h.GetPublicAgentCardHTTP(newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/", params: map[string]string{"slug": "agent-one"}})), http.StatusServiceUnavailable)
	requireA2AHTTPStatus(t, h.GetExtendedAgentCardHTTP(newA2ATestContext(&a2aHandlerRequest{method: http.MethodGet, target: "/", userID: uuid.NewString(), authMethod: "apikey", scopes: []string{"runs:read"}, params: map[string]string{"slug": "agent-one"}})), http.StatusBadRequest)
}

type noopTaskCallbackManager struct{}

func (noopTaskCallbackManager) CreateTaskCallbackSubscription(context.Context, uuid.UUID, uuid.UUID, *webhook.CreateTaskCallbackRequest) (*webhook.TaskCallbackSubscriptionResponse, error) {
	return nil, errors.New("unexpected CreateTaskCallbackSubscription call")
}

func (noopTaskCallbackManager) DeleteTaskCallbackSubscription(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error {
	return errors.New("unexpected DeleteTaskCallbackSubscription call")
}

type a2aHandlerRequest struct {
	method     string
	target     string
	body       string
	userID     string
	authMethod string
	scopes     []string
	params     map[string]string
	headers    map[string]string
}

type a2ATestContext struct {
	echo.Context
	rec *httptest.ResponseRecorder
}

func newA2ATestContext(spec *a2aHandlerRequest) echo.Context {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	base := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		base.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	if spec.authMethod != "" {
		base.Set(string(httpx.CtxKeyAuthMethod), spec.authMethod)
	}
	if spec.scopes != nil {
		base.Set(string(httpx.CtxKeyAuthScopes), spec.scopes)
	}
	if len(spec.params) > 0 {
		names := make([]string, 0, len(spec.params))
		values := make([]string, 0, len(spec.params))
		for name, value := range spec.params {
			names = append(names, name)
			values = append(values, value)
		}
		base.SetParamNames(names...)
		base.SetParamValues(values...)
	}
	return &a2ATestContext{Context: base, rec: rec}
}

func requireA2AHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if he.Status != want {
		t.Fatalf("HTTP status = %d (%s), want %d", he.Status, he.Message, want)
	}
}

func userIDFromCtxOnly(c echo.Context) error {
	_, err := userIDFromCtx(c)
	return err
}

type a2aErrorDBTX struct {
	rowErr   error
	queryErr error
}

func (f *a2aErrorDBTX) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("unexpected exec")
}

func (f *a2aErrorDBTX) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	return nil, f.queryErr
}

func (f *a2aErrorDBTX) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	return a2aErrorRow{err: f.rowErr}
}

type a2aErrorRow struct {
	err error
}

func (r a2aErrorRow) Scan(...any) error {
	return r.err
}

type fakeA2AService struct {
	*Service
	calls         []string
	errs          map[string]error
	userID        uuid.UUID
	slug          string
	taskID        string
	historyLength *int
	sendParams    A2AMessageSendParams
	streamParams  A2AMessageSendParams
	listParams    A2ATaskListParams
	pushParams    A2ATaskPushConfigParams
	afterSequence int32
}

func newFakeA2AService(taskID string) *fakeA2AService {
	return &fakeA2AService{taskID: taskID, errs: map[string]error{}}
}

func (f *fakeA2AService) called(name string) bool {
	for _, call := range f.calls {
		if call == name {
			return true
		}
	}
	return false
}

func (f *fakeA2AService) record(name string, userID uuid.UUID, slug string) {
	f.calls = append(f.calls, name)
	f.userID = userID
	f.slug = slug
}

func (f *fakeA2AService) maybeProtocolErr(name string) error {
	if f.errs == nil {
		return nil
	}
	return f.errs[name]
}

func (f *fakeA2AService) SendProtocolMessage(_ context.Context, userID uuid.UUID, slug string, params *A2AMessageSendParams) (*A2ATask, error) {
	f.record("message/send", userID, slug)
	f.sendParams = *params
	if err := f.maybeProtocolErr("message/send"); err != nil {
		return nil, err
	}
	return fakeA2ATask(f.taskID, a2aTaskStateCompleted), nil
}

func (f *fakeA2AService) StartProtocolMessage(_ context.Context, userID uuid.UUID, slug string, params *A2AMessageSendParams) (*A2ATask, error) {
	f.record("message/stream", userID, slug)
	f.streamParams = *params
	if err := f.maybeProtocolErr("message/stream"); err != nil {
		return nil, err
	}
	return fakeA2ATask(f.taskID, a2aTaskStateWorking), nil
}

func (f *fakeA2AService) GetProtocolTask(_ context.Context, userID uuid.UUID, slug, taskID string, historyLength *int) (*A2ATask, error) {
	f.record("tasks/get", userID, slug)
	f.taskID = taskID
	f.historyLength = historyLength
	if err := f.maybeProtocolErr("tasks/get"); err != nil {
		return nil, err
	}
	return fakeA2ATask(taskID, a2aTaskStateWorking), nil
}

func (f *fakeA2AService) ListProtocolTasks(_ context.Context, userID uuid.UUID, slug string, params *A2ATaskListParams) (*A2ATaskListResponse, error) {
	f.record("tasks/list", userID, slug)
	f.listParams = *params
	if err := f.maybeProtocolErr("tasks/list"); err != nil {
		return nil, err
	}
	return &A2ATaskListResponse{Tasks: []A2ATask{*fakeA2ATask(f.taskID, a2aTaskStateCompleted)}, PageSize: 1, TotalSize: 1}, nil
}

func (f *fakeA2AService) CancelProtocolTask(_ context.Context, userID uuid.UUID, slug, taskID string) (*A2ATask, error) {
	f.record("tasks/cancel", userID, slug)
	f.taskID = taskID
	if err := f.maybeProtocolErr("tasks/cancel"); err != nil {
		return nil, err
	}
	return fakeA2ATask(taskID, a2aTaskStateCanceled), nil
}

func (f *fakeA2AService) ListProtocolTaskEvents(_ context.Context, userID uuid.UUID, slug, taskID string, afterSequence int32) ([]interface{}, bool, int32, error) {
	f.record("events", userID, slug)
	f.taskID = taskID
	f.afterSequence = afterSequence
	if err := f.maybeProtocolErr("events"); err != nil {
		return nil, false, afterSequence, err
	}
	return []interface{}{&A2ATaskStatusUpdateEvent{
		Kind:      "status-update",
		TaskID:    taskID,
		ContextID: "ctx-jsonrpc",
		Status:    A2ATaskStatus{State: a2aTaskStateCompleted, Timestamp: "2026-06-21T00:00:00Z"},
		Final:     true,
		Metadata:  map[string]interface{}{"openlinker_sequence": afterSequence + 1},
	}}, true, afterSequence + 1, nil
}

func (f *fakeA2AService) SetPushNotificationConfig(_ context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	f.record("push/set", userID, slug)
	f.pushParams = *params
	if err := f.maybeProtocolErr("push/set"); err != nil {
		return nil, err
	}
	return fakeA2APushConfig(params), nil
}

func (f *fakeA2AService) GetPushNotificationConfig(_ context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	f.record("push/get", userID, slug)
	f.pushParams = *params
	if err := f.maybeProtocolErr("push/get"); err != nil {
		return nil, err
	}
	return fakeA2APushConfig(params), nil
}

func (f *fakeA2AService) ListPushNotificationConfigs(_ context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) (*A2ATaskPushConfigList, error) {
	f.record("push/list", userID, slug)
	f.pushParams = *params
	if err := f.maybeProtocolErr("push/list"); err != nil {
		return nil, err
	}
	return &A2ATaskPushConfigList{Items: []A2ATaskPushNotificationConfig{*fakeA2APushConfig(params)}}, nil
}

func (f *fakeA2AService) DeletePushNotificationConfig(_ context.Context, userID uuid.UUID, slug string, params *A2ATaskPushConfigParams) error {
	f.record("push/delete", userID, slug)
	f.pushParams = *params
	return f.maybeProtocolErr("push/delete")
}

type controlA2AService struct {
	*fakeA2AService
	errs map[string]error

	agentID         uuid.UUID
	tokenID         uuid.UUID
	parentRunID     uuid.UUID
	page            int32
	size            int32
	search          string
	createReq       CreateRuntimeTokenRequest
	updatePolicyReq UpdateCallPolicyRequest
	callToken       string
	callReq         CallAgentRequest
}

func newControlA2AService() *controlA2AService {
	return &controlA2AService{
		fakeA2AService: newFakeA2AService(uuid.NewString()),
		errs:           map[string]error{},
	}
}

func (f *controlA2AService) maybeErr(name string) error {
	if err := f.errs[name]; err != nil {
		return err
	}
	return nil
}

func (f *controlA2AService) recordControl(name string, userID uuid.UUID) error {
	f.calls = append(f.calls, name)
	f.userID = userID
	return f.maybeErr(name)
}

func (f *controlA2AService) CreateRuntimeToken(_ context.Context, userID, agentID uuid.UUID, req *CreateRuntimeTokenRequest) (*RuntimeTokenResponse, error) {
	f.agentID = agentID
	if req != nil {
		f.createReq = *req
	}
	if err := f.recordControl("create-runtime-token", userID); err != nil {
		return nil, err
	}
	return &RuntimeTokenResponse{
		ID:             uuid.NewString(),
		AgentID:        agentID.String(),
		Name:           f.createReq.Name,
		Prefix:         "rt_live_abcd",
		PlaintextToken: "rt_live_test",
		Scopes:         []string{"agents:run"},
		CreatedAt:      "2026-06-21T00:00:00Z",
	}, nil
}

func (f *controlA2AService) ListRuntimeTokens(_ context.Context, userID, agentID uuid.UUID) ([]RuntimeTokenResponse, error) {
	f.agentID = agentID
	if err := f.recordControl("list-runtime-tokens", userID); err != nil {
		return nil, err
	}
	return []RuntimeTokenResponse{{
		ID:        uuid.NewString(),
		AgentID:   agentID.String(),
		Name:      "worker",
		Prefix:    "rt_live_abcd",
		Scopes:    []string{"agents:run"},
		CreatedAt: "2026-06-21T00:00:00Z",
	}}, nil
}

func (f *controlA2AService) RevokeRuntimeToken(_ context.Context, userID, tokenID uuid.UUID) error {
	f.tokenID = tokenID
	return f.recordControl("revoke-runtime-token", userID)
}

func (f *controlA2AService) GetRuntimeWorkbench(_ context.Context, userID, agentID uuid.UUID) (*RuntimeWorkbenchResponse, error) {
	f.agentID = agentID
	if err := f.recordControl("runtime-workbench", userID); err != nil {
		return nil, err
	}
	return &RuntimeWorkbenchResponse{
		Agent: RuntimeWorkbenchAgent{
			ID:             agentID.String(),
			Slug:           "runtime-agent",
			Name:           "Runtime Agent",
			ConnectionMode: "runtime_pull",
		},
		Runtime: RuntimeWorkbenchRuntime{ActiveTokenCount: 1, PendingRunCount: 2, ClaimNow: true},
		Tokens:  []RuntimeTokenResponse{{ID: uuid.NewString(), AgentID: agentID.String(), Name: "worker", Prefix: "rt_live_abcd", CreatedAt: "2026-06-21T00:00:00Z"}},
		RecentRuns: []RuntimeWorkbenchRun{{
			RunID:     uuid.NewString(),
			Status:    "running",
			Source:    "a2a",
			StartedAt: "2026-06-21T00:00:00Z",
			DetailURL: "/runs/detail",
		}},
		Diagnostics: []RuntimeWorkbenchDiagnostic{{Code: "runtime_ready", Severity: "info", Message: "ready", NextAction: "none"}},
	}, nil
}

func (f *controlA2AService) GetCallPolicy(_ context.Context, userID, agentID uuid.UUID) (*CallPolicyResponse, error) {
	f.agentID = agentID
	if err := f.recordControl("get-call-policy", userID); err != nil {
		return nil, err
	}
	return &CallPolicyResponse{AgentID: agentID.String(), CallableBy: "public", UpdatedAt: "2026-06-21T00:00:00Z"}, nil
}

func (f *controlA2AService) UpdateCallPolicy(_ context.Context, userID, agentID uuid.UUID, req *UpdateCallPolicyRequest) (*CallPolicyResponse, error) {
	f.agentID = agentID
	if req != nil {
		f.updatePolicyReq = *req
	}
	if err := f.recordControl("update-call-policy", userID); err != nil {
		return nil, err
	}
	return &CallPolicyResponse{AgentID: agentID.String(), CallableBy: f.updatePolicyReq.CallableBy, UpdatedAt: "2026-06-21T00:00:00Z"}, nil
}

func (f *controlA2AService) CallAgent(_ context.Context, plaintextToken string, req *CallAgentRequest) (*runtimepkg.RunResponse, error) {
	f.calls = append(f.calls, "call-agent")
	f.callToken = plaintextToken
	if req != nil {
		f.callReq = *req
	}
	if err := f.maybeErr("call-agent"); err != nil {
		return nil, err
	}
	return &runtimepkg.RunResponse{
		RunID:         uuid.NewString(),
		Status:        "success",
		Output:        map[string]interface{}{"ok": true},
		ParentRunID:   f.callReq.ParentRunID,
		CallerAgentID: f.callReq.TargetAgentID,
		BillingMode:   "a2a",
	}, nil
}

func (f *controlA2AService) ListChildren(_ context.Context, userID, parentRunID uuid.UUID) ([]ChildRunResponse, error) {
	f.parentRunID = parentRunID
	if err := f.recordControl("list-children", userID); err != nil {
		return nil, err
	}
	return []ChildRunResponse{{
		ChildRunID:      uuid.NewString(),
		ParentRunID:     parentRunID.String(),
		CallerAgentID:   uuid.NewString(),
		CallerAgentSlug: "caller",
		CallerAgentName: "Caller",
		TargetAgentID:   uuid.NewString(),
		TargetAgentSlug: "target",
		TargetAgentName: "Target",
		Reason:          "need data",
		Status:          "success",
		StartedAt:       "2026-06-21T00:00:00Z",
		Source:          "a2a",
		BillingMode:     "a2a",
	}}, nil
}

func (f *controlA2AService) ListParentRuns(_ context.Context, userID uuid.UUID, page, size int32, search string) (*ParentRunListResponse, error) {
	f.page = page
	f.size = size
	f.search = search
	if err := f.recordControl("list-parent-runs", userID); err != nil {
		return nil, err
	}
	return &ParentRunListResponse{
		Items: []ParentRunSummary{{
			ParentRunID:     uuid.NewString(),
			CallerAgentID:   uuid.NewString(),
			CallerAgentSlug: "caller",
			CallerAgentName: "Caller",
			Source:          "a2a",
			Status:          "success",
			StartedAt:       "2026-06-21T00:00:00Z",
			ChildCount:      1,
		}},
		Total: 1,
		Page:  page,
		Size:  size,
	}, nil
}

func fakeA2ATask(taskID, state string) *A2ATask {
	return &A2ATask{
		Kind:      "task",
		ID:        taskID,
		ContextID: "ctx-jsonrpc",
		Status: A2ATaskStatus{
			State:     state,
			Timestamp: "2026-06-21T00:00:00Z",
			Message: &A2AMessage{
				Kind:  "message",
				Role:  "agent",
				Parts: []map[string]interface{}{{"kind": "text", "text": "ok"}},
			},
		},
	}
}

func fakeA2APushConfig(params *A2ATaskPushConfigParams) *A2ATaskPushNotificationConfig {
	cfg := pushConfigFromPushParams(params)
	if cfg.ID == "" {
		cfg.ID = configIDFromPushParams(params)
	}
	if cfg.URL == "" {
		cfg.URL = "https://hooks.example/a2a"
	}
	taskID := taskIDFromPushParams(params)
	if taskID == "" {
		taskID = params.ID
	}
	return &A2ATaskPushNotificationConfig{
		ID:                     cfg.ID,
		TaskID:                 taskID,
		URL:                    cfg.URL,
		Token:                  cfg.Token,
		Authentication:         cfg.Authentication,
		Metadata:               cfg.Metadata,
		EventTypes:             cfg.EventTypes,
		PushNotificationConfig: cfg,
	}
}

type fakeA2ACardProvider struct {
	publicSlug   string
	extendedSlug string
}

func (f *fakeA2ACardProvider) GetAgentCardBySlug(_ context.Context, slug string) (*agent.AgentCardResponse, error) {
	f.publicSlug = slug
	return &agent.AgentCardResponse{Name: slug, Version: "1.0"}, nil
}

func (f *fakeA2ACardProvider) GetExtendedAgentCardBySlug(_ context.Context, slug string) (*agent.AgentCardResponse, error) {
	f.extendedSlug = slug
	return &agent.AgentCardResponse{Name: slug, Version: "1.0", SupportsAuthenticatedExtendedCard: true}, nil
}

func diagnosticCodes(items []RuntimeWorkbenchDiagnostic) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.Code)
	}
	return out
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
