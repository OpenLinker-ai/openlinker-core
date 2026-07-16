package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestRetiredRuntimeHeartbeatRouteIsAbsentWithoutCallingService(t *testing.T) {
	svc := &mockRuntimeService{}
	e := echo.New()
	NewHandler(svc).RegisterAgentRuntime(e.Group("/api/v1"))

	tests := []struct {
		method string
		path   string
	}{{method: http.MethodPost, path: "/api/v1/agent-runtime/heartbeat"}}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(`{"legacy":true}`))
		req.Header.Set(echo.HeaderAuthorization, "Bearer must-not-be-validated")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code, test.method+" "+test.path)
	}

	require.Empty(t, svc.validateRuntimeTokenPlaintext)
}

func TestRuntimeHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	artifactID := uuid.New()
	messageID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	runResp := &RunResponse{
		RunID:     runID.String(),
		Status:    "success",
		Output:    map[string]interface{}{"text": "ok"},
		CostCents: 0,
		Source:    "api",
	}
	eventResp := RunEventResponse{
		EventID:   uuid.NewString(),
		RunID:     runID.String(),
		Sequence:  3,
		EventType: "run.completed",
		Payload:   map[string]interface{}{"status": "success"},
		CreatedAt: now,
	}
	artifactResp := RunArtifactResponse{
		ID:           artifactID.String(),
		RunID:        runID.String(),
		ArtifactType: "file",
		Title:        "Orders",
		Content:      map[string]interface{}{"rows": float64(2)},
		Visibility:   "owner",
		CreatedAt:    now,
	}
	messageResp := RunMessageResponse{
		ID:        messageID.String(),
		RunID:     runID.String(),
		Role:      "assistant",
		Content:   "done",
		Payload:   map[string]interface{}{"text": "done"},
		CreatedAt: now,
	}
	t.Run("run lifecycle reads and events", func(t *testing.T) {
		mock := &mockRuntimeService{
			runResp:       runResp,
			startRunResp:  &RunResponse{RunID: runID.String(), Status: "running", Source: "api"},
			getRunResp:    runResp,
			runEventsResp: []RunEventResponse{eventResp},
			artifactsResp: []RunArtifactResponse{artifactResp},
			messagesResp:  []RunMessageResponse{messageResp},
		}
		h := NewHandler(mock)

		postCtx, postRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodPost,
			target:     "/api/v1/run",
			userID:     userID.String(),
			authMethod: "user_token",
			scopes:     []string{"agents:run"},
			body:       `{"agent_id":"` + agentID.String() + `","input":{"q":"hi"}}`,
			headers:    map[string]string{"Idempotency-Key": "run-sync-1"},
		})
		if err := h.PostRun(postCtx); err != nil {
			t.Fatalf("PostRun error = %v", err)
		}
		if postRec.Code != http.StatusCreated || mock.runUserID != userID || mock.runReq.AgentID != agentID.String() || mock.runReq.IdempotencyKey != "run-sync-1" || mock.runSource != "api" {
			t.Fatalf("post run code=%d user=%s source=%q req=%#v", postRec.Code, mock.runUserID, mock.runSource, mock.runReq)
		}

		asyncCtx, asyncRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodPost,
			target:     "/api/v1/runs",
			userID:     userID.String(),
			authMethod: "user_token",
			scopes:     []string{"agents:run"},
			body:       `{"agent_id":"` + agentID.String() + `","input":{"q":"async"}}`,
			headers:    map[string]string{"Idempotency-Key": "run-async-1"},
		})
		if err := h.PostRunAsync(asyncCtx); err != nil {
			t.Fatalf("PostRunAsync error = %v", err)
		}
		if asyncRec.Code != http.StatusCreated || mock.startRunUserID != userID || mock.startRunSource != "api" {
			t.Fatalf("post async code=%d user=%s source=%q", asyncRec.Code, mock.startRunUserID, mock.startRunSource)
		}

		getCtx, getRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String(),
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.GetRun(getCtx); err != nil {
			t.Fatalf("GetRun error = %v", err)
		}
		if getRec.Code != http.StatusOK || mock.getRunID != runID || mock.getRunUserID != userID {
			t.Fatalf("get run code=%d run=%s user=%s", getRec.Code, mock.getRunID, mock.getRunUserID)
		}

		eventsCtx, eventsRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/events?after_sequence=2&limit=7",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.GetRunEvents(eventsCtx); err != nil {
			t.Fatalf("GetRunEvents error = %v", err)
		}
		if eventsRec.Code != http.StatusOK || mock.eventsRunID != runID || mock.eventsAfter != 2 || mock.eventsLimit != 7 {
			t.Fatalf("events code=%d run=%s after=%d limit=%d", eventsRec.Code, mock.eventsRunID, mock.eventsAfter, mock.eventsLimit)
		}
		var eventsBody struct {
			Items []RunEventResponse `json:"items"`
			Meta  RunEventPageMeta   `json:"meta"`
		}
		decodeRuntimeDispatchJSON(t, eventsRec, &eventsBody)
		if len(eventsBody.Items) != 1 || eventsBody.Items[0].EventType != "run.completed" || eventsBody.Meta.RequestedAfterSequence != 2 {
			t.Fatalf("events body = %#v", eventsBody)
		}

		artifactsCtx, artifactsRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/artifacts",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.GetRunArtifacts(artifactsCtx); err != nil {
			t.Fatalf("GetRunArtifacts error = %v", err)
		}
		if artifactsRec.Code != http.StatusOK || mock.artifactsRunID != runID || mock.artifactsUserID != userID {
			t.Fatalf("artifacts code=%d run=%s user=%s", artifactsRec.Code, mock.artifactsRunID, mock.artifactsUserID)
		}

		messagesCtx, messagesRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/messages",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.GetRunMessages(messagesCtx); err != nil {
			t.Fatalf("GetRunMessages error = %v", err)
		}
		if messagesRec.Code != http.StatusOK || mock.messagesRunID != runID || mock.messagesUserID != userID {
			t.Fatalf("messages code=%d run=%s user=%s", messagesRec.Code, mock.messagesRunID, mock.messagesUserID)
		}

		streamCtx, streamRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/stream",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := h.StreamRunEvents(streamCtx); err != nil {
			t.Fatalf("StreamRunEvents error = %v", err)
		}
		if streamRec.Code != http.StatusOK || !strings.Contains(streamRec.Body.String(), "event: run.completed") || mock.eventsLimit != defaultRunEventsLimit {
			t.Fatalf("stream code=%d limit=%d body=%s", streamRec.Code, mock.eventsLimit, streamRec.Body.String())
		}

	})

}

func TestRuntimeReplayAndDeadLetterHandlers(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	sourceRunID := uuid.New()
	newRunID := uuid.New()
	now := time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC)
	mock := &mockRuntimeService{
		getRunResp: &RunResponse{
			RunID:   sourceRunID.String(),
			AgentID: agentID.String(),
			Status:  "failed",
		},
		replayResp: &RunResponse{
			RunID:             newRunID.String(),
			AgentID:           agentID.String(),
			Status:            "running",
			RuntimeContractID: RuntimeContractID,
			DispatchState:     string(RuntimeDispatchPending),
			MaxAttempts:       3,
			ReplayOfRunID:     sourceRunID.String(),
		},
		deadLetterResp: &RuntimeDeadLetterListResponse{
			Items: []RuntimeDeadLetterListItem{{
				DeadLetterID: sourceRunID.String(),
				RunID:        sourceRunID.String(),
				AgentID:      agentID.String(),
				ReasonCode:   "RUNTIME_RETRY_EXHAUSTED",
				CreatedAt:    now,
			}},
			Total: 1,
			Limit: 25,
		},
	}
	h := NewHandler(mock)

	replayCtx, replayRecorder := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method:     http.MethodPost,
		target:     "/api/v1/runs/" + sourceRunID.String() + "/replay",
		userID:     userID.String(),
		authMethod: "user_token",
		scopes:     []string{"agents:run"},
		params:     map[string]string{"id": sourceRunID.String()},
		headers:    map[string]string{"Idempotency-Key": "replay-once"},
	})
	require.NoError(t, h.ReplayRun(replayCtx))
	require.Equal(t, http.StatusCreated, replayRecorder.Code)
	require.Equal(t, "/api/v1/runs/"+newRunID.String(), replayRecorder.Header().Get(echo.HeaderLocation))
	require.Equal(t, userID, mock.replayUserID)
	require.Equal(t, sourceRunID, mock.replaySourceRunID)
	require.Equal(t, "replay-once", mock.replayIdempotencyKey)
	require.Equal(t, "api", mock.replaySource)

	missingKeyCtx, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method:     http.MethodPost,
		target:     "/api/v1/runs/" + sourceRunID.String() + "/replay",
		userID:     userID.String(),
		authMethod: "user_token",
		scopes:     []string{"agents:run"},
		params:     map[string]string{"id": sourceRunID.String()},
	})
	err := h.ReplayRun(missingKeyCtx)
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusUnprocessableEntity, httpErr.Status)
	require.Equal(t, httpx.ErrorCode(IdempotencyErrorKeyRequired), httpErr.Code)

	dlqCtx, dlqRecorder := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodGet,
		target: "/api/v1/admin/runtime/dead-letters?limit=25&offset=5",
	})
	require.NoError(t, h.ListRuntimeDeadLetters(dlqCtx))
	require.Equal(t, http.StatusOK, dlqRecorder.Code)
	require.Equal(t, int32(25), mock.deadLetterLimit)
	require.Equal(t, int32(5), mock.deadLetterOffset)
	var listed RuntimeDeadLetterListResponse
	decodeRuntimeDispatchJSON(t, dlqRecorder, &listed)
	require.Equal(t, int32(1), listed.Total)
	require.Len(t, listed.Items, 1)
	require.Equal(t, "RUNTIME_RETRY_EXHAUSTED", listed.Items[0].ReasonCode)
}

func TestRuntimeNodeAdminHandlers(t *testing.T) {
	nodeID := uuid.New()
	now := time.Date(2026, 7, 12, 2, 3, 4, 0, time.UTC)
	item := &RuntimeNodeListItem{
		NodeID:                nodeID.String(),
		DisplayName:           "Node A",
		NodeVersion:           "v2",
		ProtocolVersion:       2,
		RuntimeContractID:     RuntimeContractID,
		RuntimeContractDigest: RuntimeContractDigest,
		ContractMatch:         true,
		Features:              RuntimeRequiredFeatures(),
		Capacity:              4,
		Status:                "active",
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	mock := &mockRuntimeService{
		runtimeNodeResp: item,
		runtimeNodeListResp: &RuntimeNodeListResponse{
			Items:                 []RuntimeNodeListItem{*item},
			Total:                 1,
			Limit:                 20,
			Offset:                5,
			CurrentContractID:     RuntimeContractID,
			CurrentContractDigest: RuntimeContractDigest,
			DatabaseTime:          now,
		},
	}
	h := NewHandler(mock)

	listCtx, listRecorder := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodGet,
		target: "/api/v1/admin/runtime/nodes?limit=20&offset=5",
	})
	require.NoError(t, h.ListRuntimeNodes(listCtx))
	require.Equal(t, http.StatusOK, listRecorder.Code)
	require.Equal(t, int32(20), mock.runtimeNodeLimit)
	require.Equal(t, int32(5), mock.runtimeNodeOffset)
	var listed RuntimeNodeListResponse
	decodeRuntimeDispatchJSON(t, listRecorder, &listed)
	require.Len(t, listed.Items, 1)
	require.True(t, listed.Items[0].ContractMatch)

	drainCtx, drainRecorder := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodPost,
		target: "/api/v1/admin/runtime/nodes/" + nodeID.String() + "/drain",
		params: map[string]string{"id": nodeID.String()},
	})
	require.NoError(t, h.DrainRuntimeNode(drainCtx))
	require.Equal(t, http.StatusOK, drainRecorder.Code)
	require.Equal(t, nodeID, mock.runtimeNodeID)

	activateCtx, activateRecorder := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodPost,
		target: "/api/v1/admin/runtime/nodes/" + nodeID.String() + "/activate",
		params: map[string]string{"id": nodeID.String()},
	})
	require.NoError(t, h.ActivateRuntimeNode(activateCtx))
	require.Equal(t, http.StatusOK, activateRecorder.Code)
	require.Equal(t, nodeID, mock.runtimeNodeID)

	revokeCtx, revokeRecorder := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodPost,
		target: "/api/v1/admin/runtime/nodes/" + nodeID.String() + "/revoke",
		params: map[string]string{"id": nodeID.String()},
		body:   `{"reason":"  certificate rotated  "}`,
	})
	require.NoError(t, h.RevokeRuntimeNode(revokeCtx))
	require.Equal(t, http.StatusOK, revokeRecorder.Code)
	require.Equal(t, nodeID, mock.runtimeNodeID)
	require.Equal(t, "certificate rotated", mock.runtimeNodeRevokeReason)

	badListCtx, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodGet,
		target: "/api/v1/admin/runtime/nodes?limit=-1",
	})
	requireRuntimeHTTPStatus(t, h.ListRuntimeNodes(badListCtx), http.StatusBadRequest)
	badIDCtx, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodPost,
		target: "/api/v1/admin/runtime/nodes/bad/drain",
		params: map[string]string{"id": "bad"},
	})
	requireRuntimeHTTPStatus(t, h.DrainRuntimeNode(badIDCtx), http.StatusBadRequest)
	emptyReasonCtx, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method: http.MethodPost,
		target: "/api/v1/admin/runtime/nodes/" + nodeID.String() + "/revoke",
		params: map[string]string{"id": nodeID.String()},
		body:   `{"reason":" "}`,
	})
	requireRuntimeHTTPStatus(t, h.RevokeRuntimeNode(emptyReasonCtx), http.StatusUnprocessableEntity)
}

func TestRuntimeHandlerPropagatesServiceErrors(t *testing.T) {
	userID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	serviceErr := httpx.Conflict("service failed")

	tests := []struct {
		name string
		call func(*Handler, echo.Context) error
		ctx  echo.Context
	}{
		{
			name: "post run",
			call: (*Handler).PostRun,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodPost,
				target:     "/api/v1/run",
				userID:     userID.String(),
				authMethod: "jwt",
				body:       `{"agent_id":"` + agentID.String() + `","input":{"q":"hi"}}`,
				headers:    map[string]string{"Idempotency-Key": "run-error-sync"},
			}),
		},
		{
			name: "post run async",
			call: (*Handler).PostRunAsync,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodPost,
				target:     "/api/v1/runs",
				userID:     userID.String(),
				authMethod: "jwt",
				body:       `{"agent_id":"` + agentID.String() + `","input":{"q":"hi"}}`,
				headers:    map[string]string{"Idempotency-Key": "run-error-async"},
			}),
		},
		{
			name: "get run",
			call: (*Handler).GetRun,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String(),
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "get events",
			call: (*Handler).GetRunEvents,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/events",
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "get artifacts",
			call: (*Handler).GetRunArtifacts,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/artifacts",
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "get messages",
			call: (*Handler).GetRunMessages,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/messages",
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "stream events",
			call: (*Handler).StreamRunEvents,
			ctx: mustRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/stream",
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireRuntimeDispatchHTTPStatus(t, tt.call(NewHandler(&mockRuntimeService{err: serviceErr}), tt.ctx), http.StatusConflict)
		})
	}
}

func TestRuntimeHandlerStreamRunEventsEdges(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	started := RunEventResponse{
		EventID:   uuid.NewString(),
		RunID:     runID.String(),
		Sequence:  1,
		EventType: "run.started",
		Payload:   map[string]interface{}{"status": "running"},
		CreatedAt: time.Now(),
	}

	noFlusherCtx, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method:     http.MethodGet,
		target:     "/api/v1/runs/" + runID.String() + "/stream",
		userID:     userID.String(),
		authMethod: "jwt",
		params:     map[string]string{"id": runID.String()},
	})
	noFlusherCtx.Response().Writer = errorResponseWriter{}
	err := NewHandler(&mockRuntimeService{}).StreamRunEvents(noFlusherCtx)
	requireRuntimeHTTPStatus(t, err, http.StatusInternalServerError)

	streamSvc := &pollingRuntimeService{
		firstEvents: []RunEventResponse{started},
		pollErr:     httpx.Internal("poll failed"),
	}
	streamCtx, streamRec := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method:     http.MethodGet,
		target:     "/api/v1/runs/" + runID.String() + "/stream",
		userID:     userID.String(),
		authMethod: "jwt",
		params:     map[string]string{"id": runID.String()},
	})
	ctx, cancel := context.WithTimeout(streamCtx.Request().Context(), 2*time.Second)
	defer cancel()
	streamCtx.SetRequest(streamCtx.Request().WithContext(ctx))
	if err := NewHandler(streamSvc).StreamRunEvents(streamCtx); err != nil {
		t.Fatalf("StreamRunEvents poll error path returned error = %v", err)
	}
	if streamRec.Code != http.StatusOK || !strings.Contains(streamRec.Body.String(), "event: run.stream.error") || streamSvc.calls < 2 {
		t.Fatalf("stream poll error code=%d calls=%d body=%s", streamRec.Code, streamSvc.calls, streamRec.Body.String())
	}
}

func TestRuntimeHandlerRunEventPageIncludesRetentionMetadata(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	earliest := int32(6)
	latest := int32(8)
	event := RunEventResponse{
		EventID:   uuid.NewString(),
		RunID:     runID.String(),
		Sequence:  6,
		EventType: "run.status.changed",
		Payload:   map[string]interface{}{"status": "running"},
		CreatedAt: time.Now(),
	}
	mock := &mockRuntimeService{runEventsPage: &RunEventPageResponse{
		Items: []RunEventResponse{event},
		Meta: RunEventPageMeta{
			RequestedAfterSequence:    2,
			EffectiveAfterSequence:    5,
			RetainedThroughSequence:   5,
			EarliestAvailableSequence: &earliest,
			LatestAvailableSequence:   &latest,
			RetentionGap:              true,
		},
	}}
	ctx, rec := newRuntimeDispatchContext(&runtimeDispatchRequest{
		method:     http.MethodGet,
		target:     "/api/v1/runs/" + runID.String() + "/events?after_sequence=2&limit=10",
		userID:     userID.String(),
		authMethod: "jwt",
		params:     map[string]string{"id": runID.String()},
	})
	if err := NewHandler(mock).GetRunEvents(ctx); err != nil {
		t.Fatalf("GetRunEvents error = %v", err)
	}
	var got RunEventPageResponse
	decodeRuntimeDispatchJSON(t, rec, &got)
	if rec.Code != http.StatusOK || len(got.Items) != 1 || got.Meta.EffectiveAfterSequence != 5 || !got.Meta.RetentionGap {
		t.Fatalf("event page code=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `"events"`) {
		t.Fatalf("event page unexpectedly exposed legacy events field: %s", rec.Body.String())
	}
}

func TestRuntimeHandlerStreamRunEventsRetentionGap(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	earliest := int32(6)
	latest := int32(6)
	terminal := RunEventResponse{
		EventID:   uuid.NewString(),
		RunID:     runID.String(),
		Sequence:  6,
		EventType: "run.completed",
		Payload:   map[string]interface{}{"status": "success"},
		CreatedAt: time.Now(),
	}

	t.Run("gap is the first frame and has no synthetic id", func(t *testing.T) {
		mock := &mockRuntimeService{runEventsPage: &RunEventPageResponse{
			Items: []RunEventResponse{terminal},
			Meta: RunEventPageMeta{
				RequestedAfterSequence:    1,
				EffectiveAfterSequence:    5,
				RetainedThroughSequence:   5,
				EarliestAvailableSequence: &earliest,
				LatestAvailableSequence:   &latest,
				RetentionGap:              true,
			},
		}}
		ctx, rec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/stream?after_sequence=1",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := NewHandler(mock).StreamRunEvents(ctx); err != nil {
			t.Fatalf("StreamRunEvents error = %v", err)
		}
		body := rec.Body.String()
		frames := strings.Split(body, "\n\n")
		if len(frames) < 2 || !strings.HasPrefix(frames[0], "event: run.stream.gap\n") || strings.Contains(frames[0], "id:") {
			t.Fatalf("first SSE frame = %q", frames[0])
		}
		if !strings.Contains(frames[0], `"retention_gap":true`) || !strings.Contains(frames[1], "id: 6") {
			t.Fatalf("SSE body = %s", body)
		}
	})

	t.Run("complete retained history emits no gap", func(t *testing.T) {
		mock := &mockRuntimeService{runEventsPage: &RunEventPageResponse{
			Items: []RunEventResponse{terminal},
			Meta: RunEventPageMeta{
				RequestedAfterSequence:    5,
				EffectiveAfterSequence:    5,
				RetainedThroughSequence:   5,
				EarliestAvailableSequence: &earliest,
				LatestAvailableSequence:   &latest,
			},
		}}
		ctx, rec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/stream?after_sequence=5",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := NewHandler(mock).StreamRunEvents(ctx); err != nil {
			t.Fatalf("StreamRunEvents error = %v", err)
		}
		if strings.Contains(rec.Body.String(), "run.stream.gap") || !strings.Contains(rec.Body.String(), "id: 6") {
			t.Fatalf("SSE body = %s", rec.Body.String())
		}
	})

	t.Run("fully retained terminal run emits gap then closes", func(t *testing.T) {
		mock := &mockRuntimeService{runEventsPage: &RunEventPageResponse{
			Items: []RunEventResponse{},
			Meta: RunEventPageMeta{
				RequestedAfterSequence:  0,
				EffectiveAfterSequence:  6,
				RetainedThroughSequence: 6,
				RetentionGap:            true,
				Terminal:                true,
				StreamComplete:          true,
			},
		}}
		ctx, rec := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/stream",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
		})
		if err := NewHandler(mock).StreamRunEvents(ctx); err != nil {
			t.Fatalf("StreamRunEvents error = %v", err)
		}
		body := rec.Body.String()
		if !strings.HasPrefix(body, "event: run.stream.gap\n") || strings.Contains(body, "id:") || mock.eventsAfter != 0 {
			t.Fatalf("SSE body=%s after=%d", body, mock.eventsAfter)
		}
	})
}

func TestRuntimeHandlerStreamRunEventsCursorValidationAndPrecedence(t *testing.T) {
	userID := uuid.New()
	runID := uuid.New()
	terminal := RunEventResponse{
		EventID:   uuid.NewString(),
		RunID:     runID.String(),
		Sequence:  8,
		EventType: "run.completed",
		Payload:   map[string]interface{}{"status": "success"},
		CreatedAt: time.Now(),
	}

	t.Run("query cursor wins over Last-Event-ID", func(t *testing.T) {
		mock := &mockRuntimeService{runEventsResp: []RunEventResponse{terminal}}
		ctx, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
			method:     http.MethodGet,
			target:     "/api/v1/runs/" + runID.String() + "/stream?after_sequence=7",
			userID:     userID.String(),
			authMethod: "jwt",
			params:     map[string]string{"id": runID.String()},
			headers:    map[string]string{"Last-Event-ID": "not-an-integer"},
		})
		if err := NewHandler(mock).StreamRunEvents(ctx); err != nil {
			t.Fatalf("StreamRunEvents error = %v", err)
		}
		if mock.eventsAfter != 7 {
			t.Fatalf("service after_sequence = %d, want 7", mock.eventsAfter)
		}
	})

	for _, test := range []struct {
		name   string
		query  string
		header string
	}{
		{name: "negative query", query: "?after_sequence=-1"},
		{name: "overflow query", query: "?after_sequence=2147483648"},
		{name: "empty query", query: "?after_sequence=", header: "7"},
		{name: "negative header", header: "-1"},
		{name: "overflow header", header: "2147483648"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, _ := newRuntimeDispatchContext(&runtimeDispatchRequest{
				method:     http.MethodGet,
				target:     "/api/v1/runs/" + runID.String() + "/stream" + test.query,
				userID:     userID.String(),
				authMethod: "jwt",
				params:     map[string]string{"id": runID.String()},
				headers:    map[string]string{"Last-Event-ID": test.header},
			})
			err := NewHandler(&mockRuntimeService{}).StreamRunEvents(ctx)
			requireRuntimeHTTPStatus(t, err, http.StatusBadRequest)
		})
	}
}

func TestWriteSSEEventsDoesNotMoveCursorBackward(t *testing.T) {
	runID := uuid.NewString()
	events := []RunEventResponse{
		{EventID: uuid.NewString(), RunID: runID, Sequence: 4, EventType: "run.started"},
		{EventID: uuid.NewString(), RunID: runID, Sequence: 5, EventType: "run.status.changed"},
		{EventID: uuid.NewString(), RunID: runID, Sequence: 3, EventType: "run.completed"},
	}
	recorder := httptest.NewRecorder()
	terminal, next, err := writeSSEEvents(recorder, events, 4)
	if err != nil {
		t.Fatalf("writeSSEEvents error = %v", err)
	}
	if terminal || next != 5 || strings.Contains(recorder.Body.String(), "id: 4") || strings.Contains(recorder.Body.String(), "id: 3") {
		t.Fatalf("terminal=%t next=%d body=%s", terminal, next, recorder.Body.String())
	}
}

type mockRuntimeService struct {
	err error

	runUserID uuid.UUID
	runReq    *RunRequest
	runSource string
	runResp   *RunResponse

	startRunUserID uuid.UUID
	startRunReq    *RunRequest
	startRunSource string
	startRunResp   *RunResponse

	getRunUserID uuid.UUID
	getRunID     uuid.UUID
	getRunResp   *RunResponse

	replayUserID         uuid.UUID
	replaySourceRunID    uuid.UUID
	replayIdempotencyKey string
	replaySource         string
	replayResp           *RunResponse

	deadLetterLimit  int32
	deadLetterOffset int32
	deadLetterResp   *RuntimeDeadLetterListResponse

	runtimeNodeLimit        int32
	runtimeNodeOffset       int32
	runtimeNodeListResp     *RuntimeNodeListResponse
	runtimeNodeID           uuid.UUID
	runtimeNodeRevokeReason string
	runtimeNodeResp         *RuntimeNodeListItem

	eventsUserID  uuid.UUID
	eventsRunID   uuid.UUID
	eventsAfter   int32
	eventsLimit   int32
	runEventsResp []RunEventResponse
	runEventsPage *RunEventPageResponse

	artifactsUserID uuid.UUID
	artifactsRunID  uuid.UUID
	artifactsResp   []RunArtifactResponse

	messagesUserID uuid.UUID
	messagesRunID  uuid.UUID
	messagesResp   []RunMessageResponse

	validateRuntimeTokenErr       error
	validateRuntimeTokenPlaintext string
	validateRuntimeTokenScopes    []string
}

func (m *mockRuntimeService) Run(_ context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	m.runUserID = userID
	m.runReq = req
	m.runSource = source
	return m.runResp, m.err
}

func (m *mockRuntimeService) StartRun(_ context.Context, userID uuid.UUID, req *RunRequest, source string) (*RunResponse, error) {
	m.startRunUserID = userID
	m.startRunReq = req
	m.startRunSource = source
	return m.startRunResp, m.err
}

func (m *mockRuntimeService) GetRun(_ context.Context, userID, runID uuid.UUID) (*RunResponse, error) {
	m.getRunUserID = userID
	m.getRunID = runID
	return m.getRunResp, m.err
}

func (m *mockRuntimeService) ReplayRun(
	_ context.Context,
	userID, sourceRunID uuid.UUID,
	idempotencyKey, source string,
) (*RunResponse, error) {
	m.replayUserID = userID
	m.replaySourceRunID = sourceRunID
	m.replayIdempotencyKey = idempotencyKey
	m.replaySource = source
	return m.replayResp, m.err
}

func (m *mockRuntimeService) ListRuntimeDeadLetters(
	_ context.Context,
	limit, offset int32,
) (*RuntimeDeadLetterListResponse, error) {
	m.deadLetterLimit = limit
	m.deadLetterOffset = offset
	return m.deadLetterResp, m.err
}

func (m *mockRuntimeService) ListRuntimeNodes(
	_ context.Context,
	limit, offset int32,
) (*RuntimeNodeListResponse, error) {
	m.runtimeNodeLimit = limit
	m.runtimeNodeOffset = offset
	return m.runtimeNodeListResp, m.err
}

func (m *mockRuntimeService) DrainRuntimeNode(
	_ context.Context,
	nodeID uuid.UUID,
) (*RuntimeNodeListItem, error) {
	m.runtimeNodeID = nodeID
	return m.runtimeNodeResp, m.err
}

func (m *mockRuntimeService) ActivateRuntimeNode(
	_ context.Context,
	nodeID uuid.UUID,
) (*RuntimeNodeListItem, error) {
	m.runtimeNodeID = nodeID
	return m.runtimeNodeResp, m.err
}

func (m *mockRuntimeService) RevokeRuntimeNode(
	_ context.Context,
	nodeID uuid.UUID,
	reason string,
) (*RuntimeNodeListItem, error) {
	m.runtimeNodeID = nodeID
	m.runtimeNodeRevokeReason = reason
	return m.runtimeNodeResp, m.err
}

func (m *mockRuntimeService) ListRunEvents(_ context.Context, userID, runID uuid.UUID, afterSequence, limit int32) ([]RunEventResponse, error) {
	m.eventsUserID = userID
	m.eventsRunID = runID
	m.eventsAfter = afterSequence
	m.eventsLimit = limit
	return m.runEventsResp, m.err
}

func (m *mockRuntimeService) ListRunEventsPage(_ context.Context, userID, runID uuid.UUID, afterSequence, limit int32) (*RunEventPageResponse, error) {
	m.eventsUserID = userID
	m.eventsRunID = runID
	m.eventsAfter = afterSequence
	m.eventsLimit = limit
	if m.err != nil {
		return nil, m.err
	}
	if m.runEventsPage != nil {
		return m.runEventsPage, nil
	}
	items := append([]RunEventResponse(nil), m.runEventsResp...)
	page := &RunEventPageResponse{
		Items: items,
		Meta: RunEventPageMeta{
			RequestedAfterSequence: afterSequence,
			EffectiveAfterSequence: afterSequence,
		},
	}
	if len(items) > 0 {
		earliest := items[0].Sequence
		latest := items[len(items)-1].Sequence
		page.Meta.EarliestAvailableSequence = &earliest
		page.Meta.LatestAvailableSequence = &latest
	}
	return page, nil
}

func (m *mockRuntimeService) ListRunArtifacts(_ context.Context, userID, runID uuid.UUID) ([]RunArtifactResponse, error) {
	m.artifactsUserID = userID
	m.artifactsRunID = runID
	return m.artifactsResp, m.err
}

func (m *mockRuntimeService) ListRunMessages(_ context.Context, userID, runID uuid.UUID) ([]RunMessageResponse, error) {
	m.messagesUserID = userID
	m.messagesRunID = runID
	return m.messagesResp, m.err
}

func (m *mockRuntimeService) ValidateRuntimeToken(_ context.Context, token string, scopes ...string) (db.AgentRuntimeToken, error) {
	m.validateRuntimeTokenPlaintext = token
	m.validateRuntimeTokenScopes = append([]string(nil), scopes...)
	if m.validateRuntimeTokenErr != nil {
		return db.AgentRuntimeToken{}, m.validateRuntimeTokenErr
	}
	if m.err != nil {
		return db.AgentRuntimeToken{}, m.err
	}
	return db.AgentRuntimeToken{ID: uuid.New(), AgentID: uuid.New(), ConnectionMode: "runtime"}, nil
}

type pollingRuntimeService struct {
	mockRuntimeService
	calls       int
	firstEvents []RunEventResponse
	pollErr     error
}

func (m *pollingRuntimeService) ListRunEvents(_ context.Context, userID, runID uuid.UUID, afterSequence, limit int32) ([]RunEventResponse, error) {
	m.calls++
	m.eventsUserID = userID
	m.eventsRunID = runID
	m.eventsAfter = afterSequence
	m.eventsLimit = limit
	if m.calls == 1 {
		return m.firstEvents, nil
	}
	return nil, m.pollErr
}

func (m *pollingRuntimeService) ListRunEventsPage(_ context.Context, userID, runID uuid.UUID, afterSequence, limit int32) (*RunEventPageResponse, error) {
	m.calls++
	m.eventsUserID = userID
	m.eventsRunID = runID
	m.eventsAfter = afterSequence
	m.eventsLimit = limit
	if m.calls == 1 {
		return &RunEventPageResponse{
			Items: append([]RunEventResponse(nil), m.firstEvents...),
			Meta: RunEventPageMeta{
				RequestedAfterSequence: afterSequence,
				EffectiveAfterSequence: afterSequence,
			},
		}, nil
	}
	return nil, m.pollErr
}

type runtimeDispatchRequest struct {
	method     string
	target     string
	body       string
	userID     string
	authMethod string
	scopes     []string
	params     map[string]string
	headers    map[string]string
}

func newRuntimeDispatchContext(spec *runtimeDispatchRequest) (echo.Context, *httptest.ResponseRecorder) {
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
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	if spec.authMethod != "" {
		c.Set(string(httpx.CtxKeyAuthMethod), spec.authMethod)
	}
	if spec.scopes != nil {
		c.Set(string(httpx.CtxKeyAuthScopes), spec.scopes)
	}
	if len(spec.params) > 0 {
		names := make([]string, 0, len(spec.params))
		values := make([]string, 0, len(spec.params))
		for name, value := range spec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c, rec
}

func mustRuntimeDispatchContext(spec *runtimeDispatchRequest) echo.Context {
	c, _ := newRuntimeDispatchContext(spec)
	return c
}

func decodeRuntimeDispatchJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}

func requireRuntimeDispatchHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var httpErr *httpx.HTTPError
	if !strings.Contains(err.Error(), "service failed") {
		t.Fatalf("error = %v, want propagated service failure", err)
	}
	if ok := errors.As(err, &httpErr); !ok || httpErr.Status != want {
		t.Fatalf("HTTP status = %#v, want %d", httpErr, want)
	}
}

func requireRuntimeHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var httpErr *httpx.HTTPError
	if ok := errors.As(err, &httpErr); !ok || httpErr.Status != want {
		t.Fatalf("HTTP status = %#v for error %v, want %d", httpErr, err, want)
	}
}
