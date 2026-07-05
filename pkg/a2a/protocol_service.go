package a2a

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const (
	a2aTaskStateSubmitted = "submitted"
	a2aTaskStateWorking   = "working"
	a2aTaskStateCompleted = "completed"
	a2aTaskStateCanceled  = "canceled"
	a2aTaskStateFailed    = "failed"
	a2aTaskStateRejected  = "rejected"
	a2aTaskStateInputReq  = "input_required"
	a2aTaskStateAuthReq   = "auth_required"
)

const (
	defaultA2AListTasksPageSize = 50
	maxA2AListTasksPageSize     = 100
)

type a2aTaskCursor struct {
	StartedAt string `json:"started_at"`
	ID        string `json:"id"`
}

type protocolContinuation struct {
	TaskID    string
	ContextID string
}

// SendProtocolMessage accepts an external A2A message/send request and runs the target Agent.
func (s *Service) SendProtocolMessage(ctx context.Context, userID uuid.UUID, slug string, params *A2AMessageSendParams) (*A2ATask, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return nil, httpx.BadRequest("缺少 Agent slug")
	}
	if params == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	agent, err := s.queries.GetAgentBySlug(ctx, slug)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Msg("a2a.SendProtocolMessage: GetAgentBySlug")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	normalizeProtocolMessageContext(params)
	if err := validateA2AMessageSendParams(params); err != nil {
		return nil, err
	}
	continuation, err := s.prepareProtocolContinuation(ctx, userID, agent.Slug, params)
	if err != nil {
		return nil, err
	}

	input, err := inputFromA2AMessage(params.Message)
	if err != nil {
		return nil, err
	}
	metadata := protocolMetadata(params)
	runReq := &runtime.RunRequest{
		AgentID:    agent.ID.String(),
		Input:      input,
		Metadata:   metadata,
		A2AContext: protocolRunA2AContext(params),
	}
	run := s.runtime.Run
	if shouldReturnA2AMessageImmediately(params) {
		run = s.runtime.StartRun
	}
	resp, err := run(ctx, userID, runReq, "api")
	if err != nil {
		return nil, err
	}
	if err := s.createInlinePushConfig(ctx, userID, slug, resp.RunID, params); err != nil {
		return nil, err
	}
	return applyProtocolContinuation(taskFromRun(resp, params.Message.ContextID, nil, nil), continuation), nil
}

// StartProtocolMessage starts an A2A message/stream request and returns the initial Task.
func (s *Service) StartProtocolMessage(ctx context.Context, userID uuid.UUID, slug string, params *A2AMessageSendParams) (*A2ATask, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return nil, httpx.BadRequest("缺少 Agent slug")
	}
	if params == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	agent, err := s.queries.GetAgentBySlug(ctx, slug)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Msg("a2a.StartProtocolMessage: GetAgentBySlug")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	normalizeProtocolMessageContext(params)
	if err := validateA2AMessageSendParams(params); err != nil {
		return nil, err
	}
	continuation, err := s.prepareProtocolContinuation(ctx, userID, agent.Slug, params)
	if err != nil {
		return nil, err
	}
	input, err := inputFromA2AMessage(params.Message)
	if err != nil {
		return nil, err
	}
	resp, err := s.runtime.StartRun(ctx, userID, &runtime.RunRequest{
		AgentID:    agent.ID.String(),
		Input:      input,
		Metadata:   protocolMetadata(params),
		A2AContext: protocolRunA2AContext(params),
	}, "api")
	if err != nil {
		return nil, err
	}
	if err := s.createInlinePushConfig(ctx, userID, slug, resp.RunID, params); err != nil {
		return nil, err
	}
	return applyProtocolContinuation(taskFromRun(resp, params.Message.ContextID, nil, nil), continuation), nil
}

func (s *Service) createInlinePushConfig(ctx context.Context, userID uuid.UUID, slug, taskID string, params *A2AMessageSendParams) error {
	if params == nil || params.Configuration == nil {
		return nil
	}
	cfg := params.Configuration.PushNotificationConfig
	if cfg == nil && params.Configuration.TaskPushNotificationConfig != nil {
		cfg = pushConfigFromTaskPushConfig(params.Configuration.TaskPushNotificationConfig)
	}
	if cfg == nil {
		return nil
	}
	_, err := s.SetPushNotificationConfig(ctx, userID, slug, &A2ATaskPushConfigParams{
		ID:                     taskID,
		PushNotificationConfig: *cfg,
	})
	return err
}

func shouldReturnA2AMessageImmediately(params *A2AMessageSendParams) bool {
	if params == nil || params.Configuration == nil {
		return false
	}
	if params.Configuration.ReturnImmediately != nil {
		return *params.Configuration.ReturnImmediately
	}
	if params.Configuration.Blocking != nil {
		// A2A uses returnImmediately for async "accept and return pending"
		// semantics, while blocking=true asks the server to wait synchronously.
		return !*params.Configuration.Blocking
	}
	return false
}

func normalizeProtocolMessageContext(params *A2AMessageSendParams) {
	if params == nil {
		return
	}
	params.Message.ContextID = strings.TrimSpace(params.Message.ContextID)
	params.Message.TaskID = strings.TrimSpace(params.Message.TaskID)
	if params.Message.ContextID == "" && params.Message.TaskID == "" {
		params.Message.ContextID = "ctx-" + uuid.NewString()
	}
	params.Message.MessageID = strings.TrimSpace(params.Message.MessageID)
}

func (s *Service) prepareProtocolContinuation(ctx context.Context, userID uuid.UUID, slug string, params *A2AMessageSendParams) (*protocolContinuation, error) {
	if params == nil {
		return nil, nil
	}
	taskID := strings.TrimSpace(params.Message.TaskID)
	if taskID == "" {
		return nil, nil
	}
	task, err := s.GetProtocolTask(ctx, userID, slug, taskID, nil)
	if err != nil {
		return nil, err
	}
	if isTerminalA2ATaskState(task.Status.State) {
		return nil, a2aUnsupportedOperation("终态任务不接受新的 A2A message")
	}
	contextID := strings.TrimSpace(params.Message.ContextID)
	if contextID != "" && task.ContextID != "" && contextID != task.ContextID {
		return nil, a2aUnsupportedOperation("message.contextId 与 taskId 所属 context 不一致")
	}
	if contextID == "" {
		params.Message.ContextID = task.ContextID
	}
	return &protocolContinuation{TaskID: taskID, ContextID: task.ContextID}, nil
}

func applyProtocolContinuation(task *A2ATask, continuation *protocolContinuation) *A2ATask {
	if task == nil || continuation == nil {
		return task
	}
	if continuation.TaskID != "" {
		task.ID = continuation.TaskID
	}
	if continuation.ContextID != "" {
		task.ContextID = continuation.ContextID
	}
	if task.Metadata == nil {
		task.Metadata = map[string]interface{}{}
	}
	openlinker, _ := task.Metadata["openlinker"].(map[string]interface{})
	if openlinker == nil {
		openlinker = map[string]interface{}{}
		task.Metadata["openlinker"] = openlinker
	}
	openlinker["protocol_task_id"] = task.ID
	return task
}

func protocolRunA2AContext(params *A2AMessageSendParams) *runtime.RunA2AContextRequest {
	if params == nil {
		return nil
	}
	traceID := protocolTraceID(params)
	if traceID == "" {
		traceID = params.Message.ContextID
	}
	return &runtime.RunA2AContextRequest{
		ProtocolContextID: params.Message.ContextID,
		ProtocolTaskID:    params.Message.TaskID,
		ParentTaskID:      params.Message.TaskID,
		RootContextID:     params.Message.ContextID,
		TraceID:           traceID,
		ReferenceTaskIDs:  normalizeProtocolReferenceTaskIDs(params.Message.ReferenceTaskIDs),
		Source:            "a2a_protocol",
	}
}

func protocolTraceID(params *A2AMessageSendParams) string {
	if params == nil {
		return ""
	}
	for _, source := range []map[string]interface{}{params.Metadata, params.Message.Metadata} {
		for _, key := range []string{"trace_id", "traceId", "trace"} {
			if value, ok := source[key].(string); ok && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

func normalizeProtocolReferenceTaskIDs(raw []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

var a2aSupportedOutputModes = map[string]struct{}{
	"application/json": {},
	"text/plain":       {},
	"text/markdown":    {},
}

var a2aSupportedInputMediaTypes = map[string]struct{}{
	"application/json":         {},
	"application/octet-stream": {},
	"application/pdf":          {},
	"image/gif":                {},
	"image/jpeg":               {},
	"image/png":                {},
	"text/csv":                 {},
	"text/markdown":            {},
	"text/plain":               {},
}

func validateA2AMessageSendParams(params *A2AMessageSendParams) error {
	if params == nil {
		return httpx.BadRequest("请求体不能为空")
	}
	if err := validateA2AAcceptedOutputModes(params.Configuration); err != nil {
		return err
	}
	return validateA2AMessagePartMediaTypes(params.Message)
}

func validateA2AAcceptedOutputModes(cfg *A2ASendConfiguration) error {
	if cfg == nil || len(cfg.AcceptedOutputModes) == 0 {
		return nil
	}
	for _, raw := range cfg.AcceptedOutputModes {
		mode := normalizeA2AMediaType(raw)
		if _, ok := a2aSupportedOutputModes[mode]; ok {
			return nil
		}
	}
	return a2aContentTypeNotSupported("acceptedOutputModes 与服务端支持的输出类型不匹配", map[string]string{
		"accepted_output_modes":  strings.Join(cfg.AcceptedOutputModes, ","),
		"supported_output_modes": strings.Join(mapKeys(a2aSupportedOutputModes), ","),
	})
}

func validateA2AMessagePartMediaTypes(message A2AMessage) error {
	for _, part := range message.Parts {
		mediaType := normalizeA2AMediaType(a2aPartMediaType(part))
		if mediaType == "" {
			continue
		}
		if _, ok := a2aSupportedInputMediaTypes[mediaType]; ok {
			continue
		}
		return a2aContentTypeNotSupported("A2A message part mediaType 不受支持: "+mediaType, map[string]string{
			"media_type":            mediaType,
			"supported_input_modes": strings.Join(mapKeys(a2aSupportedInputMediaTypes), ","),
		})
	}
	return nil
}

func a2aPartMediaType(part map[string]interface{}) string {
	if mediaType := firstPartString(part, "mediaType", "mimeType"); mediaType != "" {
		return mediaType
	}
	if file, ok := part["file"].(map[string]interface{}); ok {
		return firstPartString(file, "mediaType", "mimeType")
	}
	return ""
}

func normalizeA2AMediaType(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	if idx := strings.Index(value, ";"); idx >= 0 {
		value = strings.TrimSpace(value[:idx])
	}
	return value
}

func mapKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// GetProtocolTask maps an owner-readable OpenLinker run back to the A2A Task shape.
func (s *Service) GetProtocolTask(ctx context.Context, userID uuid.UUID, slug, taskID string, historyLength *int) (*A2ATask, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return nil, httpx.BadRequest("缺少 Agent slug")
	}
	if s.pool == nil {
		if _, err := uuid.Parse(strings.TrimSpace(taskID)); err != nil {
			return nil, httpx.BadRequest("task id 不是合法 uuid")
		}
	}

	agent, err := s.queries.GetAgentBySlug(ctx, slug)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Msg("a2a.GetProtocolTask: GetAgentBySlug")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	runID, err := s.resolveProtocolTaskRunID(ctx, userID, agent.ID, taskID)
	if err != nil {
		return nil, err
	}
	runRow, err := s.queries.GetRunByID(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (runRow.UserID != userID || runRow.AgentID != agent.ID)) {
		return nil, a2aTaskNotFound("任务不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("a2a.GetProtocolTask: GetRunByID")
		return nil, httpx.Internal("查询任务失败")
	}

	resp, err := s.runtime.GetRun(ctx, userID, runID)
	if err != nil {
		return nil, err
	}
	artifacts, err := s.runtime.ListRunArtifacts(ctx, userID, runID)
	if err != nil {
		return nil, err
	}
	var messages []runtime.RunMessageResponse
	if historyLength != nil && *historyLength > 0 {
		messages, err = s.runtime.ListRunMessages(ctx, userID, runID)
		if err != nil {
			return nil, err
		}
		if len(messages) > *historyLength {
			messages = messages[len(messages)-*historyLength:]
		}
	}
	task := taskFromRun(resp, s.protocolContextIDForRun(ctx, runID, runRow.Input), artifacts, messages)
	task.Status.Timestamp = a2aRunStatusTimestamp(runRow)
	if historyLength != nil {
		trimA2ATaskHistory(task, *historyLength)
	}
	if protocolTaskID := s.protocolTaskIDForRun(ctx, runID); protocolTaskID != "" {
		task.ID = protocolTaskID
	}
	return task, nil
}

func (s *Service) ListProtocolTasks(ctx context.Context, userID uuid.UUID, slug string, params *A2ATaskListParams) (*A2ATaskListResponse, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return nil, httpx.BadRequest("缺少 Agent slug")
	}
	if params == nil {
		params = &A2ATaskListParams{}
	}
	agent, err := s.queries.GetAgentBySlug(ctx, slug)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Msg("a2a.ListProtocolTasks: GetAgentBySlug")
		return nil, httpx.Internal("查询 Agent 失败")
	}

	pageSize, err := normalizeA2AListTasksPageSize(params.PageSize)
	if err != nil {
		return nil, err
	}
	noCursor := true
	var cursorStartedAt time.Time
	var cursorID uuid.UUID
	if strings.TrimSpace(params.PageToken) != "" {
		cursor, err := decodeA2ATaskCursor(params.PageToken)
		if err != nil {
			return nil, err
		}
		noCursor = false
		cursorStartedAt = cursor.StartedAt
		cursorID = cursor.ID
	}
	statuses, err := runStatusesFromA2ATaskState(params.Status)
	if err != nil {
		return nil, err
	}
	noStatusFilter := len(statuses) == 0
	since, noSinceFilter, err := parseA2AStatusTimestampAfter(params.StatusTimestampAfter)
	if err != nil {
		return nil, err
	}

	queryParams := db.ListRunsByUserAndAgentParams{
		UserID:          userID,
		AgentID:         agent.ID,
		NoCursor:        noCursor,
		CursorStartedAt: cursorStartedAt,
		CursorID:        cursorID,
		NoStatusFilter:  noStatusFilter,
		Statuses:        statuses,
		NoSinceFilter:   noSinceFilter,
		Since:           since,
		ContextID:       strings.TrimSpace(params.ContextID),
		Limit:           pageSize + 1,
	}
	rows, err := s.queries.ListRunsByUserAndAgent(ctx, queryParams)
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Str("user_id", userID.String()).Msg("a2a.ListProtocolTasks: ListRunsByUserAndAgent")
		return nil, httpx.Internal("查询 A2A 任务失败")
	}
	total, err := s.queries.CountRunsByUserAndAgent(ctx, db.CountRunsByUserAndAgentParams{
		UserID:         userID,
		AgentID:        agent.ID,
		NoStatusFilter: noStatusFilter,
		Statuses:       statuses,
		NoSinceFilter:  noSinceFilter,
		Since:          since,
		ContextID:      strings.TrimSpace(params.ContextID),
	})
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Str("user_id", userID.String()).Msg("a2a.ListProtocolTasks: CountRunsByUserAndAgent")
		return nil, httpx.Internal("统计 A2A 任务失败")
	}

	nextPageToken := ""
	if len(rows) > int(pageSize) {
		next := rows[int(pageSize)-1]
		nextPageToken = encodeA2ATaskCursor(next.StartedAt, next.ID)
		rows = rows[:int(pageSize)]
	}
	includeArtifacts := params.IncludeArtifacts != nil && *params.IncludeArtifacts
	tasks := make([]A2ATask, 0, len(rows))
	for _, row := range rows {
		var artifacts []runtime.RunArtifactResponse
		if includeArtifacts {
			artifacts, err = s.runtime.ListRunArtifacts(ctx, userID, row.ID)
			if err != nil {
				return nil, err
			}
		}
		var messages []runtime.RunMessageResponse
		if params.HistoryLength != nil && *params.HistoryLength > 0 {
			messages, err = s.runtime.ListRunMessages(ctx, userID, row.ID)
			if err != nil {
				return nil, err
			}
			if len(messages) > *params.HistoryLength {
				messages = messages[len(messages)-*params.HistoryLength:]
			}
		}
		task := taskFromDBRun(row, includeArtifacts, artifacts, messages)
		if params.HistoryLength != nil {
			trimA2ATaskHistory(task, *params.HistoryLength)
		}
		if contextID := s.protocolContextIDForRun(ctx, row.ID, row.Input); contextID != "" {
			task.ContextID = contextID
		}
		tasks = append(tasks, *task)
	}
	return &A2ATaskListResponse{
		Tasks:         tasks,
		NextPageToken: nextPageToken,
		PageSize:      pageSize,
		TotalSize:     total,
	}, nil
}

// CancelProtocolTask maps A2A tasks/cancel onto a real OpenLinker run cancellation.
func (s *Service) CancelProtocolTask(ctx context.Context, userID uuid.UUID, slug, taskID string) (*A2ATask, error) {
	current, err := s.GetProtocolTask(ctx, userID, slug, taskID, nil)
	if err != nil {
		return nil, err
	}
	if current.Status.State == a2aTaskStateInputReq || current.Status.State == a2aTaskStateAuthReq {
		current.Status.State = a2aTaskStateCanceled
		current.Status.Message = &A2AMessage{Kind: "message", Role: "agent", Parts: []map[string]interface{}{{"kind": "text", "text": "OpenLinker A2A task canceled"}}}
		if current.Metadata == nil {
			current.Metadata = map[string]interface{}{}
		}
		openlinker, _ := current.Metadata["openlinker"].(map[string]interface{})
		if openlinker == nil {
			openlinker = map[string]interface{}{}
			current.Metadata["openlinker"] = openlinker
		}
		openlinker["a2a_projected_state"] = a2aTaskStateCanceled
		return current, nil
	}
	runID, contextID, err := s.ensureProtocolRunContext(ctx, userID, slug, taskID)
	if err != nil {
		return nil, err
	}
	resp, err := s.runtime.CancelRun(ctx, userID, runID)
	if err != nil {
		var he *httpx.HTTPError
		if errors.As(err, &he) && he.Status == http.StatusConflict {
			return nil, a2aTaskNotCancelable(he.Message)
		}
		return nil, err
	}
	return taskFromRun(resp, contextID, nil, nil), nil
}

func (s *Service) ListProtocolTaskEvents(ctx context.Context, userID uuid.UUID, slug, taskID string, afterSequence int32) ([]interface{}, bool, int32, error) {
	runID, contextID, err := s.ensureProtocolRunContext(ctx, userID, slug, taskID)
	if err != nil {
		return nil, false, afterSequence, err
	}
	events, err := s.runtime.ListRunEvents(ctx, userID, runID, afterSequence, 50)
	if err != nil {
		return nil, false, afterSequence, err
	}
	out := make([]interface{}, 0, len(events))
	terminal := false
	nextSequence := afterSequence
	for _, event := range events {
		mapped := streamEventFromRunEvent(taskID, contextID, event)
		if mapped != nil {
			out = append(out, mapped)
		}
		nextSequence = event.Sequence
		if isTerminalRunEvent(event.EventType) {
			terminal = true
		}
	}
	return out, terminal, nextSequence, nil
}

func (s *Service) ensureProtocolRun(ctx context.Context, userID uuid.UUID, slug, taskID string) (uuid.UUID, error) {
	runID, _, err := s.ensureProtocolRunContext(ctx, userID, slug, taskID)
	return runID, err
}

func (s *Service) ensureProtocolRunContext(ctx context.Context, userID uuid.UUID, slug, taskID string) (uuid.UUID, string, error) {
	agent, err := s.queries.GetAgentBySlug(ctx, strings.TrimSpace(slug))
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, "", httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Msg("a2a.ensureProtocolRun: GetAgentBySlug")
		return uuid.Nil, "", httpx.Internal("查询 Agent 失败")
	}
	runID, err := s.resolveProtocolTaskRunID(ctx, userID, agent.ID, taskID)
	if err != nil {
		return uuid.Nil, "", err
	}
	runRow, err := s.queries.GetRunByID(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (runRow.UserID != userID || runRow.AgentID != agent.ID)) {
		return uuid.Nil, "", a2aTaskNotFound("任务不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("a2a.ensureProtocolRun: GetRunByID")
		return uuid.Nil, "", httpx.Internal("查询任务失败")
	}
	return runID, s.protocolContextIDForRun(ctx, runID, runRow.Input), nil
}

func (s *Service) resolveProtocolTaskRunID(ctx context.Context, userID, agentID uuid.UUID, taskID string) (uuid.UUID, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return uuid.Nil, httpx.BadRequest("缺少 task id")
	}
	if s.pool != nil {
		var runID uuid.UUID
		err := s.pool.QueryRow(ctx, `
SELECT run_id
FROM a2a_context_mappings
WHERE user_id = $1 AND agent_id = $2 AND protocol_task_id = $3
ORDER BY created_at DESC, run_id DESC
LIMIT 1
`, userID, agentID, taskID).Scan(&runID)
		if err == nil {
			return runID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Error().Err(err).Str("task_id", taskID).Msg("a2a.resolveProtocolTaskRunID")
			return uuid.Nil, httpx.Internal("查询 A2A task 映射失败")
		}
	}
	runID, err := uuid.Parse(taskID)
	if err != nil {
		return uuid.Nil, a2aTaskNotFound("任务不存在")
	}
	return runID, nil
}

func (s *Service) protocolContextIDForRun(ctx context.Context, runID uuid.UUID, input []byte) string {
	mapping, err := s.queries.GetA2AContextMappingByRun(ctx, runID)
	if err == nil && strings.TrimSpace(mapping.ProtocolContextID) != "" {
		return strings.TrimSpace(mapping.ProtocolContextID)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("a2a.protocolContextIDForRun")
	}
	return a2aContextIDFromRunInput(input)
}

func (s *Service) protocolTaskIDForRun(ctx context.Context, runID uuid.UUID) string {
	mapping, err := s.queries.GetA2AContextMappingByRun(ctx, runID)
	if err == nil && strings.TrimSpace(mapping.ProtocolTaskID) != "" {
		return strings.TrimSpace(mapping.ProtocolTaskID)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		log.Warn().Err(err).Str("run_id", runID.String()).Msg("a2a.protocolTaskIDForRun")
	}
	return ""
}

func normalizeA2AListTasksPageSize(raw *int) (int32, error) {
	if raw == nil || *raw == 0 {
		return defaultA2AListTasksPageSize, nil
	}
	if *raw < 1 {
		return 0, httpx.BadRequest("pageSize 必须大于 0")
	}
	if *raw > maxA2AListTasksPageSize {
		return maxA2AListTasksPageSize, nil
	}
	converted, ok := intToInt32InRange(*raw)
	if !ok {
		return 0, httpx.BadRequest("pageSize 必须大于 0")
	}
	return converted, nil
}

func decodeA2ATaskCursor(raw string) (*struct {
	StartedAt time.Time
	ID        uuid.UUID
}, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return nil, httpx.BadRequest("pageToken 不是合法游标")
	}
	var cursor a2aTaskCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return nil, httpx.BadRequest("pageToken 不是合法游标")
	}
	startedAt, err := time.Parse(time.RFC3339Nano, cursor.StartedAt)
	if err != nil {
		return nil, httpx.BadRequest("pageToken 不是合法游标")
	}
	id, err := uuid.Parse(cursor.ID)
	if err != nil {
		return nil, httpx.BadRequest("pageToken 不是合法游标")
	}
	return &struct {
		StartedAt time.Time
		ID        uuid.UUID
	}{StartedAt: startedAt, ID: id}, nil
}

func encodeA2ATaskCursor(startedAt time.Time, id uuid.UUID) string {
	raw, err := json.Marshal(a2aTaskCursor{
		StartedAt: startedAt.UTC().Format(time.RFC3339Nano),
		ID:        id.String(),
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func runStatusesFromA2ATaskState(raw string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "task_state_unspecified", "unspecified", "unknown":
		return nil, nil
	case "task_state_submitted", "submitted", a2aTaskStateWorking, "task_state_working":
		return []string{"running"}, nil
	case "task_state_completed", a2aTaskStateCompleted:
		return []string{"success"}, nil
	case "task_state_failed", a2aTaskStateFailed:
		return []string{"failed", "timeout"}, nil
	case "task_state_canceled", a2aTaskStateCanceled:
		return []string{"canceled"}, nil
	case "task_state_rejected", "rejected", "task_state_input_required", "input-required", "input_required", "task_state_auth_required", "auth-required", "auth_required":
		return []string{"__none__"}, nil
	default:
		return nil, httpx.BadRequest("不支持的 task status: " + raw)
	}
}

func parseA2AStatusTimestampAfter(raw string) (time.Time, bool, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, true, nil
	}
	value, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		value, err = time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	}
	if err != nil {
		return time.Time{}, true, httpx.BadRequest("statusTimestampAfter 必须是 ISO 8601/RFC3339 时间")
	}
	return value, false, nil
}

func taskFromDBRun(run db.Run, includeArtifacts bool, artifacts []runtime.RunArtifactResponse, messages []runtime.RunMessageResponse) *A2ATask {
	resp := &runtime.RunResponse{
		RunID:     run.ID.String(),
		Status:    run.Status,
		CostCents: run.CostCents,
		Source:    run.Source,
	}
	if run.DurationMs != nil {
		resp.DurationMs = *run.DurationMs
	}
	if run.ErrorCode != nil {
		resp.ErrorCode = *run.ErrorCode
	}
	if run.ErrorMessage != nil {
		resp.ErrorMsg = *run.ErrorMessage
	}
	if includeArtifacts && run.Status == "success" && len(run.Output) > 0 {
		var out map[string]interface{}
		if err := json.Unmarshal(run.Output, &out); err == nil {
			resp.Output = out
		}
	}
	task := taskFromRun(resp, a2aContextIDFromRunInput(run.Input), artifacts, messages)
	statusAt := run.StartedAt
	if run.FinishedAt != nil {
		statusAt = *run.FinishedAt
	}
	task.Status.Timestamp = statusAt.UTC().Format(time.RFC3339)
	return task
}

func a2aRunStatusTimestamp(run db.Run) string {
	statusAt := run.StartedAt
	if run.FinishedAt != nil {
		statusAt = *run.FinishedAt
	}
	return statusAt.UTC().Format(time.RFC3339)
}

func a2aContextIDFromRunInput(input []byte) string {
	if len(input) == 0 {
		return ""
	}
	var body map[string]interface{}
	if err := json.Unmarshal(input, &body); err != nil {
		return ""
	}
	for _, key := range []string{"a2a_context_id", "contextId", "context_id"} {
		if value, ok := body[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func inputFromA2AMessage(message A2AMessage) (map[string]interface{}, error) {
	if len(message.Parts) == 0 {
		return nil, httpx.Unprocessable("A2A message.parts 不能为空")
	}

	textParts := make([]string, 0)
	dataParts := make([]interface{}, 0)
	fileParts := make([]interface{}, 0)
	unknownParts := make([]map[string]interface{}, 0)
	for _, part := range message.Parts {
		kind := partKind(part)
		switch kind {
		case "text":
			text, ok := part["text"].(string)
			if !ok || strings.TrimSpace(text) == "" {
				return nil, httpx.Unprocessable("A2A text part 缺少 text")
			}
			textParts = append(textParts, text)
		case "data":
			data, ok := part["data"]
			if !ok {
				return nil, httpx.Unprocessable("A2A data part 缺少 data")
			}
			dataParts = append(dataParts, data)
		case "file":
			file, err := filePartInput(part)
			if err != nil {
				return nil, err
			}
			fileParts = append(fileParts, file)
		default:
			unknownParts = append(unknownParts, part)
		}
	}

	if len(textParts) == 0 && len(dataParts) == 1 && len(fileParts) == 0 && len(unknownParts) == 0 {
		var input map[string]interface{}
		if data, ok := dataParts[0].(map[string]interface{}); ok {
			input = copyMap(data)
		} else {
			input = map[string]interface{}{"data": dataParts[0]}
		}
		attachA2AInputIDs(input, message)
		return input, nil
	}

	input := map[string]interface{}{}
	if len(textParts) > 0 {
		text := strings.Join(textParts, "\n")
		input["message"] = text
		input["text"] = text
	}
	if len(dataParts) > 0 {
		input["data_parts"] = dataParts
	}
	if len(fileParts) > 0 {
		input["files"] = fileParts
	}
	if len(unknownParts) > 0 {
		input["parts"] = unknownParts
	}
	attachA2AInputIDs(input, message)
	if len(input) == 0 {
		return nil, httpx.Unprocessable("A2A message 没有可执行输入")
	}
	return input, nil
}

func attachA2AInputIDs(input map[string]interface{}, message A2AMessage) {
	if message.MessageID != "" {
		input["a2a_message_id"] = message.MessageID
	}
	if message.ContextID != "" {
		input["a2a_context_id"] = message.ContextID
	}
	if message.TaskID != "" {
		input["a2a_task_id"] = message.TaskID
	}
	if refs := normalizeProtocolReferenceTaskIDs(message.ReferenceTaskIDs); len(refs) > 0 {
		input["a2a_reference_task_ids"] = refs
	}
}

func partKind(part map[string]interface{}) string {
	if raw, ok := part["kind"].(string); ok && raw != "" {
		return strings.ToLower(raw)
	}
	if raw, ok := part["type"].(string); ok && raw != "" {
		return strings.ToLower(raw)
	}
	if _, ok := part["text"]; ok {
		return "text"
	}
	if _, ok := part["data"]; ok {
		return "data"
	}
	if _, ok := part["file"]; ok {
		return "file"
	}
	for _, key := range []string{"url", "uri", "raw", "bytes", "fileWithBytes", "filename", "fileName", "mediaType", "mimeType"} {
		if _, ok := part[key]; ok {
			return "file"
		}
	}
	return ""
}

func filePartInput(part map[string]interface{}) (map[string]interface{}, error) {
	source := part
	if legacyFile, ok := part["file"].(map[string]interface{}); ok {
		source = legacyFile
	}
	file := map[string]interface{}{}
	if uri := firstPartString(source, "url", "uri"); uri != "" {
		if err := validateA2AFileURI(uri); err != nil {
			return nil, err
		}
		file["uri"] = uri
	}
	if raw, ok := source["raw"]; ok {
		file["raw"] = raw
	}
	if bytes, ok := source["bytes"]; ok {
		file["bytes"] = bytes
	} else if bytes, ok := source["fileWithBytes"]; ok {
		file["bytes"] = bytes
	}
	if name := firstPartString(source, "filename", "fileName", "name"); name != "" {
		file["name"] = name
	}
	if mediaType := firstPartString(source, "mediaType", "mimeType"); mediaType != "" {
		file["mimeType"] = mediaType
	}
	for _, key := range []string{"sha256", "sizeBytes"} {
		if value, ok := source[key]; ok {
			file[key] = value
		}
	}
	if metadata, ok := source["metadata"].(map[string]interface{}); ok && len(metadata) > 0 {
		file["metadata"] = metadata
	}
	if _, hasURI := file["uri"]; !hasURI {
		_, hasRaw := file["raw"]
		_, hasBytes := file["bytes"]
		if !hasRaw && !hasBytes {
			return nil, httpx.Unprocessable("A2A file part 缺少 url/raw/bytes")
		}
	}
	return file, nil
}

func validateA2AFileURI(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return httpx.Unprocessable("A2A file part url 不是合法 URL")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return httpx.Unprocessable("A2A file part url 仅支持 http/https")
	}
	return nil
}

func protocolMetadata(params *A2AMessageSendParams) map[string]interface{} {
	metadata := map[string]interface{}{}
	for k, v := range params.Metadata {
		metadata[k] = v
	}
	for k, v := range params.Message.Metadata {
		metadata[k] = v
	}
	metadata["source"] = "a2a"
	metadata["a2a"] = map[string]interface{}{
		"protocol":           "jsonrpc-http",
		"method":             "message/send",
		"message_id":         params.Message.MessageID,
		"context_id":         params.Message.ContextID,
		"task_id":            params.Message.TaskID,
		"extensions":         normalizeProtocolReferenceTaskIDs(params.Message.Extensions),
		"reference_task_ids": normalizeProtocolReferenceTaskIDs(params.Message.ReferenceTaskIDs),
	}
	return metadata
}

func taskFromRun(resp *runtime.RunResponse, contextID string, artifacts []runtime.RunArtifactResponse, messages []runtime.RunMessageResponse) *A2ATask {
	if resp != nil && resp.A2AContext != nil && resp.A2AContext.ProtocolContextID != "" {
		contextID = resp.A2AContext.ProtocolContextID
	}
	if contextID == "" {
		contextID = resp.RunID
	}
	task := &A2ATask{
		Kind:      "task",
		ID:        resp.RunID,
		ContextID: contextID,
		Status: A2ATaskStatus{
			State:     stateFromRunStatus(resp.Status),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		},
		Metadata: map[string]interface{}{
			"openlinker": map[string]interface{}{
				"run_id":       resp.RunID,
				"status":       resp.Status,
				"source":       resp.Source,
				"duration_ms":  resp.DurationMs,
				"cost_cents":   resp.CostCents,
				"billing_mode": resp.BillingMode,
			},
		},
	}
	projection := a2aOutputProjection(resp.Output)
	hasProjection := projection.HasValue
	if projection.State != "" {
		task.Status.State = projection.State
		task.Metadata["openlinker"].(map[string]interface{})["a2a_projected_state"] = projection.State
	}
	if resp.ParentRunID != "" {
		task.Metadata["openlinker"].(map[string]interface{})["parent_run_id"] = resp.ParentRunID
	}
	if resp.CallerAgentID != "" {
		task.Metadata["openlinker"].(map[string]interface{})["caller_agent_id"] = resp.CallerAgentID
	}
	if resp.A2AContext != nil {
		task.Metadata["openlinker"].(map[string]interface{})["a2a_context"] = resp.A2AContext
	}

	if projection.StatusMessage != nil {
		task.Status.Message = projection.StatusMessage
	} else if msg := statusMessageFromRun(resp); msg != nil {
		task.Status.Message = msg
	}
	if projection.ResponseMessage != nil {
		task.ResponseMessage = projection.ResponseMessage
	}
	if len(projection.History) > 0 {
		task.History = append(task.History, projection.History...)
	}
	if len(projection.Artifacts) > 0 {
		task.Artifacts = append(task.Artifacts, projection.Artifacts...)
	} else if resp.Status == "success" && resp.Output != nil && !hasProjection {
		task.Artifacts = append(task.Artifacts, outputArtifact(resp.Output))
	}
	for _, artifact := range artifacts {
		task.Artifacts = append(task.Artifacts, artifactFromRunArtifact(artifact))
	}
	for _, message := range messages {
		task.History = append(task.History, messageFromRunMessage(message))
	}
	return task
}

func trimA2ATaskHistory(task *A2ATask, limit int) {
	if task == nil || limit <= 0 || len(task.History) <= limit {
		return
	}
	task.History = task.History[len(task.History)-limit:]
}

func stateFromRunStatus(status string) string {
	switch status {
	case "running":
		return a2aTaskStateWorking
	case "success":
		return a2aTaskStateCompleted
	case "canceled":
		return a2aTaskStateCanceled
	case "failed", "timeout":
		return a2aTaskStateFailed
	case "":
		return a2aTaskStateSubmitted
	default:
		return a2aTaskStateSubmitted
	}
}

func isTerminalA2ATaskState(state string) bool {
	switch normalizeProjectedA2ATaskState(state) {
	case a2aTaskStateCompleted, a2aTaskStateCanceled, a2aTaskStateFailed, a2aTaskStateRejected:
		return true
	default:
		return false
	}
}

func a2aTaskStateOrder(state string) int {
	switch normalizeProjectedA2ATaskState(state) {
	case a2aTaskStateSubmitted:
		return 0
	case a2aTaskStateWorking, a2aTaskStateInputReq, a2aTaskStateAuthReq:
		return 1
	case a2aTaskStateCompleted, a2aTaskStateCanceled, a2aTaskStateFailed, a2aTaskStateRejected:
		return 2
	default:
		return -1
	}
}

func a2aStreamItemStateOrder(item interface{}) int {
	switch typed := item.(type) {
	case *A2ATask:
		return a2aTaskStateOrder(typed.Status.State)
	case A2ATask:
		return a2aTaskStateOrder(typed.Status.State)
	case *A2ATaskStatusUpdateEvent:
		return a2aTaskStateOrder(typed.Status.State)
	case A2ATaskStatusUpdateEvent:
		return a2aTaskStateOrder(typed.Status.State)
	default:
		return -1
	}
}

func streamEventFromRunEvent(taskID, contextID string, event runtime.RunEventResponse) interface{} {
	if contextID == "" {
		contextID = taskID
	}
	switch event.EventType {
	case "run.artifact.delta":
		return artifactUpdateFromRunEvent(taskID, contextID, event)
	case "run.created", "run.started", "run.dispatch.pending", "run.dispatch.claimed", "run.message.delta", "run.completed", "run.failed", "run.canceled":
		return statusUpdateFromRunEvent(taskID, contextID, event)
	default:
		return statusUpdateFromRunEvent(taskID, contextID, event)
	}
}

func statusUpdateFromRunEvent(taskID, contextID string, event runtime.RunEventResponse) *A2ATaskStatusUpdateEvent {
	state := a2aTaskStateWorking
	switch event.EventType {
	case "run.created":
		state = a2aTaskStateSubmitted
	case "run.completed":
		state = a2aTaskStateCompleted
	case "run.canceled":
		state = a2aTaskStateCanceled
	case "run.failed":
		state = a2aTaskStateFailed
	}
	status := A2ATaskStatus{
		State:     state,
		Timestamp: event.CreatedAt.UTC().Format(time.RFC3339),
	}
	if msg := messageFromRunEvent(event); msg != nil {
		status.Message = msg
	}
	return &A2ATaskStatusUpdateEvent{
		Kind:      "status-update",
		TaskID:    taskID,
		ContextID: contextID,
		Status:    status,
		Final:     isTerminalRunEvent(event.EventType),
		Metadata: map[string]interface{}{
			"openlinker_event_type": event.EventType,
			"openlinker_sequence":   event.Sequence,
			"openlinker_event_id":   event.EventID,
			"payload":               event.Payload,
		},
	}
}

func messageFromRunEvent(event runtime.RunEventResponse) *A2AMessage {
	text := ""
	if raw, ok := event.Payload["text"].(string); ok {
		text = raw
	}
	if text == "" {
		if raw, ok := event.Payload["message"].(string); ok {
			text = raw
		}
	}
	if text == "" {
		switch event.EventType {
		case "run.created":
			text = "OpenLinker task created"
		case "run.started":
			text = "OpenLinker task started"
		case "run.dispatch.pending":
			text = "OpenLinker task is waiting for a runtime worker"
		case "run.dispatch.claimed":
			text = "OpenLinker task was claimed by a runtime worker"
		case "run.completed":
			text = "OpenLinker task completed"
		case "run.failed":
			text = "OpenLinker task failed"
		case "run.canceled":
			text = "OpenLinker task canceled"
		}
	}
	if text == "" {
		return nil
	}
	messageID := strings.TrimSpace(event.EventID)
	if messageID == "" {
		messageID = "event-" + strconv.Itoa(int(event.Sequence))
	}
	return &A2AMessage{Kind: "message", MessageID: messageID, Role: "agent", Parts: []map[string]interface{}{{"kind": "text", "text": text}}}
}

func artifactUpdateFromRunEvent(taskID, contextID string, event runtime.RunEventResponse) *A2ATaskArtifactUpdateEvent {
	artifactID := payloadString(event.Payload, "artifact_id")
	if artifactID == "" {
		artifactID = payloadString(event.Payload, "source_artifact_id")
	}
	if artifactID == "" {
		artifactID = "artifact-" + strconv.Itoa(int(event.Sequence))
	}
	title := payloadString(event.Payload, "title")
	artifactType := payloadString(event.Payload, "artifact_type")
	if artifactType == "" {
		artifactType = payloadString(event.Payload, "type")
	}
	part := artifactPartFromPayload(event.Payload)
	appendChunk, _ := event.Payload["append"].(bool)
	lastChunk, _ := event.Payload["last_chunk"].(bool)
	if raw, ok := event.Payload["lastChunk"].(bool); ok {
		lastChunk = raw
	}
	return &A2ATaskArtifactUpdateEvent{
		Kind:      "artifact-update",
		TaskID:    taskID,
		ContextID: contextID,
		Artifact: A2AArtifact{
			ArtifactID: artifactID,
			Name:       title,
			Parts:      []map[string]interface{}{part},
			Metadata: map[string]interface{}{
				"openlinker_artifact_type": artifactType,
				"openlinker_sequence":      event.Sequence,
			},
		},
		Append:    appendChunk,
		LastChunk: lastChunk,
		Metadata: map[string]interface{}{
			"openlinker_event_type": event.EventType,
			"openlinker_event_id":   event.EventID,
			"payload":               event.Payload,
		},
	}
}

func payloadString(payload map[string]interface{}, key string) string {
	if payload == nil {
		return ""
	}
	value, _ := payload[key].(string)
	return strings.TrimSpace(value)
}

func isTerminalRunEvent(eventType string) bool {
	switch eventType {
	case "run.completed", "run.failed", "run.canceled":
		return true
	default:
		return false
	}
}

func statusMessageFromRun(resp *runtime.RunResponse) *A2AMessage {
	messageID := "status-" + strings.TrimSpace(resp.RunID) + "-" + strings.TrimSpace(resp.Status)
	switch resp.Status {
	case "success":
		parts := []map[string]interface{}{}
		if text := summaryText(resp.Output); text != "" {
			parts = append(parts, map[string]interface{}{"kind": "text", "text": text})
		}
		if resp.Output != nil {
			parts = append(parts, map[string]interface{}{"kind": "data", "data": resp.Output})
		}
		return &A2AMessage{Kind: "message", MessageID: messageID, Role: "agent", Parts: parts}
	case "failed", "timeout":
		text := strings.TrimSpace(resp.ErrorMsg)
		if text == "" {
			text = resp.ErrorCode
		}
		if text == "" {
			text = "OpenLinker run failed"
		}
		return &A2AMessage{Kind: "message", MessageID: messageID, Role: "agent", Parts: []map[string]interface{}{{"kind": "text", "text": text}}}
	case "canceled":
		text := strings.TrimSpace(resp.ErrorMsg)
		if text == "" {
			text = "OpenLinker task canceled"
		}
		return &A2AMessage{Kind: "message", MessageID: messageID, Role: "agent", Parts: []map[string]interface{}{{"kind": "text", "text": text}}}
	case "running":
		return &A2AMessage{Kind: "message", MessageID: messageID, Role: "agent", Parts: []map[string]interface{}{{"kind": "text", "text": "OpenLinker run is still running"}}}
	default:
		return nil
	}
}

func summaryText(output map[string]interface{}) string {
	if output == nil {
		return ""
	}
	for _, key := range []string{"summary", "answer", "text", "message"} {
		if value, ok := output[key].(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func outputArtifact(output map[string]interface{}) A2AArtifact {
	parts := []map[string]interface{}{{"kind": "data", "data": output}}
	if text := summaryText(output); text != "" {
		parts = append([]map[string]interface{}{{"kind": "text", "text": text}}, parts...)
	}
	return A2AArtifact{
		ArtifactID: "output",
		Name:       "OpenLinker run output",
		Parts:      parts,
		Metadata:   map[string]interface{}{"source": "run.output"},
	}
}

type projectedA2AOutput struct {
	HasValue        bool
	State           string
	StatusMessage   *A2AMessage
	ResponseMessage *A2AMessage
	Artifacts       []A2AArtifact
	History         []A2AMessage
}

func a2aOutputProjection(output map[string]interface{}) projectedA2AOutput {
	source := projectedA2AMap(output)
	if len(source) == 0 {
		return projectedA2AOutput{}
	}
	out := projectedA2AOutput{
		HasValue:        true,
		State:           normalizeProjectedA2ATaskState(projectedString(source, "task_state", "taskState", "state")),
		StatusMessage:   projectedMessage(firstProjectedValue(source, "status_message", "statusMessage")),
		ResponseMessage: projectedMessage(firstProjectedValue(source, "response_message", "responseMessage", "direct_message", "directMessage")),
		Artifacts:       projectedArtifacts(firstProjectedValue(source, "artifacts", "artifact")),
		History:         projectedMessages(firstProjectedValue(source, "history", "messages")),
	}
	if out.State == "" {
		if status, ok := firstProjectedValue(source, "status").(map[string]interface{}); ok {
			out.State = normalizeProjectedA2ATaskState(projectedString(status, "state"))
			if out.StatusMessage == nil {
				out.StatusMessage = projectedMessage(firstProjectedValue(status, "message"))
			}
		}
	}
	return out
}

func projectedA2AMap(output map[string]interface{}) map[string]interface{} {
	if output == nil {
		return nil
	}
	for _, key := range []string{"a2a", "a2a_task", "a2aTask"} {
		if value, ok := output[key].(map[string]interface{}); ok {
			return value
		}
	}
	return nil
}

func normalizeProjectedA2ATaskState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "submitted", "task_state_submitted":
		return a2aTaskStateSubmitted
	case "working", "task_state_working":
		return a2aTaskStateWorking
	case "completed", "task_state_completed":
		return a2aTaskStateCompleted
	case "canceled", "cancelled", "task_state_canceled", "task_state_cancelled":
		return a2aTaskStateCanceled
	case "failed", "task_state_failed":
		return a2aTaskStateFailed
	case "rejected", "task_state_rejected":
		return a2aTaskStateRejected
	case "input-required", "input_required", "task_state_input_required":
		return a2aTaskStateInputReq
	case "auth-required", "auth_required", "task_state_auth_required":
		return a2aTaskStateAuthReq
	default:
		return strings.TrimSpace(raw)
	}
}

func projectedString(source map[string]interface{}, keys ...string) string {
	value, _ := firstProjectedValue(source, keys...).(string)
	return strings.TrimSpace(value)
}

func firstProjectedValue(source map[string]interface{}, keys ...string) interface{} {
	if source == nil {
		return nil
	}
	for _, key := range keys {
		if value, ok := source[key]; ok {
			return value
		}
	}
	return nil
}

func projectedMessage(raw interface{}) *A2AMessage {
	switch value := raw.(type) {
	case nil:
		return nil
	case string:
		text := strings.TrimSpace(value)
		if text == "" {
			return nil
		}
		return &A2AMessage{Kind: "message", Role: "agent", Parts: []map[string]interface{}{{"kind": "text", "text": text}}}
	case map[string]interface{}:
		var msg A2AMessage
		if !decodeProjectedValue(value, &msg) {
			return nil
		}
		normalizeProjectedMessage(&msg)
		return &msg
	default:
		return &A2AMessage{Kind: "message", Role: "agent", Parts: []map[string]interface{}{{"kind": "data", "data": value}}}
	}
}

func projectedMessages(raw interface{}) []A2AMessage {
	switch value := raw.(type) {
	case []interface{}:
		out := make([]A2AMessage, 0, len(value))
		for _, item := range value {
			if msg := projectedMessage(item); msg != nil {
				out = append(out, *msg)
			}
		}
		return out
	case []map[string]interface{}:
		out := make([]A2AMessage, 0, len(value))
		for _, item := range value {
			if msg := projectedMessage(item); msg != nil {
				out = append(out, *msg)
			}
		}
		return out
	default:
		if msg := projectedMessage(value); msg != nil {
			return []A2AMessage{*msg}
		}
		return nil
	}
}

func projectedArtifacts(raw interface{}) []A2AArtifact {
	values := []interface{}{}
	switch value := raw.(type) {
	case nil:
		return nil
	case []interface{}:
		values = value
	case []map[string]interface{}:
		for _, item := range value {
			values = append(values, item)
		}
	default:
		values = append(values, value)
	}
	out := make([]A2AArtifact, 0, len(values))
	for _, item := range values {
		var artifact A2AArtifact
		if !decodeProjectedValue(item, &artifact) {
			continue
		}
		if artifact.ArtifactID == "" {
			artifact.ArtifactID = "output"
		}
		out = append(out, artifact)
	}
	return out
}

func decodeProjectedValue(raw interface{}, target interface{}) bool {
	data, err := json.Marshal(raw)
	if err != nil {
		return false
	}
	return json.Unmarshal(data, target) == nil
}

func normalizeProjectedMessage(msg *A2AMessage) {
	if msg == nil {
		return
	}
	if msg.Kind == "" {
		msg.Kind = "message"
	}
	if msg.Role == "" {
		msg.Role = "agent"
	}
	if msg.MessageID == "" {
		msg.MessageID = "projected-" + uuid.NewString()
	}
}

func artifactFromRunArtifact(artifact runtime.RunArtifactResponse) A2AArtifact {
	partKind := "data"
	part := map[string]interface{}{"kind": partKind, "data": artifact.Content}
	if artifact.ArtifactType == "text" {
		part = map[string]interface{}{"kind": "text", "text": fmt.Sprint(artifact.Content)}
	} else if artifact.ArtifactType == "file" {
		part = map[string]interface{}{"kind": "file", "file": filePartFromRunArtifact(artifact)}
	}
	metadata := map[string]interface{}{
		"openlinker_artifact_type": artifact.ArtifactType,
		"visibility":               artifact.Visibility,
		"source_artifact_id":       artifact.SourceArtifactID,
		"created_at":               artifact.CreatedAt.UTC().Format(time.RFC3339),
	}
	if artifact.MimeType != "" {
		metadata["mime_type"] = artifact.MimeType
	}
	if artifact.FileSHA256 != "" {
		metadata["file_sha256"] = artifact.FileSHA256
	}
	if artifact.FileSizeBytes != nil {
		metadata["file_size_bytes"] = *artifact.FileSizeBytes
	}
	return A2AArtifact{
		ArtifactID: artifact.ID,
		Name:       artifact.Title,
		Parts:      []map[string]interface{}{part},
		Metadata:   metadata,
	}
}

func artifactPartFromPayload(payload map[string]interface{}) map[string]interface{} {
	if file, ok := payload["file"].(map[string]interface{}); ok {
		return map[string]interface{}{"kind": "file", "file": file}
	}
	if parts, ok := payload["parts"].([]interface{}); ok {
		for _, raw := range parts {
			part, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			kind := partKind(part)
			if kind == "file" {
				if file, ok := part["file"]; ok {
					return map[string]interface{}{"kind": "file", "file": file}
				}
				return map[string]interface{}{"kind": "file", "file": part}
			}
		}
	}
	if text := payloadString(payload, "text"); text != "" {
		return map[string]interface{}{"kind": "text", "text": text}
	}
	if data, ok := payload["data"]; ok {
		return map[string]interface{}{"kind": "data", "data": data}
	}
	return map[string]interface{}{"kind": "data", "data": payload}
}

func filePartFromRunArtifact(artifact runtime.RunArtifactResponse) map[string]interface{} {
	file := map[string]interface{}{}
	if artifact.FileURI != "" {
		file["uri"] = artifact.FileURI
	}
	if artifact.FileName != "" {
		file["name"] = artifact.FileName
	}
	if artifact.MimeType != "" {
		file["mimeType"] = artifact.MimeType
	}
	if artifact.FileSHA256 != "" {
		file["sha256"] = artifact.FileSHA256
	}
	if artifact.FileSizeBytes != nil {
		file["sizeBytes"] = *artifact.FileSizeBytes
	}
	if len(file) == 0 {
		file["metadata"] = artifact.Content
	}
	return file
}

func messageFromRunMessage(message runtime.RunMessageResponse) A2AMessage {
	role := message.Role
	if role == "" {
		role = "agent"
	}
	return A2AMessage{
		Kind:      "message",
		MessageID: message.ID,
		Role:      role,
		Parts:     []map[string]interface{}{{"kind": "text", "text": message.Content}},
		Metadata: map[string]interface{}{
			"openlinker": map[string]interface{}{
				"run_id":         message.RunID,
				"event_sequence": message.EventSequence,
				"payload":        message.Payload,
				"created_at":     message.CreatedAt.UTC().Format(time.RFC3339),
			},
		},
	}
}

func copyMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
