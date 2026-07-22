package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

var (
	errReplaySourceNotReplayable = errors.New("runtime replay source is not replayable")
	errReplayInputUnavailable    = errors.New("runtime replay input is unavailable")
)

// ReplayRun creates a new asynchronous Run from one owned dead-letter Run.
// The source Run, Attempt history, events, ledger and DLQ are immutable; only
// the new Run carries replay_of_run_id.
func (s *Service) ReplayRun(
	ctx context.Context,
	userID, sourceRunID uuid.UUID,
	idempotencyKey, source string,
) (*RunResponse, error) {
	if _, err := HashIdempotencyKey(idempotencyKey); err != nil {
		return nil, idempotencyHTTPError(err)
	}
	if source == "" {
		source = "web"
	}
	if source != "web" && source != "api" && source != "mcp" {
		return nil, httpx.BadRequest("source 取值非法")
	}

	original, err := s.queries.GetRunByID(ctx, sourceRunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("调用记录不存在")
		}
		return nil, httpx.Internal("查询调用记录失败")
	}
	if original.UserID != userID {
		// Preserve owner-only existence semantics even though Agent creators can
		// read ordinary Run detail.
		return nil, httpx.NotFound("调用记录不存在")
	}
	if original.RuntimeContractID != RuntimeContractID ||
		original.Status != "failed" ||
		original.DispatchState != string(RuntimeDispatchDeadLetter) {
		return nil, httpx.Conflict("只有进入死信队列的调用可以回放")
	}
	if _, err := s.queries.GetRunDeadLetterByRun(ctx, sourceRunID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.Conflict("调用缺少可验证的死信记录")
		}
		return nil, httpx.Internal("查询死信记录失败")
	}

	payload, err := s.queries.GetRunReplayPayload(ctx, sourceRunID)
	if err != nil {
		return nil, httpx.Internal("读取原调用输入失败")
	}
	input, ok := decodeReplayObject(payload.Input)
	if !ok {
		return s.replayAfterInputRetention(ctx, userID, sourceRunID, idempotencyKey, source)
	}
	metadata, ok := decodeReplayObject(payload.RequestMetadata)
	if !ok {
		return nil, httpx.Internal("原调用元数据不可读取")
	}
	var replayContext *RunA2AContextRequest
	if mapping, mappingErr := s.queries.GetA2AContextMappingByRun(ctx, sourceRunID); mappingErr == nil {
		replayContext = replayA2AContext(mapping)
	} else if !errors.Is(mappingErr, pgx.ErrNoRows) {
		return nil, httpx.Internal("读取原调用会话上下文失败")
	}

	replayID := sourceRunID
	req := &RunRequest{
		AgentID:          original.AgentID.String(),
		Input:            input,
		Metadata:         metadata,
		A2AContext:       replayContext,
		IdempotencyKey:   idempotencyKey,
		CreationProtocol: "rest",
		CreationMethod:   "runs.replay",
		CreationOptions: map[string]any{
			"replay_of_run_id": sourceRunID.String(),
		},
	}
	return s.startRunWithOptions(ctx, userID, req, source, createRunOptions{
		replayOfRunID: &replayID,
	})
}

func replayA2AContext(mapping db.A2AContextMapping) *RunA2AContextRequest {
	context := &RunA2AContextRequest{
		ProtocolContextID: mapping.ProtocolContextID,
		ProtocolTaskID:    mapping.ProtocolTaskID,
		RootContextID:     mapping.RootContextID,
		ParentContextID:   mapping.ParentContextID,
		ParentTaskID:      mapping.ParentTaskID,
		TraceID:           mapping.TraceID,
		ReferenceTaskIDs:  append([]string(nil), mapping.ReferenceTaskIDs...),
		Source:            mapping.Source,
	}
	if mapping.ParentRunID != nil {
		context.ParentRunID = mapping.ParentRunID.String()
	}
	if mapping.CallerAgentID != nil {
		context.CallerAgentID = mapping.CallerAgentID.String()
	}
	if mapping.TargetAgentID != nil {
		context.TargetAgentID = mapping.TargetAgentID.String()
	}
	return context
}

func decodeReplayObject(raw []byte) (map[string]interface{}, bool) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]interface{}
	if err := decoder.Decode(&value); err != nil || value == nil {
		return nil, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return value, true
}

func (s *Service) replayAfterInputRetention(
	ctx context.Context,
	userID, sourceRunID uuid.UUID,
	idempotencyKey, source string,
) (*RunResponse, error) {
	keyHash, err := HashIdempotencyKey(idempotencyKey)
	if err != nil {
		return nil, idempotencyHTTPError(err)
	}
	record, err := s.queries.GetRunIdempotencyRecord(ctx, db.GetRunIdempotencyRecordParams{
		UserID:             userID,
		IdempotencyKeyHash: keyHash[:],
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, replayInputUnavailableError()
	}
	if err != nil {
		return nil, httpx.Internal("查询幂等调用记录失败")
	}
	existing, err := s.queries.GetRunByID(ctx, record.ID)
	if err != nil {
		return nil, httpx.Internal("查询幂等调用记录失败")
	}
	if existing.ReplayOfRunID == nil || *existing.ReplayOfRunID != sourceRunID ||
		!strings.EqualFold(existing.Source, source) {
		return nil, idempotencyHTTPError(&IdempotencyError{Class: IdempotencyErrorKeyReused})
	}
	return s.idempotencyReplayResponse(ctx, userID, existing.ID)
}

func replayInputUnavailableError() error {
	return httpx.NewError(
		http.StatusConflict,
		httpx.ErrorCode(RuntimeErrorReplayInputUnavailable),
		"原调用输入已按保留策略清理，无法创建新的回放调用",
	)
}
