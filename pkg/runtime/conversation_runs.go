package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	conversationProjectionLimit = 64
	conversationAncestorLimit   = 64
	conversationA2ASource       = "a2a_protocol"
)

type ConversationRunListResponse struct {
	AnchorRunID                string                `json:"anchor_run_id"`
	ConversationIdentitySHA256 string                `json:"conversation_identity_sha256,omitempty"`
	Linear                     bool                  `json:"linear"`
	Revision                   string                `json:"revision"`
	Items                      []ConversationRunItem `json:"items"`
}

type ConversationRunItem struct {
	RunID                    string     `json:"run_id"`
	ParentRunID              string     `json:"parent_run_id,omitempty"`
	ConversationOrdinal      *int32     `json:"conversation_ordinal,omitempty"`
	Status                   string     `json:"status"`
	BrowserInteractionPolicy string     `json:"browser_interaction_policy,omitempty"`
	StartedAt                time.Time  `json:"started_at"`
	FinishedAt               *time.Time `json:"finished_at,omitempty"`
}

type conversationProjectionRun struct {
	Mapped          bool
	RunID           uuid.UUID
	MappingUserID   uuid.UUID
	MappingAgentID  uuid.UUID
	RunUserID       uuid.UUID
	RunAgentID      uuid.UUID
	RootContextID   string
	ParentRunID     *uuid.UUID
	ProtocolTaskID  string
	Source          string
	Status          string
	RequestMetadata []byte
	StartedAt       time.Time
	FinishedAt      *time.Time
	Depth           int32
	Cycle           bool
}

func (s *Service) GetConversationRuns(
	ctx context.Context,
	userID, anchorRunID uuid.UUID,
) (*ConversationRunListResponse, error) {
	anchorRun, err := s.queries.GetRunByID(ctx, anchorRunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("调用记录不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", anchorRunID.String()).Msg("runtime.GetConversationRuns: GetRunByID")
		return nil, httpx.Internal("查询会话运行失败")
	}
	// This endpoint is deliberately stricter than ordinary Run detail: an Agent
	// creator or administrator does not gain the caller's conversation follow
	// projection. Missing and foreign anchors remain indistinguishable.
	if anchorRun.UserID != userID {
		return nil, httpx.NotFound("调用记录不存在")
	}

	anchor := conversationProjectionRun{
		RunID:           anchorRun.ID,
		RunUserID:       anchorRun.UserID,
		RunAgentID:      anchorRun.AgentID,
		Status:          anchorRun.Status,
		RequestMetadata: anchorRun.RequestMetadata,
		StartedAt:       anchorRun.StartedAt,
		FinishedAt:      anchorRun.FinishedAt,
	}
	mapping, err := s.queries.GetA2AContextMappingByRun(ctx, anchorRunID)
	if errors.Is(err, pgx.ErrNoRows) {
		response := singleConversationRunResponse(anchor)
		return &response, nil
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", anchorRunID.String()).Msg("runtime.GetConversationRuns: GetA2AContextMappingByRun")
		return nil, httpx.Internal("查询会话运行失败")
	}
	anchor.Mapped = true
	anchor.MappingUserID = mapping.UserID
	anchor.MappingAgentID = mapping.AgentID
	anchor.RootContextID = mapping.RootContextID
	anchor.ParentRunID = mapping.ParentRunID
	anchor.ProtocolTaskID = mapping.ProtocolTaskID
	anchor.Source = mapping.Source
	if !conversationProjectionRunEligible(anchor, userID, anchorRun.AgentID, mapping.RootContextID) {
		response := singleConversationRunResponse(anchor)
		return &response, nil
	}

	forwardRows, err := s.queries.ListA2AConversationForwardRows(
		ctx,
		db.ListA2AConversationForwardRowsParams{
			RunID:    anchorRunID,
			MaxDepth: conversationProjectionLimit,
			Limit:    conversationProjectionLimit + 1,
		},
	)
	if err != nil {
		log.Error().Err(err).Str("run_id", anchorRunID.String()).Msg("runtime.GetConversationRuns: forward")
		return nil, httpx.Internal("查询会话运行失败")
	}
	ancestorRows, err := s.queries.ListA2AConversationAncestorRows(
		ctx,
		db.ListA2AConversationAncestorRowsParams{
			RunID:    anchorRunID,
			MaxDepth: conversationAncestorLimit,
			Limit:    conversationAncestorLimit + 1,
		},
	)
	if err != nil {
		log.Error().Err(err).Str("run_id", anchorRunID.String()).Msg("runtime.GetConversationRuns: ancestors")
		return nil, httpx.Internal("查询会话运行失败")
	}

	forward := make([]conversationProjectionRun, 0, len(forwardRows))
	for _, row := range forwardRows {
		forward = append(forward, conversationProjectionRun{
			Mapped: true, RunID: row.RunID, MappingUserID: row.MappingUserID,
			MappingAgentID: row.MappingAgentID, RunUserID: row.RunUserID,
			RunAgentID: row.RunAgentID, RootContextID: row.RootContextID,
			ParentRunID: row.ParentRunID, ProtocolTaskID: row.ProtocolTaskID,
			Source: row.Source, Status: row.Status, RequestMetadata: row.RequestMetadata,
			StartedAt: row.StartedAt, FinishedAt: row.FinishedAt,
			Depth: row.Depth, Cycle: row.Cycle,
		})
	}
	ancestors := make([]conversationProjectionRun, 0, len(ancestorRows))
	for _, row := range ancestorRows {
		ancestors = append(ancestors, conversationProjectionRun{
			Mapped: true, RunID: row.RunID, MappingUserID: row.MappingUserID,
			MappingAgentID: row.MappingAgentID, RunUserID: row.RunUserID,
			RunAgentID: row.RunAgentID, RootContextID: row.RootContextID,
			ParentRunID: row.ParentRunID, ProtocolTaskID: row.ProtocolTaskID,
			Source: row.Source, Depth: row.Depth, Cycle: row.Cycle,
		})
	}
	response := buildConversationRunProjection(anchor, forward, ancestors)
	return &response, nil
}

func buildConversationRunProjection(
	anchor conversationProjectionRun,
	forward, ancestors []conversationProjectionRun,
) ConversationRunListResponse {
	if !conversationProjectionRunEligible(anchor, anchor.RunUserID, anchor.RunAgentID, anchor.RootContextID) ||
		len(forward) == 0 || forward[0].RunID != anchor.RunID {
		return singleConversationRunResponse(anchor)
	}

	response := ConversationRunListResponse{
		AnchorRunID: anchor.RunID.String(),
		ConversationIdentitySHA256: conversationDigest(
			"openlinker.browser-observation-conversation.v1",
			anchor.RunUserID.String(),
			anchor.RunAgentID.String(),
			anchor.RootContextID,
		),
		Linear: true,
		// GetRunByID is the authorization source, but its compact row does not
		// carry request_metadata. The already validated forward anchor is the
		// projection source and preserves Browser policy evidence for item zero.
		Items: []ConversationRunItem{conversationRunItem(forward[0])},
	}
	children := make(map[uuid.UUID][]conversationProjectionRun)
	for index, row := range forward {
		if index == 0 {
			if !sameConversationProjectionRun(anchor, row) || row.Depth != 0 || row.Cycle {
				response.Linear = false
			}
			continue
		}
		if row.ParentRunID == nil {
			response.Linear = false
			continue
		}
		children[*row.ParentRunID] = append(children[*row.ParentRunID], row)
	}
	if len(forward) > conversationProjectionLimit {
		response.Linear = false
	}

	seenRuns := map[uuid.UUID]struct{}{anchor.RunID: {}}
	seenTasks := map[string]struct{}{}
	if anchor.ProtocolTaskID != "" {
		seenTasks[anchor.ProtocolTaskID] = struct{}{}
	}
	current := anchor.RunID
	for len(response.Items) < conversationProjectionLimit {
		candidates := children[current]
		if len(candidates) == 0 {
			break
		}
		if len(candidates) != 1 {
			response.Linear = false
			break
		}
		next := candidates[0]
		if !conversationProjectionRunEligible(next, anchor.RunUserID, anchor.RunAgentID, anchor.RootContextID) || next.Cycle {
			response.Linear = false
			break
		}
		if _, exists := seenRuns[next.RunID]; exists {
			response.Linear = false
			break
		}
		if next.ProtocolTaskID != "" {
			if _, exists := seenTasks[next.ProtocolTaskID]; exists {
				response.Linear = false
				break
			}
			seenTasks[next.ProtocolTaskID] = struct{}{}
		}
		seenRuns[next.RunID] = struct{}{}
		response.Items = append(response.Items, conversationRunItem(next))
		current = next.RunID
	}
	if len(children[current]) > 0 && len(response.Items) == conversationProjectionLimit {
		response.Linear = false
	}

	if ordinal, ok := conversationAnchorOrdinal(anchor, ancestors); ok {
		for index := range response.Items {
			value := ordinal + int32(index)
			response.Items[index].ConversationOrdinal = &value
		}
	}
	response.Revision = conversationRevision(response)
	return response
}

func singleConversationRunResponse(anchor conversationProjectionRun) ConversationRunListResponse {
	response := ConversationRunListResponse{
		AnchorRunID: anchor.RunID.String(),
		Linear:      false,
		Items:       []ConversationRunItem{conversationRunItem(anchor)},
	}
	response.Revision = conversationRevision(response)
	return response
}

func conversationRunItem(row conversationProjectionRun) ConversationRunItem {
	item := ConversationRunItem{
		RunID:                    row.RunID.String(),
		Status:                   row.Status,
		BrowserInteractionPolicy: browserInteractionPolicyFromMetadata(row.RequestMetadata),
		StartedAt:                row.StartedAt.UTC(),
		FinishedAt:               row.FinishedAt,
	}
	if row.ParentRunID != nil {
		item.ParentRunID = row.ParentRunID.String()
	}
	if item.FinishedAt != nil {
		finished := item.FinishedAt.UTC()
		item.FinishedAt = &finished
	}
	return item
}

func browserInteractionPolicyFromMetadata(raw []byte) string {
	var metadata struct {
		Authority persistedRuntimeAuthority `json:"_openlinker_runtime_authority"`
	}
	if json.Unmarshal(raw, &metadata) != nil {
		return ""
	}
	response := &RunResponse{}
	attachValidatedRunBrowserPolicy(metadata.Authority, response)
	return response.BrowserInteractionPolicy
}

func conversationProjectionRunEligible(
	row conversationProjectionRun,
	userID, agentID uuid.UUID,
	rootContextID string,
) bool {
	return row.Mapped && row.MappingUserID == userID && row.RunUserID == userID &&
		row.MappingAgentID == agentID && row.RunAgentID == agentID &&
		row.RootContextID == rootContextID && strings.TrimSpace(rootContextID) != "" &&
		row.Source == conversationA2ASource
}

func sameConversationProjectionRun(left, right conversationProjectionRun) bool {
	if left.ParentRunID == nil || right.ParentRunID == nil {
		return left.RunID == right.RunID && left.ParentRunID == nil && right.ParentRunID == nil &&
			left.MappingUserID == right.MappingUserID && left.MappingAgentID == right.MappingAgentID &&
			left.RunUserID == right.RunUserID && left.RunAgentID == right.RunAgentID &&
			left.RootContextID == right.RootContextID && left.Source == right.Source
	}
	return left.RunID == right.RunID && *left.ParentRunID == *right.ParentRunID &&
		left.MappingUserID == right.MappingUserID && left.MappingAgentID == right.MappingAgentID &&
		left.RunUserID == right.RunUserID && left.RunAgentID == right.RunAgentID &&
		left.RootContextID == right.RootContextID && left.Source == right.Source
}

func conversationAnchorOrdinal(
	anchor conversationProjectionRun,
	ancestors []conversationProjectionRun,
) (int32, bool) {
	if len(ancestors) == 0 || len(ancestors) > conversationAncestorLimit+1 ||
		ancestors[0].RunID != anchor.RunID {
		return 0, false
	}
	seen := make(map[uuid.UUID]struct{}, len(ancestors))
	for index, row := range ancestors {
		if row.Depth != int32(index) || row.Cycle ||
			!conversationProjectionRunEligible(row, anchor.RunUserID, anchor.RunAgentID, anchor.RootContextID) {
			return 0, false
		}
		if _, duplicate := seen[row.RunID]; duplicate {
			return 0, false
		}
		seen[row.RunID] = struct{}{}
		if index+1 < len(ancestors) {
			if row.ParentRunID == nil || *row.ParentRunID != ancestors[index+1].RunID {
				return 0, false
			}
		} else if row.ParentRunID != nil {
			return 0, false
		}
	}
	return int32(len(ancestors)), true
}

func conversationRevision(response ConversationRunListResponse) string {
	parts := []string{
		"openlinker.browser-observation-conversation-revision.v1",
		response.AnchorRunID,
		response.ConversationIdentitySHA256,
		strconv.FormatBool(response.Linear),
	}
	for _, item := range response.Items {
		ordinal := ""
		if item.ConversationOrdinal != nil {
			ordinal = strconv.FormatInt(int64(*item.ConversationOrdinal), 10)
		}
		finished := ""
		if item.FinishedAt != nil {
			finished = item.FinishedAt.UTC().Format(time.RFC3339Nano)
		}
		parts = append(parts,
			item.RunID, item.ParentRunID, ordinal, item.Status,
			item.BrowserInteractionPolicy,
			item.StartedAt.UTC().Format(time.RFC3339Nano), finished,
		)
	}
	return conversationDigest(parts...)
}

func conversationDigest(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
