package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	approvalDefaultMinutes = 60
	approvalSlugRandomLen  = 12 // 24 hex chars

	actionRequestCertification = "request_certification"
	actionSetVisibilityPublic  = "set_visibility_public"
)

// ApprovalService 高风险动作审批 CRUD（docs/29 §3.4）。
//
// 与 agent.Service 拆开是因为：
//   - 主要消费方是 Agent 绑定访问令牌自动写入（后置），与创作者注册路径解耦
//   - 当前阶段只暴露 JWT 路径上的 list / get / confirm / reject + 手动 create
type ApprovalService struct {
	queries *db.Queries
	pool    *pgxpool.Pool
	cfg     *config.Config
}

func NewApprovalService(pool *pgxpool.Pool, cfg *config.Config) *ApprovalService {
	return &ApprovalService{queries: db.New(pool), pool: pool, cfg: cfg}
}

// CreateApproval 手动写一条审批请求（创作者 / 运营 UI 触发）。
// Agent 绑定访问令牌自动触发的入口后置在 runtime 层，本方法不处理 token id。
func (s *ApprovalService) CreateApproval(ctx context.Context, creatorID uuid.UUID, req *CreateApprovalRequest) (*ApprovalResponse, error) {
	agentID, err := uuid.Parse(req.AgentID)
	if err != nil {
		return nil, httpx.BadRequest("agent_id 不是合法 uuid")
	}
	if _, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Msg("approval.CreateApproval: owner check")
		return nil, httpx.Internal("查询 Agent 失败")
	}

	payload, err := json.Marshal(req.Payload)
	if err != nil {
		return nil, httpx.BadRequest("payload 序列化失败")
	}
	if len(payload) == 0 || string(payload) == "null" {
		payload = []byte("{}")
	}

	slug, err := generateApprovalSlug()
	if err != nil {
		return nil, httpx.Internal("生成审批链接失败")
	}
	minutes := req.ExpiresInMinutes
	if minutes == 0 {
		minutes = approvalDefaultMinutes
	}
	userID := creatorID
	created, err := s.queries.CreateAgentApproval(ctx, db.CreateAgentApprovalParams{
		AgentID:           agentID,
		RequestedByUserID: &userID,
		Action:            strings.TrimSpace(req.Action),
		PayloadJSON:       payload,
		ApprovalURLSlug:   slug,
		ExpiresAt:         time.Now().Add(time.Duration(minutes) * time.Minute),
	})
	if err != nil {
		log.Error().Err(err).Msg("approval.CreateApproval: insert")
		return nil, httpx.Internal("创建审批请求失败")
	}
	resp := s.toApprovalResponse(&created)
	return &resp, nil
}

// ListApprovals 列出当前创作者下所有 agent 的审批记录。
func (s *ApprovalService) ListApprovals(ctx context.Context, creatorID uuid.UUID) ([]ApprovalResponse, error) {
	rows, err := s.queries.ListAgentApprovalsForCreator(ctx, creatorID)
	if err != nil {
		return nil, httpx.Internal("查询审批列表失败")
	}
	items := make([]ApprovalResponse, 0, len(rows))
	for i := range rows {
		items = append(items, s.toApprovalResponse(&rows[i]))
	}
	return items, nil
}

// GetApproval 创作者按 id 获取单条审批。
func (s *ApprovalService) GetApproval(ctx context.Context, creatorID, approvalID uuid.UUID) (*ApprovalResponse, error) {
	row, err := s.queries.GetAgentApprovalForCreator(ctx, db.GetAgentApprovalForCreatorParams{
		ID:        approvalID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("审批不存在")
		}
		log.Error().Err(err).Msg("approval.GetApproval")
		return nil, httpx.Internal("查询审批失败")
	}
	resp := s.toApprovalResponse(&row)
	return &resp, nil
}

// ConfirmApproval 创作者确认审批。pending+未过期 → confirmed。
func (s *ApprovalService) ConfirmApproval(ctx context.Context, creatorID, approvalID uuid.UUID, note string) error {
	notePtr := normalizeNote(note)
	affected, err := s.queries.ConfirmAgentApproval(ctx, db.ConfirmAgentApprovalParams{
		ID:           approvalID,
		CreatorID:    creatorID,
		DecisionNote: notePtr,
	})
	if err != nil {
		log.Error().Err(err).Msg("approval.ConfirmApproval")
		return httpx.Internal("确认审批失败")
	}
	if err := s.translateDecisionResult(ctx, affected, creatorID, approvalID); err != nil {
		return err
	}
	return s.applyConfirmedAction(ctx, creatorID, approvalID)
}

// RejectApproval 创作者拒绝审批。pending+未过期 → rejected。
func (s *ApprovalService) RejectApproval(ctx context.Context, creatorID, approvalID uuid.UUID, note string) error {
	notePtr := normalizeNote(note)
	affected, err := s.queries.RejectAgentApproval(ctx, db.RejectAgentApprovalParams{
		ID:           approvalID,
		CreatorID:    creatorID,
		DecisionNote: notePtr,
	})
	if err != nil {
		log.Error().Err(err).Msg("approval.RejectApproval")
		return httpx.Internal("拒绝审批失败")
	}
	return s.translateDecisionResult(ctx, affected, creatorID, approvalID)
}

// SweepExpiredApprovals 批量把超期 pending 标记为 expired。供后台 cron / worker 调。
func (s *ApprovalService) SweepExpiredApprovals(ctx context.Context) (int64, error) {
	return s.queries.ExpireAgentApprovals(ctx)
}

func (s *ApprovalService) translateDecisionResult(ctx context.Context, affected int64, creatorID, approvalID uuid.UUID) error {
	if affected > 0 {
		return nil
	}
	// 0 行：要么不存在 / 不属于该 creator，要么 status 非 pending，要么已过期。
	row, err := s.queries.GetAgentApprovalForCreator(ctx, db.GetAgentApprovalForCreatorParams{
		ID:        approvalID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("审批不存在")
		}
		return httpx.Internal("查询审批失败")
	}
	if row.Status != "pending" {
		return httpx.Conflict("审批当前状态 " + row.Status + " 不可再决策")
	}
	return httpx.Conflict("审批已过期")
}

func (s *ApprovalService) applyConfirmedAction(ctx context.Context, creatorID, approvalID uuid.UUID) error {
	approval, err := s.queries.GetAgentApprovalForCreator(ctx, db.GetAgentApprovalForCreatorParams{
		ID: approvalID, CreatorID: creatorID,
	})
	if err != nil {
		return httpx.Internal("读取已确认审批失败")
	}
	switch approval.Action {
	case "request-certification", actionRequestCertification:
		if _, err := s.queries.RequestCertification(ctx, db.RequestCertificationParams{
			ID: approval.AgentID, CreatorID: creatorID,
		}); err != nil {
			return httpx.Internal("提交认证请求失败")
		}
	case "set-visibility-public", actionSetVisibilityPublic, "set_visibility=public":
		if err := s.queries.SetAgentVisibilityForOwner(ctx, db.SetAgentVisibilityForOwnerParams{
			ID: approval.AgentID, CreatorID: creatorID, Visibility: "public",
		}); err != nil {
			return httpx.Internal("公开 Agent 失败")
		}
	}
	return nil
}

func (s *ApprovalService) toApprovalResponse(a *db.AgentActionApprovalRequest) ApprovalResponse {
	resp := ApprovalResponse{
		ID:              a.ID.String(),
		AgentID:         a.AgentID.String(),
		Action:          a.Action,
		Status:          a.Status,
		ApprovalURLSlug: a.ApprovalURLSlug,
		ApprovalURL:     s.approvalURL(a.ApprovalURLSlug),
		ExpiresAt:       a.ExpiresAt.UTC().Format(time.RFC3339),
		CreatedAt:       a.CreatedAt.UTC().Format(time.RFC3339),
	}
	if a.RequestedByUserID != nil {
		v := a.RequestedByUserID.String()
		resp.RequestedByUserID = &v
	}
	if a.RequestedByTokenID != nil {
		v := a.RequestedByTokenID.String()
		resp.RequestedByTokenID = &v
	}
	if a.DecidedAt != nil {
		v := a.DecidedAt.UTC().Format(time.RFC3339)
		resp.DecidedAt = &v
	}
	if a.DecidedByUserID != nil {
		v := a.DecidedByUserID.String()
		resp.DecidedByUserID = &v
	}
	if a.DecisionNote != nil {
		v := *a.DecisionNote
		resp.DecisionNote = &v
	}
	if len(a.PayloadJSON) > 0 {
		_ = json.Unmarshal(a.PayloadJSON, &resp.Payload)
	}
	return resp
}

func (s *ApprovalService) approvalURL(slug string) string {
	base := ""
	if s.cfg != nil {
		base = strings.TrimRight(s.cfg.FrontendURL, "/")
	}
	if base == "" {
		base = "https://openlinker.ai"
	}
	return base + "/hub/approvals/" + slug
}

func generateApprovalSlug() (string, error) {
	raw := make([]byte, approvalSlugRandomLen)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func normalizeNote(note string) *string {
	note = strings.TrimSpace(note)
	if note == "" {
		return nil
	}
	return &note
}
