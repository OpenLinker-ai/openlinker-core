package agent

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// MarketService 市场（用户侧只读）业务逻辑。
//
// 设计与模块 2 (Agent 注册写入) 隔离：本 service 只调用 SELECT，
// 不持有事务，也不依赖任何写入逻辑。
type MarketService struct {
	queries *db.Queries
}

// NewMarketService 构造 MarketService，pool 仅用于读。
func NewMarketService(pool *pgxpool.Pool) *MarketService {
	return &MarketService{queries: db.New(pool)}
}

// 默认与上限分页参数。
//
// size 上限设为 50，避免恶意大查询拖慢数据库（agents.tags 是 GIN 索引但仍要 scan）。
const (
	defaultPage               int32 = 1
	defaultSize               int32 = 12
	maxSize                   int32 = 50
	runtimePullConnectionMode       = "runtime_pull"
)

// ListMarket 列出已公开 Agent。
//
//   - tags：空切片表示不按 tag 筛；非空时使用 Postgres 数组重叠运算（任意命中）。
//   - keyword：空串表示不搜；非空时对 name/description 做 ILIKE。
//   - page 从 1 开始；size 由调用方 clamp 到 [1, 50]，但这里再做一次防御。
func (s *MarketService) ListMarket(ctx context.Context, tags []string, keyword string, page, size int32, callableOnlyArg ...bool) (*MarketListResponse, error) {
	if page < 1 {
		page = defaultPage
	}
	if size < 1 {
		size = defaultSize
	}
	if size > maxSize {
		size = maxSize
	}
	// pgx/v5 把 nil 序列化为 NULL；query 用 cardinality(text[]) = 0 判断空，
	// 因此这里要确保传入的是非 nil 的空切片。
	if tags == nil {
		tags = []string{}
	}
	callableOnly := len(callableOnlyArg) > 0 && callableOnlyArg[0]

	offset := (page - 1) * size

	rows, err := s.queries.ListPublicAgents(ctx, db.ListPublicAgentsParams{
		Tags:         tags,
		Keyword:      keyword,
		Limit:        size,
		Offset:       offset,
		CallableOnly: callableOnly,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.MarketService.ListMarket: ListPublicAgents")
		return nil, httpx.Internal("查询 Agent 列表失败")
	}

	total, err := s.queries.CountPublicAgents(ctx, db.CountPublicAgentsParams{
		Tags:         tags,
		Keyword:      keyword,
		CallableOnly: callableOnly,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.MarketService.ListMarket: CountPublicAgents")
		return nil, httpx.Internal("统计 Agent 数量失败")
	}

	items := make([]MarketListItem, 0, len(rows))
	for _, r := range rows {
		availability := s.agentAvailability(ctx, r.ID, r.ConnectionMode)
		verifiedCount, latestBenchmarkID := s.agentVerifiedSkillStats(ctx, r.ID)
		items = append(items, MarketListItem{
			ID:                r.ID.String(),
			Slug:              r.Slug,
			Name:              r.Name,
			Description:       r.Description,
			PricePerCallCents: r.PricePerCallCents,
			Tags:              normalizeTags(r.Tags),
			TotalCalls:        r.TotalCalls,
			Creator:           CreatorMini{DisplayName: r.CreatorName},
			ConnectionMode:    r.ConnectionMode,
			MCPToolName:       r.MCPToolName,
			Availability:      availability,
			Readiness: readinessForAgent(
				r.Slug,
				r.LifecycleStatus,
				r.Visibility,
				r.CertificationStatus,
				availability,
				verifiedCount,
				latestBenchmarkID,
			),
		})
	}

	return &MarketListResponse{
		Items: items,
		Total: total,
		Page:  page,
		Size:  size,
	}, nil
}

// GetBySlug 按 slug 查询已公开 Agent 详情。
//
// 不存在 / 未公开 / 已禁用 → NotFound（统一返回 404，避免泄露状态信息）。
// endpoint_auth_header 永不暴露给前端。
func (s *MarketService) GetBySlug(ctx context.Context, slug string) (*AgentDetailResponse, error) {
	if slug == "" {
		return nil, httpx.NotFound("Agent 不存在")
	}

	r, err := s.queries.GetAgentBySlug(ctx, slug)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("slug", slug).Msg("agent.MarketService.GetBySlug")
		return nil, httpx.Internal("查询 Agent 详情失败")
	}

	return s.detailResponseFromSlugRow(ctx, db.GetAgentBySlugRow{
		Agent:       r.Agent,
		CreatorName: r.CreatorName,
	})
}

// GetBySlugForOwner 按 slug 查询当前创作者自己的 Agent 详情。
//
// 仅用于创作者自测入口：private 也允许 owner 访问；非 owner / disabled 仍返回 404。
// endpoint_auth_header 永不暴露给前端。
func (s *MarketService) GetBySlugForOwner(ctx context.Context, slug string, creatorID uuid.UUID) (*AgentDetailResponse, error) {
	if slug == "" {
		return nil, httpx.NotFound("Agent 不存在")
	}

	r, err := s.queries.GetAgentBySlugForOwner(ctx, db.GetAgentBySlugForOwnerParams{
		Slug:      slug,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("slug", slug).Str("creator_id", creatorID.String()).Msg("agent.MarketService.GetBySlugForOwner")
		return nil, httpx.Internal("查询 Agent 详情失败")
	}

	return s.detailResponseFromSlugRow(ctx, db.GetAgentBySlugRow{
		Agent:       r.Agent,
		CreatorName: r.CreatorName,
	})
}

func (s *MarketService) detailResponseFromSlugRow(ctx context.Context, r db.GetAgentBySlugRow) (*AgentDetailResponse, error) {
	resp := &AgentDetailResponse{
		ID:                r.ID.String(),
		Slug:              r.Slug,
		Name:              r.Name,
		Description:       r.Description,
		EndpointURL:       r.EndpointURL,
		PricePerCallCents: r.PricePerCallCents,
		Tags:              normalizeTags(r.Tags),
		TotalCalls:        r.TotalCalls,
		Creator:           CreatorMini{DisplayName: r.CreatorName},
		CreatedAt:         r.CreatedAt.UTC().Format(time.RFC3339),
		Skills:            []SkillMini{},
		ConnectionMode:    r.ConnectionMode,
		MCPToolName:       r.MCPToolName,
		Availability:      s.agentAvailability(ctx, r.ID, r.ConnectionMode),
	}
	if r.CertifiedAt != nil {
		s := r.CertifiedAt.UTC().Format(time.RFC3339)
		resp.CertifiedAt = &s
	}
	resp.LifecycleStatus = r.LifecycleStatus
	resp.Visibility = r.Visibility
	resp.CertificationStatus = r.CertificationStatus

	if stats, err := s.queries.GetAgentVerifiedSkillStats(ctx, r.ID); err == nil {
		resp.VerifiedSkillCount = stats.VerifiedCount
		if stats.LatestBatchID != nil {
			id := stats.LatestBatchID.String()
			resp.LatestBenchmarkID = &id
		}
	}
	resp.Readiness = readinessForAgent(
		resp.Slug,
		resp.LifecycleStatus,
		resp.Visibility,
		resp.CertificationStatus,
		resp.Availability,
		resp.VerifiedSkillCount,
		resp.LatestBenchmarkID,
	)

	skills, err := s.queries.ListAgentSkills(ctx, r.ID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", r.ID.String()).Msg("agent.MarketService.GetBySlug: ListAgentSkills")
		return nil, httpx.Internal("查询 Agent skill 失败")
	}
	for i := range skills {
		resp.Skills = append(resp.Skills, SkillMini{
			ID:          skills[i].ID,
			Category:    skills[i].Category,
			Name:        skills[i].Name,
			Description: skills[i].Description,
		})
	}

	resp.Examples = []ExampleResponse{}
	cap, err := s.queries.GetAgentCapabilityBySlug(ctx, r.Slug)
	if err == nil {
		c := toCapabilityResponse(&cap)
		resp.Capability = &c
	} else if !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Str("slug", r.Slug).Msg("agent.MarketService.GetBySlug: GetAgentCapabilityBySlug")
		return nil, httpx.Internal("查询 Agent 能力声明失败")
	}

	examples, err := s.queries.ListAgentExamplesBySlug(ctx, r.Slug)
	if err != nil {
		log.Error().Err(err).Str("slug", r.Slug).Msg("agent.MarketService.GetBySlug: ListAgentExamplesBySlug")
		return nil, httpx.Internal("查询 Agent 示例失败")
	}
	for i := range examples {
		resp.Examples = append(resp.Examples, toExampleResponse(&examples[i]))
	}
	return resp, nil
}

// GetAgentCardBySlug returns a public Agent Card derived from the same public
// detail record used by the market page.
func (s *MarketService) GetAgentCardBySlug(ctx context.Context, slug string) (*AgentCardResponse, error) {
	return s.getAgentCardBySlug(ctx, slug, false)
}

func (s *MarketService) GetExtendedAgentCardBySlug(ctx context.Context, slug string) (*AgentCardResponse, error) {
	return s.getAgentCardBySlug(ctx, slug, true)
}

func (s *MarketService) getAgentCardBySlug(ctx context.Context, slug string, extended bool) (*AgentCardResponse, error) {
	detail, err := s.GetBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}

	cardSkills := make([]AgentCardSkill, 0, len(detail.Skills))
	skillIDs := make([]string, 0, len(detail.Skills))
	for _, skill := range detail.Skills {
		skillIDs = append(skillIDs, skill.ID)
		cardSkills = append(cardSkills, AgentCardSkill{
			ID:          skill.ID,
			Name:        skill.Name,
			Description: skill.Description,
			Tags:        []string{skill.Category},
		})
	}
	if len(cardSkills) == 0 {
		cardSkills = append(cardSkills, AgentCardSkill{
			ID:          "openlinker/" + detail.Slug,
			Name:        detail.Name,
			Description: detail.Description,
			Tags:        normalizeTags(detail.Tags),
		})
	}

	a2aEndpoint := "/api/v1/a2a/agents/" + detail.Slug
	extendedCardEndpoint := "/api/v1/agents/" + detail.Slug + "/agent-card.extended.json"
	cardVariant := "public"
	if extended {
		cardVariant = "extended"
	}
	card := &AgentCardResponse{
		Name:             detail.Name,
		Description:      detail.Description,
		URL:              a2aEndpoint,
		Version:          "v1",
		ProtocolVersion:  "1.0",
		ProtocolVersions: []string{"0.3", "1.0"},
		SupportedInterfaces: []AgentCardInterface{
			{URL: a2aEndpoint, ProtocolBinding: "JSONRPC", ProtocolVersion: "1.0"},
			{URL: a2aEndpoint, ProtocolBinding: "HTTP+JSON", ProtocolVersion: "1.0"},
			{URL: a2aEndpoint, ProtocolBinding: "JSONRPC", ProtocolVersion: "0.3"},
		},
		Provider: AgentCardProvider{
			Organization: detail.Creator.DisplayName,
		},
		Capabilities: AgentCardCapabilities{
			Streaming:               true,
			PushNotifications:       true,
			PushNotificationsLegacy: true,
			Delegation:              true,
			ExtendedAgentCard:       true,
		},
		DefaultInputModes:         []string{"application/json", "text/plain"},
		DefaultOutputModes:        []string{"application/json", "text/plain"},
		DefaultInputModesCurrent:  []string{"application/json", "text/plain"},
		DefaultOutputModesCurrent: []string{"application/json", "text/plain"},
		Skills:                    cardSkills,
		Authentication: AgentCardAuth{
			Schemes: []string{"Bearer"},
			Scopes:  []string{"agents:run", "runs:read"},
		},
		OpenLinker: AgentCardOpenLinkerExt{
			AgentID:                detail.ID,
			Slug:                   detail.Slug,
			CardVariant:            cardVariant,
			ExtendedCardEndpoint:   extendedCardEndpoint,
			ConnectionMode:         detail.ConnectionMode,
			MCPToolName:            detail.MCPToolName,
			AvailabilityStatus:     detail.Availability.Status,
			CertificationStatus:    detail.CertificationStatus,
			VerifiedSkillCount:     detail.VerifiedSkillCount,
			LatestBenchmarkBatchID: detail.LatestBenchmarkID,
			CapabilityDeclared:     detail.Capability != nil,
			ExampleCount:           int32(len(detail.Examples)),
			InvocationEndpoint:     a2aEndpoint,
			StreamEndpoint:         a2aEndpoint + "/message:stream",
			RunLookupEndpoint:      "/api/v1/runs/{run_id}",
			TaskLookupEndpoint:     a2aEndpoint + "/tasks/{task_id}",
			TaskSubscribeEndpoint:  a2aEndpoint + "/tasks/{task_id}:subscribe",
			SkillIDs:               skillIDs,
		},
	}
	if extended {
		card.Capability = detail.Capability
		card.Examples = detail.Examples
	}
	signAgentCard(card)
	return card, nil
}

func signAgentCard(card *AgentCardResponse) {
	seed := agentCardSigningSeed()
	if len(seed) != ed25519.SeedSize {
		return
	}
	card.Signature = nil
	payload, err := json.Marshal(card)
	if err != nil {
		return
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	digest := sha256.Sum256(payload)
	keyDigest := sha256.Sum256(publicKey)
	card.Signature = &AgentCardSignature{
		Algorithm:     "Ed25519",
		KeyID:         base64.RawURLEncoding.EncodeToString(keyDigest[:12]),
		PublicKey:     base64.RawURLEncoding.EncodeToString(publicKey),
		PayloadDigest: "sha256-" + base64.RawURLEncoding.EncodeToString(digest[:]),
		Signature:     base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload)),
	}
}

func agentCardSigningSeed() []byte {
	raw := strings.TrimSpace(os.Getenv("AGENT_CARD_SIGNING_SEED"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("OPENLINKER_AGENT_CARD_SIGNING_SEED"))
	}
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("JWT_SECRET"))
	}
	if raw == "" {
		return nil
	}
	if decoded, err := hex.DecodeString(raw); err == nil && len(decoded) == ed25519.SeedSize {
		return decoded
	}
	if decoded, err := base64.RawURLEncoding.DecodeString(raw); err == nil && len(decoded) == ed25519.SeedSize {
		return decoded
	}
	if decoded, err := base64.StdEncoding.DecodeString(raw); err == nil && len(decoded) == ed25519.SeedSize {
		return decoded
	}
	sum := sha256.Sum256([]byte(raw))
	return sum[:]
}

func (s *MarketService) agentAvailability(ctx context.Context, agentID uuid.UUID, connectionMode string) Availability {
	snapshot, err := s.queries.GetAgentAvailabilitySnapshot(ctx, agentID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.MarketService.agentAvailability")
		}
		return s.runtimeAwareAvailability(ctx, agentID, connectionMode, availabilityResponse("unknown", nil, nil, nil, 0))
	}
	return s.runtimeAwareAvailability(ctx, agentID, connectionMode, availabilityResponse(
		snapshot.AvailabilityStatus,
		snapshot.LastSuccessfulRunAt,
		snapshot.LastFailedRunAt,
		snapshot.LastCheckedAt,
		snapshot.ConsecutiveFailures,
	))
}

func (s *MarketService) agentVerifiedSkillStats(ctx context.Context, agentID uuid.UUID) (int32, *string) {
	stats, err := s.queries.GetAgentVerifiedSkillStats(ctx, agentID)
	if err != nil {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.MarketService.agentVerifiedSkillStats")
		return 0, nil
	}
	if stats.LatestBatchID == nil {
		return stats.VerifiedCount, nil
	}
	id := stats.LatestBatchID.String()
	return stats.VerifiedCount, &id
}

func (s *MarketService) runtimeAwareAvailability(ctx context.Context, agentID uuid.UUID, connectionMode string, availability Availability) Availability {
	if connectionMode != runtimePullConnectionMode {
		return availability
	}
	hasRuntime, err := s.queries.HasRecentRuntimePullToken(ctx, agentID)
	if err != nil {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.MarketService.runtimeAwareAvailability")
		return availability
	}
	if hasRuntime {
		return availability
	}
	availability.Status = "unreachable"
	availability.Label = "不可达"
	availability.Hint = "Runtime Pull Agent 最近没有运行时心跳或领取轮询，暂不建议试用。"
	return availability
}

func readinessForAgent(
	slug string,
	lifecycleStatus string,
	visibility string,
	certificationStatus string,
	availability Availability,
	verifiedSkillCount int32,
	latestBenchmarkID *string,
) Readiness {
	listed := lifecycleStatus == "active" && visibility == "public"
	discoverable := lifecycleStatus == "active" && slug != "" && (visibility == "public" || visibility == "unlisted")
	callable := availability.Status == "healthy" || (availability.Status != "unreachable" && availability.LastSuccessfulRunAt != nil)
	verified := verifiedSkillCount > 0
	certified := certificationStatus == "certified"
	readiness := Readiness{
		Listed:                 listed,
		Discoverable:           discoverable,
		Callable:               callable,
		Verified:               verified,
		Certified:              certified,
		PaidEnabled:            false,
		AgentCardURL:           "/api/v1/agents/" + slug + "/agent-card.json",
		A2AEndpoint:            "/api/v1/a2a/agents/" + slug,
		LastSuccessfulRunAt:    availability.LastSuccessfulRunAt,
		AvailabilityStatus:     availability.Status,
		VerifiedSkillCount:     verifiedSkillCount,
		LatestBenchmarkBatchID: latestBenchmarkID,
		Explanation: map[string]string{
			"listed":       "public listing is visible only when lifecycle is active and visibility is public",
			"discoverable": "public or unlisted active agents expose a slug and machine-readable Agent Card",
			"callable":     "true only with a healthy availability signal or at least one successful run that is not currently unreachable",
			"verified":     "true only when Skill Benchmark evidence exists",
			"certified":    "true only after OpenLinker certification review",
			"paid_enabled": "payments are not enabled in the current release",
		},
	}
	if slug == "" {
		readiness.AgentCardURL = ""
		readiness.A2AEndpoint = ""
	}
	return readiness
}

func availabilityResponse(status string, successAt, failedAt, checkedAt *time.Time, failures int32) Availability {
	label := "未验证"
	hint := "Agent 已注册，但还没有成功运行或失败记录。首次调用后会更新可用性。"
	switch status {
	case "healthy":
		label = "可用"
		hint = "最近一次真实调用成功，当前可用性良好。"
	case "degraded":
		label = "不稳定"
		hint = "最近调用失败。Agent 仍可尝试，但建议创作者检查 endpoint、认证或运行时。"
	case "unreachable":
		label = "不可达"
		hint = "连续多次调用失败，暂不建议用于关键任务。"
	default:
		status = "unknown"
	}
	format := func(t *time.Time) *string {
		if t == nil {
			return nil
		}
		out := t.UTC().Format(time.RFC3339)
		return &out
	}
	return Availability{
		Status:              status,
		Label:               label,
		Hint:                hint,
		LastSuccessfulRunAt: format(successAt),
		LastFailedRunAt:     format(failedAt),
		LastCheckedAt:       format(checkedAt),
		ConsecutiveFailures: failures,
	}
}

// normalizeTags 把 nil 切片归一化成空切片，确保 JSON 输出 [] 而不是 null。
func normalizeTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}
