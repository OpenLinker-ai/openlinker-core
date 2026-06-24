package agent

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
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

// slugPattern 与 schema CHECK 约束一致：以小写字母/数字开头结尾，中间允许连字符。
var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)

// minSlugLen / maxSlugLen 与 dto validator 保持一致。
const (
	minSlugLen       = 3
	maxSlugLen       = 80
	maxAgentExamples = 20
)

// Service Agent 注册 / 公开状态业务逻辑层。
type Service struct {
	queries   *db.Queries
	pool      *pgxpool.Pool
	cfg       *config.Config
	dryRunner DryRunner
}

// DryRunner Agent endpoint 探活调用接口；由 runtime.Service 实现。
//
// 抽象到接口避免 internal/agent 直接依赖 internal/runtime（保留交叉依赖出口）。
// 返回 (output, errMsg)：errMsg 为空字符串视为通过。
type DryRunner interface {
	DryRun(ctx context.Context, agent *db.Agent, input map[string]interface{}) (map[string]interface{}, string)
}

// NewService 构造 Service。
func NewService(pool *pgxpool.Pool, cfg *config.Config) *Service {
	return &Service{
		queries: db.New(pool),
		pool:    pool,
		cfg:     cfg,
	}
}

// SetDryRunner 注入 endpoint 探活器；为 nil 时 RunDryRun 返回 503。
//
// 构造时不接收 DryRunner 是为了保留 main.go 中循环依赖的解耦点：
// agent 和 runtime 互不引用，必要时由 cmd/api/main.go 在两者都构造完成后串起来。
func (s *Service) SetDryRunner(r DryRunner) {
	s.dryRunner = r
}

// CreateAgent 创作者新建 Agent。
//
// 流程：
//  1. 校验用户存在且 is_creator=true → 否则 Forbidden
//  2. 校验 slug 格式 → 否则 Unprocessable
//  3. CheckSlugAvailable → 否则 Conflict
//  4. INSERT agents（默认 public，可显式选择 unlisted/private）；UNIQUE 兜底再次 Conflict
func (s *Service) CreateAgent(ctx context.Context, creatorID uuid.UUID, req *CreateAgentRequest) (*AgentResponse, error) {
	user, err := s.queries.GetUserByID(ctx, creatorID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("agent.CreateAgent: GetUserByID")
		return nil, httpx.Internal("查询用户失败")
	}
	if !user.IsCreator {
		return nil, httpx.Forbidden("仅创作者可创建 Agent")
	}

	slug := strings.TrimSpace(req.Slug)
	if !isValidSlug(slug) {
		return nil, httpx.Unprocessable("slug 格式不合法：仅允许小写字母 / 数字 / 连字符，3..80 字符，且不能以连字符开头或结尾")
	}
	connection, err := normalizeConnectionSettings(
		slug,
		req.EndpointURL,
		req.ConnectionMode,
		req.MCPToolName,
		s.cfg.AllowLocalHTTPEndpoints,
	)
	if err != nil {
		return nil, err
	}

	avail, err := s.queries.CheckSlugAvailable(ctx, slug)
	if err != nil {
		log.Error().Err(err).Msg("agent.CreateAgent: CheckSlugAvailable")
		return nil, httpx.Internal("查询 slug 失败")
	}
	if !avail {
		return nil, httpx.Conflict("slug 已被占用")
	}

	authHeader := normalizeAuthHeader(req.EndpointAuthHeader)
	visibility := strings.TrimSpace(req.Visibility)
	if visibility == "" {
		visibility = "public"
	}
	created, err := s.queries.CreateAgent(ctx, db.CreateAgentParams{
		CreatorID:          creatorID,
		Slug:               slug,
		Name:               strings.TrimSpace(req.Name),
		Description:        strings.TrimSpace(req.Description),
		EndpointURL:        connection.EndpointURL,
		EndpointAuthHeader: authHeader,
		PricePerCallCents:  req.PricePerCallCents,
		Tags:               normalizeTagsForInsert(req.Tags),
		Visibility:         visibility,
		ConnectionMode:     connection.Mode,
		MCPToolName:        connection.MCPToolName,
	})
	if err != nil {
		// UNIQUE violation 兜底（并发场景）
		if isUniqueViolation(err) {
			return nil, httpx.Conflict("slug 已被占用")
		}
		// CHECK violation（如 slug 格式 / endpoint_url）
		if isCheckViolation(err) {
			return nil, httpx.Unprocessable("Agent 字段不符合约束")
		}
		log.Error().Err(err).Msg("agent.CreateAgent: insert")
		return nil, httpx.Internal("创建 Agent 失败")
	}

	resp := toAgentResponse(&created)
	s.ensureOnboardingStatusBestEffort(ctx, created.ID)
	return &resp, nil
}

// UpdateAgent 创作者编辑 Agent 基础信息（含 visibility）。
//
// SQL 仅在 lifecycle_status='active' 且 creator_id 匹配时返回；
// RETURNING 命中 0 行 → pgx.ErrNoRows，service 层再用 GetAgentByIDForOwner 区分两种 case：
//   - 不存在 / 不属于该 creator → NotFound
//   - disabled → Forbidden
//
// Visibility 空串视为不改（默认沿用旧值）；显式传值则按 public/unlisted/private 校验。
func (s *Service) UpdateAgent(ctx context.Context, agentID, creatorID uuid.UUID, req *UpdateAgentRequest) (*AgentResponse, error) {
	existing, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Msg("agent.UpdateAgent: GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if existing.LifecycleStatus == "disabled" {
		return nil, httpx.Forbidden("已下架的 Agent 不可编辑")
	}

	connectionMode := req.ConnectionMode
	if connectionMode == "" {
		connectionMode = existing.ConnectionMode
	}
	endpointURL := req.EndpointURL
	if endpointURL == "" {
		endpointURL = existing.EndpointURL
	}
	mcpToolName := req.MCPToolName
	if mcpToolName == "" && existing.MCPToolName != nil && connectionMode == existing.ConnectionMode {
		mcpToolName = *existing.MCPToolName
	}
	connection, err := normalizeConnectionSettings(
		existing.Slug,
		endpointURL,
		connectionMode,
		mcpToolName,
		s.cfg.AllowLocalHTTPEndpoints,
	)
	if err != nil {
		return nil, err
	}
	authHeader := normalizeAuthHeader(req.EndpointAuthHeader)
	visibility := strings.TrimSpace(req.Visibility)
	if visibility == "" {
		visibility = existing.Visibility
	}
	updated, err := s.queries.UpdateAgentDraft(ctx, db.UpdateAgentDraftParams{
		ID:                 agentID,
		Name:               strings.TrimSpace(req.Name),
		Description:        strings.TrimSpace(req.Description),
		EndpointURL:        connection.EndpointURL,
		EndpointAuthHeader: authHeader,
		PricePerCallCents:  req.PricePerCallCents,
		Tags:               normalizeTagsForInsert(req.Tags),
		CreatorID:          creatorID,
		Visibility:         visibility,
		ConnectionMode:     connection.Mode,
		MCPToolName:        connection.MCPToolName,
	})
	if err == nil {
		resp := toAgentResponse(&updated)
		return &resp, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		if isCheckViolation(err) {
			return nil, httpx.Unprocessable("Agent 字段不符合约束")
		}
		log.Error().Err(err).Msg("agent.UpdateAgent: UpdateAgentDraft")
		return nil, httpx.Internal("更新 Agent 失败")
	}

	// RETURNING 0 行：可能不存在 / 不属于该 creator / 状态不允许编辑
	existing, err = s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Msg("agent.UpdateAgent: GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if existing.LifecycleStatus == "disabled" {
		return nil, httpx.Forbidden("已下架的 Agent 不可编辑")
	}
	// 理论上 active 应当命中 RETURNING；走到这里说明并发导致状态变化
	return nil, httpx.Conflict("Agent 状态已变化，请刷新后重试")
}

// SetVisibility 仅变更市场可见范围，避免客户端为了改状态而重传 endpoint 凭据。
func (s *Service) SetVisibility(ctx context.Context, agentID, creatorID uuid.UUID, visibility string) (*AgentResponse, error) {
	existing, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Msg("agent.SetVisibility: GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if existing.LifecycleStatus == "disabled" {
		return nil, httpx.Forbidden("已下架的 Agent 不可编辑")
	}
	if err := s.queries.SetAgentVisibilityForOwner(ctx, db.SetAgentVisibilityForOwnerParams{
		ID:         agentID,
		CreatorID:  creatorID,
		Visibility: strings.TrimSpace(visibility),
	}); err != nil {
		if isCheckViolation(err) {
			return nil, httpx.Unprocessable("可见性不符合约束")
		}
		log.Error().Err(err).Msg("agent.SetVisibility: update")
		return nil, httpx.Internal("更新可见性失败")
	}
	updated, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.SetVisibility: refresh")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	resp := toAgentResponse(&updated)
	return &resp, nil
}

// DisableAgent 创作者主动下架。
func (s *Service) DisableAgent(ctx context.Context, agentID, creatorID uuid.UUID) error {
	rows, err := s.queries.DisableAgent(ctx, db.DisableAgentParams{
		ID:        agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.DisableAgent: DisableAgent")
		return httpx.Internal("下架失败")
	}
	if rows == 0 {
		return httpx.NotFound("Agent 不存在")
	}
	return nil
}

// ListMyAgents 创作者中心列表。
func (s *Service) ListMyAgents(ctx context.Context, creatorID uuid.UUID) ([]AgentResponse, error) {
	rows, err := s.queries.ListAgentsByCreator(ctx, creatorID)
	if err != nil {
		log.Error().Err(err).Str("user_id", creatorID.String()).Msg("agent.ListMyAgents")
		return nil, httpx.Internal("查询 Agent 列表失败")
	}
	out := make([]AgentResponse, 0, len(rows))
	for i := range rows {
		out = append(out, toAgentResponse(&rows[i]))
	}
	return out, nil
}

// GetMyAgent 创作者按 id 查自己的 Agent（编辑前预填用）。
func (s *Service) GetMyAgent(ctx context.Context, agentID, creatorID uuid.UUID) (*AgentResponse, error) {
	a, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Msg("agent.GetMyAgent: GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	resp := toAgentResponse(&a)
	return &resp, nil
}

// GetAgentOnboarding 查询创作者侧接入完成度、能力声明和 examples。
func (s *Service) GetAgentOnboarding(ctx context.Context, agentID, creatorID uuid.UUID) (*OnboardingResponse, error) {
	if err := s.ensureAgentOwner(ctx, agentID, creatorID); err != nil {
		return nil, err
	}
	s.ensureOnboardingStatusBestEffort(ctx, agentID)

	status, err := s.queries.GetOnboardingStatusForOwner(ctx, db.GetOnboardingStatusForOwnerParams{
		AgentID:   agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.GetAgentOnboarding: GetOnboardingStatusForOwner")
		return nil, httpx.Internal("查询接入状态失败")
	}

	var capability *CapabilityResponse
	capRow, err := s.queries.GetAgentCapabilityByAgentID(ctx, agentID)
	if err == nil {
		resp := toCapabilityResponse(&capRow)
		capability = &resp
	} else if !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.GetAgentOnboarding: GetAgentCapabilityByAgentID")
		return nil, httpx.Internal("查询能力声明失败")
	}

	exampleRows, err := s.queries.ListAgentExamplesByAgentID(ctx, agentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.GetAgentOnboarding: ListAgentExamplesByAgentID")
		return nil, httpx.Internal("查询示例失败")
	}
	examples := make([]ExampleResponse, 0, len(exampleRows))
	for i := range exampleRows {
		examples = append(examples, toExampleResponse(&exampleRows[i]))
	}

	return &OnboardingResponse{
		Status:       toOnboardingStatusResponse(&status),
		Capability:   capability,
		Examples:     examples,
		Availability: s.agentAvailability(ctx, agentID),
	}, nil
}

// UpsertCapability 保存 Agent input/output JSON Schema。
func (s *Service) UpsertCapability(ctx context.Context, agentID, creatorID uuid.UUID, req *UpsertCapabilityRequest) (*CapabilityResponse, error) {
	if req == nil || req.InputSchema == nil || req.OutputSchema == nil {
		return nil, httpx.Unprocessable("input_schema 和 output_schema 必填")
	}
	if err := s.ensureAgentOwner(ctx, agentID, creatorID); err != nil {
		return nil, err
	}
	summary := strings.TrimSpace(req.Summary)
	if len(summary) > 1000 {
		return nil, httpx.Unprocessable("summary 不能超过 1000 字符")
	}
	if err := validateCapabilitySchema(req.InputSchema, "input_schema"); err != nil {
		return nil, httpx.Unprocessable(err.Error())
	}
	if err := validateCapabilitySchema(req.OutputSchema, "output_schema"); err != nil {
		return nil, httpx.Unprocessable(err.Error())
	}
	inputJSON, err := json.Marshal(req.InputSchema)
	if err != nil {
		return nil, httpx.BadRequest("input_schema 不是合法 JSON")
	}
	outputJSON, err := json.Marshal(req.OutputSchema)
	if err != nil {
		return nil, httpx.BadRequest("output_schema 不是合法 JSON")
	}

	s.ensureOnboardingStatusBestEffort(ctx, agentID)
	row, err := s.queries.UpsertAgentCapability(ctx, db.UpsertAgentCapabilityParams{
		AgentID:      agentID,
		CreatorID:    creatorID,
		InputSchema:  inputJSON,
		OutputSchema: outputJSON,
		Summary:      summary,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.UpsertCapability: UpsertAgentCapability")
		return nil, httpx.Internal("保存能力声明失败")
	}
	if _, err := s.queries.MarkCapabilitiesSet(ctx, agentID); err != nil {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.UpsertCapability: MarkCapabilitiesSet")
	}
	resp := toCapabilityResponse(&row)
	return &resp, nil
}

// CreateExample 新增 Agent 输入/输出示例。
func (s *Service) CreateExample(ctx context.Context, agentID, creatorID uuid.UUID, req *CreateExampleRequest) (*ExampleResponse, error) {
	if req == nil || req.InputJSON == nil {
		return nil, httpx.Unprocessable("input_json 必填")
	}
	title := strings.TrimSpace(req.Title)
	if title == "" || len(title) > 120 {
		return nil, httpx.Unprocessable("title 长度需在 1-120 字符之间")
	}
	if err := s.ensureAgentOwner(ctx, agentID, creatorID); err != nil {
		return nil, err
	}
	count, err := s.queries.CountAgentExamplesByAgentID(ctx, agentID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.CreateExample: CountAgentExamplesByAgentID")
		return nil, httpx.Internal("查询示例数量失败")
	}
	if count >= maxAgentExamples {
		return nil, httpx.BadRequest("每个 Agent 最多 20 条示例")
	}
	if err := s.validateExampleWithCapability(ctx, agentID, req.InputJSON, req.ExpectedOutputJSON); err != nil {
		return nil, err
	}

	inputJSON, err := json.Marshal(req.InputJSON)
	if err != nil {
		return nil, httpx.BadRequest("input_json 不是合法 JSON")
	}
	var expectedJSON []byte
	if req.ExpectedOutputJSON != nil {
		expectedJSON, err = json.Marshal(req.ExpectedOutputJSON)
		if err != nil {
			return nil, httpx.BadRequest("expected_output_json 不是合法 JSON")
		}
	}

	s.ensureOnboardingStatusBestEffort(ctx, agentID)
	row, err := s.queries.CreateAgentExample(ctx, db.CreateAgentExampleParams{
		AgentID:            agentID,
		CreatorID:          creatorID,
		Title:              title,
		InputJSON:          inputJSON,
		ExpectedOutputJSON: expectedJSON,
		SortOrder:          req.SortOrder,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		if isCheckViolation(err) {
			return nil, httpx.Unprocessable("示例字段不符合约束")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.CreateExample: CreateAgentExample")
		return nil, httpx.Internal("创建示例失败")
	}
	if _, err := s.queries.MarkExamplesSet(ctx, db.MarkExamplesSetParams{
		AgentID:     agentID,
		ExamplesSet: true,
	}); err != nil {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.CreateExample: MarkExamplesSet")
	}
	resp := toExampleResponse(&row)
	return &resp, nil
}

// DeleteExample 删除 Agent 示例。
func (s *Service) DeleteExample(ctx context.Context, agentID, exampleID, creatorID uuid.UUID) error {
	rows, err := s.queries.DeleteAgentExampleForOwner(ctx, db.DeleteAgentExampleForOwnerParams{
		ID:        exampleID,
		AgentID:   agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		log.Error().Err(err).Str("example_id", exampleID.String()).Msg("agent.DeleteExample: DeleteAgentExampleForOwner")
		return httpx.Internal("删除示例失败")
	}
	if rows == 0 {
		return httpx.NotFound("示例不存在")
	}
	count, err := s.queries.CountAgentExamplesByAgentID(ctx, agentID)
	if err != nil {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.DeleteExample: CountAgentExamplesByAgentID")
		return nil
	}
	if _, err := s.queries.MarkExamplesSet(ctx, db.MarkExamplesSetParams{
		AgentID:     agentID,
		ExamplesSet: count > 0,
	}); err != nil {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.DeleteExample: MarkExamplesSet")
	}
	return nil
}

// RunDryRun 用首条 example 的 input 调用创作者 endpoint，更新 onboarding 状态。
//
// 流程：
//  1. 校验 Agent 归属（GetAgentByIDForOwner）
//  2. 取首条 example.input_json
//  3. 调 DryRunner（不计费 / 不写 runs）
//  4. 把结果写到 agent_onboarding_status.dry_run_*
//  5. 返回 DryRunResponse
func (s *Service) RunDryRun(ctx context.Context, agentID, creatorID uuid.UUID) (*DryRunResponse, error) {
	if s.dryRunner == nil {
		return nil, httpx.ServiceUnavailable("dry-run 暂不可用")
	}

	agentRow, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.RunDryRun: GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}

	rawInput, err := s.queries.GetFirstExampleInputByAgentID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.Unprocessable("请先添加至少 1 条示例后再执行 dry-run")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.RunDryRun: GetFirstExampleInputByAgentID")
		return nil, httpx.Internal("查询示例失败")
	}

	input := map[string]interface{}{}
	if err := json.Unmarshal(rawInput, &input); err != nil {
		return nil, httpx.Unprocessable("示例 input 不是合法 JSON object，无法 dry-run")
	}
	capability, err := s.queries.GetAgentCapabilityByAgentID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.Unprocessable("请先保存能力声明后再执行 dry-run")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.RunDryRun: GetAgentCapabilityByAgentID")
		return nil, httpx.Internal("查询能力声明失败")
	}
	inputSchema := decodeJSONMap(capability.InputSchema)
	if err := validateJSONAgainstSchema(input, inputSchema, "input_json"); err != nil {
		return nil, httpx.Unprocessable("示例 input 不匹配 input_schema：" + err.Error())
	}
	outputSchema := decodeJSONMap(capability.OutputSchema)

	s.ensureOnboardingStatusBestEffort(ctx, agentID)

	output, errMsg := s.dryRunner.DryRun(ctx, &agentRow, input)
	if errMsg == "" {
		if err := validateJSONAgainstSchema(output, outputSchema, "output"); err != nil {
			errMsg = "output 不匹配 output_schema：" + err.Error()
		}
	}
	result := "pass"
	var errPtr *string
	if errMsg != "" {
		result = "fail"
		e := errMsg
		errPtr = &e
	}

	if _, dbErr := s.queries.UpdateDryRunResult(ctx, db.UpdateDryRunResultParams{
		AgentID: agentID,
		Result:  result,
		Error:   errPtr,
	}); dbErr != nil {
		log.Warn().Err(dbErr).Str("agent_id", agentID.String()).Msg("agent.RunDryRun: UpdateDryRunResult")
	}

	availability := s.markAvailabilityAfterDryRun(ctx, agentID, result, errMsg)

	return &DryRunResponse{
		Result:       result,
		Error:        errPtr,
		Output:       output,
		Availability: availability,
		RepairHints:  repairHintsForDryRun(&agentRow, errMsg),
	}, nil
}

func (s *Service) validateExampleWithCapability(
	ctx context.Context,
	agentID uuid.UUID,
	input map[string]interface{},
	expectedOutput map[string]interface{},
) error {
	capability, err := s.queries.GetAgentCapabilityByAgentID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.validateExampleWithCapability")
		return httpx.Internal("查询能力声明失败")
	}
	inputSchema := decodeJSONMap(capability.InputSchema)
	if err := validateJSONAgainstSchema(input, inputSchema, "input_json"); err != nil {
		return httpx.Unprocessable("input_json 不匹配 input_schema：" + err.Error())
	}
	if expectedOutput != nil {
		outputSchema := decodeJSONMap(capability.OutputSchema)
		if err := validateJSONAgainstSchema(expectedOutput, outputSchema, "expected_output_json"); err != nil {
			return httpx.Unprocessable("expected_output_json 不匹配 output_schema：" + err.Error())
		}
	}
	return nil
}

// CheckSlug 提交前实时校验 slug 是否可用。
func (s *Service) CheckSlug(ctx context.Context, slug string) (*SlugCheckResponse, error) {
	slug = strings.TrimSpace(slug)
	if !isValidSlug(slug) {
		// 格式不合法直接返回 not available（前端拿到 false 给出对应提示）
		return &SlugCheckResponse{Slug: slug, Available: false}, nil
	}
	avail, err := s.queries.CheckSlugAvailable(ctx, slug)
	if err != nil {
		log.Error().Err(err).Msg("agent.CheckSlug: CheckSlugAvailable")
		return nil, httpx.Internal("查询 slug 失败")
	}
	return &SlugCheckResponse{Slug: slug, Available: avail}, nil
}

// BecomeCreator 设置 user.is_creator=true（一键，无审核）。
//
// Phase 1 决策：不做创作者认证审核，creator_verified 字段保留给后续运营手动设置。
func (s *Service) BecomeCreator(ctx context.Context, userID uuid.UUID) error {
	rows, err := s.queries.UpdateUserBecomeCreator(ctx, userID)
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("agent.BecomeCreator: UpdateUserBecomeCreator")
		return httpx.Internal("设置创作者身份失败")
	}
	if rows == 0 {
		return httpx.NotFound("用户不存在")
	}
	return nil
}

// === Admin 接口 ===

// ListPendingForAdmin admin/运营人工处理队列。
func (s *Service) ListPendingForAdmin(ctx context.Context) ([]AgentResponse, error) {
	rows, err := s.queries.ListPendingAgents(ctx)
	if err != nil {
		log.Error().Err(err).Msg("agent.ListPendingForAdmin")
		return nil, httpx.Internal("查询待处理 Agent 失败")
	}
	out := make([]AgentResponse, 0, len(rows))
	for _, r := range rows {
		resp := toAgentResponse(&r.Agent)
		resp.Creator = &Creator{
			ID:          r.CreatorID.String(),
			Email:       r.CreatorEmail,
			DisplayName: r.CreatorName,
		}
		out = append(out, resp)
	}
	return out, nil
}

// RequestCertification 创作者发起认证申请。unreviewed/rejected → pending。
func (s *Service) RequestCertification(ctx context.Context, agentID, creatorID uuid.UUID) error {
	rows, err := s.queries.RequestCertification(ctx, db.RequestCertificationParams{
		ID:        agentID,
		CreatorID: creatorID,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.RequestCertification")
		return httpx.Internal("申请认证失败")
	}
	if rows == 0 {
		// 区分：不属于该 creator vs 状态不允许重复触发
		a, getErr := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
			ID:        agentID,
			CreatorID: creatorID,
		})
		if getErr != nil {
			if errors.Is(getErr, pgx.ErrNoRows) {
				return httpx.NotFound("Agent 不存在")
			}
			log.Error().Err(getErr).Msg("agent.RequestCertification: GetAgentByIDForOwner")
			return httpx.Internal("查询 Agent 失败")
		}
		return httpx.Conflict("当前认证状态 " + a.CertificationStatus + " 不允许重复申请")
	}
	return nil
}

// CertifyAgent 运营授予认证。pending → certified。
func (s *Service) CertifyAgent(ctx context.Context, agentID uuid.UUID) error {
	rows, err := s.queries.CertifyAgent(ctx, agentID)
	if err != nil {
		log.Error().Err(err).Msg("agent.CertifyAgent")
		return httpx.Internal("授予认证失败")
	}
	if rows == 0 {
		a, getErr := s.queries.GetAgentByID(ctx, agentID)
		if getErr != nil {
			if errors.Is(getErr, pgx.ErrNoRows) {
				return httpx.NotFound("Agent 不存在")
			}
			log.Error().Err(getErr).Msg("agent.CertifyAgent: GetAgentByID")
			return httpx.Internal("查询 Agent 失败")
		}
		return httpx.Conflict("当前认证状态 " + a.CertificationStatus + " 不允许授予认证")
	}
	return nil
}

// RejectCertification 运营拒绝认证申请。pending → rejected。
func (s *Service) RejectCertification(ctx context.Context, agentID uuid.UUID, reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return httpx.BadRequest("拒绝原因不能为空")
	}
	rows, err := s.queries.RejectCertification(ctx, db.RejectCertificationParams{
		ID:              agentID,
		RejectionReason: reason,
	})
	if err != nil {
		log.Error().Err(err).Msg("agent.RejectCertification")
		return httpx.Internal("拒绝认证失败")
	}
	if rows == 0 {
		a, getErr := s.queries.GetAgentByID(ctx, agentID)
		if getErr != nil {
			if errors.Is(getErr, pgx.ErrNoRows) {
				return httpx.NotFound("Agent 不存在")
			}
			log.Error().Err(getErr).Msg("agent.RejectCertification: GetAgentByID")
			return httpx.Internal("查询 Agent 失败")
		}
		return httpx.Conflict("当前认证状态 " + a.CertificationStatus + " 不允许拒绝认证")
	}
	return nil
}

// isValidSlug 校验 slug 满足长度 + 字符 regex。
func isValidSlug(s string) bool {
	if len(s) < minSlugLen || len(s) > maxSlugLen {
		return false
	}
	return slugPattern.MatchString(s)
}

// normalizeAuthHeader 空串 → nil（schema 列允许 NULL）。
func normalizeAuthHeader(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}

// normalizeTagsForInsert 去前后空格、过滤空 tag、转小写。
// validator 已约束长度 / 数量；这里保证写入数据规范。
//
// 与 market_service.go 中的 normalizeTags（输出归一化）职责不同：
// 此函数处理输入；那边处理输出，故拆成两个函数。
func normalizeTagsForInsert(in []string) []string {
	out := make([]string, 0, len(in))
	for _, t := range in {
		t = strings.ToLower(strings.TrimSpace(t))
		if t == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// toAgentResponse db.Agent → API DTO。
//
// Status 字段从三维状态机派生，保留给老前端读：
//   - lifecycle=disabled         → "disabled"
//   - cert=pending               → "pending"
//   - cert=rejected              → "rejected"
//   - 否则                       → "approved"
//
// 新前端应直接读 lifecycle_status / visibility / certification_status。
func toAgentResponse(a *db.Agent) AgentResponse {
	resp := AgentResponse{
		ID:                  a.ID.String(),
		Slug:                a.Slug,
		Name:                a.Name,
		Description:         a.Description,
		EndpointURL:         a.EndpointURL,
		PricePerCallCents:   a.PricePerCallCents,
		Tags:                normalizeTags(a.Tags),
		Status:              deriveLegacyStatus(a),
		LifecycleStatus:     a.LifecycleStatus,
		Visibility:          a.Visibility,
		CertificationStatus: a.CertificationStatus,
		RejectionReason:     a.RejectionReason,
		TotalCalls:          a.TotalCalls,
		TotalRevenueCents:   a.TotalRevenueCents,
		ConnectionMode:      a.ConnectionMode,
		MCPToolName:         a.MCPToolName,
		CreatedAt:           a.CreatedAt.UTC().Format(time.RFC3339),
	}
	if a.CertifiedAt != nil {
		ts := a.CertifiedAt.UTC().Format(time.RFC3339)
		resp.CertifiedAt = &ts
	}
	return resp
}

// deriveLegacyStatus 用三维字段还原老的 status 文案（仅给老消费者读）。
func deriveLegacyStatus(a *db.Agent) string {
	switch {
	case a.LifecycleStatus == "disabled":
		return "disabled"
	case a.CertificationStatus == "pending":
		return "pending"
	case a.CertificationStatus == "rejected":
		return "rejected"
	default:
		return "approved"
	}
}

func (s *Service) markAvailabilityAfterDryRun(ctx context.Context, agentID uuid.UUID, result, errMsg string) Availability {
	var (
		snapshot db.AgentAvailabilitySnapshot
		err      error
	)
	if result == "pass" {
		snapshot, err = s.queries.MarkAgentAvailabilitySuccess(ctx, agentID)
	} else {
		snapshot, err = s.queries.MarkAgentAvailabilityFailure(ctx, agentID)
	}
	if err != nil {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Str("result", result).Str("error", errMsg).
			Msg("agent.markAvailabilityAfterDryRun")
		return s.agentAvailability(ctx, agentID)
	}
	return availabilityFromSnapshot(snapshot)
}

func availabilityFromSnapshot(snapshot db.AgentAvailabilitySnapshot) Availability {
	return availabilityResponse(
		snapshot.AvailabilityStatus,
		snapshot.LastSuccessfulRunAt,
		snapshot.LastFailedRunAt,
		snapshot.LastCheckedAt,
		snapshot.ConsecutiveFailures,
	)
}

func (s *Service) agentAvailability(ctx context.Context, agentID uuid.UUID) Availability {
	snapshot, err := s.queries.GetAgentAvailabilitySnapshot(ctx, agentID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.agentAvailability")
		}
		return availabilityResponse("unknown", nil, nil, nil, 0)
	}
	return availabilityFromSnapshot(snapshot)
}

func repairHintsForDryRun(agent *db.Agent, errMsg string) []string {
	if strings.TrimSpace(errMsg) == "" {
		return nil
	}
	hints := []string{}
	switch agent.ConnectionMode {
	case ConnectionModeRuntimeWS:
		hints = append(hints,
			"确认本地 Agent 进程正在运行，并使用含 agent:pull scope、绑定当前 Agent 的访问令牌连接 /api/v1/agent-runtime/ws。",
			"如果 WebSocket 被代理或网络中断阻断，可临时降级到 heartbeat + claim?wait=25 + result。",
		)
	case ConnectionModeRuntimePull:
		hints = append(hints,
			"确认本地 Agent 进程正在运行，并使用含 agent:pull scope、绑定当前 Agent 的访问令牌轮询任务。",
			"如果刚重启过 Agent，先调用 /api/v1/agent-runtime/heartbeat 刷新可用性。",
		)
	case ConnectionModeMCPServer:
		hints = append(hints,
			"确认 MCP Server 地址可被 OpenLinker 访问，且工具名与 mcp_tool_name 配置一致。",
			"检查示例 input 是否能被目标 MCP tool 接受，并返回 JSON object。",
		)
	default:
		hints = append(hints,
			"确认 endpoint_url 可访问、认证头有效，并在超时时间内返回 2xx。",
			"确认响应是 JSON object；如返回 { output: {...} }，平台会自动读取 output。",
		)
	}
	if strings.Contains(errMsg, "schema") || strings.Contains(errMsg, "不匹配") {
		hints = append(hints, "根据当前 input_schema / output_schema 调整示例或 Agent 输出字段。")
	}
	if strings.Contains(strings.ToLower(errMsg), "timeout") || strings.Contains(errMsg, "超时") {
		hints = append(hints, "缩短 Agent 首包响应时间，或把长任务改成先返回运行已接收再通过事件上报进度。")
	}
	return hints
}

func (s *Service) ensureOnboardingStatusBestEffort(ctx context.Context, agentID uuid.UUID) {
	if err := s.queries.EnsureOnboardingStatus(ctx, agentID); err != nil && !isUndefinedTable(err) {
		log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("agent.ensureOnboardingStatusBestEffort")
	}
}

func (s *Service) ensureAgentOwner(ctx context.Context, agentID, creatorID uuid.UUID) error {
	if _, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: creatorID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("agent.ensureAgentOwner")
		return httpx.Internal("查询 Agent 失败")
	}
	return nil
}

func toCapabilityResponse(c *db.AgentCapability) CapabilityResponse {
	return CapabilityResponse{
		ID:           c.ID.String(),
		AgentID:      c.AgentID.String(),
		InputSchema:  decodeJSONMap(c.InputSchema),
		OutputSchema: decodeJSONMap(c.OutputSchema),
		Summary:      c.Summary,
		Version:      c.Version,
		PublishedAt:  c.PublishedAt.UTC().Format(time.RFC3339),
		UpdatedAt:    c.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func toExampleResponse(e *db.AgentExample) ExampleResponse {
	resp := ExampleResponse{
		ID:        e.ID.String(),
		AgentID:   e.AgentID.String(),
		Title:     e.Title,
		InputJSON: decodeJSONMap(e.InputJSON),
		SortOrder: e.SortOrder,
		CreatedAt: e.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt: e.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if len(e.ExpectedOutputJSON) > 0 {
		resp.ExpectedOutputJSON = decodeJSONMap(e.ExpectedOutputJSON)
	}
	return resp
}

func toOnboardingStatusResponse(s *db.AgentOnboardingStatus) OnboardingStatusResponse {
	resp := OnboardingStatusResponse{
		AgentID:          s.AgentID.String(),
		EndpointSet:      s.EndpointSet,
		CapabilitiesSet:  s.CapabilitiesSet,
		ExamplesSet:      s.ExamplesSet,
		DryRunPassed:     s.DryRunPassed,
		DryRunLastResult: s.DryRunLastResult,
		DryRunError:      s.DryRunError,
		UpdatedAt:        s.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if s.DryRunAt != nil {
		ts := s.DryRunAt.UTC().Format(time.RFC3339)
		resp.DryRunAt = &ts
	}
	return resp
}

func decodeJSONMap(raw []byte) map[string]interface{} {
	out := map[string]interface{}{}
	if len(raw) == 0 {
		return out
	}
	_ = json.Unmarshal(raw, &out)
	return out
}

// isUniqueViolation Postgres UNIQUE 约束冲突（SQLSTATE 23505）。
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	type sqlState interface{ SQLState() string }
	var ss sqlState
	if errors.As(err, &ss) {
		return ss.SQLState() == "23505"
	}
	return false
}

// isCheckViolation Postgres CHECK 约束冲突（SQLSTATE 23514）。
func isCheckViolation(err error) bool {
	if err == nil {
		return false
	}
	type sqlState interface{ SQLState() string }
	var ss sqlState
	if errors.As(err, &ss) {
		return ss.SQLState() == "23514"
	}
	return false
}

func isUndefinedTable(err error) bool {
	if err == nil {
		return false
	}
	type sqlState interface{ SQLState() string }
	var ss sqlState
	if errors.As(err, &ss) {
		return ss.SQLState() == "42P01"
	}
	return false
}
