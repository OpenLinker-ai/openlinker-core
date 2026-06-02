package task

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/llm"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
)

const (
	maxTaskSkillRefs        = 5
	maxTaskMCPTools         = 5
	maxTaskResultSummaryLen = 2000
	maxTaskRevisionNoteLen  = 2000
	maxTaskPublicSummaryLen = 240
	taskVisibilityPrivate   = "private"
	taskVisibilityPublic    = "public"
)

var mcpToolCatalog = []MCPToolRef{
	{Name: "create_task", Description: "发布自然语言任务，解析 Skill/MCP 引用并返回推荐 Agent"},
	{Name: "search_agents", Description: "按关键词或标签搜索市场里的 Agent"},
	{Name: "get_agent", Description: "读取单个 Agent 的详情、能力声明和示例"},
	{Name: "run_agent", Description: "调用选定 Agent 并记录一次运行"},
	{Name: "get_run", Description: "查询一次 Agent 调用的运行状态和结果"},
}

// AgentMatch skill 模块返回给推荐器的最小信息。
//
// 本模块独立定义同名结构是为避免 internal/task 与 internal/skill 的潜在循环依赖，
// 调用方（main.go）需把 internal/skill 的实现适配到 SkillRecommender 接口。
type AgentMatch struct {
	AgentID    uuid.UUID
	MatchCount int32
}

// SkillRecommender skill 模块对外暴露给本模块的能力。由 internal/skill.Service 实现。
//
//	ListAll                     — 启动时取全部 skill catalog（用于 LLM prompt + 规则匹配）
//	RecommendAgentsBySkills     — 给定 skill_id 列表，返回命中数量最多的 Agent
type SkillRecommender interface {
	ListAll(ctx context.Context) ([]db.Skill, error)
	RecommendAgentsBySkills(ctx context.Context, skillIDs []string, limit int) ([]AgentMatch, error)
}

// RuntimeStarter 是任务直接运行 Agent 时需要的最小 runtime 能力。
type RuntimeStarter interface {
	StartRun(ctx context.Context, userID uuid.UUID, req *runtime.RunRequest, source string) (*runtime.RunResponse, error)
}

// Service 任务驱动 A 形态业务逻辑。
//
// allSkills 在 NewService 时一次性预热，后续仅读不写；30 个固定值，内存可忽略。
type Service struct {
	pool      *pgxpool.Pool
	queries   *db.Queries
	llm       llm.Client
	skillSvc  SkillRecommender
	runner    RuntimeStarter
	allSkills []db.Skill
}

// NewService 构造 Service，并立即预热 skill catalog。
//
// 任何启动期失败（skillSvc.ListAll 出错）记 warn 但仍返回 Service —— 后续 Recommend
// 调用会重新尝试加载，避免启动顺序耦合。
func NewService(pool *pgxpool.Pool, llmClient llm.Client, skillSvc SkillRecommender) *Service {
	s := &Service{
		pool:     pool,
		queries:  db.New(pool),
		llm:      llmClient,
		skillSvc: skillSvc,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	skills, err := skillSvc.ListAll(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("task.NewService: warmup ListAll failed")
		return s
	}
	s.allSkills = skills
	log.Info().Int("skill_count", len(skills)).Bool("llm_enabled", llmClient != nil).
		Msg("task service ready")
	return s
}

// SetRunStarter 注入 runtime.Service，使任务详情可以直接启动一次 Agent run。
func (s *Service) SetRunStarter(runner RuntimeStarter) {
	s.runner = runner
}

// Recommend 主流程：解析 → 推荐 → 回填 → 持久化。
func (s *Service) Recommend(ctx context.Context, userID uuid.UUID, req *RecommendRequest) (*RecommendResponse, error) {
	if req == nil {
		return nil, httpx.Unprocessable("请求体不能为空")
	}
	query := req.Query
	query = strings.TrimSpace(query)
	if n := len(query); n < 4 || n > 500 {
		return nil, httpx.Unprocessable("query 长度需在 4-500 字符之间")
	}

	// 0. 兜底刷新 catalog（启动时 warmup 失败的场景）
	skills := s.allSkills
	if len(skills) == 0 {
		var err error
		skills, err = s.skillSvc.ListAll(ctx)
		if err != nil {
			log.Error().Err(err).Msg("task.Recommend: ListAll")
			return nil, httpx.Internal("加载 skill 列表失败")
		}
		s.allSkills = skills
	}
	skillByID := skillCatalogByID(skills)

	explicitSkills, err := normalizeExplicitSkillIDs(req.SkillIDs, skillByID)
	if err != nil {
		return nil, err
	}
	mcpTools, err := normalizeMCPTools(req.MCPTools)
	if err != nil {
		return nil, err
	}

	// 1. 解析 skill：优先 LLM；失败/未配置 → 规则
	var parsed []string
	if s.llm != nil {
		out, err := llmParse(ctx, s.llm, query, skills)
		if err != nil {
			log.Warn().Err(err).Str("query", query).Msg("task.Recommend: llmParse fallback to rule")
		} else {
			parsed = out
		}
	}
	if len(parsed) == 0 {
		parsed = ruleParse(query, skills)
	}
	parsed = mergeSkillIDs(explicitSkills, parsed, maxTaskSkillRefs)

	// 2. 解析空 → 写入 task_query 但 recommendations 为空，便于离线追踪 miss
	resp := &RecommendResponse{
		Visibility:      taskVisibilityPrivate,
		ParsedSkills:    parsed,
		ParsedSkillRefs: skillRefsForIDs(parsed, skillByID),
		MCPTools:        append([]string{}, mcpTools...),
		MCPToolRefs:     mcpToolRefsForNames(mcpTools),
		Recommendations: []Recommendation{},
	}

	if len(parsed) == 0 {
		taskID, err := s.persist(ctx, userID, query, parsed, mcpTools, nil)
		if err != nil {
			return nil, err
		}
		resp.TaskID = taskID
		resp.NextAction = nextActionForNeedsAgent(taskID, "暂未识别到足够稳定的 Skill，任务已先保存为私有草稿")
		return resp, nil
	}

	// 3. 调 skill 推荐器
	matches, err := s.skillSvc.RecommendAgentsBySkills(ctx, parsed, 3)
	if err != nil {
		log.Error().Err(err).Strs("skills", parsed).Msg("task.Recommend: RecommendAgentsBySkills")
		return nil, httpx.Internal("推荐 Agent 失败")
	}

	if len(matches) == 0 {
		taskID, err := s.persist(ctx, userID, query, parsed, mcpTools, nil)
		if err != nil {
			return nil, err
		}
		resp.TaskID = taskID
		resp.NextAction = nextActionForNeedsAgent(taskID, "已识别 Skill，但当前没有可推荐的公开 Agent")
		return resp, nil
	}

	// 4. 回填 Agent 详情
	agentIDs := make([]uuid.UUID, len(matches))
	for i := range matches {
		agentIDs[i] = matches[i].AgentID
	}
	rows, err := s.queries.GetAgentsByIDs(ctx, agentIDs)
	if err != nil {
		log.Error().Err(err).Msg("task.Recommend: GetAgentsByIDs")
		return nil, httpx.Internal("加载 Agent 详情失败")
	}
	byID := make(map[uuid.UUID]*db.GetAgentsByIDsRow, len(rows))
	for i := range rows {
		byID[rows[i].ID] = &rows[i]
	}

	// skill_id → 中文名（用于 Why 文案）
	nameByID := skillNameByID(skills)
	parsedCount := float32(len(parsed))

	recs := make([]Recommendation, 0, len(matches))
	for i := range matches {
		row, ok := byID[matches[i].AgentID]
		if !ok {
			// 推荐了但回填不到（可能被下架）→ 跳过
			continue
		}
		score := float32(matches[i].MatchCount) / parsedCount
		if score > 1 {
			score = 1
		}
		matchedSkills, err := s.matchedSkillRefs(ctx, matches[i].AgentID, parsed)
		if err != nil {
			log.Error().Err(err).Str("agent_id", matches[i].AgentID.String()).Msg("task.Recommend: ListAgentSkills")
			return nil, httpx.Internal("加载 Agent Skill 详情失败")
		}
		recs = append(recs, Recommendation{
			Agent:         toAgentSummary(row),
			MatchScore:    score,
			Why:           buildWhy(parsed, nameByID),
			MatchedSkills: matchedSkills,
		})
	}

	// 5. 持久化（用回填后实际有效的 agent_id 顺序）
	storedIDs := make([]uuid.UUID, 0, len(recs))
	for i := range recs {
		id, _ := uuid.Parse(recs[i].Agent.ID)
		storedIDs = append(storedIDs, id)
	}
	taskID, err := s.persist(ctx, userID, query, parsed, mcpTools, storedIDs)
	if err != nil {
		return nil, err
	}
	resp.TaskID = taskID
	resp.Recommendations = recs
	if len(recs) == 0 {
		resp.NextAction = nextActionForNeedsAgent(taskID, "候选 Agent 当前不可公开推荐或已不可用")
	}
	return resp, nil
}

// Publish 把私有推荐草稿显式发布到任务广场。公开列表只展示 public_summary。
func (s *Service) Publish(ctx context.Context, taskID, userID uuid.UUID, req *PublishRequest) (*DetailResponse, error) {
	t, err := s.queries.GetTaskQuery(ctx, taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("任务不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Publish: GetTaskQuery")
		return nil, httpx.Internal("查询任务失败")
	}
	if t.UserID != userID {
		return nil, httpx.NotFound("任务不存在")
	}
	if t.Visibility == taskVisibilityPublic {
		return s.GetByID(ctx, taskID, userID)
	}
	if t.CompletedAt != nil {
		return nil, httpx.Conflict("任务已完成，不能发布到任务广场")
	}
	summary, err := normalizePublicSummary(req, t.Query)
	if err != nil {
		return nil, err
	}
	published, err := s.queries.PublishTaskQuery(ctx, db.PublishTaskQueryParams{
		ID:            taskID,
		UserID:        userID,
		PublicSummary: summary,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Conflict("任务状态已变化，请刷新后重试")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Publish: PublishTaskQuery")
		return nil, httpx.Internal("发布任务失败")
	}
	return s.detailFromTask(ctx, &published)
}

// Choose 用户在推荐里选定一个 Agent。校验：
//
//   - task_id 必须属于该 user（否则 404，不暴露存在性）
//   - agent_id 必须出现在 recommended_agent_ids 里（否则 400）
func (s *Service) Choose(ctx context.Context, taskID, userID, agentID uuid.UUID) error {
	t, err := s.queries.GetTaskQuery(ctx, taskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("任务不存在")
		}
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Choose: GetTaskQuery")
		return httpx.Internal("查询任务失败")
	}
	if t.UserID != userID {
		return httpx.NotFound("任务不存在")
	}

	in := false
	for _, id := range t.RecommendedAgentIDs {
		if id == agentID {
			in = true
			break
		}
	}
	if !in {
		return httpx.BadRequest("agent_id 不在推荐列表里")
	}

	if _, err := s.queries.MarkTaskQueryChosen(ctx, db.MarkTaskQueryChosenParams{
		ID:            taskID,
		UserID:        userID,
		ChosenAgentID: agentID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("任务不存在")
		}
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Choose: MarkTaskQueryChosen")
		return httpx.Internal("更新任务失败")
	}
	return nil
}

// Claim 让创作者用自己的 Agent 接入公开任务。它只登记任务工作关系；
// 实际运行仍走 /runs，成功后再调用 Complete 写回结果。
func (s *Service) Claim(ctx context.Context, taskID, userID, agentID uuid.UUID) (*WorkResponse, error) {
	if agentID == uuid.Nil {
		return nil, httpx.Unprocessable("agent_id 不能为空")
	}
	agent, err := s.queries.GetAgentByIDForOwner(ctx, db.GetAgentByIDForOwnerParams{
		ID:        agentID,
		CreatorID: userID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("task.Claim: GetAgentByIDForOwner")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.LifecycleStatus != "active" || agent.Visibility == "private" {
		return nil, httpx.Conflict("Agent 当前不可用于接任务")
	}

	t, err := s.queries.GetTaskQuery(ctx, taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("任务不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Claim: GetTaskQuery")
		return nil, httpx.Internal("查询任务失败")
	}
	if t.CompletedAt != nil {
		return nil, httpx.Conflict("任务已完成，不能重复接入")
	}
	if t.Visibility != taskVisibilityPublic {
		return nil, httpx.Conflict("任务还是私有草稿，发布到任务广场后才能接入")
	}
	if t.ClaimedAgentID != nil {
		return nil, httpx.Conflict("任务已经有 Agent 接入")
	}
	if t.ChosenAgentID != nil {
		return nil, httpx.Conflict("任务发布者已经选择了 Agent")
	}

	claimed, err := s.queries.ClaimTaskQuery(ctx, db.ClaimTaskQueryParams{
		ID:      taskID,
		UserID:  userID,
		AgentID: agentID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Conflict("任务已经被接入或完成")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Claim: ClaimTaskQuery")
		return nil, httpx.Internal("接入任务失败")
	}
	return toWorkResponse(&claimed, agentID), nil
}

// Complete 把一次成功 run 绑定回任务。任务发布者选择推荐 Agent 后可完成；
// 任务广场接单方也可在自己的 Agent 跑完后完成。
func (s *Service) Complete(ctx context.Context, taskID, userID uuid.UUID, req *CompleteRequest) (*WorkResponse, error) {
	if req == nil {
		return nil, httpx.Unprocessable("请求体不能为空")
	}
	summary := strings.TrimSpace(req.ResultSummary)
	if summary == "" || len(summary) > maxTaskResultSummaryLen {
		return nil, httpx.Unprocessable("result_summary 长度需在 1-2000 字符之间")
	}
	if req.AgentID == uuid.Nil || req.RunID == uuid.Nil {
		return nil, httpx.Unprocessable("agent_id 和 run_id 不能为空")
	}

	t, err := s.queries.GetTaskQuery(ctx, taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("任务不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Complete: GetTaskQuery")
		return nil, httpx.Internal("查询任务失败")
	}
	if t.CompletedAt != nil && t.DeliveryStatus != "revision_requested" {
		return nil, httpx.Conflict("任务已完成，不能重复提交结果")
	}
	allowed := t.UserID == userID
	if t.ClaimedByUserID != nil && *t.ClaimedByUserID == userID {
		allowed = true
	}
	if !allowed {
		return nil, httpx.NotFound("任务不存在")
	}

	if t.ClaimedAgentID != nil {
		if *t.ClaimedAgentID != req.AgentID {
			return nil, httpx.Conflict("run 的 Agent 与接入任务的 Agent 不一致")
		}
	} else if t.ChosenAgentID != nil {
		if *t.ChosenAgentID != req.AgentID {
			return nil, httpx.Conflict("run 的 Agent 与任务选择的 Agent 不一致")
		}
	} else {
		return nil, httpx.Conflict("任务还没有接入 Agent")
	}

	run, err := s.queries.GetRunByID(ctx, req.RunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("运行记录不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", req.RunID.String()).Msg("task.Complete: GetRunByID")
		return nil, httpx.Internal("查询运行记录失败")
	}
	if run.UserID != userID || run.AgentID != req.AgentID {
		return nil, httpx.NotFound("运行记录不存在")
	}
	if run.Status != "success" {
		return nil, httpx.Conflict("只有成功完成的 run 才能写回任务结果")
	}
	artifact := req.ResultArtifact
	if artifact == nil {
		artifact = map[string]interface{}{}
		if len(run.Output) > 0 {
			_ = json.Unmarshal(run.Output, &artifact)
		}
	}
	artifactJSON, err := json.Marshal(artifact)
	if err != nil {
		return nil, httpx.BadRequest("result_artifact 不是合法 JSON")
	}
	visibility := normalizeDeliveryVisibility(req.DeliveryVisibility)

	completed, err := s.queries.CompleteTaskQuery(ctx, db.CompleteTaskQueryParams{
		ID:                 taskID,
		UserID:             userID,
		AgentID:            req.AgentID,
		CompletionRunID:    req.RunID,
		CompletionSummary:  summary,
		DeliveryArtifact:   artifactJSON,
		DeliveryVisibility: visibility,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Conflict("任务状态已变化，请刷新后重试")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.Complete: CompleteTaskQuery")
		return nil, httpx.Internal("提交任务结果失败")
	}
	return toWorkResponse(&completed, req.AgentID), nil
}

// AcceptDelivery marks a submitted task result as accepted. Only the task
// poster can accept a delivery.
func (s *Service) AcceptDelivery(ctx context.Context, taskID, userID uuid.UUID) (*WorkResponse, error) {
	accepted, err := s.queries.AcceptTaskDelivery(ctx, db.AcceptTaskDeliveryParams{
		ID:     taskID,
		UserID: userID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Conflict("任务结果不可验收，请确认已提交且未验收")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.AcceptDelivery")
		return nil, httpx.Internal("验收任务失败")
	}
	agentID := uuid.Nil
	if accepted.ClaimedAgentID != nil {
		agentID = *accepted.ClaimedAgentID
	} else if accepted.ChosenAgentID != nil {
		agentID = *accepted.ChosenAgentID
	}
	return toWorkResponse(&accepted, agentID), nil
}

// RequestRevision asks the worker to resubmit a completed task delivery.
func (s *Service) RequestRevision(ctx context.Context, taskID, userID uuid.UUID, req *RevisionRequest) (*WorkResponse, error) {
	if req == nil {
		return nil, httpx.Unprocessable("请求体不能为空")
	}
	note := strings.TrimSpace(req.Note)
	if note == "" || len(note) > maxTaskRevisionNoteLen {
		return nil, httpx.Unprocessable("note 长度需在 1-2000 字符之间")
	}
	revision, err := s.queries.RequestTaskRevision(ctx, db.RequestTaskRevisionParams{
		ID:           taskID,
		UserID:       userID,
		RevisionNote: note,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.Conflict("任务结果不可要求修订，请确认已提交且未验收")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.RequestRevision")
		return nil, httpx.Internal("请求修订失败")
	}
	agentID := uuid.Nil
	if revision.ClaimedAgentID != nil {
		agentID = *revision.ClaimedAgentID
	} else if revision.ChosenAgentID != nil {
		agentID = *revision.ChosenAgentID
	}
	return toWorkResponse(&revision, agentID), nil
}

// RunTask 从任务详情启动一次运行。它要求任务已经被发布者选择 Agent，
// 或已经由创作者接入；结果终态仍由 Complete 写回，便于 async run 统一处理。
func (s *Service) RunTask(ctx context.Context, taskID, userID uuid.UUID, req *RunTaskRequest) (*RunTaskResponse, error) {
	if s.runner == nil {
		return nil, httpx.Internal("任务运行服务未配置")
	}
	if req == nil {
		return nil, httpx.Unprocessable("请求体不能为空")
	}
	if req.AgentID == uuid.Nil {
		return nil, httpx.Unprocessable("agent_id 不能为空")
	}
	t, err := s.queries.GetTaskQuery(ctx, taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("任务不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.RunTask: GetTaskQuery")
		return nil, httpx.Internal("查询任务失败")
	}
	if t.CompletedAt != nil && t.DeliveryStatus != "revision_requested" {
		return nil, httpx.Conflict("任务已完成，不能重复运行")
	}
	allowed := t.UserID == userID
	if t.ClaimedByUserID != nil && *t.ClaimedByUserID == userID {
		allowed = true
	}
	if !allowed {
		return nil, httpx.NotFound("任务不存在")
	}

	if t.ClaimedAgentID != nil {
		if *t.ClaimedAgentID != req.AgentID {
			return nil, httpx.Conflict("只能运行已接入任务的 Agent")
		}
	} else if t.ChosenAgentID != nil {
		if *t.ChosenAgentID != req.AgentID {
			return nil, httpx.Conflict("只能运行任务发布者已选择的 Agent")
		}
	} else {
		return nil, httpx.Conflict("请先选择或接入 Agent")
	}

	input := req.Input
	if input == nil {
		input = map[string]interface{}{"text": runnableTaskText(&t, userID)}
	}
	metadata := map[string]interface{}{
		"task_id": taskID.String(),
		"source":  "task",
	}
	if usedTools := taskRunUsedMCPTools(t.MCPTools); len(usedTools) > 0 {
		metadata["used_mcp_tools"] = usedTools
	}

	resp, err := s.runner.StartRun(ctx, userID, &runtime.RunRequest{
		AgentID:  req.AgentID.String(),
		Input:    input,
		Metadata: metadata,
	}, "web")
	if err != nil {
		return nil, err
	}
	return &RunTaskResponse{
		TaskID: taskID.String(),
		Status: taskStatus(&t),
		Run:    resp,
	}, nil
}

// ListMine 用户最近 limit 条任务历史（默认上限 20）。
func (s *Service) ListMine(ctx context.Context, userID uuid.UUID, limit int32) ([]HistoryItem, error) {
	if limit <= 0 || limit > 20 {
		limit = 20
	}
	rows, err := s.queries.ListTaskQueriesByUser(ctx, db.ListTaskQueriesByUserParams{
		UserID: userID,
		Limit:  limit,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("task.ListMine: query")
		return nil, httpx.Internal("查询任务历史失败")
	}
	out := make([]HistoryItem, 0, len(rows))
	for i := range rows {
		out = append(out, toHistoryItem(&rows[i]))
	}
	return out, nil
}

// ListBoard 返回任务广场最近公开任务。列表不暴露发布者身份。
func (s *Service) ListBoard(ctx context.Context, limit int32) ([]PublicTaskItem, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	rows, err := s.queries.ListPublicTaskQueries(ctx, limit)
	if err != nil {
		log.Error().Err(err).Msg("task.ListBoard: query")
		return nil, httpx.Internal("查询任务广场失败")
	}
	skills := s.allSkills
	if len(skills) == 0 {
		if loaded, err := s.skillSvc.ListAll(ctx); err == nil {
			skills = loaded
			s.allSkills = loaded
		} else {
			log.Warn().Err(err).Msg("task.ListBoard: ListAll")
		}
	}
	skillByID := skillCatalogByID(skills)
	out := make([]PublicTaskItem, 0, len(rows))
	for i := range rows {
		out = append(out, toPublicTaskItem(&rows[i], skillByID))
	}
	return out, nil
}

// GetByID 取单个任务 + 回填推荐卡。用于冷链接（sessionStorage 缓存丢失）。
//
// 权限：task 必须属于该 user，否则 404。
// recommendations 按 recommended_agent_ids 顺序回填，跳过已下架/找不到的 agent。
// parsed_skills 用于生成 Why 文案，保持与 Recommend 一致。
func (s *Service) GetByID(ctx context.Context, taskID, userID uuid.UUID) (*DetailResponse, error) {
	t, err := s.queries.GetTaskQuery(ctx, taskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("任务不存在")
		}
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("task.GetByID: GetTaskQuery")
		return nil, httpx.Internal("查询任务失败")
	}
	if t.UserID != userID {
		return nil, httpx.NotFound("任务不存在")
	}

	return s.detailFromTask(ctx, &t)
}

func (s *Service) detailFromTask(ctx context.Context, t *db.TaskQuery) (*DetailResponse, error) {
	resp := &DetailResponse{
		ID:            t.ID.String(),
		Query:         t.Query,
		Visibility:    normalizedTaskVisibility(t.Visibility),
		PublicSummary: copyStringPtr(t.PublicSummary),
		ParsedSkills:  append([]string{}, t.ParsedSkills...),
		MCPTools:      append([]string{}, t.MCPTools...),
		MCPToolRefs:   mcpToolRefsForNames(t.MCPTools),
		Status:        taskStatus(t),
		CreatedAt:     t.CreatedAt.UTC().Format(time.RFC3339),

		Recommendations: []Recommendation{},
	}
	if t.PublishedAt != nil {
		ts := t.PublishedAt.UTC().Format(time.RFC3339)
		resp.PublishedAt = &ts
	}
	if t.ChosenAgentID != nil {
		s := t.ChosenAgentID.String()
		resp.ChosenAgentID = &s
	}
	if t.ChosenAt != nil {
		ts := t.ChosenAt.UTC().Format(time.RFC3339)
		resp.ChosenAt = &ts
	}
	attachTaskWorkFields(t, &resp.ClaimedAgentID, &resp.ClaimedByUserID, &resp.ClaimedAt, &resp.CompletionRunID, &resp.CompletedAt, &resp.CompletionSummary)
	attachTaskDeliveryFields(t, &resp.DeliveryStatus, &resp.DeliveryVisibility, &resp.DeliveryArtifact, &resp.AcceptedAt, &resp.RevisionRequestedAt, &resp.RevisionNote)

	// 用 catalog 回填 Why 文案（与 Recommend 一致）
	skills := s.allSkills
	if len(skills) == 0 {
		if loaded, err := s.skillSvc.ListAll(ctx); err == nil {
			skills = loaded
			s.allSkills = loaded
		}
	}
	skillByID := skillCatalogByID(skills)
	resp.ParsedSkillRefs = skillRefsForIDs(t.ParsedSkills, skillByID)

	if len(t.RecommendedAgentIDs) == 0 {
		if t.Visibility != taskVisibilityPublic {
			resp.NextAction = nextActionForNeedsAgent(t.ID, "当前任务没有可直接推荐的 Agent")
		}
		return resp, nil
	}

	rows, err := s.queries.GetAgentsByIDs(ctx, t.RecommendedAgentIDs)
	if err != nil {
		log.Error().Err(err).Msg("task.GetByID: GetAgentsByIDs")
		return nil, httpx.Internal("加载 Agent 详情失败")
	}
	byID := make(map[uuid.UUID]*db.GetAgentsByIDsRow, len(rows))
	for i := range rows {
		byID[rows[i].ID] = &rows[i]
	}
	nameByID := skillNameByID(skills)

	parsedCount := float32(len(t.ParsedSkills))
	if parsedCount == 0 {
		parsedCount = 1
	}
	for _, aid := range t.RecommendedAgentIDs {
		row, ok := byID[aid]
		if !ok {
			continue
		}
		matchedSkills, err := s.matchedSkillRefs(ctx, aid, t.ParsedSkills)
		if err != nil {
			log.Error().Err(err).Str("agent_id", aid.String()).Msg("task.GetByID: ListAgentSkills")
			return nil, httpx.Internal("加载 Agent Skill 详情失败")
		}
		score := float32(len(matchedSkills)) / parsedCount
		if score > 1 {
			score = 1
		}
		resp.Recommendations = append(resp.Recommendations, Recommendation{
			Agent:         toAgentSummary(row),
			MatchScore:    score,
			Why:           buildWhy(t.ParsedSkills, nameByID),
			MatchedSkills: matchedSkills,
		})
	}
	if len(resp.Recommendations) == 0 && t.Visibility != taskVisibilityPublic {
		resp.NextAction = nextActionForNeedsAgent(t.ID, "历史候选 Agent 当前不可用")
	}
	return resp, nil
}

// persist 写入 task_queries 并返回 id；recommended 为 nil 时存空数组。
func (s *Service) persist(ctx context.Context, userID uuid.UUID, query string, parsed []string, mcpTools []string, recommended []uuid.UUID) (uuid.UUID, error) {
	if recommended == nil {
		recommended = []uuid.UUID{}
	}
	if parsed == nil {
		parsed = []string{}
	}
	if mcpTools == nil {
		mcpTools = []string{}
	}
	t, err := s.queries.CreateTaskQuery(ctx, db.CreateTaskQueryParams{
		UserID:              userID,
		Query:               query,
		ParsedSkills:        parsed,
		MCPTools:            mcpTools,
		RecommendedAgentIDs: recommended,
	})
	if err != nil {
		log.Error().Err(err).Str("user_id", userID.String()).Msg("task.persist: CreateTaskQuery")
		return uuid.Nil, httpx.Internal("保存任务记录失败")
	}
	return t.ID, nil
}

// buildWhy 按解析顺序生成中文文案，如 "匹配 SQL 查询 + 数据分析"。
func buildWhy(parsed []string, nameByID map[string]string) string {
	parts := make([]string, 0, len(parsed))
	for _, id := range parsed {
		if name, ok := nameByID[id]; ok {
			parts = append(parts, name)
		} else {
			parts = append(parts, id)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "匹配 " + strings.Join(parts, " + ")
}

func skillNameByID(skills []db.Skill) map[string]string {
	out := make(map[string]string, len(skills))
	for i := range skills {
		out[skills[i].ID] = skills[i].Name
	}
	return out
}

func skillCatalogByID(skills []db.Skill) map[string]db.Skill {
	out := make(map[string]db.Skill, len(skills))
	for i := range skills {
		out[skills[i].ID] = skills[i]
	}
	return out
}

func normalizeExplicitSkillIDs(ids []string, byID map[string]db.Skill) ([]string, error) {
	if len(ids) > maxTaskSkillRefs {
		return nil, httpx.Unprocessable("skill_ids 最多关联 5 个")
	}
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			return nil, httpx.Unprocessable("skill_ids 不能包含空值")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		if _, ok := byID[id]; !ok {
			return nil, httpx.Unprocessable("未知 Skill: " + id)
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

func mergeSkillIDs(explicit []string, parsed []string, max int) []string {
	if max <= 0 {
		return []string{}
	}
	out := make([]string, 0, max)
	seen := make(map[string]struct{}, max)
	add := func(id string) {
		if len(out) >= max {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, id := range explicit {
		add(id)
	}
	for _, id := range parsed {
		add(id)
	}
	return out
}

func normalizeMCPTools(names []string) ([]string, error) {
	if len(names) > maxTaskMCPTools {
		return nil, httpx.Unprocessable("mcp_tools 最多关联 5 个")
	}
	byName := mcpToolCatalogByName()
	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			return nil, httpx.Unprocessable("mcp_tools 不能包含空值")
		}
		if _, ok := seen[name]; ok {
			continue
		}
		if _, ok := byName[name]; !ok {
			return nil, httpx.Unprocessable("未知 MCP 工具: " + name)
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out, nil
}

func normalizePublicSummary(req *PublishRequest, query string) (string, error) {
	summary := ""
	if req != nil {
		summary = strings.TrimSpace(req.PublicSummary)
	}
	if summary == "" {
		summary = publicSummaryFromQuery(query)
	}
	summary = compactWhitespace(summary)
	runeLen := len([]rune(summary))
	if runeLen < 4 || runeLen > maxTaskPublicSummaryLen {
		return "", httpx.Unprocessable("public_summary 长度需在 4-240 字符之间")
	}
	return summary, nil
}

func publicSummaryFromQuery(query string) string {
	summary := compactWhitespace(query)
	return truncateRunes(summary, maxTaskPublicSummaryLen)
}

func publicTaskSummary(t *db.TaskQuery) string {
	if t.PublicSummary != nil && strings.TrimSpace(*t.PublicSummary) != "" {
		return *t.PublicSummary
	}
	return publicSummaryFromQuery(t.Query)
}

func runnableTaskText(t *db.TaskQuery, userID uuid.UUID) string {
	if t.UserID != userID && normalizedTaskVisibility(t.Visibility) == taskVisibilityPublic {
		return publicTaskSummary(t)
	}
	return t.Query
}

func workResponseQuery(t *db.TaskQuery) string {
	if normalizedTaskVisibility(t.Visibility) == taskVisibilityPublic {
		return publicTaskSummary(t)
	}
	return t.Query
}

func normalizedTaskVisibility(raw string) string {
	if raw == taskVisibilityPublic {
		return taskVisibilityPublic
	}
	return taskVisibilityPrivate
}

func nextActionForNeedsAgent(taskID uuid.UUID, reason string) *TaskNextAction {
	return &TaskNextAction{
		Type:   "publish_task",
		Label:  "发布到任务广场",
		Hint:   "当前任务是私有推荐草稿。发布公开摘要后，创作者可以用自己的 Agent 接入并开始处理。",
		Href:   "/tasks/" + taskID.String(),
		Reason: reason,
	}
}

func compactWhitespace(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func truncateRunes(value string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

func copyStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func mcpToolRefsForNames(names []string) []MCPToolRef {
	byName := mcpToolCatalogByName()
	out := make([]MCPToolRef, 0, len(names))
	for _, name := range names {
		if ref, ok := byName[name]; ok {
			out = append(out, ref)
		}
	}
	return out
}

func mcpToolCatalogByName() map[string]MCPToolRef {
	out := make(map[string]MCPToolRef, len(mcpToolCatalog))
	for _, tool := range mcpToolCatalog {
		out[tool.Name] = tool
	}
	return out
}

func skillRefsForIDs(ids []string, byID map[string]db.Skill) []SkillRef {
	out := make([]SkillRef, 0, len(ids))
	for _, id := range ids {
		if s, ok := byID[id]; ok {
			out = append(out, skillRefFromSkill(s))
			continue
		}
		out = append(out, SkillRef{ID: id, Name: id})
	}
	return out
}

func skillRefFromSkill(s db.Skill) SkillRef {
	return SkillRef{
		ID:          s.ID,
		Category:    s.Category,
		Name:        s.Name,
		Description: s.Description,
	}
}

func (s *Service) matchedSkillRefs(ctx context.Context, agentID uuid.UUID, parsed []string) ([]SkillRef, error) {
	if len(parsed) == 0 {
		return []SkillRef{}, nil
	}
	declared, err := s.queries.ListAgentSkills(ctx, agentID)
	if err != nil {
		return nil, err
	}
	declaredByID := make(map[string]db.Skill, len(declared))
	for i := range declared {
		declaredByID[declared[i].ID] = declared[i]
	}
	out := make([]SkillRef, 0, len(parsed))
	for _, id := range parsed {
		if s, ok := declaredByID[id]; ok {
			out = append(out, skillRefFromSkill(s))
		}
	}
	return out, nil
}

// toAgentSummary 把 GetAgentsByIDsRow 转成响应 DTO。
func toAgentSummary(r *db.GetAgentsByIDsRow) AgentSummary {
	return AgentSummary{
		ID:                r.ID.String(),
		Slug:              r.Slug,
		Name:              r.Name,
		Description:       r.Description,
		PricePerCallCents: r.PricePerCallCents,
		TotalCalls:        r.TotalCalls,
		CreatorName:       r.CreatorName,
		Tags:              append([]string{}, r.Tags...),
	}
}

// toHistoryItem db.TaskQuery → HistoryItem。
func toHistoryItem(t *db.TaskQuery) HistoryItem {
	item := HistoryItem{
		ID:                  t.ID.String(),
		Query:               t.Query,
		Visibility:          normalizedTaskVisibility(t.Visibility),
		PublicSummary:       copyStringPtr(t.PublicSummary),
		ParsedSkills:        append([]string{}, t.ParsedSkills...),
		MCPTools:            append([]string{}, t.MCPTools...),
		RecommendedAgentIDs: make([]string, 0, len(t.RecommendedAgentIDs)),
		Status:              taskStatus(t),
		CreatedAt:           t.CreatedAt.UTC().Format(time.RFC3339),
	}
	if t.PublishedAt != nil {
		ts := t.PublishedAt.UTC().Format(time.RFC3339)
		item.PublishedAt = &ts
	}
	for _, id := range t.RecommendedAgentIDs {
		item.RecommendedAgentIDs = append(item.RecommendedAgentIDs, id.String())
	}
	if t.ChosenAgentID != nil {
		s := t.ChosenAgentID.String()
		item.ChosenAgentID = &s
	}
	if t.ChosenAt != nil {
		ts := t.ChosenAt.UTC().Format(time.RFC3339)
		item.ChosenAt = &ts
	}
	attachTaskWorkFields(t, &item.ClaimedAgentID, &item.ClaimedByUserID, &item.ClaimedAt, &item.CompletionRunID, &item.CompletedAt, &item.CompletionSummary)
	attachTaskDeliveryFields(t, &item.DeliveryStatus, &item.DeliveryVisibility, nil, &item.AcceptedAt, &item.RevisionRequestedAt, &item.RevisionNote)
	return item
}

func toPublicTaskItem(t *db.TaskQuery, skillByID map[string]db.Skill) PublicTaskItem {
	publicSummary := publicTaskSummary(t)
	item := PublicTaskItem{
		ID:                    t.ID.String(),
		Query:                 publicSummary,
		PublicSummary:         publicSummary,
		ParsedSkills:          append([]string{}, t.ParsedSkills...),
		ParsedSkillRefs:       skillRefsForIDs(t.ParsedSkills, skillByID),
		MCPTools:              append([]string{}, t.MCPTools...),
		MCPToolRefs:           mcpToolRefsForNames(t.MCPTools),
		RecommendedAgentCount: len(t.RecommendedAgentIDs),
		Status:                taskStatus(t),
		CreatedAt:             t.CreatedAt.UTC().Format(time.RFC3339),
	}
	if t.PublishedAt != nil {
		ts := t.PublishedAt.UTC().Format(time.RFC3339)
		item.PublishedAt = &ts
	}
	if t.ClaimedAgentID != nil {
		s := t.ClaimedAgentID.String()
		item.ClaimedAgentID = &s
	}
	if t.ClaimedAt != nil {
		ts := t.ClaimedAt.UTC().Format(time.RFC3339)
		item.ClaimedAt = &ts
	}
	if t.CompletedAt != nil {
		ts := t.CompletedAt.UTC().Format(time.RFC3339)
		item.CompletedAt = &ts
	}
	item.DeliveryStatus = t.DeliveryStatus
	if item.DeliveryStatus == "" {
		item.DeliveryStatus = "pending"
	}
	return item
}

func taskStatus(t *db.TaskQuery) string {
	switch {
	case t.DeliveryStatus == "accepted":
		return "accepted"
	case t.DeliveryStatus == "revision_requested":
		return "revision_requested"
	case t.CompletedAt != nil:
		return "completed"
	case t.ClaimedAgentID != nil:
		return "in_progress"
	case t.ChosenAgentID != nil:
		return "matched"
	case len(t.RecommendedAgentIDs) == 0:
		return "needs_agent"
	default:
		return "open"
	}
}

func normalizeDeliveryVisibility(raw string) string {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "shared", "public_example":
		return raw
	default:
		return "private"
	}
}

func taskRunUsedMCPTools(required []string) []string {
	out := make([]string, 0, 1)
	for _, tool := range required {
		if strings.TrimSpace(tool) == "run_agent" {
			out = append(out, "run_agent")
			break
		}
	}
	return out
}

func deliveryArtifact(raw []byte) DeliveryArtifact {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return DeliveryArtifact(out)
}

func attachTaskDeliveryFields(
	t *db.TaskQuery,
	deliveryStatus *string,
	deliveryVisibility *string,
	deliveryArtifactOut *DeliveryArtifact,
	acceptedAt **string,
	revisionRequestedAt **string,
	revisionNote **string,
) {
	if deliveryStatus != nil {
		*deliveryStatus = t.DeliveryStatus
		if *deliveryStatus == "" {
			*deliveryStatus = "pending"
		}
	}
	if deliveryVisibility != nil {
		*deliveryVisibility = t.DeliveryVisibility
		if *deliveryVisibility == "" {
			*deliveryVisibility = "private"
		}
	}
	if deliveryArtifactOut != nil {
		*deliveryArtifactOut = deliveryArtifact(t.DeliveryArtifact)
	}
	if acceptedAt != nil && t.AcceptedAt != nil {
		ts := t.AcceptedAt.UTC().Format(time.RFC3339)
		*acceptedAt = &ts
	}
	if revisionRequestedAt != nil && t.RevisionRequestedAt != nil {
		ts := t.RevisionRequestedAt.UTC().Format(time.RFC3339)
		*revisionRequestedAt = &ts
	}
	if revisionNote != nil && t.RevisionNote != nil {
		s := *t.RevisionNote
		*revisionNote = &s
	}
}

func attachTaskWorkFields(
	t *db.TaskQuery,
	claimedAgentID **string,
	claimedByUserID **string,
	claimedAt **string,
	completionRunID **string,
	completedAt **string,
	completionSummary **string,
) {
	if t.ClaimedAgentID != nil {
		s := t.ClaimedAgentID.String()
		*claimedAgentID = &s
	}
	if t.ClaimedByUserID != nil {
		s := t.ClaimedByUserID.String()
		*claimedByUserID = &s
	}
	if t.ClaimedAt != nil {
		ts := t.ClaimedAt.UTC().Format(time.RFC3339)
		*claimedAt = &ts
	}
	if t.CompletionRunID != nil {
		s := t.CompletionRunID.String()
		*completionRunID = &s
	}
	if t.CompletedAt != nil {
		ts := t.CompletedAt.UTC().Format(time.RFC3339)
		*completedAt = &ts
	}
	if t.CompletionSummary != nil {
		s := *t.CompletionSummary
		*completionSummary = &s
	}
}

func toWorkResponse(t *db.TaskQuery, agentID uuid.UUID) *WorkResponse {
	resp := &WorkResponse{
		TaskID:  t.ID.String(),
		Status:  taskStatus(t),
		Query:   workResponseQuery(t),
		AgentID: agentID.String(),
	}
	if t.ClaimedAt != nil {
		ts := t.ClaimedAt.UTC().Format(time.RFC3339)
		resp.ClaimedAt = &ts
	}
	if t.CompletionRunID != nil {
		s := t.CompletionRunID.String()
		resp.CompletionRunID = &s
	}
	if t.CompletedAt != nil {
		ts := t.CompletedAt.UTC().Format(time.RFC3339)
		resp.CompletedAt = &ts
	}
	if t.CompletionSummary != nil {
		s := *t.CompletionSummary
		resp.CompletionSummary = &s
	}
	attachTaskDeliveryFields(t, &resp.DeliveryStatus, &resp.DeliveryVisibility, nil, &resp.AcceptedAt, &resp.RevisionRequestedAt, &resp.RevisionNote)
	return resp
}
