package skill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/llm"
)

// 模块 B：Skill Benchmark 执行器。
//
// 触发 → 异步 worker 跑 N 条测试用例（每条调 Agent endpoint + LLM judge）→
// 写 agent_skill_benchmark_runs → 聚合到 agent_skill_scores。

// EndpointRunner 抽象 Agent endpoint 调用，由 runtime.Service 实现（通过 DryRun 方法满足）。
// 返回 (output, errMsg)。errMsg 非空视为失败，output 可能为 nil。
type EndpointRunner interface {
	DryRun(ctx context.Context, agent *db.Agent, input map[string]interface{}) (map[string]interface{}, string)
}

// BenchmarkService 负责 Skill Benchmark 的触发、执行、聚合。
//
// 复用 skill.Service 持有的 pool / queries，不再单开连接。
type BenchmarkService struct {
	parent *Service
	q      *db.Queries
	runner EndpointRunner
	llm    llm.Client
	// 等待中 / 进行中的 batch 防止并发重复触发：key = agent_id|skill_id。
	inflight   sync.Map
	judgeRegex *regexp.Regexp
}

// NewBenchmarkService 构造。runner / llmClient 都可为 nil（service 会返回 503）。
func NewBenchmarkService(parent *Service, runner EndpointRunner, llmClient llm.Client) *BenchmarkService {
	return &BenchmarkService{
		parent:     parent,
		q:          parent.q,
		runner:     runner,
		llm:        llmClient,
		judgeRegex: regexp.MustCompile(`\{output\}`),
	}
}

// RunBenchmark 创作者触发某 skill 的 benchmark。
//
// 校验：
//  1. Agent 归属 creatorID
//  2. skill_id 已被 agent 声明（agent_skills）
//  3. 该 skill 已 seed 测试用例
//  4. EndpointRunner + LLM 已就绪
//
// 触发成功后异步启动 worker，立即返回 batch_id。
func (b *BenchmarkService) RunBenchmark(ctx context.Context, agentID, creatorID uuid.UUID, skillID string) (*RunBenchmarkResponse, error) {
	if b.runner == nil || b.llm == nil {
		return nil, httpx.ServiceUnavailable("benchmark 服务暂不可用（缺少 endpoint runner 或 LLM）")
	}
	skillID = strings.TrimSpace(skillID)
	if skillID == "" {
		return nil, httpx.BadRequest("skill_id 不能为空")
	}

	agent, err := b.q.GetAgentByID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("benchmark.RunBenchmark: GetAgentByID")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.CreatorID != creatorID {
		return nil, httpx.NotFound("Agent 不存在")
	}

	declared, err := b.q.ListAgentSkills(ctx, agentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("benchmark.RunBenchmark: ListAgentSkills")
		return nil, httpx.Internal("查询 Agent skill 失败")
	}
	if !skillDeclared(declared, skillID) {
		return nil, httpx.BadRequest("该 Agent 未声明此 skill，请先在能力声明中添加")
	}

	cases, err := b.q.ListTestCasesBySkill(ctx, skillID)
	if err != nil {
		log.Error().Err(err).Str("skill_id", skillID).Msg("benchmark.RunBenchmark: ListTestCasesBySkill")
		return nil, httpx.Internal("查询测试用例失败")
	}
	if len(cases) == 0 {
		return nil, httpx.Unprocessable("该 skill 暂未配置测试用例，无法 benchmark")
	}

	key := agentID.String() + "|" + skillID
	if _, loaded := b.inflight.LoadOrStore(key, time.Now()); loaded {
		return nil, httpx.Conflict("该 skill 的 benchmark 正在执行中，请稍候")
	}

	// 立即把 status 置 pending，避免界面显示旧的 verified/failed
	if _, err := b.q.UpsertAgentSkillScore(ctx, db.UpsertAgentSkillScoreParams{
		AgentID:    agentID,
		SkillID:    skillID,
		Status:     BenchmarkStatusPending,
		TotalCount: int32(len(cases)),
	}); err != nil {
		b.inflight.Delete(key)
		log.Error().Err(err).Msg("benchmark.RunBenchmark: pre-mark pending")
		return nil, httpx.Internal("初始化 benchmark 状态失败")
	}

	batchID := uuid.New()
	go b.runBatch(agent, skillID, batchID, cases, key)

	return &RunBenchmarkResponse{
		BatchID: batchID.String(),
		SkillID: skillID,
		Status:  "running",
	}, nil
}

// runBatch 异步执行一次 benchmark 的全部测试用例。新建独立 ctx，避免请求 ctx 取消。
func (b *BenchmarkService) runBatch(agent db.Agent, skillID string, batchID uuid.UUID, cases []db.SkillTestCase, inflightKey string) {
	// 全 batch 30 分钟兜底
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	defer b.inflight.Delete(inflightKey)

	scores := make([]int32, 0, len(cases))
	for i := range cases {
		caseRow := cases[i]
		run, err := b.q.CreateBenchmarkRun(ctx, db.CreateBenchmarkRunParams{
			BatchID:    batchID,
			AgentID:    agent.ID,
			SkillID:    skillID,
			TestCaseID: caseRow.ID,
		})
		if err != nil {
			log.Error().Err(err).Str("batch_id", batchID.String()).Str("case_id", caseRow.ID.String()).
				Msg("benchmark.runBatch: CreateBenchmarkRun")
			continue
		}
		score, ok := b.runCase(ctx, &agent, &caseRow, run.ID)
		if ok {
			scores = append(scores, score)
		}
	}

	b.aggregate(ctx, agent.ID, skillID, batchID, scores, len(cases))
}

// runCase 执行单条 case：调 endpoint → LLM judge → 写结果。返回 score + 是否成功。
func (b *BenchmarkService) runCase(ctx context.Context, agent *db.Agent, caseRow *db.SkillTestCase, runID uuid.UUID) (int32, bool) {
	// 单条 case 5 分钟兜底（endpoint 60s + judge 15s + 缓冲）。
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	var input map[string]interface{}
	if err := json.Unmarshal(caseRow.InputJSON, &input); err != nil {
		b.markFailed(ctx, runID, "测试用例 input_json 解析失败: "+err.Error())
		return 0, false
	}

	output, errMsg := b.runner.DryRun(callCtx, agent, input)
	if errMsg != "" {
		b.markFailed(ctx, runID, "endpoint 调用失败: "+errMsg)
		return 0, false
	}
	if output == nil {
		b.markFailed(ctx, runID, "endpoint 返回为空")
		return 0, false
	}

	rawOutput, err := json.Marshal(output)
	if err != nil {
		b.markFailed(ctx, runID, "endpoint 输出序列化失败: "+err.Error())
		return 0, false
	}

	score, reasoning, err := b.judge(callCtx, caseRow.JudgePrompt, output)
	if err != nil {
		b.markFailed(ctx, runID, "judge 失败: "+err.Error())
		return 0, false
	}

	if _, err := b.q.MarkBenchmarkRunSuccess(ctx, db.MarkBenchmarkRunSuccessParams{
		ID:             runID,
		Score:          score,
		RawOutput:      rawOutput,
		JudgeReasoning: reasoning,
	}); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("benchmark.runCase: MarkBenchmarkRunSuccess")
		return 0, false
	}
	return score, true
}

// judge 调用 LLM 给输出打分。
//
// 期望 LLM 返回 JSON：{"score": 0-100, "reason": "..."}。
// 如果解析失败兜底用正则抓数字；都失败返回 error。
func (b *BenchmarkService) judge(ctx context.Context, promptTpl string, output map[string]interface{}) (int32, string, error) {
	outputJSON, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return 0, "", err
	}
	userPrompt := b.judgeRegex.ReplaceAllString(promptTpl, string(outputJSON))
	system := "你是 Agent 输出质量评审员，必须以 JSON 形式返回 {\"score\": 0-100 整数, \"reason\": \"中文一句话点评\"}，不要任何额外文字。"

	resp, err := b.llm.Complete(ctx, system, userPrompt)
	if err != nil {
		return 0, "", err
	}
	score, reason, err := parseJudgeResponse(resp)
	if err != nil {
		return 0, "", fmt.Errorf("解析评分失败: %w (raw=%s)", err, truncateText(resp, 200))
	}
	return clampScore(score), reason, nil
}

// markFailed 工具方法：把 run 标记为失败。
func (b *BenchmarkService) markFailed(ctx context.Context, runID uuid.UUID, msg string) {
	if _, err := b.q.MarkBenchmarkRunFailed(ctx, db.MarkBenchmarkRunFailedParams{
		ID:           runID,
		ErrorMessage: truncateText(msg, 500),
	}); err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("benchmark.markFailed")
	}
}

// aggregate 把单次 batch 的全部分数聚合到 agent_skill_scores。
func (b *BenchmarkService) aggregate(ctx context.Context, agentID uuid.UUID, skillID string, batchID uuid.UUID, scores []int32, totalCases int) {
	status := BenchmarkStatusFailed
	var avgPtr *int32
	var verifiedAt *time.Time
	passCount := int32(0)

	if len(scores) > 0 {
		var sum int32
		for _, s := range scores {
			sum += s
			if s >= VerifiedThreshold {
				passCount++
			}
		}
		avg := sum / int32(len(scores))
		avgPtr = &avg
		if avg >= VerifiedThreshold {
			status = BenchmarkStatusVerified
			now := time.Now().UTC()
			verifiedAt = &now
		}
	}

	bid := batchID
	if _, err := b.q.UpsertAgentSkillScore(ctx, db.UpsertAgentSkillScoreParams{
		AgentID:      agentID,
		SkillID:      skillID,
		Status:       status,
		AverageScore: avgPtr,
		PassCount:    passCount,
		TotalCount:   int32(totalCases),
		LastBatchID:  &bid,
		VerifiedAt:   verifiedAt,
	}); err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Str("skill_id", skillID).Msg("benchmark.aggregate: UpsertAgentSkillScore")
	}
}

// ListAgentScores 创作者中心 / 内部用：列出某 agent 全部 skill 评分。
func (b *BenchmarkService) ListAgentScores(ctx context.Context, agentID uuid.UUID) ([]SkillScoreItem, error) {
	rows, err := b.q.ListAgentSkillScoresByAgent(ctx, agentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("benchmark.ListAgentScores")
		return nil, httpx.Internal("查询 Agent skill 评分失败")
	}
	out := make([]SkillScoreItem, 0, len(rows))
	for i := range rows {
		out = append(out, toSkillScoreItem(&rows[i], ""))
	}
	return out, nil
}

// assertOwner 校验 agentID 归 creatorID 所有，否则统一返回 NotFound。
func (b *BenchmarkService) assertOwner(ctx context.Context, agentID, creatorID uuid.UUID) error {
	agent, err := b.q.GetAgentByID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("benchmark.assertOwner: GetAgentByID")
		return httpx.Internal("查询 Agent 失败")
	}
	if agent.CreatorID != creatorID {
		return httpx.NotFound("Agent 不存在")
	}
	return nil
}

// ListAgentScoresBySlug 公开详情页用：按 slug 查（限 approved Agent）。
func (b *BenchmarkService) ListAgentScoresBySlug(ctx context.Context, slug string) ([]SkillScoreItem, error) {
	rows, err := b.q.ListAgentSkillScoresBySlug(ctx, slug)
	if err != nil {
		log.Error().Err(err).Str("slug", slug).Msg("benchmark.ListAgentScoresBySlug")
		return nil, httpx.Internal("查询 Agent skill 评分失败")
	}
	out := make([]SkillScoreItem, 0, len(rows))
	for i := range rows {
		out = append(out, toSkillScoreItem(&rows[i], ""))
	}
	return out, nil
}

// GetBatchDetail 单次 batch 详情。
func (b *BenchmarkService) GetBatchDetail(ctx context.Context, agentID, creatorID, batchID uuid.UUID) (*BenchmarkBatchDetail, error) {
	// 校验归属
	agent, err := b.q.GetAgentByID(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("Agent 不存在")
		}
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("benchmark.GetBatchDetail: GetAgentByID")
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if agent.CreatorID != creatorID {
		return nil, httpx.NotFound("Agent 不存在")
	}

	rows, err := b.q.ListBenchmarkRunsByBatch(ctx, batchID)
	if err != nil {
		log.Error().Err(err).Str("batch_id", batchID.String()).Msg("benchmark.GetBatchDetail: ListBenchmarkRunsByBatch")
		return nil, httpx.Internal("查询 benchmark 明细失败")
	}
	if len(rows) == 0 || rows[0].AgentID != agentID {
		return nil, httpx.NotFound("benchmark 不存在")
	}

	skillID := rows[0].SkillID
	items := make([]BenchmarkRunItem, 0, len(rows))
	var sum, count int32
	allFinal := true
	for i := range rows {
		r := &rows[i]
		item := BenchmarkRunItem{
			ID:             r.ID.String(),
			TestCaseTitle:  r.TestCaseTitle,
			Status:         r.Status,
			Score:          r.Score,
			JudgeReasoning: r.JudgeReasoning,
			ErrorMessage:   r.ErrorMessage,
			StartedAt:      r.StartedAt.UTC().Format(time.RFC3339),
			FinishedAt:     formatTimePtr(r.FinishedAt),
		}
		items = append(items, item)
		if r.Status == "pending" {
			allFinal = false
		}
		if r.Status == "success" && r.Score != nil {
			sum += *r.Score
			count++
		}
	}

	detail := &BenchmarkBatchDetail{
		BatchID: batchID.String(),
		AgentID: agentID.String(),
		SkillID: skillID,
		Status:  "running",
		Items:   items,
	}
	if allFinal {
		if count > 0 {
			avg := sum / count
			detail.AverageScore = &avg
			if avg >= VerifiedThreshold {
				detail.Status = BenchmarkStatusVerified
			} else {
				detail.Status = BenchmarkStatusFailed
			}
		} else {
			detail.Status = BenchmarkStatusFailed
		}
	}
	return detail, nil
}

// ListTopAgents /skills 列表页：某 skill 下 top-N verified Agent。
func (b *BenchmarkService) ListTopAgents(ctx context.Context, skillID string, limit int) ([]TopAgentForSkill, error) {
	if limit <= 0 || limit > 10 {
		limit = 3
	}
	rows, err := b.q.ListTopAgentsBySkill(ctx, db.ListTopAgentsBySkillParams{
		SkillID: skillID,
		Limit:   int32(limit),
	})
	if err != nil {
		log.Error().Err(err).Str("skill_id", skillID).Msg("benchmark.ListTopAgents")
		return nil, httpx.Internal("查询 top agent 失败")
	}
	out := make([]TopAgentForSkill, 0, len(rows))
	for _, r := range rows {
		out = append(out, TopAgentForSkill{
			AgentID:           r.AgentID.String(),
			Slug:              r.Slug,
			Name:              r.Name,
			Description:       r.Description,
			Tags:              r.Tags,
			PricePerCallCents: r.PricePerCallCents,
			TotalCalls:        r.TotalCalls,
			AverageScore:      r.AverageScore,
			VerifiedAt:        formatTimePtr(r.VerifiedAt),
		})
	}
	return out, nil
}

// ──────────────────────────────────────────────────────
// 工具：判分解析、status mapping、字符串截断。
// ──────────────────────────────────────────────────────

// parseJudgeResponse 优先解析 JSON；失败时正则兜底抓 score 数字。
func parseJudgeResponse(raw string) (int32, string, error) {
	raw = strings.TrimSpace(raw)
	// 去掉 ```json``` 围栏（LLM 偶发）
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var parsed struct {
		Score  float64 `json:"score"`
		Reason string  `json:"reason"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
		return int32(parsed.Score), parsed.Reason, nil
	}

	// 兜底：抓第一个 0-100 的数字。
	scoreRegex := regexp.MustCompile(`(\d{1,3})`)
	matches := scoreRegex.FindStringSubmatch(raw)
	if len(matches) == 0 {
		return 0, "", errors.New("无法从响应中提取 score")
	}
	n, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, "", err
	}
	return int32(n), truncateText(raw, 200), nil
}

// clampScore 把 score 限制到 [0, 100]。
func clampScore(s int32) int32 {
	if s < 0 {
		return 0
	}
	if s > 100 {
		return 100
	}
	return s
}

// skillDeclared 判断 skillID 是否在 declared 列表中。
func skillDeclared(declared []db.Skill, skillID string) bool {
	for i := range declared {
		if declared[i].ID == skillID {
			return true
		}
	}
	return false
}

// toSkillScoreItem db.AgentSkillScore → DTO。
func toSkillScoreItem(s *db.AgentSkillScore, skillName string) SkillScoreItem {
	item := SkillScoreItem{
		SkillID:      s.SkillID,
		SkillName:    skillName,
		Status:       s.Status,
		AverageScore: s.AverageScore,
		PassCount:    s.PassCount,
		TotalCount:   s.TotalCount,
		LastBatchID:  uuidPtrString(s.LastBatchID),
		VerifiedAt:   formatTimePtr(s.VerifiedAt),
		UpdatedAt:    s.UpdatedAt.UTC().Format(time.RFC3339),
	}
	return item
}

// truncateText 截断字符串（防爆 DB / 日志）。
func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
