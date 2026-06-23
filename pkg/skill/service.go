package skill

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// MaxSkillsPerAgent 单个 Agent 最多可声明的 skill 数量（PRD：5 个上限）。
const MaxSkillsPerAgent = 5

// Service Skill 业务逻辑层。
type Service struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

// NewService 构造 Service。
func NewService(pool *pgxpool.Pool) *Service {
	return &Service{
		pool: pool,
		q:    db.New(pool),
	}
}

// ListAll 返回平台全部内置 skill（公开，给 /publish 表单与发现页用）。
func (s *Service) ListAll(ctx context.Context) ([]db.Skill, error) {
	items, err := s.q.ListSkills(ctx)
	if err != nil {
		log.Error().Err(err).Msg("skill.ListAll: ListSkills")
		return nil, httpx.Internal("查询 skill 列表失败")
	}
	return items, nil
}

// ListForAgent 返回某 Agent 已声明的 skill 详情。
func (s *Service) ListForAgent(ctx context.Context, agentID uuid.UUID) ([]db.Skill, error) {
	items, err := s.q.ListAgentSkills(ctx, agentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("skill.ListForAgent: ListAgentSkills")
		return nil, httpx.Internal("查询 Agent skill 失败")
	}
	return items, nil
}

// SetAgentSkills 用 skillIDs 覆盖某 Agent 的关联（事务内 DELETE + 批量 INSERT）。
//
// 校验：
//  1. 数量 <= MaxSkillsPerAgent；
//  2. 去重后非空串；
//  3. 每个 id 必须存在于 skills 表（否则 400 报告第一个非法 id）。
//
// 调用方负责鉴权：仅 Agent.creator_id == 当前用户 时才允许调用。
func (s *Service) SetAgentSkills(ctx context.Context, agentID uuid.UUID, skillIDs []string) error {
	cleaned, err := normalizeSkillIDs(skillIDs)
	if err != nil {
		return err
	}

	// 校验每个 skill_id 存在性（5 条上限，N+1 查询可接受）。
	for _, sid := range cleaned {
		if _, err := s.q.GetSkill(ctx, sid); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return httpx.BadRequest(fmt.Sprintf("skill_id 不存在: %s", sid))
			}
			log.Error().Err(err).Str("skill_id", sid).Msg("skill.SetAgentSkills: GetSkill")
			return httpx.Internal("校验 skill 失败")
		}
	}

	// 事务覆盖：DELETE 旧的 + 批量 INSERT 新的。
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return db.ReplaceAgentSkills(ctx, tx, agentID, cleaned)
	})
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("skill.SetAgentSkills: ReplaceAgentSkills")
		return httpx.Internal("更新 Agent skill 失败")
	}
	return nil
}

// RecommendAgentsBySkills 任务驱动推荐：按命中 skill 数量降序返回 Agent 列表。
//
// 由子轮 2.4 task 模块调用：传入 LLM 解析出的 skill_id 列表 + top-N，
// 拿到候选 Agent。limit <= 0 时不截断。
//
// runtime_pull Agent 复用市场 readiness，并把最近 token 使用作为 fresh 在线信号：
// 只有 healthy、成功运行或近期 runtime token 证据的 Agent 才进入候选，避免推荐到无人领取或已不可达的运行时。
// 排序：match_count desc → availability → recent online/success evidence → verified_count desc → total_calls desc → agent_id（稳定）。
// verified_count 来自 agent_skill_scores（模块 B 写入），把 verified 过的命中数当作信任加权。
func (s *Service) RecommendAgentsBySkills(ctx context.Context, skillIDs []string, limit int) ([]AgentMatch, error) {
	cleaned := dedupNonEmpty(skillIDs)
	if len(cleaned) == 0 {
		return []AgentMatch{}, nil
	}
	rows, err := s.q.ListAgentsBySkillsWithVerified(ctx, cleaned)
	if err != nil {
		log.Error().Err(err).Msg("skill.RecommendAgentsBySkills: ListAgentsBySkillsWithVerified")
		return nil, httpx.Internal("查询推荐 Agent 失败")
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]AgentMatch, len(rows))
	for i := range rows {
		out[i] = AgentMatch{
			AgentID:       rows[i].AgentID,
			MatchCount:    rows[i].MatchCount,
			VerifiedCount: rows[i].VerifiedCount,
			TotalCalls:    rows[i].TotalCalls,
		}
	}
	return out, nil
}

// normalizeSkillIDs trim + 去空 + 去重 + 上限检查；返回 [] 表示清空。
func normalizeSkillIDs(in []string) ([]string, error) {
	cleaned := dedupNonEmpty(in)
	if len(cleaned) > MaxSkillsPerAgent {
		return nil, httpx.BadRequest(fmt.Sprintf("最多只能选择 %d 个 skill", MaxSkillsPerAgent))
	}
	return cleaned, nil
}

// dedupNonEmpty trim 空白 + 去空串 + 去重，保持原顺序。
func dedupNonEmpty(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
