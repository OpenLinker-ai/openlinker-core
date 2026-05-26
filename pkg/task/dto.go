// Package task 实现子轮 2.4 的"任务驱动 A 形态"：
//
//	用户自然语言描述任务 → LLM/规则解析 skill → 推荐 Top 3 Agent → 用户选择。
//
// 与 internal/skill 协作：本模块只消费 SkillRecommender 接口，不直接读 skill 表。
package task

import (
	"github.com/google/uuid"
)

// RecommendRequest 推荐请求体。Query 长度由 schema CHECK 与 validator 双重保障。
type RecommendRequest struct {
	Query string `json:"query" validate:"required,min=4,max=500"`
}

// AgentSummary 推荐返回的 Agent 简要信息（不含 endpoint / 鉴权头）。
//
// AvgRating 暂时用 4.8 占位（评分系统后续子轮接入）。
type AgentSummary struct {
	ID                string   `json:"id"`
	Slug              string   `json:"slug"`
	Name              string   `json:"name"`
	Description       string   `json:"description"`
	PricePerCallCents int32    `json:"price_per_call_cents"`
	TotalCalls        int32    `json:"total_calls"`
	AvgRating         float32  `json:"avg_rating"`
	CreatorName       string   `json:"creator_name"`
	Tags              []string `json:"tags"`
}

// SkillRef 是任务发布流对 Skill catalog 的稳定引用。
type SkillRef struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// Recommendation 单条推荐：Agent + 匹配分 + 解释。
type Recommendation struct {
	Agent         AgentSummary `json:"agent"`
	MatchScore    float32      `json:"match_score"` // [0,1]
	Why           string       `json:"why"`         // 中文解释，如 "匹配 SQL 查询 + 数据分析"
	MatchedSkills []SkillRef   `json:"matched_skills"`
}

// RecommendResponse 推荐响应。
//
// TaskID 用于后续 POST /tasks/:id/choose；空数组表示无匹配，前端可提示用户改写描述。
type RecommendResponse struct {
	TaskID          uuid.UUID        `json:"task_id"`
	ParsedSkills    []string         `json:"parsed_skills"`
	ParsedSkillRefs []SkillRef       `json:"parsed_skill_refs"`
	Recommendations []Recommendation `json:"recommendations"`
}

// ChooseRequest 用户选定推荐里某个 Agent 的请求体。
type ChooseRequest struct {
	AgentID uuid.UUID `json:"agent_id" validate:"required"`
}

// HistoryItem "我的任务"列表项（GET /tasks/me）。
type HistoryItem struct {
	ID                  string   `json:"id"`
	Query               string   `json:"query"`
	ParsedSkills        []string `json:"parsed_skills"`
	RecommendedAgentIDs []string `json:"recommended_agent_ids"`
	ChosenAgentID       *string  `json:"chosen_agent_id,omitempty"`
	ChosenAt            *string  `json:"chosen_at,omitempty"`
	CreatedAt           string   `json:"created_at"`
}

// DetailResponse GET /tasks/:id 详情响应。
//
// 用于冷链接（直接打开 /tasks/<id> URL，sessionStorage 无缓存）时
// 让前端依然能渲染 3 张推荐卡。recommendations 按 recommended_agent_ids 顺序回填；
// 若某 agent 已下架则跳过该位置。
type DetailResponse struct {
	ID              string           `json:"id"`
	Query           string           `json:"query"`
	ParsedSkills    []string         `json:"parsed_skills"`
	ParsedSkillRefs []SkillRef       `json:"parsed_skill_refs"`
	ChosenAgentID   *string          `json:"chosen_agent_id,omitempty"`
	ChosenAt        *string          `json:"chosen_at,omitempty"`
	CreatedAt       string           `json:"created_at"`
	Recommendations []Recommendation `json:"recommendations"`
}
