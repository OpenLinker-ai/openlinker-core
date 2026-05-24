// Package skill 实现 Skill 注册表（30 个内置 skill）+ Agent ↔ Skill 关联管理。
// 子轮 2.3 引入。任务驱动推荐（子轮 2.4）通过 Service.RecommendAgentsBySkills 调用。
package skill

import "github.com/google/uuid"

// SkillItem 单个 skill 的对外 DTO。
type SkillItem struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Name        string `json:"name"`
	Description string `json:"description"`
	SortOrder   int32  `json:"sort_order"`
}

// SetSkillsRequest 创作者绑定 skill 列表。
type SetSkillsRequest struct {
	// SkillIDs Agent 声明的 skill_id 列表，最多 5 个；重复 / 空串视为非法。
	SkillIDs []string `json:"skill_ids" validate:"required"`
}

// SetSkillsResponse 绑定后回写最新列表。
type SetSkillsResponse struct {
	AgentID string      `json:"agent_id"`
	Items   []SkillItem `json:"items"`
}

// AgentMatch 任务驱动推荐结果（供 2.4 task 模块直接使用）。
//
// MatchCount 是输入 skill 中命中的数量，用于排序与"匹配度"展示；
// VerifiedCount 是命中 skill 中已 verified 的子集，用于"可信度"加权（模块 B）；
// TotalCalls 用作热度 tie-break。
type AgentMatch struct {
	AgentID       uuid.UUID
	MatchCount    int32
	VerifiedCount int32
	TotalCalls    int32
}
