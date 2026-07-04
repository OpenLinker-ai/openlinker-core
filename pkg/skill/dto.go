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

// CreateSkillProposalRequest 是用户提交缺失 Skill / 导入声明后的提案请求。
type CreateSkillProposalRequest struct {
	AgentID         *string `json:"agent_id,omitempty"`
	ProposedSkillID string  `json:"proposed_skill_id" validate:"required,min=3,max=120"`
	Category        string  `json:"category" validate:"required,min=2,max=80"`
	Name            string  `json:"name" validate:"required,min=1,max=120"`
	Description     string  `json:"description" validate:"required,min=4,max=1000"`
	Source          string  `json:"source,omitempty" validate:"omitempty,oneof=manual imported_text imported_json"`
}

// SkillProposalItem 是 Skill Proposal 的对外 DTO。
type SkillProposalItem struct {
	ID              string  `json:"id"`
	AgentID         *string `json:"agent_id,omitempty"`
	ProposedSkillID string  `json:"proposed_skill_id"`
	Category        string  `json:"category"`
	Name            string  `json:"name"`
	Description     string  `json:"description"`
	Source          string  `json:"source"`
	Status          string  `json:"status"`
	MatchedSkillID  *string `json:"matched_skill_id,omitempty"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
}

// SkillProposalListResponse 是创作者侧提案列表。
type SkillProposalListResponse struct {
	Items []SkillProposalItem `json:"items"`
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
