// Package task 实现子轮 2.4 的"任务驱动 A 形态"：
//
//	用户自然语言描述任务 → LLM/规则解析 skill → 推荐 Top 3 Agent → 用户选择。
//
// 与 internal/skill 协作：本模块只消费 SkillRecommender 接口，不直接读 skill 表。
package task

import (
	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// RecommendRequest 推荐请求体。Query 长度由 schema CHECK 与 validator 双重保障。
type RecommendRequest struct {
	Query      string   `json:"query" validate:"required,min=4,max=500"`
	TemplateID string   `json:"template_id,omitempty" validate:"omitempty,min=2,max=80"`
	SkillIDs   []string `json:"skill_ids,omitempty" validate:"omitempty,max=5,dive,min=1,max=80"`
	MCPTools   []string `json:"mcp_tools,omitempty" validate:"omitempty,max=5,dive,min=1,max=80"`
	AgentSlugs []string `json:"agent_slugs,omitempty" validate:"omitempty,max=5,dive,min=1,max=120"`
}

// AgentSummary 推荐返回的 Agent 简要信息（不含 endpoint / 鉴权头）。
type AgentSummary struct {
	ID                string   `json:"id"`
	Slug              string   `json:"slug"`
	Name              string   `json:"name"`
	Description       string   `json:"description"`
	PricePerCallCents int32    `json:"price_per_call_cents"`
	TotalCalls        int32    `json:"total_calls"`
	AvgRating         *float32 `json:"avg_rating,omitempty"`
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

// MCPToolRef 是任务发布流对 OpenLinker MCP 工具的稳定引用。
type MCPToolRef struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// TaskTemplateTranslation contains the user-facing fields that vary by locale.
// The top-level template fields remain the catalog's default copy so existing
// consumers can continue to read them without selecting a translation.
type TaskTemplateTranslation struct {
	Title        string `json:"title"`
	Summary      string `json:"summary"`
	ExampleQuery string `json:"example_query"`
}

// TaskTemplateResponse is the public catalog item that lowers the first-run
// burden without exposing or publishing a user's private task input.
type TaskTemplateResponse struct {
	ID                    string                             `json:"id"`
	Slug                  string                             `json:"slug"`
	Title                 string                             `json:"title"`
	Category              string                             `json:"category"`
	Summary               string                             `json:"summary"`
	Translations          map[string]TaskTemplateTranslation `json:"translations"`
	RequiredSkillIDs      []string                           `json:"required_skill_ids"`
	RequiredSkillRefs     []SkillRef                         `json:"required_skill_refs"`
	RequiredMCPTools      []string                           `json:"required_mcp_tools"`
	RequiredMCPToolRefs   []MCPToolRef                       `json:"required_mcp_tool_refs"`
	ExampleQuery          string                             `json:"example_query"`
	ExpectedArtifactTypes []string                           `json:"expected_artifact_types"`
	DefaultVisibility     string                             `json:"default_visibility"`
}

// Recommendation 单条推荐：Agent + 匹配分 + 解释。
type Recommendation struct {
	Agent         AgentSummary `json:"agent"`
	MatchScore    float32      `json:"match_score"` // [0,1]
	Why           string       `json:"why"`         // 中文解释，如 "匹配 SQL 查询 + 数据分析"
	MatchedSkills []SkillRef   `json:"matched_skills"`
}

// TaskNextAction 是推荐/详情页给人类和外部 Agent 的结构化下一步。
// 无匹配供给时返回 connect_agent，并把私有任务意图编码到站内 /publish 链接。
type TaskNextAction struct {
	Type       string `json:"type"`
	Label      string `json:"label"`
	Hint       string `json:"hint"`
	Href       string `json:"href"`
	ReasonCode string `json:"reason_code,omitempty"`
	Reason     string `json:"reason"`
}

// RecommendResponse 推荐响应。
//
// TaskID 用于后续 POST /tasks/:id/choose；空数组表示无匹配，前端可提示用户改写描述。
type RecommendResponse struct {
	TaskID          uuid.UUID        `json:"task_id"`
	Visibility      string           `json:"visibility"`
	ParsedSkills    []string         `json:"parsed_skills"`
	ParsedSkillRefs []SkillRef       `json:"parsed_skill_refs"`
	MCPTools        []string         `json:"mcp_tools"`
	MCPToolRefs     []MCPToolRef     `json:"mcp_tool_refs"`
	Recommendations []Recommendation `json:"recommendations"`
	NextAction      *TaskNextAction  `json:"next_action,omitempty"`
}

// ChooseRequest 用户选定推荐里某个 Agent 的请求体。
type ChooseRequest struct {
	AgentID uuid.UUID `json:"agent_id" validate:"required"`
}

// RunTaskRequest 从私有任务详情直接启动一次 Agent 运行。调用方必须为
// 每次语义运行提供稳定幂等键，不能只用 task_id 推导。
type RunTaskRequest struct {
	AgentID        uuid.UUID              `json:"agent_id" validate:"required"`
	Input          map[string]interface{} `json:"input,omitempty"`
	IdempotencyKey string                 `json:"idempotency_key" validate:"required,min=1,max=255,printascii"`
}

// RunTaskResponse 返回任务级启动结果。run 字段保持 runtime.RunResponse 的 JSON 形状。
type RunTaskResponse struct {
	TaskID string               `json:"task_id"`
	Status string               `json:"status"`
	Run    *runtime.RunResponse `json:"run"`
}

// HistoryItem "我的任务"列表项（GET /tasks/me）。
type HistoryItem struct {
	ID                  string   `json:"id"`
	Query               string   `json:"query"`
	Visibility          string   `json:"visibility"`
	ParsedSkills        []string `json:"parsed_skills"`
	MCPTools            []string `json:"mcp_tools"`
	RecommendedAgentIDs []string `json:"recommended_agent_ids"`
	Status              string   `json:"status"`
	ChosenAgentID       *string  `json:"chosen_agent_id,omitempty"`
	ChosenAt            *string  `json:"chosen_at,omitempty"`
	CompletionRunID     *string  `json:"completion_run_id,omitempty"`
	CompletedAt         *string  `json:"completed_at,omitempty"`
	CompletionSummary   *string  `json:"completion_summary,omitempty"`
	CreatedAt           string   `json:"created_at"`

	// CompletionSummaryRedacted 表示摘要已按查看者过滤。
	CompletionSummaryRedacted bool `json:"completion_summary_redacted,omitempty"`
}

type HistoryListResponse struct {
	Items        []HistoryItem `json:"items"`
	Total        int32         `json:"total"`
	Page         int32         `json:"page"`
	Size         int32         `json:"size"`
	Query        string        `json:"query,omitempty"`
	Sort         string        `json:"sort"`
	StatusFilter string        `json:"status_filter,omitempty"`
}

// DetailResponse GET /tasks/:id 详情响应。
//
// 用于冷链接（直接打开 /tasks/<id> URL，sessionStorage 无缓存）时
// 让前端依然能渲染 3 张推荐卡。recommendations 按 recommended_agent_ids 顺序回填；
// 若某 agent 已下架则跳过该位置。
type DetailResponse struct {
	ID                string           `json:"id"`
	Query             string           `json:"query"`
	Visibility        string           `json:"visibility"`
	ParsedSkills      []string         `json:"parsed_skills"`
	ParsedSkillRefs   []SkillRef       `json:"parsed_skill_refs"`
	MCPTools          []string         `json:"mcp_tools"`
	MCPToolRefs       []MCPToolRef     `json:"mcp_tool_refs"`
	Status            string           `json:"status"`
	ChosenAgentID     *string          `json:"chosen_agent_id,omitempty"`
	ChosenAt          *string          `json:"chosen_at,omitempty"`
	CompletionRunID   *string          `json:"completion_run_id,omitempty"`
	CompletedAt       *string          `json:"completed_at,omitempty"`
	CompletionSummary *string          `json:"completion_summary,omitempty"`
	CreatedAt         string           `json:"created_at"`
	Recommendations   []Recommendation `json:"recommendations"`
	NextAction        *TaskNextAction  `json:"next_action,omitempty"`

	// CompletionSummaryRedacted 表示摘要已按查看者过滤。
	CompletionSummaryRedacted bool `json:"completion_summary_redacted,omitempty"`
}
