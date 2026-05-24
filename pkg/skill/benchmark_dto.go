package skill

import (
	"time"

	"github.com/google/uuid"
)

// 模块 B（Phase 2 §5）：Skill Benchmark DTO。
//
// 仅展示型字段，DB 模型见 pkg/db/generated/{models,benchmark.sql}.go。

// BenchmarkResultStatus = "pending" | "verified" | "failed"。
// 与 db.AgentSkillScore.Status 同集合，但增加 "not_run" 用于 UI 区分"从未跑过"。
const (
	BenchmarkStatusPending  = "pending"
	BenchmarkStatusVerified = "verified"
	BenchmarkStatusFailed   = "failed"
	BenchmarkStatusNotRun   = "not_run"
)

// VerifiedThreshold 平均分 >= 该值 → status=verified；否则 status=failed。
//
// 75 是 docs/25 §5 给出的默认阈值，可调；改阈值时同步更新 README / 详情页说明。
const VerifiedThreshold = 75

// RunBenchmarkRequest 创作者触发某 skill 的 benchmark。
type RunBenchmarkRequest struct {
	SkillID string `json:"skill_id" validate:"required"`
}

// RunBenchmarkResponse 触发后立即返回 batch_id；执行异步进行。
type RunBenchmarkResponse struct {
	BatchID string `json:"batch_id"`
	SkillID string `json:"skill_id"`
	Status  string `json:"status"` // "running"
}

// SkillScoreItem 单条 (agent × skill) 评分概览。
type SkillScoreItem struct {
	SkillID      string  `json:"skill_id"`
	SkillName    string  `json:"skill_name,omitempty"`
	Status       string  `json:"status"`
	AverageScore *int32  `json:"average_score,omitempty"`
	PassCount    int32   `json:"pass_count"`
	TotalCount   int32   `json:"total_count"`
	LastBatchID  *string `json:"last_batch_id,omitempty"`
	VerifiedAt   *string `json:"verified_at,omitempty"`
	UpdatedAt    string  `json:"updated_at"`
}

// BenchmarkRunItem 详情页单条 case 结果（脱敏过 raw_output）。
type BenchmarkRunItem struct {
	ID             string  `json:"id"`
	TestCaseTitle  string  `json:"test_case_title"`
	Status         string  `json:"status"`
	Score          *int32  `json:"score,omitempty"`
	JudgeReasoning *string `json:"judge_reasoning,omitempty"`
	ErrorMessage   *string `json:"error_message,omitempty"`
	StartedAt      string  `json:"started_at"`
	FinishedAt     *string `json:"finished_at,omitempty"`
}

// BenchmarkBatchDetail 批次详情。
type BenchmarkBatchDetail struct {
	BatchID      string             `json:"batch_id"`
	AgentID      string             `json:"agent_id"`
	SkillID      string             `json:"skill_id"`
	Status       string             `json:"status"`
	AverageScore *int32             `json:"average_score,omitempty"`
	Items        []BenchmarkRunItem `json:"items"`
}

// TopAgentForSkill /skills 列表页 top-N 行。
type TopAgentForSkill struct {
	AgentID           string  `json:"agent_id"`
	Slug              string  `json:"slug"`
	Name              string  `json:"name"`
	Description       string  `json:"description"`
	Tags              []string `json:"tags"`
	PricePerCallCents int32   `json:"price_per_call_cents"`
	TotalCalls        int32   `json:"total_calls"`
	AverageScore      *int32  `json:"average_score,omitempty"`
	VerifiedAt        *string `json:"verified_at,omitempty"`
}

// formatTimePtr 把 *time.Time 转 RFC3339 字符串指针；nil → nil。
func formatTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

// uuidPtrString *uuid.UUID → *string。
func uuidPtrString(u *uuid.UUID) *string {
	if u == nil {
		return nil
	}
	s := u.String()
	return &s
}
