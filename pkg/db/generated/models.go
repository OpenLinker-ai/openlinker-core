// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成。

package db

import (
	"time"

	"github.com/google/uuid"
)

// User 对应 users 表。
// password_hash / oauth_provider / oauth_id / avatar_url / deleted_at 可空。
type User struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	Email           string     `db:"email" json:"email"`
	PasswordHash    *string    `db:"password_hash" json:"password_hash"`
	OauthProvider   *string    `db:"oauth_provider" json:"oauth_provider"`
	OauthID         *string    `db:"oauth_id" json:"oauth_id"`
	DisplayName     string     `db:"display_name" json:"display_name"`
	AvatarURL       *string    `db:"avatar_url" json:"avatar_url"`
	IsCreator       bool       `db:"is_creator" json:"is_creator"`
	CreatorVerified bool       `db:"creator_verified" json:"creator_verified"`
	IsAdmin         bool       `db:"is_admin" json:"is_admin"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at" json:"updated_at"`
	DeletedAt       *time.Time `db:"deleted_at" json:"deleted_at"`
}

// Wallet 对应 wallets 表。
// 所有金额单位为 cents（int64）。
type Wallet struct {
	UserID              uuid.UUID `db:"user_id" json:"user_id"`
	BalanceCents        int64     `db:"balance_cents" json:"balance_cents"`
	EarningsCents       int64     `db:"earnings_cents" json:"earnings_cents"`
	TotalChargedCents   int64     `db:"total_charged_cents" json:"total_charged_cents"`
	TotalSpentCents     int64     `db:"total_spent_cents" json:"total_spent_cents"`
	TotalEarnedCents    int64     `db:"total_earned_cents" json:"total_earned_cents"`
	TotalWithdrawnCents int64     `db:"total_withdrawn_cents" json:"total_withdrawn_cents"`
	UpdatedAt           time.Time `db:"updated_at" json:"updated_at"`
}

// Charge 对应 charges 表（充值记录）。
type Charge struct {
	ID                    uuid.UUID  `db:"id" json:"id"`
	UserID                uuid.UUID  `db:"user_id" json:"user_id"`
	AmountCents           int32      `db:"amount_cents" json:"amount_cents"`
	Currency              string     `db:"currency" json:"currency"`
	StripePaymentIntentID *string    `db:"stripe_payment_intent_id" json:"stripe_payment_intent_id"`
	Status                string     `db:"status" json:"status"`
	FailureMessage        *string    `db:"failure_message" json:"failure_message"`
	CreatedAt             time.Time  `db:"created_at" json:"created_at"`
	SucceededAt           *time.Time `db:"succeeded_at" json:"succeeded_at"`
}

// Withdrawal 对应 withdrawals 表（提现记录）。
type Withdrawal struct {
	ID          uuid.UUID  `db:"id" json:"id"`
	CreatorID   uuid.UUID  `db:"creator_id" json:"creator_id"`
	AmountCents int32      `db:"amount_cents" json:"amount_cents"`
	Status      string     `db:"status" json:"status"`
	Notes       *string    `db:"notes" json:"notes"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
	PaidAt      *time.Time `db:"paid_at" json:"paid_at"`
}

// Agent 对应 agents 表（Phase 2 缺口 2 后的三维状态机模型）。
//
// lifecycle_status     active | disabled                              （docs/29 §三）
// visibility           public | unlisted | private
// certification_status unreviewed | pending | certified | rejected
// 金额：price_per_call_cents 用 int32（INTEGER）；累计 total_revenue_cents 用 int64（BIGINT）。
// tags 用 []string（Postgres TEXT[]）。
// webhook_url / webhook_secret 子轮 2.1 引入，可为 NULL（未配置）。
type Agent struct {
	ID                  uuid.UUID  `db:"id" json:"id"`
	CreatorID           uuid.UUID  `db:"creator_id" json:"creator_id"`
	Slug                string     `db:"slug" json:"slug"`
	Name                string     `db:"name" json:"name"`
	Description         string     `db:"description" json:"description"`
	EndpointURL         string     `db:"endpoint_url" json:"endpoint_url"`
	EndpointAuthHeader  *string    `db:"endpoint_auth_header" json:"endpoint_auth_header"`
	PricePerCallCents   int32      `db:"price_per_call_cents" json:"price_per_call_cents"`
	Tags                []string   `db:"tags" json:"tags"`
	LifecycleStatus     string     `db:"lifecycle_status" json:"lifecycle_status"`
	Visibility          string     `db:"visibility" json:"visibility"`
	CertificationStatus string     `db:"certification_status" json:"certification_status"`
	RejectionReason     *string    `db:"rejection_reason" json:"rejection_reason"`
	CertifiedAt         *time.Time `db:"certified_at" json:"certified_at"`
	TotalCalls          int32      `db:"total_calls" json:"total_calls"`
	TotalRevenueCents   int64      `db:"total_revenue_cents" json:"total_revenue_cents"`
	WebhookURL          *string    `db:"webhook_url" json:"webhook_url"`
	WebhookSecret       *string    `db:"webhook_secret" json:"-"` // 不暴露给前端
	ConnectionMode      string     `db:"connection_mode" json:"connection_mode"`
	MCPToolName         *string    `db:"mcp_tool_name" json:"mcp_tool_name"`
	CreatedAt           time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt           time.Time  `db:"updated_at" json:"updated_at"`
}

// Run 对应 runs 表（每次调用一行）。
//
// status: 'running' | 'success' | 'failed' | 'timeout'
// 金额单位 cents（int32）：cost_cents = platform_fee_cents + creator_revenue_cents
// input/output 是 JSONB（[]byte，原样透传）
type Run struct {
	ID                      uuid.UUID  `db:"id" json:"id"`
	UserID                  uuid.UUID  `db:"user_id" json:"user_id"`
	AgentID                 uuid.UUID  `db:"agent_id" json:"agent_id"`
	Input                   []byte     `db:"input" json:"input"`
	Output                  []byte     `db:"output" json:"output"`
	Status                  string     `db:"status" json:"status"`
	ErrorCode               *string    `db:"error_code" json:"error_code"`
	ErrorMessage            *string    `db:"error_message" json:"error_message"`
	CostCents               int32      `db:"cost_cents" json:"cost_cents"`
	PlatformFeeCents        int32      `db:"platform_fee_cents" json:"platform_fee_cents"`
	CreatorRevenueCents     int32      `db:"creator_revenue_cents" json:"creator_revenue_cents"`
	DurationMs              *int32     `db:"duration_ms" json:"duration_ms"`
	StartedAt               time.Time  `db:"started_at" json:"started_at"`
	FinishedAt              *time.Time `db:"finished_at" json:"finished_at"`
	Source                  string     `db:"source" json:"source"`
	ClaimedByRuntimeTokenID *uuid.UUID `db:"claimed_by_runtime_token_id" json:"claimed_by_runtime_token_id"`
	ClaimedAt               *time.Time `db:"claimed_at" json:"claimed_at"`
}

// RunEvent 对应 run_events 表。
//
// sequence 在单个 run 内递增，用于 SSE Last-Event-ID / 断线续传。
// payload 是 JSONB 原始字节，调用方按 event_type 解析。
type RunEvent struct {
	ID          uuid.UUID  `db:"id" json:"id"`
	RunID       uuid.UUID  `db:"run_id" json:"run_id"`
	ParentRunID *uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	Sequence    int32      `db:"sequence" json:"sequence"`
	EventType   string     `db:"event_type" json:"event_type"`
	Payload     []byte     `db:"payload" json:"payload"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
}

// AgentActionApprovalRequest Phase 2 缺口 2：高风险动作审批记录（docs/29 §3.4）。
//
// status: 'pending' | 'confirmed' | 'rejected' | 'expired'
// PayloadJSON 是 JSONB 原始字节，调用方按 action 解析。
// RequestedByTokenID 允许 NULL：人类直接发起的审批不绑 Runtime Token。
type AgentActionApprovalRequest struct {
	ID                 uuid.UUID  `db:"id" json:"id"`
	AgentID            uuid.UUID  `db:"agent_id" json:"agent_id"`
	RequestedByUserID  *uuid.UUID `db:"requested_by_user_id" json:"requested_by_user_id"`
	RequestedByTokenID *uuid.UUID `db:"requested_by_token_id" json:"requested_by_token_id"`
	Action             string     `db:"action" json:"action"`
	PayloadJSON        []byte     `db:"payload_json" json:"payload_json"`
	Status             string     `db:"status" json:"status"`
	ApprovalURLSlug    string     `db:"approval_url_slug" json:"approval_url_slug"`
	ExpiresAt          time.Time  `db:"expires_at" json:"expires_at"`
	DecidedAt          *time.Time `db:"decided_at" json:"decided_at"`
	DecidedByUserID    *uuid.UUID `db:"decided_by_user_id" json:"decided_by_user_id"`
	DecisionNote       *string    `db:"decision_note" json:"decision_note"`
	CreatedAt          time.Time  `db:"created_at" json:"created_at"`
}

// AgentMetricSnapshot Phase 2 缺口 2：Agent 指标快照（docs/29 §3.4）。
// 由 metric worker 每 5 分钟为每个 active Agent × {24h, 7d, 30d} upsert 一行。
type AgentMetricSnapshot struct {
	AgentID         uuid.UUID `db:"agent_id" json:"agent_id"`
	TimeWindow      string    `db:"time_window" json:"time_window"`
	CallCount       int32     `db:"call_count" json:"call_count"`
	SuccessCount    int32     `db:"success_count" json:"success_count"`
	FailureCount    int32     `db:"failure_count" json:"failure_count"`
	SuccessRateBps  int32     `db:"success_rate_bps" json:"success_rate_bps"`
	MedianLatencyMs *int32    `db:"median_latency_ms" json:"median_latency_ms"`
	P95LatencyMs    *int32    `db:"p95_latency_ms" json:"p95_latency_ms"`
	SnapshottedAt   time.Time `db:"snapshotted_at" json:"snapshotted_at"`
}

// AgentRegistrationToken 是创作者侧短期 Bootstrap Token，供 Agent 自注册流程使用。
// max_agents 控制同一枚 token 可换取多少个 Agent，used_count 原子递增。
// 明文 token 仅创建时返回，数据库只保存 hash 与显示用 prefix。
type AgentRegistrationToken struct {
	ID            uuid.UUID  `db:"id" json:"id"`
	CreatorUserID uuid.UUID  `db:"creator_user_id" json:"creator_user_id"`
	Label         string     `db:"label" json:"label"`
	Prefix        string     `db:"prefix" json:"prefix"`
	TokenHash     string     `db:"token_hash" json:"-"`
	MaxAgents     int32      `db:"max_agents" json:"max_agents"`
	UsedCount     int32      `db:"used_count" json:"used_count"`
	ExpiresAt     time.Time  `db:"expires_at" json:"expires_at"`
	RevokedAt     *time.Time `db:"revoked_at" json:"revoked_at"`
	LastUsedAt    *time.Time `db:"last_used_at" json:"last_used_at"`
	CreatedAt     time.Time  `db:"created_at" json:"created_at"`
}

// AgentRuntimeToken 是绑定单个 Agent 的自动化调用凭证。
// 明文 token 仅创建时返回，数据库只保存 hash 与显示用 prefix。
type AgentRuntimeToken struct {
	ID              uuid.UUID  `db:"id" json:"id"`
	AgentID         uuid.UUID  `db:"agent_id" json:"agent_id"`
	CreatedByUserID uuid.UUID  `db:"created_by_user_id" json:"created_by_user_id"`
	Name            string     `db:"name" json:"name"`
	Prefix          string     `db:"prefix" json:"prefix"`
	TokenHash       string     `db:"token_hash" json:"-"`
	Scopes          []string   `db:"scopes" json:"scopes"`
	LastUsedAt      *time.Time `db:"last_used_at" json:"last_used_at"`
	RevokedAt       *time.Time `db:"revoked_at" json:"revoked_at"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
}

// AgentCallPolicy 控制目标 Agent 是否接受来自其他 Agent 的平台代理调用。
type AgentCallPolicy struct {
	AgentID    uuid.UUID `db:"agent_id" json:"agent_id"`
	CallableBy string    `db:"callable_by" json:"callable_by"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

// RunDelegation 将被调用的 child run 关联到发起委派的 parent run。
type RunDelegation struct {
	ChildRunID    uuid.UUID `db:"child_run_id" json:"child_run_id"`
	ParentRunID   uuid.UUID `db:"parent_run_id" json:"parent_run_id"`
	CallerAgentID uuid.UUID `db:"caller_agent_id" json:"caller_agent_id"`
	Reason        string    `db:"reason" json:"reason"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
}

// ApiKey 对应 api_keys 表。
//
// 子轮 2.1（Phase 2）引入，给开发者用 cURL 调 API 用。
// key_hash = bcrypt(完整明文 sk_live_xxx)；明文仅创建时返回一次。
type ApiKey struct {
	ID         uuid.UUID  `db:"id" json:"id"`
	UserID     uuid.UUID  `db:"user_id" json:"user_id"`
	Name       string     `db:"name" json:"name"`
	Prefix     string     `db:"prefix" json:"prefix"` // sk_live_abcd 前缀（UI 展示）
	KeyHash    string     `db:"key_hash" json:"-"`    // 不暴露
	Scopes     []string   `db:"scopes" json:"scopes"`
	LastUsedAt *time.Time `db:"last_used_at" json:"last_used_at"`
	RevokedAt  *time.Time `db:"revoked_at" json:"revoked_at"`
	CreatedAt  time.Time  `db:"created_at" json:"created_at"`
}

// Skill 对应 skills 表（平台维护的 30 个核心 skill）。
//
// id 形如 "content/translation" / "dev/code-review"；category 是 id 前的目录名。
// description 用于 LLM 解析任务描述时做语义匹配。
type Skill struct {
	ID          string    `db:"id" json:"id"`
	Category    string    `db:"category" json:"category"`
	Name        string    `db:"name" json:"name"`
	Description string    `db:"description" json:"description"`
	SortOrder   int32     `db:"sort_order" json:"sort_order"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
}

// AgentSkill 对应 agent_skills 关联表。
type AgentSkill struct {
	AgentID   uuid.UUID `db:"agent_id" json:"agent_id"`
	SkillID   string    `db:"skill_id" json:"skill_id"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// TaskQuery 对应 task_queries 表（任务驱动 A 形态）。
//
// query 是用户自然语言描述；parsed_skills 是关联/解析出的 skill_id 列表；mcp_tools 是任务关联的 MCP 工具名。
// recommended_agent_ids 按推荐顺序保存（top 3）；chosen_agent_id 记录用户最终选择（可空）。
type TaskQuery struct {
	ID                  uuid.UUID   `db:"id" json:"id"`
	UserID              uuid.UUID   `db:"user_id" json:"user_id"`
	Query               string      `db:"query" json:"query"`
	ParsedSkills        []string    `db:"parsed_skills" json:"parsed_skills"`
	MCPTools            []string    `db:"mcp_tools" json:"mcp_tools"`
	RecommendedAgentIDs []uuid.UUID `db:"recommended_agent_ids" json:"recommended_agent_ids"`
	ChosenAgentID       *uuid.UUID  `db:"chosen_agent_id" json:"chosen_agent_id"`
	ChosenAt            *time.Time  `db:"chosen_at" json:"chosen_at"`
	CreatedAt           time.Time   `db:"created_at" json:"created_at"`
}

// AgentCapability 对应 agent_capabilities 表。
//
// 模块 A（Phase 2 §4）：当前版本的能力声明（JSON Schema input/output）。
// input_schema / output_schema 是 JSONB（[]byte，原样透传，业务层负责解码 + 校验 draft-07）。
// version 在 upsert 时自增；published_at 反映最近一次发布时间。
type AgentCapability struct {
	ID           uuid.UUID `db:"id" json:"id"`
	AgentID      uuid.UUID `db:"agent_id" json:"agent_id"`
	InputSchema  []byte    `db:"input_schema" json:"input_schema"`
	OutputSchema []byte    `db:"output_schema" json:"output_schema"`
	Summary      string    `db:"summary" json:"summary"`
	Version      int32     `db:"version" json:"version"`
	PublishedAt  time.Time `db:"published_at" json:"published_at"`
	UpdatedAt    time.Time `db:"updated_at" json:"updated_at"`
}

// AgentExample 对应 agent_examples 表。
//
// input_json 必填；expected_output_json 可空（dry-run 仅校验结构，benchmark 才比对）。
// sort_order 决定展示与 dry-run 取首条的顺序。
type AgentExample struct {
	ID                 uuid.UUID `db:"id" json:"id"`
	AgentID            uuid.UUID `db:"agent_id" json:"agent_id"`
	Title              string    `db:"title" json:"title"`
	InputJSON          []byte    `db:"input_json" json:"input_json"`
	ExpectedOutputJSON []byte    `db:"expected_output_json" json:"expected_output_json"`
	SortOrder          int32     `db:"sort_order" json:"sort_order"`
	CreatedAt          time.Time `db:"created_at" json:"created_at"`
	UpdatedAt          time.Time `db:"updated_at" json:"updated_at"`
}

// AgentOnboardingStatus 对应 agent_onboarding_status 表。
//
// 与 agents 1:1。endpoint_set 在 CreateAgent 后由 EnsureOnboardingStatus 写入 TRUE。
// dry_run_last_result: 'pending' | 'pass' | 'fail'。
type AgentOnboardingStatus struct {
	AgentID          uuid.UUID  `db:"agent_id" json:"agent_id"`
	EndpointSet      bool       `db:"endpoint_set" json:"endpoint_set"`
	CapabilitiesSet  bool       `db:"capabilities_set" json:"capabilities_set"`
	ExamplesSet      bool       `db:"examples_set" json:"examples_set"`
	DryRunPassed     bool       `db:"dry_run_passed" json:"dry_run_passed"`
	DryRunLastResult string     `db:"dry_run_last_result" json:"dry_run_last_result"`
	DryRunError      *string    `db:"dry_run_error" json:"dry_run_error"`
	DryRunAt         *time.Time `db:"dry_run_at" json:"dry_run_at"`
	UpdatedAt        time.Time  `db:"updated_at" json:"updated_at"`
}

// SkillTestCase 对应 skill_test_cases 表。
//
// 模块 B（Phase 2 §5）：Skill Benchmark 测试用例（平台维护）。
// input_json 喂给 Agent endpoint；judge_prompt 是 LLM-as-judge 评分模板，
// service 层会用真实 endpoint 输出替换 prompt 中的 {output} 占位。
type SkillTestCase struct {
	ID          uuid.UUID `db:"id" json:"id"`
	SkillID     string    `db:"skill_id" json:"skill_id"`
	Title       string    `db:"title" json:"title"`
	InputJSON   []byte    `db:"input_json" json:"input_json"`
	JudgePrompt string    `db:"judge_prompt" json:"judge_prompt"`
	SortOrder   int32     `db:"sort_order" json:"sort_order"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
}

// AgentSkillBenchmarkRun 对应 agent_skill_benchmark_runs 表。
//
// 单条 case 的执行记录。status: 'pending' | 'success' | 'failed'。
// score 仅 success 非空（0-100）；raw_output / judge_reasoning 仅 success 写入。
// 同一次"跑某 skill"的多条 case 共享 batch_id。
type AgentSkillBenchmarkRun struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	BatchID        uuid.UUID  `db:"batch_id" json:"batch_id"`
	AgentID        uuid.UUID  `db:"agent_id" json:"agent_id"`
	SkillID        string     `db:"skill_id" json:"skill_id"`
	TestCaseID     uuid.UUID  `db:"test_case_id" json:"test_case_id"`
	Status         string     `db:"status" json:"status"`
	Score          *int32     `db:"score" json:"score"`
	RawOutput      []byte     `db:"raw_output" json:"raw_output"`
	JudgeReasoning *string    `db:"judge_reasoning" json:"judge_reasoning"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	StartedAt      time.Time  `db:"started_at" json:"started_at"`
	FinishedAt     *time.Time `db:"finished_at" json:"finished_at"`
}

// AgentSkillScore 对应 agent_skill_scores 表（聚合）。
//
// status: 'pending' | 'verified' | 'failed'
// verified = 平均分 >= 阈值（service 中配置，默认 75）。
type AgentSkillScore struct {
	AgentID      uuid.UUID  `db:"agent_id" json:"agent_id"`
	SkillID      string     `db:"skill_id" json:"skill_id"`
	Status       string     `db:"status" json:"status"`
	AverageScore *int32     `db:"average_score" json:"average_score"`
	PassCount    int32      `db:"pass_count" json:"pass_count"`
	TotalCount   int32      `db:"total_count" json:"total_count"`
	LastBatchID  *uuid.UUID `db:"last_batch_id" json:"last_batch_id"`
	VerifiedAt   *time.Time `db:"verified_at" json:"verified_at"`
	UpdatedAt    time.Time  `db:"updated_at" json:"updated_at"`
}

// DeliveryTarget 对应 delivery_targets 表（用户拥有的投递目标）。
//
// type: 'webhook' | 'slack'。Config 是 JSONB（webhook: {url}; slack: {url}）。
// Secret 给 webhook 投递做 HMAC 签名；slack 也存一份以备未来扩展。
type DeliveryTarget struct {
	ID        uuid.UUID `db:"id" json:"id"`
	UserID    uuid.UUID `db:"user_id" json:"user_id"`
	Name      string    `db:"name" json:"name"`
	Type      string    `db:"type" json:"type"`
	Config    []byte    `db:"config" json:"config"`
	Secret    string    `db:"secret" json:"-"`
	IsDefault bool      `db:"is_default" json:"is_default"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

// RunDelivery 对应 run_deliveries 表（用户侧投递记录）。
//
// status: 'pending' | 'success' | 'failed'
// target_type / target_url 是投递时快照，即便 target 被删也能展示历史。
type RunDelivery struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	RunID          uuid.UUID  `db:"run_id" json:"run_id"`
	TargetID       uuid.UUID  `db:"target_id" json:"target_id"`
	UserID         uuid.UUID  `db:"user_id" json:"user_id"`
	TargetType     string     `db:"target_type" json:"target_type"`
	TargetURL      string     `db:"target_url" json:"target_url"`
	Payload        []byte     `db:"payload" json:"payload"`
	Status         string     `db:"status" json:"status"`
	ResponseStatus *int32     `db:"response_status" json:"response_status"`
	ResponseBody   *string    `db:"response_body" json:"response_body"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	AttemptCount   int32      `db:"attempt_count" json:"attempt_count"`
	NextRetryAt    *time.Time `db:"next_retry_at" json:"next_retry_at"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}

// WebhookDelivery 对应 webhook_deliveries 表（投递日志）。
//
// status: 'pending' | 'success' | 'failed'
// next_retry_at NULL 表示完成或放弃；非 NULL 时由 worker 重试
type WebhookDelivery struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	AgentID        uuid.UUID  `db:"agent_id" json:"agent_id"`
	RunID          uuid.UUID  `db:"run_id" json:"run_id"`
	URL            string     `db:"url" json:"url"`
	Payload        []byte     `db:"payload" json:"payload"`
	Status         string     `db:"status" json:"status"`
	ResponseStatus *int32     `db:"response_status" json:"response_status"`
	ResponseBody   *string    `db:"response_body" json:"response_body"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	AttemptCount   int32      `db:"attempt_count" json:"attempt_count"`
	NextRetryAt    *time.Time `db:"next_retry_at" json:"next_retry_at"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}
