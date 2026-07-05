package config

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

// Config 集中所有运行时配置。
// 通过 envconfig 从环境变量加载，required 字段缺失时启动失败。
type Config struct {
	// 服务
	Env  string `envconfig:"ENV" default:"development"`
	Port int    `envconfig:"PORT" default:"8080"`

	// 日志
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`

	// 数据库 / 缓存
	DatabaseURL                string `envconfig:"DATABASE_URL" required:"true"`
	RedisURL                   string `envconfig:"REDIS_URL" default:"redis://localhost:6379/0"`
	DBMaxConns                 int32  `envconfig:"DB_MAX_CONNS" default:"20"`
	DBMinConns                 int32  `envconfig:"DB_MIN_CONNS" default:"2"`
	DBMaxConnLifetimeMinutes   int    `envconfig:"DB_MAX_CONN_LIFETIME_MINUTES" default:"30"`
	DBMaxConnIdleTimeMinutes   int    `envconfig:"DB_MAX_CONN_IDLE_TIME_MINUTES" default:"5"`
	DBHealthCheckPeriodSeconds int    `envconfig:"DB_HEALTH_CHECK_PERIOD_SECONDS" default:"60"`

	// 鉴权
	JWTSecret          string `envconfig:"JWT_SECRET" required:"true"`
	JWTExpireHours     int    `envconfig:"JWT_EXPIRE_HOURS" default:"24"`
	OAuthSessionSecret string `envconfig:"OAUTH_SESSION_SECRET"`

	// Google OAuth
	GoogleClientID     string `envconfig:"GOOGLE_OAUTH_CLIENT_ID"`
	GoogleClientSecret string `envconfig:"GOOGLE_OAUTH_CLIENT_SECRET"`

	// GitHub OAuth
	GithubClientID     string `envconfig:"GITHUB_OAUTH_CLIENT_ID"`
	GithubClientSecret string `envconfig:"GITHUB_OAUTH_CLIENT_SECRET"`

	// 前端 URL
	FrontendURL                 string `envconfig:"FRONTEND_URL"`
	APIURL                      string `envconfig:"API_URL" default:"http://localhost:8080"`
	OAuthCallbackBaseURL        string `envconfig:"OAUTH_CALLBACK_BASE_URL"`
	OAuthAllowedFrontendOrigins string `envconfig:"OAUTH_ALLOWED_FRONTEND_ORIGINS"`
	// UserTokenVerifyURL 是 openlinker.ai 云端专属配置，指向私有 User Token 验证服务。
	// 自托管请留空；为空时 ol_user_* User Token 远程鉴权不可用，JWT 与 Agent Token 不受影响。
	UserTokenVerifyURL string `envconfig:"USER_TOKEN_VERIFY_URL"`
	// InternalToken 用于 Core 与 openlinker.ai 私有服务（LLM 代理、Token 验证）之间的内部鉴权。
	// 自托管请留空。
	InternalToken string `envconfig:"OPENLINKER_INTERNAL_TOKEN"`

	// A2A gRPC binding. Disabled by default so existing HTTP deployments do not
	// need to expose an additional HTTP/2 port.
	A2AGRPCEnabled   bool   `envconfig:"A2A_GRPC_ENABLED" default:"false"`
	A2AGRPCPort      int    `envconfig:"A2A_GRPC_PORT" default:"9090"`
	A2AGRPCPublicURL string `envconfig:"A2A_GRPC_PUBLIC_URL"`

	// LLMCompleteURL 指向内部 LLM 代理服务（openlinker.ai 云端部署使用）。
	// 自托管用户应配置 LLMOpenAIURL + LLMOpenAIAPIKey 直连 OpenAI 兼容接口；
	// 两者均未配置时，task 路由自动降级到关键词规则匹配。
	LLMCompleteURL string `envconfig:"LLM_COMPLETE_URL"`
	// LLMOpenAIURL 是 OpenAI 兼容 API 的 base URL，例如 https://api.openai.com/v1 或任意 OpenAI 兼容代理。
	// 当 LLMCompleteURL 为空时生效，供自托管用户直连 LLM。
	LLMOpenAIURL string `envconfig:"LLM_OPENAI_URL"`
	// LLMOpenAIAPIKey 是 LLMOpenAIURL 对应的 API Key。
	LLMOpenAIAPIKey string `envconfig:"LLM_OPENAI_API_KEY"`
	// LLMOpenAIModel 指定调用的模型名称，默认 gpt-4o-mini。
	LLMOpenAIModel string `envconfig:"LLM_OPENAI_MODEL" default:"gpt-4o-mini"`

	// 监控
	SentryDSN string `envconfig:"SENTRY_DSN"`

	// Runtime parameters.
	RunTimeoutSeconds       int  `envconfig:"RUN_TIMEOUT_SECONDS" default:"60"`
	AllowLocalHTTPEndpoints bool `envconfig:"ALLOW_LOCAL_HTTP_ENDPOINTS" default:"false"`
	HTTPRateLimitRate       int  `envconfig:"HTTP_RATE_LIMIT_RATE" default:"50"`
	HTTPRateLimitBurst      int  `envconfig:"HTTP_RATE_LIMIT_BURST" default:"200"`
	HTTPRateLimitPeriodSec  int  `envconfig:"HTTP_RATE_LIMIT_PERIOD_SECONDS" default:"1"`

	// Agent availability monitor.
	AvailabilityMonitorEnabled             bool `envconfig:"AVAILABILITY_MONITOR_ENABLED" default:"true"`
	AvailabilityMonitorIntervalSeconds     int  `envconfig:"AVAILABILITY_MONITOR_INTERVAL_SECONDS" default:"300"`
	AvailabilityMonitorInitialDelaySeconds int  `envconfig:"AVAILABILITY_MONITOR_INITIAL_DELAY_SECONDS" default:"60"`
	AvailabilityMonitorStaleSeconds        int  `envconfig:"AVAILABILITY_MONITOR_STALE_SECONDS" default:"900"`
	AvailabilityMonitorBatchSize           int  `envconfig:"AVAILABILITY_MONITOR_BATCH_SIZE" default:"20"`

	// Registry / Bridge proxy run timeout worker.
	RegistryProxyRunWorkerEnabled         bool `envconfig:"REGISTRY_PROXY_RUN_WORKER_ENABLED" default:"true"`
	RegistryProxyRunWorkerIntervalSeconds int  `envconfig:"REGISTRY_PROXY_RUN_WORKER_INTERVAL_SECONDS" default:"30"`
	RegistryProxyRunTimeoutSeconds        int  `envconfig:"REGISTRY_PROXY_RUN_TIMEOUT_SECONDS" default:"900"`

	// Workflow async run worker.
	WorkflowRunWorkerEnabled         bool `envconfig:"WORKFLOW_RUN_WORKER_ENABLED" default:"true"`
	WorkflowRunWorkerIntervalSeconds int  `envconfig:"WORKFLOW_RUN_WORKER_INTERVAL_SECONDS" default:"10"`
	WorkflowRunStaleSeconds          int  `envconfig:"WORKFLOW_RUN_STALE_SECONDS" default:"1800"`
	WorkflowRunClaimBurst            int  `envconfig:"WORKFLOW_RUN_CLAIM_BURST" default:"5"`

	// Runtime Pull run timeout worker.
	RuntimePullRunWorkerEnabled          bool `envconfig:"RUNTIME_PULL_RUN_WORKER_ENABLED" default:"true"`
	RuntimePullRunWorkerIntervalSeconds  int  `envconfig:"RUNTIME_PULL_RUN_WORKER_INTERVAL_SECONDS" default:"30"`
	RuntimePullDispatchTimeoutSeconds    int  `envconfig:"RUNTIME_PULL_DISPATCH_TIMEOUT_SECONDS" default:"120"`
	RuntimePullResultTimeoutSeconds      int  `envconfig:"RUNTIME_PULL_RESULT_TIMEOUT_SECONDS" default:"900"`
	RuntimePullRunWorkerTimeoutBatchSize int  `envconfig:"RUNTIME_PULL_RUN_WORKER_TIMEOUT_BATCH_SIZE" default:"50"`

	// Endpoint run timeout worker for direct_http / mcp_server runs. A zero
	// timeout means max(RUN_TIMEOUT_SECONDS+30s, 180s).
	RuntimeEndpointRunWorkerEnabled         bool `envconfig:"RUNTIME_ENDPOINT_RUN_WORKER_ENABLED" default:"true"`
	RuntimeEndpointRunWorkerIntervalSeconds int  `envconfig:"RUNTIME_ENDPOINT_RUN_WORKER_INTERVAL_SECONDS" default:"30"`
	RuntimeEndpointRunTimeoutSeconds        int  `envconfig:"RUNTIME_ENDPOINT_RUN_TIMEOUT_SECONDS" default:"0"`
	RuntimeEndpointRunWorkerBatchSize       int  `envconfig:"RUNTIME_ENDPOINT_RUN_WORKER_BATCH_SIZE" default:"50"`
}

// Load 从环境变量加载配置。
func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &cfg, nil
}

// IsProduction 是否生产环境。
func (c *Config) IsProduction() bool {
	return c.Env == "production"
}
