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

	// Stripe
	StripeSecretKey     string `envconfig:"STRIPE_SECRET_KEY"`
	StripeWebhookSecret string `envconfig:"STRIPE_WEBHOOK_SECRET"`

	// Google OAuth
	GoogleClientID     string `envconfig:"GOOGLE_OAUTH_CLIENT_ID"`
	GoogleClientSecret string `envconfig:"GOOGLE_OAUTH_CLIENT_SECRET"`

	// GitHub OAuth
	GithubClientID     string `envconfig:"GITHUB_OAUTH_CLIENT_ID"`
	GithubClientSecret string `envconfig:"GITHUB_OAUTH_CLIENT_SECRET"`

	// 前端 URL
	FrontendURL          string `envconfig:"FRONTEND_URL"`
	APIURL               string `envconfig:"API_URL" default:"http://localhost:8080"`
	OAuthCallbackBaseURL string `envconfig:"OAUTH_CALLBACK_BASE_URL"`
	APIKeyVerifyURL      string `envconfig:"API_KEY_VERIFY_URL"`
	APIKeyVerifySecret   string `envconfig:"API_KEY_VERIFY_SECRET"`

	// A2A gRPC binding. Disabled by default so existing HTTP deployments do not
	// need to expose an additional HTTP/2 port.
	A2AGRPCEnabled   bool   `envconfig:"A2A_GRPC_ENABLED" default:"false"`
	A2AGRPCPort      int    `envconfig:"A2A_GRPC_PORT" default:"9090"`
	A2AGRPCPublicURL string `envconfig:"A2A_GRPC_PUBLIC_URL"`

	// LLM（任务驱动 A 形态。空 → 走规则 fallback）
	AnthropicAPIKey string `envconfig:"ANTHROPIC_API_KEY"`

	// 监控
	SentryDSN string `envconfig:"SENTRY_DSN"`

	// 业务参数
	PlatformFeeRate         float64 `envconfig:"PLATFORM_FEE_RATE" default:"0.25"`
	RunBillingMode          string  `envconfig:"RUN_BILLING_MODE" default:"free"`
	RunTimeoutSeconds       int     `envconfig:"RUN_TIMEOUT_SECONDS" default:"60"`
	AllowLocalHTTPEndpoints bool    `envconfig:"ALLOW_LOCAL_HTTP_ENDPOINTS" default:"false"`

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
