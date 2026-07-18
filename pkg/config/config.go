package config

import (
	"fmt"
	"strings"

	"github.com/kelseyhightower/envconfig"
)

// Config 集中所有运行时配置。
// 通过 envconfig 从环境变量加载，required 字段缺失时启动失败。
type Config struct {
	// 服务
	Env            string `envconfig:"ENV" default:"development"`
	Port           int    `envconfig:"PORT" default:"8080"`
	ReleaseVersion string `envconfig:"OPENLINKER_RELEASE_ID" default:"local"`
	ReleaseCommit  string `envconfig:"OPENLINKER_GIT_SHA" default:"unknown"`

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
	// OAuthCodeStorageMode controls only the temporary OAuth handoff row format.
	// legacy-jwt is the rolling-deployment compatibility default; subject-only
	// must be enabled only after every reader understands nullable JWT rows.
	OAuthCodeStorageMode string `envconfig:"OAUTH_CODE_STORAGE_MODE" default:"legacy-jwt"`

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
	// InternalToken 保护 /internal/user-tokens/introspect，并可复用于 Core
	// 与受信任私有服务（如 LLM 代理）的内部鉴权。不使用内部接口可留空。
	InternalToken string `envconfig:"OPENLINKER_INTERNAL_TOKEN"`

	// External Execution uses a dedicated asymmetric service identity. It is
	// intentionally independent from JWT_SECRET and OPENLINKER_INTERNAL_TOKEN.
	// When the public key is empty the optional internal API is not mounted.
	ExternalExecutionJWTCurrentKeyID     string `envconfig:"EXTERNAL_EXECUTION_JWT_CURRENT_KEY_ID" default:"current"`
	ExternalExecutionJWTCurrentPublicKey string `envconfig:"EXTERNAL_EXECUTION_JWT_CURRENT_PUBLIC_KEY"`
	ExternalExecutionJWTNextKeyID        string `envconfig:"EXTERNAL_EXECUTION_JWT_NEXT_KEY_ID"`
	ExternalExecutionJWTNextPublicKey    string `envconfig:"EXTERNAL_EXECUTION_JWT_NEXT_PUBLIC_KEY"`
	ExternalExecutionJWTIssuer           string `envconfig:"EXTERNAL_EXECUTION_JWT_ISSUER" default:"openlinker-cloud"`
	ExternalExecutionJWTAudience         string `envconfig:"EXTERNAL_EXECUTION_JWT_AUDIENCE" default:"openlinker-core.external-execution"`
	// Fixed to the migration-074 legacy namespace until an explicit rekey migration exists.
	ExternalExecutionCallerServiceID        string `envconfig:"EXTERNAL_EXECUTION_CALLER_SERVICE_ID" default:"openlinker-cloud"`
	ExternalExecutionRequestBindingRequired bool   `envconfig:"EXTERNAL_EXECUTION_REQUEST_BINDING_REQUIRED" default:"false"`

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
	// Agent Runtime device traffic terminates mTLS directly in Core. It is a
	// separate listener so a reverse proxy cannot replace verified peer
	// certificates with spoofable headers.
	RuntimeMTLSEnabled      bool   `envconfig:"RUNTIME_MTLS_ENABLED" default:"false"`
	RuntimeMTLSPort         int    `envconfig:"RUNTIME_MTLS_PORT" default:"8443"`
	RuntimeMTLSAPIURL       string `envconfig:"RUNTIME_MTLS_API_URL"`
	RuntimeMTLSCertFile     string `envconfig:"RUNTIME_MTLS_CERT_FILE"`
	RuntimeMTLSKeyFile      string `envconfig:"RUNTIME_MTLS_KEY_FILE"`
	RuntimeMTLSClientCAFile string `envconfig:"RUNTIME_MTLS_CLIENT_CA_FILE"`
	// RuntimeHAMode selects the Redis-backed cross-instance signal dependency.
	// PostgreSQL remains the fact source and its workers continue if Redis is
	// unavailable; production HA readiness fails closed until Redis recovers.
	RuntimeHAMode bool `envconfig:"RUNTIME_HA_MODE" default:"false"`
	// Separate from JWT_SECRET so runtime capability key rotation cannot
	// invalidate user sessions or reuse one key across protocols.
	RuntimeInvocationSigningKeyID          string `envconfig:"RUNTIME_INVOCATION_SIGNING_KEY_ID" default:"current"`
	RuntimeInvocationSigningSecret         string `envconfig:"RUNTIME_INVOCATION_SIGNING_SECRET"`
	RuntimeInvocationPreviousSigningKeyID  string `envconfig:"RUNTIME_INVOCATION_PREVIOUS_SIGNING_KEY_ID"`
	RuntimeInvocationPreviousSigningSecret string `envconfig:"RUNTIME_INVOCATION_PREVIOUS_SIGNING_SECRET"`
	HTTPRateLimitRate                      int    `envconfig:"HTTP_RATE_LIMIT_RATE" default:"50"`
	HTTPRateLimitBurst                     int    `envconfig:"HTTP_RATE_LIMIT_BURST" default:"200"`
	HTTPRateLimitPeriodSec                 int    `envconfig:"HTTP_RATE_LIMIT_PERIOD_SECONDS" default:"1"`

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
}

// Load 从环境变量加载配置。
func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	mode, err := normalizeOAuthCodeStorageMode(cfg.OAuthCodeStorageMode)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	cfg.OAuthCodeStorageMode = mode
	return &cfg, nil
}

func normalizeOAuthCodeStorageMode(value string) (string, error) {
	switch value {
	case "", "legacy-jwt":
		return "legacy-jwt", nil
	case "subject-only":
		return "subject-only", nil
	default:
		return "", fmt.Errorf("OAUTH_CODE_STORAGE_MODE is invalid")
	}
}

// IsProduction 是否生产环境。
func (c *Config) IsProduction() bool {
	return c.Env == "production"
}

func (c *Config) ExternalExecutionEnabled() bool {
	return c != nil && strings.TrimSpace(c.ExternalExecutionJWTCurrentPublicKey) != ""
}
