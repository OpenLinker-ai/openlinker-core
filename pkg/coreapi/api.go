package coreapi

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/sessions"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/markbates/goth"
	"github.com/markbates/goth/gothic"
	gothgithub "github.com/markbates/goth/providers/github"
	gothgoogle "github.com/markbates/goth/providers/google"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/a2a"
	coreadmin "github.com/OpenLinker-ai/openlinker-core/pkg/admin"
	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/cloudbridge"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/delivery"
	"github.com/OpenLinker-ai/openlinker-core/pkg/discovery"
	corellm "github.com/OpenLinker-ai/openlinker-core/pkg/llm"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcp"
	"github.com/OpenLinker-ai/openlinker-core/pkg/registry"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skill"
	"github.com/OpenLinker-ai/openlinker-core/pkg/task"
	"github.com/OpenLinker-ai/openlinker-core/pkg/webhook"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

type Options struct {
	APIKeyVerifier  auth.ApiKeyVerifier
	LLMClient       corellm.Client
	AdminMiddleware echo.MiddlewareFunc
	UserProvisioner auth.UserProvisioner
	WalletCharger   runtime.WalletCharger
	RuntimeLimiter  runtime.EndpointLimiter
}

type Services struct {
	Auth        *auth.Service
	Admin       *coreadmin.Service
	AgentMarket *agent.MarketService
	Agent       *agent.Service
	Skill       *skill.Service
	Runtime     *runtime.Service
	Webhook     *webhook.Service
	A2A         *a2a.Service
	Workflow    *workflow.Service
	Registry    *registry.Service
	Benchmark   *skill.BenchmarkService
	Task        *task.Service
	MCP         *mcp.Service
	Delivery    *delivery.Service
}

// Register mounts all core-owned HTTP routes and starts core-owned workers.
func Register(rootCtx context.Context, e *echo.Echo, pool *pgxpool.Pool, cfg *config.Config, opts Options) *Services {
	e.GET("/.well-known/openlinker.json", discovery.ServeOpenLinkerManifest(cfg))
	api := e.Group("/api/v1")

	ConfigureGoth(cfg)
	authSvc := auth.NewService(pool, cfg.JWTSecret, time.Duration(cfg.JWTExpireHours)*time.Hour)
	authSvc.SetUserProvisioner(opts.UserProvisioner)
	authHandler := auth.NewHandler(authSvc, cfg)
	jwtMiddleware := auth.JWTMiddleware(cfg.JWTSecret)
	authHandler.Register(api)
	authHandler.RegisterProtected(api, jwtMiddleware)
	var adminSvc *coreadmin.Service
	if opts.AdminMiddleware != nil {
		adminSvc = coreadmin.NewService(pool)
		coreadmin.NewHandler(adminSvc).Register(api, jwtMiddleware, opts.AdminMiddleware)
	}
	cloudBridgeHandler := cloudbridge.NewHandler(cloudbridge.NewService(pool))
	cloudBridgeHandler.Register(e, jwtMiddleware)
	cloudBridgeHandler.RegisterCoreAPI(api, jwtMiddleware)

	agentMarketSvc := agent.NewMarketService(pool)
	agentMarketHandler := agent.NewMarketHandler(agentMarketSvc)
	agentMarketHandler.Register(api)
	agentMarketHandler.RegisterProtected(api, jwtMiddleware)

	agentSvc := agent.NewService(pool, cfg)
	agentHandler := agent.NewHandler(agentSvc, cfg)
	agentHandler.Register(api)
	agentHandler.RegisterProtected(api, jwtMiddleware)
	if opts.AdminMiddleware != nil {
		agentHandler.RegisterAdmin(api, jwtMiddleware, opts.AdminMiddleware)
	}

	registrationSvc := agent.NewRegistrationService(pool, cfg)
	registrationHandler := agent.NewRegistrationHandler(registrationSvc)
	registrationHandler.RegisterProtected(api, jwtMiddleware)
	registrationHandler.RegisterPublic(api)

	approvalSvc := agent.NewApprovalService(pool, cfg)
	approvalHandler := agent.NewApprovalHandler(approvalSvc)
	approvalHandler.RegisterProtected(api, jwtMiddleware)

	metricSvc := agent.NewMetricService(pool)
	metricHandler := agent.NewMetricHandler(metricSvc)
	metricHandler.Register(api)
	agent.StartMetricWorker(rootCtx, metricSvc, approvalSvc)

	e.GET("/skill/publish-agent", agent.ServePublishAgentSkill)
	e.GET("/skill/consume-agent", agent.ServeConsumeAgentSkill)

	skillSvc := skill.NewService(pool)
	skillHandler := skill.NewHandler(skillSvc, pool)
	skillHandler.Register(api)
	skillHandler.RegisterProtected(api, jwtMiddleware)

	hybridMw := auth.HybridAuthMiddleware(cfg.JWTSecret, opts.APIKeyVerifier)

	runtimeSvc := runtime.NewService(pool, cfg)
	if opts.WalletCharger != nil {
		runtimeSvc.SetWalletCharger(opts.WalletCharger)
	}
	runtimeHandler := runtime.NewHandler(runtimeSvc, cfg)
	runtimeHandler.SetEndpointLimiter(opts.RuntimeLimiter)
	runtimeHandler.RegisterProtected(api, hybridMw, hybridMw)
	runtimeHandler.RegisterAgentRuntime(api)
	agentSvc.SetDryRunner(runtimeSvc)
	if cfg.RuntimePullRunWorkerEnabled {
		go runtime.StartRuntimePullRunWorker(rootCtx, runtimeSvc, runtime.RuntimePullRunWorkerConfig{
			Interval:        time.Duration(cfg.RuntimePullRunWorkerIntervalSeconds) * time.Second,
			DispatchTimeout: time.Duration(cfg.RuntimePullDispatchTimeoutSeconds) * time.Second,
			ResultTimeout:   time.Duration(cfg.RuntimePullResultTimeoutSeconds) * time.Second,
			BatchSize:       int32(cfg.RuntimePullRunWorkerTimeoutBatchSize),
		})
	}
	if cfg.AvailabilityMonitorEnabled {
		agent.StartAvailabilityMonitor(rootCtx, agentSvc, agent.AvailabilityMonitorConfig{
			Interval:     time.Duration(cfg.AvailabilityMonitorIntervalSeconds) * time.Second,
			InitialDelay: time.Duration(cfg.AvailabilityMonitorInitialDelaySeconds) * time.Second,
			StaleAfter:   time.Duration(cfg.AvailabilityMonitorStaleSeconds) * time.Second,
			BatchSize:    int32(cfg.AvailabilityMonitorBatchSize),
		})
	}

	webhookSvc := webhook.NewService(pool, cfg)
	webhookHandler := webhook.NewHandler(webhookSvc, cfg)
	webhookHandler.RegisterProtected(api, jwtMiddleware)
	runtimeSvc.SetTaskCallbackEnqueuer(webhookSvc)
	go webhook.StartWorker(rootCtx, webhookSvc)

	a2aSvc := a2a.NewService(pool, runtimeSvc)
	a2aSvc.SetTaskCallbackManager(webhookSvc)
	a2aHandler := a2a.NewHandler(a2aSvc)
	a2aHandler.SetAgentCardProvider(agentMarketSvc)
	a2aHandler.Register(api, jwtMiddleware, hybridMw)
	configureA2AGRPCAgentCard(cfg, &Services{AgentMarket: agentMarketSvc})

	workflowSvc := workflow.NewService(pool, runtimeSvc)
	workflowHandler := workflow.NewHandler(workflowSvc)
	workflowHandler.RegisterProtected(api, jwtMiddleware)
	if cfg.WorkflowRunWorkerEnabled {
		go workflow.StartRunWorker(rootCtx, workflowSvc, workflow.RunWorkerConfig{
			Interval:   time.Duration(cfg.WorkflowRunWorkerIntervalSeconds) * time.Second,
			StaleAfter: time.Duration(cfg.WorkflowRunStaleSeconds) * time.Second,
			ClaimBurst: cfg.WorkflowRunClaimBurst,
		})
	}

	registrySvc := registry.NewService(pool)
	registryHandler := registry.NewHandler(registrySvc)
	registryHandler.RegisterProtected(api, jwtMiddleware)
	if cfg.RegistryProxyRunWorkerEnabled {
		go registry.StartProxyRunWorker(rootCtx, registrySvc, registry.ProxyRunWorkerConfig{
			Interval: time.Duration(cfg.RegistryProxyRunWorkerIntervalSeconds) * time.Second,
			Timeout:  time.Duration(cfg.RegistryProxyRunTimeoutSeconds) * time.Second,
		})
	}

	benchmarkSvc := skill.NewBenchmarkService(skillSvc, runtimeSvc, opts.LLMClient)
	benchmarkHandler := skill.NewBenchmarkHandler(benchmarkSvc)
	benchmarkHandler.Register(api)
	benchmarkHandler.RegisterProtected(api, jwtMiddleware)

	taskSvc := task.NewService(pool, opts.LLMClient, skillAdapter{inner: skillSvc})
	taskSvc.SetRunStarter(runtimeSvc)
	taskHandler := task.NewHandler(taskSvc)
	taskHandler.RegisterProtected(api, jwtMiddleware)

	mcpSvc := mcp.NewService(agentMarketSvc, runtimeSvc, taskSvc)
	mcpHandler := mcp.NewHandler(mcpSvc)
	mcpHandler.Register(api, hybridMw)

	deliverySvc := delivery.NewService(pool, cfg)
	deliveryHandler := delivery.NewHandler(deliverySvc)
	deliveryHandler.RegisterProtected(api, jwtMiddleware)
	runtimeSvc.SetDeliveryEnqueuer(deliverySvc)
	go delivery.StartWorker(rootCtx, deliverySvc)

	return &Services{
		Auth:        authSvc,
		AgentMarket: agentMarketSvc,
		Admin:       adminSvc,
		Agent:       agentSvc,
		Skill:       skillSvc,
		Runtime:     runtimeSvc,
		Webhook:     webhookSvc,
		A2A:         a2aSvc,
		Workflow:    workflowSvc,
		Registry:    registrySvc,
		Benchmark:   benchmarkSvc,
		Task:        taskSvc,
		MCP:         mcpSvc,
		Delivery:    deliverySvc,
	}
}

// ConfigureGoth initializes OAuth providers and the cookie session store.
func ConfigureGoth(cfg *config.Config) {
	store := sessions.NewCookieStore([]byte(cfg.JWTSecret))
	store.Options.HttpOnly = true
	store.Options.Secure = cfg.IsProduction()
	store.Options.SameSite = http.SameSiteLaxMode
	store.Options.Path = "/"
	store.MaxAge(600)
	gothic.Store = store

	if cfg.GoogleClientID != "" && cfg.GoogleClientSecret != "" {
		callback := oauthCallbackBaseURL(cfg) + "/api/v1/auth/google/callback"
		goth.UseProviders(gothgoogle.New(cfg.GoogleClientID, cfg.GoogleClientSecret, callback, "email", "profile"))
		log.Info().Str("callback", callback).Msg("google oauth configured")
	}
	if cfg.GithubClientID != "" && cfg.GithubClientSecret != "" {
		callback := oauthCallbackBaseURL(cfg) + "/api/v1/auth/github/callback"
		goth.UseProviders(gothgithub.New(cfg.GithubClientID, cfg.GithubClientSecret, callback, "user:email"))
		log.Info().Str("callback", callback).Msg("github oauth configured")
	}
}

func oauthCallbackBaseURL(cfg *config.Config) string {
	if cfg == nil {
		return "http://localhost:8080"
	}
	if value := strings.TrimRight(strings.TrimSpace(cfg.OAuthCallbackBaseURL), "/"); value != "" {
		return value
	}
	if value := strings.TrimRight(strings.TrimSpace(cfg.APIURL), "/"); value != "" {
		return value
	}
	return "http://localhost:8080"
}

type skillAdapter struct {
	inner *skill.Service
}

func (a skillAdapter) ListAll(ctx context.Context) ([]db.Skill, error) {
	return a.inner.ListAll(ctx)
}

func (a skillAdapter) RecommendAgentsBySkills(ctx context.Context, skillIDs []string, limit int) ([]task.AgentMatch, error) {
	matches, err := a.inner.RecommendAgentsBySkills(ctx, skillIDs, limit)
	if err != nil {
		return nil, err
	}
	out := make([]task.AgentMatch, len(matches))
	for i := range matches {
		out[i] = task.AgentMatch{AgentID: matches[i].AgentID, MatchCount: matches[i].MatchCount}
	}
	return out, nil
}
