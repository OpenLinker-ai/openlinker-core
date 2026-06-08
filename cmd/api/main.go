// Command api is the openlinker-core HTTP server.
//
// Pulls in only modules that live under openlinker-core/pkg/:
// auth (sans HybridAuthMiddleware API-key path), agent, runtime, skill, task,
// mcp, delivery, webhook, user. wallet / payment / dashboard / admin /
// apikey live in openlinker-cloud and are not wired here -- core is meant
// to be self-host-able without billing or operator tooling.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	migratecmd "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/gorilla/sessions"
	"github.com/labstack/echo/v4"
	emw "github.com/labstack/echo/v4/middleware"
	"github.com/markbates/goth"
	"github.com/markbates/goth/gothic"
	gothgithub "github.com/markbates/goth/providers/github"
	gothgoogle "github.com/markbates/goth/providers/google"
	"github.com/rs/zerolog/log"

	"github.com/kinzhi/openlinker-core/pkg/a2a"
	"github.com/kinzhi/openlinker-core/pkg/agent"
	"github.com/kinzhi/openlinker-core/pkg/auth"
	"github.com/kinzhi/openlinker-core/pkg/config"
	"github.com/kinzhi/openlinker-core/pkg/db"
	dbgen "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/delivery"
	"github.com/kinzhi/openlinker-core/pkg/discovery"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
	corellm "github.com/kinzhi/openlinker-core/pkg/llm"
	openlinkerlog "github.com/kinzhi/openlinker-core/pkg/log"
	"github.com/kinzhi/openlinker-core/pkg/mcp"
	"github.com/kinzhi/openlinker-core/pkg/registry"
	"github.com/kinzhi/openlinker-core/pkg/runtime"
	"github.com/kinzhi/openlinker-core/pkg/skill"
	"github.com/kinzhi/openlinker-core/pkg/task"
	"github.com/kinzhi/openlinker-core/pkg/webhook"
	"github.com/kinzhi/openlinker-core/pkg/workflow"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "migrate" {
		runMigrate(os.Args[2:])
		return
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	openlinkerlog.Init(cfg.LogLevel, cfg.IsProduction())
	log.Info().Str("env", cfg.Env).Int("port", cfg.Port).Msg("openlinker-core starting")

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.Connect(rootCtx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("connect database failed")
	}
	defer pool.Close()
	log.Info().Msg("database connected")

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(emw.Recover())
	e.Use(emw.RequestID())
	e.Use(emw.CORSWithConfig(emw.CORSConfig{
		AllowOrigins:     allowedCORSOrigins(cfg),
		AllowCredentials: true,
		AllowHeaders:     []string{echo.HeaderContentType, echo.HeaderAuthorization},
	}))
	if cfg.IsProduction() {
		e.Use(emw.RateLimiterWithConfig(rateLimiterConfig()))
	}
	e.Use(requestLogger())
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}

	e.GET("/healthz", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{"status": "ok", "env": cfg.Env})
	})
	e.HEAD("/healthz", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	e.GET("/healthz/db", func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			return httpx.NewError(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable, "database unavailable")
		}
		return c.JSON(http.StatusOK, map[string]string{"db": "ok"})
	})
	e.HEAD("/healthz/db", func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			return httpx.NewError(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable, "database unavailable")
		}
		return c.NoContent(http.StatusOK)
	})
	e.GET("/.well-known/openlinker.json", discovery.ServeOpenLinkerManifest(cfg))

	api := e.Group("/api/v1")

	configureGoth(cfg)
	authSvc := auth.NewService(pool, cfg.JWTSecret, time.Duration(cfg.JWTExpireHours)*time.Hour)
	authHandler := auth.NewHandler(authSvc, cfg)
	jwtMiddleware := auth.JWTMiddleware(cfg.JWTSecret)
	authHandler.Register(api)
	authHandler.RegisterProtected(api, jwtMiddleware)

	agentMarketSvc := agent.NewMarketService(pool)
	agentMarketHandler := agent.NewMarketHandler(agentMarketSvc)
	agentMarketHandler.Register(api)
	agentMarketHandler.RegisterProtected(api, jwtMiddleware)

	agentSvc := agent.NewService(pool, cfg)
	agentHandler := agent.NewHandler(agentSvc, cfg)
	agentHandler.Register(api)
	agentHandler.RegisterProtected(api, jwtMiddleware)

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

	// core 单独部署:HybridAuthMiddleware 传 nil verifier,访问令牌直接 401。
	// 只接受 JWT(浏览器登录),所有访问令牌请求被拒。
	hybridMw := auth.HybridAuthMiddleware(cfg.JWTSecret, nil)

	// core 部署:不注入 WalletCharger,扣费跳过(余额无限),
	// 只写 runs.cost_cents 做账面记录。
	runtimeSvc := runtime.NewService(pool, cfg)
	runtimeHandler := runtime.NewHandler(runtimeSvc, cfg)
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
	runtimeSvc.SetWebhookEnqueuer(webhookSvc)
	runtimeSvc.SetRunWebhookEnqueuer(webhookSvc)
	go webhook.StartWorker(rootCtx, webhookSvc)

	a2aSvc := a2a.NewService(pool, runtimeSvc)
	a2aSvc.SetRunPushManager(webhookSvc)
	a2aHandler := a2a.NewHandler(a2aSvc)
	a2aHandler.Register(api, jwtMiddleware, hybridMw)

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

	// core 部署:LLM client 注入 nil,task / benchmark 自动 fallback 到规则匹配 / 503。
	var llmClient corellm.Client
	benchmarkSvc := skill.NewBenchmarkService(skillSvc, runtimeSvc, llmClient)
	benchmarkHandler := skill.NewBenchmarkHandler(benchmarkSvc)
	benchmarkHandler.Register(api)
	benchmarkHandler.RegisterProtected(api, jwtMiddleware)

	taskSvc := task.NewService(pool, llmClient, skillAdapter{inner: skillSvc})
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

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := e.StartServer(srv); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("server failed")
		}
	}()
	log.Info().Int("port", cfg.Port).Msg("listening")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info().Msg("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("shutdown failed")
	}
	log.Info().Msg("bye")
}

func configureGoth(cfg *config.Config) {
	store := sessions.NewCookieStore([]byte(cfg.JWTSecret))
	store.Options.HttpOnly = true
	store.Options.Secure = cfg.IsProduction()
	store.Options.SameSite = http.SameSiteLaxMode
	store.Options.Path = "/"
	store.MaxAge(600)
	gothic.Store = store

	if cfg.GoogleClientID != "" && cfg.GoogleClientSecret != "" {
		callback := cfg.APIURL + "/api/v1/auth/google/callback"
		goth.UseProviders(gothgoogle.New(cfg.GoogleClientID, cfg.GoogleClientSecret, callback, "email", "profile"))
		log.Info().Str("callback", callback).Msg("google oauth configured")
	}
	if cfg.GithubClientID != "" && cfg.GithubClientSecret != "" {
		callback := cfg.APIURL + "/api/v1/auth/github/callback"
		goth.UseProviders(gothgithub.New(cfg.GithubClientID, cfg.GithubClientSecret, callback, "user:email"))
		log.Info().Str("callback", callback).Msg("github oauth configured")
	}
}

// skillAdapter 把 skill.Service 包装成 task.SkillRecommender,避免 task → skill 反向 import。
type skillAdapter struct {
	inner *skill.Service
}

func (a skillAdapter) ListAll(ctx context.Context) ([]dbgen.Skill, error) {
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

func requestLogger() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)
			req := c.Request()
			res := c.Response()
			ev := log.Info()
			if err != nil {
				ev = log.Warn().Err(err)
			}
			ev.Str("method", req.Method).
				Str("path", req.URL.Path).
				Int("status", res.Status).
				Int64("size", res.Size).
				Dur("dur", time.Since(start)).
				Str("rid", res.Header().Get(echo.HeaderXRequestID)).
				Msg("http")
			return err
		}
	}
}

func allowedCORSOrigins(cfg *config.Config) []string {
	origins := []string{}
	if origin := strings.TrimSpace(cfg.FrontendURL); origin != "" {
		origins = append(origins, origin)
	}
	if !cfg.IsProduction() {
		origins = append(origins, "http://localhost:3000")
	}
	return origins
}

func rateLimiterConfig() emw.RateLimiterConfig {
	return emw.RateLimiterConfig{
		Skipper: func(c echo.Context) bool {
			path := c.Request().URL.Path
			return path == "/healthz" || path == "/healthz/db"
		},
		Store: emw.NewRateLimiterMemoryStoreWithConfig(emw.RateLimiterMemoryStoreConfig{
			Rate:      50,
			Burst:     200,
			ExpiresIn: 3 * time.Minute,
		}),
		DenyHandler: func(c echo.Context, _ string, _ error) error {
			return httpx.NewError(http.StatusTooManyRequests, "RATE_LIMITED", "请求过于频繁，请稍后再试")
		},
	}
}

// runMigrate runs goose-style up/down/status against MIGRATIONS_DIR (default ./migrations).
func runMigrate(args []string) {
	if len(args) == 0 {
		fmt.Println("usage: api migrate <up|down|status>")
		os.Exit(2)
	}
	cmd := args[0]

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL not set")
		os.Exit(1)
	}
	src := os.Getenv("MIGRATIONS_DIR")
	if src == "" {
		src = "./migrations"
	}

	m, err := migratecmd.New("file://"+src, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate init: %v\n", err)
		os.Exit(1)
	}
	defer func() { _, _ = m.Close() }()

	switch cmd {
	case "up":
		if err := m.Up(); err != nil && !errors.Is(err, migratecmd.ErrNoChange) {
			fmt.Fprintf(os.Stderr, "migrate up: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("migrate up: ok")
	case "down":
		if err := m.Steps(-1); err != nil {
			fmt.Fprintf(os.Stderr, "migrate down: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("migrate down 1 step: ok")
	case "status":
		v, dirty, err := m.Version()
		if err != nil {
			fmt.Fprintf(os.Stderr, "status: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("version=%d dirty=%v\n", v, dirty)
	default:
		fmt.Fprintf(os.Stderr, "unknown migrate command: %s\n", cmd)
		os.Exit(2)
	}
}
