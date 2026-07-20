// Command api is the openlinker-core HTTP server.
//
// Pulls in only modules that live under openlinker-core/pkg/:
// auth, agent, runtime, skill, task, mcp, delivery, webhook, user, and local
// admin. Wallet, payment, hosted dashboard, and managed token-policy products
// live outside Core. Local User Token issuance belongs to Core; it is separate
// from billing logic.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	migratecmd "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	emw "github.com/labstack/echo/v4/middleware"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/coreapi"
	"github.com/OpenLinker-ai/openlinker-core/pkg/db"
	dbgen "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/externalexecution"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/llm"
	openlinkerlog "github.com/OpenLinker-ai/openlinker-core/pkg/log"
	"github.com/OpenLinker-ai/openlinker-core/pkg/ratelimit"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const maxRequestBodySize = "8M"

const minimumProductionSecretBytes = 32

type coreServingMode string

const (
	coreServingModeFull              coreServingMode = "full"
	coreServingModeRuntimeAttachOnly coreServingMode = "runtime-attach-only"
)

func parseCoreServingMode(value string) (coreServingMode, error) {
	switch strings.TrimSpace(value) {
	case "", string(coreServingModeFull):
		return coreServingModeFull, nil
	case string(coreServingModeRuntimeAttachOnly):
		return coreServingModeRuntimeAttachOnly, nil
	default:
		return "", fmt.Errorf("OPENLINKER_CORE_MODE must be full or runtime-attach-only")
	}
}

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "migrate" {
		runMigrate(os.Args[2:])
		return
	}
	if len(os.Args) >= 2 && os.Args[1] == "bootstrap-admin" {
		runBootstrapAdmin(os.Args[2:])
		return
	}
	if len(os.Args) >= 2 && os.Args[1] == "runtime-node" {
		if code := runRuntimeNode(os.Args[2:], os.Getenv, os.Stdout, os.Stderr); code != 0 {
			os.Exit(code)
		}
		return
	}
	processExitCode := 0
	// Registered before resource cleanup defers so a strict shutdown failure
	// becomes a non-zero process result only after those cleanups have run.
	defer func() {
		if processExitCode != 0 {
			os.Exit(processExitCode)
		}
	}()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	if err := validateProductionConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	servingMode, err := parseCoreServingMode(os.Getenv("OPENLINKER_CORE_MODE"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	runtimeAttachOnly := servingMode == coreServingModeRuntimeAttachOnly

	openlinkerlog.Init(cfg.LogLevel, cfg.IsProduction())
	log.Info().Str("env", cfg.Env).Str("mode", string(servingMode)).Int("port", cfg.Port).
		Msg("openlinker-core starting")

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.Connect(rootCtx, cfg.DatabaseURL, dbPoolOptions(cfg))
	if err != nil {
		log.Fatal().Err(err).Msg("connect database failed")
	}
	defer pool.Close()
	log.Info().Msg("database connected")
	if !runtimeAttachOnly {
		if err := autoBootstrapAdminIfNeeded(rootCtx, pool, cfg.Env); err != nil {
			log.Fatal().Err(err).Msg("bootstrap admin failed")
		}
	}

	var rateLimiterStore emw.RateLimiterStore
	var redisClient *redis.Client
	redisAccelerationRequested := strings.TrimSpace(os.Getenv("REDIS_URL")) != ""
	if redisAccelerationRequested || cfg.RuntimeHAMode ||
		(!runtimeAttachOnly && cfg.ExternalExecutionEnabled()) {
		redisClient, err = newRedisClient(cfg.RedisURL)
		if err != nil {
			log.Fatal().Err(err).Msg("configure redis failed")
		}
		defer func() { _ = redisClient.Close() }()
	}
	if !runtimeAttachOnly && cfg.IsProduction() && redisClient != nil {
		rateLimiterStore = ratelimit.NewRedisStore(
			redisClient,
			"openlinker:core:http",
			httpRateLimitRate(cfg),
			httpRateLimitBurst(cfg),
			httpRateLimitPeriod(cfg),
			time.Second,
		)
		log.Info().Msg("redis-backed HTTP rate limiter configured")
	} else if !runtimeAttachOnly && cfg.IsProduction() {
		log.Info().Msg("single-instance in-memory HTTP rate limiter configured")
	}

	coreInstanceID := uuid.New()
	var runtimeSignalBus runtime.RuntimeSignalBus = runtime.NewLocalSignalBus(coreInstanceID)
	if redisClient != nil {
		runtimeSignalBus, err = runtime.NewRedisSignalBus(redisClient, runtime.RedisSignalBusConfig{
			InstanceID: coreInstanceID,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("configure runtime signal bus failed")
		}
	}
	defer func() { _ = runtimeSignalBus.Close() }()
	cluster, err := runtime.NewRuntimeClusterCoordinator(
		pool,
		runtimeSignalBus,
		runtime.RuntimeClusterIdentity{
			InstanceID:            coreInstanceID,
			ReleaseVersion:        cfg.ReleaseVersion,
			ReleaseCommit:         cfg.ReleaseCommit,
			SchemaVersion:         runtime.RuntimeSchemaVersion,
			SchemaChecksum:        runtime.RuntimeSchemaChecksum,
			RuntimeContractID:     runtime.RuntimeContractID,
			RuntimeContractDigest: runtime.RuntimeContractDigest,
		},
		cfg.RuntimeHAMode,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("configure runtime cluster failed")
	}
	cluster.Start(rootCtx)
	var externalExecutionAuthorizer *externalexecution.Authorizer
	if !runtimeAttachOnly {
		externalExecutionAuthorizer, err = buildExternalExecutionAuthorizer(cfg, redisClient)
		if err != nil {
			log.Fatal().Err(err).Msg("configure external execution authentication failed")
		}
	}

	e := newEcho(cfg, rateLimiterStore)
	readiness := clusterReadinessChecker(cluster)
	if !runtimeAttachOnly {
		readiness = applicationReadiness(cfg, cluster, redisClient)
	}
	registerHealthRoutes(e, cfg, pool, readiness)
	opts := coreapi.Options{
		CoreInstanceID:   coreInstanceID,
		RuntimeSignalBus: runtimeSignalBus,
	}
	if !runtimeAttachOnly {
		if redisClient != nil {
			opts.AgentMetricDirtyStore, err = agent.NewRedisAgentMetricDirtyStore(redisClient, "")
			if err != nil {
				log.Fatal().Err(err).Msg("configure agent metric dirty store failed")
			}
		}
		opts.AdminMiddleware = auth.AdminMiddleware(dbgen.New(pool))
		opts.LLMClient = buildLLMClient(cfg)
		opts.ExternalExecutionAuthorizer = externalExecutionAuthorizer
		log.Info().Msg("runtime billing is not part of core; run cost metadata is not settled")
		if opts.LLMClient == nil {
			log.Info().Msg("no llm client configured; task routing uses rule-based keyword fallback")
		}
	}
	var services *coreapi.Services
	var shutdownA2AGRPC coreapi.ShutdownFunc
	if runtimeAttachOnly {
		services = coreapi.RegisterRuntimeAttachOnly(rootCtx, e, pool, cfg, opts)
		log.Info().Msg("runtime attach-only boundary active; application workers and execution routes are disabled")
	} else {
		services = coreapi.Register(rootCtx, e, pool, cfg, opts)
		shutdownA2AGRPC, err = coreapi.StartA2AGRPCServer(rootCtx, cfg, services)
		if err != nil {
			log.Fatal().Err(err).Msg("start a2a grpc server failed")
		}
	}
	runtimeMTLSServer, runtimeMTLSListener, err := startRuntimeMTLSListener(cfg, e)
	if err != nil {
		log.Fatal().Err(err).Msg("start runtime mTLS listener failed")
	}
	serveErrors := make(chan error, 2)
	if runtimeMTLSServer != nil {
		go func() {
			if serveErr := runtimeMTLSServer.Serve(runtimeMTLSListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				serveErrors <- fmt.Errorf("runtime mTLS server failed: %w", serveErr)
			}
		}()
		log.Info().Int("port", cfg.RuntimeMTLSPort).Msg("agent runtime mTLS listener active")
	}

	srv := newHTTPServer(cfg.Port)
	go func() {
		if err := e.StartServer(srv); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrors <- fmt.Errorf("HTTP server failed: %w", err)
		}
	}()
	log.Info().Int("port", cfg.Port).Msg("listening")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	if serveErr := waitForCoreStop(stop, serveErrors); serveErr != nil {
		processExitCode = 1
		log.Error().Err(serveErr).Msg("server stopped unexpectedly; beginning strict shutdown")
	}
	signal.Stop(stop)
	log.Info().Msg("shutting down")
	cancel()

	shutdownPlan := coreShutdownPlan{
		PhaseTimeout:        defaultCoreShutdownPhaseTimeout,
		RuntimeAttachOnly:   runtimeAttachOnly,
		ShutdownHTTP:        e.Shutdown,
		CloseRuntimeCluster: cluster.Close,
	}
	if services != nil && services.RuntimeController != nil {
		shutdownPlan.ShutdownRuntimeController = services.RuntimeController.Shutdown
	}
	if runtimeMTLSServer != nil {
		shutdownPlan.ShutdownRuntimeMTLS = runtimeMTLSServer.Shutdown
	}
	if runtimeAttachOnly && services != nil && services.RuntimeSessions != nil {
		shutdownPlan.DetachSessions = services.RuntimeSessions.DetachCutoverSessions
	}
	if shutdownA2AGRPC != nil {
		shutdownPlan.ShutdownA2AGRPC = coreShutdownFunc(shutdownA2AGRPC)
	}
	shutdownResult, shutdownErr := runCoreShutdown(shutdownPlan)
	if shutdownResult.DetachCompleted {
		log.Info().Int64("sessions", shutdownResult.DetachedSessions).
			Msg("runtime attach-only Sessions detached for Core handoff")
	}
	if shutdownErr != nil {
		processExitCode = 1
		log.Error().Err(shutdownErr).Msg("strict Core shutdown failed")
	}
	log.Info().Msg("bye")
}

func requestLogger() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)
			if err != nil {
				c.Error(err)
			}
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
			return nil
		}
	}
}

type dbPinger interface {
	Ping(context.Context) error
}

type clusterReadinessChecker interface {
	Readiness(context.Context) runtime.RuntimeClusterReadiness
}

type readinessDependencyPinger interface {
	Ping(context.Context) error
}

type redisReadinessPinger struct {
	client *redis.Client
}

func (p redisReadinessPinger) Ping(ctx context.Context) error {
	if p.client == nil {
		return errors.New("redis client is unavailable")
	}
	return p.client.Ping(ctx).Err()
}

type externalExecutionReadiness struct {
	base   clusterReadinessChecker
	replay readinessDependencyPinger
}

func applicationReadiness(cfg *config.Config, base clusterReadinessChecker, redisClient *redis.Client) clusterReadinessChecker {
	if cfg == nil || !cfg.ExternalExecutionEnabled() {
		return base
	}
	return externalExecutionReadiness{base: base, replay: redisReadinessPinger{client: redisClient}}
}

func (r externalExecutionReadiness) Readiness(ctx context.Context) runtime.RuntimeClusterReadiness {
	result := runtime.RuntimeClusterReadiness{Status: "not_ready", Reasons: []string{"cluster_unavailable"}}
	if r.base != nil {
		result = r.base.Readiness(ctx)
	}
	if r.replay == nil || r.replay.Ping(ctx) != nil {
		result.Ready = false
		result.Status = "not_ready"
		for _, reason := range result.Reasons {
			if reason == "external_execution_replay_dependency_unavailable" {
				return result
			}
		}
		result.Reasons = append(result.Reasons, "external_execution_replay_dependency_unavailable")
	}
	return result
}

func newEcho(cfg *config.Config, stores ...emw.RateLimiterStore) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(runtimeListenerIsolation)
	e.Use(emw.Recover())
	e.Use(emw.RequestID())
	e.Use(emw.BodyLimit(maxRequestBodySize))
	e.Use(emw.CORSWithConfig(emw.CORSConfig{
		AllowOrigins:     allowedCORSOrigins(cfg),
		AllowCredentials: true,
		AllowHeaders: []string{
			echo.HeaderContentType,
			echo.HeaderAuthorization,
			"Idempotency-Key",
			"Prefer",
		},
		ExposeHeaders: []string{
			echo.HeaderLocation,
			"Idempotency-Replayed",
			"Preference-Applied",
		},
	}))
	if cfg.IsProduction() {
		e.Use(emw.RateLimiterWithConfig(rateLimiterConfigWithConfig(cfg, stores...)))
	}
	e.Use(requestLogger())
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}
	return e
}

func registerHealthRoutes(e *echo.Echo, cfg *config.Config, pool dbPinger, readiness clusterReadinessChecker) {
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
	e.GET("/readyz", func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 3*time.Second)
		defer cancel()
		if readiness == nil {
			return c.JSON(http.StatusServiceUnavailable, runtime.RuntimeClusterReadiness{
				Status:  "not_ready",
				Reasons: []string{"cluster_unavailable"},
			})
		}
		result := readiness.Readiness(ctx)
		return c.JSON(result.HTTPStatus(), result)
	})
	e.HEAD("/readyz", func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 3*time.Second)
		defer cancel()
		if readiness == nil {
			return c.NoContent(http.StatusServiceUnavailable)
		}
		return c.NoContent(readiness.Readiness(ctx).HTTPStatus())
	})
}

func newRedisClient(rawURL string) (*redis.Client, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, errors.New("redis url is empty")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	// Deliberately do not Ping here. In HA, Redis availability is a readiness
	// dependency, not a process-start dependency; PostgreSQL reconciliation
	// must keep running and the client must recover without a restart.
	return redis.NewClient(options), nil
}

func newHTTPServer(port int) *http.Server {
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       120 * time.Second,
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

func validateProductionConfig(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	if err := validateRuntimeMTLSConfig(cfg); err != nil {
		return err
	}
	if err := validateExternalExecutionCallerServiceID(cfg); err != nil {
		return err
	}
	if _, err := auth.ParseOAuthCodeStorageMode(cfg.OAuthCodeStorageMode); err != nil {
		return fmt.Errorf("OAUTH_CODE_STORAGE_MODE: %w", err)
	}
	if !cfg.IsProduction() {
		return nil
	}
	if strings.TrimSpace(cfg.FrontendURL) == "" {
		return fmt.Errorf("FRONTEND_URL is required in production")
	}
	if release := strings.TrimSpace(cfg.ReleaseVersion); release == "" || release == "local" {
		return fmt.Errorf("OPENLINKER_RELEASE_ID must identify the deployed release in production")
	}
	if commit := strings.TrimSpace(cfg.ReleaseCommit); commit == "" || commit == "unknown" {
		return fmt.Errorf("OPENLINKER_GIT_SHA must identify the deployed commit in production")
	}
	if len(cfg.JWTSecret) < minimumProductionSecretBytes {
		return fmt.Errorf("JWT_SECRET must be at least %d bytes in production", minimumProductionSecretBytes)
	}
	if internalToken := strings.TrimSpace(cfg.InternalToken); internalToken != "" && len(internalToken) < minimumProductionSecretBytes {
		return fmt.Errorf("OPENLINKER_INTERNAL_TOKEN must be at least %d bytes in production when configured", minimumProductionSecretBytes)
	}
	if cfg.RuntimeHAMode && strings.TrimSpace(cfg.RedisURL) == "" {
		return fmt.Errorf("REDIS_URL is required for production runtime HA")
	}
	if cfg.ExternalExecutionEnabled() {
		if strings.TrimSpace(cfg.RedisURL) == "" {
			return fmt.Errorf("REDIS_URL is required for external execution replay protection")
		}
		if strings.TrimSpace(cfg.ExternalExecutionJWTCurrentKeyID) == "" ||
			strings.TrimSpace(cfg.ExternalExecutionJWTIssuer) == "" ||
			strings.TrimSpace(cfg.ExternalExecutionJWTAudience) == "" ||
			strings.TrimSpace(cfg.ExternalExecutionCallerServiceID) == "" {
			return fmt.Errorf("external execution JWT current key id, issuer, audience, and caller service id are required")
		}
		nextKeyID := strings.TrimSpace(cfg.ExternalExecutionJWTNextKeyID)
		nextPublicKey := strings.TrimSpace(cfg.ExternalExecutionJWTNextPublicKey)
		if (nextKeyID == "") != (nextPublicKey == "") {
			return fmt.Errorf("external execution JWT next key id and public key must be configured together")
		}
		if nextKeyID != "" && nextKeyID == strings.TrimSpace(cfg.ExternalExecutionJWTCurrentKeyID) {
			return fmt.Errorf("external execution JWT current and next key ids must differ")
		}
	}
	return nil
}

func validateExternalExecutionCallerServiceID(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	callerServiceID := strings.TrimSpace(cfg.ExternalExecutionCallerServiceID)
	if callerServiceID == "" && !cfg.ExternalExecutionEnabled() {
		return nil
	}
	if callerServiceID != externalexecution.LegacyCutoverCallerServiceID {
		return fmt.Errorf(
			"external execution caller service id (EXTERNAL_EXECUTION_CALLER_SERVICE_ID) must be %q while migration 074 legacy rows are supported",
			externalexecution.LegacyCutoverCallerServiceID,
		)
	}
	return nil
}

func buildExternalExecutionAuthorizer(cfg *config.Config, redisClient *redis.Client) (*externalexecution.Authorizer, error) {
	if cfg == nil {
		return nil, nil
	}
	if err := validateExternalExecutionCallerServiceID(cfg); err != nil {
		return nil, err
	}
	if !cfg.ExternalExecutionEnabled() {
		if strings.TrimSpace(cfg.ExternalExecutionJWTNextKeyID) != "" || strings.TrimSpace(cfg.ExternalExecutionJWTNextPublicKey) != "" {
			return nil, errors.New("external execution current verification key is required before a next key")
		}
		return nil, nil
	}
	if redisClient == nil {
		return nil, errors.New("external execution replay protection requires Redis")
	}
	keys := []externalexecution.VerificationKey{{
		KeyID: cfg.ExternalExecutionJWTCurrentKeyID, PublicKey: cfg.ExternalExecutionJWTCurrentPublicKey,
	}}
	if strings.TrimSpace(cfg.ExternalExecutionJWTNextKeyID) != "" || strings.TrimSpace(cfg.ExternalExecutionJWTNextPublicKey) != "" {
		keys = append(keys, externalexecution.VerificationKey{
			KeyID: cfg.ExternalExecutionJWTNextKeyID, PublicKey: cfg.ExternalExecutionJWTNextPublicKey,
		})
	}
	options := make([]externalexecution.AuthorizerOption, 0, 1)
	if cfg.ExternalExecutionRequestBindingRequired {
		options = append(options, externalexecution.WithRequestBindingRequired())
	}
	return externalexecution.NewAuthorizer(
		keys,
		cfg.ExternalExecutionJWTIssuer,
		cfg.ExternalExecutionJWTAudience,
		cfg.ExternalExecutionCallerServiceID,
		externalexecution.NewRedisReplayStore(redisClient),
		options...,
	)
}

func rateLimiterConfig(stores ...emw.RateLimiterStore) emw.RateLimiterConfig {
	return rateLimiterConfigWithConfig(nil, stores...)
}

func rateLimiterConfigWithConfig(cfg *config.Config, stores ...emw.RateLimiterStore) emw.RateLimiterConfig {
	var store emw.RateLimiterStore = emw.NewRateLimiterMemoryStoreWithConfig(emw.RateLimiterMemoryStoreConfig{
		Rate:      rate.Limit(httpRateLimitRate(cfg)),
		Burst:     httpRateLimitBurst(cfg),
		ExpiresIn: 3 * time.Minute,
	})
	if len(stores) > 0 && stores[0] != nil {
		store = stores[0]
	}
	return emw.RateLimiterConfig{
		Skipper: func(c echo.Context) bool {
			path := c.Request().URL.Path
			// Runtime uses long-lived WebSocket or pull transports, so the shared
			// request/IP limiter is the wrong admission boundary even when clients
			// connect directly. Runtime is protected by device mTLS, Agent Token
			// authentication, principal limits, and its protocol admission limiter.
			return path == "/healthz" || path == "/healthz/db" || path == "/readyz" ||
				strings.HasPrefix(path, runtimePathPrefix)
		},
		Store: store,
		DenyHandler: func(c echo.Context, _ string, _ error) error {
			return httpx.NewError(http.StatusTooManyRequests, httpx.CodeRateLimited, "请求过于频繁，请稍后再试")
		},
	}
}

func httpRateLimitRate(cfg *config.Config) int {
	if cfg != nil && cfg.HTTPRateLimitRate > 0 {
		return cfg.HTTPRateLimitRate
	}
	return 50
}

func httpRateLimitBurst(cfg *config.Config) int {
	if cfg != nil && cfg.HTTPRateLimitBurst > 0 {
		return cfg.HTTPRateLimitBurst
	}
	return 200
}

func httpRateLimitPeriod(cfg *config.Config) time.Duration {
	if cfg != nil && cfg.HTTPRateLimitPeriodSec > 0 {
		return time.Duration(cfg.HTTPRateLimitPeriodSec) * time.Second
	}
	return time.Second
}

func dbPoolOptions(cfg *config.Config) db.PoolOptions {
	return db.PoolOptions{
		MaxConns:          cfg.DBMaxConns,
		MinConns:          cfg.DBMinConns,
		MaxConnLifetime:   time.Duration(cfg.DBMaxConnLifetimeMinutes) * time.Minute,
		MaxConnIdleTime:   time.Duration(cfg.DBMaxConnIdleTimeMinutes) * time.Minute,
		HealthCheckPeriod: time.Duration(cfg.DBHealthCheckPeriodSeconds) * time.Second,
	}
}

// buildLLMClient selects an LLM client based on configuration priority:
//  1. LLM_COMPLETE_URL (internal proxy, openlinker.ai cloud only)
//  2. LLM_OPENAI_URL + LLM_OPENAI_API_KEY (self-hosted, any OpenAI-compatible API)
//  3. nil → task routing falls back to rule-based keyword matching
func buildLLMClient(cfg *config.Config) llm.Client {
	if cfg.LLMCompleteURL != "" {
		c := llm.NewRemoteClient(cfg.LLMCompleteURL, cfg.InternalToken)
		if c != nil {
			log.Info().Str("endpoint", cfg.LLMCompleteURL).Msg("llm: using internal proxy client")
			return c
		}
	}
	if cfg.LLMOpenAIURL != "" {
		c := llm.NewOpenAIClient(cfg.LLMOpenAIURL, cfg.LLMOpenAIAPIKey, cfg.LLMOpenAIModel)
		if c != nil {
			log.Info().Str("url", cfg.LLMOpenAIURL).Str("model", cfg.LLMOpenAIModel).Msg("llm: using openai-compatible client")
			return c
		}
	}
	return nil
}

// runMigrate runs goose-style up/down/status against MIGRATIONS_DIR (default ./migrations).
func runMigrate(args []string) {
	code := runMigrateWith(args, os.Getenv, func(sourceURL, databaseURL string) (migrator, error) {
		return migratecmd.New(sourceURL, databaseURL)
	}, os.Stdout, os.Stderr)
	if code != 0 {
		os.Exit(code)
	}
}

type migrator interface {
	Up() error
	Steps(int) error
	Version() (uint, bool, error)
	Close() (error, error)
}

func runMigrateWith(args []string, getenv func(string) string, newMigrator func(string, string) (migrator, error), stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stdout, "usage: api migrate <up|down|status>")
		return 2
	}
	cmd := args[0]

	dbURL, src, err := migrationConfig(getenv)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	m, err := newMigrator("file://"+src, dbURL)
	if err != nil {
		fmt.Fprintf(stderr, "migrate init: %v\n", err)
		return 1
	}
	defer func() { _, _ = m.Close() }()

	switch cmd {
	case "up":
		if err := m.Up(); err != nil && !errors.Is(err, migratecmd.ErrNoChange) {
			fmt.Fprintf(stderr, "migrate up: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, "migrate up: ok")
	case "down":
		if err := m.Steps(-1); err != nil {
			fmt.Fprintf(stderr, "migrate down: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, "migrate down 1 step: ok")
	case "status":
		v, dirty, err := m.Version()
		if err != nil {
			fmt.Fprintf(stderr, "status: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "version=%d dirty=%v\n", v, dirty)
	default:
		fmt.Fprintf(stderr, "unknown migrate command: %s\n", cmd)
		return 2
	}
	return 0
}

func migrationConfig(getenv func(string) string) (dbURL string, src string, err error) {
	dbURL = getenv("DATABASE_URL")
	if dbURL == "" {
		return "", "", errors.New("DATABASE_URL not set")
	}
	src = getenv("MIGRATIONS_DIR")
	if src == "" {
		src = "./migrations"
	}
	return dbURL, src, nil
}
