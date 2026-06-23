// Command api is the openlinker-core HTTP server.
//
// Pulls in only modules that live under openlinker-core/pkg/:
// auth, agent, runtime, skill, task, mcp, delivery, webhook, user. wallet /
// payment / dashboard / cloud API-key storage live in openlinker-cloud and
// are not wired here -- core is meant to be self-host-able without billing
// or operator tooling.
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
	"github.com/labstack/echo/v4"
	emw "github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/coreapi"
	"github.com/OpenLinker-ai/openlinker-core/pkg/db"
	dbgen "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	openlinkerlog "github.com/OpenLinker-ai/openlinker-core/pkg/log"
	"github.com/OpenLinker-ai/openlinker-core/pkg/ratelimit"
	"github.com/OpenLinker-ai/openlinker-core/pkg/redisx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
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

	pool, err := db.Connect(rootCtx, cfg.DatabaseURL, dbPoolOptions(cfg))
	if err != nil {
		log.Fatal().Err(err).Msg("connect database failed")
	}
	defer pool.Close()
	log.Info().Msg("database connected")

	var runtimeLimiter runtime.EndpointLimiter
	var rateLimiterStore emw.RateLimiterStore
	if cfg.IsProduction() {
		redisClient, err := redisx.Connect(rootCtx, cfg.RedisURL)
		if err != nil {
			log.Fatal().Err(err).Msg("connect redis failed")
		}
		defer func() { _ = redisClient.Close() }()
		rateLimiterStore = ratelimit.NewRedisStore(redisClient, "openlinker:core:http", 50, 200, time.Second, time.Second)
		runtimeLimiter = runtime.NewRedisEndpointLimiter(redisClient, "openlinker:core:runtime", time.Second)
		log.Info().Msg("redis-backed rate limiters configured")
	}

	e := newEcho(cfg, rateLimiterStore)
	registerHealthRoutes(e, cfg, pool)
	opts := coreapi.Options{
		AdminMiddleware: auth.AdminMiddleware(dbgen.New(pool)),
		RuntimeLimiter:  runtimeLimiter,
	}
	if verifier := auth.NewRemoteAPIKeyVerifier(cfg.APIKeyVerifyURL); verifier != nil {
		opts.APIKeyVerifier = verifier
		log.Info().Str("endpoint", cfg.APIKeyVerifyURL).Msg("remote api key verifier configured")
	}
	coreapi.Register(rootCtx, e, pool, cfg, opts)

	srv := newHTTPServer(cfg.Port)
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

type dbPinger interface {
	Ping(context.Context) error
}

func newEcho(cfg *config.Config, stores ...emw.RateLimiterStore) *echo.Echo {
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
		e.Use(emw.RateLimiterWithConfig(rateLimiterConfig(stores...)))
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

func registerHealthRoutes(e *echo.Echo, cfg *config.Config, pool dbPinger) {
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

func rateLimiterConfig(stores ...emw.RateLimiterStore) emw.RateLimiterConfig {
	var store emw.RateLimiterStore = emw.NewRateLimiterMemoryStoreWithConfig(emw.RateLimiterMemoryStoreConfig{
		Rate:      50,
		Burst:     200,
		ExpiresIn: 3 * time.Minute,
	})
	if len(stores) > 0 && stores[0] != nil {
		store = stores[0]
	}
	return emw.RateLimiterConfig{
		Skipper: func(c echo.Context) bool {
			path := c.Request().URL.Path
			return path == "/healthz" || path == "/healthz/db"
		},
		Store: store,
		DenyHandler: func(c echo.Context, _ string, _ error) error {
			return httpx.NewError(http.StatusTooManyRequests, "RATE_LIMITED", "请求过于频繁，请稍后再试")
		},
	}
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
