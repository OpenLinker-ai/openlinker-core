package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PoolOptions struct {
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

func DefaultPoolOptions() PoolOptions {
	return PoolOptions{
		MaxConns:          20,
		MinConns:          2,
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: time.Minute,
	}
}

// Connect 建立 pgx 连接池。
// 调用方必须在程序退出时调用 pool.Close()。
func Connect(ctx context.Context, databaseURL string, opts ...PoolOptions) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}

	applyPoolOptions(cfg, opts...)

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("new pgx pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db ping: %w", err)
	}
	return pool, nil
}

func applyPoolOptions(cfg *pgxpool.Config, opts ...PoolOptions) {
	poolOpts := DefaultPoolOptions()
	if len(opts) > 0 {
		poolOpts = opts[0]
		defaults := DefaultPoolOptions()
		if poolOpts.MaxConns <= 0 {
			poolOpts.MaxConns = defaults.MaxConns
		}
		if poolOpts.MinConns < 0 {
			poolOpts.MinConns = defaults.MinConns
		}
		if poolOpts.MaxConnLifetime <= 0 {
			poolOpts.MaxConnLifetime = defaults.MaxConnLifetime
		}
		if poolOpts.MaxConnIdleTime <= 0 {
			poolOpts.MaxConnIdleTime = defaults.MaxConnIdleTime
		}
		if poolOpts.HealthCheckPeriod <= 0 {
			poolOpts.HealthCheckPeriod = defaults.HealthCheckPeriod
		}
	}
	if poolOpts.MinConns > poolOpts.MaxConns {
		poolOpts.MinConns = poolOpts.MaxConns
	}
	cfg.MaxConns = poolOpts.MaxConns
	cfg.MinConns = poolOpts.MinConns
	cfg.MaxConnLifetime = poolOpts.MaxConnLifetime
	cfg.MaxConnIdleTime = poolOpts.MaxConnIdleTime
	cfg.HealthCheckPeriod = poolOpts.HealthCheckPeriod
}
