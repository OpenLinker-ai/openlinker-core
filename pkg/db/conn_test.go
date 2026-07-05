package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConnectRejectsInvalidDatabaseURL(t *testing.T) {
	pool, err := Connect(context.Background(), "://not-a-dsn")
	if err == nil {
		if pool != nil {
			pool.Close()
		}
		t.Fatalf("Connect should reject invalid database URL")
	}
	if !strings.Contains(err.Error(), "parse db url") {
		t.Fatalf("Connect error = %v, want parse db url context", err)
	}
}

func TestConnectClosesPoolWhenPingFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pool, err := Connect(ctx, "postgres://user:pass@127.0.0.1:1/openlinker?connect_timeout=1")
	if err == nil {
		if pool != nil {
			pool.Close()
		}
		t.Fatalf("Connect should fail when ping context is canceled")
	}
	if pool != nil {
		t.Fatalf("Connect should not return a pool after ping failure")
	}
	if !strings.Contains(err.Error(), "db ping") && !strings.Contains(err.Error(), "new pgx pool") {
		t.Fatalf("Connect error = %v, want db ping or pool creation context", err)
	}
}

func TestApplyPoolOptions(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://user:pass@localhost/db")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	applyPoolOptions(cfg, PoolOptions{
		MaxConns:          8,
		MinConns:          12,
		MaxConnLifetime:   11 * time.Minute,
		MaxConnIdleTime:   7 * time.Minute,
		HealthCheckPeriod: 13 * time.Second,
	})
	if cfg.MaxConns != 8 || cfg.MinConns != 8 {
		t.Fatalf("pool conn bounds = max %d min %d", cfg.MaxConns, cfg.MinConns)
	}
	if cfg.MaxConnLifetime != 11*time.Minute || cfg.MaxConnIdleTime != 7*time.Minute || cfg.HealthCheckPeriod != 13*time.Second {
		t.Fatalf("pool durations = %#v", cfg)
	}
}
