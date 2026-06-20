package db

import (
	"context"
	"strings"
	"testing"
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
