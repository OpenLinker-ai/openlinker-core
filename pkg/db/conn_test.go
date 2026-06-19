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
