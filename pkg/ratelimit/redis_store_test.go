package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/redisx"
)

func TestRedisStoreAllowsAndDeniesWithRealRedis(t *testing.T) {
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL not set")
	}
	client, err := redisx.Connect(context.Background(), redisURL)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	defer func() { _ = client.Close() }()

	prefix := "test:core:http:" + t.Name() + ":"
	store := NewRedisStore(client, prefix, 1, 1, time.Minute, time.Second)
	allowed, err := store.Allow("client")
	if err != nil || !allowed {
		t.Fatalf("first Allow = %v, %v; want allowed", allowed, err)
	}
	allowed, err = store.Allow("client")
	if err == nil && allowed {
		t.Fatalf("second Allow should be denied by Redis bucket")
	}
}
