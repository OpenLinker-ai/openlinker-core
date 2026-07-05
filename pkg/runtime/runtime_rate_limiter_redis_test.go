package runtime

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/redisx"
)

func TestRedisRuntimeEndpointLimiterWithRealRedis(t *testing.T) {
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL not set")
	}
	client, err := redisx.Connect(context.Background(), redisURL)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	defer func() { _ = client.Close() }()

	limiter := NewRedisEndpointLimiter(client, "test:core:runtime:"+t.Name(), time.Second)
	if retry := limiter.allowHeartbeat("token"); retry != 0 {
		t.Fatalf("first heartbeat retry = %s, want 0", retry)
	}
	if retry := limiter.allowHeartbeat("token"); retry <= 0 {
		t.Fatalf("second heartbeat retry = %s, want > 0", retry)
	}

	retry, release := limiter.beginClaim("claim", 5*time.Second)
	if retry != 0 {
		t.Fatalf("first claim retry = %s, want 0", retry)
	}
	retry, noop := limiter.beginClaim("claim", 5*time.Second)
	noop()
	if retry <= 0 {
		t.Fatalf("concurrent claim retry = %s, want > 0", retry)
	}
	release()
	retry, release = limiter.beginClaim("claim", 5*time.Second)
	if retry != 0 {
		t.Fatalf("claim after release retry = %s, want 0", retry)
	}
	release()

	limiter.markEmptyClaim("empty", 0)
	retry, release = limiter.beginClaim("empty", time.Second)
	release()
	if retry <= 0 {
		t.Fatalf("empty claim retry = %s, want > 0", retry)
	}
}
