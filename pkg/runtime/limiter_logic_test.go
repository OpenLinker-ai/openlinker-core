package runtime

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeEndpointLimiterStateMachine(t *testing.T) {
	now := time.Date(2026, 6, 20, 11, 0, 0, 0, time.UTC)
	limiter := newRuntimeEndpointLimiter()
	limiter.now = func() time.Time { return now }

	if retry := limiter.allowMalformedAuth("ip:1"); retry != 0 {
		t.Fatalf("first malformed auth retry = %v", retry)
	}
	if retry := limiter.allowMalformedAuth("ip:1"); retry != runtimeMalformedAuthMinInterval {
		t.Fatalf("second malformed auth retry = %v", retry)
	}
	now = now.Add(runtimeMalformedAuthMinInterval)
	if retry := limiter.allowMalformedAuth("ip:1"); retry != 0 {
		t.Fatalf("malformed auth after interval retry = %v", retry)
	}

	if retry := limiter.allowHeartbeat("token:1"); retry != 0 {
		t.Fatalf("first heartbeat retry = %v", retry)
	}
	if retry := limiter.allowHeartbeat("token:1"); retry != runtimePullHeartbeatMinInterval {
		t.Fatalf("second heartbeat retry = %v", retry)
	}

	if retry, release := limiter.beginClaim("token:claim", 0); retry != 0 {
		t.Fatalf("non-long-poll claim retry = %v", retry)
	} else {
		release()
	}
	if retry, release := limiter.beginClaim("token:claim", time.Second); retry != 0 {
		t.Fatalf("first long-poll claim retry = %v", retry)
	} else {
		if retry, noop := limiter.beginClaim("token:claim", time.Second); retry != runtimePullConcurrentClaimRetryAfter {
			t.Fatalf("concurrent long-poll retry = %v", retry)
		} else {
			noop()
		}
		release()
	}
	if retry, release := limiter.beginClaim("token:claim", time.Second); retry != 0 {
		t.Fatalf("released long-poll claim retry = %v", retry)
	} else {
		release()
	}

	limiter.markEmptyClaim("token:empty", 0)
	if retry, release := limiter.beginClaim("token:empty", time.Second); retry != runtimePullEmptyClaimRetryAfter {
		t.Fatalf("empty claim cooldown retry = %v", retry)
	} else {
		release()
	}
	now = now.Add(runtimePullEmptyClaimRetryAfter)
	if retry, release := limiter.beginClaim("token:empty", time.Second); retry != 0 {
		t.Fatalf("empty claim after cooldown retry = %v", retry)
	} else {
		release()
	}
}

func TestRuntimeEndpointLimiterPurgeAndNilWorker(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	limiter := newRuntimeEndpointLimiter()
	limiter.now = func() time.Time { return now }

	limiter.allowHeartbeat("idle")
	if _, release := limiter.beginClaim("active", time.Second); release != nil {
		now = now.Add(runtimeLimiterIdleTTL + runtimeLimiterPurgeInterval + time.Second)
		limiter.allowHeartbeat("fresh")
		if _, ok := limiter.states["idle"]; ok {
			t.Fatalf("idle limiter state should be purged")
		}
		if _, ok := limiter.states["active"]; !ok {
			t.Fatalf("active long-poll state should not be purged")
		}
		release()
	}

	StartRuntimePullRunWorker(context.Background(), nil, RuntimePullRunWorkerConfig{})
}
