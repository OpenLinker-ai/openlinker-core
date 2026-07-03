package runtime

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type redisRuntimeEndpointLimiter struct {
	client  redis.UniversalClient
	prefix  string
	timeout time.Duration
}

func NewRedisEndpointLimiter(client redis.UniversalClient, prefix string, timeout time.Duration) EndpointLimiter {
	if timeout <= 0 {
		timeout = time.Second
	}
	return &redisRuntimeEndpointLimiter{
		client:  client,
		prefix:  strings.TrimRight(strings.TrimSpace(prefix), ":"),
		timeout: timeout,
	}
}

func (l *redisRuntimeEndpointLimiter) allowMalformedAuth(key string) time.Duration {
	return l.allowInterval("malformed", key, runtimeMalformedAuthMinInterval)
}

func (l *redisRuntimeEndpointLimiter) allowHeartbeat(key string) time.Duration {
	return l.allowInterval("heartbeat", key, runtimePullHeartbeatMinInterval)
}

func (l *redisRuntimeEndpointLimiter) beginClaim(key string, wait time.Duration) (time.Duration, func()) {
	if retry := l.ttl("empty", key); retry > 0 {
		return retry, func() {}
	}
	if wait <= 0 {
		return 0, func() {}
	}
	ttl := wait + runtimePullConcurrentClaimRetryAfter
	ctx, cancel := l.context()
	defer cancel()
	ok, err := l.client.SetNX(ctx, l.key("claim", key), "1", ttl).Result()
	if err != nil {
		log.Error().Err(err).Msg("runtime redis limiter: begin claim")
		return runtimePullConcurrentClaimRetryAfter, func() {}
	}
	if !ok {
		if retry := l.ttl("claim", key); retry > 0 && retry < runtimePullConcurrentClaimRetryAfter {
			return retry, func() {}
		}
		return runtimePullConcurrentClaimRetryAfter, func() {}
	}
	return 0, func() {
		ctx, cancel := l.context()
		defer cancel()
		if err := l.client.Del(ctx, l.key("claim", key)).Err(); err != nil {
			log.Warn().Err(err).Msg("runtime redis limiter: release claim")
		}
	}
}

func (l *redisRuntimeEndpointLimiter) markEmptyClaim(key string, wait time.Duration) {
	ctx, cancel := l.context()
	defer cancel()
	if err := l.client.Set(ctx, l.key("empty", key), "1", runtimePullEmptyClaimRetryAfter).Err(); err != nil {
		log.Error().Err(err).Msg("runtime redis limiter: mark empty claim")
	}
}

func (l *redisRuntimeEndpointLimiter) allowInterval(kind, key string, interval time.Duration) time.Duration {
	ctx, cancel := l.context()
	defer cancel()
	ok, err := l.client.SetNX(ctx, l.key(kind, key), "1", interval).Result()
	if err != nil {
		log.Error().Err(err).Str("kind", kind).Msg("runtime redis limiter")
		return interval
	}
	if ok {
		return 0
	}
	return l.ttl(kind, key)
}

func (l *redisRuntimeEndpointLimiter) ttl(kind, key string) time.Duration {
	ctx, cancel := l.context()
	defer cancel()
	ttl, err := l.client.PTTL(ctx, l.key(kind, key)).Result()
	if err != nil {
		log.Error().Err(err).Str("kind", kind).Msg("runtime redis limiter ttl")
		return runtimePullConcurrentClaimRetryAfter
	}
	if ttl <= 0 {
		return 0
	}
	return ttl
}

func (l *redisRuntimeEndpointLimiter) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), l.timeout)
}

func (l *redisRuntimeEndpointLimiter) key(kind, key string) string {
	prefix := l.prefix
	if prefix == "" {
		prefix = "openlinker:runtime-limiter"
	}
	return prefix + ":" + kind + ":" + key
}
