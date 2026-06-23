package ratelimit

import (
	"context"
	"fmt"
	"strings"
	"time"

	redis_rate "github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

// RedisStore implements Echo's RateLimiterStore with a Redis token bucket.
type RedisStore struct {
	limiter *redis_rate.Limiter
	limit   redis_rate.Limit
	prefix  string
	timeout time.Duration
}

func NewRedisStore(client redis.UniversalClient, prefix string, rate, burst int, period, timeout time.Duration) *RedisStore {
	if timeout <= 0 {
		timeout = time.Second
	}
	return &RedisStore{
		limiter: redis_rate.NewLimiter(client),
		limit: redis_rate.Limit{
			Rate:   rate,
			Burst:  burst,
			Period: period,
		},
		prefix:  strings.TrimSpace(prefix),
		timeout: timeout,
	}
}

func (s *RedisStore) Allow(identifier string) (bool, error) {
	if s == nil || s.limiter == nil {
		return false, fmt.Errorf("redis rate limiter is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	result, err := s.limiter.Allow(ctx, s.prefix+":"+identifier, s.limit)
	if err != nil {
		return false, err
	}
	return result.Allowed > 0, nil
}
