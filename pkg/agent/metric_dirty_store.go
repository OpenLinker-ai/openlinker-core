package agent

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	defaultMetricDirtyPrefix = "openlinker:metrics:v1"
	maxMetricDirtyBatch      = 512
	metricDirtyShardCount    = 16
	metricCursorTimeFormat   = "20060102T150405.000000Z"
)

var errMetricDirtyStoreUnavailable = errors.New("agent metric dirty store is unavailable")

type AgentMetricCursor struct {
	Time time.Time
	ID   uuid.UUID
}

type AgentMetricDirtyClaim struct {
	AgentID uuid.UUID
	Version int64
	Owner   uuid.UUID
}

type AgentMetricDirtyStore interface {
	Mark(context.Context, []uuid.UUID) error
	Claim(context.Context, uuid.UUID, time.Duration, int) ([]AgentMetricDirtyClaim, error)
	Ack(context.Context, AgentMetricDirtyClaim) (bool, error)
	Nack(context.Context, AgentMetricDirtyClaim) (bool, error)
	Cursor(context.Context) (AgentMetricCursor, bool, error)
	AdvanceCursor(context.Context, AgentMetricCursor) (bool, error)
}

type RedisAgentMetricDirtyStore struct {
	client redis.UniversalClient
	prefix string
}

const metricDirtyMarkScript = `
local clock = redis.call('TIME')
local now_ms = clock[1] * 1000 + math.floor(clock[2] / 1000)
for index = 1, #ARGV do
  redis.call('HINCRBY', KEYS[1], ARGV[index], 1)
  redis.call('ZADD', KEYS[2], now_ms, ARGV[index])
end
return #ARGV
`

const metricDirtyClaimScript = `
local clock = redis.call('TIME')
local now_ms = clock[1] * 1000 + math.floor(clock[2] / 1000)
local owner = ARGV[1]
local lease_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local expired = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now_ms, 'LIMIT', 0, limit * 4)
for _, agent_id in ipairs(expired) do
  redis.call('HDEL', KEYS[3], agent_id)
  redis.call('ZREM', KEYS[4], agent_id)
  redis.call('ZADD', KEYS[2], 'LT', now_ms, agent_id)
end
local candidates = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms, 'LIMIT', 0, limit * 16)
local result = {}
for _, agent_id in ipairs(candidates) do
  if #result >= limit * 2 then
    break
  end
  if not redis.call('HGET', KEYS[3], agent_id) then
    local version = redis.call('HGET', KEYS[1], agent_id)
    if version then
      redis.call('ZREM', KEYS[2], agent_id)
      redis.call('HSET', KEYS[3], agent_id, owner .. '|' .. version)
      redis.call('ZADD', KEYS[4], now_ms + lease_ms, agent_id)
      table.insert(result, agent_id)
      table.insert(result, version)
    end
  end
end
return result
`

const metricDirtyAckScript = `
local expected = ARGV[2] .. '|' .. ARGV[3]
if redis.call('HGET', KEYS[1], ARGV[1]) ~= expected then
  return 0
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`

const metricDirtyNackScript = `
local expected = ARGV[2] .. '|' .. ARGV[3]
if redis.call('HGET', KEYS[1], ARGV[1]) ~= expected then
  return 0
end
local clock = redis.call('TIME')
local now_ms = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], now_ms, ARGV[1])
return 1
`

const metricCursorAdvanceScript = `
local current = redis.call('GET', KEYS[1])
if current and current >= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
`

func NewRedisAgentMetricDirtyStore(
	client redis.UniversalClient,
	prefix string,
) (*RedisAgentMetricDirtyStore, error) {
	if redisClientNil(client) {
		return nil, errMetricDirtyStoreUnavailable
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultMetricDirtyPrefix
	}
	if len(prefix) > 160 || strings.ContainsAny(prefix, "{}\x00\r\n") {
		return nil, errors.New("agent metric dirty Redis prefix is invalid")
	}
	return &RedisAgentMetricDirtyStore{client: client, prefix: prefix}, nil
}

func (s *RedisAgentMetricDirtyStore) Mark(ctx context.Context, agentIDs []uuid.UUID) error {
	if s == nil || redisClientNil(s.client) {
		return errMetricDirtyStoreUnavailable
	}
	if len(agentIDs) == 0 {
		return nil
	}
	if len(agentIDs) > maxMetricDirtyBatch {
		return errors.New("agent metric dirty batch is too large")
	}
	shards := make([][]any, metricDirtyShardCount)
	seen := make(map[uuid.UUID]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		if agentID == uuid.Nil {
			return errors.New("agent metric dirty Agent ID is invalid")
		}
		if _, duplicate := seen[agentID]; duplicate {
			continue
		}
		seen[agentID] = struct{}{}
		shard := metricDirtyShard(agentID)
		shards[shard] = append(shards[shard], agentID.String())
	}
	for shard, args := range shards {
		if len(args) == 0 {
			continue
		}
		if err := s.client.Eval(
			ctx, metricDirtyMarkScript,
			[]string{s.versionsKey(shard), s.dirtyKey(shard)}, args...,
		).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisAgentMetricDirtyStore) Claim(
	ctx context.Context,
	owner uuid.UUID,
	lease time.Duration,
	limit int,
) ([]AgentMetricDirtyClaim, error) {
	if s == nil || redisClientNil(s.client) {
		return nil, errMetricDirtyStoreUnavailable
	}
	if owner == uuid.Nil || lease < time.Second || lease > 5*time.Minute || limit < 1 || limit > maxMetricDirtyBatch {
		return nil, errors.New("agent metric dirty claim parameters are invalid")
	}
	claims := make([]AgentMetricDirtyClaim, 0, limit)
	startShard := int(owner[0]) % metricDirtyShardCount
	for offset := 0; offset < metricDirtyShardCount && len(claims) < limit; offset++ {
		shard := (startShard + offset) % metricDirtyShardCount
		remaining := limit - len(claims)
		values, err := s.client.Eval(
			ctx, metricDirtyClaimScript,
			[]string{
				s.versionsKey(shard), s.dirtyKey(shard),
				s.claimsKey(shard), s.claimExpiryKey(shard),
			},
			owner.String(), lease.Milliseconds(), remaining,
		).StringSlice()
		if err != nil {
			return nil, err
		}
		if len(values)%2 != 0 {
			return nil, errors.New("agent metric dirty claim response is invalid")
		}
		for index := 0; index < len(values); index += 2 {
			agentID, parseErr := uuid.Parse(values[index])
			version, versionErr := strconv.ParseInt(values[index+1], 10, 64)
			if parseErr != nil || versionErr != nil || version < 1 {
				return nil, errors.New("agent metric dirty claim identity is invalid")
			}
			claims = append(claims, AgentMetricDirtyClaim{AgentID: agentID, Version: version, Owner: owner})
		}
	}
	return claims, nil
}

func (s *RedisAgentMetricDirtyStore) Ack(ctx context.Context, claim AgentMetricDirtyClaim) (bool, error) {
	shard := metricDirtyShard(claim.AgentID)
	return s.finishClaim(
		ctx, metricDirtyAckScript,
		[]string{s.claimsKey(shard), s.claimExpiryKey(shard)}, claim,
	)
}

func (s *RedisAgentMetricDirtyStore) Nack(ctx context.Context, claim AgentMetricDirtyClaim) (bool, error) {
	shard := metricDirtyShard(claim.AgentID)
	return s.finishClaim(
		ctx, metricDirtyNackScript,
		[]string{s.claimsKey(shard), s.claimExpiryKey(shard), s.dirtyKey(shard)}, claim,
	)
}

func (s *RedisAgentMetricDirtyStore) finishClaim(
	ctx context.Context,
	script string,
	keys []string,
	claim AgentMetricDirtyClaim,
) (bool, error) {
	if s == nil || redisClientNil(s.client) {
		return false, errMetricDirtyStoreUnavailable
	}
	if claim.AgentID == uuid.Nil || claim.Owner == uuid.Nil || claim.Version < 1 {
		return false, errors.New("agent metric dirty claim is invalid")
	}
	result, err := s.client.Eval(
		ctx, script, keys, claim.AgentID.String(), claim.Owner.String(), claim.Version,
	).Int64()
	return result == 1, err
}

func (s *RedisAgentMetricDirtyStore) Cursor(ctx context.Context) (AgentMetricCursor, bool, error) {
	if s == nil || redisClientNil(s.client) {
		return AgentMetricCursor{}, false, errMetricDirtyStoreUnavailable
	}
	encoded, err := s.client.Get(ctx, s.cursorKey()).Result()
	if errors.Is(err, redis.Nil) {
		return AgentMetricCursor{}, false, nil
	}
	if err != nil {
		return AgentMetricCursor{}, false, err
	}
	cursor, err := parseAgentMetricCursor(encoded)
	return cursor, err == nil, err
}

func (s *RedisAgentMetricDirtyStore) AdvanceCursor(
	ctx context.Context,
	cursor AgentMetricCursor,
) (bool, error) {
	if s == nil || redisClientNil(s.client) {
		return false, errMetricDirtyStoreUnavailable
	}
	encoded, err := encodeAgentMetricCursor(cursor)
	if err != nil {
		return false, err
	}
	result, err := s.client.Eval(ctx, metricCursorAdvanceScript, []string{s.cursorKey()}, encoded).Int64()
	return result == 1, err
}

func encodeAgentMetricCursor(cursor AgentMetricCursor) (string, error) {
	if cursor.Time.IsZero() {
		return "", errors.New("agent metric cursor is invalid")
	}
	return cursor.Time.UTC().Format(metricCursorTimeFormat) + "|" + cursor.ID.String(), nil
}

func parseAgentMetricCursor(encoded string) (AgentMetricCursor, error) {
	parts := strings.Split(encoded, "|")
	if len(parts) != 2 {
		return AgentMetricCursor{}, errors.New("agent metric cursor encoding is invalid")
	}
	cursorTime, err := time.Parse(metricCursorTimeFormat, parts[0])
	if err != nil {
		return AgentMetricCursor{}, fmt.Errorf("parse agent metric cursor time: %w", err)
	}
	cursorID, err := uuid.Parse(parts[1])
	if err != nil {
		return AgentMetricCursor{}, errors.New("agent metric cursor ID is invalid")
	}
	return AgentMetricCursor{Time: cursorTime, ID: cursorID}, nil
}

func (s *RedisAgentMetricDirtyStore) versionsKey(shard int) string {
	return s.prefix + ":{" + metricDirtyShardTag(shard) + "}:versions"
}
func (s *RedisAgentMetricDirtyStore) dirtyKey(shard int) string {
	return s.prefix + ":{" + metricDirtyShardTag(shard) + "}:dirty"
}
func (s *RedisAgentMetricDirtyStore) claimsKey(shard int) string {
	return s.prefix + ":{" + metricDirtyShardTag(shard) + "}:claims"
}
func (s *RedisAgentMetricDirtyStore) claimExpiryKey(shard int) string {
	return s.prefix + ":{" + metricDirtyShardTag(shard) + "}:claim-expiry"
}
func (s *RedisAgentMetricDirtyStore) cursorKey() string { return s.prefix + ":{cursor}:run-events" }

func metricDirtyShard(agentID uuid.UUID) int { return int(agentID[0]) % metricDirtyShardCount }

func metricDirtyShardTag(shard int) string {
	if shard < 0 || shard >= metricDirtyShardCount {
		panic("agent metric dirty shard is out of range")
	}
	return fmt.Sprintf("%02x", shard)
}

func redisClientNil(client redis.UniversalClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	return (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) && value.IsNil()
}

var _ AgentMetricDirtyStore = (*RedisAgentMetricDirtyStore)(nil)
