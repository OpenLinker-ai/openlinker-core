package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	rand "math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	runtimeSessionLeaseVersion         = 1
	defaultRuntimeSessionLeasePrefix   = "openlinker:runtime:v3"
	defaultRuntimeLeaseRefresh         = 15 * time.Second
	defaultRuntimeSessionLeaseTTL      = 45 * time.Second
	defaultRuntimeSessionLeaseWarmup   = 60 * time.Second
	defaultRuntimeSessionLeaseBatch    = 512
	maxRuntimeSessionLeaseBatch        = 512
	runtimeSessionLeaseRefreshJitterPC = 10
)

const runtimeSessionLeaseRemoveScript = `
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
local ok, value = pcall(cjson.decode, raw)
if not ok then
  return 0
end
if value['core_instance_id'] ~= ARGV[1]
   or value['attachment_id'] ~= ARGV[2]
   or value['connection_id'] ~= ARGV[3] then
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[4])
return 1
`

const runtimeSessionLeaseRefreshScript = `
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
return 1
`

const runtimeSessionLeaseScheduleScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
return redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
`

const runtimeSessionLeaseForgetScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
return redis.call('ZREM', KEYS[2], ARGV[1])
`

// RuntimeSessionLease is an expiring, advisory liveness hint. It intentionally
// contains no token, certificate, Run, input, output, or authorization data.
// PostgreSQL Session/attachment state and fencing remain authoritative.
type RuntimeSessionLease struct {
	Version          int       `json:"version"`
	CoreInstanceID   uuid.UUID `json:"core_instance_id"`
	NodeID           uuid.UUID `json:"node_id"`
	AgentID          uuid.UUID `json:"agent_id"`
	RuntimeSessionID uuid.UUID `json:"runtime_session_id"`
	AttachmentID     uuid.UUID `json:"attachment_id"`
	ConnectionID     string    `json:"connection_id"`
	WorkerID         string    `json:"worker_id"`
	SessionEpoch     int64     `json:"session_epoch"`
	RefreshedAt      time.Time `json:"refreshed_at"`
}

type RuntimeSessionLeaseRecord struct {
	Lease    RuntimeSessionLease
	Presence RuntimePresence
}

type RuntimeSessionLeaseStore interface {
	RefreshBatch(context.Context, []RuntimeSessionLeaseRecord, time.Duration, time.Duration) error
	Lookup(context.Context, uuid.UUID) (RuntimeSessionLease, bool, error)
	Remove(context.Context, RuntimeSessionLeaseRecord) error
	ListExpired(context.Context, int) ([]uuid.UUID, error)
	ScheduleCheck(context.Context, uuid.UUID, time.Duration) error
	Forget(context.Context, uuid.UUID) error
}

type RuntimeSessionLeaseStoreProvider interface {
	RuntimeSessionLeaseStore() (RuntimeSessionLeaseStore, error)
}

type RedisRuntimeSessionLeaseStore struct {
	client         redis.UniversalClient
	leasePrefix    string
	presencePrefix string
}

func NewRedisRuntimeSessionLeaseStore(
	client redis.UniversalClient,
	leasePrefix string,
	presencePrefix string,
) (*RedisRuntimeSessionLeaseStore, error) {
	if runtimeRedisClientUnavailable(client) {
		return nil, fmt.Errorf("%w: Redis client is required", ErrRuntimeSignalBusUnavailable)
	}
	leasePrefix = strings.TrimSpace(leasePrefix)
	if leasePrefix == "" {
		leasePrefix = defaultRuntimeSessionLeasePrefix
	}
	presencePrefix = strings.TrimSpace(presencePrefix)
	if presencePrefix == "" {
		presencePrefix = defaultRuntimePresencePrefix
	}
	if !validRuntimeRedisPrefix(leasePrefix) || !validRuntimeRedisPrefix(presencePrefix) {
		return nil, errors.New("runtime Session lease Redis prefix is invalid")
	}
	return &RedisRuntimeSessionLeaseStore{
		client: client, leasePrefix: leasePrefix, presencePrefix: presencePrefix,
	}, nil
}

func (s *RedisRuntimeSessionLeaseStore) RefreshBatch(
	ctx context.Context,
	records []RuntimeSessionLeaseRecord,
	leaseTTL time.Duration,
	presenceTTL time.Duration,
) error {
	if s == nil || runtimeRedisClientUnavailable(s.client) {
		return ErrRuntimeSignalBusUnavailable
	}
	if len(records) > maxRuntimeSessionLeaseBatch {
		return fmt.Errorf("runtime Session lease batch exceeds %d entries", maxRuntimeSessionLeaseBatch)
	}
	if leaseTTL < minRuntimePresenceTTL || leaseTTL > maxRuntimePresenceTTL ||
		presenceTTL < minRuntimePresenceTTL || presenceTTL > maxRuntimePresenceTTL {
		return errors.New("runtime Session lease TTL is invalid")
	}
	seenSessions := make(map[uuid.UUID]struct{}, len(records))
	for _, record := range records {
		if err := validateRuntimeSessionLease(record.Lease); err != nil {
			return err
		}
		if err := validateRuntimePresence(record.Presence); err != nil {
			return err
		}
		if !runtimeLeaseMatchesPresence(record.Lease, record.Presence) {
			return errors.New("runtime Session lease does not match presence")
		}
		if _, duplicate := seenSessions[record.Lease.RuntimeSessionID]; duplicate {
			return errors.New("runtime Session lease batch contains a duplicate Session")
		}
		seenSessions[record.Lease.RuntimeSessionID] = struct{}{}
	}

	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("read Redis time for runtime Session lease: %w", err)
	}
	if len(records) == 0 {
		return nil
	}
	type encodedRecord struct {
		lease    []byte
		presence []byte
	}
	encoded := make([]encodedRecord, len(records))
	for index, record := range records {
		record.Lease.RefreshedAt = now.UTC()
		encoded[index].lease, err = json.Marshal(record.Lease)
		if err != nil {
			return fmt.Errorf("encode runtime Session lease: %w", err)
		}
		encoded[index].presence, err = json.Marshal(record.Presence)
		if err != nil {
			return fmt.Errorf("encode runtime presence: %w", err)
		}
	}

	_, err = s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, record := range records {
			pipe.Eval(
				ctx,
				runtimeSessionLeaseRefreshScript,
				[]string{s.leaseKey(record.Lease.RuntimeSessionID), s.leaseIndexKey()},
				encoded[index].lease,
				leaseTTL.Milliseconds(),
				now.Add(leaseTTL).UnixMilli(),
				record.Lease.RuntimeSessionID.String(),
			)
			presenceKey := s.presenceKey(record.Presence)
			indexKey := s.presenceIndexKey(record.Presence.AgentID)
			pipe.Set(ctx, presenceKey, encoded[index].presence, presenceTTL)
			pipe.ZAdd(ctx, indexKey, redis.Z{
				Score: float64(now.Add(presenceTTL).UnixMilli()), Member: presenceKey,
			})
			pipe.Expire(ctx, indexKey, runtimePresenceIndexTTL)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("refresh runtime Session lease batch: %w", err)
	}
	return nil
}

func (s *RedisRuntimeSessionLeaseStore) Lookup(
	ctx context.Context,
	runtimeSessionID uuid.UUID,
) (RuntimeSessionLease, bool, error) {
	if s == nil || runtimeRedisClientUnavailable(s.client) {
		return RuntimeSessionLease{}, false, ErrRuntimeSignalBusUnavailable
	}
	if runtimeSessionID == uuid.Nil {
		return RuntimeSessionLease{}, false, errors.New("runtime Session lease Session ID is required")
	}
	encoded, err := s.client.Get(ctx, s.leaseKey(runtimeSessionID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return RuntimeSessionLease{}, false, nil
	}
	if err != nil {
		return RuntimeSessionLease{}, false, fmt.Errorf("lookup runtime Session lease: %w", err)
	}
	lease, err := parseRuntimeSessionLease(encoded)
	if err != nil {
		return RuntimeSessionLease{}, false, err
	}
	if lease.RuntimeSessionID != runtimeSessionID {
		return RuntimeSessionLease{}, false, errors.New("runtime Session lease key identity mismatch")
	}
	return lease, true, nil
}

func (s *RedisRuntimeSessionLeaseStore) Remove(
	ctx context.Context,
	record RuntimeSessionLeaseRecord,
) error {
	if s == nil || runtimeRedisClientUnavailable(s.client) {
		return ErrRuntimeSignalBusUnavailable
	}
	if err := validateRuntimeSessionLease(record.Lease); err != nil {
		return err
	}
	if err := validateRuntimePresence(record.Presence); err != nil {
		return err
	}
	if !runtimeLeaseMatchesPresence(record.Lease, record.Presence) {
		return errors.New("runtime Session lease does not match presence")
	}
	leaseKey := s.leaseKey(record.Lease.RuntimeSessionID)
	presenceKey := s.presenceKey(record.Presence)
	_, err := s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Eval(
			ctx,
			runtimeSessionLeaseRemoveScript,
			[]string{leaseKey, s.leaseIndexKey()},
			record.Lease.CoreInstanceID.String(),
			record.Lease.AttachmentID.String(),
			record.Lease.ConnectionID,
			record.Lease.RuntimeSessionID.String(),
		)
		pipe.Del(ctx, presenceKey)
		pipe.ZRem(ctx, s.presenceIndexKey(record.Presence.AgentID), presenceKey)
		return nil
	})
	if err != nil {
		return fmt.Errorf("remove runtime Session lease: %w", err)
	}
	return nil
}

func (s *RedisRuntimeSessionLeaseStore) ListExpired(
	ctx context.Context,
	limit int,
) ([]uuid.UUID, error) {
	if s == nil || runtimeRedisClientUnavailable(s.client) {
		return nil, ErrRuntimeSignalBusUnavailable
	}
	if limit <= 0 || limit > maxRuntimeSessionLeaseBatch {
		return nil, errors.New("runtime Session lease expiry limit is invalid")
	}
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("read Redis time for expired runtime Session leases: %w", err)
	}
	members, err := s.client.ZRangeByScore(ctx, s.leaseIndexKey(), &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", now.UnixMilli()), Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list expired runtime Session leases: %w", err)
	}
	ids := make([]uuid.UUID, 0, len(members))
	for _, member := range members {
		id, parseErr := uuid.Parse(member)
		if parseErr != nil || id == uuid.Nil {
			return nil, errors.New("runtime Session lease expiry index contains an invalid Session ID")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *RedisRuntimeSessionLeaseStore) ScheduleCheck(
	ctx context.Context,
	runtimeSessionID uuid.UUID,
	after time.Duration,
) error {
	if s == nil || runtimeRedisClientUnavailable(s.client) {
		return ErrRuntimeSignalBusUnavailable
	}
	if runtimeSessionID == uuid.Nil || after <= 0 || after > maxRuntimePresenceTTL {
		return errors.New("runtime Session lease absence check is invalid")
	}
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("read Redis time for runtime Session lease absence check: %w", err)
	}
	if err := s.client.Eval(
		ctx,
		runtimeSessionLeaseScheduleScript,
		[]string{s.leaseKey(runtimeSessionID), s.leaseIndexKey()},
		now.Add(after).UnixMilli(),
		runtimeSessionID.String(),
	).Err(); err != nil {
		return fmt.Errorf("schedule runtime Session lease absence check: %w", err)
	}
	return nil
}

func (s *RedisRuntimeSessionLeaseStore) Forget(
	ctx context.Context,
	runtimeSessionID uuid.UUID,
) error {
	if s == nil || runtimeRedisClientUnavailable(s.client) {
		return ErrRuntimeSignalBusUnavailable
	}
	if runtimeSessionID == uuid.Nil {
		return errors.New("runtime Session lease Session ID is required")
	}
	if err := s.client.Eval(
		ctx,
		runtimeSessionLeaseForgetScript,
		[]string{s.leaseKey(runtimeSessionID), s.leaseIndexKey()},
		runtimeSessionID.String(),
	).Err(); err != nil {
		return fmt.Errorf("forget runtime Session lease absence check: %w", err)
	}
	return nil
}

func (s *RedisRuntimeSessionLeaseStore) leaseKey(runtimeSessionID uuid.UUID) string {
	return fmt.Sprintf("%s:{sessions}:lease:%s", s.leasePrefix, runtimeSessionID)
}

func (s *RedisRuntimeSessionLeaseStore) leaseIndexKey() string {
	return fmt.Sprintf("%s:{sessions}:expiry", s.leasePrefix)
}

func (s *RedisRuntimeSessionLeaseStore) presenceKey(presence RuntimePresence) string {
	store := RedisRuntimePresenceStore{prefix: s.presencePrefix}
	return store.presenceKey(presence)
}

func (s *RedisRuntimeSessionLeaseStore) presenceIndexKey(agentID uuid.UUID) string {
	store := RedisRuntimePresenceStore{prefix: s.presencePrefix}
	return store.agentIndexKey(agentID)
}

func validRuntimeRedisPrefix(prefix string) bool {
	return prefix != "" && len(prefix) <= 120 && !strings.ContainsAny(prefix, "{}\x00\r\n")
}

func validateRuntimeSessionLease(lease RuntimeSessionLease) error {
	if lease.Version != runtimeSessionLeaseVersion || lease.CoreInstanceID == uuid.Nil ||
		lease.NodeID == uuid.Nil || lease.AgentID == uuid.Nil || lease.RuntimeSessionID == uuid.Nil ||
		lease.AttachmentID == uuid.Nil || lease.SessionEpoch < 1 {
		return errors.New("runtime Session lease identity is invalid")
	}
	if !validPresenceString(lease.ConnectionID, 200) || !validPresenceString(lease.WorkerID, 200) {
		return errors.New("runtime Session lease strings are invalid")
	}
	return nil
}

func parseRuntimeSessionLease(encoded []byte) (RuntimeSessionLease, error) {
	if len(encoded) == 0 || len(encoded) > 2048 {
		return RuntimeSessionLease{}, errors.New("runtime Session lease payload size is invalid")
	}
	var lease RuntimeSessionLease
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lease); err != nil {
		return RuntimeSessionLease{}, errors.New("runtime Session lease JSON is invalid")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return RuntimeSessionLease{}, errors.New("runtime Session lease must contain one JSON value")
	}
	if err := validateRuntimeSessionLease(lease); err != nil {
		return RuntimeSessionLease{}, err
	}
	if lease.RefreshedAt.IsZero() {
		return RuntimeSessionLease{}, errors.New("runtime Session lease refreshed_at is required")
	}
	return lease, nil
}

func runtimeLeaseMatchesPresence(lease RuntimeSessionLease, presence RuntimePresence) bool {
	return lease.CoreInstanceID == presence.CoreInstanceID &&
		lease.NodeID == presence.NodeID &&
		lease.AgentID == presence.AgentID &&
		lease.RuntimeSessionID == presence.RuntimeSessionID &&
		lease.ConnectionID == presence.ConnectionID &&
		lease.WorkerID == presence.WorkerID
}

type RuntimeSessionLeaseManagerConfig struct {
	RefreshInterval time.Duration
	LeaseTTL        time.Duration
	PresenceTTL     time.Duration
	Warmup          time.Duration
	BatchSize       int
	DisableJitter   bool
}

type RuntimeSessionLeaseManagerHealth struct {
	Connected       bool
	AbsenceReady    bool
	Reason          string
	Generation      uint64
	Registered      int
	LastSuccessAt   time.Time
	HealthySince    time.Time
	RefreshFailures uint64
}

type runtimeManagedSessionLease struct {
	record          RuntimeSessionLeaseRecord
	registeredAt    time.Time
	lastRefreshedAt time.Time
}

// RuntimeSessionLeaseManager owns one process-level refresh loop. It replaces
// per-WebSocket Redis and database heartbeat loops without becoming a source
// of truth. Missing Redis keys are ignored until a full warmup has elapsed.
type RuntimeSessionLeaseManager struct {
	store  RuntimeSessionLeaseStore
	config RuntimeSessionLeaseManagerConfig

	mu       sync.Mutex
	records  map[string]runtimeManagedSessionLease
	sessions map[uuid.UUID]string
	health   RuntimeSessionLeaseManagerHealth
	wake     chan struct{}
}

func NewRuntimeSessionLeaseManager(
	store RuntimeSessionLeaseStore,
	config RuntimeSessionLeaseManagerConfig,
) (*RuntimeSessionLeaseManager, error) {
	if store == nil {
		return nil, errors.New("runtime Session lease store is required")
	}
	config = normalizeRuntimeSessionLeaseManagerConfig(config)
	return &RuntimeSessionLeaseManager{
		store: store, config: config,
		records:  make(map[string]runtimeManagedSessionLease),
		sessions: make(map[uuid.UUID]string),
		wake:     make(chan struct{}, 1),
		health:   RuntimeSessionLeaseManagerHealth{Reason: "starting"},
	}, nil
}

func normalizeRuntimeSessionLeaseManagerConfig(
	config RuntimeSessionLeaseManagerConfig,
) RuntimeSessionLeaseManagerConfig {
	if config.RefreshInterval <= 0 {
		config.RefreshInterval = defaultRuntimeLeaseRefresh
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = defaultRuntimeSessionLeaseTTL
	}
	if config.PresenceTTL <= 0 {
		config.PresenceTTL = RuntimePresenceTTL
	}
	if config.Warmup <= 0 {
		config.Warmup = defaultRuntimeSessionLeaseWarmup
	}
	if config.BatchSize <= 0 || config.BatchSize > maxRuntimeSessionLeaseBatch {
		config.BatchSize = defaultRuntimeSessionLeaseBatch
	}
	return config
}

func (m *RuntimeSessionLeaseManager) Register(record RuntimeSessionLeaseRecord) error {
	if m == nil {
		return errors.New("runtime Session lease manager is not configured")
	}
	if err := validateRuntimeSessionLease(record.Lease); err != nil {
		return err
	}
	if err := validateRuntimePresence(record.Presence); err != nil {
		return err
	}
	if !runtimeLeaseMatchesPresence(record.Lease, record.Presence) {
		return errors.New("runtime Session lease does not match presence")
	}
	m.mu.Lock()
	if oldConnection := m.sessions[record.Lease.RuntimeSessionID]; oldConnection != "" && oldConnection != record.Lease.ConnectionID {
		delete(m.records, oldConnection)
	}
	m.records[record.Lease.ConnectionID] = runtimeManagedSessionLease{
		record: record, registeredAt: time.Now(),
	}
	m.sessions[record.Lease.RuntimeSessionID] = record.Lease.ConnectionID
	m.health.Registered = len(m.records)
	m.mu.Unlock()
	m.signalRefresh()
	return nil
}

// RefreshConnection establishes the first lease before Ready is emitted. The
// process loop owns all later refreshes in batches; this one synchronous write
// prevents a newly attached Session from existing only in process memory.
func (m *RuntimeSessionLeaseManager) RefreshConnection(ctx context.Context, connectionID string) error {
	if m == nil {
		return errors.New("runtime Session lease manager is not configured")
	}
	m.mu.Lock()
	managed, ok := m.records[connectionID]
	m.mu.Unlock()
	if !ok {
		return errors.New("runtime Session lease connection is not registered")
	}
	if err := m.store.RefreshBatch(
		ctx,
		[]RuntimeSessionLeaseRecord{managed.record},
		m.config.LeaseTTL,
		m.config.PresenceTTL,
	); err != nil {
		m.markFailure()
		return err
	}
	m.markSuccess([]RuntimeSessionLeaseRecord{managed.record})
	return nil
}

func (m *RuntimeSessionLeaseManager) UpdatePresence(connectionID string, presence RuntimePresence) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	managed, ok := m.records[connectionID]
	updated := ok && runtimeLeaseMatchesPresence(managed.record.Lease, presence)
	if updated {
		managed.record.Presence = presence
		m.records[connectionID] = managed
	}
	m.mu.Unlock()
	if updated {
		m.signalRefresh()
	}
	return updated
}

func (m *RuntimeSessionLeaseManager) Unregister(
	ctx context.Context,
	connectionID string,
	attachmentID uuid.UUID,
) (bool, error) {
	if m == nil {
		return false, nil
	}
	m.mu.Lock()
	managed, ok := m.records[connectionID]
	if ok && managed.record.Lease.AttachmentID == attachmentID {
		delete(m.records, connectionID)
		if m.sessions[managed.record.Lease.RuntimeSessionID] == connectionID {
			delete(m.sessions, managed.record.Lease.RuntimeSessionID)
		}
		m.health.Registered = len(m.records)
	} else {
		ok = false
	}
	m.mu.Unlock()
	if !ok {
		return false, nil
	}
	return true, m.store.Remove(ctx, managed.record)
}

func (m *RuntimeSessionLeaseManager) HealthyFor(connectionID string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	managed, ok := m.records[connectionID]
	now := time.Now()
	return ok && m.health.Connected && !managed.lastRefreshedAt.IsZero() &&
		!managed.lastRefreshedAt.Before(m.health.HealthySince) &&
		now.Sub(managed.lastRefreshedAt) < m.databaseFallbackAge()
}

func (m *RuntimeSessionLeaseManager) Lookup(
	ctx context.Context,
	runtimeSessionID uuid.UUID,
) (RuntimeSessionLease, bool, error) {
	if m == nil {
		return RuntimeSessionLease{}, false, errors.New("runtime Session lease manager is not configured")
	}
	return m.store.Lookup(ctx, runtimeSessionID)
}

func (m *RuntimeSessionLeaseManager) ListExpired(
	ctx context.Context,
	limit int,
) ([]uuid.UUID, error) {
	if m == nil {
		return nil, errors.New("runtime Session lease manager is not configured")
	}
	return m.store.ListExpired(ctx, limit)
}

func (m *RuntimeSessionLeaseManager) ScheduleCheck(
	ctx context.Context,
	runtimeSessionID uuid.UUID,
	after time.Duration,
) error {
	if m == nil {
		return errors.New("runtime Session lease manager is not configured")
	}
	return m.store.ScheduleCheck(ctx, runtimeSessionID, after)
}

func (m *RuntimeSessionLeaseManager) Forget(
	ctx context.Context,
	runtimeSessionID uuid.UUID,
) error {
	if m == nil {
		return errors.New("runtime Session lease manager is not configured")
	}
	return m.store.Forget(ctx, runtimeSessionID)
}

func (m *RuntimeSessionLeaseManager) AbsenceReady() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.absenceReadyLocked(time.Now())
}

func (m *RuntimeSessionLeaseManager) Health() RuntimeSessionLeaseManagerHealth {
	if m == nil {
		return RuntimeSessionLeaseManagerHealth{Reason: "not_configured"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	health := m.health
	health.AbsenceReady = m.absenceReadyLocked(time.Now())
	health.Registered = len(m.records)
	return health
}

func (m *RuntimeSessionLeaseManager) Run(ctx context.Context) error {
	if m == nil || m.store == nil {
		return errors.New("runtime Session lease manager is not configured")
	}
	for {
		m.refresh(ctx)
		if ctx.Err() != nil {
			m.markStopped()
			return nil
		}
		timer := time.NewTimer(m.nextRefreshDelay())
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			m.markStopped()
			return nil
		case <-m.wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func (m *RuntimeSessionLeaseManager) refresh(ctx context.Context) {
	records := m.snapshotRecords()
	if len(records) == 0 {
		if err := m.store.RefreshBatch(ctx, nil, m.config.LeaseTTL, m.config.PresenceTTL); err != nil {
			m.markFailure()
			return
		}
		m.markSuccess(nil)
		return
	}
	refreshed := make([]RuntimeSessionLeaseRecord, 0, len(records))
	for start := 0; start < len(records); start += m.config.BatchSize {
		end := min(start+m.config.BatchSize, len(records))
		batch := records[start:end]
		if err := m.store.RefreshBatch(ctx, batch, m.config.LeaseTTL, m.config.PresenceTTL); err != nil {
			m.markFailure()
			return
		}
		refreshed = append(refreshed, batch...)
	}
	m.markSuccess(refreshed)
}

func (m *RuntimeSessionLeaseManager) snapshotRecords() []RuntimeSessionLeaseRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	records := make([]RuntimeSessionLeaseRecord, 0, len(m.records))
	now := time.Now()
	for _, managed := range m.records {
		freshness := managed.lastRefreshedAt
		if freshness.IsZero() {
			freshness = managed.registeredAt
		}
		if freshness.IsZero() || now.Sub(freshness) >= m.config.LeaseTTL {
			continue
		}
		records = append(records, managed.record)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Lease.ConnectionID < records[j].Lease.ConnectionID
	})
	return records
}

func (m *RuntimeSessionLeaseManager) markSuccess(refreshed []RuntimeSessionLeaseRecord) {
	now := time.Now()
	m.mu.Lock()
	if !m.health.Connected {
		m.health.Generation++
		m.health.HealthySince = now
	}
	m.health.Connected = true
	m.health.Reason = "connected"
	m.health.LastSuccessAt = now
	for _, record := range refreshed {
		managed, ok := m.records[record.Lease.ConnectionID]
		if ok && managed.record.Lease.AttachmentID == record.Lease.AttachmentID {
			managed.lastRefreshedAt = now
			m.records[record.Lease.ConnectionID] = managed
		}
	}
	m.mu.Unlock()
}

func (m *RuntimeSessionLeaseManager) markFailure() {
	m.mu.Lock()
	m.health.Connected = false
	m.health.Reason = "refresh_failed"
	m.health.HealthySince = time.Time{}
	m.health.RefreshFailures++
	m.mu.Unlock()
}

func (m *RuntimeSessionLeaseManager) markStopped() {
	m.mu.Lock()
	m.health.Connected = false
	m.health.Reason = "stopped"
	m.health.HealthySince = time.Time{}
	m.mu.Unlock()
}

func (m *RuntimeSessionLeaseManager) absenceReadyLocked(now time.Time) bool {
	return m.health.Connected && !m.health.HealthySince.IsZero() &&
		!m.health.LastSuccessAt.IsZero() && now.Sub(m.health.LastSuccessAt) < m.config.LeaseTTL &&
		now.Sub(m.health.HealthySince) >= m.config.Warmup
}

func (m *RuntimeSessionLeaseManager) databaseFallbackAge() time.Duration {
	safety := 5 * time.Second
	if proportional := m.config.LeaseTTL / 10; proportional < safety {
		safety = proportional
	}
	age := m.config.LeaseTTL - RuntimeHeartbeatInterval - safety
	if age <= 0 {
		return m.config.LeaseTTL / 2
	}
	return age
}

func (m *RuntimeSessionLeaseManager) nextRefreshDelay() time.Duration {
	if m.config.DisableJitter {
		return m.config.RefreshInterval
	}
	window := m.config.RefreshInterval / runtimeSessionLeaseRefreshJitterPC
	if window <= 0 {
		return m.config.RefreshInterval
	}
	return m.config.RefreshInterval + time.Duration(rand.Int64N(int64(window)+1))
}

func (m *RuntimeSessionLeaseManager) signalRefresh() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func runtimeSessionLeaseRecordFromState(
	state RuntimeSessionState,
	connectionID string,
) (RuntimeSessionLeaseRecord, error) {
	if state.Attachment == nil || state.Attachment.TransportReason == nil {
		return RuntimeSessionLeaseRecord{}, errors.New("runtime Session lease requires an active attachment")
	}
	session := state.Session
	if session.AttachedCoreInstanceID == nil {
		return RuntimeSessionLeaseRecord{}, errors.New("runtime Session lease requires an attached Core")
	}
	presence := RuntimePresence{
		CoreInstanceID: *session.AttachedCoreInstanceID,
		NodeID:         session.NodeID, AgentID: session.AgentID,
		RuntimeSessionID: session.RuntimeSessionID,
		ConnectionID:     connectionID, WorkerID: session.WorkerID,
		Capacity: session.Capacity, Inflight: session.Inflight,
		NodeVersion:        session.NodeVersion,
		Transport:          RuntimeTransport(state.Attachment.Transport),
		TransportReason:    RuntimeTransportReason(*state.Attachment.TransportReason),
		TransportChangedAt: state.Attachment.TransportChangedAt,
	}
	lease := RuntimeSessionLease{
		Version:        runtimeSessionLeaseVersion,
		CoreInstanceID: *session.AttachedCoreInstanceID,
		NodeID:         session.NodeID, AgentID: session.AgentID,
		RuntimeSessionID: session.RuntimeSessionID,
		AttachmentID:     state.Attachment.ID,
		ConnectionID:     connectionID, WorkerID: session.WorkerID,
		SessionEpoch: session.SessionEpoch,
	}
	record := RuntimeSessionLeaseRecord{Lease: lease, Presence: presence}
	if err := validateRuntimeSessionLease(record.Lease); err != nil {
		return RuntimeSessionLeaseRecord{}, err
	}
	if err := validateRuntimePresence(record.Presence); err != nil {
		return RuntimeSessionLeaseRecord{}, err
	}
	return record, nil
}

var _ RuntimeSessionLeaseStore = (*RedisRuntimeSessionLeaseStore)(nil)
