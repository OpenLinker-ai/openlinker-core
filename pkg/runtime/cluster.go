package runtime

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	// RuntimeSchemaVersion is the schema contract compiled into this Core
	// release. A migration that changes the current runtime schema contract must
	// update both this version and RuntimeSchemaChecksum.
	RuntimeSchemaVersion       int32 = 69
	RuntimeSchemaMigrationName       = "069_runtime_entry_discovery"
	// RuntimeSchemaChecksum is SHA-256 over the canonical current schema
	// contract tuple:
	// 69:069_runtime_entry_discovery:<contract id>:<contract digest>.
	RuntimeSchemaChecksum = "793bd58888a749e4e1c73e5ebe74a6a476ad2e6dd35d38d76a426e6f15c79978"

	RuntimeClusterModeNormal          RuntimeClusterMode = "normal"
	RuntimeClusterModeDraining        RuntimeClusterMode = "draining"
	RuntimeClusterModeHardMaintenance RuntimeClusterMode = "hard_maintenance"

	RuntimeClusterNewRun     RuntimeClusterOperation = "new_run"
	RuntimeClusterNewSession RuntimeClusterOperation = "new_session"
	RuntimeClusterClaim      RuntimeClusterOperation = "claim"

	runtimeClusterHeartbeatInterval = 5 * time.Second
	runtimeClusterHeartbeatTimeout  = 4 * time.Second
	runtimeClusterMemberLiveWindow  = 15 * time.Second
	runtimeClusterDependencyTimeout = 2 * time.Second
)

type RuntimeClusterMode string

type RuntimeClusterOperation string

// RuntimeClusterIdentity is immutable process evidence stored on every
// heartbeat. Release fields deliberately come from deployment metadata rather
// than a mutable database setting.
type RuntimeClusterIdentity struct {
	InstanceID            uuid.UUID `json:"instance_id"`
	ReleaseVersion        string    `json:"release_version"`
	ReleaseCommit         string    `json:"release_commit"`
	SchemaVersion         int32     `json:"schema_version"`
	SchemaChecksum        string    `json:"schema_checksum"`
	RuntimeContractID     string    `json:"runtime_contract_id"`
	RuntimeContractDigest string    `json:"runtime_contract_digest"`
}

type RuntimeClusterControlSnapshot struct {
	Mode             RuntimeClusterMode `json:"mode"`
	ExpectedReplicas int32              `json:"expected_replicas"`
}

type RuntimeSchemaContractSnapshot struct {
	SchemaVersion         int32  `json:"schema_version"`
	MigrationName         string `json:"migration_name"`
	RuntimeContractID     string `json:"runtime_contract_id"`
	RuntimeContractDigest string `json:"runtime_contract_digest"`
}

type RuntimeClusterMemberSnapshot struct {
	RuntimeClusterIdentity
	HeartbeatAt time.Time `json:"heartbeat_at"`
	Draining    bool      `json:"draining"`
	Ready       bool      `json:"ready"`
}

type RuntimeClusterSnapshot struct {
	DatabaseTime  time.Time                      `json:"database_time"`
	Control       RuntimeClusterControlSnapshot  `json:"control"`
	CurrentSchema RuntimeSchemaContractSnapshot  `json:"current_schema"`
	LiveMembers   []RuntimeClusterMemberSnapshot `json:"live_members"`
}

// RuntimeClusterReadiness is safe for an unauthenticated health endpoint: it
// contains version/contract evidence and stable reason codes, never secrets,
// Run payloads, credentials, or infrastructure error strings.
type RuntimeClusterReadiness struct {
	Ready             bool               `json:"ready"`
	Status            string             `json:"status"`
	Reasons           []string           `json:"reasons,omitempty"`
	Mode              RuntimeClusterMode `json:"mode,omitempty"`
	ExpectedReplicas  int32              `json:"expected_replicas,omitempty"`
	LiveReplicas      int                `json:"live_replicas"`
	InstanceID        uuid.UUID          `json:"instance_id"`
	ReleaseVersion    string             `json:"release_version"`
	ReleaseCommit     string             `json:"release_commit"`
	SchemaVersion     int32              `json:"schema_version"`
	SchemaChecksum    string             `json:"schema_checksum"`
	RuntimeContractID string             `json:"runtime_contract_id"`
	DatabaseTime      *time.Time         `json:"database_time,omitempty"`
}

type RuntimeClusterRepository interface {
	UpsertMember(context.Context, RuntimeClusterIdentity, bool, bool) error
	Snapshot(context.Context, time.Duration) (RuntimeClusterSnapshot, error)
	CloseMember(context.Context, uuid.UUID) error
}

type RuntimeClusterCoordinator struct {
	repository     RuntimeClusterRepository
	signalBus      RuntimeSignalBus
	identity       RuntimeClusterIdentity
	requireSignal  bool
	heartbeatEvery time.Duration
	liveWindow     time.Duration

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

// NewRuntimeClusterCoordinator creates a coordinator with the protocol's
// fixed five-second heartbeat and fifteen-second live-member window.
func NewRuntimeClusterCoordinator(
	pool *pgxpool.Pool,
	signalBus RuntimeSignalBus,
	identity RuntimeClusterIdentity,
	requireSignal bool,
) (*RuntimeClusterCoordinator, error) {
	if pool == nil {
		return nil, errors.New("runtime cluster database is required")
	}
	return newRuntimeClusterCoordinator(
		&postgresRuntimeClusterRepository{pool: pool},
		signalBus,
		identity,
		requireSignal,
		runtimeClusterHeartbeatInterval,
		runtimeClusterMemberLiveWindow,
	)
}

func newRuntimeClusterCoordinator(
	repository RuntimeClusterRepository,
	signalBus RuntimeSignalBus,
	identity RuntimeClusterIdentity,
	requireSignal bool,
	heartbeatEvery time.Duration,
	liveWindow time.Duration,
) (*RuntimeClusterCoordinator, error) {
	identity.ReleaseVersion = strings.TrimSpace(identity.ReleaseVersion)
	identity.ReleaseCommit = strings.TrimSpace(identity.ReleaseCommit)
	identity.SchemaChecksum = strings.TrimSpace(identity.SchemaChecksum)
	identity.RuntimeContractID = strings.TrimSpace(identity.RuntimeContractID)
	identity.RuntimeContractDigest = strings.TrimSpace(identity.RuntimeContractDigest)
	if repository == nil || identity.InstanceID == uuid.Nil ||
		len(identity.ReleaseVersion) < 1 || len(identity.ReleaseVersion) > 100 ||
		len(identity.ReleaseCommit) < 1 || len(identity.ReleaseCommit) > 100 ||
		identity.SchemaVersion < 1 || !validSHA256Hex(identity.SchemaChecksum) ||
		identity.RuntimeContractID == "" || len(identity.RuntimeContractID) > 200 ||
		!validSHA256Hex(identity.RuntimeContractDigest) || heartbeatEvery <= 0 ||
		liveWindow < heartbeatEvery || (requireSignal && signalBus == nil) {
		return nil, errors.New("runtime cluster configuration is invalid")
	}
	return &RuntimeClusterCoordinator{
		repository: repository, signalBus: signalBus, identity: identity,
		requireSignal: requireSignal, heartbeatEvery: heartbeatEvery,
		liveWindow: liveWindow,
	}, nil
}

// Start registers immediately, then refreshes membership every five seconds.
// A transient dependency or database failure is reported through readiness
// and retried; it never stops the database reconciliation workers.
func (c *RuntimeClusterCoordinator) Start(parent context.Context) {
	if c == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	c.done = make(chan struct{})
	c.started = true
	done := c.done
	c.mu.Unlock()

	c.heartbeat(ctx)
	go func() {
		defer close(done)
		ticker := time.NewTicker(c.heartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.heartbeat(ctx)
			}
		}
	}()
}

func (c *RuntimeClusterCoordinator) heartbeat(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, runtimeClusterHeartbeatTimeout)
	defer cancel()
	localReady := c.signalHealthy(ctx)
	snapshot, err := c.repository.Snapshot(ctx, c.liveWindow)
	if err != nil {
		localReady = false
	} else if snapshot.Control.Mode != RuntimeClusterModeNormal ||
		!runtimeSchemaMatchesIdentity(snapshot.CurrentSchema, c.identity) ||
		(snapshot.Control.ExpectedReplicas > 1 && !c.requireSignal) {
		localReady = false
	}
	if err = c.repository.UpsertMember(
		ctx,
		c.identity,
		snapshot.Control.Mode != RuntimeClusterModeNormal,
		localReady,
	); err != nil && !errors.Is(err, context.Canceled) {
		log.Error().Err(err).Str("instance_id", c.identity.InstanceID.String()).
			Msg("runtime cluster heartbeat failed")
	}
}

func (c *RuntimeClusterCoordinator) signalHealthy(parent context.Context) bool {
	if c == nil || !c.requireSignal {
		return true
	}
	if c.signalBus == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(parent, runtimeClusterDependencyTimeout)
	defer cancel()
	return c.signalBus.Health(ctx) == nil
}

func (c *RuntimeClusterCoordinator) Readiness(ctx context.Context) RuntimeClusterReadiness {
	if c == nil {
		return RuntimeClusterReadiness{
			Status:  "not_ready",
			Reasons: []string{"cluster_unavailable"},
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	readiness := RuntimeClusterReadiness{
		Status:            "not_ready",
		InstanceID:        c.identity.InstanceID,
		ReleaseVersion:    c.identity.ReleaseVersion,
		ReleaseCommit:     c.identity.ReleaseCommit,
		SchemaVersion:     c.identity.SchemaVersion,
		SchemaChecksum:    c.identity.SchemaChecksum,
		RuntimeContractID: c.identity.RuntimeContractID,
	}
	if c.repository == nil {
		readiness.Reasons = []string{"cluster_unavailable"}
		return readiness
	}
	if !c.signalHealthy(ctx) {
		readiness.Reasons = append(readiness.Reasons, "signal_dependency_unavailable")
	}
	snapshot, err := c.repository.Snapshot(ctx, c.liveWindow)
	if err != nil {
		readiness.Reasons = append(readiness.Reasons, "cluster_unavailable")
		return readiness
	}
	readiness.Mode = snapshot.Control.Mode
	readiness.ExpectedReplicas = snapshot.Control.ExpectedReplicas
	readiness.LiveReplicas = len(snapshot.LiveMembers)
	readiness.DatabaseTime = &snapshot.DatabaseTime
	// More than one expected replica is HA by database fact. A deployment
	// cannot make a local in-process bus look distributed by omitting the HA
	// environment flag.
	if snapshot.Control.ExpectedReplicas > 1 && !c.requireSignal {
		readiness.Reasons = append(readiness.Reasons, "signal_dependency_unavailable")
	}
	if snapshot.Control.Mode != RuntimeClusterModeNormal {
		readiness.Reasons = append(readiness.Reasons, "maintenance")
	}
	if !runtimeSchemaMatchesIdentity(snapshot.CurrentSchema, c.identity) {
		readiness.Reasons = append(readiness.Reasons, "schema_contract_mismatch")
	}
	if int32(len(snapshot.LiveMembers)) < snapshot.Control.ExpectedReplicas {
		readiness.Reasons = append(readiness.Reasons, "replicas_unavailable")
	}
	currentFound := false
	memberMismatch := false
	memberNotReady := false
	for _, member := range snapshot.LiveMembers {
		if member.InstanceID == c.identity.InstanceID {
			currentFound = true
		}
		if !runtimeClusterIdentityEqual(member.RuntimeClusterIdentity, c.identity) {
			memberMismatch = true
		}
		if !member.Ready {
			memberNotReady = true
		}
	}
	if !currentFound {
		readiness.Reasons = append(readiness.Reasons, "instance_not_registered")
	}
	if memberMismatch {
		readiness.Reasons = append(readiness.Reasons, "member_contract_mismatch")
	}
	if memberNotReady {
		readiness.Reasons = append(readiness.Reasons, "member_not_ready")
	}
	readiness.Reasons = uniqueRuntimeClusterReasons(readiness.Reasons)
	readiness.Ready = len(readiness.Reasons) == 0
	if readiness.Ready {
		readiness.Status = "ready"
	}
	return readiness
}

// Close stops future heartbeats before deleting this process's membership.
// Stale rows left by SIGKILL naturally stop counting after the live window.
func (c *RuntimeClusterCoordinator) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	cancel, done, started := c.cancel, c.done, c.started
	c.started = false
	c.cancel = nil
	c.done = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if started && done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.repository.CloseMember(ctx, c.identity.InstanceID)
}

func runtimeSchemaMatchesIdentity(contract RuntimeSchemaContractSnapshot, identity RuntimeClusterIdentity) bool {
	return contract.SchemaVersion == identity.SchemaVersion &&
		constantTimeTextEqual(contract.MigrationName, RuntimeSchemaMigrationName) &&
		constantTimeTextEqual(contract.RuntimeContractID, identity.RuntimeContractID) &&
		constantTimeTextEqual(contract.RuntimeContractDigest, identity.RuntimeContractDigest)
}

func runtimeClusterIdentityEqual(left, right RuntimeClusterIdentity) bool {
	return constantTimeTextEqual(left.ReleaseVersion, right.ReleaseVersion) &&
		constantTimeTextEqual(left.ReleaseCommit, right.ReleaseCommit) &&
		left.SchemaVersion == right.SchemaVersion &&
		constantTimeTextEqual(left.SchemaChecksum, right.SchemaChecksum) &&
		constantTimeTextEqual(left.RuntimeContractID, right.RuntimeContractID) &&
		constantTimeTextEqual(left.RuntimeContractDigest, right.RuntimeContractDigest)
}

func constantTimeTextEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func uniqueRuntimeClusterReasons(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// RequireRuntimeClusterOperation takes a row lock that conflicts with a
// control-mode update. This makes the mode transition the linearization point
// for new Run inserts, new Session inserts, and claims.
func RequireRuntimeClusterOperation(
	ctx context.Context,
	querier interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	operation RuntimeClusterOperation,
) error {
	if querier == nil {
		return httpx.NewError(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable, "runtime cluster is unavailable")
	}
	var mode RuntimeClusterMode
	if err := querier.QueryRow(ctx, `
SELECT mode
FROM runtime_cluster_control
WHERE singleton_id = 1
FOR SHARE
`).Scan(&mode); err != nil {
		return httpx.NewError(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable, "runtime cluster is unavailable")
	}
	allowed := mode == RuntimeClusterModeNormal ||
		(mode == RuntimeClusterModeDraining && operation != RuntimeClusterNewRun)
	if mode == RuntimeClusterModeHardMaintenance {
		allowed = false
	}
	if operation != RuntimeClusterNewRun && operation != RuntimeClusterNewSession && operation != RuntimeClusterClaim {
		allowed = false
	}
	if allowed {
		return nil
	}
	return httpx.NewError(
		http.StatusServiceUnavailable,
		httpx.CodeServiceUnavailable,
		"runtime is temporarily unavailable during maintenance",
	)
}

type postgresRuntimeClusterRepository struct {
	pool *pgxpool.Pool
}

func (r *postgresRuntimeClusterRepository) UpsertMember(
	ctx context.Context,
	identity RuntimeClusterIdentity,
	draining bool,
	ready bool,
) error {
	if r == nil || r.pool == nil {
		return errors.New("runtime cluster database is unavailable")
	}
	_, err := r.pool.Exec(ctx, `
INSERT INTO runtime_cluster_members (
    instance_id, release_version, release_commit, schema_version,
    schema_checksum, runtime_contract_id, runtime_contract_digest,
    draining, ready
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (instance_id) DO UPDATE
SET release_version = EXCLUDED.release_version,
    release_commit = EXCLUDED.release_commit,
    schema_version = EXCLUDED.schema_version,
    schema_checksum = EXCLUDED.schema_checksum,
    runtime_contract_id = EXCLUDED.runtime_contract_id,
    runtime_contract_digest = EXCLUDED.runtime_contract_digest,
    heartbeat_at = clock_timestamp(),
    draining = EXCLUDED.draining,
    ready = EXCLUDED.ready
`, identity.InstanceID, identity.ReleaseVersion, identity.ReleaseCommit,
		identity.SchemaVersion, identity.SchemaChecksum, identity.RuntimeContractID,
		identity.RuntimeContractDigest, draining, ready)
	return err
}

func (r *postgresRuntimeClusterRepository) Snapshot(
	ctx context.Context,
	liveWindow time.Duration,
) (RuntimeClusterSnapshot, error) {
	if r == nil || r.pool == nil || liveWindow <= 0 {
		return RuntimeClusterSnapshot{}, errors.New("runtime cluster database is unavailable")
	}
	var snapshot RuntimeClusterSnapshot
	if err := r.pool.QueryRow(ctx, `
SELECT clock_timestamp(), c.mode, c.expected_replicas,
       s.schema_version, s.migration_name,
       s.runtime_contract_id, s.runtime_contract_digest
FROM runtime_cluster_control c
JOIN runtime_schema_contracts s ON s.is_current
WHERE c.singleton_id = 1
`).Scan(
		&snapshot.DatabaseTime,
		&snapshot.Control.Mode,
		&snapshot.Control.ExpectedReplicas,
		&snapshot.CurrentSchema.SchemaVersion,
		&snapshot.CurrentSchema.MigrationName,
		&snapshot.CurrentSchema.RuntimeContractID,
		&snapshot.CurrentSchema.RuntimeContractDigest,
	); err != nil {
		return RuntimeClusterSnapshot{}, err
	}
	rows, err := r.pool.Query(ctx, `
SELECT instance_id, release_version, release_commit, schema_version,
       schema_checksum, runtime_contract_id, runtime_contract_digest,
       heartbeat_at, draining, ready
FROM runtime_cluster_members
WHERE heartbeat_at >= clock_timestamp() - ($1::bigint * interval '1 millisecond')
ORDER BY started_at ASC, instance_id ASC
`, liveWindow.Milliseconds())
	if err != nil {
		return RuntimeClusterSnapshot{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var member RuntimeClusterMemberSnapshot
		if err = rows.Scan(
			&member.InstanceID,
			&member.ReleaseVersion,
			&member.ReleaseCommit,
			&member.SchemaVersion,
			&member.SchemaChecksum,
			&member.RuntimeContractID,
			&member.RuntimeContractDigest,
			&member.HeartbeatAt,
			&member.Draining,
			&member.Ready,
		); err != nil {
			return RuntimeClusterSnapshot{}, err
		}
		snapshot.LiveMembers = append(snapshot.LiveMembers, member)
	}
	if err = rows.Err(); err != nil {
		return RuntimeClusterSnapshot{}, err
	}
	return snapshot, nil
}

func (r *postgresRuntimeClusterRepository) CloseMember(ctx context.Context, instanceID uuid.UUID) error {
	if r == nil || r.pool == nil || instanceID == uuid.Nil {
		return errors.New("runtime cluster database is unavailable")
	}
	_, err := r.pool.Exec(ctx, `DELETE FROM runtime_cluster_members WHERE instance_id = $1`, instanceID)
	return err
}

func (r RuntimeClusterReadiness) HTTPStatus() int {
	if r.Ready {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}
