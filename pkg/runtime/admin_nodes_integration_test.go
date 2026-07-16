package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRuntimeNodeAdminInventoryDrainAndRevokeAgainstPostgres(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)
	fixture := insertRuntimeNodeAdminFixture(t, pool)
	staleSessionID := insertStaleRuntimeNodeAdminSession(t, pool, fixture)
	svc := newTestService(t, pool)

	inventory, err := svc.ListRuntimeNodes(context.Background(), 25, 0)
	require.NoError(t, err)
	require.Equal(t, int32(1), inventory.Total)
	require.Equal(t, int32(25), inventory.Limit)
	require.Equal(t, runtime.RuntimeContractID, inventory.CurrentContractID)
	require.Equal(t, runtime.RuntimeContractDigest, inventory.CurrentContractDigest)
	require.False(t, inventory.DatabaseTime.IsZero())
	require.Len(t, inventory.Items, 1)
	node := inventory.Items[0]
	require.Equal(t, fixture.nodeID.String(), node.NodeID)
	require.True(t, node.ContractMatch)
	require.Equal(t, int32(1), node.ActiveSessionCount)
	require.Equal(t, int32(1), node.ActiveAgentCount)
	require.Equal(t, int32(4), node.Capacity)
	require.Equal(t, int32(1), node.Inflight)

	drained, err := svc.DrainRuntimeNode(context.Background(), fixture.nodeID)
	require.NoError(t, err)
	require.Equal(t, "draining", drained.Status)
	require.Equal(t, int32(1), drained.ActiveSessionCount)
	requireRuntimeNodeAdminState(t, pool, fixture, "draining", "draining", false)
	requireRuntimeNodeSignal(t, pool, fixture, "node.drain", 1)

	_, err = svc.ActivateRuntimeNode(context.Background(), fixture.nodeID)
	require.Error(t, err, "activation must reject non-zero durable inflight")
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_sessions SET inflight = 0 WHERE runtime_session_id = $1`, fixture.sessionID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_nodes SET inflight = 0 WHERE node_id = $1`, fixture.nodeID)
	require.NoError(t, err)
	activated, err := svc.ActivateRuntimeNode(context.Background(), fixture.nodeID)
	require.NoError(t, err)
	require.Equal(t, "active", activated.Status)
	requireRuntimeNodeAdminState(t, pool, fixture, "active", "active", false)
	var staleStatus string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT status FROM runtime_sessions WHERE runtime_session_id = $1`, staleSessionID).Scan(&staleStatus))
	require.Equal(t, "draining", staleStatus, "activation must not revive a stale Session")

	// A second drain proves activation is a guarded rollback, not removal of
	// durable drain support. Revoke remains the irreversible terminal path.
	_, err = svc.DrainRuntimeNode(context.Background(), fixture.nodeID)
	require.NoError(t, err)
	requireRuntimeNodeSignal(t, pool, fixture, "node.drain", 2)

	revoked, err := svc.RevokeRuntimeNode(context.Background(), fixture.nodeID, "rotating the host certificate")
	require.NoError(t, err)
	require.Equal(t, "revoked", revoked.Status)
	require.Equal(t, int32(0), revoked.ActiveSessionCount)
	require.Equal(t, "rotating the host certificate", *revoked.RevokeReason)
	requireRuntimeNodeAdminState(t, pool, fixture, "revoked", "revoked", true)
	requireRuntimeNodeSignal(t, pool, fixture, "node.revoke", 1)
	_, err = svc.ActivateRuntimeNode(context.Background(), fixture.nodeID)
	require.Error(t, err, "revoked Node activation must be impossible")

	// Revocation is idempotent and never rewrites immutable evidence or emits
	// duplicate durable signals.
	replayed, err := svc.RevokeRuntimeNode(context.Background(), fixture.nodeID, "different retry reason")
	require.NoError(t, err)
	require.Equal(t, "rotating the host certificate", *replayed.RevokeReason)
	requireRuntimeNodeSignal(t, pool, fixture, "node.revoke", 1)

	var tokenStatus string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT status FROM agent_tokens WHERE id = $1`, fixture.credentialID).Scan(&tokenStatus))
	require.Equal(t, "active_runtime", tokenStatus, "Node revocation must not revoke a portable Agent credential")
}

func TestRuntimeNodeActivationRejectsZeroLiveSessions(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)
	nodeID := uuid.New()
	serial := strings.ReplaceAll(nodeID.String(), "-", "")
	_, err := pool.Exec(context.Background(), `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features,
    capacity, inflight, status, last_seen_at
) VALUES ($1, 'zero-live', $2, $3, 'node-admin-v2', 2,
          $4, $5, $6, 4, 0, 'active', clock_timestamp())`,
		nodeID, serial, strings.Repeat("c", 64), runtime.RuntimeContractID,
		runtime.RuntimeContractDigest, runtime.RuntimeRequiredFeatures())
	require.NoError(t, err)
	svc := newTestService(t, pool)
	_, err = svc.DrainRuntimeNode(context.Background(), nodeID)
	require.NoError(t, err)
	_, err = svc.ActivateRuntimeNode(context.Background(), nodeID)
	require.Error(t, err)
	var status string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT status FROM runtime_nodes WHERE node_id = $1`, nodeID).Scan(&status))
	require.Equal(t, "draining", status)
}

func TestRuntimeNodeActivationDoesNotReviveOfflineSession(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)
	fixture := insertRuntimeNodeAdminFixture(t, pool)
	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		if _, execErr := tx.Exec(context.Background(), `
UPDATE runtime_sessions SET inflight = 0 WHERE runtime_session_id = $1`, fixture.sessionID); execErr != nil {
			return execErr
		}
		if _, execErr := tx.Exec(context.Background(), `
UPDATE runtime_nodes SET inflight = 0 WHERE node_id = $1`, fixture.nodeID); execErr != nil {
			return execErr
		}
		if _, execErr := tx.Exec(context.Background(), `
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(), disconnect_reason = 'test_offline'
WHERE runtime_session_id = $1 AND detached_at IS NULL`, fixture.sessionID); execErr != nil {
			return execErr
		}
		_, execErr := tx.Exec(context.Background(), `
UPDATE runtime_sessions
SET status = 'offline', attached_core_instance_id = NULL,
    disconnected_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE runtime_session_id = $1`, fixture.sessionID)
		return execErr
	})
	require.NoError(t, err)
	svc := newTestService(t, pool)
	_, err = svc.DrainRuntimeNode(context.Background(), fixture.nodeID)
	require.NoError(t, err)
	_, err = svc.ActivateRuntimeNode(context.Background(), fixture.nodeID)
	require.Error(t, err)
	var nodeStatus, sessionStatus string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT n.status, s.status
FROM runtime_nodes n JOIN runtime_sessions s ON s.node_id = n.node_id
WHERE n.node_id = $1`, fixture.nodeID).Scan(&nodeStatus, &sessionStatus))
	require.Equal(t, "draining", nodeStatus)
	require.Equal(t, "offline", sessionStatus)
}

func TestRuntimeNodeActivationRejectsAcceptedUnfinishedAttempt(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	require.NotNil(t, fixture.identity.NodeID)
	require.NotNil(t, fixture.identity.RuntimeSessionID)

	var accepted, unfinished bool
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT accepted_at IS NOT NULL, finished_at IS NULL
FROM run_attempts WHERE id = $1`, fixture.identity.AttemptID).Scan(&accepted, &unfinished))
	require.True(t, accepted)
	require.True(t, unfinished)

	svc := newTestService(t, pool)
	_, err := svc.DrainRuntimeNode(context.Background(), *fixture.identity.NodeID)
	require.NoError(t, err)

	// Zero the advisory counters to isolate the durable unfinished-attempt
	// predicate as the activation blocker.
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_sessions SET inflight = 0 WHERE runtime_session_id = $1`, *fixture.identity.RuntimeSessionID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_nodes SET inflight = 0 WHERE node_id = $1`, *fixture.identity.NodeID)
	require.NoError(t, err)

	_, err = svc.ActivateRuntimeNode(context.Background(), *fixture.identity.NodeID)
	requireRuntimeNodeAdminErrorCode(t, err, "RUNTIME_NODE_NOT_QUIESCENT")
}

func TestRuntimeNodeDrainActivateRevokeRaceIsRevocationTerminal(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)
	fixture := insertRuntimeNodeAdminFixture(t, pool)
	_, err := pool.Exec(context.Background(), `
UPDATE runtime_sessions SET inflight = 0 WHERE runtime_session_id = $1`, fixture.sessionID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_nodes SET inflight = 0 WHERE node_id = $1`, fixture.nodeID)
	require.NoError(t, err)
	svc := newTestService(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, 24)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			_, mutationErr := svc.DrainRuntimeNode(ctx, fixture.nodeID)
			errs <- mutationErr
		}()
		go func() {
			defer wg.Done()
			<-start
			_, mutationErr := svc.ActivateRuntimeNode(ctx, fixture.nodeID)
			errs <- mutationErr
		}()
		go func(round int) {
			defer wg.Done()
			<-start
			_, mutationErr := svc.RevokeRuntimeNode(ctx, fixture.nodeID, fmt.Sprintf("terminal race %d", round))
			errs <- mutationErr
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for mutationErr := range errs {
		if mutationErr == nil {
			continue
		}
		var httpErr *httpx.HTTPError
		require.True(t, errors.As(mutationErr, &httpErr), "unexpected mutation error: %v", mutationErr)
		require.Equal(t, 409, httpErr.Status, "race must not surface an internal failure")
	}

	var nodeStatus, sessionStatus string
	var revokedAt, detachedAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT n.status, n.revoked_at, s.status, attachment.detached_at
FROM runtime_nodes n
JOIN runtime_sessions s ON s.node_id = n.node_id
JOIN runtime_session_attachments attachment ON attachment.runtime_session_id = s.runtime_session_id
WHERE n.node_id = $1 AND s.runtime_session_id = $2`, fixture.nodeID, fixture.sessionID).Scan(
		&nodeStatus, &revokedAt, &sessionStatus, &detachedAt,
	))
	require.Equal(t, "revoked", nodeStatus)
	require.NotNil(t, revokedAt)
	require.Equal(t, "revoked", sessionStatus)
	require.NotNil(t, detachedAt)
	_, err = svc.ActivateRuntimeNode(context.Background(), fixture.nodeID)
	requireRuntimeNodeAdminErrorCode(t, err, "RUNTIME_NODE_REVOKED")
}

type runtimeNodeAdminFixture struct {
	nodeID         uuid.UUID
	agentID        uuid.UUID
	credentialID   uuid.UUID
	sessionID      uuid.UUID
	attachmentID   uuid.UUID
	coreInstanceID uuid.UUID
}

func resetRuntimeNodeAdminTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
TRUNCATE runtime_signal_outbox, runtime_session_attachments,
         runtime_sessions, runtime_nodes RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
}

func insertRuntimeNodeAdminFixture(t *testing.T, pool *pgxpool.Pool) runtimeNodeAdminFixture {
	t.Helper()
	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/runtime", 0, "approved")
	_, err := pool.Exec(context.Background(), `
UPDATE agents
SET connection_mode = 'runtime', endpoint_url = 'openlinker-runtime://admin-test'
WHERE id = $1`, agentID)
	require.NoError(t, err)

	fixture := runtimeNodeAdminFixture{
		nodeID:         uuid.New(),
		agentID:        agentID,
		credentialID:   uuid.New(),
		sessionID:      uuid.New(),
		attachmentID:   uuid.New(),
		coreInstanceID: uuid.New(),
	}
	serial := strings.ReplaceAll(fixture.nodeID.String(), "-", "")
	prefix := "ol_agent_" + fixture.credentialID.String()[:8]
	features := runtime.RuntimeRequiredFeatures()
	sort.Strings(features)
	err = pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    status, redeemed_at
) VALUES ($1, $2, $3, 'node-admin', $4, 'test-hash',
          ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
			fixture.credentialID, fixture.agentID, creatorID, prefix); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features,
    capacity, inflight, status, last_seen_at
) VALUES ($1, 'Node admin fixture', $2, $3, 'node-admin-v2', 2,
          $4, $5, $6, 4, 1, 'active',
          clock_timestamp() - INTERVAL '30 seconds')`,
			fixture.nodeID, serial, strings.Repeat("b", 64),
			runtime.RuntimeContractID, runtime.RuntimeContractDigest,
			features); err != nil {
			return err
		}
		if _, err := tx.Exec(context.Background(), `
INSERT INTO runtime_sessions (
    runtime_session_id, node_id, agent_id, credential_id, worker_id,
    session_epoch, device_certificate_serial, node_version,
    protocol_version, runtime_contract_id, runtime_contract_digest,
    features, capacity, inflight, status, attached_core_instance_id,
    heartbeat_at
) VALUES ($1, $2, $3, $4, 'admin-worker', 1, $5, 'node-admin-v2',
          2, $6, $7, $8, 2, 1, 'active', $9,
          clock_timestamp() - INTERVAL '30 seconds')`,
			fixture.sessionID, fixture.nodeID, fixture.agentID,
			fixture.credentialID, serial, runtime.RuntimeContractID,
			runtime.RuntimeContractDigest, features,
			fixture.coreInstanceID); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
INSERT INTO runtime_session_attachments (
    id, runtime_session_id, core_instance_id, attachment_kind,
    transport, transport_reason
) VALUES ($1, $2, $3, 'connected', 'websocket', 'explicit')`,
			fixture.attachmentID, fixture.sessionID, fixture.coreInstanceID)
		return err
	})
	require.NoError(t, err)
	return fixture
}

func insertStaleRuntimeNodeAdminSession(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture runtimeNodeAdminFixture,
) uuid.UUID {
	t.Helper()
	sessionID := uuid.New()
	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		if _, execErr := tx.Exec(context.Background(), `
INSERT INTO runtime_sessions (
    runtime_session_id, node_id, agent_id, credential_id, worker_id,
    session_epoch, device_certificate_serial, node_version,
    protocol_version, runtime_contract_id, runtime_contract_digest,
    features, capacity, inflight, status, attached_core_instance_id,
    heartbeat_at
) SELECT $1, n.node_id, $2, $3, 'stale-admin-worker', 1,
         n.device_certificate_serial, n.node_version, n.protocol_version,
         n.runtime_contract_id, n.runtime_contract_digest, n.features,
         2, 0, 'active', $4, clock_timestamp() - INTERVAL '2 minutes'
  FROM runtime_nodes n WHERE n.node_id = $5`,
			sessionID, fixture.agentID, fixture.credentialID,
			fixture.coreInstanceID, fixture.nodeID); execErr != nil {
			return execErr
		}
		_, execErr := tx.Exec(context.Background(), `
INSERT INTO runtime_session_attachments (
    runtime_session_id, core_instance_id, attachment_kind,
    transport, transport_reason
) VALUES ($1, $2, 'connected', 'websocket', 'explicit')`, sessionID, fixture.coreInstanceID)
		return execErr
	})
	require.NoError(t, err)
	return sessionID
}

func requireRuntimeNodeAdminState(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture runtimeNodeAdminFixture,
	nodeStatus, sessionStatus string,
	attachmentClosed bool,
) {
	t.Helper()
	var gotNode, gotSession string
	var attachedCore *uuid.UUID
	var detachedAtPresent bool
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT n.status, s.status, s.attached_core_instance_id,
       attachment.detached_at IS NOT NULL
FROM runtime_nodes n
JOIN runtime_sessions s ON s.node_id = n.node_id
JOIN runtime_session_attachments attachment
  ON attachment.runtime_session_id = s.runtime_session_id
WHERE n.node_id = $1 AND s.runtime_session_id = $2`,
		fixture.nodeID, fixture.sessionID,
	).Scan(&gotNode, &gotSession, &attachedCore, &detachedAtPresent))
	require.Equal(t, nodeStatus, gotNode)
	require.Equal(t, sessionStatus, gotSession)
	require.Equal(t, attachmentClosed, detachedAtPresent)
	if attachmentClosed {
		require.Nil(t, attachedCore)
	} else {
		require.NotNil(t, attachedCore)
		require.Equal(t, fixture.coreInstanceID, *attachedCore)
	}
}

func requireRuntimeNodeSignal(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture runtimeNodeAdminFixture,
	eventType string,
	want int32,
) {
	t.Helper()
	var count int32
	var payload []byte
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT COUNT(*)::int, COALESCE(MAX(payload::text), '{}')::bytea
FROM runtime_signal_outbox
WHERE event_type = $1 AND agent_id = $2`, eventType, fixture.agentID).Scan(&count, &payload))
	require.Equal(t, want, count)
	var decoded map[string]string
	require.NoError(t, json.Unmarshal(payload, &decoded))
	require.Equal(t, fixture.coreInstanceID.String(), decoded["target_instance_id"])
}

func requireRuntimeNodeAdminErrorCode(t *testing.T, err error, want httpx.ErrorCode) {
	t.Helper()
	require.Error(t, err)
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, want, httpErr.Code)
}
