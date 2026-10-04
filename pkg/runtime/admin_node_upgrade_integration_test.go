package runtime_test

import (
	"context"
	"sync"
	"testing"
	"time"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const upgradeVersionA = "openlinker-agent-node/0.1.62-rc.1"
const upgradeVersionB = "openlinker-agent-node/0.2.0"

func upgradeFixture(t *testing.T, closed bool) (*pgxpool.Pool, runtimeNodeAdminFixture, *runtime.Service, uuid.UUID) {
	return upgradeFixtureWithCluster(t, closed, true)
}

func upgradeFixtureWithCluster(t *testing.T, closed, register bool) (*pgxpool.Pool, runtimeNodeAdminFixture, *runtime.Service, uuid.UUID) {
	t.Helper()
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)
	f := insertRuntimeNodeAdminFixtureVersion(t, pool, upgradeVersionA)
	svc := newTestService(t, pool)
	svc.ConfigureCoreRuntime(f.coreInstanceID)
	_, err := pool.Exec(context.Background(), `UPDATE runtime_sessions SET inflight=0 WHERE runtime_session_id=$1`, f.sessionID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `UPDATE runtime_nodes SET inflight=0 WHERE node_id=$1`, f.nodeID)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `INSERT INTO runtime_node_bindings(credential_id,node_id,agent_id,public_key_thumbprint,binding_mode)
 SELECT $1,node_id,$2,device_public_key_thumbprint,'token_only' FROM runtime_nodes WHERE node_id=$3`, f.credentialID, f.agentID, f.nodeID)
	require.NoError(t, err)
	_, err = svc.DrainRuntimeNode(context.Background(), f.nodeID)
	require.NoError(t, err)
	detachUpgradeSession(t, pool, f, closed)
	if register {
		registerUpgradeFixtureCluster(t, pool, f)
	}
	return pool, f, svc, insertCreator(t, pool)
}

func registerUpgradeFixtureCluster(t *testing.T, pool *pgxpool.Pool, f runtimeNodeAdminFixture) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `TRUNCATE runtime_cluster_members`)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `INSERT INTO runtime_cluster_members(instance_id,release_version,release_commit,schema_version,schema_checksum,runtime_contract_id,runtime_contract_digest,ready)
 VALUES($1,'upgrade-test','synthetic',$2,$3,$4,$5,true)`, f.coreInstanceID, runtime.RuntimeSchemaVersion, runtime.RuntimeSchemaChecksum, runtime.RuntimeContractID, runtime.RuntimeContractDigest)
	require.NoError(t, err)
}

func detachUpgradeSession(t *testing.T, pool *pgxpool.Pool, f runtimeNodeAdminFixture, closed bool) {
	t.Helper()
	_, err := runtime.NewRuntimeSessionService(pool, f.coreInstanceID).DetachCutoverSessions(context.Background())
	require.NoError(t, err)
	if closed {
		_, err = pool.Exec(context.Background(), `UPDATE runtime_sessions SET status='closed',updated_at=clock_timestamp() WHERE node_id=$1 AND status='offline'`, f.nodeID)
		require.NoError(t, err)
	}
}

func upgradeRequest(kind, from, to string, revision int64) runtime.RuntimeNodeUpgradeRequest {
	return runtime.RuntimeNodeUpgradeRequest{OperationID: uuid.New(), Kind: kind, ExpectedVersion: from, TargetVersion: to, ExpectedRevision: revision, DeadlineAt: time.Now().Add(5 * time.Minute)}
}

func upgradedSessionRequest(f runtimeNodeAdminFixture, version string, epoch int64) runtime.RuntimeSessionRequest {
	r := runtimeNodeAdminSessionRequest(f, uuid.New(), "admin-worker", epoch, 4)
	r.NodeVersion = version
	return r
}

func requireUpgradeCode(t *testing.T, err error, code httpx.ErrorCode) {
	t.Helper()
	var e *httpx.HTTPError
	require.ErrorAs(t, err, &e)
	require.Equal(t, code, e.Code)
}

func TestRuntimeNodeControlledUpgradeLifecycle(t *testing.T) {
	for _, closed := range []bool{false, true} {
		name := "offline"
		if closed {
			name = "closed"
		}
		t.Run(name, func(t *testing.T) {
			pool, f, svc, actor := upgradeFixture(t, closed)
			ctx := context.Background()
			status, err := svc.CheckRuntimeNodeUpgrade(ctx, f.nodeID)
			require.NoError(t, err)
			require.True(t, status.Eligible, "%+v", status)
			require.Equal(t, 1, status.AdmissionCount)
			r := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
			receipt, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
			require.NoError(t, err)
			require.EqualValues(t, 1, receipt.Revision)
			require.False(t, receipt.Replayed)
			replay, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
			require.NoError(t, err)
			require.True(t, replay.Replayed)
			require.Equal(t, receipt.CreatedAt, replay.CreatedAt)
			changed := r
			changed.TargetVersion = "openlinker-agent-node/9.0.0"
			_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, changed)
			requireUpgradeCode(t, err, "RUNTIME_NODE_OPERATION_CONFLICT")
			var version, statusText string
			var epoch int64
			require.NoError(t, pool.QueryRow(ctx, `SELECT node_version,status,session_epoch FROM runtime_sessions WHERE runtime_session_id=$1`, f.sessionID).Scan(&version, &statusText, &epoch))
			require.Equal(t, upgradeVersionA, version)
			require.Equal(t, "closed", statusText)
			require.EqualValues(t, 1, epoch)
			sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
			principal := runtimeNodeAdminPrincipal(f)
			old := upgradedSessionRequest(f, upgradeVersionA, 1)
			old.RuntimeSessionID = f.sessionID
			_, err = sessions.CreateOrAttachSession(ctx, principal, old)
			require.Error(t, err)
			wrong := upgradedSessionRequest(f, upgradeVersionB, 2)
			wrong.WorkerID = "different-worker"
			_, err = sessions.CreateOrAttachSession(ctx, principal, wrong)
			require.Error(t, err)
			request := upgradedSessionRequest(f, upgradeVersionB, 2)
			state, err := sessions.CreateOrAttachSession(ctx, principal, request)
			require.NoError(t, err)
			require.Equal(t, "draining", state.Session.Status)
			require.Zero(t, state.Session.Capacity)
			// A failed start before activation is retryable with the same permission,
			// strictly higher persisted epoch and a new Session ID.
			detachUpgradeSession(t, pool, f, closed)
			request = upgradedSessionRequest(f, upgradeVersionB, 3)
			state, err = sessions.CreateOrAttachSession(ctx, principal, request)
			require.NoError(t, err)
			require.EqualValues(t, 3, state.Session.SessionEpoch)
			_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
			require.NoError(t, err)
			var current *uuid.UUID
			require.NoError(t, pool.QueryRow(ctx, `SELECT current_operation_id FROM runtime_node_upgrade_state WHERE node_id=$1`, f.nodeID).Scan(&current))
			require.Nil(t, current)
			// Reverse is another operation, never a revision/epoch or state rollback.
			_, err = svc.DrainRuntimeNode(ctx, f.nodeID)
			require.NoError(t, err)
			detachUpgradeSession(t, pool, f, true)
			_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionB, upgradeVersionA, 1))
			require.NoError(t, err)
			old.NodeVersion = upgradeVersionA
			_, err = sessions.CreateOrAttachSession(ctx, principal, old)
			require.Error(t, err, "ABA must not resurrect the original Session")
			state, err = sessions.CreateOrAttachSession(ctx, principal, upgradedSessionRequest(f, upgradeVersionA, 4))
			require.NoError(t, err)
			require.Equal(t, "draining", state.Session.Status)
		})
	}
}

func TestRuntimeNodeControlledRestartAndUnstartedRetarget(t *testing.T) {
	for _, retarget := range []bool{false, true} {
		t.Run(map[bool]string{false: "restart", true: "unstarted-retarget"}[retarget], func(t *testing.T) {
			pool, f, svc, actor := upgradeFixture(t, true)
			ctx := context.Background()
			sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
			principal := runtimeNodeAdminPrincipal(f)
			_, err := sessions.CreateOrAttachSession(ctx, principal, upgradedSessionRequest(f, upgradeVersionA, 2))
			require.Error(t, err, "ordinary closed predecessor must stay rejected")
			if retarget {
				_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
				require.NoError(t, err)
				_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionB, upgradeVersionA, 1))
				require.NoError(t, err)
			} else {
				_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("restart_after_drain", upgradeVersionA, upgradeVersionA, 0))
				require.NoError(t, err)
			}
			state, err := sessions.CreateOrAttachSession(ctx, principal, upgradedSessionRequest(f, upgradeVersionA, 2))
			require.NoError(t, err)
			require.Equal(t, "draining", state.Session.Status)
		})
	}
}

func TestRuntimeNodeControlledUpgradeCASAndAtomicity(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	start := make(chan struct{})
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
		}(i)
	}
	close(start)
	wg.Wait()
	success := 0
	for _, err := range errs {
		if err == nil {
			success++
		} else {
			requireUpgradeCode(t, err, "RUNTIME_NODE_REVISION_CONFLICT")
		}
	}
	require.Equal(t, 1, success)
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_upgrade_operations`).Scan(&count))
	require.Equal(t, 1, count)
	_, err := pool.Exec(ctx, `UPDATE runtime_nodes SET node_version=$2 WHERE node_id=$1`, f.nodeID, upgradeVersionA)
	require.Error(t, err, "credential renewal cannot bypass version CAS")
	_, err = pool.Exec(ctx, `UPDATE runtime_node_upgrade_operations SET deadline_at=deadline_at+INTERVAL '1 second'`)
	require.Error(t, err, "audit and deadline are immutable")
}

func TestRuntimeNodeControlledUpgradeClusterGate(t *testing.T) {
	for _, change := range []string{
		`UPDATE runtime_cluster_control SET mode='hard_maintenance'`,
		`UPDATE runtime_cluster_control SET expected_replicas=2`,
		`UPDATE runtime_cluster_members SET schema_version=80`,
		`UPDATE runtime_cluster_members SET schema_checksum=repeat('a',64)`,
		`UPDATE runtime_cluster_members SET ready=false`,
		`UPDATE runtime_cluster_members SET draining=true`,
		`UPDATE runtime_cluster_members SET heartbeat_at=clock_timestamp()-INTERVAL '16 seconds'`,
	} {
		t.Run(change, func(t *testing.T) {
			pool, f, svc, actor := upgradeFixture(t, true)
			ctx := context.Background()
			_, err := pool.Exec(ctx, change)
			require.NoError(t, err)
			status, err := svc.CheckRuntimeNodeUpgrade(ctx, f.nodeID)
			require.NoError(t, err)
			require.False(t, status.Eligible)
			require.Contains(t, status.Blockers, httpx.ErrorCode("RUNTIME_NODE_CORE_NOT_READY"))
			_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
			requireUpgradeCode(t, err, "RUNTIME_NODE_CORE_NOT_READY")
			var count int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_upgrade_operations`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestRuntimeNodeControlledUpgradeExpiryDoesNotFallback(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	r := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	r.DeadlineAt = time.Now().Add(time.Second)
	_, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
	require.NoError(t, err)
	sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
	principal := runtimeNodeAdminPrincipal(f)
	admitted := upgradedSessionRequest(f, upgradeVersionB, 2)
	_, err = sessions.CreateOrAttachSession(ctx, principal, admitted)
	require.NoError(t, err)
	detachUpgradeSession(t, pool, f, false)
	time.Sleep(time.Until(r.DeadlineAt) + 20*time.Millisecond)
	_, err = sessions.CreateOrAttachSession(ctx, principal, admitted)
	require.Error(t, err, "expired admission must also fence offline reattachment by the admitted ID")
	_, err = sessions.CreateOrAttachSession(ctx, principal, upgradedSessionRequest(f, upgradeVersionB, 3))
	require.Error(t, err, "expired permit must not fall back to ordinary offline recovery")
	receipt, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
	require.NoError(t, err)
	require.True(t, receipt.Replayed)
}

func TestRuntimeNodeControlledUpgradeIgnoresRetiredTerminalWorker(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	retiredID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO runtime_sessions(runtime_session_id,node_id,agent_id,credential_id,worker_id,session_epoch,
 device_certificate_serial,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 capacity,status,disconnected_at,drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity)
 SELECT $2,node_id,agent_id,credential_id,'never-restarted',1,device_certificate_serial,node_version,
 protocol_version,runtime_contract_id,runtime_contract_digest,features,0,'closed',clock_timestamp(),
 drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity FROM runtime_sessions WHERE runtime_session_id=$1`, f.sessionID, retiredID)
	require.NoError(t, err)
	r, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
	require.NoError(t, err)
	require.Equal(t, 2, r.AdmissionCount)
	sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
	_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), upgradedSessionRequest(f, upgradeVersionB, 2))
	require.NoError(t, err)
	_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	_, err = svc.DrainRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	detachUpgradeSession(t, pool, f, true)
	r, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionB, upgradeVersionA, 1))
	require.NoError(t, err)
	require.Equal(t, 1, r.AdmissionCount)
	var retired string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM runtime_sessions WHERE runtime_session_id=$1`, retiredID).Scan(&retired))
	require.Equal(t, "closed", retired)
}

func TestRuntimeNodeControlledUpgradeClosedHelloLockOrder(t *testing.T) {
	pool, f, _, _ := upgradeFixture(t, true)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `SELECT 1 FROM runtime_sessions WHERE runtime_session_id=$1 FOR UPDATE`, f.sessionID)
	require.NoError(t, err)
	request := upgradedSessionRequest(f, upgradeVersionA, 1)
	request.RuntimeSessionID = f.sessionID
	done := make(chan error, 1)
	go func() {
		_, err := runtime.NewRuntimeSessionService(pool, f.coreInstanceID).CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), request)
		done <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		return pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%OR (status IN%')`).Scan(&waiting) == nil && waiting
	}, 3*time.Second, 10*time.Millisecond)
	_, err = tx.Exec(ctx, `SELECT 1 FROM runtime_nodes WHERE node_id=$1 FOR UPDATE NOWAIT`, f.nodeID)
	require.NoError(t, err, "closed Hello must wait on Session before holding Node")
	require.NoError(t, tx.Rollback(ctx))
	select {
	case err = <-done:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("closed Hello did not release locks")
	}
}

func TestRuntimeNodeControlledUpgradeDatabaseRejectsOrdinarySuccessor(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	_, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
	require.NoError(t, err)
	principal := runtimeNodeAdminPrincipal(f)
	request := upgradedSessionRequest(f, upgradeVersionB, 2)
	_, err = runtime.NewRuntimeSessionService(pool, f.coreInstanceID).CreateOrAttachSession(ctx, principal, request)
	require.NoError(t, err)
	detachUpgradeSession(t, pool, f, false)
	// This is the real ordinary query, as called by a Core unaware of upgrade
	// tickets. Even before expiry it lacks the transaction's admission proof.
	_, err = db.New(pool).CreateDrainingRuntimeSessionSuccessor(ctx, db.CreateDrainingRuntimeSessionSuccessorParams{
		RuntimeSessionID: uuid.New(), NodeID: f.nodeID, AgentID: f.agentID, CredentialID: f.credentialID, WorkerID: request.WorkerID,
		SessionEpoch: 3, DeviceCertificateSerial: principal.Device.CertificateSerial, DevicePublicKeyThumbprint: principal.Device.PublicKeyThumbprintSHA256,
		NodeVersion: upgradeVersionB, ProtocolVersion: request.ProtocolVersion, RuntimeContractID: request.RuntimeContractID, RuntimeContractDigest: request.RuntimeContractDigest,
		Features: request.Features, ResumeCapacity: 4, AttachedCoreInstanceID: f.coreInstanceID, DrainDeadlineMS: 60000,
	})
	require.ErrorContains(t, err, "requires a current controlled admission")
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_sessions WHERE node_id=$1`, f.nodeID).Scan(&count))
	require.Equal(t, 2, count)
}

func TestRuntimeNodeControlledUpgradeLockTimeout(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `SELECT 1 FROM runtime_sessions WHERE runtime_session_id=$1 FOR UPDATE`, f.sessionID)
	require.NoError(t, err)
	r := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	started := time.Now()
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
	requireUpgradeCode(t, err, "RUNTIME_NODE_OPERATION_BUSY")
	require.GreaterOrEqual(t, time.Since(started), 1900*time.Millisecond)
	require.Less(t, time.Since(started), 5*time.Second)
	require.NoError(t, tx.Rollback(ctx))
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_upgrade_operations`).Scan(&count))
	require.Zero(t, count)
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
	require.NoError(t, err, "failed transaction must release all locks")
}
