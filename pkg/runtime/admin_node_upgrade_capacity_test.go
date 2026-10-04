package runtime_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// An explicit gate, not part of the quick suite. Uses the production schema,
// indexes and service queries on PostgreSQL, including multi-Worker writes.
func TestRuntimeNodeControlledUpgradeCapacity(t *testing.T) {
	if os.Getenv("RUNTIME_NODE_UPGRADE_CAPACITY") != "1" {
		t.Skip("set RUNTIME_NODE_UPGRADE_CAPACITY=1 for the release capacity gate")
	}
	for _, manyWorkers := range []bool{false, true} {
		for _, count := range []int{1, 100, 10000, 10001} {
			t.Run(fmt.Sprintf("workers=%t/history=%d", manyWorkers, count), func(t *testing.T) {
				pool, f, svc, actor := upgradeFixture(t, true)
				ctx := context.Background()
				if os.Getenv("RUNTIME_NODE_UPGRADE_CAPACITY_LOAD") == "1" {
					pool = upgradeDelayedPool(t, pool)
					svc = newTestService(t, pool)
					svc.ConfigureCoreRuntime(f.coreInstanceID)
					stopLoad := startUpgradeRunLoad(t, pool)
					defer stopLoad()
				}

				if count > 1 {
					_, err := pool.Exec(ctx, `INSERT INTO runtime_sessions(runtime_session_id,node_id,agent_id,credential_id,worker_id,session_epoch,
 device_certificate_serial,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 capacity,status,disconnected_at,drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity)
 SELECT gen_random_uuid(),s.node_id,s.agent_id,s.credential_id,
 CASE WHEN $3 THEN 'capacity-worker-'||g ELSE s.worker_id END,CASE WHEN $3 THEN 1 ELSE g+1 END,
 s.device_certificate_serial,s.node_version,s.protocol_version,s.runtime_contract_id,s.runtime_contract_digest,s.features,
 0,'closed',clock_timestamp(),clock_timestamp(),clock_timestamp()+INTERVAL '1 minute','ADMIN_REQUESTED',4
 FROM runtime_sessions s CROSS JOIN generate_series(1,$2::int-1) g WHERE s.runtime_session_id=$1`, f.sessionID, count, manyWorkers)
					require.NoError(t, err)
				}
				_, err := pool.Exec(ctx, `ANALYZE runtime_sessions`)
				require.NoError(t, err)
				var gets, posts []time.Duration
				for sample := 0; sample < 20; sample++ {
					_, err = pool.Exec(ctx, `UPDATE runtime_cluster_members SET heartbeat_at=clock_timestamp() WHERE instance_id=$1`, f.coreInstanceID)
					require.NoError(t, err)
					started := time.Now()
					status, err := svc.CheckRuntimeNodeUpgrade(ctx, f.nodeID)
					gets = append(gets, time.Since(started))
					require.NoError(t, err)
					r := upgradeRequest("restart_after_drain", upgradeVersionA, upgradeVersionA, int64(sample))
					started = time.Now()
					receipt, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
					posts = append(posts, time.Since(started))
					if manyWorkers && count > 10000 {
						require.False(t, status.Eligible)
						requireUpgradeCode(t, err, "RUNTIME_NODE_SCOPE_LIMIT")
					} else {
						require.True(t, status.Eligible, "%+v", status)
						require.NoError(t, err)
						expected := 1
						if manyWorkers {
							expected = count
						}
						require.Equal(t, expected, receipt.AdmissionCount)
					}
				}
				reportUpgradeDurations(t, "GET", gets)
				reportUpgradeDurations(t, "POST", posts)
			})
		}
	}
}

func reportUpgradeDurations(t *testing.T, name string, values []time.Duration) {
	t.Helper()
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	quantile := func(q float64) time.Duration { return values[int(math.Ceil(q*float64(len(values))))-1] }
	t.Logf("%s samples=%d p95=%s p99=%s max=%s", name, len(values), quantile(.95), quantile(.99), values[len(values)-1])
}

func TestRuntimeNodeControlledUpgradeSupersededHistory(t *testing.T) {
	if os.Getenv("RUNTIME_NODE_UPGRADE_CAPACITY") != "1" {
		t.Skip("set RUNTIME_NODE_UPGRADE_CAPACITY=1 for the release history gate")
	}
	pool, f, svc, actor := upgradeFixture(t, false)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO runtime_sessions(runtime_session_id,node_id,agent_id,credential_id,worker_id,session_epoch,
 device_certificate_serial,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 capacity,status,disconnected_at,drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity)
 SELECT gen_random_uuid(),s.node_id,s.agent_id,s.credential_id,
 CASE WHEN g%100=0 THEN s.worker_id ELSE 'history-worker-'||(g%100) END,(g/100)+1,
 s.device_certificate_serial,s.node_version,s.protocol_version,s.runtime_contract_id,s.runtime_contract_digest,s.features,
 0,'offline',clock_timestamp(),s.drain_requested_at,s.drain_deadline_at,s.drain_reason_code,4
 FROM runtime_sessions s CROSS JOIN generate_series(1,99999) g WHERE s.runtime_session_id=$1`, f.sessionID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ANALYZE runtime_sessions`)
	require.NoError(t, err)
	if os.Getenv("RUNTIME_NODE_UPGRADE_CAPACITY_LOAD") == "1" {
		pool = upgradeDelayedPool(t, pool)
		svc = newTestService(t, pool)
		svc.ConfigureCoreRuntime(f.coreInstanceID)
		stop := startUpgradeRunLoad(t, pool)
		defer stop()
	}
	var gets, posts []time.Duration
	for sample := 0; sample < 20; sample++ {
		_, err = pool.Exec(ctx, `UPDATE runtime_cluster_members SET heartbeat_at=clock_timestamp() WHERE instance_id=$1`, f.coreInstanceID)
		require.NoError(t, err)
		started := time.Now()
		status, err := svc.CheckRuntimeNodeUpgrade(ctx, f.nodeID)
		gets = append(gets, time.Since(started))
		require.NoError(t, err)
		require.True(t, status.Eligible)
		require.Equal(t, 100, status.SessionCount)
		started = time.Now()
		receipt, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("restart_after_drain", upgradeVersionA, upgradeVersionA, int64(sample)))
		posts = append(posts, time.Since(started))
		require.NoError(t, err)
		require.Equal(t, 100, receipt.AdmissionCount)
	}
	reportUpgradeDurations(t, "100 Workers / 100000 epochs GET", gets)
	reportUpgradeDurations(t, "100 Workers / 100000 epochs POST", posts)
	// The original Session remains historical offline but cannot resume before,
	// during or after admission/activation, even when its version matches again.
	sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
	principal := runtimeNodeAdminPrincipal(f)
	old := upgradedSessionRequest(f, upgradeVersionA, 1)
	old.RuntimeSessionID = f.sessionID
	var otherID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT runtime_session_id FROM runtime_sessions WHERE node_id=$1 AND worker_id='history-worker-1' AND session_epoch=1`, f.nodeID).Scan(&otherID))
	other := old
	other.RuntimeSessionID = otherID
	other.WorkerID = "history-worker-1"
	assertHistorical := func() {
		for _, id := range []uuid.UUID{f.sessionID, otherID} {
			var status string
			require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM runtime_sessions WHERE runtime_session_id=$1`, id).Scan(&status))
			require.Equal(t, "offline", status, "superseded rows remain unchanged")
		}
	}
	assertHistorical()
	_, err = sessions.CreateOrAttachSession(ctx, principal, other)
	require.Error(t, err)
	_, err = sessions.CreateOrAttachSession(ctx, principal, old)
	require.Error(t, err)
	_, err = sessions.CreateOrAttachSession(ctx, principal, upgradedSessionRequest(f, upgradeVersionA, 1001))
	require.NoError(t, err)
	_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	_, err = sessions.CreateOrAttachSession(ctx, principal, old)
	require.Error(t, err)
	assertHistorical()
	_, err = sessions.CreateOrAttachSession(ctx, principal, other)
	require.Error(t, err)
	var total int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_sessions WHERE node_id=$1`, f.nodeID).Scan(&total))
	require.Equal(t, 100001, total, "identity history must never be deleted")
}
