package runtime_test

import (
	"context"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRuntimeNodeControlledUpgradeTotalBudget(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `CREATE FUNCTION qa_upgrade_slow_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(11); RETURN NEW; END $$;
 CREATE TRIGGER qa_upgrade_slow_write BEFORE INSERT ON runtime_node_upgrade_operations FOR EACH ROW EXECUTE FUNCTION qa_upgrade_slow_write();`)
	require.NoError(t, err)
	cleanup := func() {
		_, err := pool.Exec(ctx, `DROP TRIGGER IF EXISTS qa_upgrade_slow_write ON runtime_node_upgrade_operations; DROP FUNCTION IF EXISTS qa_upgrade_slow_write();`)
		require.NoError(t, err)
	}
	defer cleanup()
	r := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	started := time.Now()
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
	requireUpgradeCode(t, err, "RUNTIME_NODE_OPERATION_BUSY")
	require.InDelta(t, 10, time.Since(started).Seconds(), 1.5)
	// Cancellation must roll back every write and release the Session/Node locks.
	require.Eventually(t, func() bool {
		var count int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_upgrade_operations`).Scan(&count)
		return err == nil && count == 0
	}, time.Second, 10*time.Millisecond)
	cleanup()
	_, err = pool.Exec(ctx, `UPDATE runtime_cluster_members SET heartbeat_at=clock_timestamp()`)
	require.NoError(t, err)
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
	require.NoError(t, err)
}

func TestRuntimeNodeControlledUpgradeScopeChangeRetries(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	held, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer held.Rollback(ctx)
	_, err = held.Exec(ctx, `SELECT 1 FROM runtime_nodes WHERE node_id=$1 FOR NO KEY UPDATE`, f.nodeID)
	require.NoError(t, err)
	type result struct {
		receipt *runtime.RuntimeNodeUpgradeReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		r, e := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
		done <- result{r, e}
	}()
	require.Eventually(t, func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%FROM runtime_nodes%FOR UPDATE%'`).Scan(&n)
		return err == nil && n > 0
	}, time.Second, 10*time.Millisecond)
	// Simulate a Session committed by an operation already holding the Node lock.
	_, err = held.Exec(ctx, `INSERT INTO runtime_sessions(runtime_session_id,node_id,agent_id,credential_id,worker_id,session_epoch,
 device_certificate_serial,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 capacity,status,disconnected_at,drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity)
 SELECT $2,node_id,agent_id,credential_id,'scope-new-worker',1,
 device_certificate_serial,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 0,'closed',clock_timestamp(),clock_timestamp(),clock_timestamp()+INTERVAL '1 minute','ADMIN_REQUESTED',4
 FROM runtime_sessions WHERE runtime_session_id=$1`, f.sessionID, uuid.New())
	require.NoError(t, err)
	require.NoError(t, held.Commit(ctx))
	resultValue := <-done
	require.NoError(t, resultValue.err)
	require.Equal(t, 2, resultValue.receipt.AdmissionCount)
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_upgrade_operations`).Scan(&n))
	require.Equal(t, 1, n)
}

func TestRuntimeNodeControlledUpgradeMaintenanceLinearization(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	held, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer held.Rollback(ctx)
	_, err = held.Exec(ctx, `SELECT 1 FROM runtime_cluster_control WHERE singleton_id=1 FOR UPDATE`)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
		done <- err
	}()
	require.Eventually(t, func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%runtime_cluster_control%FOR SHARE%'`).Scan(&n)
		return err == nil && n > 0
	}, time.Second, 10*time.Millisecond)
	_, err = held.Exec(ctx, `UPDATE runtime_cluster_control SET mode='hard_maintenance' WHERE singleton_id=1`)
	require.NoError(t, err)
	require.NoError(t, held.Commit(ctx))
	requireUpgradeCode(t, <-done, "RUNTIME_NODE_CORE_NOT_READY")
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_upgrade_operations`).Scan(&n))
	require.Zero(t, n)
}

func TestRuntimeNodeControlledUpgradeRejectsNonQuiescentAndExpired(t *testing.T) {
	for _, change := range []string{
		`UPDATE runtime_nodes SET inflight=1`,
		`UPDATE agent_tokens SET expires_at=clock_timestamp()-INTERVAL '1 second'`,
		`DELETE FROM runtime_node_bindings`,
	} {
		t.Run(change, func(t *testing.T) {
			pool, f, svc, actor := upgradeFixture(t, true)
			ctx := context.Background()
			_, err := pool.Exec(ctx, change)
			require.NoError(t, err)
			_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
			var he *httpx.HTTPError
			require.ErrorAs(t, err, &he)
			require.Equal(t, 409, he.Status)
			var n int
			var version string
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_upgrade_operations`).Scan(&n))
			require.Zero(t, n)
			require.NoError(t, pool.QueryRow(ctx, `SELECT node_version FROM runtime_nodes WHERE node_id=$1`, f.nodeID).Scan(&version))
			require.Equal(t, upgradeVersionA, version)
		})
	}
}

func TestRuntimeNodeControlledUpgradeRejectsCrossAgentAdmission(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	creator := insertCreator(t, pool)
	agent := insertAgent(t, pool, creator, "openlinker-runtime://upgrade-second-agent", 0, "approved")
	token := uuid.New()
	source := uuid.New()
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime' WHERE id=$1`, agent)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO agent_tokens(id,agent_id,creator_user_id,name,prefix,token_hash,scopes,status,redeemed_at)
 VALUES($1,$2,$3,'upgrade second agent',$4,$4,ARRAY['agent:pull']::text[],'active_runtime',clock_timestamp())`, token, agent, creator, "ol_agent_"+token.String()[:8])
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO runtime_node_bindings(credential_id,node_id,agent_id,public_key_thumbprint,binding_mode)
 SELECT $1,node_id,$2,device_public_key_thumbprint,'token_only' FROM runtime_nodes WHERE node_id=$3`, token, agent, f.nodeID)
	require.Error(t, err, "existing enrollment allows only one Agent credential binding per Node")
	_, err = pool.Exec(ctx, `INSERT INTO runtime_sessions(runtime_session_id,node_id,agent_id,credential_id,worker_id,session_epoch,
 device_certificate_serial,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 capacity,status,disconnected_at,drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity)
 SELECT $2,node_id,$3,$4,'second-agent-worker',1,device_certificate_serial,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 0,'closed',clock_timestamp(),clock_timestamp(),clock_timestamp()+INTERVAL '1 minute','ADMIN_REQUESTED',4
 FROM runtime_sessions WHERE runtime_session_id=$1`, f.sessionID, source, agent, token)
	require.NoError(t, err)
	receipt, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
	require.NoError(t, err)
	require.Equal(t, 1, receipt.AdmissionCount)
	sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
	p := runtimeNodeAdminPrincipal(f)
	p.AgentID = agent
	p.CredentialID = token
	req := upgradedSessionRequest(f, upgradeVersionB, 2)
	req.AgentID = agent
	req.WorkerID = "second-agent-worker"
	_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), req)
	require.Error(t, err, "one Agent cannot consume the other's admission")
	_, err = sessions.CreateOrAttachSession(ctx, p, req)
	require.Error(t, err, "unbound other Agent must not consume a controlled admission")
	_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), upgradedSessionRequest(f, upgradeVersionB, 2))
	require.NoError(t, err)
	_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	var active int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_sessions WHERE node_id=$1 AND status='active'`, f.nodeID).Scan(&active))
	require.Equal(t, 1, active)
}

func TestRuntimeNodeControlledUpgradeConcurrentReplay(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	held, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer held.Rollback(ctx)
	_, err = held.Exec(ctx, `SELECT 1 FROM runtime_sessions WHERE runtime_session_id=$1 FOR UPDATE`, f.sessionID)
	require.NoError(t, err)
	request := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	type outcome struct {
		receipt *runtime.RuntimeNodeUpgradeReceipt
		err     error
	}
	done := make(chan outcome, 2)
	for i := 0; i < 2; i++ {
		go func() { r, e := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, request); done <- outcome{r, e} }()
	}
	require.Eventually(t, func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%WITH latest AS%'`).Scan(&n)
		return err == nil && n == 2
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, held.Rollback(ctx))
	replayed := 0
	for i := 0; i < 2; i++ {
		got := <-done
		require.NoError(t, got.err)
		require.EqualValues(t, 1, got.receipt.Revision)
		if got.receipt.Replayed {
			replayed++
		}
	}
	require.Equal(t, 1, replayed)
}

func TestRuntimeNodeControlledUpgradeFencesInvalidOfflineHistory(t *testing.T) {
	for _, version := range []string{upgradeVersionA, "openlinker-agent-node/older"} {
		t.Run(version, func(t *testing.T) {
			migrateToCurrent := controlledUpgradePredecessor(t)
			pool, f, svc, actor := upgradeFixtureWithCluster(t, true, false)
			ctx := context.Background()
			retired := uuid.New()
			// Seed the exact state through released schema 094, where credential
			// renewal could change Node version while a Worker was offline.
			_, err := pool.Exec(ctx, `UPDATE runtime_nodes SET node_version=$2 WHERE node_id=$1`, f.nodeID, version)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `INSERT INTO runtime_sessions(runtime_session_id,node_id,agent_id,credential_id,worker_id,session_epoch,
 device_certificate_serial,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 capacity,status,disconnected_at,resume_capacity)
 SELECT $2,node_id,agent_id,credential_id,'pre-drain-offline',1,device_certificate_serial,$3,protocol_version,runtime_contract_id,runtime_contract_digest,features,
 0,'offline',clock_timestamp(),NULL FROM runtime_sessions WHERE runtime_session_id=$1`, f.sessionID, retired, version)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `UPDATE runtime_nodes SET node_version=$2 WHERE node_id=$1`, f.nodeID, upgradeVersionA)
			require.NoError(t, err)
			migrateToCurrent()
			registerUpgradeFixtureCluster(t, pool, f)
			receipt, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0))
			require.NoError(t, err)
			require.Equal(t, 1, receipt.AdmissionCount)
			var status string
			require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM runtime_sessions WHERE runtime_session_id=$1`, retired).Scan(&status))
			require.Equal(t, "closed", status)
			req := upgradedSessionRequest(f, upgradeVersionB, 2)
			req.WorkerID = "pre-drain-offline"
			sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
			_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), req)
			require.Error(t, err, "fenced history must not grant a new identity admission")
			_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), upgradedSessionRequest(f, upgradeVersionB, 2))
			require.NoError(t, err)
		})
	}
}

// A fresh disposable predecessor DB avoids weakening current-schema guards to
// fabricate historical state. Cleanup never selects a caller-supplied DB name.
func controlledUpgradePredecessor(t *testing.T) func() {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	parsed, err := url.Parse(base)
	require.NoError(t, err)
	adminURL := *parsed
	adminURL.Path = "/postgres"
	name := "upgrade_predecessor_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminURL.String())
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	require.NoError(t, err)
	require.NoError(t, admin.Close(ctx))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c, err := pgx.Connect(ctx, adminURL.String())
		require.NoError(t, err)
		defer c.Close(ctx)
		_, err = c.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		require.NoError(t, err)
	})
	parsed.Path = "/" + name
	dsn := parsed.String()
	directory, err := filepath.Abs(filepath.Join("..", "..", "migrations"))
	require.NoError(t, err)
	migrateTo := func(version uint) {
		m, err := migrate.New("file://"+directory, dsn)
		require.NoError(t, err)
		err = m.Migrate(version)
		sourceErr, dbErr := m.Close()
		require.NoError(t, err)
		require.NoError(t, sourceErr)
		require.NoError(t, dbErr)
	}
	migrateTo(94)
	t.Setenv("TEST_DATABASE_URL", dsn)
	return func() { migrateTo(95) }
}

func TestRuntimeNodeControlledUpgradeExpiresWhileWaitingToInsert(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	held, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer held.Rollback(ctx)
	_, err = held.Exec(ctx, `LOCK TABLE runtime_node_upgrade_operations IN SHARE MODE`)
	require.NoError(t, err)
	request := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	request.DeadlineAt = time.Now().Add(500 * time.Millisecond)
	done := make(chan error, 1)
	go func() { _, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, request); done <- err }()
	require.Eventually(t, func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO runtime_node_upgrade_operations%'`).Scan(&n)
		return err == nil && n > 0
	}, time.Second, 10*time.Millisecond)
	time.Sleep(time.Until(request.DeadlineAt) + 20*time.Millisecond)
	require.NoError(t, held.Rollback(ctx))
	requireUpgradeCode(t, <-done, "RUNTIME_NODE_OPERATION_EXPIRED")
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_upgrade_operations`).Scan(&count))
	require.Zero(t, count)
	var version string
	require.NoError(t, pool.QueryRow(ctx, `SELECT node_version FROM runtime_nodes WHERE node_id=$1`, f.nodeID).Scan(&version))
	require.Equal(t, upgradeVersionA, version)
}
