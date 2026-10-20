package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRuntimeNodeControlledUpgradeExactFeatures(t *testing.T) {
	for _, startTarget := range []bool{false, true} {
		name := "reverse_before_start"
		if startTarget {
			name = "upgrade_restart_reverse"
		}
		t.Run(name, func(t *testing.T) {
			pool, f, svc, actor := upgradeFixture(t, true)
			ctx := context.Background()
			status, err := svc.CheckRuntimeNodeUpgrade(ctx, f.nodeID)
			require.NoError(t, err)
			source := slices.Clone(status.Features)
			target := append(slices.Clone(source), "mcp_bindings.v1", "delegated_run_read.v1")
			r := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
			r.TargetFeatures = target
			receipt, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
			require.NoError(t, err)
			require.ElementsMatch(t, source, receipt.FromFeatures)
			require.ElementsMatch(t, target, receipt.TargetFeatures)
			// Reordering is the same set and therefore the same idempotent intent.
			slices.Reverse(r.TargetFeatures)
			replay, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
			require.NoError(t, err)
			require.True(t, replay.Replayed)
			changed := r
			changed.TargetFeatures = source
			_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, changed)
			requireUpgradeCode(t, err, "RUNTIME_NODE_OPERATION_CONFLICT")

			var history []string
			require.NoError(t, pool.QueryRow(ctx, `SELECT features FROM runtime_sessions WHERE runtime_session_id=$1`, f.sessionID).Scan(&history))
			require.ElementsMatch(t, source, history, "historical features are immutable")
			sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
			principal := runtimeNodeAdminPrincipal(f)
			for _, wrong := range [][]string{source, append(slices.Clone(target), "unapproved.extension")} {
				req := upgradedSessionRequest(f, upgradeVersionB, 2)
				req.Features = wrong
				_, err = sessions.CreateOrAttachSession(ctx, principal, req)
				require.True(t, runtime.IsRuntimeSessionError(err, runtime.RuntimeSessionErrorContractMismatch), "%v", err)
			}
			// A caller bypassing Go still cannot insert a successor with the wrong
			// features, even with the correct transaction-local operation marker.
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)
			_, err = tx.Exec(ctx, `SELECT set_config('openlinker.runtime_node_upgrade_admission',$1,true)`, r.OperationID.String())
			require.NoError(t, err)

			_, err = tx.Exec(ctx, "SAVEPOINT feature_check")
			require.NoError(t, err)
			insert := `INSERT INTO runtime_sessions
 (runtime_session_id,node_id,agent_id,credential_id,worker_id,session_epoch,device_certificate_serial,node_version,
 protocol_version,runtime_contract_id,runtime_contract_digest,features,capacity,status,
 drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity,attached_core_instance_id)
 SELECT $1,node_id,agent_id,credential_id,worker_id,session_epoch+1,device_certificate_serial,$2,
 protocol_version,runtime_contract_id,runtime_contract_digest,$4,0,'draining',
 drain_requested_at,drain_deadline_at,drain_reason_code,resume_capacity,$5
 FROM runtime_sessions WHERE runtime_session_id=$3`
			_, err = tx.Exec(ctx, insert, uuid.New(), upgradeVersionB, f.sessionID, source, f.coreInstanceID)
			require.ErrorContains(t, err, "runtime session node contract identity mismatch")
			_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT feature_check")
			require.NoError(t, err)
			_, err = tx.Exec(ctx, insert, uuid.New(), upgradeVersionB, f.sessionID, target, f.coreInstanceID)
			require.NoError(t, err, "identical SQL with the exact target features must succeed")
			require.NoError(t, tx.Rollback(ctx))

			revision, epoch := int64(1), int64(2)
			if startTarget {
				req := upgradedSessionRequest(f, upgradeVersionB, epoch)
				req.Features = target
				state, err := sessions.CreateOrAttachSession(ctx, principal, req)
				require.NoError(t, err)
				require.Equal(t, "draining", state.Session.Status)
				require.Zero(t, state.Session.Capacity)
				_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
				require.NoError(t, err)
				_, err = svc.DrainRuntimeNode(ctx, f.nodeID)
				require.NoError(t, err)
				detachUpgradeSession(t, pool, f, true)
				restart := upgradeRequest("restart_after_drain", upgradeVersionB, upgradeVersionB, revision)
				restart.TargetFeatures = source
				_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, restart)
				require.Error(t, err, "same-version restarts cannot change features")
				restart.TargetFeatures = nil // legacy six-field requests preserve features
				_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, restart)
				require.NoError(t, err)
				revision++
				epoch++
				req = upgradedSessionRequest(f, upgradeVersionB, epoch)
				req.Features = target
				_, err = sessions.CreateOrAttachSession(ctx, principal, req)
				require.NoError(t, err)
				_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
				require.NoError(t, err)
				_, err = svc.DrainRuntimeNode(ctx, f.nodeID)
				require.NoError(t, err)
				detachUpgradeSession(t, pool, f, true)
				epoch++
			}
			reverse := upgradeRequest("version_change", upgradeVersionB, upgradeVersionA, revision)
			if !startTarget {
				pending, e := svc.CheckRuntimeNodeUpgrade(ctx, f.nodeID)
				require.NoError(t, e)
				require.NotNil(t, pending.CurrentOperation)
				require.ElementsMatch(t, source, pending.CurrentOperation.FromFeatures)
				_, e = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, reverse)
				require.ErrorContains(t, e, "target_features")
			}
			reverse.TargetFeatures = source
			_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, reverse)
			require.NoError(t, err)
			req := upgradedSessionRequest(f, upgradeVersionA, epoch)
			req.Features = source
			_, err = sessions.CreateOrAttachSession(ctx, principal, req)
			require.NoError(t, err)
			_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
			require.NoError(t, err)
		})
	}
}

func TestRuntimeNodeControlledUpgradeFeaturesFailClosed(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	status, err := svc.CheckRuntimeNodeUpgrade(ctx, f.nodeID)
	require.NoError(t, err)
	for _, features := range [][]string{
		{}, {"mcp_bindings.v1"},
		append(slices.Clone(status.Features), status.Features[0]),
		append(slices.Clone(status.Features), ""),
		make([]string, 65),
	} {
		r := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
		r.TargetFeatures = features
		_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
		require.Error(t, err)
	}
	// Omission does not auto-learn the successor's advertised extensions.
	r := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, r)
	require.NoError(t, err)
	req := upgradedSessionRequest(f, upgradeVersionB, 2)
	req.Features = append(slices.Clone(status.Features), "mcp_bindings.v1")
	sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
	_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), req)
	require.True(t, runtime.IsRuntimeSessionError(err, runtime.RuntimeSessionErrorContractMismatch), "%v", err)
	req.Features = status.Features
	_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), req)
	require.NoError(t, err, "an exact request succeeds while the unchanged permit is live")
}

func TestRuntimeNodeControlledUpgradePre099Permit(t *testing.T) {
	migrateCurrent := controlledUpgradePredecessorVersion(t, 98)
	pool, f, svc, actor := upgradeFixtureWithCluster(t, true, false)
	ctx := context.Background()
	legacy := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	legacy.DeadlineAt = legacy.DeadlineAt.UTC().Truncate(time.Microsecond)
	operation := legacy.OperationID
	canonical, err := json.Marshal(struct {
		Node, Actor uuid.UUID
		Request     runtime.RuntimeNodeUpgradeRequest
	}{f.nodeID, actor, legacy})
	require.NoError(t, err)
	require.NotContains(t, string(canonical), "target_features")
	hash := sha256.Sum256(canonical)
	fingerprint := hex.EncodeToString(hash[:])
	// Seed a permit using the released 095/098 table/guard contract, before the
	// new columns exist. No current-schema guard or immutable history is disabled.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO runtime_node_upgrade_operations
 (operation_id,node_id,actor_id,request_fingerprint,kind,from_version,target_version,revision,admission_count,deadline_at)
 VALUES ($1,$2,$3,$6,'version_change',$4,$5,1,1,$7)`,
		operation, f.nodeID, actor, upgradeVersionA, upgradeVersionB, fingerprint, legacy.DeadlineAt)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO runtime_node_upgrade_state VALUES($1,1,$2)`, f.nodeID, operation)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO runtime_node_upgrade_admissions
 (operation_id,agent_id,worker_id,credential_id,source_session_id,minimum_epoch,last_admitted_epoch)
 SELECT $1,agent_id,worker_id,credential_id,runtime_session_id,session_epoch,session_epoch
 FROM runtime_sessions WHERE runtime_session_id=$2`, operation, f.sessionID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SELECT set_config('openlinker.runtime_node_upgrade',$1,true)`, operation.String())
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `UPDATE runtime_nodes SET node_version=$2 WHERE node_id=$1`, f.nodeID, upgradeVersionB)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	migrateCurrent()
	registerUpgradeFixtureCluster(t, pool, f)
	var from, target []string
	require.NoError(t, pool.QueryRow(ctx, `SELECT from_features,target_features FROM runtime_node_upgrade_operations WHERE operation_id=$1`, operation).Scan(&from, &target))
	require.Nil(t, from)
	require.Nil(t, target)
	replay, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, legacy)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
	req := upgradedSessionRequest(f, upgradeVersionB, 2)
	req.Features = append(req.Features, "mcp_bindings.v1")
	_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), req)
	require.Error(t, err, "migration never broadens an existing permit")
	req = upgradedSessionRequest(f, upgradeVersionB, 2)
	_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), req)
	require.NoError(t, err)
	_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	_, err = svc.DrainRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	detachUpgradeSession(t, pool, f, true)
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("version_change", upgradeVersionB, upgradeVersionA, 1))
	require.NoError(t, err)
	_, err = sessions.CreateOrAttachSession(ctx, runtimeNodeAdminPrincipal(f), upgradedSessionRequest(f, upgradeVersionA, 3))
	require.NoError(t, err)
}
