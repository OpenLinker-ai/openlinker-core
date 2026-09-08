package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func dryRunReadyFixture(t *testing.T) (*pgxpool.Pool, *runtime.Service, *runtime.RuntimeLeaseService, runtime.RuntimeSessionPrincipal, db.Agent) {
	t.Helper()
	pool := setupTestDB(t)
	ctx := context.Background()
	coreID := uuid.New()
	svc := runtime.NewService(pool, newTestConfig())
	svc.ConfigureCoreRuntime(coreID)

	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "openlinker-runtime://ready-session", 0, "approved")
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode = 'runtime', visibility = 'private' WHERE id = $1`, agentID)
	require.NoError(t, err)

	tokenID, nodeID, sessionID, attachmentID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	workerID := uuid.NewString()
	certificateSerial := hex.EncodeToString(nodeID[:])
	thumbprintDigest := sha256.Sum256(nodeID[:])
	thumbprint := hex.EncodeToString(thumbprintDigest[:])
	prefix := "ol_agent_" + hex.EncodeToString(tokenID[:6])
	fixtureTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = fixtureTx.Rollback(ctx) }()
	_, err = fixtureTx.Exec(ctx, `
		INSERT INTO agent_tokens (
			id, agent_id, creator_user_id, name, prefix, token_hash,
			scopes, status, redeemed_at
		) VALUES ($1, $2, $3, 'Runtime Worker ready Session', $4, $5,
			ARRAY['agent:call', 'agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		tokenID, agentID, creatorID, prefix, "hash-"+tokenID.String())
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
		INSERT INTO runtime_nodes (
			node_id, display_name, device_certificate_serial,
			device_public_key_thumbprint, node_version, protocol_version,
			runtime_contract_id, runtime_contract_digest, features,
			capacity, last_seen_at
		) VALUES ($1, 'Runtime Worker ready Session', $2, $3, 'test-v2', 2,
			'openlinker.runtime.v2', $4, $5, 1, clock_timestamp())`,
		nodeID, certificateSerial, thumbprint, runtime.RuntimeContractDigest,
		runtime.RuntimeRequiredFeatures())
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
		INSERT INTO runtime_sessions (
			runtime_session_id, node_id, agent_id, credential_id, worker_id,
			session_epoch, device_certificate_serial, node_version,
			protocol_version, runtime_contract_id, runtime_contract_digest,
			features, capacity, attached_core_instance_id
		) VALUES ($1, $2, $3, $4, $5, 1, $6, 'test-v2', 2,
			'openlinker.runtime.v2', $7, $8, 1, $9)`,
		sessionID, nodeID, agentID, tokenID, workerID, certificateSerial,
		runtime.RuntimeContractDigest, runtime.RuntimeRequiredFeatures(), coreID)
	require.NoError(t, err)
	_, err = fixtureTx.Exec(ctx, `
		INSERT INTO runtime_session_attachments (
			id, runtime_session_id, core_instance_id, attachment_kind
		) VALUES ($1, $2, $3, 'connected')`, attachmentID, sessionID, coreID)
	require.NoError(t, err)
	require.NoError(t, fixtureTx.Commit(ctx))

	signer, err := runtime.NewRuntimeInvocationSignerWithPrevious(
		"test-current", "runtime-v2-integration-signing-secret-32-bytes", "", "",
	)
	require.NoError(t, err)
	leases := runtime.NewRuntimeLeaseService(pool, coreID, signer, runtime.DefaultRuntimeLeaseConfig())
	principal := runtime.RuntimeSessionPrincipal{
		RuntimeSessionID:                sessionID,
		NodeID:                          nodeID,
		AgentID:                         agentID,
		CredentialID:                    tokenID,
		WorkerID:                        workerID,
		SessionEpoch:                    1,
		RuntimeContractDigest:           runtime.RuntimeContractDigest,
		CoreInstanceID:                  coreID,
		AttachmentID:                    attachmentID,
		DeviceCertificateSerial:         certificateSerial,
		DevicePublicKeyThumbprintSHA256: thumbprint,
		Status:                          "active",
	}
	agent, err := db.New(pool).GetAgentByID(ctx, agentID)
	require.NoError(t, err)
	return pool, svc, leases, principal, agent
}

func TestDryRunRuntimeUsesDurableExecution(t *testing.T) {
	for _, status := range []string{"success", "failed"} {
		t.Run(status, func(t *testing.T) {
			pool, svc, leases, principal, agent := dryRunReadyFixture(t)
			ctx := context.Background()
			type result struct {
				output  map[string]interface{}
				message string
			}
			results := make(chan result, 1)
			go func() {
				output, message := svc.DryRun(ctx, &agent, map[string]interface{}{"task": "diagnostic"})
				results <- result{output, message}
			}()
			var assignment *runtime.RunAssignedPayload
			require.Eventually(t, func() bool {
				var err error
				assignment, err = leases.ClaimOffer(ctx, principal)
				require.NoError(t, err)
				return assignment != nil
			}, time.Second, 10*time.Millisecond)
			_, err := leases.AckAssignment(ctx, principal, runtime.RunAssignmentAckPayload{AttemptIdentity: assignment.AttemptIdentity})
			require.NoError(t, err)
			request := runtime.RuntimeResultRequest{AttemptIdentity: assignment.AttemptIdentity.RuntimeIdentity(), ResultID: uuid.New(), Status: status, DurationMS: 25}
			if status == "success" {
				request.Output = map[string]any{"answer": "runtime diagnostic completed"}
			} else {
				request.Error = &runtime.RuntimeResultFailure{ErrorCode: "DIAGNOSTIC_FAILURE", Message: "controlled diagnostic failure"}
			}
			_, err = runtime.NewResultFinalizer(pool, nil, nil).Finalize(ctx, runtime.RuntimeResultPrincipal{
				AgentID: principal.AgentID, RuntimeContractDigest: principal.RuntimeContractDigest,
				CredentialID: &principal.CredentialID, NodeID: &principal.NodeID, WorkerID: &principal.WorkerID,
				RuntimeSessionID: &principal.RuntimeSessionID, CoreInstanceID: &principal.CoreInstanceID,
				AttachmentID: &principal.AttachmentID, DeviceCertificateSerial: &principal.DeviceCertificateSerial,
				DevicePublicKeyThumbprintSHA256: &principal.DevicePublicKeyThumbprintSHA256,
			}, request)
			require.NoError(t, err)
			select {
			case got := <-results:
				if status == "success" {
					require.Empty(t, got.message)
					require.Equal(t, request.Output, got.output)
				} else {
					require.Nil(t, got.output)
					require.Contains(t, got.message, "DIAGNOSTIC_FAILURE")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("DryRun did not return after finalization")
			}
			var purpose, finalStatus string
			var cost int32
			require.NoError(t, pool.QueryRow(ctx, `SELECT request_metadata->>'purpose',status,cost_cents FROM runs WHERE id=$1`, assignment.AttemptIdentity.RunID).Scan(&purpose, &finalStatus, &cost))
			require.Equal(t, "dry_run", purpose)
			require.Equal(t, status, finalStatus)
			require.Zero(t, cost)
		})
	}
}

func TestDryRunRuntimeCancelsUnfinishedDiagnostic(t *testing.T) {
	pool, svc, _, _, agent := dryRunReadyFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	output, message := svc.DryRun(ctx, &agent, map[string]interface{}{"task": "timeout"})
	require.Nil(t, output)
	require.NotEmpty(t, message)
	var status, dispatch string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT status,dispatch_state FROM runs WHERE agent_id=$1 AND request_metadata->>'purpose'='dry_run'`, agent.ID).Scan(&status, &dispatch))
	require.Equal(t, "canceled", status)
	require.Equal(t, "terminal", dispatch)
}

func TestDryRunRuntimeRejectsOfflineAgentWithoutOrphan(t *testing.T) {
	pool, svc, _, principal, agent := dryRunReadyFixture(t)
	ctx := context.Background()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE runtime_session_attachments SET detached_at=clock_timestamp(), disconnect_reason='qa-offline' WHERE id=$1`, principal.AttachmentID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE runtime_sessions SET status='offline', attached_core_instance_id=NULL, disconnected_at=clock_timestamp(), updated_at=clock_timestamp() WHERE runtime_session_id=$1`, principal.RuntimeSessionID)
		return err
	})
	require.NoError(t, err)
	output, message := svc.DryRun(ctx, &agent, map[string]interface{}{"task": "offline"})
	require.Nil(t, output)
	require.NotEmpty(t, message)
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE agent_id=$1`, agent.ID).Scan(&count))
	require.Zero(t, count)
}
