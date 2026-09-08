package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDelegatedReadEnforcesLiveAttemptAndDirectChild(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	parent := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, runtime.RuntimeDelegatedRunReadFeature)
	unrelated := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute)
	childID := uuid.New()
	key := sha256.Sum256([]byte(childID.String()))
	_, err := pool.Exec(ctx, `
INSERT INTO runs (id,user_id,agent_id,input,status,cost_cents,platform_fee_cents,creator_revenue_cents,source,
 runtime_contract_id,idempotency_key_hash,idempotency_fingerprint,request_metadata,connection_mode_snapshot,
 endpoint_idempotency_snapshot,dispatch_state,max_offer_count,max_attempts,dispatch_deadline_at,run_deadline_at)
SELECT $1,user_id,$2,'{}'::jsonb,'running',0,0,0,'api',runtime_contract_id,$3,$3,'{}'::jsonb,'runtime',NULL,
 'pending',20,3,dispatch_deadline_at,run_deadline_at FROM runs WHERE id=$4`,
		childID, unrelated.identity.AgentID, key[:], parent.identity.RunID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO run_delegations(child_run_id,parent_run_id,caller_agent_id,reason) VALUES($1,$2,$3,'read test')`, childID, parent.identity.RunID, parent.identity.AgentID)
	require.NoError(t, err)
	capability := runtime.RuntimeInvocationCapability{
		Audience: "openlinker.runtime.v2/delegation", RunID: parent.identity.RunID, AttemptID: parent.identity.AttemptID,
		LeaseID: parent.identity.LeaseID, FencingToken: parent.identity.FencingToken, AgentID: parent.identity.AgentID,
		CredentialID: parent.credentialID, NodeID: *parent.identity.NodeID, InputSHA256: sha256.Sum256([]byte(`{}`)),
	}
	device := runtime.RuntimeDeviceIdentity{NodeID: *parent.identity.NodeID, CertificateFingerprintSHA256: strings.Repeat("a", 64)}
	require.NoError(t, pool.QueryRow(ctx, `SELECT a.runtime_worker_id,a.runtime_session_id,a.offered_at,a.attempt_deadline_at,n.device_certificate_serial,n.device_public_key_thumbprint
 FROM run_attempts a JOIN runtime_nodes n ON n.node_id=a.node_id WHERE a.id=$1`, parent.identity.AttemptID).Scan(
		&capability.WorkerID, &capability.RuntimeSessionID, &capability.IssuedAt, &capability.ExpiresAt, &device.CertificateSerial, &device.PublicKeyThumbprintSHA256))
	signer, err := runtime.NewRuntimeInvocationSigner(strings.Repeat("delegation-test-secret-", 3))
	require.NoError(t, err)
	authorize := func(cap runtime.RuntimeInvocationCapability, id uuid.UUID) runtime.RuntimeDelegationAuthorization {
		envelope, token, err := signer.Issue(cap)
		require.NoError(t, err)
		body, err := json.Marshal(runtime.DelegatedRunReadRequest{RunID: id})
		require.NoError(t, err)
		proofRequest := runtime.RuntimeInvocationProofRequest{Method: http.MethodPost, Path: "/api/v1/agent-runtime/delegated-runs/read", IdempotencyKey: "read-child", Context: envelope, Body: body}
		proof, err := runtime.BuildRuntimeInvocationProof(token, proofRequest)
		require.NoError(t, err)
		return runtime.RuntimeDelegationAuthorization{Device: device, InvocationContext: envelope, InvocationToken: token, InvocationProof: proof, IdempotencyKey: proofRequest.IdempotencyKey, ProofRequest: proofRequest}
	}
	service := runtime.NewRuntimeDelegationService(pool, nil, signer)
	auth := authorize(capability, childID)
	result, err := service.ReadDelegatedRun(ctx, auth)
	require.NoError(t, err)
	require.Equal(t, childID, result.RunID)
	require.Equal(t, runtime.RuntimeRunRunning, result.Status)
	// A valid signature cannot expand the direct-child relationship.
	for _, id := range []uuid.UUID{parent.identity.RunID, unrelated.identity.RunID, uuid.New()} {
		_, err = service.ReadDelegatedRun(ctx, authorize(capability, id))
		require.Error(t, err)
	}
	oldCapability := capability
	oldCapability.Audience = ""
	_, err = service.ReadDelegatedRun(ctx, authorize(oldCapability, childID))
	require.Error(t, err, "create-only audience must not read")
	tampered := auth
	tampered.ProofRequest.Path = "/api/v1/agent-runtime/call-agent"
	_, err = service.ReadDelegatedRun(ctx, tampered)
	require.Error(t, err)
	tampered = auth
	tampered.ProofRequest.Body = []byte(`{"run_id":"` + unrelated.identity.RunID.String() + `"}`)
	_, err = service.ReadDelegatedRun(ctx, tampered)
	require.Error(t, err)
	var ownerID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT user_id FROM runs WHERE id=$1`, parent.identity.RunID).Scan(&ownerID))
	coordinator := runtime.NewRuntimeCancellationCoordinator(pool)
	_, err = coordinator.CancelOwnedRun(ctx, ownerID, childID, "test child terminal result")
	require.NoError(t, err)
	result, err = service.ReadDelegatedRun(ctx, auth)
	require.NoError(t, err)
	require.Equal(t, runtime.RuntimeRunCanceled, result.Status)
	require.NotEmpty(t, result.ErrorCode)
	_, err = coordinator.CancelOwnedRun(ctx, ownerID, parent.identity.RunID, "stop parent authority")
	require.NoError(t, err)
	_, err = service.ReadDelegatedRun(ctx, auth)
	require.Error(t, err, "canceled parent retained read authority")
}
