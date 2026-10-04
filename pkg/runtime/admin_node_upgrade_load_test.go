package runtime_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// Controlled 2 ms delay in each direction models a small private-network hop.
// It is deliberately a reproducible test profile, not a production SLA.
type upgradeDelayedConnection struct{ net.Conn }

func (c upgradeDelayedConnection) Write(p []byte) (int, error) {
	time.Sleep(2 * time.Millisecond)
	return c.Conn.Write(p)
}
func (c upgradeDelayedConnection) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	time.Sleep(2 * time.Millisecond)
	return n, err
}
func upgradeDelayedPool(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config()
	dial := cfg.ConnConfig.DialFunc
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return upgradeDelayedConnection{c}, nil
	}
	delayed, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(delayed.Close)
	return delayed
}

// Runs on unrelated active Nodes exercise production create/claim/ACK/finalize
// and capacity release while the upgrade locks and writes its own Node scope.
func startUpgradeRunLoad(t *testing.T, pool *pgxpool.Pool) func() {
	t.Helper()
	ctx := context.Background()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errors := make(chan error, 2)
	samples := make(chan []time.Duration, 2)
	var loaders []func() error
	for worker := 0; worker < 2; worker++ {
		thumbprint := strings.Repeat(strings.ReplaceAll(uuid.NewString(), "-", ""), 2)
		f := insertRuntimeNodeAdminFixtureDevice(t, pool, "node-load-v1", thumbprint)
		_, err := pool.Exec(ctx, `UPDATE runtime_nodes SET inflight=0,last_seen_at=clock_timestamp() WHERE node_id=$1`, f.nodeID)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE runtime_sessions SET inflight=0,heartbeat_at=clock_timestamp() WHERE runtime_session_id=$1`, f.sessionID)
		require.NoError(t, err)
		svc := newTestService(t, pool)
		svc.ConfigureCoreRuntime(f.coreInstanceID)
		owner := insertRuntimeUser(t, pool)
		signer, err := runtime.NewRuntimeInvocationSigner(strings.Repeat("load-fixture-", 4))
		require.NoError(t, err)
		leases := runtime.NewRuntimeLeaseService(pool, f.coreInstanceID, signer, runtime.DefaultRuntimeLeaseConfig())
		device := runtimeNodeAdminPrincipal(f).Device
		device.PublicKeyThumbprintSHA256 = thumbprint
		principal := runtime.RuntimeSessionPrincipal{RuntimeSessionID: f.sessionID, NodeID: f.nodeID, AgentID: f.agentID, CredentialID: f.credentialID, WorkerID: "admin-worker", SessionEpoch: 1, RuntimeContractDigest: runtime.RuntimeContractDigest, CoreInstanceID: f.coreInstanceID, AttachmentID: f.attachmentID, DeviceCertificateSerial: device.CertificateSerial, DevicePublicKeyThumbprintSHA256: device.PublicKeyThumbprintSHA256, Status: "active"}
		resultPrincipal := runtime.RuntimeResultPrincipal{AgentID: principal.AgentID, RuntimeContractDigest: principal.RuntimeContractDigest, CredentialID: &principal.CredentialID, NodeID: &principal.NodeID, WorkerID: &principal.WorkerID, RuntimeSessionID: &principal.RuntimeSessionID, CoreInstanceID: &principal.CoreInstanceID, AttachmentID: &principal.AttachmentID, DeviceCertificateSerial: &principal.DeviceCertificateSerial, DevicePublicKeyThumbprintSHA256: &principal.DevicePublicKeyThumbprintSHA256}
		run := func() error {
			_, err := pool.Exec(ctx, `UPDATE runtime_sessions SET heartbeat_at=clock_timestamp() WHERE runtime_session_id=$1`, f.sessionID)
			if err != nil {
				return err
			}
			created, err := svc.StartRun(ctx, owner, &runtime.RunRequest{AgentID: f.agentID.String(), Input: map[string]any{"task": "upgrade capacity load"}, IdempotencyKey: uuid.NewString()}, "api")
			if err != nil {
				return err
			}
			assigned, err := leases.ClaimOffer(ctx, principal)
			if err != nil {
				return err
			}
			if assigned == nil || assigned.AttemptIdentity.RunID.String() != created.RunID {
				return fmt.Errorf("load Run was not claimed")
			}
			_, err = leases.AckAssignment(ctx, principal, runtime.RunAssignmentAckPayload{AttemptIdentity: assigned.AttemptIdentity})
			if err != nil {
				return err
			}
			_, err = runtime.NewResultFinalizer(pool, nil, nil).Finalize(ctx, resultPrincipal, runtime.RuntimeResultRequest{AttemptIdentity: assigned.AttemptIdentity.RuntimeIdentity(), ResultID: uuid.New(), Status: "success", Output: map[string]any{"summary": "capacity load"}, DurationMS: 1})
			return err
		}
		baseline := []time.Duration{}
		for i := 0; i < 10; i++ {
			started := time.Now()
			require.NoError(t, run())
			baseline = append(baseline, time.Since(started))
		}
		reportUpgradeDurations(t, fmt.Sprintf("normal Run worker=%d baseline", worker), baseline)
		loaders = append(loaders, run)
	}
	for _, run := range loaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			durations := []time.Duration{}
			defer func() { samples <- durations }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				started := time.Now()
				if err := run(); err != nil {
					errors <- err
					return
				}
				durations = append(durations, time.Since(started))
			}
		}()
	}
	return func() {
		close(stop)
		wg.Wait()
		close(errors)
		close(samples)
		for err := range errors {
			require.NoError(t, err)
		}
		for values := range samples {
			require.NotEmpty(t, values)
			reportUpgradeDurations(t, "normal Run concurrent", values)
		}
	}
}
