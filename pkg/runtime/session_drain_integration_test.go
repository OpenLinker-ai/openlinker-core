package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRuntimeSessionDrainIsCommittedAdmissionFenceAgainstHeartbeatAndClaim(t *testing.T) {
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

	principal := runtimeNodeAdminPrincipal(fixture)
	sessions := runtime.NewRuntimeSessionService(pool, fixture.coreInstanceID)
	queries := db.New(pool)
	const drainers = 8
	const claimers = 8
	type drainResult struct {
		receipt runtime.RuntimeDrainPayload
		err     error
	}
	type claimResult struct {
		claimed bool
		err     error
	}
	drains := make(chan drainResult, drainers)
	claims := make(chan claimResult, claimers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < drainers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			receipt, drainErr := sessions.DrainSession(context.Background(), principal, runtime.RuntimeSessionDrainRequest{
				RuntimeSessionID: fixture.sessionID,
				AttachmentID:     fixture.attachmentID,
				Payload: runtime.RuntimeDrainPayload{
					DeadlineAt: time.Now().UTC().Add(time.Duration(i+1) * time.Minute),
					ReasonCode: fmt.Sprintf("CONCURRENT_DRAIN_%02d", i),
					Capacity:   0,
					Inflight:   999,
				},
			})
			drains <- drainResult{receipt: receipt, err: drainErr}
		}()
	}
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, claimErr := queries.ClaimRuntimeSessionSlot(context.Background(), db.ClaimRuntimeSessionSlotParams{
				RuntimeSessionID: fixture.sessionID,
				AgentID:          fixture.agentID,
				CoreInstanceID:   fixture.coreInstanceID,
			})
			claims <- claimResult{claimed: claimErr == nil, err: claimErr}
		}()
	}
	close(start)
	wg.Wait()
	close(drains)
	close(claims)

	var committed *runtime.RuntimeDrainPayload
	for result := range drains {
		require.NoError(t, result.err)
		require.Zero(t, result.receipt.Capacity)
		if committed == nil {
			copy := result.receipt
			committed = &copy
			continue
		}
		require.Equal(t, committed.ReasonCode, result.receipt.ReasonCode)
		require.True(t, committed.DeadlineAt.Equal(result.receipt.DeadlineAt))
		require.Equal(t, committed.Inflight, result.receipt.Inflight)
	}
	require.NotNil(t, committed)

	var successfulClaims int64
	for result := range claims {
		if result.claimed {
			successfulClaims++
			continue
		}
		require.True(t, errors.Is(result.err, pgx.ErrNoRows), "claim error = %v", result.err)
	}
	require.Equal(t, successfulClaims, committed.Inflight,
		"the committed receipt must contain the server-side inflight snapshot")

	var status, reason string
	var capacity, inflight int32
	var deadline time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT status, capacity, inflight, drain_deadline_at, drain_reason_code
FROM runtime_sessions WHERE runtime_session_id = $1`, fixture.sessionID).Scan(
		&status, &capacity, &inflight, &deadline, &reason,
	))
	require.Equal(t, "draining", status)
	require.Zero(t, capacity)
	require.Equal(t, successfulClaims, int64(inflight))
	require.Equal(t, committed.ReasonCode, reason)
	require.True(t, committed.DeadlineAt.Equal(deadline))

	// A heartbeat can refresh liveness but cannot clear the committed fence or
	// restore client-advertised capacity.
	heartbeat, err := sessions.HeartbeatSession(context.Background(), principal, runtime.RuntimeSessionHeartbeatRequest{
		RuntimeSessionIdentity: runtime.RuntimeSessionIdentity{
			RuntimeSessionID: fixture.sessionID,
			NodeID:           fixture.nodeID,
			AgentID:          fixture.agentID,
			WorkerID:         "admin-worker",
			SessionEpoch:     1,
		},
		NodeVersion:           "node-admin-v2",
		ProtocolVersion:       runtime.RuntimeProtocolVersion,
		RuntimeContractID:     runtime.RuntimeContractID,
		RuntimeContractDigest: runtime.RuntimeContractDigest,
		Features:              runtime.RuntimeRequiredFeatures(),
		Capacity:              99,
		AttachmentID:          fixture.attachmentID,
		Transport:             runtime.RuntimeTransportWebSocket,
	})
	require.NoError(t, err)
	require.Equal(t, "draining", heartbeat.Session.Status)
	require.Zero(t, heartbeat.Session.Capacity)

	resolved, err := sessions.ResolveSessionPrincipal(context.Background(), principal, fixture.sessionID)
	require.NoError(t, err)
	require.Equal(t, "draining", resolved.Status)
	readBack, err := queries.GetRuntimeSession(context.Background(), fixture.sessionID)
	require.NoError(t, err)
	require.Equal(t, "draining", readBack.Status)
	require.Zero(t, readBack.Capacity)

	_, err = queries.ClaimRuntimeSessionSlot(context.Background(), db.ClaimRuntimeSessionSlotParams{
		RuntimeSessionID: fixture.sessionID,
		AgentID:          fixture.agentID,
		CoreInstanceID:   fixture.coreInstanceID,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows, "no claim may cross the committed drain fence")
}

func runtimeNodeAdminPrincipal(fixture runtimeNodeAdminFixture) runtime.AuthenticatedRuntimePrincipal {
	return runtime.AuthenticatedRuntimePrincipal{
		AgentID:      fixture.agentID,
		CredentialID: fixture.credentialID,
		Device: runtime.RuntimeDeviceIdentity{
			NodeID:                       fixture.nodeID,
			CertificateSerial:            strings.ReplaceAll(fixture.nodeID.String(), "-", ""),
			CertificateFingerprintSHA256: strings.Repeat("a", 64),
			PublicKeyThumbprintSHA256:    strings.Repeat("b", 64),
		},
	}
}
