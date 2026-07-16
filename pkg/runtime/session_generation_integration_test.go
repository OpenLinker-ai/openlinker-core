package runtime_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRuntimeSessionGenerationSwitchPreviousToCurrentCommitsThroughDBConstraints(t *testing.T) {
	pool := setupTestDB(t)
	requireReliableRuntimeSchema(t, pool)
	resetRuntimeNodeAdminTables(t, pool)

	creatorID := insertCreator(t, pool)
	agentID := insertAgent(t, pool, creatorID, "https://example.com/generation-switch", 0, "approved")
	_, err := pool.Exec(context.Background(), `
UPDATE agents SET connection_mode = 'runtime', endpoint_url = 'openlinker-runtime://generation-switch'
WHERE id = $1`, agentID)
	require.NoError(t, err)

	var previousDigest string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT runtime_contract_digest FROM runtime_wire_contracts
WHERE runtime_contract_id = $1 AND support_tier = 'previous'`, runtime.RuntimeContractID).Scan(&previousDigest))

	nodeID, credentialID, coreID, sessionID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	serial := strings.ReplaceAll(nodeID.String(), "-", "")
	thumbprint := strings.Repeat("d", 64)
	previousFeatures := make([]string, 0, len(runtime.RuntimeRequiredFeatures())-1)
	for _, feature := range runtime.RuntimeRequiredFeatures() {
		if feature != "session_drain" {
			previousFeatures = append(previousFeatures, feature)
		}
	}
	sort.Strings(previousFeatures)
	_, err = pool.Exec(context.Background(), `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    status, redeemed_at
) VALUES ($1, $2, $3, 'generation-switch', $4, $5,
          ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		credentialID, agentID, creatorID, "ol_agent_"+credentialID.String()[:8],
		"generation-switch-"+credentialID.String())
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features,
    capacity, inflight, status, last_seen_at
) VALUES ($1, 'Previous generation Node', $2, $3, 'node-switch-v2', $4,
          $5, $6, $7, 4, 0, 'active', clock_timestamp())`,
		nodeID, serial, thumbprint, runtime.RuntimeProtocolVersion,
		runtime.RuntimeContractID, previousDigest, previousFeatures)
	require.NoError(t, err, "077 constraints must admit the supported previous generation")
	extraFeatures := append(runtime.RuntimeRequiredFeatures(), "future_extension")
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_nodes
SET runtime_contract_digest = $2, features = $3
WHERE node_id = $1`, nodeID, runtime.RuntimeContractDigest, extraFeatures)
	require.ErrorContains(t, err, "target generation features must be exact")

	principal := runtime.AuthenticatedRuntimePrincipal{
		AgentID:      agentID,
		CredentialID: credentialID,
		Device: runtime.RuntimeDeviceIdentity{
			NodeID:                       nodeID,
			CertificateSerial:            serial,
			CertificateFingerprintSHA256: strings.Repeat("e", 64),
			PublicKeyThumbprintSHA256:    thumbprint,
		},
	}
	sessions := runtime.NewRuntimeSessionService(pool, coreID)
	previousRequest := runtime.RuntimeSessionRequest{
		RuntimeSessionIdentity: runtime.RuntimeSessionIdentity{
			RuntimeSessionID: sessionID,
			NodeID:           nodeID,
			AgentID:          agentID,
			WorkerID:         "generation-switch-worker",
			SessionEpoch:     1,
		},
		NodeVersion:           "node-switch-v2",
		ProtocolVersion:       runtime.RuntimeProtocolVersion,
		RuntimeContractID:     runtime.RuntimeContractID,
		RuntimeContractDigest: previousDigest,
		Features:              previousFeatures,
		Capacity:              3,
		Transport:             runtime.RuntimeTransportWebSocket,
	}
	previousState, err := sessions.CreateOrAttachSession(context.Background(), principal, previousRequest)
	require.NoError(t, err)
	require.NotNil(t, previousState.Attachment)

	request := previousRequest
	request.RuntimeSessionID = uuid.New()
	request.SessionEpoch = 2
	request.RuntimeContractDigest = runtime.RuntimeContractDigest
	request.Features = runtime.RuntimeRequiredFeatures()
	_, err = sessions.CreateOrAttachSession(context.Background(), principal, request)
	require.True(t, runtime.IsRuntimeSessionError(err, runtime.RuntimeSessionErrorSessionConflict))
	var stillPrevious string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT runtime_contract_digest FROM runtime_nodes WHERE node_id = $1`, nodeID).Scan(&stillPrevious))
	require.Equal(t, previousDigest, stillPrevious, "a live previous Session must fence the switch")

	_, err = sessions.CloseSession(context.Background(), principal, runtime.RuntimeSessionCloseRequest{
		RuntimeSessionIdentity: previousRequest.RuntimeSessionIdentity,
		Status:                 "offline",
		Reason:                 "generation switch test",
		AttachmentID:           previousState.Attachment.ID,
	})
	require.NoError(t, err)

	state, err := sessions.CreateOrAttachSession(context.Background(), principal, request)
	require.NoError(t, err)
	require.NotNil(t, state.Attachment)
	require.Equal(t, runtime.RuntimeContractDigest, state.Session.RuntimeContractDigest)
	require.ElementsMatch(t, runtime.RuntimeRequiredFeatures(), state.Session.Features)
	var retiredPreviousStatus string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT status FROM runtime_sessions WHERE runtime_session_id = $1`, previousRequest.RuntimeSessionID).Scan(&retiredPreviousStatus))
	require.Equal(t, "closed", retiredPreviousStatus,
		"the switch must atomically retire resumable Sessions from the old generation")

	_, err = sessions.CloseSession(context.Background(), principal, runtime.RuntimeSessionCloseRequest{
		RuntimeSessionIdentity: request.RuntimeSessionIdentity,
		Status:                 "offline",
		Reason:                 "reproduce stale previous replay",
		AttachmentID:           state.Attachment.ID,
	})
	require.NoError(t, err)

	// SQL cannot bypass the same fence while a current-generation Session is
	// resumable, even though there is no active transport.
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_nodes
SET runtime_contract_digest = $2, features = $3
WHERE node_id = $1`, nodeID, previousDigest, previousFeatures)
	require.ErrorContains(t, err, "live or resumable sessions")

	_, err = sessions.CreateOrAttachSession(context.Background(), principal, previousRequest)
	require.True(t, runtime.IsRuntimeSessionError(err, runtime.RuntimeSessionErrorSessionConflict),
		"epoch-1 previous replay must not regain authority after epoch-2 current committed")

	var gotNodeID uuid.UUID
	var gotSerial, gotThumbprint, gotVersion, gotContractID, gotDigest string
	var gotProtocol int32
	var gotFeatures []string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT node_id, device_certificate_serial, device_public_key_thumbprint,
       node_version, protocol_version, runtime_contract_id,
       runtime_contract_digest, features
FROM runtime_nodes WHERE node_id = $1`, nodeID).Scan(
		&gotNodeID, &gotSerial, &gotThumbprint, &gotVersion, &gotProtocol,
		&gotContractID, &gotDigest, &gotFeatures,
	))
	require.Equal(t, nodeID, gotNodeID)
	require.Equal(t, serial, gotSerial)
	require.Equal(t, thumbprint, gotThumbprint)
	require.Equal(t, request.NodeVersion, gotVersion)
	require.Equal(t, request.ProtocolVersion, gotProtocol)
	require.Equal(t, request.RuntimeContractID, gotContractID)
	require.Equal(t, runtime.RuntimeContractDigest, gotDigest)
	require.ElementsMatch(t, runtime.RuntimeRequiredFeatures(), gotFeatures)
}
