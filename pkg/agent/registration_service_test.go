package agent_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestRegistrationService_CreateAgentToken_PendingDefaults(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Agent Token Creator")

	svc := agent.NewRegistrationService(pool)
	resp, err := svc.CreateAgentToken(context.Background(), creatorID, &agent.CreateAgentTokenRequest{
		Name: "local install",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.PlaintextToken)
	require.True(t, strings.HasPrefix(resp.PlaintextToken, "ol_agent_"))
	require.Equal(t, "pending_registration", resp.Status)
	require.Nil(t, resp.AgentID)
	require.NotNil(t, resp.ExpiresAt)
	require.Equal(t, []string{"agent:call", "agent:pull"}, resp.Scopes)
	require.Equal(t, resp.Prefix, resp.PlaintextToken[:12])

	var tokenHash string
	err = pool.QueryRow(context.Background(), `SELECT token_hash FROM agent_tokens WHERE id = $1`, uuid.MustParse(resp.ID)).Scan(&tokenHash)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(tokenHash, credential.FastTokenHashPrefix))
	require.True(t, credential.VerifyFastTokenHash(tokenHash, resp.PlaintextToken))
}

func TestRegistrationService_CreateAgentToken_NonCreatorRejected(t *testing.T) {
	pool := setupTestDB(t)
	userID := insertNonCreatorUser(t, pool)

	svc := agent.NewRegistrationService(pool)
	_, err := svc.CreateAgentToken(context.Background(), userID, &agent.CreateAgentTokenRequest{Name: "nope"})
	assertHTTPStatus(t, err, 403)
}

func TestRegistrationService_RegisterAgentViaToken_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Agent Token Creator")
	ctx := context.Background()

	svc := agent.NewRegistrationService(pool)
	minted, err := svc.CreateAgentToken(ctx, creatorID, &agent.CreateAgentTokenRequest{Name: "self registration"})
	require.NoError(t, err)

	resp, err := svc.RegisterAgentViaToken(ctx, &agent.RegisterAgentViaTokenRequest{
		AgentToken:        minted.PlaintextToken,
		Name:              "Self Registered Translator",
		Description:       "Registration token state-machine test",
		EndpointURL:       "https://example.com/agent/translator",
		PricePerCallCents: 50,
		Tags:              []string{"content/translation"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Agent.ID)
	require.True(t, strings.HasPrefix(resp.Agent.Slug, "self-registered-translator-"))
	require.Equal(t, "private", resp.Agent.Visibility)
	require.Equal(t, "active_runtime", resp.AgentToken.Status)
	require.Empty(t, resp.AgentToken.PlaintextToken, "registration response must not mint a second plaintext token")
	require.NotNil(t, resp.AgentToken.AgentID)

	var status string
	var agentID uuid.UUID
	var redeemed bool
	var lastUsedAt *time.Time
	var tokenHash string
	err = pool.QueryRow(ctx,
		`SELECT status, agent_id, redeemed_at IS NOT NULL, last_used_at, token_hash
		 FROM agent_tokens
		 WHERE id = $1`,
		uuid.MustParse(minted.ID),
	).Scan(&status, &agentID, &redeemed, &lastUsedAt, &tokenHash)
	require.NoError(t, err)
	require.Equal(t, "active_runtime", status)
	require.Equal(t, uuid.MustParse(resp.Agent.ID), agentID)
	require.True(t, redeemed)
	require.Nil(t, lastUsedAt, "registration redeems Agent Token but must not mark runtime activity before a Runtime Session")
	require.True(t, strings.HasPrefix(tokenHash, credential.FastTokenHashPrefix))
	require.True(t, credential.VerifyFastTokenHash(tokenHash, minted.PlaintextToken))
}

func TestRegistrationService_RegisterAgentViaToken_RevokedRejected(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Revoked Agent Token Creator")
	ctx := context.Background()

	svc := agent.NewRegistrationService(pool)
	minted, err := svc.CreateAgentToken(ctx, creatorID, &agent.CreateAgentTokenRequest{Name: "revoked"})
	require.NoError(t, err)
	require.NoError(t, svc.RevokeAgentToken(ctx, creatorID, uuid.MustParse(minted.ID)))

	_, err = svc.RegisterAgentViaToken(ctx, &agent.RegisterAgentViaTokenRequest{
		AgentToken:        minted.PlaintextToken,
		Name:              "Rejected Agent",
		EndpointURL:       "https://example.com/agent/rejected",
		PricePerCallCents: 0,
		Tags:              []string{"data"},
	})
	assertHTTPStatus(t, err, 401)
}

func TestRegistrationService_RevokeAgentToken_ClosesOnlyCredentialSessions(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Runtime Credential Revoke Creator")
	agentID := createApprovedAgent(t, pool, creatorID, "runtime-credential-revoke")
	ctx := context.Background()

	targetTokenID, otherTokenID := uuid.New(), uuid.New()
	nodeID := uuid.New()
	targetActiveSessionID, targetDrainingSessionID := uuid.New(), uuid.New()
	targetOfflineSessionID, otherSessionID := uuid.New(), uuid.New()
	targetInflight := map[uuid.UUID]int32{
		targetActiveSessionID:   1,
		targetDrainingSessionID: 1,
		targetOfflineSessionID:  0,
	}
	targetCoreA, targetCoreB, otherCore := uuid.New(), uuid.New(), uuid.New()
	serial := strings.ReplaceAll(nodeID.String(), "-", "")

	_, err := pool.Exec(ctx, `
UPDATE agents
SET connection_mode = 'runtime', endpoint_url = 'openlinker-runtime://credential-revoke-test'
WHERE id = $1`, agentID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    status, redeemed_at
) VALUES
    ($1, $3, $4, 'target-token', 'ol_agent_a01', 'target-hash',
     ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp()),
    ($2, $3, $4, 'other-token', 'ol_agent_b02', 'other-hash',
     ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		targetTokenID, otherTokenID, agentID, creatorID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features,
    capacity, inflight, status, last_seen_at
) VALUES ($1, 'Shared credential node', $2, $3, 'credential-revoke-test', 2,
          $4, $5, $6, 8, 3, 'active', clock_timestamp())`,
		nodeID, serial, strings.Repeat("c", 64),
		coreruntime.RuntimeContractID, coreruntime.RuntimeContractDigest,
		coreruntime.RuntimeRequiredFeatures())
	require.NoError(t, err)
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, txErr := tx.Exec(ctx, `
INSERT INTO runtime_sessions (
    runtime_session_id, node_id, agent_id, credential_id, worker_id,
    session_epoch, device_certificate_serial, node_version,
    protocol_version, runtime_contract_id, runtime_contract_digest,
    features, capacity, inflight, status, attached_core_instance_id,
    disconnected_at, drain_requested_at, drain_deadline_at,
    drain_reason_code, resume_capacity
) VALUES
    ($1, $5, $6, $7, 'target-active', 1, $8, 'credential-revoke-test',
     2, $9, $10, $11, 4, 1, 'active', $12, NULL, NULL, NULL, NULL, NULL),
    ($2, $5, $6, $7, 'target-draining', 1, $8, 'credential-revoke-test',
     2, $9, $10, $11, 0, 1, 'draining', $13, NULL,
     clock_timestamp(), clock_timestamp() + INTERVAL '30 seconds',
     'credential_revoke_test', 4),
    ($3, $5, $6, $7, 'target-offline', 1, $8, 'credential-revoke-test',
     2, $9, $10, $11, 4, 0, 'offline', NULL, clock_timestamp(),
     NULL, NULL, NULL, NULL),
    ($4, $5, $6, $14, 'other-active', 1, $8, 'credential-revoke-test',
     2, $9, $10, $11, 4, 1, 'active', $15, NULL,
     NULL, NULL, NULL, NULL)`,
			targetActiveSessionID, targetDrainingSessionID, targetOfflineSessionID,
			otherSessionID, nodeID, agentID, targetTokenID, serial,
			coreruntime.RuntimeContractID, coreruntime.RuntimeContractDigest,
			coreruntime.RuntimeRequiredFeatures(), targetCoreA, targetCoreB,
			otherTokenID, otherCore); txErr != nil {
			return txErr
		}
		_, txErr := tx.Exec(ctx, `
INSERT INTO runtime_session_attachments (runtime_session_id, core_instance_id, attachment_kind)
VALUES ($1, $4, 'connected'), ($2, $5, 'connected'), ($3, $6, 'connected')`,
			targetActiveSessionID, targetDrainingSessionID, otherSessionID,
			targetCoreA, targetCoreB, otherCore)
		return txErr
	})
	require.NoError(t, err)

	svc := agent.NewRegistrationService(pool)
	require.NoError(t, svc.RevokeAgentToken(ctx, creatorID, targetTokenID))

	var targetStatus, targetRevocationKind, otherStatus string
	require.NoError(t, pool.QueryRow(ctx, `
SELECT status, revocation_kind FROM agent_tokens WHERE id = $1`, targetTokenID).Scan(
		&targetStatus, &targetRevocationKind,
	))
	require.Equal(t, "revoked", targetStatus)
	require.Equal(t, "manual", targetRevocationKind)
	require.NoError(t, pool.QueryRow(ctx, `
SELECT status FROM agent_tokens WHERE id = $1`, otherTokenID).Scan(&otherStatus))
	require.Equal(t, "active_runtime", otherStatus)

	for _, sessionID := range []uuid.UUID{
		targetActiveSessionID,
		targetDrainingSessionID,
		targetOfflineSessionID,
	} {
		var status string
		var attachedCore *uuid.UUID
		var disconnected bool
		var inflight int32
		var hasDrainEvidence bool
		require.NoError(t, pool.QueryRow(ctx, `
SELECT status, attached_core_instance_id, disconnected_at IS NOT NULL, inflight,
       drain_requested_at IS NOT NULL
       OR drain_deadline_at IS NOT NULL
       OR drain_reason_code IS NOT NULL
       OR resume_capacity IS NOT NULL
FROM runtime_sessions WHERE runtime_session_id = $1`, sessionID).Scan(
			&status, &attachedCore, &disconnected, &inflight, &hasDrainEvidence,
		))
		require.Equal(t, "revoked", status)
		require.Nil(t, attachedCore)
		require.True(t, disconnected)
		require.Equal(t, targetInflight[sessionID], inflight)
		require.False(t, hasDrainEvidence)
	}
	for _, sessionID := range []uuid.UUID{targetActiveSessionID, targetDrainingSessionID} {
		var reason *string
		require.NoError(t, pool.QueryRow(ctx, `
SELECT disconnect_reason
FROM runtime_session_attachments
WHERE runtime_session_id = $1`, sessionID).Scan(&reason))
		require.NotNil(t, reason)
		require.Equal(t, "credential_revoked", *reason)
	}
	var offlineAttachmentCount int
	require.NoError(t, pool.QueryRow(ctx, `
SELECT COUNT(*) FROM runtime_session_attachments WHERE runtime_session_id = $1`,
		targetOfflineSessionID,
	).Scan(&offlineAttachmentCount))
	require.Zero(t, offlineAttachmentCount)

	var otherSessionStatus string
	var otherAttachedCore uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `
SELECT status, attached_core_instance_id
FROM runtime_sessions WHERE runtime_session_id = $1`, otherSessionID).Scan(
		&otherSessionStatus, &otherAttachedCore,
	))
	require.Equal(t, "active", otherSessionStatus)
	require.Equal(t, otherCore, otherAttachedCore)
	var otherDetached bool
	require.NoError(t, pool.QueryRow(ctx, `
SELECT detached_at IS NOT NULL
FROM runtime_session_attachments
WHERE runtime_session_id = $1`, otherSessionID).Scan(&otherDetached))
	require.False(t, otherDetached)

	var nodeStatus string
	var nodeCapacity, nodeInflight int32
	var nodeDraining, nodeRevoked bool
	require.NoError(t, pool.QueryRow(ctx, `
SELECT status, capacity, inflight, draining_at IS NOT NULL, revoked_at IS NOT NULL
FROM runtime_nodes WHERE node_id = $1`, nodeID).Scan(
		&nodeStatus, &nodeCapacity, &nodeInflight, &nodeDraining, &nodeRevoked,
	))
	require.Equal(t, "active", nodeStatus)
	require.Equal(t, int32(8), nodeCapacity)
	require.Equal(t, int32(3), nodeInflight)
	require.False(t, nodeDraining)
	require.False(t, nodeRevoked)

	rows, err := pool.Query(ctx, `
SELECT payload
FROM runtime_signal_outbox
WHERE event_type = 'credential.revoke' AND agent_id = $1
ORDER BY payload->>'target_instance_id'`, agentID)
	require.NoError(t, err)
	defer rows.Close()
	signalTargets := make(map[uuid.UUID]coreruntime.RuntimeConnectionIdentity)
	for rows.Next() {
		var encoded []byte
		require.NoError(t, rows.Scan(&encoded))
		var payload struct {
			TargetInstanceID uuid.UUID                               `json:"target_instance_id"`
			CredentialID     uuid.UUID                               `json:"credential_id"`
			Connections      []coreruntime.RuntimeConnectionIdentity `json:"connections"`
		}
		require.NoError(t, json.Unmarshal(encoded, &payload))
		require.Equal(t, targetTokenID, payload.CredentialID)
		require.Len(t, payload.Connections, 1)
		signalTargets[payload.TargetInstanceID] = payload.Connections[0]
	}
	require.NoError(t, rows.Err())
	require.Equal(t, targetActiveSessionID, signalTargets[targetCoreA].RuntimeSessionID)
	require.Equal(t, int64(1), signalTargets[targetCoreA].SessionEpoch)
	require.NotEqual(t, uuid.Nil, signalTargets[targetCoreA].AttachmentID)
	require.Equal(t, targetDrainingSessionID, signalTargets[targetCoreB].RuntimeSessionID)
	require.Equal(t, int64(1), signalTargets[targetCoreB].SessionEpoch)
	require.NotEqual(t, uuid.Nil, signalTargets[targetCoreB].AttachmentID)

	err = svc.RevokeAgentToken(ctx, creatorID, targetTokenID)
	assertHTTPStatus(t, err, 404)
	var signalCount int
	require.NoError(t, pool.QueryRow(ctx, `
SELECT COUNT(*) FROM runtime_signal_outbox WHERE event_type = 'credential.revoke'`).Scan(&signalCount))
	require.Equal(t, 2, signalCount)
}

func TestRegistrationService_CreateAgentToken_ForExistingAgent(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Rotation Creator")
	agentID := createApprovedAgent(t, pool, creatorID, "rotation-agent")

	svc := agent.NewRegistrationService(pool)
	resp, err := svc.CreateAgentToken(context.Background(), creatorID, &agent.CreateAgentTokenRequest{
		Name:    "rotation",
		AgentID: agentID.String(),
	})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(resp.PlaintextToken, "ol_agent_"))
	require.Equal(t, "active_runtime", resp.Status)
	require.NotNil(t, resp.AgentID)
	require.Equal(t, agentID.String(), *resp.AgentID)
	require.Nil(t, resp.ExpiresAt)
	require.Equal(t, []string{"agent:call"}, resp.Scopes)

	var tokenHash string
	err = pool.QueryRow(context.Background(), `SELECT token_hash FROM agent_tokens WHERE id = $1`, uuid.MustParse(resp.ID)).Scan(&tokenHash)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(tokenHash, credential.FastTokenHashPrefix))
	require.True(t, credential.VerifyFastTokenHash(tokenHash, resp.PlaintextToken))
}

func TestRegistrationService_ListAgentTokens_OnlyOwn(t *testing.T) {
	pool := setupTestDB(t)
	creatorA := insertCreatorUser(t, pool, "Creator A")
	creatorB := insertCreatorUser(t, pool, "Creator B")
	ctx := context.Background()
	svc := agent.NewRegistrationService(pool)

	_, err := svc.CreateAgentToken(ctx, creatorA, &agent.CreateAgentTokenRequest{Name: "A token"})
	require.NoError(t, err)
	_, err = svc.CreateAgentToken(ctx, creatorB, &agent.CreateAgentTokenRequest{Name: "B token"})
	require.NoError(t, err)

	resp, err := svc.ListAgentTokens(ctx, creatorA, nil, agent.ListAgentTokensOptions{})
	require.NoError(t, err)
	items := resp.Items
	require.Len(t, items, 1)
	require.Equal(t, int32(1), resp.Total)
	require.Equal(t, "A token", items[0].Name)
	require.Empty(t, items[0].PlaintextToken)
}
