package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
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
	require.Equal(t, "active_runtime", resp.AgentToken.Status)
	require.Empty(t, resp.AgentToken.PlaintextToken, "registration response must not mint a second plaintext token")
	require.NotNil(t, resp.AgentToken.AgentID)

	var status string
	var agentID uuid.UUID
	var redeemed bool
	err = pool.QueryRow(ctx,
		`SELECT status, agent_id, redeemed_at IS NOT NULL
		 FROM agent_tokens
		 WHERE id = $1`,
		uuid.MustParse(minted.ID),
	).Scan(&status, &agentID, &redeemed)
	require.NoError(t, err)
	require.Equal(t, "active_runtime", status)
	require.Equal(t, uuid.MustParse(resp.Agent.ID), agentID)
	require.True(t, redeemed)
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

	items, err := svc.ListAgentTokens(ctx, creatorA, nil)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "A token", items[0].Name)
	require.Empty(t, items[0].PlaintextToken)
}
