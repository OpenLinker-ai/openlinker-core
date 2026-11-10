package agent_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func mcpCreateReq(slug string) *agent.CreateAgentRequest {
	req := validCreateReq(slug)
	req.ConnectionMode = agent.ConnectionModeMCPServer
	req.MCPToolName = "lookup"
	req.EndpointURL = "https://example.com/mcp/" + slug
	return req
}

func TestCreateAgentDefaultsToLegacyJSONAndRejectsTransportOutsideMCP(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)

	created, err := svc.CreateAgent(ctx, ownerID, mcpCreateReq(freshSlug("mcp-default")))
	require.NoError(t, err)
	assert.Equal(t, "legacy_json", created.MCPTransport)
	assert.Nil(t, created.EndpointAuthHeaderName)

	direct, err := svc.CreateAgent(ctx, ownerID, validCreateReq(freshSlug("direct-default")))
	require.NoError(t, err)
	assert.Empty(t, direct.MCPTransport, "direct_http Agents do not show an MCP transport")
	var stored string
	require.NoError(t, pool.QueryRow(ctx, `SELECT mcp_transport FROM agents WHERE id=$1`, uuid.MustParse(direct.ID)).Scan(&stored))
	assert.Equal(t, "legacy_json", stored)

	req := validCreateReq(freshSlug("direct-transport"))
	req.MCPTransport = "streamable_http"
	_, err = svc.CreateAgent(ctx, ownerID, req)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)
	req = validCreateReq(freshSlug("direct-header"))
	req.EndpointAuthHeaderName = "X-Api-Key"
	_, err = svc.CreateAgent(ctx, ownerID, req)
	assertHTTPStatus(t, err, http.StatusUnprocessableEntity)
	for _, bad := range []string{"Cookie", "Mcp-Session-Id", "X-OpenLinker-Token", "Content-Type", "Bad Header"} {
		req = mcpCreateReq(freshSlug("mcp-bad-header"))
		req.EndpointAuthHeaderName = bad
		_, err = svc.CreateAgent(ctx, ownerID, req)
		assertHTTPStatus(t, err, http.StatusUnprocessableEntity)
	}
}

func TestUpdateAgentKeepsOmittedTransportAndInvalidatesReachabilityOnChange(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)

	req := mcpCreateReq(freshSlug("mcp-transport"))
	req.MCPTransport = "streamable_http"
	req.EndpointAuthHeaderName = "X-Api-Key"
	created, err := svc.CreateAgent(ctx, ownerID, req)
	require.NoError(t, err)
	require.Equal(t, "streamable_http", created.MCPTransport)
	require.NotNil(t, created.EndpointAuthHeaderName)
	require.Equal(t, "X-Api-Key", *created.EndpointAuthHeaderName)
	agentID := uuid.MustParse(created.ID)

	pass := func() {
		svc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
		_, err := svc.UpsertCapability(ctx, agentID, ownerID, &agent.UpsertCapabilityRequest{
			InputSchema:  map[string]interface{}{"type": "object"},
			OutputSchema: map[string]interface{}{"type": "object"},
		})
		require.NoError(t, err)
		_, err = svc.CreateExample(ctx, agentID, ownerID, &agent.CreateExampleRequest{
			Title: "example", InputJSON: map[string]interface{}{}, ExpectedOutputJSON: map[string]interface{}{"result": "pong"},
		})
		require.NoError(t, err)
		dry, err := svc.RunDryRun(ctx, agentID, ownerID)
		require.NoError(t, err)
		require.Equal(t, "pass", dry.Result)
	}
	pass()

	// An edit that omits the new fields keeps them and keeps the evidence.
	owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)
	update := updateRequestFrom(owned)
	update.Description = "edited"
	updated, err := svc.UpdateAgent(ctx, agentID, ownerID, update)
	require.NoError(t, err)
	assert.Equal(t, "streamable_http", updated.MCPTransport)
	require.NotNil(t, updated.EndpointAuthHeaderName)
	assert.True(t, updated.Readiness.Callable)

	for _, tc := range []struct {
		name   string
		mutate func(*agent.UpdateAgentRequest)
		check  func(*agent.AgentResponse)
	}{
		{"transport", func(r *agent.UpdateAgentRequest) { r.MCPTransport = "legacy_sse" }, func(r *agent.AgentResponse) {
			assert.Equal(t, "legacy_sse", r.MCPTransport)
		}},
		{"header name", func(r *agent.UpdateAgentRequest) { name := "X-Other-Key"; r.EndpointAuthHeaderName = &name }, func(r *agent.AgentResponse) {
			require.NotNil(t, r.EndpointAuthHeaderName)
			assert.Equal(t, "X-Other-Key", *r.EndpointAuthHeaderName)
		}},
		{"clear header name", func(r *agent.UpdateAgentRequest) { empty := ""; r.EndpointAuthHeaderName = &empty }, func(r *agent.AgentResponse) {
			assert.Nil(t, r.EndpointAuthHeaderName)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := svc.GetMyAgent(ctx, agentID, ownerID)
			require.NoError(t, err)
			if !before.Readiness.Callable {
				svc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
				_, err = svc.RunDryRun(ctx, agentID, ownerID)
				require.NoError(t, err)
			}
			req := updateRequestFrom(before)
			tc.mutate(req)
			after, err := svc.UpdateAgent(ctx, agentID, ownerID, req)
			require.NoError(t, err)
			tc.check(after)
			assert.False(t, after.Readiness.Callable, "a new call shape invalidates reachability evidence")
		})
	}

	// Leaving mcp_server resets the call shape to the defaults.
	owned, err = svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)
	update = updateRequestFrom(owned)
	update.ConnectionMode = agent.ConnectionModeDirectHTTP
	update.EndpointURL = "https://example.com/agent/now-direct"
	_, err = svc.UpdateAgent(ctx, agentID, ownerID, update)
	require.NoError(t, err)
	var transport string
	var headerName *string
	require.NoError(t, pool.QueryRow(ctx, `SELECT mcp_transport, endpoint_auth_header_name FROM agents WHERE id=$1`, agentID).Scan(&transport, &headerName))
	assert.Equal(t, "legacy_json", transport)
	assert.Nil(t, headerName)
}

// A probe result is evidence only for the call shape it used.
func TestEvidenceLockComparesMCPTransportAndHeaderName(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)
	created, err := svc.CreateAgent(ctx, ownerID, mcpCreateReq(freshSlug("mcp-evidence-lock")))
	require.NoError(t, err)
	agentID := uuid.MustParse(created.ID)
	q := db.New(pool)
	probed, err := q.GetAgentByID(ctx, agentID)
	require.NoError(t, err)
	target := db.AgentEvidenceTargetOf(probed)

	lock := func(compareCredential bool) error {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		return q.WithTx(tx).LockAgentEvidenceTargetUnchanged(ctx, agentID, target, compareCredential)
	}
	require.NoError(t, lock(true))
	_, err = pool.Exec(ctx, `UPDATE agents SET mcp_transport='streamable_http' WHERE id=$1`, agentID)
	require.NoError(t, err)
	require.ErrorIs(t, lock(true), db.ErrAgentEvidenceTargetChanged)
	require.NoError(t, lock(false), "benchmarks compare the connection target only")
	_, err = pool.Exec(ctx, `UPDATE agents SET mcp_transport='legacy_json', endpoint_auth_header_name='X-Api-Key' WHERE id=$1`, agentID)
	require.NoError(t, err)
	require.ErrorIs(t, lock(true), db.ErrAgentEvidenceTargetChanged)
	_, err = pool.Exec(ctx, `UPDATE agents SET endpoint_auth_header_name=NULL WHERE id=$1`, agentID)
	require.NoError(t, err)
	require.NoError(t, lock(true))
}
