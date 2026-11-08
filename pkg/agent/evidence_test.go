package agent_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
)

const evidenceTestSkillID = "content/translation"

type agentEvidenceSnapshot struct {
	availabilityStatus  string
	lastSuccessfulRunAt *time.Time
	lastFailedRunAt     *time.Time
	lastCheckedAt       *time.Time
	consecutiveFailures int32

	dryRunPassed     bool
	dryRunLastResult string
	dryRunAt         *time.Time
	capabilitiesSet  bool
	examplesSet      bool

	certificationStatus string
	certifiedAt         *time.Time

	scoreStatus       string
	scoreAverage      *int32
	scoreLastBatchID  *uuid.UUID
	scoreVerifiedAt   *time.Time
	benchmarkRunCount int

	capabilityVersion int32
	exampleCount      int
}

func readAgentEvidence(t *testing.T, pool *pgxpool.Pool, agentID uuid.UUID) agentEvidenceSnapshot {
	t.Helper()
	ctx := context.Background()
	var snap agentEvidenceSnapshot
	require.NoError(t, pool.QueryRow(ctx, `
SELECT availability_status, last_successful_run_at, last_failed_run_at, last_checked_at, consecutive_failures
FROM agent_availability_snapshots WHERE agent_id = $1`, agentID).Scan(
		&snap.availabilityStatus, &snap.lastSuccessfulRunAt, &snap.lastFailedRunAt,
		&snap.lastCheckedAt, &snap.consecutiveFailures))
	require.NoError(t, pool.QueryRow(ctx, `
SELECT dry_run_passed, dry_run_last_result, dry_run_at, capabilities_set, examples_set
FROM agent_onboarding_status WHERE agent_id = $1`, agentID).Scan(
		&snap.dryRunPassed, &snap.dryRunLastResult, &snap.dryRunAt,
		&snap.capabilitiesSet, &snap.examplesSet))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT certification_status, certified_at FROM agents WHERE id = $1`, agentID).
		Scan(&snap.certificationStatus, &snap.certifiedAt))
	require.NoError(t, pool.QueryRow(ctx, `
SELECT status, average_score, last_batch_id, verified_at
FROM agent_skill_scores WHERE agent_id = $1 AND skill_id = $2`, agentID, evidenceTestSkillID).Scan(
		&snap.scoreStatus, &snap.scoreAverage, &snap.scoreLastBatchID, &snap.scoreVerifiedAt))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_skill_benchmark_runs WHERE agent_id = $1`, agentID).
		Scan(&snap.benchmarkRunCount))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT version FROM agent_capabilities WHERE agent_id = $1`, agentID).Scan(&snap.capabilityVersion))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_examples WHERE agent_id = $1`, agentID).Scan(&snap.exampleCount))
	return snap
}

// createEvidencedAgent builds a public direct_http Agent that is callable
// (passed dry-run), certified and benchmark-verified, with one benchmark case
// result kept as history.
func createEvidencedAgent(t *testing.T, pool *pgxpool.Pool, svc *agent.Service, ownerID uuid.UUID, slug string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	agentID := createDryRunReadyAgent(t, svc, ownerID, slug)
	_, err := svc.SetVisibility(ctx, agentID, ownerID, "public")
	require.NoError(t, err)
	svc.SetDryRunner(mockDryRunner{output: map[string]interface{}{"result": "pong"}})
	dry, err := svc.RunDryRun(ctx, agentID, ownerID)
	require.NoError(t, err)
	require.Equal(t, "pass", dry.Result)
	require.NoError(t, svc.RequestCertification(ctx, agentID, ownerID))
	require.NoError(t, svc.CertifyAgent(ctx, agentID))

	batchID := uuid.New()
	var testCaseID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT id FROM skill_test_cases WHERE skill_id = $1 ORDER BY sort_order LIMIT 1`, evidenceTestSkillID).
		Scan(&testCaseID))
	_, err = pool.Exec(ctx, `
INSERT INTO agent_skill_benchmark_runs (batch_id, agent_id, skill_id, test_case_id, status, score, finished_at)
VALUES ($1, $2, $3, $4, 'success', 92, NOW())`, batchID, agentID, evidenceTestSkillID, testCaseID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
INSERT INTO agent_skill_scores (agent_id, skill_id, status, average_score, pass_count, total_count, last_batch_id, verified_at)
VALUES ($1, $2, 'verified', 92, 1, 1, $3, NOW())`, agentID, evidenceTestSkillID, batchID)
	require.NoError(t, err)

	owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)
	require.True(t, owned.Readiness.Callable, "fixture must start callable")
	require.True(t, owned.Readiness.Certified, "fixture must start certified")
	return agentID
}

func updateRequestFrom(resp *agent.AgentResponse) *agent.UpdateAgentRequest {
	return &agent.UpdateAgentRequest{
		Name:              resp.Name,
		Description:       resp.Description,
		PricePerCallCents: resp.PricePerCallCents,
		Tags:              resp.Tags,
	}
}

func requireEvidenceIntact(t *testing.T, before, after agentEvidenceSnapshot) {
	t.Helper()
	assert.Equal(t, "healthy", after.availabilityStatus)
	assert.Equal(t, before.lastSuccessfulRunAt, after.lastSuccessfulRunAt)
	assert.Equal(t, before.lastCheckedAt, after.lastCheckedAt)
	assert.True(t, after.dryRunPassed)
	assert.Equal(t, "pass", after.dryRunLastResult)
	assert.Equal(t, before.dryRunAt, after.dryRunAt)
	assert.Equal(t, "certified", after.certificationStatus)
	assert.Equal(t, before.certifiedAt, after.certifiedAt)
	assert.Equal(t, "verified", after.scoreStatus)
	assert.Equal(t, before.scoreAverage, after.scoreAverage)
	assert.Equal(t, before.scoreLastBatchID, after.scoreLastBatchID)
	assert.Equal(t, before.scoreVerifiedAt, after.scoreVerifiedAt)
}

func TestUpdateAgentEndpointChangeInvalidatesEvidenceInSameTransaction(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)
	slug := freshSlug("evidence-endpoint")
	agentID := createEvidencedAgent(t, pool, svc, ownerID, slug)
	before := readAgentEvidence(t, pool, agentID)

	market := agent.NewMarketService(pool)
	detail, err := market.GetBySlug(ctx, slug)
	require.NoError(t, err)
	require.True(t, detail.Readiness.Callable)
	require.True(t, detail.Readiness.Verified)
	require.True(t, detail.Readiness.Certified)

	owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)
	req := updateRequestFrom(owned)
	req.EndpointURL = "https://broken.example.com/agent/" + slug
	updated, err := svc.UpdateAgent(ctx, agentID, ownerID, req)
	require.NoError(t, err)

	assert.Equal(t, req.EndpointURL, updated.EndpointURL)
	assert.Equal(t, "pending", updated.CertificationStatus, "certification is re-queued for review")
	assert.Nil(t, updated.CertifiedAt)
	assert.Equal(t, "unknown", updated.Availability.Status)
	assert.False(t, updated.Readiness.Callable)
	assert.False(t, updated.Readiness.Certified)
	assert.Equal(t, "public", updated.Visibility, "visibility is not evidence")

	after := readAgentEvidence(t, pool, agentID)
	assert.Equal(t, "unknown", after.availabilityStatus)
	assert.Nil(t, after.lastSuccessfulRunAt)
	assert.Nil(t, after.lastFailedRunAt)
	assert.Nil(t, after.lastCheckedAt, "the monitor must re-check the new target on its next pass")
	assert.Zero(t, after.consecutiveFailures)
	assert.False(t, after.dryRunPassed)
	assert.Equal(t, "pending", after.dryRunLastResult)
	assert.Nil(t, after.dryRunAt)
	assert.True(t, after.capabilitiesSet, "declarations are not evidence")
	assert.True(t, after.examplesSet)
	assert.Equal(t, "pending", after.certificationStatus)
	assert.Nil(t, after.certifiedAt)
	assert.Equal(t, "pending", after.scoreStatus)
	assert.Nil(t, after.scoreAverage)
	assert.Nil(t, after.scoreLastBatchID)
	assert.Nil(t, after.scoreVerifiedAt)
	assert.Equal(t, before.benchmarkRunCount, after.benchmarkRunCount, "benchmark case history is kept")
	assert.Equal(t, before.capabilityVersion, after.capabilityVersion)
	assert.Equal(t, before.exampleCount, after.exampleCount)

	detail, err = market.GetBySlug(ctx, slug)
	require.NoError(t, err)
	assert.False(t, detail.Readiness.Callable)
	assert.False(t, detail.Readiness.Verified)
	assert.False(t, detail.Readiness.Certified)

	pending, err := svc.ListPendingForAdmin(ctx)
	require.NoError(t, err)
	found := false
	for _, item := range pending {
		found = found || item.ID == agentID.String()
	}
	assert.True(t, found, "the repointed Agent is back in the certification review queue")

	svc.SetDryRunner(mockDryRunner{errMsg: "endpoint 调用失败: connection refused"})
	batch, err := svc.RunDueAvailabilityChecks(ctx, 10, 900)
	require.NoError(t, err)
	assert.Equal(t, int32(1), batch.Checked, "a reset snapshot is due immediately despite the 15-minute window")
	assert.Equal(t, int32(1), batch.Failed)
}

func TestUpdateAgentConnectionTargetChangesInvalidateEvidence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(req *agent.UpdateAgentRequest, slug string)
	}{
		{"connection mode", func(req *agent.UpdateAgentRequest, _ string) {
			req.ConnectionMode = agent.ConnectionModeRuntime
		}},
		{"mcp server", func(req *agent.UpdateAgentRequest, slug string) {
			req.ConnectionMode = agent.ConnectionModeMCPServer
			req.MCPToolName = "search"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			svc := newTestService(t, pool)
			ctx := context.Background()
			ownerID := insertCreator(t, pool)
			slug := freshSlug("evidence-target")
			agentID := createEvidencedAgent(t, pool, svc, ownerID, slug)
			owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
			require.NoError(t, err)
			req := updateRequestFrom(owned)
			tc.mutate(req, slug)
			_, err = svc.UpdateAgent(ctx, agentID, ownerID, req)
			require.NoError(t, err)
			after := readAgentEvidence(t, pool, agentID)
			assert.Equal(t, "unknown", after.availabilityStatus)
			assert.False(t, after.dryRunPassed)
			assert.Equal(t, "pending", after.certificationStatus)
			assert.Equal(t, "pending", after.scoreStatus)
		})
	}

	t.Run("mcp tool name", func(t *testing.T) {
		pool := setupTestDB(t)
		svc := newTestService(t, pool)
		ctx := context.Background()
		ownerID := insertCreator(t, pool)
		slug := freshSlug("evidence-mcp-tool")
		agentID := createEvidencedAgent(t, pool, svc, ownerID, slug)
		owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
		require.NoError(t, err)
		req := updateRequestFrom(owned)
		req.ConnectionMode = agent.ConnectionModeMCPServer
		req.MCPToolName = "search"
		_, err = svc.UpdateAgent(ctx, agentID, ownerID, req)
		require.NoError(t, err)

		// Re-establish evidence against the MCP target, then change only the tool.
		_, err = pool.Exec(ctx, `UPDATE agents SET certification_status = 'certified', certified_at = NOW() WHERE id = $1`, agentID)
		require.NoError(t, err)
		_, err = svc.RunDryRun(ctx, agentID, ownerID)
		require.NoError(t, err)
		require.Equal(t, "healthy", readAgentEvidence(t, pool, agentID).availabilityStatus)

		req.MCPToolName = "  search  "
		_, err = svc.UpdateAgent(ctx, agentID, ownerID, req)
		require.NoError(t, err)
		kept := readAgentEvidence(t, pool, agentID)
		assert.Equal(t, "healthy", kept.availabilityStatus, "a normalized-equal tool name is the same target")
		assert.Equal(t, "certified", kept.certificationStatus)

		req.MCPToolName = "fetch"
		_, err = svc.UpdateAgent(ctx, agentID, ownerID, req)
		require.NoError(t, err)
		after := readAgentEvidence(t, pool, agentID)
		assert.Equal(t, "unknown", after.availabilityStatus)
		assert.Equal(t, "pending", after.certificationStatus)
	})
}

func TestUpdateAgentWithoutConnectionChangeKeepsEvidence(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)
	slug := freshSlug("evidence-keep")
	agentID := createEvidencedAgent(t, pool, svc, ownerID, slug)
	before := readAgentEvidence(t, pool, agentID)

	owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)

	// Omitted connection fields carry over.
	req := updateRequestFrom(owned)
	req.Name = "Renamed Agent"
	req.Description = "new description"
	req.PricePerCallCents = 900
	req.Tags = []string{"renamed"}
	req.Visibility = "unlisted"
	updated, err := svc.UpdateAgent(ctx, agentID, ownerID, req)
	require.NoError(t, err)
	assert.Equal(t, "certified", updated.CertificationStatus)
	assert.True(t, updated.Readiness.Callable)
	requireEvidenceIntact(t, before, readAgentEvidence(t, pool, agentID))

	// Resending the same target, with surrounding whitespace and the same
	// credential, is not a change either.
	req = updateRequestFrom(updated)
	req.EndpointURL = "  " + owned.EndpointURL + " "
	req.ConnectionMode = agent.ConnectionModeDirectHTTP
	req.EndpointAuthHeader = "Bearer secret"
	_, err = svc.UpdateAgent(ctx, agentID, ownerID, req)
	require.NoError(t, err)
	requireEvidenceIntact(t, before, readAgentEvidence(t, pool, agentID))
}

func TestUpdateAgentCredentialChangeResetsOnlyReachabilityEvidence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(req *agent.UpdateAgentRequest)
	}{
		{"rotate", func(req *agent.UpdateAgentRequest) { req.EndpointAuthHeader = "Bearer rotated" }},
		{"clear", func(req *agent.UpdateAgentRequest) { req.ClearEndpointAuth = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			svc := newTestService(t, pool)
			ctx := context.Background()
			ownerID := insertCreator(t, pool)
			agentID := createEvidencedAgent(t, pool, svc, ownerID, freshSlug("evidence-credential"))
			before := readAgentEvidence(t, pool, agentID)

			owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
			require.NoError(t, err)
			req := updateRequestFrom(owned)
			tc.mutate(req)
			updated, err := svc.UpdateAgent(ctx, agentID, ownerID, req)
			require.NoError(t, err)
			assert.False(t, updated.Readiness.Callable, "calls now present a different credential")
			assert.True(t, updated.Readiness.Certified, "credential rotation keeps certification")

			after := readAgentEvidence(t, pool, agentID)
			assert.Equal(t, "unknown", after.availabilityStatus)
			assert.Nil(t, after.lastSuccessfulRunAt)
			assert.Nil(t, after.lastCheckedAt)
			assert.False(t, after.dryRunPassed)
			assert.Equal(t, "pending", after.dryRunLastResult)
			assert.Equal(t, "certified", after.certificationStatus)
			assert.Equal(t, before.certifiedAt, after.certifiedAt)
			assert.Equal(t, "verified", after.scoreStatus)
			assert.Equal(t, before.scoreAverage, after.scoreAverage)
			assert.Equal(t, before.scoreLastBatchID, after.scoreLastBatchID)
		})
	}
}

func TestUpdateAgentRejectedOrPendingCertificationIsNotTouched(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)
	agentID := createDryRunReadyAgent(t, svc, ownerID, freshSlug("evidence-rejected"))
	require.NoError(t, svc.RequestCertification(ctx, agentID, ownerID))
	require.NoError(t, svc.RejectCertification(ctx, agentID, "needs better examples"))

	owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)
	req := updateRequestFrom(owned)
	req.EndpointURL = "https://example.com/agent/moved"
	updated, err := svc.UpdateAgent(ctx, agentID, ownerID, req)
	require.NoError(t, err)
	assert.Equal(t, "rejected", updated.CertificationStatus)
	require.NotNil(t, updated.RejectionReason)
	assert.Equal(t, "needs better examples", *updated.RejectionReason)
}

// A creator dry-run that probed the previous target must not store its result
// after the owner repointed the Agent mid-probe.
func TestRunDryRunDiscardsResultWhenConnectionChangesDuringProbe(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)
	slug := freshSlug("evidence-dryrun-race")
	agentID := createDryRunReadyAgent(t, svc, ownerID, slug)
	owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)

	runner := newScriptedDryRunner("")
	runner.hook = func(ctx context.Context, _ uuid.UUID) {
		req := updateRequestFrom(owned)
		req.EndpointURL = "https://example.com/agent/repointed-" + slug
		_, updateErr := svc.UpdateAgent(ctx, agentID, ownerID, req)
		require.NoError(t, updateErr)
	}
	svc.SetDryRunner(runner)

	_, err = svc.RunDryRun(ctx, agentID, ownerID)
	assertHTTPStatus(t, err, http.StatusConflict)

	var dryRunResult string
	var dryRunPassed bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT dry_run_last_result, dry_run_passed FROM agent_onboarding_status WHERE agent_id = $1`, agentID).
		Scan(&dryRunResult, &dryRunPassed))
	assert.Equal(t, "pending", dryRunResult)
	assert.False(t, dryRunPassed)
	var snapshots int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM agent_availability_snapshots WHERE agent_id = $1 AND availability_status <> 'unknown'`, agentID).
		Scan(&snapshots))
	assert.Zero(t, snapshots, "no availability evidence for the new target from a probe of the old one")

	// Without a concurrent change the result is stored as before.
	runner.hook = nil
	resp, err := svc.RunDryRun(ctx, agentID, ownerID)
	require.NoError(t, err)
	assert.Equal(t, "pass", resp.Result)
	assert.Equal(t, "healthy", resp.Availability.Status)
}

// The endpoint change and the evidence reset commit together: if the reset
// fails, the Agent keeps its previous target and evidence.
func TestUpdateAgentRollsBackEndpointWhenEvidenceResetFails(t *testing.T) {
	pool := setupTestDB(t)
	svc := newTestService(t, pool)
	ctx := context.Background()
	ownerID := insertCreator(t, pool)
	slug := freshSlug("evidence-rollback")
	agentID := createEvidencedAgent(t, pool, svc, ownerID, slug)
	before := readAgentEvidence(t, pool, agentID)

	trigger := "evidence_reset_fail_" + uuid.NewString()[:8]
	_, err := pool.Exec(ctx, `
CREATE FUNCTION `+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'injected evidence reset failure';
END $$`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE UPDATE ON agent_skill_scores
FOR EACH ROW WHEN (OLD.agent_id = '`+agentID.String()+`'::uuid) EXECUTE FUNCTION `+trigger+`()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS `+trigger+` ON agent_skill_scores`)
		_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS `+trigger+`()`)
	})

	owned, err := svc.GetMyAgent(ctx, agentID, ownerID)
	require.NoError(t, err)
	req := updateRequestFrom(owned)
	req.EndpointURL = "https://example.com/agent/never-saved"
	_, err = svc.UpdateAgent(ctx, agentID, ownerID, req)
	assertHTTPStatus(t, err, http.StatusInternalServerError)

	var endpoint string
	require.NoError(t, pool.QueryRow(ctx, `SELECT endpoint_url FROM agents WHERE id = $1`, agentID).Scan(&endpoint))
	assert.Equal(t, owned.EndpointURL, endpoint)
	requireEvidenceIntact(t, before, readAgentEvidence(t, pool, agentID))
}
