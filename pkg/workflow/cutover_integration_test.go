package workflow_test

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

func cutoverBlockerCodes(report workflow.CutoverReport) []string {
	codes := make([]string, 0, len(report.Blockers))
	for _, blocker := range report.Blockers {
		codes = append(codes, blocker.Code)
	}
	return codes
}

func TestWorkflowCutoverPreflightVerifyAndReopen(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	f.release()

	// Admission open and a pending run: both phases are blocked.
	pending, err := f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	report, err := workflow.PreflightWorkflowCutover(ctx, f.pool)
	require.ErrorIs(t, err, workflow.ErrWorkflowCutoverBlocked)
	require.False(t, report.Ready)
	require.ElementsMatch(t, []string{
		workflow.CutoverBlockerAdmissionOpen, workflow.CutoverBlockerNonterminalRuns,
	}, cutoverBlockerCodes(report))

	// Drain: switch admission and cancel the pending run as an administrator.
	f.setAdmission(t, workflow.WorkflowAdmissionDraining)
	_, err = f.svc.AdminCancelWorkflowRun(ctx, f.adminID, uuid.MustParse(pending.ID))
	require.NoError(t, err)
	report, err = workflow.PreflightWorkflowCutover(ctx, f.pool)
	require.NoError(t, err)
	require.True(t, report.Ready)
	report, err = workflow.VerifyWorkflowCutover(ctx, f.pool)
	require.NoError(t, err)
	require.True(t, report.Ready)

	// A launch that lost its step link without being a legacy orphan blocks
	// verify, and therefore reopen.
	var nodeID uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT id FROM workflow_nodes WHERE workflow_id = $1`, f.workflowID).Scan(&nodeID))
	sum := sha256.Sum256([]byte(pending.ID))
	_, err = f.pool.Exec(ctx, `
		INSERT INTO workflow_step_launches (
		    workflow_run_id, workflow_node_id, node_key, launch_token,
		    idempotency_key_hash, creation_fingerprint, state
		) VALUES ($1, $2, 'worker', $3, $4, $4, 'invalidated')`,
		uuid.MustParse(pending.ID), nodeID, uuid.New(), sum[:])
	require.NoError(t, err)
	report, err = workflow.VerifyWorkflowCutover(ctx, f.pool)
	require.ErrorIs(t, err, workflow.ErrWorkflowCutoverBlocked)
	require.Equal(t, []string{workflow.CutoverBlockerUnlinkedLaunches}, cutoverBlockerCodes(report))
	report, err = workflow.ReopenWorkflowAdmission(ctx, f.pool, "")
	require.ErrorIs(t, err, workflow.ErrWorkflowCutoverBlocked)
	state, err := f.svc.GetWorkflowAdmission(ctx)
	require.NoError(t, err)
	require.Equal(t, workflow.WorkflowAdmissionDraining, state.State)

	_, err = f.pool.Exec(ctx, `UPDATE workflow_step_launches SET legacy_orphan = TRUE WHERE workflow_run_id = $1`, uuid.MustParse(pending.ID))
	require.NoError(t, err)
	report, err = workflow.ReopenWorkflowAdmission(ctx, f.pool, "release verified in test")
	require.NoError(t, err)
	require.True(t, report.Ready)
	require.Equal(t, workflow.WorkflowAdmissionOpen, report.AdmissionState)
	require.EqualValues(t, 1, report.LegacyOrphanLaunches)
	state, err = f.svc.GetWorkflowAdmission(ctx)
	require.NoError(t, err)
	require.Equal(t, workflow.WorkflowAdmissionOpen, state.State)
	require.Equal(t, "release verified in test", state.Reason)
}
