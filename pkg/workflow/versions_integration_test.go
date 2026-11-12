package workflow_test

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

func requireWorkflowErrorCode(t *testing.T, err error, status int, code string) {
	t.Helper()
	var httpErr *httpx.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, status, httpErr.Status)
	require.Equal(t, httpx.ErrorCode(code), httpErr.Code)
}

func okAgentFixture(t *testing.T) *engineFixture {
	return newEngineFixture(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		writeAgentOutput(w, map[string]interface{}{"ok": true})
	})
}

func renamedDefinition(f *engineFixture, key string) *workflow.UpdateWorkflowRequest {
	return &workflow.UpdateWorkflowRequest{
		Name:  "Engine workflow",
		Nodes: []workflow.WorkflowNodeRequest{{Key: key, Title: "Renamed", AgentID: f.agentID}},
	}
}

func TestWorkflowUpdateCreatesImmutableVersions(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	v1, err := f.svc.GetWorkflow(ctx, f.ownerID, f.workflowID)
	require.NoError(t, err)
	require.EqualValues(t, 1, v1.Version)
	require.Equal(t, "v1", v1.Semantics)

	_, err = f.svc.UpdateWorkflow(ctx, f.ownerID, f.workflowID, 0, renamedDefinition(f, "renamed"))
	requireWorkflowErrorCode(t, err, 428, "WORKFLOW_VERSION_CONFLICT")

	v2, err := f.svc.UpdateWorkflow(ctx, f.ownerID, f.workflowID, 1, renamedDefinition(f, "renamed"))
	require.NoError(t, err)
	require.EqualValues(t, 2, v2.Version)
	require.Equal(t, "renamed", v2.Nodes[0].Key)
	require.NotEqual(t, v1.Nodes[0].ID, v2.Nodes[0].ID, "a new version gets new node ids")

	_, err = f.svc.UpdateWorkflow(ctx, f.ownerID, f.workflowID, 1, renamedDefinition(f, "stale"))
	requireWorkflowErrorCode(t, err, http.StatusConflict, "WORKFLOW_VERSION_CONFLICT")

	versions, err := f.svc.ListWorkflowVersions(ctx, f.ownerID, f.workflowID)
	require.NoError(t, err)
	require.EqualValues(t, 2, versions.CurrentVersion)
	require.Len(t, versions.Items, 2)
	require.True(t, versions.Items[0].Current)
	require.NotEqual(t, versions.Items[0].DefinitionDigest, versions.Items[1].DefinitionDigest)

	old, err := f.svc.GetWorkflowVersion(ctx, f.ownerID, f.workflowID, 1)
	require.NoError(t, err)
	require.Equal(t, v1.Nodes[0].ID, old.Nodes[0].ID)
	require.Equal(t, "worker", old.Nodes[0].Key)

	// The projection follows the current version.
	var projected string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT node_key FROM workflow_nodes WHERE workflow_id = $1`, f.workflowID).Scan(&projected))
	require.Equal(t, "renamed", projected)
}

// Concurrent edits from the same version: exactly one wins, version numbers
// stay unique.
func TestWorkflowConcurrentUpdatesNumberVersionsOnce(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = f.svc.UpdateWorkflow(ctx, f.ownerID, f.workflowID, 1, renamedDefinition(f, "concurrent"))
		}(i)
	}
	wg.Wait()
	wins := 0
	for _, err := range errs {
		if err == nil {
			wins++
		} else {
			requireWorkflowErrorCode(t, err, http.StatusConflict, "WORKFLOW_VERSION_CONFLICT")
		}
	}
	require.Equal(t, 1, wins)
	versions, err := f.svc.ListWorkflowVersions(ctx, f.ownerID, f.workflowID)
	require.NoError(t, err)
	require.Len(t, versions.Items, 2)
}

// A queued run executes the version current when it was created, even after
// the workflow is edited; Retry executes the source run's version.
func TestWorkflowRunsExecuteTheirPinnedVersion(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	queued, err := f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	_, err = f.svc.UpdateWorkflow(ctx, f.ownerID, f.workflowID, 1, renamedDefinition(f, "renamed"))
	require.NoError(t, err)

	claimed, err := f.svc.ClaimAndRunPendingWorkflow(ctx)
	require.NoError(t, err)
	require.True(t, claimed)
	done, err := f.svc.GetWorkflowRun(ctx, f.ownerID, uuid.MustParse(queued.ID))
	require.NoError(t, err)
	require.Equal(t, "success", done.Status)
	require.EqualValues(t, 1, done.WorkflowVersion)
	require.Equal(t, "worker", done.Steps[0].NodeKey)

	current, err := f.svc.RunWorkflow(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 2, current.WorkflowVersion)
	require.Equal(t, "renamed", current.Steps[0].NodeKey)

	explicit, err := f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{Version: 1})
	require.NoError(t, err)
	got, err := f.svc.GetWorkflowRun(ctx, f.ownerID, uuid.MustParse(explicit.ID))
	require.NoError(t, err)
	require.EqualValues(t, 1, got.WorkflowVersion)
	_, err = f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{Version: 9})
	require.Error(t, err)
}

func TestArchivedWorkflowRefusesRunsAndVersions(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	archived, err := f.svc.SetWorkflowArchived(ctx, f.ownerID, f.workflowID, true)
	require.NoError(t, err)
	require.Equal(t, "archived", archived.Status)

	_, err = f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	requireWorkflowErrorCode(t, err, http.StatusConflict, "WORKFLOW_ARCHIVED")
	_, err = f.svc.RunWorkflow(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	requireWorkflowErrorCode(t, err, http.StatusConflict, "WORKFLOW_ARCHIVED")
	_, err = f.svc.UpdateWorkflow(ctx, f.ownerID, f.workflowID, 1, renamedDefinition(f, "renamed"))
	requireWorkflowErrorCode(t, err, http.StatusConflict, "WORKFLOW_ARCHIVED")

	_, err = f.svc.SetWorkflowArchived(ctx, f.ownerID, f.workflowID, false)
	require.NoError(t, err)
	_, err = f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
}

func TestDuplicateWorkflowCopiesTheCurrentVersion(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	_, err := f.svc.UpdateWorkflow(ctx, f.ownerID, f.workflowID, 1, renamedDefinition(f, "renamed"))
	require.NoError(t, err)
	copied, err := f.svc.DuplicateWorkflow(ctx, f.ownerID, f.workflowID, &workflow.DuplicateWorkflowRequest{Name: "Copy"})
	require.NoError(t, err)
	require.NotEqual(t, f.workflowID.String(), copied.ID)
	require.Equal(t, "Copy", copied.Name)
	require.EqualValues(t, 1, copied.Version)
	require.Equal(t, "renamed", copied.Nodes[0].Key)
}

// The contract hash of a version never changes; editing changes the current
// version's hash, and an explicit target version still validates to the old
// hash.
func TestExternalExecutionTargetVersionKeepsItsContract(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	insertWorkflowAgentCapability(t, f.pool, f.agentID, `{"type":"object"}`)
	first, err := f.svc.ValidateExternalExecutionTargetVersion(ctx, f.ownerID, f.workflowID, nil)
	require.NoError(t, err)
	require.True(t, first.Executable)
	require.NotEmpty(t, first.TargetVersionID)

	_, err = f.svc.UpdateWorkflow(ctx, f.ownerID, f.workflowID, 1, renamedDefinition(f, "renamed"))
	require.NoError(t, err)
	current, err := f.svc.ValidateExternalExecutionTargetVersion(ctx, f.ownerID, f.workflowID, nil)
	require.NoError(t, err)
	require.NotEqual(t, first.ContractHash, current.ContractHash)
	require.NotEqual(t, first.TargetVersionID, current.TargetVersionID)

	versionOne := uuid.MustParse(first.TargetVersionID)
	pinned, err := f.svc.ValidateExternalExecutionTargetVersion(ctx, f.ownerID, f.workflowID, &versionOne)
	require.NoError(t, err)
	require.Equal(t, first.ContractHash, pinned.ContractHash)
}
