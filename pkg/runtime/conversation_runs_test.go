package runtime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestConversationProjectionUsesAbsoluteOrdinalAndStableIdentity(t *testing.T) {
	t.Parallel()
	userID := uuid.New()
	agentID := uuid.New()
	root := "private-root-context"
	first := conversationProjectionFixture(userID, agentID, root, uuid.New(), nil, "task-1", "success")
	second := conversationProjectionFixture(userID, agentID, root, uuid.New(), &first.RunID, "task-2", "success")
	anchor := conversationProjectionFixture(userID, agentID, root, uuid.New(), &second.RunID, "task-3", "success")
	successor := conversationProjectionFixture(userID, agentID, root, uuid.New(), &anchor.RunID, "task-4", "running")
	anchor.Depth = 0
	successor.Depth = 1
	first.Depth = 2
	second.Depth = 1

	authorizedAnchor := anchor
	authorizedAnchor.RequestMetadata = nil
	response := buildConversationRunProjection(
		authorizedAnchor,
		[]conversationProjectionRun{anchor, successor},
		[]conversationProjectionRun{anchor, second, first},
	)
	require.True(t, response.Linear)
	require.Len(t, response.Items, 2)
	require.Equal(t, int32(3), *response.Items[0].ConversationOrdinal)
	require.Equal(t, int32(4), *response.Items[1].ConversationOrdinal)
	require.Equal(t, "restricted", response.Items[0].BrowserInteractionPolicy)
	require.Len(t, response.ConversationIdentitySHA256, 64)
	require.Len(t, response.Revision, 64)

	changed := successor
	changed.Status = "success"
	changedResponse := buildConversationRunProjection(
		anchor,
		[]conversationProjectionRun{anchor, changed},
		[]conversationProjectionRun{anchor, second, first},
	)
	require.Equal(t, response.ConversationIdentitySHA256, changedResponse.ConversationIdentitySHA256)
	require.NotEqual(t, response.Revision, changedResponse.Revision)

	raw, err := json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(raw), root)
	for _, prohibited := range []string{
		"browser_session", "attachment", "lease_id", "runtime_session",
		"input", "output", "message", "artifact", "credential",
	} {
		require.NotContains(t, string(raw), prohibited)
	}
}

func TestConversationProjectionOmitsUnprovableOrdinal(t *testing.T) {
	t.Parallel()
	userID := uuid.New()
	agentID := uuid.New()
	root := "root"
	missing := uuid.New()
	anchor := conversationProjectionFixture(userID, agentID, root, uuid.New(), &missing, "task-3", "running")
	anchor.Depth = 0

	response := buildConversationRunProjection(anchor, []conversationProjectionRun{anchor}, []conversationProjectionRun{anchor})
	require.True(t, response.Linear)
	require.Nil(t, response.Items[0].ConversationOrdinal)
}

func TestConversationProjectionFailsClosedOnBranchAndScopeDrift(t *testing.T) {
	t.Parallel()
	userID := uuid.New()
	agentID := uuid.New()
	root := "root"
	anchor := conversationProjectionFixture(userID, agentID, root, uuid.New(), nil, "task-1", "running")
	left := conversationProjectionFixture(userID, agentID, root, uuid.New(), &anchor.RunID, "task-2", "running")
	right := conversationProjectionFixture(userID, agentID, root, uuid.New(), &anchor.RunID, "task-3", "running")
	left.Depth, right.Depth = 1, 1

	branched := buildConversationRunProjection(
		anchor,
		[]conversationProjectionRun{anchor, left, right},
		[]conversationProjectionRun{anchor},
	)
	require.False(t, branched.Linear)
	require.Len(t, branched.Items, 1)

	drifted := left
	drifted.MappingUserID = uuid.New()
	driftedResponse := buildConversationRunProjection(
		anchor,
		[]conversationProjectionRun{anchor, drifted},
		[]conversationProjectionRun{anchor},
	)
	require.False(t, driftedResponse.Linear)
	require.Len(t, driftedResponse.Items, 1)
}

func TestConversationProjectionRejectsDuplicateTaskCycleAndOverLimit(t *testing.T) {
	t.Parallel()
	userID := uuid.New()
	agentID := uuid.New()
	root := "root"
	anchor := conversationProjectionFixture(userID, agentID, root, uuid.New(), nil, "task-1", "running")
	duplicateTask := conversationProjectionFixture(userID, agentID, root, uuid.New(), &anchor.RunID, "task-1", "running")
	duplicateTask.Depth = 1
	response := buildConversationRunProjection(
		anchor,
		[]conversationProjectionRun{anchor, duplicateTask},
		[]conversationProjectionRun{anchor},
	)
	require.False(t, response.Linear)
	require.Len(t, response.Items, 1)

	cycle := anchor
	cycle.ParentRunID = &duplicateTask.RunID
	cycle.Depth = 2
	cycle.Cycle = true
	response = buildConversationRunProjection(
		anchor,
		[]conversationProjectionRun{anchor, duplicateTask, cycle},
		[]conversationProjectionRun{anchor},
	)
	require.False(t, response.Linear)

	chain := []conversationProjectionRun{anchor}
	parent := anchor.RunID
	for index := 1; index <= conversationProjectionLimit; index++ {
		parentID := parent
		next := conversationProjectionFixture(
			userID, agentID, root, uuid.New(), &parentID,
			"task-"+strings.Repeat("x", index), "running",
		)
		next.Depth = int32(index)
		chain = append(chain, next)
		parent = next.RunID
	}
	response = buildConversationRunProjection(anchor, chain, []conversationProjectionRun{anchor})
	require.False(t, response.Linear)
	require.Len(t, response.Items, conversationProjectionLimit)
}

func TestConversationProjectionWithoutEligibleMappingIsSingleRun(t *testing.T) {
	t.Parallel()
	row := conversationProjectionRun{
		RunID: uuid.New(), RunUserID: uuid.New(), RunAgentID: uuid.New(),
		Status: "timeout", StartedAt: time.Now().UTC(),
	}
	response := buildConversationRunProjection(row, nil, nil)
	require.False(t, response.Linear)
	require.Empty(t, response.ConversationIdentitySHA256)
	require.Len(t, response.Items, 1)
	require.Nil(t, response.Items[0].ConversationOrdinal)
	require.Equal(t, "timeout", response.Items[0].Status)
}

func conversationProjectionFixture(
	userID, agentID uuid.UUID,
	root string,
	runID uuid.UUID,
	parentRunID *uuid.UUID,
	taskID, status string,
) conversationProjectionRun {
	metadata, err := json.Marshal(map[string]any{
		"_openlinker_runtime_authority": persistedRuntimeAuthority{
			ExecutionProfile:                   runtimeExecutionProfileBrowser,
			BrowserInteractionPolicy:           "restricted",
			BrowserInteractionPolicyGeneration: 1,
			BrowserMutationOrigins:             []string{},
			BrowserMutationOriginsSHA256:       "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
		},
	})
	if err != nil {
		panic(err)
	}
	return conversationProjectionRun{
		Mapped: true, RunID: runID, MappingUserID: userID, MappingAgentID: agentID,
		RunUserID: userID, RunAgentID: agentID, RootContextID: root,
		ParentRunID: parentRunID, ProtocolTaskID: taskID, Source: conversationA2ASource,
		Status: status, RequestMetadata: metadata, StartedAt: time.Unix(1_800_000_000, 0).UTC(),
	}
}
