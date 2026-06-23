package agent_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
)

// 覆盖 Phase 2 缺口 2：审批 CRUD（docs/29 §3.4）。
// 复用 testhelpers_test.go 中的 setupTestDB / insertCreatorUser / createApprovedAgent / assertHTTPStatus。

func TestApprovalService_Create_RequiresOwnership(t *testing.T) {
	pool := setupTestDB(t)
	creatorA := insertCreatorUser(t, pool, "A")
	creatorB := insertCreatorUser(t, pool, "B")
	agentB := createApprovedAgent(t, pool, creatorB, "approval-foreign")

	svc := agent.NewApprovalService(pool, nil)
	_, err := svc.CreateApproval(context.Background(), creatorA, &agent.CreateApprovalRequest{
		AgentID: agentB.String(),
		Action:  "set-visibility-public",
	})
	// 不属于 A → 404
	assertHTTPStatus(t, err, 404)
}

func TestApprovalService_CreateRejectsMalformedAgentIDAndMissingApproval(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Malformed")
	svc := agent.NewApprovalService(pool, nil)

	_, err := svc.CreateApproval(context.Background(), creator, &agent.CreateApprovalRequest{
		AgentID: "not-a-uuid",
		Action:  "x",
	})
	assertHTTPStatus(t, err, 400)

	_, err = svc.GetApproval(context.Background(), creator, uuid.New())
	assertHTTPStatus(t, err, 404)
}

func TestApprovalService_CreateAndList_OwnerOnly(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Owner")
	otherCreator := insertCreatorUser(t, pool, "Other")
	agentID := createApprovedAgent(t, pool, creator, "approval-list")

	svc := agent.NewApprovalService(pool, nil)
	created, err := svc.CreateApproval(context.Background(), creator, &agent.CreateApprovalRequest{
		AgentID: agentID.String(),
		Action:  "set-visibility-public",
		Payload: map[string]interface{}{"visibility": "public"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)
	require.Equal(t, "pending", created.Status)
	require.NotEmpty(t, created.ApprovalURL)
	require.NotEmpty(t, created.ApprovalURLSlug)

	own, err := svc.ListApprovals(context.Background(), creator)
	require.NoError(t, err)
	require.Len(t, own, 1)
	require.Equal(t, "pending", own[0].Status)

	other, err := svc.ListApprovals(context.Background(), otherCreator)
	require.NoError(t, err)
	require.Empty(t, other, "不同 creator 不应看到他人 agent 的审批")
}

func TestApprovalService_ConfirmTransitionsStatus(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Confirmer")
	agentID := createApprovedAgent(t, pool, creator, "approval-confirm")
	ctx := context.Background()

	svc := agent.NewApprovalService(pool, nil)
	created, err := svc.CreateApproval(ctx, creator, &agent.CreateApprovalRequest{
		AgentID: agentID.String(),
		Action:  "request-certification",
	})
	require.NoError(t, err)
	approvalID := uuid.MustParse(created.ID)

	require.NoError(t, svc.ConfirmApproval(ctx, creator, approvalID, "looks good"))

	got, err := svc.GetApproval(ctx, creator, approvalID)
	require.NoError(t, err)
	require.Equal(t, "confirmed", got.Status)
	require.NotNil(t, got.DecidedAt)
	require.NotNil(t, got.DecisionNote)
	require.Equal(t, "looks good", *got.DecisionNote)
	var certification string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT certification_status FROM agents WHERE id=$1`, agentID).Scan(&certification))
	require.Equal(t, "pending", certification)
}

func TestApprovalService_ConfirmPublicVisibilityAppliesAction(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Visibility Confirmer")
	agentID := createApprovedAgent(t, pool, creator, "approval-public")
	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE agents SET visibility='private' WHERE id=$1`, agentID)
	require.NoError(t, err)

	svc := agent.NewApprovalService(pool, nil)
	created, err := svc.CreateApproval(ctx, creator, &agent.CreateApprovalRequest{
		AgentID: agentID.String(),
		Action:  "set_visibility=public",
	})
	require.NoError(t, err)
	require.NoError(t, svc.ConfirmApproval(ctx, creator, uuid.MustParse(created.ID), "publish"))

	var visibility string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT visibility FROM agents WHERE id=$1`, agentID).Scan(&visibility))
	require.Equal(t, "public", visibility)
}

func TestApprovalService_RejectTransitionsStatus(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Rejecter")
	agentID := createApprovedAgent(t, pool, creator, "approval-reject")
	ctx := context.Background()

	svc := agent.NewApprovalService(pool, nil)
	created, err := svc.CreateApproval(ctx, creator, &agent.CreateApprovalRequest{
		AgentID: agentID.String(),
		Action:  "set-visibility-public",
	})
	require.NoError(t, err)
	approvalID := uuid.MustParse(created.ID)

	require.NoError(t, svc.RejectApproval(ctx, creator, approvalID, "scope insufficient"))

	got, err := svc.GetApproval(ctx, creator, approvalID)
	require.NoError(t, err)
	require.Equal(t, "rejected", got.Status)
}

func TestApprovalService_ConfirmFailsAfterDecision(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Twice")
	agentID := createApprovedAgent(t, pool, creator, "approval-twice")
	ctx := context.Background()

	svc := agent.NewApprovalService(pool, nil)
	created, err := svc.CreateApproval(ctx, creator, &agent.CreateApprovalRequest{
		AgentID: agentID.String(),
		Action:  "x",
	})
	require.NoError(t, err)
	approvalID := uuid.MustParse(created.ID)

	require.NoError(t, svc.ConfirmApproval(ctx, creator, approvalID, ""))
	// 第二次：已 confirmed → 409
	err = svc.ConfirmApproval(ctx, creator, approvalID, "")
	assertHTTPStatus(t, err, 409)
}

func TestApprovalService_RejectExpiredApprovalConflicts(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Reject Expired")
	agentID := createApprovedAgent(t, pool, creator, "approval-reject-expired")
	ctx := context.Background()

	svc := agent.NewApprovalService(pool, nil)
	created, err := svc.CreateApproval(ctx, creator, &agent.CreateApprovalRequest{
		AgentID:          agentID.String(),
		Action:           "x",
		ExpiresInMinutes: 5,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		`UPDATE agent_action_approval_requests SET expires_at = NOW() - INTERVAL '1 hour' WHERE id = $1`,
		uuid.MustParse(created.ID))
	require.NoError(t, err)

	err = svc.RejectApproval(ctx, creator, uuid.MustParse(created.ID), "too late")
	assertHTTPStatus(t, err, 409)

	got, err := svc.GetApproval(ctx, creator, uuid.MustParse(created.ID))
	require.NoError(t, err)
	require.Equal(t, "pending", got.Status)
}

func TestApprovalService_SweepExpires(t *testing.T) {
	pool := setupTestDB(t)
	creator := insertCreatorUser(t, pool, "Expirer")
	agentID := createApprovedAgent(t, pool, creator, "approval-expire")
	ctx := context.Background()

	svc := agent.NewApprovalService(pool, nil)
	created, err := svc.CreateApproval(ctx, creator, &agent.CreateApprovalRequest{
		AgentID:          agentID.String(),
		Action:           "x",
		ExpiresInMinutes: 5,
	})
	require.NoError(t, err)

	// 把 expires_at 拨回过去
	_, err = pool.Exec(ctx,
		`UPDATE agent_action_approval_requests SET expires_at = NOW() - INTERVAL '1 hour' WHERE id = $1`,
		uuid.MustParse(created.ID))
	require.NoError(t, err)

	affected, err := svc.SweepExpiredApprovals(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	got, err := svc.GetApproval(ctx, creator, uuid.MustParse(created.ID))
	require.NoError(t, err)
	require.Equal(t, "expired", got.Status)

	// 已 expired 后再 confirm → 409
	err = svc.ConfirmApproval(ctx, creator, uuid.MustParse(created.ID), "")
	assertHTTPStatus(t, err, 409)
}
