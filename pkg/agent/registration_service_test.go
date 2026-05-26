package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/agent"
)

// 覆盖 Phase 2 缺口 1：Bootstrap Token → Agent 自注册流程。
// 复用 testhelpers_test.go / market_service_test.go 已有的 helpers：
//   setupTestDB / insertCreatorUser / insertNonCreatorUser / assertHTTPStatus。

func TestRegistrationService_MintBootstrapToken_Defaults(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Bootstrap Creator")

	svc := agent.NewRegistrationService(pool)
	resp, err := svc.MintBootstrapToken(context.Background(), creatorID, &agent.CreateBootstrapTokenRequest{
		Label: "CI 调用 token",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.PlaintextToken)
	require.True(t, strings.HasPrefix(resp.PlaintextToken, "br_live_"), "明文 token 必须以 br_live_ 开头")
	require.Equal(t, int32(1), resp.MaxAgents, "缺省 max_agents=1")
	require.Equal(t, int32(0), resp.UsedCount)
	require.NotEmpty(t, resp.Prefix)
	require.Equal(t, resp.Prefix, resp.PlaintextToken[:12])
}

func TestRegistrationService_MintBootstrapToken_NonCreatorRejected(t *testing.T) {
	pool := setupTestDB(t)
	userID := insertNonCreatorUser(t, pool)

	svc := agent.NewRegistrationService(pool)
	_, err := svc.MintBootstrapToken(context.Background(), userID, &agent.CreateBootstrapTokenRequest{
		Label: "无权 token",
	})
	assertHTTPStatus(t, err, 403)
}

func TestRegistrationService_RegisterAgentViaBootstrap_HappyPath(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Bootstrap Creator")
	ctx := context.Background()

	svc := agent.NewRegistrationService(pool)
	minted, err := svc.MintBootstrapToken(ctx, creatorID, &agent.CreateBootstrapTokenRequest{
		Label: "登记 token",
	})
	require.NoError(t, err)

	resp, err := svc.RegisterAgentViaBootstrap(ctx, &agent.RegisterAgentViaBootstrapRequest{
		BootstrapToken:    minted.PlaintextToken,
		Name:              "Self Registered Translator",
		Description:       "中英互译，自注册测试用例",
		EndpointURL:       "https://example.com/agent/translator",
		PricePerCallCents: 50,
		Tags:              []string{"content/translation"},
		SkillIDs:          []string{"content/translation", "ai/agent-orchestration", "content/translation"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Agent.ID)
	require.NotEmpty(t, resp.Agent.Slug, "未传 slug 时应自动派生")
	require.True(t, strings.HasPrefix(resp.Agent.Slug, "self-registered-translator-"),
		"slug 应从 name 派生，得到 %q", resp.Agent.Slug)
	require.Equal(t, "approved", resp.Agent.Status, "Bootstrap 注册的 Agent 默认公开")
	require.NotEmpty(t, resp.RuntimeToken.PlaintextToken)
	require.True(t, strings.HasPrefix(resp.RuntimeToken.PlaintextToken, "rt_live_"))
	require.Equal(t, int32(1), resp.UsedCount)
	require.Equal(t, int32(1), resp.MaxAgents)

	rows, err := pool.Query(ctx, `SELECT skill_id FROM agent_skills WHERE agent_id = $1 ORDER BY skill_id`, uuid.MustParse(resp.Agent.ID))
	require.NoError(t, err)
	defer rows.Close()
	var declared []string
	for rows.Next() {
		var skillID string
		require.NoError(t, rows.Scan(&skillID))
		declared = append(declared, skillID)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"ai/agent-orchestration", "content/translation"}, declared)
}

func TestRegistrationService_RegisterAgentViaBootstrap_ExhaustedToken(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Bootstrap Creator")
	ctx := context.Background()

	svc := agent.NewRegistrationService(pool)
	minted, err := svc.MintBootstrapToken(ctx, creatorID, &agent.CreateBootstrapTokenRequest{
		Label:     "单次 token",
		MaxAgents: 1,
	})
	require.NoError(t, err)

	_, err = svc.RegisterAgentViaBootstrap(ctx, &agent.RegisterAgentViaBootstrapRequest{
		BootstrapToken:    minted.PlaintextToken,
		Name:              "First Agent",
		Description:       "第一次消费 token",
		EndpointURL:       "https://example.com/agent/first",
		PricePerCallCents: 50,
		Tags:              []string{"content/translation"},
	})
	require.NoError(t, err)

	_, err = svc.RegisterAgentViaBootstrap(ctx, &agent.RegisterAgentViaBootstrapRequest{
		BootstrapToken:    minted.PlaintextToken,
		Name:              "Second Agent",
		Description:       "第二次应当被拒绝",
		EndpointURL:       "https://example.com/agent/second",
		PricePerCallCents: 50,
		Tags:              []string{"content/translation"},
	})
	assertHTTPStatus(t, err, 401)
}

func TestRegistrationService_RegisterAgentViaBootstrap_ExpiredToken(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Bootstrap Creator")
	ctx := context.Background()

	svc := agent.NewRegistrationService(pool)
	minted, err := svc.MintBootstrapToken(ctx, creatorID, &agent.CreateBootstrapTokenRequest{
		Label:            "过期 token",
		ExpiresInMinutes: 5,
	})
	require.NoError(t, err)

	// 把 expires_at 拨回 1 小时前，模拟过期。
	_, err = pool.Exec(ctx,
		`UPDATE agent_registration_tokens SET expires_at = NOW() - INTERVAL '1 hour' WHERE id = $1`,
		uuid.MustParse(minted.ID),
	)
	require.NoError(t, err)

	_, err = svc.RegisterAgentViaBootstrap(ctx, &agent.RegisterAgentViaBootstrapRequest{
		BootstrapToken:    minted.PlaintextToken,
		Name:              "Late Agent",
		Description:       "应当被过期拦截",
		EndpointURL:       "https://example.com/agent/late",
		PricePerCallCents: 50,
		Tags:              []string{"content/translation"},
	})
	assertHTTPStatus(t, err, 401)
}

func TestRegistrationService_RegisterAgentViaBootstrap_RevokedToken(t *testing.T) {
	pool := setupTestDB(t)
	creatorID := insertCreatorUser(t, pool, "Bootstrap Creator")
	ctx := context.Background()

	svc := agent.NewRegistrationService(pool)
	minted, err := svc.MintBootstrapToken(ctx, creatorID, &agent.CreateBootstrapTokenRequest{
		Label: "撤销 token",
	})
	require.NoError(t, err)

	require.NoError(t, svc.RevokeBootstrapToken(ctx, creatorID, uuid.MustParse(minted.ID)))

	_, err = svc.RegisterAgentViaBootstrap(ctx, &agent.RegisterAgentViaBootstrapRequest{
		BootstrapToken:    minted.PlaintextToken,
		Name:              "Revoked Agent",
		Description:       "应当被撤销拦截",
		EndpointURL:       "https://example.com/agent/revoked",
		PricePerCallCents: 50,
		Tags:              []string{"content/translation"},
	})
	assertHTTPStatus(t, err, 401)
}

func TestRegistrationService_RegisterAgentViaBootstrap_BadToken(t *testing.T) {
	pool := setupTestDB(t)
	insertCreatorUser(t, pool, "Bootstrap Creator")

	// 64 个 hex 字符 + br_live_ 前缀 = 与真实 token 长度一致，但内容是假的 → 401。
	bogus := "br_live_" + strings.Repeat("0", 64)

	svc := agent.NewRegistrationService(pool)
	_, err := svc.RegisterAgentViaBootstrap(context.Background(), &agent.RegisterAgentViaBootstrapRequest{
		BootstrapToken:    bogus,
		Name:              "Should Fail",
		Description:       "格式合法但内容是假的",
		EndpointURL:       "https://example.com/agent/x",
		PricePerCallCents: 50,
		Tags:              []string{"content/translation"},
	})
	assertHTTPStatus(t, err, 401)
}

func TestRegistrationService_ListBootstrapTokens_OnlyOwn(t *testing.T) {
	pool := setupTestDB(t)
	a := insertCreatorUser(t, pool, "Creator A")
	b := insertCreatorUser(t, pool, "Creator B")
	ctx := context.Background()

	svc := agent.NewRegistrationService(pool)
	_, err := svc.MintBootstrapToken(ctx, a, &agent.CreateBootstrapTokenRequest{Label: "A token"})
	require.NoError(t, err)
	_, err = svc.MintBootstrapToken(ctx, b, &agent.CreateBootstrapTokenRequest{Label: "B token"})
	require.NoError(t, err)

	items, err := svc.ListBootstrapTokens(ctx, a)
	require.NoError(t, err)
	require.Len(t, items, 1, "List 只返回当前 creator 自己的 token")
	require.Equal(t, "A token", items[0].Label)
	require.Empty(t, items[0].PlaintextToken, "List 时不返回明文")
}
