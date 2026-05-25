package skill_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
	"github.com/kinzhi/openlinker-core/pkg/skill"
)

// 覆盖 Phase 2 缺口 3：benchmark 公开 GET 路径（docs/29 §四）。
// 复用 service_test.go 里的 setupSkillTestDB / insertSkillCreator / insertSkillAgent。

func TestBenchmarkService_ListBatchSummariesPublic_HiddenForDisabled(t *testing.T) {
	pool := setupSkillTestDB(t)
	svc := skill.NewService(pool)
	bench := skill.NewBenchmarkService(svc, nil, nil)
	creator := insertSkillCreator(t, pool)
	disabled := insertSkillAgent(t, pool, creator, "bench-pub-disabled", "disabled", 0)

	_, err := bench.ListBatchSummariesPublic(context.Background(), disabled, 10)
	requireHTTPStatus(t, err, 404)
}

func TestBenchmarkService_ListBatchSummariesPublic_HappyPath(t *testing.T) {
	pool := setupSkillTestDB(t)
	svc := skill.NewService(pool)
	bench := skill.NewBenchmarkService(svc, nil, nil)
	creator := insertSkillCreator(t, pool)
	agentID := insertSkillAgent(t, pool, creator, "bench-pub-list", "approved", 0)

	// 直接 SQL 插入两个 batch（绕过异步 runner，单测目的）。
	batch1 := uuid.New()
	batch2 := uuid.New()
	caseID := firstTestCaseID(t, pool, "content/translation")
	insertBenchmarkRun(t, pool, batch1, agentID, "content/translation", caseID, "success", ptrInt32(80), time.Now().Add(-1*time.Hour))
	insertBenchmarkRun(t, pool, batch2, agentID, "content/translation", caseID, "success", ptrInt32(60), time.Now().Add(-2*time.Hour))

	items, err := bench.ListBatchSummariesPublic(context.Background(), agentID, 10)
	require.NoError(t, err)
	require.Len(t, items, 2)
	// 最新 batch 在前
	require.Equal(t, batch1.String(), items[0].BatchID)
	require.Equal(t, int32(80), *items[0].AverageScore)
	require.Equal(t, int32(1), items[0].SuccessCount)
	require.Equal(t, int32(1), items[0].TotalCount)
}

func TestBenchmarkService_GetBatchDetailPublic_Sanitized(t *testing.T) {
	pool := setupSkillTestDB(t)
	svc := skill.NewService(pool)
	bench := skill.NewBenchmarkService(svc, nil, nil)
	creator := insertSkillCreator(t, pool)
	agentID := insertSkillAgent(t, pool, creator, "bench-pub-detail", "approved", 0)

	batch := uuid.New()
	caseID := firstTestCaseID(t, pool, "content/translation")
	insertBenchmarkRunWithExtras(t, pool, batch, agentID, "content/translation", caseID, "success",
		ptrInt32(90), `{"secret":"raw output"}`, "internal reasoning", time.Now().Add(-30*time.Minute))

	detail, err := bench.GetBatchDetailPublic(context.Background(), agentID, batch)
	require.NoError(t, err)
	require.Equal(t, batch.String(), detail.BatchID)
	require.Len(t, detail.Items, 1)
	require.Nil(t, detail.Items[0].JudgeReasoning, "JudgeReasoning 不应暴露给公开侧")

	// 二次序列化双重保险：JSON 里不能出现 raw_output / judge_reasoning 字符串。
	raw, err := json.Marshal(detail)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "judge_reasoning")
}

func TestBenchmarkService_GetBatchDetailPublic_NotOwned(t *testing.T) {
	// 一个 agent 的 batchID 不应能从另一个 agent 的路径取到。
	pool := setupSkillTestDB(t)
	svc := skill.NewService(pool)
	bench := skill.NewBenchmarkService(svc, nil, nil)
	creator := insertSkillCreator(t, pool)
	agentA := insertSkillAgent(t, pool, creator, "bench-pub-a", "approved", 0)
	agentB := insertSkillAgent(t, pool, creator, "bench-pub-b", "approved", 0)

	batch := uuid.New()
	caseID := firstTestCaseID(t, pool, "content/translation")
	insertBenchmarkRun(t, pool, batch, agentA, "content/translation", caseID, "success", ptrInt32(70), time.Now())

	_, err := bench.GetBatchDetailPublic(context.Background(), agentB, batch)
	requireHTTPStatus(t, err, 404)
}

// ─── helpers ────────────────────────────────────────────

func firstTestCaseID(t *testing.T, pool *pgxpool.Pool, skillID string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := pool.QueryRow(context.Background(),
		`SELECT id FROM skill_test_cases WHERE skill_id = $1 ORDER BY sort_order LIMIT 1`,
		skillID).Scan(&id)
	require.NoError(t, err)
	return id
}

func insertBenchmarkRun(t *testing.T, pool *pgxpool.Pool, batchID, agentID uuid.UUID, skillID string, caseID uuid.UUID, status string, score *int32, startedAt time.Time) {
	insertBenchmarkRunWithExtras(t, pool, batchID, agentID, skillID, caseID, status, score, "", "", startedAt)
}

func insertBenchmarkRunWithExtras(t *testing.T, pool *pgxpool.Pool, batchID, agentID uuid.UUID, skillID string, caseID uuid.UUID, status string, score *int32, rawOutput, judgeReasoning string, startedAt time.Time) {
	t.Helper()
	finishedAt := startedAt.Add(1 * time.Second)
	var raw []byte
	if rawOutput != "" {
		raw = []byte(rawOutput)
	}
	var reason *string
	if judgeReasoning != "" {
		reason = &judgeReasoning
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO agent_skill_benchmark_runs
		   (id, batch_id, agent_id, skill_id, test_case_id, status, score, raw_output, judge_reasoning, started_at, finished_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		uuid.New(), batchID, agentID, skillID, caseID, status, score, raw, reason, startedAt, finishedAt)
	require.NoError(t, err, "insert benchmark run")
}

func ptrInt32(v int32) *int32 { return &v }

// requireHTTPStatus 与 agent_test 包同名 helper 对齐口径，但本包没有引入 testify/assert 的等价物，
// 用 require + 类型断言确保失败时立即停。
func requireHTTPStatus(t *testing.T, err error, status int) {
	t.Helper()
	require.Error(t, err)
	herr, ok := err.(*httpx.HTTPError)
	require.Truef(t, ok, "expected *httpx.HTTPError, got %T (%v)", err, err)
	require.Equalf(t, status, herr.Status, "expected HTTP status %d, got %d (%s)", status, herr.Status, herr.Message)
}
