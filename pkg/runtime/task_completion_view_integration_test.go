package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/admin"
	"github.com/OpenLinker-ai/openlinker-core/pkg/task"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const taskEchoToken = "seller-endpoint-token"

func TestTaskCompletionSummaryIsFilteredForViewer(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	svc := newTestService(t, pool)
	buyer := insertRuntimeUser(t, pool)
	creator := insertCreator(t, pool)
	tasks := task.NewService(pool, nil, nil)

	runTask := func(user uuid.UUID, output func(token string) map[string]any) uuid.UUID {
		t.Helper()
		agent := insertAgent(t, pool, creator, startMockEndpointForService(t, svc, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"output": output(r.Header.Get("X-Openlinker-Token"))})
		}), 0, "approved")
		setAgentEndpointToken(t, pool, agent, taskEchoToken)
		var taskID uuid.UUID
		require.NoError(t, pool.QueryRow(ctx, `INSERT INTO task_queries (user_id, query, parsed_skills, mcp_tools, recommended_agent_ids, chosen_agent_id, chosen_at)
 VALUES ($1,'task',ARRAY[]::text[],ARRAY[]::text[],ARRAY[$2]::uuid[],$2,NOW()) RETURNING id`, user, agent).Scan(&taskID))
		request := makeRunReq(agent, map[string]any{"q": "x"})
		request.Metadata = map[string]any{"task_id": taskID.String()}
		_, err := svc.Run(ctx, user, request, "")
		require.NoError(t, err)
		return taskID
	}
	read := func(user, taskID uuid.UUID) (string, bool) {
		t.Helper()
		detail, err := tasks.GetByID(ctx, taskID, user)
		require.NoError(t, err)
		require.NotNil(t, detail.CompletionSummary)
		page, err := tasks.ListMinePage(ctx, user, "", "", "", 1, 50)
		require.NoError(t, err)
		found := false
		for _, item := range page.Items {
			if item.ID == taskID.String() {
				found = true
				assert.Equal(t, *detail.CompletionSummary, *item.CompletionSummary, "list and detail must agree")
				assert.Equal(t, detail.CompletionSummaryRedacted, item.CompletionSummaryRedacted)
			}
		}
		require.True(t, found)
		return *detail.CompletionSummary, detail.CompletionSummaryRedacted
	}
	echo := func(token string) map[string]any {
		return map[string]any{
			"origin":  "203.0.113.7",
			"headers": map[string]any{"X-Forwarded-For": []string{"203.0.113.7"}, "X-Openlinker-Token": []string{token}},
			"note":    "signed with " + token,
		}
	}

	t.Run("buyer reading an echo", func(t *testing.T) {
		summary, redacted := read(buyer, runTask(buyer, echo))
		assert.True(t, redacted)
		assert.NotContains(t, summary, taskEchoToken)
		assert.NotContains(t, summary, "203.0.113.7")
		assert.NotContains(t, summary, "X-Forwarded-For")
		assert.Contains(t, summary, "signed with [redacted]")
	})
	t.Run("creator reads the original", func(t *testing.T) {
		summary, redacted := read(creator, runTask(creator, echo))
		assert.False(t, redacted)
		assert.Contains(t, summary, taskEchoToken)
	})
	t.Run("token cut by the summary limit", func(t *testing.T) {
		summary, redacted := read(buyer, runTask(buyer, func(token string) map[string]any {
			return map[string]any{"message": strings.Repeat("x", 1990) + " " + token}
		}))
		assert.True(t, redacted)
		assert.NotContains(t, summary, taskEchoToken[:5])
	})
	t.Run("business summary stays", func(t *testing.T) {
		summary, redacted := read(buyer, runTask(buyer, func(string) map[string]any {
			return map[string]any{"summary": "季度营收增长 12%", "rows": 3}
		}))
		assert.False(t, redacted)
		assert.Equal(t, "季度营收增长 12%", summary)
	})
	t.Run("stored summary is not rewritten", func(t *testing.T) {
		taskID := runTask(buyer, echo)
		_, _ = read(buyer, taskID)
		var stored string
		require.NoError(t, pool.QueryRow(ctx, `SELECT completion_summary FROM task_queries WHERE id=$1`, taskID).Scan(&stored))
		assert.Contains(t, stored, taskEchoToken)
	})
	t.Run("admin reads the filtered summary", func(t *testing.T) {
		ownTask := runTask(creator, echo)
		list, err := admin.NewService(pool).ListTasks(ctx, "", "", 50, 0)
		require.NoError(t, err)
		seen := false
		for _, item := range list.Items {
			raw, err := json.Marshal(item)
			require.NoError(t, err)
			assert.NotContains(t, string(raw), taskEchoToken)
			if item.ID == ownTask.String() {
				seen = true
				assert.True(t, item.CompletionSummaryRedacted)
			}
		}
		assert.True(t, seen)
	})
}
