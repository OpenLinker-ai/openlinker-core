package skill_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skill"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestCapabilityDetailBeyondFirstPage(t *testing.T) {
	pool := setupSkillTestDB(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO skills(id,category,name,description,sort_order) SELECT 'review/'||n,'review','Review '||n,'Test capability',10000+n FROM generate_series(1,205) n`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM skills WHERE category='review'`) })
	service := skill.NewService(pool)
	first, err := service.ListPage(ctx, "", "", "order", "en", 1, 200)
	require.NoError(t, err)
	require.Greater(t, first.Total, int64(200))
	for _, item := range first.Items {
		require.NotEqual(t, "review/205", item.ID)
	}
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	skill.NewHandler(service, pool).Register(e.Group("/api/v1"))
	// Register both production handlers to exercise static/dynamic route matching.
	skill.NewBenchmarkHandler(skill.NewBenchmarkService(service, nil, nil)).Register(e.Group("/api/v1"))
	for _, path := range []string{"/api/v1/skills/review/205", "/api/v1/skills/review%2F205"} {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, 200, rec.Code, rec.Body.String())
		var item skill.SkillItem
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &item))
		require.Equal(t, "review/205", item.ID)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/skills/review/missing", nil))
	require.Equal(t, 404, rec.Code)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/skills/review/205/top-agents", nil))
	require.Equal(t, 200, rec.Code)
	require.JSONEq(t, `{"items":[]}`, rec.Body.String())
}
