package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestRuntimeNodeControlledUpgradeAdminHTTP(t *testing.T) {
	pool, f, svc, actor := upgradeFixture(t, true)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE users SET is_admin=true WHERE id=$1`, actor)
	require.NoError(t, err)
	adminJWT, err := auth.GenerateToken(actor.String(), newTestConfig().JWTSecret, time.Hour)
	require.NoError(t, err)
	userJWT, err := auth.GenerateToken(insertCreator(t, pool).String(), newTestConfig().JWTSecret, time.Hour)
	require.NoError(t, err)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	runtime.NewHandler(svc).RegisterAdmin(e.Group("/api/v1"), auth.JWTMiddlewareWithUserStatus(newTestConfig().JWTSecret, auth.NewDBUserStatusChecker(pool)), auth.AdminMiddleware(db.New(pool)))
	url := "/api/v1/admin/runtime/nodes/" + f.nodeID.String() + "/upgrade"
	r := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	raw, err := json.Marshal(r)
	require.NoError(t, err)
	request := func(method, token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, url, strings.NewReader(body))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		if token != "" {
			req.Header.Set(echo.HeaderAuthorization, "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, test := range []struct {
			token  string
			status int
		}{{"", 401}, {"ol_agent_synthetic_runtime_token", 401}, {"ol_user_synthetic_platform_token", 401}, {userJWT, 403}} {
			rec := request(method, test.token, string(raw))
			require.Equal(t, test.status, rec.Code, rec.Body.String())
		}
	}
	rec := request(http.MethodGet, adminJWT, "")
	require.Equal(t, 200, rec.Code, rec.Body.String())
	var status runtime.RuntimeNodeUpgradeStatus
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	require.True(t, status.Eligible)
	for _, body := range []string{
		strings.TrimSuffix(string(raw), "}") + `,"actor_id":"` + actor.String() + `"}`,
		strings.TrimSuffix(string(raw), "}") + `,"expected_revision":0}`,
		strings.Replace(string(raw), `"expected_revision":0,`, "", 1),
		strings.Replace(string(raw), `"expected_revision":0`, `"expected_revision":null`, 1),
		string(raw) + ` {}`, `[]`,
		strings.Replace(string(raw), upgradeVersionB, "different-product/0.2.0", 1),
		strings.Replace(string(raw), `"version_change"`, `"restart_after_drain"`, 1),
	} {
		rec = request(http.MethodPost, adminJWT, body)
		require.Equal(t, 400, rec.Code, rec.Body.String())
	}
	rec = request(http.MethodPost, adminJWT, string(raw))
	require.Equal(t, 200, rec.Code, rec.Body.String())
	var persistedActor string
	require.NoError(t, pool.QueryRow(ctx, `SELECT actor_id::text FROM runtime_node_upgrade_operations WHERE operation_id=$1`, r.OperationID).Scan(&persistedActor))
	require.Equal(t, actor.String(), persistedActor)
}
