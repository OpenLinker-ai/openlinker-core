package a2a

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

type runtimeProxyIdentityResolverStub struct {
	agent   db.Agent
	err     error
	userErr error
	checked *[]uuid.UUID
}

func (s runtimeProxyIdentityResolverStub) GetAgentByID(context.Context, uuid.UUID) (db.Agent, error) {
	return s.agent, s.err
}

func (s runtimeProxyIdentityResolverStub) EnsureUserEnabled(_ context.Context, userID uuid.UUID) error {
	if s.checked != nil {
		*s.checked = append(*s.checked, userID)
	}
	return s.userErr
}

func TestAgentRuntimeProxyMiddlewareDerivesCreatorAndSlug(t *testing.T) {
	agentID := uuid.New()
	creatorID := uuid.New()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/a2a-proxy/agents/self-reported", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetPath("/api/v1/agent-runtime/a2a-proxy/agents/:slug")
	c.SetParamNames("slug")
	c.SetParamValues("self-reported")

	authenticator := runtimeProxyAuthenticatorFunc(func(echo.Context) (runtime.AuthenticatedRuntimePrincipal, *runtime.RuntimeTransportError) {
		return runtime.AuthenticatedRuntimePrincipal{AgentID: agentID, CredentialID: uuid.New()}, nil
	})
	middleware := AgentRuntimeProxyMiddleware(authenticator, runtimeProxyIdentityResolverStub{agent: db.Agent{
		ID: agentID, CreatorID: creatorID, Slug: "server-owned-slug", LifecycleStatus: "active", Visibility: "private",
	}})

	err := middleware(func(c echo.Context) error {
		require.Equal(t, "server-owned-slug", c.Param("slug"))
		require.Equal(t, agentID, c.Get(a2aTargetAgentIDContextKey))
		require.Equal(t, creatorID.String(), httpx.UserIDFrom(c))
		require.Equal(t, auth.AuthMethodJWT, httpx.AuthMethodFrom(c))
		target, ok := agentRuntimeProxyTargetFromContext(c.Request().Context())
		require.True(t, ok)
		require.Equal(t, "private", target.Visibility)
		return c.NoContent(http.StatusNoContent)
	})(c)

	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, rec.Code)
}

func TestAgentRuntimeProxyMiddlewareRejectsInactiveAgent(t *testing.T) {
	agentID := uuid.New()
	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), rec)
	authenticator := runtimeProxyAuthenticatorFunc(func(echo.Context) (runtime.AuthenticatedRuntimePrincipal, *runtime.RuntimeTransportError) {
		return runtime.AuthenticatedRuntimePrincipal{AgentID: agentID, CredentialID: uuid.New()}, nil
	})
	middleware := AgentRuntimeProxyMiddleware(authenticator, runtimeProxyIdentityResolverStub{agent: db.Agent{
		ID: agentID, CreatorID: uuid.New(), Slug: "inactive-agent", LifecycleStatus: "disabled",
	}})

	err := middleware(func(echo.Context) error {
		t.Fatal("next must not be called")
		return nil
	})(c)

	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestAgentRuntimeProxyMiddlewareRejectsCreatorAccountProblems(t *testing.T) {
	// Bodies must match the exact envelopes SDK A2A proxies recognize.
	for _, tc := range []struct {
		name     string
		userErr  error
		wantCode int
		wantBody string
	}{
		{name: "disabled account", userErr: httpx.Unauthorized("账号已禁用"), wantCode: http.StatusForbidden,
			wantBody: `{"error":{"code":"FORBIDDEN","message":"Runtime permission denied"}}`},
		{name: "deleted account", userErr: httpx.Unauthorized("用户不存在或会话已失效"), wantCode: http.StatusForbidden,
			wantBody: `{"error":{"code":"FORBIDDEN","message":"Runtime permission denied"}}`},
		{name: "status lookup failure", userErr: httpx.Internal("认证状态校验失败"), wantCode: http.StatusServiceUnavailable,
			wantBody: `{"error":{"code":"SERVICE_UNAVAILABLE","message":"Agent Runtime A2A proxy is unavailable"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agentID := uuid.New()
			creatorID := uuid.New()
			var checked []uuid.UUID
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), rec)
			authenticator := runtimeProxyAuthenticatorFunc(func(echo.Context) (runtime.AuthenticatedRuntimePrincipal, *runtime.RuntimeTransportError) {
				return runtime.AuthenticatedRuntimePrincipal{AgentID: agentID, CredentialID: uuid.New()}, nil
			})
			middleware := AgentRuntimeProxyMiddleware(authenticator, runtimeProxyIdentityResolverStub{
				agent: db.Agent{
					ID: agentID, CreatorID: creatorID, Slug: "runtime-agent", LifecycleStatus: "active",
				},
				userErr: tc.userErr,
				checked: &checked,
			})

			err := middleware(func(echo.Context) error {
				t.Fatal("next must not be called")
				return nil
			})(c)

			require.NoError(t, err)
			require.Equal(t, tc.wantCode, rec.Code)
			require.JSONEq(t, tc.wantBody, rec.Body.String())
			require.Equal(t, []uuid.UUID{creatorID}, checked)
		})
	}
}

func TestRuntimeProxyTargetPreservesPrivateAgentExecutionWithoutPublicLookup(t *testing.T) {
	ownerID := uuid.New()
	target := db.Agent{
		ID: uuid.New(), CreatorID: ownerID, Slug: "private-runtime-agent",
		LifecycleStatus: "active", Visibility: "private",
	}
	ctx := context.WithValue(context.Background(), agentRuntimeProxyTargetContextKey{}, target)

	resolved, err := (&Service{}).resolveProtocolAgent(ctx, ownerID, target.Slug)
	require.NoError(t, err)
	require.Equal(t, target, resolved)

	_, err = (&Service{}).resolveProtocolAgent(ctx, uuid.New(), target.Slug)
	require.Error(t, err)
}

type runtimeProxyAuthenticatorFunc func(echo.Context) (runtime.AuthenticatedRuntimePrincipal, *runtime.RuntimeTransportError)

func (f runtimeProxyAuthenticatorFunc) AuthenticateAgentRequest(c echo.Context) (runtime.AuthenticatedRuntimePrincipal, *runtime.RuntimeTransportError) {
	return f(c)
}

func TestAgentRuntimeProxyMiddlewareRejectsAuthenticationFailure(t *testing.T) {
	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), rec)
	authenticator := runtimeProxyAuthenticatorFunc(func(echo.Context) (runtime.AuthenticatedRuntimePrincipal, *runtime.RuntimeTransportError) {
		return runtime.AuthenticatedRuntimePrincipal{}, runtime.NewRuntimeTransportError(runtime.RuntimeErrorUnauthorized, "unauthorized")
	})

	err := AgentRuntimeProxyMiddleware(authenticator, runtimeProxyIdentityResolverStub{})(func(echo.Context) error {
		t.Fatal("next must not be called")
		return nil
	})(c)

	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestAgentRuntimeProxyRoutesStayOnRuntimePrefix(t *testing.T) {
	e := echo.New()
	h := NewHandler(nil)
	h.RegisterAgentRuntimeProxy(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc {
		return next
	})

	routes := make(map[string]bool)
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/agent-runtime/a2a-proxy/agents/:slug",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/extendedAgentCard",
		"POST /api/v1/agent-runtime/a2a-proxy/agents/:slug/message:action",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID",
		"POST /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/cancel",
		"POST /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/pushNotificationConfig",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/pushNotificationConfig",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/pushNotificationConfig/:configID",
		"DELETE /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/pushNotificationConfig/:configID",
		"POST /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/pushNotificationConfigs",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/pushNotificationConfigs",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/pushNotificationConfigs/:configID",
		"DELETE /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/pushNotificationConfigs/:configID",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/subscribe",
		"POST /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/:taskID/subscribe",
		"GET /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/*",
		"POST /api/v1/agent-runtime/a2a-proxy/agents/:slug/tasks/*",
	} {
		require.True(t, routes[route], route)
	}
}
