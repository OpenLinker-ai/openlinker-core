package a2a

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// AgentRuntimeRequestAuthenticator verifies the same Agent Token plus device
// mTLS pair used by the canonical Runtime transport.
type AgentRuntimeRequestAuthenticator interface {
	AuthenticateAgentRequest(echo.Context) (runtime.AuthenticatedRuntimePrincipal, *runtime.RuntimeTransportError)
}

// AgentRuntimeIdentityResolver resolves ownership from Core's authoritative
// Agent record. A compatibility adapter can never nominate the acting user.
type AgentRuntimeIdentityResolver interface {
	GetAgentByID(context.Context, uuid.UUID) (db.Agent, error)
	// EnsureUserEnabled applies the same account check as JWT and User Token
	// requests to the creator the proxy is about to act as.
	EnsureUserEnabled(context.Context, uuid.UUID) error
}

type agentRuntimeProxyTargetContextKey struct{}

type SQLAgentRuntimeIdentityResolver struct {
	queries *db.Queries
	users   *auth.DBUserStatusChecker
}

func NewSQLAgentRuntimeIdentityResolver(pool *pgxpool.Pool) *SQLAgentRuntimeIdentityResolver {
	if pool == nil {
		return &SQLAgentRuntimeIdentityResolver{}
	}
	return &SQLAgentRuntimeIdentityResolver{queries: db.New(pool), users: auth.NewDBUserStatusChecker(pool)}
}

func (r *SQLAgentRuntimeIdentityResolver) EnsureUserEnabled(ctx context.Context, userID uuid.UUID) error {
	if r == nil || r.users == nil {
		return errors.New("Agent Runtime user status checker is not configured")
	}
	return r.users.EnsureUserEnabled(ctx, userID)
}

func (r *SQLAgentRuntimeIdentityResolver) GetAgentByID(ctx context.Context, agentID uuid.UUID) (db.Agent, error) {
	if r == nil || r.queries == nil {
		return db.Agent{}, errors.New("Agent Runtime identity resolver is not configured")
	}
	return r.queries.GetAgentByID(ctx, agentID)
}

// AgentRuntimeProxyMiddleware turns a validated Runtime principal into the
// existing first-party A2A handler identity. The caller-provided slug is
// intentionally replaced by the slug bound to the authenticated Agent Token.
func AgentRuntimeProxyMiddleware(
	authenticator AgentRuntimeRequestAuthenticator,
	resolver AgentRuntimeIdentityResolver,
) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if authenticator == nil || resolver == nil {
				return writeAgentRuntimeProxyError(c, runtime.RuntimeErrorServiceUnavailable)
			}
			principal, transportErr := authenticator.AuthenticateAgentRequest(c)
			if transportErr != nil {
				return c.JSON(runtime.RuntimeHTTPStatus(transportErr.Body.Code), transportErr.Envelope())
			}
			agentRecord, err := resolver.GetAgentByID(c.Request().Context(), principal.AgentID)
			if err != nil || agentRecord.ID != principal.AgentID || agentRecord.CreatorID == uuid.Nil ||
				agentRecord.Slug == "" || agentRecord.LifecycleStatus != "active" {
				return writeAgentRuntimeProxyError(c, runtime.RuntimeErrorServiceUnavailable)
			}
			// The proxy acts as the Agent's creator, so a disabled or deleted
			// account must stop here as it does for that user's own JWT.
			if err := resolver.EnsureUserEnabled(c.Request().Context(), agentRecord.CreatorID); err != nil {
				var httpErr *httpx.HTTPError
				if errors.As(err, &httpErr) && httpErr.Status == http.StatusUnauthorized {
					// Use Runtime's canonical permission-denied message: SDK
					// proxies only recognize that exact 403 envelope.
					transportErr := runtime.NewRuntimeTransportError(runtime.RuntimeErrorForbidden, "Runtime permission denied")
					return c.JSON(runtime.RuntimeHTTPStatus(runtime.RuntimeErrorForbidden), transportErr.Envelope())
				}
				return writeAgentRuntimeProxyError(c, runtime.RuntimeErrorServiceUnavailable)
			}

			replaceEchoPathParam(c, "slug", agentRecord.Slug)
			c.Set(a2aTargetAgentIDContextKey, agentRecord.ID)
			requestContext := context.WithValue(
				c.Request().Context(), agentRuntimeProxyTargetContextKey{}, agentRecord,
			)
			c.SetRequest(c.Request().WithContext(requestContext))
			auth.SetPrincipal(c, &auth.AuthPrincipal{
				UserID:     agentRecord.CreatorID,
				AuthMethod: auth.AuthMethodJWT,
			})
			return next(c)
		}
	}
}

func agentRuntimeProxyTargetFromContext(ctx context.Context) (db.Agent, bool) {
	if ctx == nil {
		return db.Agent{}, false
	}
	target, ok := ctx.Value(agentRuntimeProxyTargetContextKey{}).(db.Agent)
	return target, ok
}

func replaceEchoPathParam(c echo.Context, name, value string) {
	names := c.ParamNames()
	values := c.ParamValues()
	for index, candidate := range names {
		if candidate == name && index < len(values) {
			values[index] = value
			c.SetParamValues(values...)
			return
		}
	}
	c.SetParamNames(append(names, name)...)
	c.SetParamValues(append(values, value)...)
}

func writeAgentRuntimeProxyError(c echo.Context, code runtime.RuntimeErrorCode) error {
	transportErr := runtime.NewRuntimeTransportError(code, "Agent Runtime A2A proxy is unavailable")
	return c.JSON(runtime.RuntimeHTTPStatus(code), transportErr.Envelope())
}
