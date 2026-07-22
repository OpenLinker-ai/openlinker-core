package auth

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	AuthMethodJWT       = "jwt"
	AuthMethodUserToken = "user_token"
)

// Grant narrows a Core permission to one resource, or to all resources when
// ResourceID is nil. Domain owner/visibility/state checks still apply.
type Grant struct {
	Permission   string          `json:"permission"`
	ResourceType string          `json:"resource_type"`
	ResourceID   *uuid.UUID      `json:"resource_id,omitempty"`
	Constraints  json.RawMessage `json:"constraints"`
}

// AuthPrincipal is the single authenticated identity passed to Core handlers.
// JWT sessions represent the first-party user and are not narrowed by token
// grants; User Token requests always go through Allows.
type AuthPrincipal struct {
	UserID             uuid.UUID  `json:"user_id"`
	AuthMethod         string     `json:"auth_method"`
	TokenID            *uuid.UUID `json:"token_id,omitempty"`
	IssuerInstanceID   string     `json:"issuer_instance_id,omitempty"`
	Grants             []Grant    `json:"grants"`
	UserStatusVerified bool       `json:"-"`
}

func (p *AuthPrincipal) Permissions() []string {
	if p == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(p.Grants))
	out := make([]string, 0, len(p.Grants))
	for _, grant := range p.Grants {
		if _, ok := seen[grant.Permission]; ok {
			continue
		}
		seen[grant.Permission] = struct{}{}
		out = append(out, grant.Permission)
	}
	return out
}

func (p *AuthPrincipal) HasPermission(permission, resourceType string) bool {
	if p == nil {
		return false
	}
	if p.AuthMethod == AuthMethodJWT {
		return true
	}
	for _, grant := range p.Grants {
		if grant.Permission == permission && grant.ResourceType == resourceType {
			return true
		}
	}
	return false
}

// Allows evaluates the token grant only. It deliberately does not replace
// downstream ownership, visibility, or state-machine checks.
func (p *AuthPrincipal) Allows(permission, resourceType string, resourceID *uuid.UUID) bool {
	if p == nil {
		return false
	}
	if p.AuthMethod == AuthMethodJWT {
		return true
	}
	if p.AuthMethod != AuthMethodUserToken {
		return false
	}
	for _, grant := range p.Grants {
		if grant.Permission != permission || grant.ResourceType != resourceType {
			continue
		}
		if grant.ResourceID == nil {
			return true
		}
		if resourceID != nil && *grant.ResourceID == *resourceID {
			return true
		}
	}
	return false
}

func PrincipalFrom(c echo.Context) *AuthPrincipal {
	principal, _ := c.Get(string(httpx.CtxKeyAuthPrincipal)).(*AuthPrincipal)
	return principal
}

func SetPrincipal(c echo.Context, principal *AuthPrincipal) {
	if principal == nil {
		return
	}
	c.Set(string(httpx.CtxKeyAuthPrincipal), principal)
	c.Set(string(httpx.CtxKeyUserID), principal.UserID.String())
	c.Set(string(httpx.CtxKeyAuthMethod), principal.AuthMethod)
	c.Set(string(httpx.CtxKeyAuthScopes), principal.Permissions())
}

func RequirePermission(c echo.Context, permission, resourceType string, resourceID *uuid.UUID) error {
	principal := PrincipalFrom(c)
	if principal == nil {
		// Compatibility for direct handler tests and one-release bridge callers.
		// Real JWT/User Token middleware always installs a structured principal.
		method := httpx.AuthMethodFrom(c)
		if method == "" {
			return nil
		}
		if method == AuthMethodUserToken {
			if httpx.HasScope(c, permission) {
				return nil
			}
			return httpx.PermissionDenied(permission, resourceType)
		}
		if method == AuthMethodJWT && httpx.UserIDFrom(c) != "" {
			return nil
		}
		return httpx.Unauthorized("")
	}
	if principal.Allows(permission, resourceType, resourceID) {
		return nil
	}
	resourceIDString := ""
	if resourceID != nil {
		resourceIDString = resourceID.String()
	}
	return httpx.PermissionDenied(permission, resourceType, resourceIDString)
}

// RequireAnyPermission performs a pre-body check without treating a
// resource-specific grant as wildcard. Callers must still call
// RequirePermission after parsing the concrete resource ID.
func RequireAnyPermission(c echo.Context, permission, resourceType string) error {
	principal := PrincipalFrom(c)
	if principal != nil {
		if principal.HasPermission(permission, resourceType) {
			return nil
		}
		return httpx.PermissionDenied(permission, resourceType)
	}
	return RequirePermission(c, permission, resourceType, nil)
}
