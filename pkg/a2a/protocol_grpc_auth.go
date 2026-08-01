package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type GRPCAuthInfo struct {
	UserID     uuid.UUID
	AuthMethod string
	Scopes     []string
	Principal  *auth.AuthPrincipal
}

type GRPCAuthenticator interface {
	AuthenticateA2AGRPC(ctx context.Context) (*GRPCAuthInfo, error)
}

type BearerGRPCAuthenticator struct {
	jwtSecret string
	verifier  auth.ApiKeyVerifier
	users     auth.UserStatusChecker
}

func NewBearerGRPCAuthenticator(jwtSecret string, verifier auth.ApiKeyVerifier) *BearerGRPCAuthenticator {
	return &BearerGRPCAuthenticator{jwtSecret: jwtSecret, verifier: verifier}
}

func NewBearerGRPCAuthenticatorWithUserStatus(jwtSecret string, verifier auth.ApiKeyVerifier, users auth.UserStatusChecker) (*BearerGRPCAuthenticator, error) {
	if err := auth.ValidateUserStatusChecker(users); err != nil {
		return nil, fmt.Errorf("a2a grpc: %w", err)
	}
	return &BearerGRPCAuthenticator{jwtSecret: jwtSecret, verifier: verifier, users: users}, nil
}

func (a *BearerGRPCAuthenticator) AuthenticateA2AGRPC(ctx context.Context) (*GRPCAuthInfo, error) {
	token, err := bearerTokenFromGRPCMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if credential.HasAnyPrefix(token, credential.UserTokenPrefix) {
		if a.verifier == nil {
			return nil, httpx.Unauthorized("User Token 鉴权未启用")
		}
		if principalVerifier, ok := a.verifier.(auth.PrincipalAPIKeyVerifier); ok {
			principal, err := principalVerifier.VerifyPrincipal(ctx, token)
			if err != nil {
				return nil, httpx.Unauthorized("User Token 无效或已撤销")
			}
			return &GRPCAuthInfo{
				UserID: principal.UserID, AuthMethod: auth.AuthMethodUserToken,
				Scopes: principal.Permissions(), Principal: principal,
			}, nil
		}
		uid, scopes, err := a.verifier.Verify(ctx, token)
		if err != nil {
			return nil, httpx.Unauthorized("User Token 无效或已撤销")
		}
		grants := make([]auth.Grant, 0, len(scopes))
		for _, scope := range scopes {
			grants = append(grants, auth.Grant{
				Permission: scope, ResourceType: grpcResourceType(scope), Constraints: json.RawMessage(`{}`),
			})
		}
		principal := &auth.AuthPrincipal{UserID: uid, AuthMethod: auth.AuthMethodUserToken, Grants: grants}
		return &GRPCAuthInfo{UserID: uid, AuthMethod: auth.AuthMethodUserToken, Scopes: scopes, Principal: principal}, nil
	}
	claims, err := auth.ParseTokenClaims(token, a.jwtSecret)
	if err != nil {
		return nil, httpx.Unauthorized("token 无效或已过期")
	}
	parsed, err := uuid.Parse(claims.Subject)
	if err != nil {
		return nil, httpx.Unauthorized("token 无效")
	}
	if a.users == nil {
		return nil, httpx.Unauthorized("token 无效或已过期")
	}
	if err := a.users.EnsureJWTUserVersion(ctx, parsed, claims.TokenVersion); err != nil {
		return nil, err
	}
	principal := &auth.AuthPrincipal{UserID: parsed, AuthMethod: auth.AuthMethodJWT, Grants: []auth.Grant{}}
	return &GRPCAuthInfo{UserID: parsed, AuthMethod: auth.AuthMethodJWT, Principal: principal}, nil
}

func bearerTokenFromGRPCMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", httpx.Unauthorized("缺少 Authorization 头")
	}
	for _, value := range md.Get("authorization") {
		parts := strings.SplitN(strings.TrimSpace(value), " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") && strings.TrimSpace(parts[1]) != "" {
			return strings.TrimSpace(parts[1]), nil
		}
	}
	return "", httpx.Unauthorized("Authorization 格式错误")
}

func grpcAuthAllows(info *GRPCAuthInfo, permission, resourceType string, resourceID *uuid.UUID) bool {
	if info == nil {
		return false
	}
	if info.Principal != nil {
		return info.Principal.Allows(permission, resourceType, resourceID)
	}
	if info.AuthMethod != auth.AuthMethodUserToken {
		return true
	}
	for _, item := range info.Scopes {
		if item == permission && resourceID == nil {
			return true
		}
	}
	return false
}

func grpcResourceType(permission string) string {
	switch {
	case strings.HasPrefix(permission, "agents:"), strings.HasPrefix(permission, "agent-tokens:"):
		return "agent"
	case strings.HasPrefix(permission, "runs:"):
		return "run"
	case strings.HasPrefix(permission, "tasks:"):
		return "task"
	case strings.HasPrefix(permission, "workflows:"):
		return "workflow"
	default:
		return "core"
	}
}
