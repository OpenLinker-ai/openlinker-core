package a2a

import (
	"context"
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
}

type GRPCAuthenticator interface {
	AuthenticateA2AGRPC(ctx context.Context) (*GRPCAuthInfo, error)
}

type BearerGRPCAuthenticator struct {
	jwtSecret string
	verifier  auth.ApiKeyVerifier
}

func NewBearerGRPCAuthenticator(jwtSecret string, verifier auth.ApiKeyVerifier) *BearerGRPCAuthenticator {
	return &BearerGRPCAuthenticator{jwtSecret: jwtSecret, verifier: verifier}
}

func (a *BearerGRPCAuthenticator) AuthenticateA2AGRPC(ctx context.Context) (*GRPCAuthInfo, error) {
	token, err := bearerTokenFromGRPCMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if credential.HasAnyPrefix(token, credential.AccessTokenPrefix, credential.LegacyAPIKeyPrefix) {
		if a.verifier == nil {
			return nil, httpx.Unauthorized("访问令牌鉴权未启用")
		}
		uid, scopes, err := a.verifier.Verify(ctx, token)
		if err != nil {
			return nil, httpx.Unauthorized("访问令牌无效或已撤销")
		}
		return &GRPCAuthInfo{UserID: uid, AuthMethod: "apikey", Scopes: scopes}, nil
	}
	uid, err := auth.ParseToken(token, a.jwtSecret)
	if err != nil {
		return nil, httpx.Unauthorized("token 无效或已过期")
	}
	parsed, err := uuid.Parse(uid)
	if err != nil {
		return nil, httpx.Unauthorized("token 无效")
	}
	return &GRPCAuthInfo{UserID: parsed, AuthMethod: "jwt"}, nil
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

func grpcAuthHasScope(info *GRPCAuthInfo, scope string) bool {
	if info == nil || info.AuthMethod != "apikey" {
		return true
	}
	for _, item := range info.Scopes {
		if item == scope {
			return true
		}
	}
	return false
}
