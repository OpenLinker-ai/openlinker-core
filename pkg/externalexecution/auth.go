package externalexecution

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	ScopeValidateTarget            = "target.validate"
	ScopeStartExecution            = "execution.start"
	ScopeReadExecution             = "execution.read"
	ScopeCancelExecution           = "execution.cancel"
	ErrorCodeJTIReplay             = "EXTERNAL_EXECUTION_JTI_REPLAY"
	ErrorCodeRequestBindingInvalid = "EXTERNAL_EXECUTION_REQUEST_BINDING_INVALID"
	RequestBindingVersionV1        = "v1"
	// LegacyCutoverCallerServiceID is the caller namespace assigned to every
	// pre-074 execution row. Deployments must keep this identity until a future
	// migration explicitly rekeys that historical namespace.
	LegacyCutoverCallerServiceID = "openlinker-cloud"
	serviceTokenSubject          = "external-execution-client"

	defaultServiceTokenTTL = 30 * time.Second
	maximumServiceTokenTTL = 60 * time.Second
	serviceTokenLeeway     = 2 * time.Second
)

type ServiceTokenClaims struct {
	Scope                 string `json:"scope"`
	DelegatedActor        string `json:"delegated_actor"`
	CallerServiceID       string `json:"caller_service_id"`
	RequestBindingVersion string `json:"request_binding_version,omitempty"`
	RequestMethod         string `json:"request_method,omitempty"`
	RequestPath           string `json:"request_path,omitempty"`
	RequestBodySHA256     string `json:"request_body_sha256,omitempty"`
	jwt.RegisteredClaims
}

// serviceTokenVerificationClaims deliberately omits request-binding fields.
// Binding values are decoded from the raw claims JSON only after signature and
// base service claims verification, so null/partial/wrongly typed binding keys
// are classified as binding failures instead of being mistaken for legacy or
// collapsed into a generic JWT parse failure.
type serviceTokenVerificationClaims struct {
	Scope           string `json:"scope"`
	DelegatedActor  string `json:"delegated_actor"`
	CallerServiceID string `json:"caller_service_id"`
	jwt.RegisteredClaims
}

type Principal struct {
	CallerServiceID string
	ActorUserID     uuid.UUID
}

type VerificationKey struct {
	KeyID     string
	PublicKey string
}

type ServiceTokenSigner struct {
	privateKey ed25519.PrivateKey
	keyID      string
	issuer     string
	audience   string
	callerID   string
	ttl        time.Duration
	now        func() time.Time
}

func NewServiceTokenSigner(rawPrivateKey, keyID, issuer, audience, callerServiceID string, ttl time.Duration) (*ServiceTokenSigner, error) {
	privateKey, err := ParseEd25519PrivateKey(rawPrivateKey)
	if err != nil {
		return nil, err
	}
	keyID = strings.TrimSpace(keyID)
	issuer = strings.TrimSpace(issuer)
	audience = strings.TrimSpace(audience)
	callerServiceID = strings.TrimSpace(callerServiceID)
	if keyID == "" || issuer == "" || audience == "" || callerServiceID == "" {
		return nil, errors.New("external execution JWT key id, issuer, audience, and caller service id are required")
	}
	if len(callerServiceID) > 200 {
		return nil, errors.New("external execution caller service id must not exceed 200 bytes")
	}
	if ttl <= 0 {
		ttl = defaultServiceTokenTTL
	}
	if ttl > maximumServiceTokenTTL {
		return nil, fmt.Errorf("external execution JWT TTL must not exceed %s", maximumServiceTokenTTL)
	}
	return &ServiceTokenSigner{
		privateKey: privateKey, keyID: keyID, issuer: issuer, audience: audience, callerID: callerServiceID, ttl: ttl, now: time.Now,
	}, nil
}

func (s *ServiceTokenSigner) Sign(scope, delegatedActor string) (string, error) {
	return s.sign(scope, delegatedActor, nil)
}

// SignRequest issues a v1 credential bound to the exact HTTP request bytes.
// escapedPath must be the final request URL's EscapedPath, not a route template.
func (s *ServiceTokenSigner) SignRequest(scope, delegatedActor, method, escapedPath string, body []byte) (string, error) {
	method = strings.TrimSpace(method)
	if method == "" || method != strings.ToUpper(method) {
		return "", errors.New("external execution request method must be uppercase")
	}
	if escapedPath == "" || !strings.HasPrefix(escapedPath, "/") ||
		strings.ContainsAny(escapedPath, "?#") || strings.TrimSpace(escapedPath) != escapedPath {
		return "", errors.New("external execution request escaped path is invalid")
	}
	return s.sign(scope, delegatedActor, &RequestBinding{
		Version: RequestBindingVersionV1,
		Method:  method,
		Path:    escapedPath,
		BodySHA: RequestBodySHA256(body),
	})
}

func (s *ServiceTokenSigner) sign(scope, delegatedActor string, binding *RequestBinding) (string, error) {
	if s == nil || len(s.privateKey) != ed25519.PrivateKeySize {
		return "", errors.New("external execution service signer is not configured")
	}
	if !validServiceScope(scope) {
		return "", errors.New("external execution service JWT scope is invalid")
	}
	actorID, err := uuid.Parse(strings.TrimSpace(delegatedActor))
	if err != nil || actorID == uuid.Nil {
		return "", errors.New("external execution delegated actor must be a non-zero UUID")
	}
	now := s.now().UTC()
	claims := ServiceTokenClaims{
		Scope: scope, DelegatedActor: actorID.String(), CallerServiceID: s.callerID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   serviceTokenSubject,
			Audience:  jwt.ClaimStrings{s.audience},
			ExpiresAt: jwt.NewNumericDate(now.Add(s.ttl)),
			NotBefore: jwt.NewNumericDate(now.Add(-serviceTokenLeeway)),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        uuid.NewString(),
		},
	}
	if binding != nil {
		claims.RequestBindingVersion = binding.Version
		claims.RequestMethod = binding.Method
		claims.RequestPath = binding.Path
		claims.RequestBodySHA256 = binding.BodySHA
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = s.keyID
	return token.SignedString(s.privateKey)
}

type ReplayStore interface {
	Consume(context.Context, string, string, time.Duration) (bool, error)
}

type RedisReplayStore struct {
	client *redis.Client
	prefix string
}

func NewRedisReplayStore(client *redis.Client) *RedisReplayStore {
	return &RedisReplayStore{client: client, prefix: "openlinker:external-execution:jti:"}
}

func (s *RedisReplayStore) Consume(ctx context.Context, issuer, jti string, ttl time.Duration) (bool, error) {
	if s == nil || s.client == nil {
		return false, errors.New("external execution replay store is unavailable")
	}
	if ttl <= 0 {
		return false, errors.New("external execution replay TTL is invalid")
	}
	digest := sha256.Sum256([]byte(strings.TrimSpace(issuer) + "\x00" + strings.TrimSpace(jti)))
	return s.client.SetNX(ctx, s.prefix+hex.EncodeToString(digest[:]), "1", ttl).Result()
}

type Authorizer struct {
	publicKeys            map[string]ed25519.PublicKey
	issuer                string
	audience              string
	callerID              string
	replay                ReplayStore
	requireRequestBinding bool
	now                   func() time.Time
}

type AuthorizerOption func(*Authorizer)

// WithRequestBindingRequired selects the Release B fail-closed mode. Without
// this option Release A accepts only tokens whose four binding keys are all
// absent; partial, null, malformed, or unknown binding claims still fail.
func WithRequestBindingRequired() AuthorizerOption {
	return func(authorizer *Authorizer) {
		authorizer.requireRequestBinding = true
	}
}

func NewAuthorizer(keys []VerificationKey, issuer, audience, callerServiceID string, replay ReplayStore, options ...AuthorizerOption) (*Authorizer, error) {
	issuer = strings.TrimSpace(issuer)
	audience = strings.TrimSpace(audience)
	callerServiceID = strings.TrimSpace(callerServiceID)
	if issuer == "" || len(issuer) > 200 || audience == "" || callerServiceID == "" {
		return nil, errors.New("external execution JWT issuer, audience, and caller service id are required")
	}
	if len(callerServiceID) > 200 {
		return nil, errors.New("external execution caller service id must not exceed 200 bytes")
	}
	if replay == nil {
		return nil, errors.New("external execution replay store is required")
	}
	publicKeys := make(map[string]ed25519.PublicKey, len(keys))
	for _, candidate := range keys {
		keyID := strings.TrimSpace(candidate.KeyID)
		if keyID == "" || strings.TrimSpace(candidate.PublicKey) == "" {
			return nil, errors.New("external execution JWT verification key id and public key are required")
		}
		if _, exists := publicKeys[keyID]; exists {
			return nil, fmt.Errorf("duplicate external execution JWT key id %q", keyID)
		}
		publicKey, err := ParseEd25519PublicKey(candidate.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("parse external execution JWT public key %q: %w", keyID, err)
		}
		publicKeys[keyID] = publicKey
	}
	if len(publicKeys) == 0 {
		return nil, errors.New("at least one external execution JWT verification key is required")
	}
	authorizer := &Authorizer{
		publicKeys: publicKeys, issuer: issuer, audience: audience, callerID: callerServiceID, replay: replay, now: time.Now,
	}
	for _, option := range options {
		if option != nil {
			option(authorizer)
		}
	}
	return authorizer, nil
}

func (a *Authorizer) Authorize(ctx context.Context, rawToken, requiredScope string) (*Principal, error) {
	verified, err := a.verify(rawToken, requiredScope)
	if err != nil {
		return nil, err
	}
	if verified.bindingClaimCount != 0 || a.requireRequestBinding {
		return nil, requestBindingInvalidError()
	}
	return a.consume(ctx, verified)
}

func (a *Authorizer) verify(rawToken, requiredScope string) (*verifiedServiceToken, error) {
	if a == nil || len(a.publicKeys) == 0 || a.replay == nil {
		return nil, httpx.ServiceUnavailable("外部执行认证暂不可用")
	}
	claims := &serviceTokenVerificationClaims{}
	token, err := jwt.ParseWithClaims(
		strings.TrimSpace(rawToken),
		claims,
		func(token *jwt.Token) (interface{}, error) {
			if token.Method != jwt.SigningMethodEdDSA {
				return nil, errors.New("unexpected JWT signing method")
			}
			kid, _ := token.Header["kid"].(string)
			publicKey, ok := a.publicKeys[strings.TrimSpace(kid)]
			if !ok {
				return nil, errors.New("unexpected JWT key id")
			}
			return publicKey, nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
		jwt.WithIssuer(a.issuer),
		jwt.WithAudience(a.audience),
		jwt.WithExpirationRequired(),
		jwt.WithNotBeforeRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(serviceTokenLeeway),
	)
	if err != nil || token == nil || !token.Valid {
		return nil, httpx.Unauthorized("外部执行服务凭据无效")
	}
	if claims.ID == "" || claims.Subject != serviceTokenSubject || claims.IssuedAt == nil || claims.NotBefore == nil || claims.ExpiresAt == nil {
		return nil, httpx.Unauthorized("外部执行服务凭据无效")
	}
	if len(claims.Audience) != 1 || claims.Audience[0] != a.audience {
		return nil, httpx.Unauthorized("外部执行服务凭据无效")
	}
	if claims.CallerServiceID != a.callerID {
		return nil, httpx.Unauthorized("外部执行服务凭据调用方身份无效")
	}
	if !validServiceScope(requiredScope) || claims.Scope != requiredScope {
		return nil, httpx.Forbidden("外部执行服务凭据权限不足")
	}
	actorID, err := uuid.Parse(strings.TrimSpace(claims.DelegatedActor))
	if err != nil || actorID == uuid.Nil || claims.DelegatedActor != actorID.String() {
		return nil, httpx.Unauthorized("外部执行服务凭据缺少有效代理身份")
	}
	now := a.now().UTC()
	issuedAt := claims.IssuedAt.Time.UTC()
	expiresAt := claims.ExpiresAt.Time.UTC()
	if issuedAt.After(now.Add(serviceTokenLeeway)) || !expiresAt.After(now) || expiresAt.Sub(issuedAt) > maximumServiceTokenTTL {
		return nil, httpx.Unauthorized("外部执行服务凭据无效")
	}
	binding, bindingClaimCount, bindingValuesValid, err := parseRequestBindingClaims(rawToken)
	if err != nil {
		return nil, httpx.Unauthorized("外部执行服务凭据无效")
	}
	return &verifiedServiceToken{
		claims:             claims,
		principal:          &Principal{CallerServiceID: claims.CallerServiceID, ActorUserID: actorID},
		expiresAt:          expiresAt,
		binding:            binding,
		bindingClaimCount:  bindingClaimCount,
		bindingValuesValid: bindingValuesValid,
	}, nil
}

func (a *Authorizer) consume(ctx context.Context, verified *verifiedServiceToken) (*Principal, error) {
	if a == nil || verified == nil || verified.claims == nil || verified.principal == nil {
		return nil, httpx.ServiceUnavailable("外部执行认证暂不可用")
	}
	now := a.now().UTC()
	if !verified.expiresAt.After(now) {
		return nil, httpx.Unauthorized("外部执行服务凭据无效")
	}
	consumed, err := a.replay.Consume(
		ctx,
		verified.claims.Issuer,
		verified.claims.ID,
		verified.expiresAt.Sub(now)+serviceTokenLeeway,
	)
	if err != nil {
		return nil, httpx.ServiceUnavailable("外部执行防重放校验暂不可用")
	}
	if !consumed {
		return nil, httpx.NewError(http.StatusConflict, httpx.ErrorCode(ErrorCodeJTIReplay), "外部执行服务凭据已使用")
	}
	if verified.bindingClaimCount == 0 {
		recordLegacyRequestBindingAccepted()
	}
	return verified.principal, nil
}

func ParseEd25519PrivateKey(raw string) (ed25519.PrivateKey, error) {
	decoded, err := decodeKeyMaterial(raw)
	if err != nil {
		return nil, fmt.Errorf("parse external execution Ed25519 private key: %w", err)
	}
	if block, _ := pem.Decode(decoded); block != nil {
		key, parseErr := x509.ParsePKCS8PrivateKey(block.Bytes)
		if parseErr != nil {
			return nil, fmt.Errorf("parse PKCS#8 private key: %w", parseErr)
		}
		privateKey, ok := key.(ed25519.PrivateKey)
		if !ok {
			return nil, errors.New("private key is not Ed25519")
		}
		if err := validateEd25519PrivateKey(privateKey); err != nil {
			return nil, err
		}
		return append(ed25519.PrivateKey(nil), privateKey...), nil
	}
	switch len(decoded) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(decoded), nil
	case ed25519.PrivateKeySize:
		if err := validateEd25519PrivateKey(ed25519.PrivateKey(decoded)); err != nil {
			return nil, err
		}
		return append(ed25519.PrivateKey(nil), decoded...), nil
	default:
		return nil, fmt.Errorf("private key must contain %d-byte seed, %d-byte key, or PKCS#8 PEM", ed25519.SeedSize, ed25519.PrivateKeySize)
	}
}

func validateEd25519PrivateKey(privateKey ed25519.PrivateKey) error {
	if len(privateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("private key must contain %d bytes", ed25519.PrivateKeySize)
	}
	derived := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if !bytes.Equal(privateKey, derived) {
		return errors.New("Ed25519 private key public half does not match its seed")
	}
	return nil
}

func ParseEd25519PublicKey(raw string) (ed25519.PublicKey, error) {
	decoded, err := decodeKeyMaterial(raw)
	if err != nil {
		return nil, fmt.Errorf("parse external execution Ed25519 public key: %w", err)
	}
	if block, _ := pem.Decode(decoded); block != nil {
		key, parseErr := x509.ParsePKIXPublicKey(block.Bytes)
		if parseErr != nil {
			return nil, fmt.Errorf("parse PKIX public key: %w", parseErr)
		}
		publicKey, ok := key.(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("public key is not Ed25519")
		}
		return append(ed25519.PublicKey(nil), publicKey...), nil
	}
	if len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("public key must contain %d raw bytes or PKIX PEM", ed25519.PublicKeySize)
	}
	return append(ed25519.PublicKey(nil), decoded...), nil
}

func decodeKeyMaterial(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("key is empty")
	}
	if strings.Contains(raw, "-----BEGIN") {
		return []byte(raw), nil
	}
	for _, encoding := range []*base64.Encoding{
		base64.RawStdEncoding, base64.StdEncoding, base64.RawURLEncoding, base64.URLEncoding,
	} {
		if decoded, err := encoding.DecodeString(raw); err == nil {
			return decoded, nil
		}
	}
	return nil, errors.New("key is not valid base64 or PEM")
}

func validServiceScope(scope string) bool {
	switch strings.TrimSpace(scope) {
	case ScopeValidateTarget, ScopeStartExecution, ScopeReadExecution, ScopeCancelExecution:
		return true
	default:
		return false
	}
}
