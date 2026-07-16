package externalexecution

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestHandlerRequiresScopedOneTimeServiceJWT(t *testing.T) {
	signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	handler := NewHandler(&fakeExecutionHandlerService{}, authorizer)
	actorID := uuid.NewString()

	missing := echo.New().NewContext(
		httptest.NewRequest(http.MethodPost, "/internal/external-execution-targets/validate", strings.NewReader(`{}`)),
		httptest.NewRecorder(),
	)
	assertHTTPStatus(t, handler.ValidateTarget(missing), http.StatusUnauthorized)

	wrongScope, err := signer.Sign(ScopeReadExecution, actorID)
	if err != nil {
		t.Fatal(err)
	}
	wrong := echo.New().NewContext(
		httptest.NewRequest(http.MethodPost, "/internal/external-execution-targets/validate", strings.NewReader(`{}`)),
		httptest.NewRecorder(),
	)
	wrong.Request().Header.Set(echo.HeaderAuthorization, "Bearer "+wrongScope)
	assertHTTPStatus(t, handler.ValidateTarget(wrong), http.StatusForbidden)

	valid, err := signer.Sign(ScopeValidateTarget, actorID)
	if err != nil {
		t.Fatal(err)
	}
	request := func() echo.Context {
		req := httptest.NewRequest(http.MethodPost, "/internal/external-execution-targets/validate", strings.NewReader(`{"target_type":"agent","target_id":"target"}`))
		req.Header.Set(echo.HeaderAuthorization, "Bearer "+valid)
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		return echo.New().NewContext(req, httptest.NewRecorder())
	}
	if err := handler.ValidateTarget(request()); err != nil {
		t.Fatalf("valid service JWT rejected: %v", err)
	}
	assertHTTPStatusAndCode(t, handler.ValidateTarget(request()), http.StatusConflict, httpx.ErrorCode("EXTERNAL_EXECUTION_JTI_REPLAY"))
}

func TestHandlerFailsClosedWhenReplayStoreFails(t *testing.T) {
	signer, authorizer := testServiceJWT(t, failingReplayStore{})
	token, err := signer.Sign(ScopeValidateTarget, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/external-execution-targets/validate", strings.NewReader(`{}`))
	req.Header.Set(echo.HeaderAuthorization, "Bearer "+token)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	c := echo.New().NewContext(req, httptest.NewRecorder())
	assertHTTPStatus(t, NewHandler(&fakeExecutionHandlerService{}, authorizer).ValidateTarget(c), http.StatusServiceUnavailable)
}

func TestServiceJWTEd25519ClaimsAndKeyParsing(t *testing.T) {
	if ScopeValidateTarget != "target.validate" || ScopeStartExecution != "execution.start" || ScopeReadExecution != "execution.read" {
		t.Fatalf("unexpected stable scopes: %q %q %q", ScopeValidateTarget, ScopeStartExecution, ScopeReadExecution)
	}
	seed := bytes.Repeat([]byte{7}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	privateEncoded := base64.RawStdEncoding.EncodeToString(seed)
	publicEncoded := base64.RawStdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey))

	parsedPrivate, err := ParseEd25519PrivateKey(privateEncoded)
	if err != nil || !bytes.Equal(parsedPrivate, privateKey) {
		t.Fatalf("private key parse = %v", err)
	}
	parsedPublic, err := ParseEd25519PublicKey(publicEncoded)
	if err != nil || !bytes.Equal(parsedPublic, privateKey.Public().(ed25519.PublicKey)) {
		t.Fatalf("public key parse = %v", err)
	}
	if _, err := NewServiceTokenSigner(privateEncoded, "key-1", "cloud", "core", "openlinker-cloud", 2*time.Minute); err == nil {
		t.Fatal("TTL above maximum must fail")
	}
	fullPrivateEncoded := base64.RawStdEncoding.EncodeToString(privateKey)
	parsedFullPrivate, err := ParseEd25519PrivateKey(fullPrivateEncoded)
	if err != nil || !bytes.Equal(parsedFullPrivate, privateKey) {
		t.Fatalf("full private key parse = %v", err)
	}
	corruptedPrivate := append(ed25519.PrivateKey(nil), privateKey...)
	corruptedPrivate[ed25519.SeedSize] ^= 0xff
	if _, err := ParseEd25519PrivateKey(base64.RawStdEncoding.EncodeToString(corruptedPrivate)); err == nil {
		t.Fatal("64-byte private key with mismatched public half must fail")
	}
}

func TestRedisReplayStoreAtomicallyConsumesIssuerAndJTI(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisReplayStore(client)

	consumed, err := store.Consume(context.Background(), "openlinker-cloud", "request-jti", 30*time.Second)
	if err != nil || !consumed {
		t.Fatalf("first Consume() = %v, %v", consumed, err)
	}
	consumed, err = store.Consume(context.Background(), "openlinker-cloud", "request-jti", 30*time.Second)
	if err != nil || consumed {
		t.Fatalf("replayed Consume() = %v, %v", consumed, err)
	}
	consumed, err = store.Consume(context.Background(), "other-issuer", "request-jti", 30*time.Second)
	if err != nil || !consumed {
		t.Fatalf("issuer-isolated Consume() = %v, %v", consumed, err)
	}

	server.Close()
	if _, err := store.Consume(context.Background(), "openlinker-cloud", "new-jti", 30*time.Second); err == nil {
		t.Fatal("Consume must return an error when Redis is unavailable")
	}
}

func TestAuthorizerRejectsWrongServiceSubject(t *testing.T) {
	signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	now := time.Now().UTC()
	claims := ServiceTokenClaims{
		Scope: ScopeValidateTarget, DelegatedActor: uuid.NewString(), CallerServiceID: "openlinker-cloud",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "openlinker-cloud",
			Subject:   "unrelated-service",
			Audience:  jwt.ClaimStrings{"openlinker-core.external-execution"},
			ExpiresAt: jwt.NewNumericDate(now.Add(30 * time.Second)),
			NotBefore: jwt.NewNumericDate(now.Add(-serviceTokenLeeway)),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        "wrong-subject-jti",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = signer.keyID
	raw, err := token.SignedString(signer.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	_, authErr := authorizer.Authorize(context.Background(), raw, ScopeValidateTarget)
	assertHTTPStatus(t, authErr, http.StatusUnauthorized)
}

func TestAuthorizerSupportsCurrentAndNextKeysAndRejectsUnknownKid(t *testing.T) {
	currentSeed := bytes.Repeat([]byte{4}, ed25519.SeedSize)
	nextSeed := bytes.Repeat([]byte{5}, ed25519.SeedSize)
	unknownSeed := bytes.Repeat([]byte{6}, ed25519.SeedSize)
	keys := []VerificationKey{
		{KeyID: "current", PublicKey: base64.RawStdEncoding.EncodeToString(ed25519.NewKeyFromSeed(currentSeed).Public().(ed25519.PublicKey))},
		{KeyID: "next", PublicKey: base64.RawStdEncoding.EncodeToString(ed25519.NewKeyFromSeed(nextSeed).Public().(ed25519.PublicKey))},
	}
	authorizer, err := NewAuthorizer(keys, "openlinker-cloud", "openlinker-core.external-execution", "openlinker-cloud", &memoryReplayStore{seen: map[string]struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		seed []byte
		kid  string
		want int
	}{
		{name: "current", seed: currentSeed, kid: "current"},
		{name: "next", seed: nextSeed, kid: "next"},
		{name: "unknown", seed: unknownSeed, kid: "unknown", want: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signer, signErr := NewServiceTokenSigner(base64.RawStdEncoding.EncodeToString(tc.seed), tc.kid, "openlinker-cloud", "openlinker-core.external-execution", "openlinker-cloud", 30*time.Second)
			if signErr != nil {
				t.Fatal(signErr)
			}
			raw, signErr := signer.Sign(ScopeValidateTarget, uuid.NewString())
			if signErr != nil {
				t.Fatal(signErr)
			}
			principal, authErr := authorizer.Authorize(context.Background(), raw, ScopeValidateTarget)
			if tc.want != 0 {
				assertHTTPStatus(t, authErr, tc.want)
				return
			}
			if authErr != nil || principal == nil || principal.CallerServiceID != "openlinker-cloud" || principal.ActorUserID == uuid.Nil {
				t.Fatalf("Authorize() = %#v, %v", principal, authErr)
			}
		})
	}
}

func TestAuthorizerRejectsExpiredTokenAndRemovedPreviousKey(t *testing.T) {
	previousSeed := bytes.Repeat([]byte{10}, ed25519.SeedSize)
	nextSeed := bytes.Repeat([]byte{11}, ed25519.SeedSize)
	nextPrivateKey := ed25519.NewKeyFromSeed(nextSeed)
	authorizer, err := NewAuthorizer(
		[]VerificationKey{{
			KeyID:     "next",
			PublicKey: base64.RawStdEncoding.EncodeToString(nextPrivateKey.Public().(ed25519.PublicKey)),
		}},
		"openlinker-cloud", "openlinker-core.external-execution", "openlinker-cloud",
		&memoryReplayStore{seen: map[string]struct{}{}},
	)
	if err != nil {
		t.Fatal(err)
	}

	previousSigner, err := NewServiceTokenSigner(
		base64.RawStdEncoding.EncodeToString(previousSeed), "previous", "openlinker-cloud",
		"openlinker-core.external-execution", "openlinker-cloud", 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	previousToken, err := previousSigner.Sign(ScopeValidateTarget, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	_, authErr := authorizer.Authorize(context.Background(), previousToken, ScopeValidateTarget)
	assertHTTPStatus(t, authErr, http.StatusUnauthorized)

	now := time.Now().UTC()
	expiredClaims := ServiceTokenClaims{
		Scope: ScopeValidateTarget, DelegatedActor: uuid.NewString(), CallerServiceID: "openlinker-cloud",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: "openlinker-cloud", Subject: serviceTokenSubject,
			Audience:  jwt.ClaimStrings{"openlinker-core.external-execution"},
			ExpiresAt: jwt.NewNumericDate(now.Add(-10 * time.Second)),
			NotBefore: jwt.NewNumericDate(now.Add(-47 * time.Second)),
			IssuedAt:  jwt.NewNumericDate(now.Add(-45 * time.Second)),
			ID:        "expired-after-rotation-jti",
		},
	}
	expired := jwt.NewWithClaims(jwt.SigningMethodEdDSA, expiredClaims)
	expired.Header["kid"] = "next"
	expiredToken, err := expired.SignedString(nextPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	_, authErr = authorizer.Authorize(context.Background(), expiredToken, ScopeValidateTarget)
	assertHTTPStatus(t, authErr, http.StatusUnauthorized)
}

func TestAuthorizerSeparatesIssuerFromCallerServiceIdentity(t *testing.T) {
	seed := bytes.Repeat([]byte{8}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	replay := &memoryReplayStore{seen: map[string]struct{}{}}
	authorizer, err := NewAuthorizer(
		[]VerificationKey{{KeyID: "custom-issuer", PublicKey: base64.RawStdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey))}},
		"https://identity.example.test", "openlinker-core.external-execution", "openlinker-cloud", replay,
	)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewServiceTokenSigner(
		base64.RawStdEncoding.EncodeToString(seed), "custom-issuer", "https://identity.example.test",
		"openlinker-core.external-execution", "openlinker-cloud", 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := signer.Sign(ScopeValidateTarget, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	principal, err := authorizer.Authorize(context.Background(), raw, ScopeValidateTarget)
	if err != nil || principal == nil || principal.CallerServiceID != "openlinker-cloud" {
		t.Fatalf("Authorize() = %#v, %v", principal, err)
	}

	wrongCallerSigner, err := NewServiceTokenSigner(
		base64.RawStdEncoding.EncodeToString(seed), "custom-issuer", "https://identity.example.test",
		"openlinker-core.external-execution", "other-service", 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = wrongCallerSigner.Sign(ScopeValidateTarget, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	_, err = authorizer.Authorize(context.Background(), raw, ScopeValidateTarget)
	assertHTTPStatus(t, err, http.StatusUnauthorized)
}

func TestAuthorizerRejectsMissingOrTamperedDelegatedActor(t *testing.T) {
	signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	now := time.Now().UTC()
	claims := ServiceTokenClaims{
		Scope: ScopeValidateTarget, CallerServiceID: "openlinker-cloud",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: "openlinker-cloud", Subject: serviceTokenSubject,
			Audience: jwt.ClaimStrings{"openlinker-core.external-execution"}, ExpiresAt: jwt.NewNumericDate(now.Add(30 * time.Second)),
			NotBefore: jwt.NewNumericDate(now.Add(-serviceTokenLeeway)), IssuedAt: jwt.NewNumericDate(now), ID: "missing-actor-jti",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = signer.keyID
	raw, err := token.SignedString(signer.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	_, authErr := authorizer.Authorize(context.Background(), raw, ScopeValidateTarget)
	assertHTTPStatus(t, authErr, http.StatusUnauthorized)

	valid, err := signer.Sign(ScopeValidateTarget, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(valid, ".")
	if len(parts) != 3 {
		t.Fatalf("unexpected JWT format")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	payload = bytes.Replace(payload, []byte("delegated_actor"), []byte("delegated_actoz"), 1)
	parts[1] = base64.RawURLEncoding.EncodeToString(payload)
	_, authErr = authorizer.Authorize(context.Background(), strings.Join(parts, "."), ScopeValidateTarget)
	assertHTTPStatus(t, authErr, http.StatusUnauthorized)
}

func TestAuthorizerRejectsMissingNotBefore(t *testing.T) {
	signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	now := time.Now().UTC()
	claims := ServiceTokenClaims{
		Scope: ScopeValidateTarget, DelegatedActor: uuid.NewString(), CallerServiceID: "openlinker-cloud",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: "openlinker-cloud", Subject: serviceTokenSubject,
			Audience: jwt.ClaimStrings{"openlinker-core.external-execution"}, ExpiresAt: jwt.NewNumericDate(now.Add(30 * time.Second)),
			IssuedAt: jwt.NewNumericDate(now), ID: "missing-nbf-jti",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = signer.keyID
	raw, err := token.SignedString(signer.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	_, authErr := authorizer.Authorize(context.Background(), raw, ScopeValidateTarget)
	assertHTTPStatus(t, authErr, http.StatusUnauthorized)
}

func TestAuthorizerRejectsMultipleScopes(t *testing.T) {
	signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	now := time.Now().UTC()
	claims := ServiceTokenClaims{
		Scope: ScopeValidateTarget + " " + ScopeStartExecution, DelegatedActor: uuid.NewString(), CallerServiceID: "openlinker-cloud",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: "openlinker-cloud", Subject: serviceTokenSubject,
			Audience: jwt.ClaimStrings{"openlinker-core.external-execution"}, ExpiresAt: jwt.NewNumericDate(now.Add(30 * time.Second)),
			NotBefore: jwt.NewNumericDate(now.Add(-serviceTokenLeeway)), IssuedAt: jwt.NewNumericDate(now), ID: "multiple-scope-jti",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = signer.keyID
	raw, err := token.SignedString(signer.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	_, authErr := authorizer.Authorize(context.Background(), raw, ScopeValidateTarget)
	assertHTTPStatus(t, authErr, http.StatusForbidden)
}

func TestAuthorizerRequiresExactlyOneConfiguredAudience(t *testing.T) {
	signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	now := time.Now().UTC()
	claims := ServiceTokenClaims{
		Scope: ScopeValidateTarget, DelegatedActor: uuid.NewString(), CallerServiceID: "openlinker-cloud",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: "openlinker-cloud", Subject: serviceTokenSubject,
			Audience:  jwt.ClaimStrings{"openlinker-core.external-execution", "unrelated-service"},
			ExpiresAt: jwt.NewNumericDate(now.Add(30 * time.Second)),
			NotBefore: jwt.NewNumericDate(now.Add(-serviceTokenLeeway)),
			IssuedAt:  jwt.NewNumericDate(now), ID: "multiple-audience-jti",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = signer.keyID
	raw, err := token.SignedString(signer.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	_, authErr := authorizer.Authorize(context.Background(), raw, ScopeValidateTarget)
	assertHTTPStatus(t, authErr, http.StatusUnauthorized)
}

func testServiceJWT(t *testing.T, replay ReplayStore) (*ServiceTokenSigner, *Authorizer) {
	t.Helper()
	seed := bytes.Repeat([]byte{9}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	signer, err := NewServiceTokenSigner(
		base64.RawStdEncoding.EncodeToString(seed), "test-key", "openlinker-cloud", "openlinker-core.external-execution", "openlinker-cloud", 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewAuthorizer(
		[]VerificationKey{{KeyID: "test-key", PublicKey: base64.RawStdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey))}},
		"openlinker-cloud", "openlinker-core.external-execution", "openlinker-cloud", replay,
	)
	if err != nil {
		t.Fatal(err)
	}
	return signer, authorizer
}

type fakeExecutionHandlerService struct{}

func (*fakeExecutionHandlerService) ValidateTarget(context.Context, *Principal, *TargetValidationRequest) (*TargetValidationResponse, error) {
	return &TargetValidationResponse{}, nil
}

func (*fakeExecutionHandlerService) StartExecution(context.Context, *Principal, *ExecutionRequest) (*ExecutionStartResponse, error) {
	return &ExecutionStartResponse{}, nil
}

func (*fakeExecutionHandlerService) GetExecution(context.Context, *Principal, string) (*ExecutionStatusResponse, error) {
	return &ExecutionStatusResponse{}, nil
}

func (*fakeExecutionHandlerService) CancelExecution(context.Context, *Principal, string, *ExecutionCancelRequest) (*ExecutionStatusResponse, error) {
	return &ExecutionStatusResponse{}, nil
}

type memoryReplayStore struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func (s *memoryReplayStore) Consume(_ context.Context, issuer, jti string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := issuer + "\x00" + jti
	if _, exists := s.seen[key]; exists {
		return false, nil
	}
	s.seen[key] = struct{}{}
	return true, nil
}

type failingReplayStore struct{}

func (failingReplayStore) Consume(context.Context, string, string, time.Duration) (bool, error) {
	return false, errors.New("redis unavailable")
}

func assertHTTPStatusAndCode(t *testing.T, err error, status int, code httpx.ErrorCode) {
	t.Helper()
	var he *httpx.HTTPError
	if !errors.As(err, &he) || he.Status != status || he.Code != code {
		t.Fatalf("error = %#v, want HTTP %d code %s", err, status, code)
	}
}
