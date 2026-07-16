package externalexecution

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestServiceTokenSignerBindsExactRequest(t *testing.T) {
	signer, _ := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	body := []byte("{\n  \"target_id\": \"exact-bytes\"\n}")
	raw, err := signer.SignRequest(
		ScopeValidateTarget,
		uuid.NewString(),
		http.MethodPost,
		"/internal/external-execution-targets/a%2Fb/validate",
		body,
	)
	if err != nil {
		t.Fatal(err)
	}

	claims := &ServiceTokenClaims{}
	_, _, err = jwt.NewParser().ParseUnverified(raw, claims)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if claims.RequestBindingVersion != RequestBindingVersionV1 ||
		claims.RequestMethod != http.MethodPost ||
		claims.RequestPath != "/internal/external-execution-targets/a%2Fb/validate" ||
		claims.RequestBodySHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("request binding claims = %#v", claims)
	}
	if RequestBodySHA256(nil) != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatal("GET empty-body digest is not the SHA-256 of zero bytes")
	}
}

func TestHandlerRequestBindingSupportsCurrentAndNextKeysAndRejectsRemovedKey(t *testing.T) {
	currentSeed := bytes.Repeat([]byte{21}, ed25519.SeedSize)
	nextSeed := bytes.Repeat([]byte{22}, ed25519.SeedSize)
	removedSeed := bytes.Repeat([]byte{23}, ed25519.SeedSize)
	keys := []VerificationKey{
		{KeyID: "current", PublicKey: encodePublicKey(ed25519.NewKeyFromSeed(currentSeed))},
		{KeyID: "next", PublicKey: encodePublicKey(ed25519.NewKeyFromSeed(nextSeed))},
	}
	authorizer, err := NewAuthorizer(
		keys,
		"openlinker-cloud",
		"openlinker-core.external-execution",
		"openlinker-cloud",
		&memoryReplayStore{seen: map[string]struct{}{}},
		WithRequestBindingRequired(),
	)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"target_type":"agent","target_id":"target"}`)
	path := "/internal/external-execution-targets/validate"
	for _, tc := range []struct {
		name       string
		seed       []byte
		keyID      string
		wantStatus int
	}{
		{name: "current", seed: currentSeed, keyID: "current"},
		{name: "next", seed: nextSeed, keyID: "next"},
		{name: "removed", seed: removedSeed, keyID: "removed", wantStatus: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signer, err := NewServiceTokenSigner(
				base64.RawStdEncoding.EncodeToString(tc.seed),
				tc.keyID,
				"openlinker-cloud",
				"openlinker-core.external-execution",
				"openlinker-cloud",
				30*time.Second,
			)
			if err != nil {
				t.Fatal(err)
			}
			token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), http.MethodPost, path, body)
			if err != nil {
				t.Fatal(err)
			}
			err = NewHandler(&countingExecutionHandlerService{}, authorizer).ValidateTarget(
				bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body),
			)
			if tc.wantStatus == 0 {
				if err != nil {
					t.Fatalf("valid rotated key = %v", err)
				}
				return
			}
			assertHTTPStatus(t, err, tc.wantStatus)
		})
	}
}

func TestHandlerRequestBindingMismatchDoesNotConsumeJTI(t *testing.T) {
	replay := &countingReplayStore{seen: map[string]struct{}{}}
	signer, authorizer := testServiceJWT(t, replay)
	service := &countingExecutionHandlerService{}
	handler := NewHandler(service, authorizer)
	body := []byte(`{"target_type":"agent","target_id":"target"}`)
	path := "/internal/external-execution-targets/validate"
	token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), http.MethodPost, path, body)
	if err != nil {
		t.Fatal(err)
	}

	tampered := []byte("{ \"target_id\":\"target\",\"target_type\":\"agent\" }")
	assertHTTPStatusAndCode(
		t,
		handler.ValidateTarget(bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, tampered)),
		http.StatusUnauthorized,
		httpx.ErrorCode(ErrorCodeRequestBindingInvalid),
	)
	if replay.Calls() != 0 || service.validateCalls.Load() != 0 {
		t.Fatalf("mismatch consumed replay=%d or called service=%d", replay.Calls(), service.validateCalls.Load())
	}

	if err := handler.ValidateTarget(bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body)); err != nil {
		t.Fatalf("correct request after mismatch = %v", err)
	}
	if replay.Calls() != 1 || service.validateCalls.Load() != 1 {
		t.Fatalf("correct request replay=%d service=%d", replay.Calls(), service.validateCalls.Load())
	}
	assertHTTPStatusAndCode(
		t,
		handler.ValidateTarget(bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body)),
		http.StatusConflict,
		httpx.ErrorCode(ErrorCodeJTIReplay),
	)
	if replay.Calls() != 2 || service.validateCalls.Load() != 1 {
		t.Fatalf("replay attempts=%d service=%d", replay.Calls(), service.validateCalls.Load())
	}
}

func TestHandlerRequestBindingRejectsMethodPathAndExactByteChanges(t *testing.T) {
	body := []byte(`{"target_type":"agent","target_id":"target"}`)
	path := "/internal/external-execution-targets/validate"
	for _, tc := range []struct {
		name       string
		signMethod string
		signPath   string
		signBody   []byte
		reqMethod  string
		reqPath    string
		reqBody    []byte
	}{
		{name: "method", signMethod: http.MethodPost, signPath: path, signBody: body, reqMethod: http.MethodPut, reqPath: path, reqBody: body},
		{name: "method case", signMethod: http.MethodPost, signPath: path, signBody: body, reqMethod: "post", reqPath: path, reqBody: body},
		{name: "escaped path", signMethod: http.MethodPost, signPath: path + "/a%2Fb", signBody: body, reqMethod: http.MethodPost, reqPath: path + "/a/b", reqBody: body},
		{name: "whitespace", signMethod: http.MethodPost, signPath: path, signBody: body, reqMethod: http.MethodPost, reqPath: path, reqBody: []byte("{ \"target_type\":\"agent\",\"target_id\":\"target\" }")},
		{name: "key order", signMethod: http.MethodPost, signPath: path, signBody: body, reqMethod: http.MethodPost, reqPath: path, reqBody: []byte(`{"target_id":"target","target_type":"agent"}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replay := &countingReplayStore{seen: map[string]struct{}{}}
			signer, authorizer := testServiceJWT(t, replay)
			token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), tc.signMethod, tc.signPath, tc.signBody)
			if err != nil {
				t.Fatal(err)
			}
			handler := NewHandler(&countingExecutionHandlerService{}, authorizer)
			err = handler.ValidateTarget(bindingContext(tc.reqMethod, tc.reqPath, token, echo.MIMEApplicationJSON, tc.reqBody))
			assertHTTPStatusAndCode(t, err, http.StatusUnauthorized, httpx.ErrorCode(ErrorCodeRequestBindingInvalid))
			if replay.Calls() != 0 {
				t.Fatalf("binding mismatch consumed JTI %d times", replay.Calls())
			}
		})
	}
}

func TestHandlerRequestBindingRawClaimPresenceFailsClosed(t *testing.T) {
	body := []byte(`{"target_type":"agent","target_id":"target"}`)
	path := "/internal/external-execution-targets/validate"
	digest := RequestBodySHA256(body)
	for _, tc := range []struct {
		name   string
		claims map[string]any
	}{
		{name: "partial", claims: map[string]any{"request_binding_version": RequestBindingVersionV1}},
		{name: "explicit null", claims: map[string]any{
			"request_binding_version": nil, "request_method": nil, "request_path": nil, "request_body_sha256": nil,
		}},
		{name: "explicit empty", claims: map[string]any{
			"request_binding_version": "", "request_method": "", "request_path": "", "request_body_sha256": "",
		}},
		{name: "unknown version", claims: map[string]any{
			"request_binding_version": "v2", "request_method": http.MethodPost, "request_path": path, "request_body_sha256": digest,
		}},
		{name: "malformed digest", claims: map[string]any{
			"request_binding_version": RequestBindingVersionV1, "request_method": http.MethodPost, "request_path": path, "request_body_sha256": strings.Repeat("A", 64),
		}},
		{name: "wrong claim types", claims: map[string]any{
			"request_binding_version": 1, "request_method": []string{http.MethodPost}, "request_path": map[string]any{"path": path}, "request_body_sha256": true,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replay := &countingReplayStore{seen: map[string]struct{}{}}
			signer, authorizer := testServiceJWT(t, replay)
			token := signServiceMapClaims(t, signer, ScopeValidateTarget, uuid.NewString(), tc.claims)
			err := NewHandler(&countingExecutionHandlerService{}, authorizer).ValidateTarget(
				bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body),
			)
			assertHTTPStatusAndCode(t, err, http.StatusUnauthorized, httpx.ErrorCode(ErrorCodeRequestBindingInvalid))
			if replay.Calls() != 0 {
				t.Fatalf("invalid binding consumed JTI %d times", replay.Calls())
			}
		})
	}
}

func TestHandlerRequestBindingCompatibilityAndRequiredModes(t *testing.T) {
	body := []byte(`{"target_type":"agent","target_id":"target"}`)
	path := "/internal/external-execution-targets/validate"

	t.Run("release A accepts completely legacy token", func(t *testing.T) {
		signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
		token, err := signer.Sign(ScopeValidateTarget, uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		err = NewHandler(&countingExecutionHandlerService{}, authorizer).ValidateTarget(
			bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body),
		)
		if err != nil {
			t.Fatalf("legacy token in compatibility mode = %v", err)
		}
	})

	t.Run("release B rejects completely legacy token", func(t *testing.T) {
		seedSigner, _ := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
		token, err := seedSigner.Sign(ScopeValidateTarget, uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		authorizer, err := NewAuthorizer(
			[]VerificationKey{{KeyID: seedSigner.keyID, PublicKey: encodePublicKey(seedSigner.privateKey)}},
			"openlinker-cloud",
			"openlinker-core.external-execution",
			"openlinker-cloud",
			&countingReplayStore{seen: map[string]struct{}{}},
			WithRequestBindingRequired(),
		)
		if err != nil {
			t.Fatal(err)
		}
		err = NewHandler(&countingExecutionHandlerService{}, authorizer).ValidateTarget(
			bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body),
		)
		assertHTTPStatusAndCode(t, err, http.StatusUnauthorized, httpx.ErrorCode(ErrorCodeRequestBindingInvalid))
	})
}

func TestHandlerRequestBindingEnforcesRouteHTTPEnvelopeBeforeReplay(t *testing.T) {
	body := []byte(`{"target_type":"agent","target_id":"target"}`)
	path := "/internal/external-execution-targets/validate"
	for _, tc := range []struct {
		name        string
		contentType string
		requestPath string
		wantStatus  int
	}{
		{name: "json charset is accepted", contentType: "application/json; charset=utf-8", requestPath: path},
		{name: "missing content type", requestPath: path, wantStatus: http.StatusUnsupportedMediaType},
		{name: "non json content type", contentType: "text/plain", requestPath: path, wantStatus: http.StatusUnsupportedMediaType},
		{name: "nonempty query", contentType: echo.MIMEApplicationJSON, requestPath: path + "?debug=true", wantStatus: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replay := &countingReplayStore{seen: map[string]struct{}{}}
			signer, authorizer := testServiceJWT(t, replay)
			token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), http.MethodPost, path, body)
			if err != nil {
				t.Fatal(err)
			}
			err = NewHandler(&countingExecutionHandlerService{}, authorizer).ValidateTarget(
				bindingContext(http.MethodPost, tc.requestPath, token, tc.contentType, body),
			)
			if tc.wantStatus == 0 {
				if err != nil {
					t.Fatalf("valid envelope = %v", err)
				}
				return
			}
			assertHTTPStatus(t, err, tc.wantStatus)
			if replay.Calls() != 0 {
				t.Fatalf("invalid envelope consumed JTI %d times", replay.Calls())
			}
		})
	}
}

func TestHandlerRequestBindingGETRequiresEmptyBodyAndExactConcretePath(t *testing.T) {
	requestID := uuid.NewString()
	path := "/internal/external-executions/" + requestID
	for _, tc := range []struct {
		name       string
		signedPath string
		requestURL string
		body       []byte
		wantStatus int
	}{
		{name: "valid", signedPath: path, requestURL: path},
		{name: "different id", signedPath: path, requestURL: "/internal/external-executions/" + uuid.NewString(), wantStatus: http.StatusUnauthorized},
		{name: "query", signedPath: path, requestURL: path + "?expand=1", wantStatus: http.StatusBadRequest},
		{name: "body", signedPath: path, requestURL: path, body: []byte(`{}`), wantStatus: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replay := &countingReplayStore{seen: map[string]struct{}{}}
			signer, authorizer := testServiceJWT(t, replay)
			token, err := signer.SignRequest(ScopeReadExecution, uuid.NewString(), http.MethodGet, tc.signedPath, tc.body)
			if err != nil {
				t.Fatal(err)
			}
			ctx := bindingContext(http.MethodGet, tc.requestURL, token, "", tc.body)
			ctx.SetParamNames("external_request_id")
			ctx.SetParamValues(requestID)
			err = NewHandler(&countingExecutionHandlerService{}, authorizer).GetExecution(ctx)
			if tc.wantStatus == 0 {
				if err != nil {
					t.Fatalf("valid GET = %v", err)
				}
				return
			}
			assertHTTPStatus(t, err, tc.wantStatus)
			if replay.Calls() != 0 {
				t.Fatalf("invalid GET consumed JTI %d times", replay.Calls())
			}
		})
	}
}

func TestHandlerRequestBindingEnforcesEightMiBBoundary(t *testing.T) {
	path := "/internal/external-execution-targets/validate"
	jsonBody := func(size int) []byte {
		prefix := []byte(`{"padding":"`)
		suffix := []byte(`"}`)
		if size < len(prefix)+len(suffix) {
			t.Fatal("invalid test body size")
		}
		return append(append(prefix, bytes.Repeat([]byte("x"), size-len(prefix)-len(suffix))...), suffix...)
	}
	for _, tc := range []struct {
		name       string
		size       int
		wantStatus int
	}{
		{name: "exact limit", size: maximumExternalExecutionRequestBodyBytes},
		{name: "one byte over", size: maximumExternalExecutionRequestBodyBytes + 1, wantStatus: http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := jsonBody(tc.size)
			replay := &countingReplayStore{seen: map[string]struct{}{}}
			signer, authorizer := testServiceJWT(t, replay)
			token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), http.MethodPost, path, body)
			if err != nil {
				t.Fatal(err)
			}
			err = NewHandler(&countingExecutionHandlerService{}, authorizer).ValidateTarget(
				bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body),
			)
			if tc.wantStatus == 0 {
				if err != nil {
					t.Fatalf("exact limit = %v", err)
				}
				if replay.Calls() != 1 {
					t.Fatalf("exact limit replay calls = %d", replay.Calls())
				}
				return
			}
			assertHTTPStatus(t, err, tc.wantStatus)
			if replay.Calls() != 0 {
				t.Fatalf("oversize body consumed JTI %d times", replay.Calls())
			}
		})
	}
}

func TestHandlerRequestBindingConcurrentReplayCallsServiceOnce(t *testing.T) {
	body := []byte(`{"target_type":"agent","target_id":"target"}`)
	path := "/internal/external-execution-targets/validate"
	replay := &countingReplayStore{seen: map[string]struct{}{}}
	signer, authorizer := testServiceJWT(t, replay)
	token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), http.MethodPost, path, body)
	if err != nil {
		t.Fatal(err)
	}
	service := &countingExecutionHandlerService{}
	handler := NewHandler(service, authorizer)

	const workers = 24
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- handler.ValidateTarget(bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body))
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	successes, replays := 0, 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var httpErr *httpx.HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusConflict && httpErr.Code == httpx.ErrorCode(ErrorCodeJTIReplay) {
			replays++
			continue
		}
		t.Fatalf("unexpected concurrent result: %v", err)
	}
	if successes != 1 || replays != workers-1 || service.validateCalls.Load() != 1 {
		t.Fatalf("successes=%d replays=%d service=%d", successes, replays, service.validateCalls.Load())
	}
}

func TestHandlerRequestBindingRedisFailureOccursAfterValidBinding(t *testing.T) {
	body := []byte(`{"target_type":"agent","target_id":"target"}`)
	path := "/internal/external-execution-targets/validate"
	signer, authorizer := testServiceJWT(t, failingReplayStore{})
	token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), http.MethodPost, path, body)
	if err != nil {
		t.Fatal(err)
	}
	err = NewHandler(&countingExecutionHandlerService{}, authorizer).ValidateTarget(
		bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body),
	)
	assertHTTPStatus(t, err, http.StatusServiceUnavailable)
}

func TestHandlerRequestBindingPreservesBusinessJSONErrorAfterConsumingJTI(t *testing.T) {
	body := []byte(`{"target_type":`)
	path := "/internal/external-execution-targets/validate"
	replay := &countingReplayStore{seen: map[string]struct{}{}}
	signer, authorizer := testServiceJWT(t, replay)
	token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), http.MethodPost, path, body)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(&countingExecutionHandlerService{}, authorizer)
	err = handler.ValidateTarget(bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body))
	assertHTTPStatus(t, err, http.StatusBadRequest)
	if replay.Calls() != 1 {
		t.Fatalf("validly bound malformed JSON replay calls = %d", replay.Calls())
	}
	assertHTTPStatusAndCode(
		t,
		handler.ValidateTarget(bindingContext(http.MethodPost, path, token, echo.MIMEApplicationJSON, body)),
		http.StatusConflict,
		httpx.ErrorCode(ErrorCodeJTIReplay),
	)
}

func TestRequestBindingErrorDoesNotExposeCredentialOrRequestMaterial(t *testing.T) {
	body := []byte(`{"secret":"do-not-log-or-return"}`)
	path := "/internal/external-execution-targets/validate"
	signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	token, err := signer.SignRequest(ScopeValidateTarget, uuid.NewString(), http.MethodPost, path, body)
	if err != nil {
		t.Fatal(err)
	}
	err = NewHandler(&countingExecutionHandlerService{}, authorizer).ValidateTarget(
		bindingContext(http.MethodPost, path+"/different", token, echo.MIMEApplicationJSON, body),
	)
	assertHTTPStatusAndCode(t, err, http.StatusUnauthorized, httpx.ErrorCode(ErrorCodeRequestBindingInvalid))
	message := err.Error()
	digest := RequestBodySHA256(body)
	for _, secret := range []string{token, string(body), digest, path} {
		if strings.Contains(message, secret) {
			t.Fatalf("binding error exposed request material %q in %q", secret, message)
		}
	}
}

func bindingContext(method, target, token, contentType string, body []byte) echo.Context {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	if token != "" {
		req.Header.Set(echo.HeaderAuthorization, "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set(echo.HeaderContentType, contentType)
	}
	return echo.New().NewContext(req, httptest.NewRecorder())
}

func signServiceMapClaims(t *testing.T, signer *ServiceTokenSigner, scope, actor string, extra map[string]any) string {
	t.Helper()
	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"scope": scope, "delegated_actor": actor, "caller_service_id": "openlinker-cloud",
		"iss": "openlinker-cloud", "sub": serviceTokenSubject,
		"aud": []string{"openlinker-core.external-execution"},
		"exp": now.Add(30 * time.Second).Unix(), "nbf": now.Add(-serviceTokenLeeway).Unix(),
		"iat": now.Unix(), "jti": uuid.NewString(),
	}
	for key, value := range extra {
		claims[key] = value
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = signer.keyID
	raw, err := token.SignedString(signer.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func encodePublicKey(privateKey []byte) string {
	return base64.RawStdEncoding.EncodeToString(privateKey[32:])
}

type countingReplayStore struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	calls int
}

func (s *countingReplayStore) Consume(_ context.Context, issuer, jti string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	key := issuer + "\x00" + jti
	if _, exists := s.seen[key]; exists {
		return false, nil
	}
	s.seen[key] = struct{}{}
	return true, nil
}

func (s *countingReplayStore) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type countingExecutionHandlerService struct {
	validateCalls atomic.Int32
	startCalls    atomic.Int32
	getCalls      atomic.Int32
	cancelCalls   atomic.Int32
}

func (s *countingExecutionHandlerService) ValidateTarget(context.Context, *Principal, *TargetValidationRequest) (*TargetValidationResponse, error) {
	s.validateCalls.Add(1)
	return &TargetValidationResponse{}, nil
}

func (s *countingExecutionHandlerService) StartExecution(context.Context, *Principal, *ExecutionRequest) (*ExecutionStartResponse, error) {
	s.startCalls.Add(1)
	return &ExecutionStartResponse{}, nil
}

func (s *countingExecutionHandlerService) GetExecution(context.Context, *Principal, string) (*ExecutionStatusResponse, error) {
	s.getCalls.Add(1)
	return &ExecutionStatusResponse{}, nil
}

func (s *countingExecutionHandlerService) CancelExecution(context.Context, *Principal, string, *ExecutionCancelRequest) (*ExecutionStatusResponse, error) {
	s.cancelCalls.Add(1)
	return &ExecutionStatusResponse{}, nil
}
