package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/sessions"
	"github.com/labstack/echo/v4"
	"github.com/markbates/goth"
	"github.com/markbates/goth/gothic"
	"golang.org/x/oauth2"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const pureAuthSecret = "pure-auth-secret-32-chars-aaaaaa"

func TestAuthHandlerValidationOAuthHelpersAndRoutes(t *testing.T) {
	h := NewHandler(&Service{}, &config.Config{
		FrontendURL:        "https://app.example",
		GoogleClientID:     "google-id",
		GoogleClientSecret: "google-secret",
		GithubClientID:     "github-id",
		GithubClientSecret: "github-secret",
	})
	if !h.googleConfigured() || !h.githubConfigured() {
		t.Fatalf("oauth providers should be configured")
	}
	if NewHandler(&Service{}).googleConfigured() || NewHandler(&Service{}).githubConfigured() {
		t.Fatalf("nil config should not be configured")
	}
	if nonEmpty("", "nick", "name") != "nick" || nonEmpty("", "") != "" {
		t.Fatalf("nonEmpty failed")
	}

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    *authHandlerRequest
		want   int
	}{
		{name: "login invalid json", method: h.PostLogin, req: &authHandlerRequest{method: http.MethodPost, target: "/", body: "{"}, want: http.StatusBadRequest},
		{name: "login validation", method: h.PostLogin, req: &authHandlerRequest{method: http.MethodPost, target: "/", body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "oauth exchange invalid json", method: h.PostOAuthExchange, req: &authHandlerRequest{method: http.MethodPost, target: "/", body: "{"}, want: http.StatusBadRequest},
		{name: "oauth exchange validation", method: h.PostOAuthExchange, req: &authHandlerRequest{method: http.MethodPost, target: "/", body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "me missing user", method: h.GetMe, req: &authHandlerRequest{method: http.MethodGet, target: "/"}, want: http.StatusUnauthorized},
		{name: "me invalid user", method: h.GetMe, req: &authHandlerRequest{method: http.MethodGet, target: "/", userID: "bad"}, want: http.StatusUnauthorized},
		{name: "patch missing user", method: h.PatchMe, req: &authHandlerRequest{method: http.MethodPatch, target: "/"}, want: http.StatusUnauthorized},
		{name: "patch invalid json", method: h.PatchMe, req: &authHandlerRequest{method: http.MethodPatch, target: "/", userID: uuid.NewString(), body: "{"}, want: http.StatusBadRequest},
		{name: "patch validation", method: h.PatchMe, req: &authHandlerRequest{method: http.MethodPatch, target: "/", userID: uuid.NewString(), body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "password missing user", method: h.PostChangePassword, req: &authHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "password invalid json", method: h.PostChangePassword, req: &authHandlerRequest{method: http.MethodPost, target: "/", userID: uuid.NewString(), body: "{"}, want: http.StatusBadRequest},
		{name: "password validation", method: h.PostChangePassword, req: &authHandlerRequest{method: http.MethodPost, target: "/", userID: uuid.NewString(), body: `{}`}, want: http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newAuthTestContext(tc.req)
			requireAuthHTTPStatus(t, tc.method(c), tc.want)
		})
	}

	c := newAuthTestContext(&authHandlerRequest{method: http.MethodGet, target: "/", userID: uuid.NewString()})
	if got, err := currentUserID(c); err != nil || got.String() != httpx.UserIDFrom(c) {
		t.Fatalf("currentUserID valid = %s %v", got, err)
	}
	c = newAuthTestContext(&authHandlerRequest{method: http.MethodGet, target: "/", userID: "bad"})
	requireAuthHTTPStatus(t, currentUserIDOnly(c), http.StatusUnauthorized)

	unconfigured := NewHandler(&Service{})
	requireAuthHTTPStatus(t, unconfigured.GoogleStart(newAuthTestContext(&authHandlerRequest{method: http.MethodGet, target: "/"})), http.StatusInternalServerError)
	requireAuthHTTPStatus(t, unconfigured.GoogleCallback(newAuthTestContext(&authHandlerRequest{method: http.MethodGet, target: "/"})), http.StatusInternalServerError)

	callbackOnly := NewHandler(&Service{}, &config.Config{FrontendURL: "https://app.example"})
	rec := httptest.NewRecorder()
	c = echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
	if err := callbackOnly.GithubCallback(c); err != nil {
		t.Fatalf("unconfigured callback should redirect with error: %v", err)
	}
	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("callback status = %d", rec.Code)
	}
	location := rec.Header().Get(echo.HeaderLocation)
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse redirect: %v", err)
	}
	if parsed.Scheme != "https" || parsed.Host != "app.example" || parsed.Query().Get("error") == "" {
		t.Fatalf("unexpected callback redirect = %q", location)
	}

	e := echo.New()
	api := e.Group("/api/v1")
	noop := func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	h.Register(api)
	h.RegisterProtected(api, noop)
	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/auth/register",
		"POST /api/v1/auth/login",
		"POST /api/v1/auth/oauth/exchange",
		"GET /api/v1/auth/google",
		"GET /api/v1/auth/google/callback",
		"GET /api/v1/auth/github",
		"GET /api/v1/auth/github/callback",
		"GET /api/v1/me",
		"PATCH /api/v1/me",
		"POST /api/v1/me/password",
	} {
		if !routes[route] {
			t.Fatalf("missing route %s", route)
		}
	}
}

func TestOAuthStartAndCallbackRedirectWithCodeProviderAndCallbackURL(t *testing.T) {
	goth.ClearProviders()
	t.Cleanup(goth.ClearProviders)
	gothic.Store = sessions.NewCookieStore([]byte(pureAuthSecret))
	goth.UseProviders(&testOAuthProvider{
		name:       providerGoogle,
		userID:     "google-user-1",
		email:      "oauth@example.com",
		display:    "OAuth User",
		avatarURL:  "https://cdn.example/avatar.png",
		authOrigin: "https://accounts.example",
	})

	userID := uuid.New()
	authResp := &AuthResponse{
		UserID:      userID.String(),
		Email:       "oauth@example.com",
		DisplayName: "OAuth User",
		JWT:         "jwt-token",
	}
	mock := &mockAuthService{
		oauthResp:       authResp,
		issuedOAuthCode: strings.Repeat("c", 64),
	}
	h := NewHandler(mock, &config.Config{
		FrontendURL:        "https://app.example/",
		GoogleClientID:     "google-id",
		GoogleClientSecret: "google-secret",
	})

	e := echo.New()
	startRec := httptest.NewRecorder()
	startReq := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/auth/google?callbackUrl="+url.QueryEscape("/runs?tab=mine"),
		nil,
	)
	startCtx := e.NewContext(startReq, startRec)
	if err := h.GoogleStart(startCtx); err != nil {
		t.Fatalf("GoogleStart error = %v", err)
	}
	if startRec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("start status = %d body=%s", startRec.Code, startRec.Body.String())
	}
	startURL, err := url.Parse(startRec.Header().Get(echo.HeaderLocation))
	if err != nil {
		t.Fatalf("parse start redirect: %v", err)
	}
	state := startURL.Query().Get("state")
	if startURL.Scheme != "https" || startURL.Host != "accounts.example" || callbackFromOAuthState(state) != "/runs?tab=mine" {
		t.Fatalf("unexpected start redirect = %q state callback=%q", startURL.String(), callbackFromOAuthState(state))
	}

	callbackRec := httptest.NewRecorder()
	callbackReq := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/auth/google/callback?state="+url.QueryEscape(state)+"&code=provider-code",
		nil,
	)
	for _, cookie := range startRec.Result().Cookies() {
		callbackReq.AddCookie(cookie)
	}
	callbackCtx := e.NewContext(callbackReq, callbackRec)
	if err := h.GoogleCallback(callbackCtx); err != nil {
		t.Fatalf("GoogleCallback error = %v", err)
	}
	if callbackRec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("callback status = %d body=%s", callbackRec.Code, callbackRec.Body.String())
	}

	callbackURL, err := url.Parse(callbackRec.Header().Get(echo.HeaderLocation))
	if err != nil {
		t.Fatalf("parse callback redirect: %v", err)
	}
	if callbackURL.Scheme != "https" || callbackURL.Host != "app.example" || callbackURL.Path != "/auth/callback" {
		t.Fatalf("unexpected frontend callback = %q", callbackURL.String())
	}
	if callbackURL.Query().Get("provider") != providerGoogle ||
		callbackURL.Query().Get("code") != strings.Repeat("c", 64) ||
		callbackURL.Query().Get("callbackUrl") != "/runs?tab=mine" {
		t.Fatalf("unexpected frontend callback query = %s", callbackURL.RawQuery)
	}
	if mock.oauthProvider != providerGoogle ||
		mock.oauthID != "google-user-1" ||
		mock.oauthEmail != "oauth@example.com" ||
		mock.oauthDisplayName != "OAuth User" ||
		mock.oauthAvatarURL != "https://cdn.example/avatar.png" {
		t.Fatalf("captured oauth user = provider=%q id=%q email=%q display=%q avatar=%q",
			mock.oauthProvider, mock.oauthID, mock.oauthEmail, mock.oauthDisplayName, mock.oauthAvatarURL)
	}
	if mock.issuedOAuthResp != authResp {
		t.Fatalf("issued oauth response = %#v", mock.issuedOAuthResp)
	}
}

func TestOAuthStateHelpersSanitizeCallbackURL(t *testing.T) {
	state, err := newOAuthState("/hub?tab=agents")
	if err != nil {
		t.Fatalf("newOAuthState: %v", err)
	}
	if got := callbackFromOAuthState(state); got != "/hub?tab=agents" {
		t.Fatalf("callbackFromOAuthState = %q", got)
	}

	for _, raw := range []string{"https://evil.example", "//evil.example/path", "", "settings"} {
		state, err := newOAuthState(raw)
		if err != nil {
			t.Fatalf("newOAuthState(%q): %v", raw, err)
		}
		if got := callbackFromOAuthState(state); got != "/" {
			t.Fatalf("unsafe callback %q decoded as %q", raw, got)
		}
	}
	if got := callbackFromOAuthState("not-a-valid-state"); got != "/" {
		t.Fatalf("invalid state decoded as %q", got)
	}
}

func TestHybridAuthMiddlewareBranches(t *testing.T) {
	jwtUID := uuid.NewString()
	token, err := GenerateToken(jwtUID, pureAuthSecret, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	rec, got := invokeHybridAuth(t, pureAuthSecret, nil, "Bearer "+token)
	if rec.Code != http.StatusOK || got.userID != jwtUID || got.authMethod != "jwt" {
		t.Fatalf("jwt branch code=%d got=%+v", rec.Code, got)
	}

	apiUID := uuid.New()
	verifier := &fakeAPIKeyVerifier{userID: apiUID, scopes: []string{"runs:read", "agents:write"}}
	for _, token := range []string{"ol_user_abc", "ol_user_test"} {
		t.Run(token, func(t *testing.T) {
			rec, got := invokeHybridAuth(t, pureAuthSecret, verifier, "Bearer "+token)
			if rec.Code != http.StatusOK || got.userID != apiUID.String() || got.authMethod != "user_token" {
				t.Fatalf("apikey branch code=%d got=%+v", rec.Code, got)
			}
			if verifier.seenToken != token {
				t.Fatalf("verifier token = %q", verifier.seenToken)
			}
			if !reflect.DeepEqual(got.scopes, verifier.scopes) {
				t.Fatalf("scopes = %#v", got.scopes)
			}
		})
	}

	for _, tc := range []struct {
		name     string
		header   string
		verifier ApiKeyVerifier
	}{
		{name: "missing header"},
		{name: "bad format", header: "Bearer"},
		{name: "user token verifier missing", header: "Bearer ol_user_abc"},
		{name: "user token verifier rejects", header: "Bearer ol_user_abc", verifier: &fakeAPIKeyVerifier{err: errors.New("revoked")}},
		{name: "jwt invalid", header: "Bearer not.a.jwt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, got := invokeHybridAuth(t, pureAuthSecret, tc.verifier, tc.header)
			if rec.Code != http.StatusUnauthorized || got.nextCalled {
				t.Fatalf("expected unauthorized without next, code=%d got=%+v", rec.Code, got)
			}
		})
	}
}

func TestAuthServicePureHelpers(t *testing.T) {
	userID := uuid.New()
	svc := &Service{jwtSecret: pureAuthSecret, jwtExpire: time.Hour}
	resp, err := svc.respondWithToken(&db.User{
		ID:          userID,
		Email:       "user@example.com",
		DisplayName: "User",
	})
	if err != nil {
		t.Fatalf("respondWithToken: %v", err)
	}
	if resp.UserID != userID.String() || resp.Email != "user@example.com" || resp.DisplayName != "User" || resp.JWT == "" {
		t.Fatalf("auth response = %+v", resp)
	}
	if got, err := ParseToken(resp.JWT, pureAuthSecret); err != nil || got != userID.String() {
		t.Fatalf("response token = %s %v", got, err)
	}

	if isUniqueViolation(nil) || isUniqueViolation(errors.New("plain")) {
		t.Fatalf("non-sqlstate errors should not be unique violations")
	}
	if !isUniqueViolation(fakeSQLState("23505")) || isUniqueViolation(fakeSQLState("23503")) {
		t.Fatalf("isUniqueViolation SQLState handling failed")
	}
}

type authHandlerRequest struct {
	method  string
	target  string
	body    string
	userID  string
	headers map[string]string
}

func newAuthTestContext(spec *authHandlerRequest) echo.Context {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	return c
}

func requireAuthHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if he.Status != want {
		t.Fatalf("HTTP status = %d (%s), want %d", he.Status, he.Message, want)
	}
}

func currentUserIDOnly(c echo.Context) error {
	_, err := currentUserID(c)
	return err
}

type hybridCapture struct {
	userID     string
	authMethod string
	scopes     []string
	nextCalled bool
}

func invokeHybridAuth(t *testing.T, secret string, verifier ApiKeyVerifier, authHeader string) (*httptest.ResponseRecorder, hybridCapture) {
	t.Helper()
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}
		_ = httpx.SendError(c, err)
	}
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	if authHeader != "" {
		req.Header.Set(echo.HeaderAuthorization, authHeader)
	}
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	got := hybridCapture{}
	next := func(c echo.Context) error {
		got.nextCalled = true
		got.userID, _ = c.Get(string(httpx.CtxKeyUserID)).(string)
		got.authMethod, _ = c.Get(string(httpx.CtxKeyAuthMethod)).(string)
		got.scopes, _ = c.Get(string(httpx.CtxKeyAuthScopes)).([]string)
		return c.NoContent(http.StatusOK)
	}
	if err := HybridAuthMiddleware(secret, verifier)(next)(c); err != nil {
		e.HTTPErrorHandler(err, c)
	}
	return rec, got
}

type testOAuthProvider struct {
	name       string
	userID     string
	email      string
	display    string
	avatarURL  string
	authOrigin string
}

func (p *testOAuthProvider) Name() string {
	return p.name
}

func (p *testOAuthProvider) SetName(name string) {
	p.name = name
}

func (p *testOAuthProvider) BeginAuth(state string) (goth.Session, error) {
	return &testOAuthSession{
		AuthURL: fmt.Sprintf("%s/auth?state=%s", p.authOrigin, url.QueryEscape(state)),
		UserID:  p.userID,
		Email:   p.email,
		Name:    p.display,
		Avatar:  p.avatarURL,
	}, nil
}

func (p *testOAuthProvider) UnmarshalSession(data string) (goth.Session, error) {
	var sess testOAuthSession
	if err := json.Unmarshal([]byte(data), &sess); err != nil {
		return nil, err
	}
	return &sess, nil
}

func (p *testOAuthProvider) FetchUser(session goth.Session) (goth.User, error) {
	sess, ok := session.(*testOAuthSession)
	if !ok {
		return goth.User{}, errors.New("unexpected session")
	}
	if sess.AccessToken == "" {
		return goth.User{}, errors.New("missing access token")
	}
	return goth.User{
		UserID:      sess.UserID,
		Email:       sess.Email,
		Name:        sess.Name,
		AvatarURL:   sess.Avatar,
		Provider:    p.name,
		AccessToken: sess.AccessToken,
	}, nil
}

func (p *testOAuthProvider) Debug(bool) {}

func (p *testOAuthProvider) RefreshToken(string) (*oauth2.Token, error) {
	return nil, nil
}

func (p *testOAuthProvider) RefreshTokenAvailable() bool {
	return false
}

type testOAuthSession struct {
	AuthURL     string
	UserID      string
	Email       string
	Name        string
	Avatar      string
	AccessToken string
}

func (s *testOAuthSession) GetAuthURL() (string, error) {
	return s.AuthURL, nil
}

func (s *testOAuthSession) Marshal() string {
	raw, _ := json.Marshal(s)
	return string(raw)
}

func (s *testOAuthSession) Authorize(goth.Provider, goth.Params) (string, error) {
	s.AccessToken = "test-access-token"
	return s.AccessToken, nil
}

type fakeAPIKeyVerifier struct {
	userID    uuid.UUID
	scopes    []string
	err       error
	seenToken string
}

func (f *fakeAPIKeyVerifier) Verify(_ context.Context, plaintextKey string) (uuid.UUID, []string, error) {
	f.seenToken = plaintextKey
	if f.err != nil {
		return uuid.Nil, nil, f.err
	}
	return f.userID, f.scopes, nil
}

type fakeSQLState string

func (f fakeSQLState) Error() string {
	return string(f)
}

func (f fakeSQLState) SQLState() string {
	return string(f)
}
