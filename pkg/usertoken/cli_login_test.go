package usertoken

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
)

func TestCLIRedirectValidation(t *testing.T) {
	for _, value := range []string{"http://127.0.0.1:32123/callback", "http://[::1]:42/callback"} {
		if !validCLIRedirect(value) {
			t.Errorf("valid redirect rejected: %s", value)
		}
	}
	for _, value := range []string{"https://example.test/callback", "http://localhost:42/callback", "http://127.0.0.1/callback", "http://127.0.0.1:99999/callback", "http://127.0.0.1:0/callback", "http://user@127.0.0.1:42/callback", "http://127.0.0.1:42/callback?x=1", "http://127.0.0.1:42/callback#x", "http://127.0.0.1:42/%63allback", "http://127.0.0.1:42/other", "http://0.0.0.0:42/callback"} {
		if validCLIRedirect(value) {
			t.Errorf("unsafe redirect accepted: %s", value)
		}
	}
}

type cliFixture struct {
	t       *testing.T
	e       *echo.Echo
	pool    *pgxpool.Pool
	user    uuid.UUID
	jwt, ip string
}

func newCLIFixture(t *testing.T, proxies ...*net.IPNet) *cliFixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for CLI login PostgreSQL integration")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.cli_login_requests') IS NOT NULL`).Scan(&exists); err != nil || !exists {
		t.Fatal("CLI login integration requires migration 094", err)
	}
	uid := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'test-only','CLI tester')`, uid, uid.String()+"@example.test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM users WHERE id=$1`, uid) })
	secret := "test-only-cli-jwt-secret-with-32-bytes"
	jwt, err := auth.GenerateToken(uid.String(), secret, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	svc := NewService(pool)
	NewCLILoginHandler(svc, "https://platform.example.test", proxies...).Register(e.Group("/api/v1"), auth.JWTMiddlewareWithUserStatus(secret, auth.NewDBUserStatusChecker(pool)))
	return &cliFixture{t: t, e: e, pool: pool, user: uid, jwt: jwt, ip: fmt.Sprintf("[2001:db8:%x:%x:%x:%x:%x:%x]:1234", uid[0:2], uid[2:4], uid[4:6], uid[6:8], uid[8:10], uid[10:12])}
}
func (f *cliFixture) call(method, path, credential string, body any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest(method, "/api/v1/cli-auth"+path, bytes.NewReader(raw))
	r.RemoteAddr = f.ip
	r.Header.Set("Content-Type", "application/json")
	if credential != "" {
		r.Header.Set("Authorization", "Bearer "+credential)
	}
	w := httptest.NewRecorder()
	f.e.ServeHTTP(w, r)
	return w
}
func cliJSON(t *testing.T, w *httptest.ResponseRecorder, status int) map[string]any {
	t.Helper()
	if w.Code != status {
		t.Fatalf("response: HTTP %d %s", w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("missing no-store")
	}
	return out
}
func (f *cliFixture) start(browser bool) (map[string]any, string) {
	f.t.Helper()
	verifier, _ := cliRandom()
	hash := sha256.Sum256([]byte(verifier))
	req := cliStartRequest{CodeChallenge: base64.RawURLEncoding.EncodeToString(hash[:]), CodeChallengeMethod: "S256", Scopes: []string{"agents:read", "runs:read"}}
	if browser {
		req.RedirectURI = "http://127.0.0.1:54321/callback"
		req.State = strings.Repeat("s", 43)
	}
	return cliJSON(f.t, f.call("POST", "/start", "", req), 201), verifier
}
func (f *cliFixture) approve(start map[string]any, approve bool) map[string]any {
	return cliJSON(f.t, f.call("POST", "/decision", f.jwt, map[string]any{"user_code": start["user_code"], "approve": approve}), 200)
}
func browserExchange(t *testing.T, decision map[string]any, verifier string) map[string]any {
	t.Helper()
	u, err := url.Parse(decision["redirect_uri"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("state") != strings.Repeat("s", 43) {
		t.Fatal("state lost")
	}
	return map[string]any{"grant_type": "authorization_code", "code": u.Query().Get("code"), "code_verifier": verifier, "redirect_uri": "http://127.0.0.1:54321/callback"}
}

func TestCLILoginBrowserPKCEReplayAndRevocationIntegration(t *testing.T) {
	f := newCLIFixture(t)
	start, verifier := f.start(true)
	inspect := cliJSON(t, f.call("POST", "/request", f.jwt, map[string]any{"user_code": start["user_code"]}), 200)
	if inspect["instance_url"] != "https://platform.example.test" || strings.Contains(fmt.Sprint(inspect), start["device_code"].(string)) {
		t.Fatal("unsafe request display")
	}
	if f.call("POST", "/decision", "", map[string]any{"user_code": start["user_code"], "approve": true}).Code != 401 {
		t.Fatal("anonymous approval accepted")
	}
	decision := f.approve(start, true)
	exchange := browserExchange(t, decision, verifier)
	bad := browserExchange(t, decision, strings.Repeat("x", 43))
	cliJSON(t, f.call("POST", "/token", "", bad), 400)
	bad = browserExchange(t, decision, verifier)
	bad["redirect_uri"] = "http://127.0.0.1:54322/callback"
	cliJSON(t, f.call("POST", "/token", "", bad), 400)
	result := cliJSON(t, f.call("POST", "/token", "", exchange), 200)
	token := result["access_token"].(string)
	if !strings.HasPrefix(token, "ol_user_") {
		t.Fatal("wrong credential kind")
	}
	cliJSON(t, f.call("POST", "/token", "", exchange), 400)
	if f.call("POST", "/request", token, map[string]any{"user_code": start["user_code"]}).Code != 401 {
		t.Fatal("User Token approved login")
	}
	session := cliJSON(t, f.call("GET", "/session", token, nil), 200)
	if account := session["user"].(map[string]any); len(account) != 1 || account["id"] != f.user.String() {
		t.Fatal("narrow-scope credential exposed account profile")
	}
	if strings.Contains(fmt.Sprint(session), token) || strings.Contains(fmt.Sprint(session), "plaintext_token") {
		t.Fatal("session leaked credential")
	}
	p, err := NewService(f.pool).VerifyPrincipal(context.Background(), token)
	if err != nil || p.Allows("agents:run", "agent", nil) || !p.Allows("agents:read", "agent", nil) {
		t.Fatal("scope was broadened")
	}
	if f.call("DELETE", "/session", token, nil).Code != 204 || f.call("GET", "/session", token, nil).Code != 401 {
		t.Fatal("revocation ineffective")
	}
}

func TestCLILoginDevicePollingDenialExpiryAndAccountInvalidationIntegration(t *testing.T) {
	for _, scenario := range []string{"approve", "deny", "expired", "disabled", "password-change"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCLIFixture(t)
			start, verifier := f.start(false)
			payload := map[string]any{"grant_type": "urn:ietf:params:oauth:grant-type:device_code", "device_code": start["device_code"], "code_verifier": verifier}
			pending := cliJSON(t, f.call("POST", "/token", "", payload), 400)
			if pending["error"].(map[string]any)["code"] != "AUTHORIZATION_PENDING" {
				t.Fatal(pending)
			}
			slow := cliJSON(t, f.call("POST", "/token", "", payload), 400)
			if slow["error"].(map[string]any)["code"] != "SLOW_DOWN" {
				t.Fatal(slow)
			}
			f.approve(start, scenario != "deny")
			ctx := context.Background()
			_, err := f.pool.Exec(ctx, `UPDATE cli_login_requests SET last_poll_at=now()-interval '1 minute' WHERE device_hash=$1`, cliHash(start["device_code"].(string)))
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "expired":
				_, err = f.pool.Exec(ctx, `UPDATE cli_login_requests SET expires_at=now()-interval '1 second' WHERE device_hash=$1`, cliHash(start["device_code"].(string)))
			case "disabled":
				_, err = f.pool.Exec(ctx, `UPDATE users SET disabled_at=now() WHERE id=$1`, f.user)
			case "password-change":
				_, err = f.pool.Exec(ctx, `UPDATE users SET token_version=token_version+1 WHERE id=$1`, f.user)
			}
			if err != nil {
				t.Fatal(err)
			}
			w := f.call("POST", "/token", "", payload)
			if scenario == "approve" {
				cliJSON(t, w, 200)
			} else if w.Code == 200 {
				t.Fatal("invalid grant issued a credential")
			}
			var count int
			f.pool.QueryRow(ctx, `SELECT count(*) FROM user_tokens WHERE user_id=$1`, f.user).Scan(&count)
			if (scenario == "approve" && count != 1) || (scenario != "approve" && count != 0) {
				t.Fatalf("unexpected issued token count %d", count)
			}
		})
	}
}

func TestCLILoginConcurrentExchangeMintsOnceIntegration(t *testing.T) {
	f := newCLIFixture(t)
	start, verifier := f.start(true)
	exchange := browserExchange(t, f.approve(start, true), verifier)
	var wg sync.WaitGroup
	var successes atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if f.call("POST", "/token", "", exchange).Code == 200 {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	var count int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM user_tokens WHERE user_id=$1`, f.user).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if successes.Load() != 1 || count != 1 {
		t.Fatalf("successes=%d tokens=%d", successes.Load(), count)
	}
}

func TestCLILoginRejectsExpandedScopesAndRateLimitsIntegration(t *testing.T) {
	f := newCLIFixture(t)
	req := cliStartRequest{CodeChallenge: strings.Repeat("a", 43), CodeChallengeMethod: "S256", Scopes: []string{"agent-tokens:issue"}}
	cliJSON(t, f.call("POST", "/start", "", req), 400)
	req.Scopes = []string{"agents:read"}
	for i := 0; i < 10; i++ {
		cliJSON(t, f.call("POST", "/start", "", req), 201)
	}
	if f.call("POST", "/start", "", req).Code != 429 {
		t.Fatal("start flood was not limited")
	}
}

func TestCLILoginQuotaFailureDoesNotConsumeGrantIntegration(t *testing.T) {
	f := newCLIFixture(t)
	ctx := context.Background()
	svc := NewService(f.pool)
	start, verifier := f.start(true)
	exchange := browserExchange(t, f.approve(start, true), verifier)
	var token *TokenResponse
	for i := 0; i < 10; i++ {
		var err error
		token, err = svc.Create(ctx, f.user, &CreateRequest{Name: "quota test", Scopes: []string{"agents:read"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	full, _ := f.start(false)
	for _, response := range []*httptest.ResponseRecorder{
		f.call("POST", "/decision", f.jwt, map[string]any{"user_code": full["user_code"], "approve": true}),
		f.call("POST", "/token", "", exchange),
	} {
		result := cliJSON(t, response, 409)
		if result["error"].(map[string]any)["code"] != "TOKEN_QUOTA_EXCEEDED" {
			t.Fatal("missing actionable quota code")
		}
	}
	id, _ := uuid.Parse(token.ID)
	if err := svc.Revoke(ctx, f.user, id); err != nil {
		t.Fatal(err)
	}
	cliJSON(t, f.call("POST", "/token", "", exchange), 200)
}

func TestCLILoginRejectsCrossModeExchangeIntegration(t *testing.T) {
	f := newCLIFixture(t)
	browser, verifier := f.start(true)
	decision := f.approve(browser, true)
	cliJSON(t, f.call("POST", "/token", "", map[string]any{"grant_type": "urn:ietf:params:oauth:grant-type:device_code", "device_code": browser["device_code"], "code_verifier": verifier}), 400)
	cliJSON(t, f.call("POST", "/token", "", browserExchange(t, decision, verifier)), 200)
	device, verifier := f.start(false)
	f.approve(device, true)
	cliJSON(t, f.call("POST", "/token", "", map[string]any{"grant_type": "authorization_code", "code": device["device_code"], "code_verifier": verifier, "redirect_uri": "http://127.0.0.1:54321/callback"}), 400)
	cliJSON(t, f.call("POST", "/token", "", map[string]any{"grant_type": "urn:ietf:params:oauth:grant-type:device_code", "device_code": device["device_code"], "code_verifier": verifier}), 200)
}

func TestCLILoginProxyTrust(t *testing.T) {
	_, proxy, _ := net.ParseCIDR("10.2.3.0/24")
	for _, tc := range []struct{ peer, xff, want string }{
		{"198.51.100.4:1234", "203.0.113.9", "198.51.100.4"},
		{"10.2.3.4:1234", "203.0.113.9, 198.51.100.4", "198.51.100.4"},
		{"10.2.3.4:1234", "203.0.113.9, 10.2.3.5", "203.0.113.9"},
		{"127.0.0.1:1234", "203.0.113.9", "127.0.0.1"},
		{"10.2.3.4:1234", "garbage", "10.2.3.4"},
	} {
		r := httptest.NewRequest("POST", "/start", nil)
		r.RemoteAddr = tc.peer
		r.Header.Set("X-Forwarded-For", tc.xff)
		r.Header.Set("X-Real-IP", "192.0.2.9")
		if got := NewCLILoginHandler(nil, "", proxy).clientIP(r); got != tc.want {
			t.Fatalf("peer=%s xff=%s got=%s want=%s", tc.peer, tc.xff, got, tc.want)
		}
	}
}

func TestCLILoginSpoofedForwardedIPCannotBypassLimitIntegration(t *testing.T) {
	f := newCLIFixture(t)
	body, _ := json.Marshal(cliStartRequest{CodeChallenge: strings.Repeat("a", 43), CodeChallengeMethod: "S256", Scopes: []string{"agents:read"}})
	for i := 0; i < 11; i++ {
		r := httptest.NewRequest("POST", "/api/v1/cli-auth/start", bytes.NewReader(body))
		r.RemoteAddr = f.ip
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Forwarded-For", fmt.Sprintf("192.0.2.%d", i+1))
		r.Header.Set("X-Real-IP", fmt.Sprintf("192.0.2.%d", i+1))
		w := httptest.NewRecorder()
		f.e.ServeHTTP(w, r)
		want := 201
		if i == 10 {
			want = 429
		}
		cliJSON(t, w, want)
	}
}

func TestCLILoginPasswordChangeBetweenMiddlewareAndApprovalIntegration(t *testing.T) {
	f := newCLIFixture(t)
	start, _ := f.start(true)
	h := NewCLILoginHandler(NewService(f.pool), "https://platform.example.test")
	f.e.POST("/api/v1/cli-auth/race", h.Decide,
		auth.JWTMiddlewareWithUserStatus("test-only-cli-jwt-secret-with-32-bytes", auth.NewDBUserStatusChecker(f.pool)),
		func(next echo.HandlerFunc) echo.HandlerFunc {
			return func(c echo.Context) error {
				if _, err := f.pool.Exec(c.Request().Context(), `UPDATE users SET token_version=token_version+1 WHERE id=$1`, f.user); err != nil {
					return err
				}
				return next(c)
			}
		})
	if response := f.call("POST", "/race", f.jwt, map[string]any{"user_code": start["user_code"], "approve": true}); response.Code != 401 {
		t.Fatalf("stale JWT approved login: HTTP %d", response.Code)
	}
}

func TestCLILoginSkillScopesRequireExplicitBrowserApprovalIntegration(t *testing.T) {
	f := newCLIFixture(t)
	verifier, _ := cliRandom()
	hash := sha256.Sum256([]byte(verifier))
	scopes := []string{"skill-packages:read", "skill-packages:import", "skill-bindings:read", "skill-bindings:manage"}
	req := cliStartRequest{CodeChallenge: base64.RawURLEncoding.EncodeToString(hash[:]), CodeChallengeMethod: "S256", Scopes: scopes, RedirectURI: "http://127.0.0.1:54321/callback", State: strings.Repeat("s", 43)}
	start := cliJSON(t, f.call("POST", "/start", "", req), 201)
	inspect := cliJSON(t, f.call("POST", "/request", f.jwt, map[string]any{"user_code": start["user_code"]}), 200)
	displayed := inspect["scopes"].([]any)
	if len(displayed) != len(scopes) {
		t.Fatal("skill approval scope count changed")
	}
	for i, p := range displayed {
		if p != scopes[i] {
			t.Fatal("approval scope changed")
		}
	}
	result := cliJSON(t, f.call("POST", "/token", "", browserExchange(t, f.approve(start, true), verifier)), 200)
	principal, err := NewService(f.pool).VerifyPrincipal(context.Background(), result["access_token"].(string))
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range scopes {
		kind := "agent"
		if strings.HasPrefix(scope, "skill-packages:") {
			kind = "skill_package"
		}
		if !principal.Allows(scope, kind, nil) {
			t.Fatal("approved skill scope absent", scope)
		}
	}
	if principal.Allows("agents:run", "agent", nil) || principal.Allows("agent-tokens:issue", "agent", nil) {
		t.Fatal("explicit skill approval broadened other authority")
	}
}
