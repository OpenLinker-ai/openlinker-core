package usertoken

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const cliLoginTTL = 10 * time.Minute
const cliTokenTTL = 30 * 24 * time.Hour

var cliScopes = []string{"agents:read", "agents:run", "runs:read", "runs:cancel", "tasks:create"}
var cliProof = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
var cliVerifier = regexp.MustCompile(`^[A-Za-z0-9._~-]{43,128}$`)
var cliUserCode = regexp.MustCompile(`^[A-Z2-9]{8}$`)

// CLILoginHandler owns platform client authorization only. The browser approves
// a narrow User Token; it never hands its JWT or a Runtime credential to the CLI.
type CLILoginHandler struct {
	svc      *Service
	frontend string
	clientIP echo.IPExtractor
}

func NewCLILoginHandler(svc *Service, frontend string, trustedProxies ...*net.IPNet) *CLILoginHandler {
	ip := echo.ExtractIPDirect()
	if len(trustedProxies) > 0 {
		options := []echo.TrustOption{echo.TrustLoopback(false), echo.TrustLinkLocal(false), echo.TrustPrivateNet(false)}
		for _, network := range trustedProxies {
			options = append(options, echo.TrustIPRange(network))
		}
		ip = echo.ExtractIPFromXFFHeader(options...)
	}
	return &CLILoginHandler{svc: svc, frontend: strings.TrimRight(frontend, "/"), clientIP: ip}
}

func (h *CLILoginHandler) Register(api *echo.Group, jwt echo.MiddlewareFunc) {
	g := api.Group("/cli-auth", func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Response().Header().Set("Cache-Control", "no-store")
			c.Response().Header().Set("Pragma", "no-cache")
			c.Response().Header().Set("Referrer-Policy", "no-referrer")
			return next(c)
		}
	})
	g.POST("/start", h.Start)
	g.POST("/token", h.Exchange)
	g.POST("/request", h.Inspect, jwt)
	g.POST("/decision", h.Decide, jwt)
	g.GET("/session", h.Session)
	g.DELETE("/session", h.Logout)
}

type cliStartRequest struct {
	CodeChallenge       string   `json:"code_challenge"`
	CodeChallengeMethod string   `json:"code_challenge_method"`
	RedirectURI         string   `json:"redirect_uri"`
	State               string   `json:"state"`
	Scopes              []string `json:"scopes"`
}

type cliGrant struct {
	DeviceHash  string
	Challenge   string
	RedirectURI string
	State       string
	Scopes      []string
	Status      string
	UserID      *uuid.UUID
	UserVersion *int64
	ExpiresAt   time.Time
	LastPoll    *time.Time
	Interval    int
}

func cliError(code string) error {
	return httpx.NewError(http.StatusBadRequest, httpx.ErrorCode(code), code)
}

func cliHash(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func cliRandom() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func validCLIRedirect(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/callback" || u.RawPath != "" {
		return false
	}
	if u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
		return false
	}
	port, err := strconv.Atoi(u.Port())
	return err == nil && port > 0 && port <= 65535
}

func cliDecode(c echo.Context, dst any) error {
	if !strings.HasPrefix(strings.ToLower(c.Request().Header.Get("Content-Type")), "application/json") {
		return httpx.BadRequest("JSON required")
	}
	d := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, 4096))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return httpx.BadRequest("Invalid CLI authorization request")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return httpx.BadRequest("Invalid CLI authorization request")
	}
	return nil
}

// These counters remain effective across processes and reject on storage errors.
func (h *CLILoginHandler) limit(ctx context.Context, key string, max int) error {
	var hits int
	err := h.svc.pool.QueryRow(ctx, `INSERT INTO cli_login_rate_limits(key,hits,expires_at)
	 VALUES($1,1,now()+interval '10 minutes') ON CONFLICT(key) DO UPDATE SET
	 hits=CASE WHEN cli_login_rate_limits.expires_at < now() THEN 1 ELSE cli_login_rate_limits.hits+1 END,
	 expires_at=CASE WHEN cli_login_rate_limits.expires_at < now() THEN now()+interval '10 minutes' ELSE cli_login_rate_limits.expires_at END
	 RETURNING hits`, cliHash(key)).Scan(&hits)
	if err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	if hits > max {
		return httpx.NewError(429, httpx.CodeRateLimited, "Too many CLI authorization requests")
	}
	return nil
}

func (h *CLILoginHandler) Start(c echo.Context) error {
	var req cliStartRequest
	if err := cliDecode(c, &req); err != nil {
		return err
	}
	if req.CodeChallengeMethod != "S256" || !cliProof.MatchString(req.CodeChallenge) {
		return cliError("INVALID_REQUEST")
	}
	if req.RedirectURI != "" && (!validCLIRedirect(req.RedirectURI) || !cliProof.MatchString(req.State)) {
		return cliError("INVALID_REQUEST")
	}
	if req.RedirectURI == "" && req.State != "" {
		return cliError("INVALID_REQUEST")
	}
	if len(req.Scopes) == 0 || len(req.Scopes) > len(cliScopes) {
		return cliError("INVALID_SCOPE")
	}
	seen := map[string]bool{}
	for _, scope := range req.Scopes {
		if !slices.Contains(cliScopes, scope) || seen[scope] {
			return cliError("INVALID_SCOPE")
		}
		seen[scope] = true
	}
	web, err := url.Parse(h.frontend)
	if err != nil || web.Host == "" || web.User != nil || web.RawQuery != "" || web.Fragment != "" ||
		(web.Scheme != "https" && !(web.Scheme == "http" && (web.Hostname() == "localhost" || web.Hostname() == "127.0.0.1" || web.Hostname() == "::1"))) {
		return httpx.Internal("CLI login frontend is not configured")
	}
	ctx := c.Request().Context()
	if err := h.limit(ctx, "start:"+h.clientIP(c.Request()), 10); err != nil {
		return err
	}
	// Expired grants contain no reusable secrets. Cleanup is bounded per start.
	if _, err := h.svc.pool.Exec(ctx, `DELETE FROM cli_login_requests WHERE device_hash IN (SELECT device_hash FROM cli_login_requests WHERE expires_at < now() LIMIT 1000)`); err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	if _, err := h.svc.pool.Exec(ctx, `DELETE FROM cli_login_rate_limits WHERE key IN (SELECT key FROM cli_login_rate_limits WHERE expires_at < now() LIMIT 1000)`); err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	device, err := cliRandom()
	if err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	// 40 bits of entropy, no ambiguous characters; independent from device secret.
	alphabet := "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	var code strings.Builder
	for i := 0; i < 8; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return httpx.Internal("CLI authorization unavailable")
		}
		code.WriteByte(alphabet[n.Int64()])
	}
	userCode := code.String()
	_, err = h.svc.pool.Exec(ctx, `INSERT INTO cli_login_requests(device_hash,user_code_hash,challenge,redirect_uri,state,scopes,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, cliHash(device), cliHash(userCode), req.CodeChallenge, req.RedirectURI, req.State, req.Scopes, h.svc.now().Add(cliLoginTTL))
	if err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	verification := h.frontend + "/cli/authorize"
	return c.JSON(http.StatusCreated, map[string]any{"device_code": device, "user_code": userCode[:4] + "-" + userCode[4:], "verification_uri": verification, "verification_uri_complete": verification + "?user_code=" + userCode, "expires_in": int(cliLoginTTL.Seconds()), "interval": 5})
}

func normalizeCLICode(raw string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(raw), "-", ""))
}

func readCLIGrant(ctx context.Context, tx pgx.Tx, column, secret string) (*cliGrant, error) {
	// column is selected only by fixed call sites, never by request data.
	if column != "device_hash" && column != "user_code_hash" && column != "code_hash" {
		return nil, cliError("INVALID_GRANT")
	}
	g := &cliGrant{}
	err := tx.QueryRow(ctx, `SELECT device_hash,challenge,redirect_uri,state,scopes,status,user_id,user_version,expires_at,last_poll_at,poll_interval FROM cli_login_requests WHERE `+column+`=$1 FOR UPDATE`, cliHash(secret)).Scan(&g.DeviceHash, &g.Challenge, &g.RedirectURI, &g.State, &g.Scopes, &g.Status, &g.UserID, &g.UserVersion, &g.ExpiresAt, &g.LastPoll, &g.Interval)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, cliError("INVALID_GRANT")
	}
	if err != nil {
		return nil, httpx.Internal("CLI authorization unavailable")
	}
	return g, nil
}

func (h *CLILoginHandler) Inspect(c echo.Context) error {
	uid, err := jwtUserID(c)
	if err != nil {
		return err
	}
	var req struct {
		UserCode string `json:"user_code"`
	}
	if err := cliDecode(c, &req); err != nil {
		return err
	}
	code := normalizeCLICode(req.UserCode)
	if !cliUserCode.MatchString(code) {
		return cliError("INVALID_GRANT")
	}
	ctx := c.Request().Context()
	if err := h.limit(ctx, "inspect:"+uid.String(), 60); err != nil {
		return err
	}
	tx, err := h.svc.pool.Begin(ctx)
	if err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	g, err := readCLIGrant(ctx, tx, "user_code_hash", code)
	if err != nil {
		return err
	}
	if !g.ExpiresAt.After(h.svc.now()) || g.Status != "pending" {
		return cliError("INVALID_GRANT")
	}
	return c.JSON(200, map[string]any{"client_name": "OpenLinker CLI", "instance_url": h.frontend, "user_code": code[:4] + "-" + code[4:], "scopes": g.Scopes, "expires_at": g.ExpiresAt, "token_lifetime_days": 30, "redirect_uri": g.RedirectURI})
}

func (h *CLILoginHandler) Decide(c echo.Context) error {
	uid, err := jwtUserID(c)
	if err != nil {
		return err
	}
	var req struct {
		UserCode string `json:"user_code"`
		Approve  *bool  `json:"approve"`
	}
	if err := cliDecode(c, &req); err != nil {
		return err
	}
	code := normalizeCLICode(req.UserCode)
	if req.Approve == nil || !cliUserCode.MatchString(code) {
		return cliError("INVALID_REQUEST")
	}
	ctx := c.Request().Context()
	if err := h.limit(ctx, "inspect:"+uid.String(), 60); err != nil {
		return err
	}
	tx, err := h.svc.pool.Begin(ctx)
	if err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	g, err := readCLIGrant(ctx, tx, "user_code_hash", code)
	if err != nil {
		return err
	}
	if !g.ExpiresAt.After(h.svc.now()) || g.Status != "pending" {
		return cliError("INVALID_GRANT")
	}
	status := "denied"
	authCode := ""
	var codeHash *string
	if *req.Approve {
		// Preflight improves consent UX; issuance still checks under the shared
		// quota lock because other requests can create tokens after approval.
		count, err := h.svc.queries.WithTx(tx).CountActiveUserTokensByUser(ctx, uid)
		if err != nil {
			return httpx.Internal("CLI authorization unavailable")
		}
		if count >= maxActiveUserTokens {
			return cliQuotaError()
		}
		status = "approved"
		if g.RedirectURI != "" {
			authCode, err = cliRandom()
			if err != nil {
				return httpx.Internal("CLI authorization unavailable")
			}
			hash := cliHash(authCode)
			codeHash = &hash
		}
	}
	// Persist the approved account's session generation; password changes before
	// redemption invalidate the pending authorization too.
	var version int64
	principal := auth.PrincipalFrom(c)
	if principal == nil || principal.JWTTokenVersion == nil {
		return httpx.Unauthorized("")
	}
	if err := tx.QueryRow(ctx, `SELECT token_version FROM users WHERE id=$1 AND token_version=$2 AND disabled_at IS NULL AND deleted_at IS NULL`, uid, *principal.JWTTokenVersion).Scan(&version); err != nil {
		return httpx.Unauthorized("")
	}
	_, err = tx.Exec(ctx, `UPDATE cli_login_requests SET status=$2,user_id=$3,user_version=$4,code_hash=$5 WHERE device_hash=$1`, g.DeviceHash, status, uid, version, codeHash)
	if err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	redirect := ""
	if g.RedirectURI != "" {
		u, _ := url.Parse(g.RedirectURI)
		q := u.Query()
		q.Set("state", g.State)
		if *req.Approve {
			q.Set("code", authCode)
		} else {
			q.Set("error", "access_denied")
		}
		u.RawQuery = q.Encode()
		redirect = u.String()
	}
	return c.JSON(200, map[string]any{"status": status, "redirect_uri": redirect})
}

func (h *CLILoginHandler) Exchange(c echo.Context) error {
	var req struct {
		GrantType    string `json:"grant_type"`
		Code         string `json:"code"`
		DeviceCode   string `json:"device_code"`
		CodeVerifier string `json:"code_verifier"`
		RedirectURI  string `json:"redirect_uri"`
	}
	if err := cliDecode(c, &req); err != nil {
		return err
	}
	if !cliVerifier.MatchString(req.CodeVerifier) {
		return cliError("INVALID_GRANT")
	}
	column, secret := "code_hash", req.Code
	switch req.GrantType {
	case "authorization_code":
		if req.DeviceCode != "" || !validCLIRedirect(req.RedirectURI) {
			return cliError("INVALID_GRANT")
		}
	case "urn:ietf:params:oauth:grant-type:device_code":
		column, secret = "device_hash", req.DeviceCode
		if req.Code != "" || req.RedirectURI != "" {
			return cliError("INVALID_GRANT")
		}
	default:
		return cliError("UNSUPPORTED_GRANT_TYPE")
	}
	if !cliProof.MatchString(secret) {
		return cliError("INVALID_GRANT")
	}
	ctx := c.Request().Context()
	if err := h.limit(ctx, "exchange:"+h.clientIP(c.Request()), 1800); err != nil {
		return err
	}
	tx, err := h.svc.pool.Begin(ctx)
	if err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	g, err := readCLIGrant(ctx, tx, column, secret)
	if err != nil {
		return err
	}
	proof := sha256.Sum256([]byte(req.CodeVerifier))
	if subtle.ConstantTimeCompare([]byte(g.Challenge), []byte(base64.RawURLEncoding.EncodeToString(proof[:]))) != 1 || req.RedirectURI != g.RedirectURI {
		return cliError("INVALID_GRANT")
	}
	if !g.ExpiresAt.After(h.svc.now()) {
		return cliError("EXPIRED_TOKEN")
	}
	if g.Status == "consumed" {
		return cliError("INVALID_GRANT")
	}
	if g.Status == "denied" {
		return cliError("ACCESS_DENIED")
	}
	if column == "device_hash" {
		interval := g.Interval
		errCode := ""
		if g.LastPoll != nil && h.svc.now().Sub(*g.LastPoll) < time.Duration(interval)*time.Second {
			interval += 5
			errCode = "SLOW_DOWN"
		} else if g.Status == "pending" {
			errCode = "AUTHORIZATION_PENDING"
		}
		if errCode != "" {
			if _, err := tx.Exec(ctx, `UPDATE cli_login_requests SET last_poll_at=$2,poll_interval=$3 WHERE device_hash=$1`, g.DeviceHash, h.svc.now(), interval); err != nil {
				return httpx.Internal("CLI authorization unavailable")
			}
			if err := tx.Commit(ctx); err != nil {
				return httpx.Internal("CLI authorization unavailable")
			}
			return cliError(errCode)
		}
	}
	if g.Status != "approved" || g.UserID == nil || g.UserVersion == nil {
		return cliError("INVALID_GRANT")
	}
	if err := auth.NewDBUserStatusChecker(tx).EnsureJWTUserVersion(ctx, *g.UserID, *g.UserVersion); err != nil {
		return err
	}
	expires := h.svc.now().Add(cliTokenTTL)
	token, err := h.svc.createInTx(ctx, tx, *g.UserID, &CreateRequest{Name: "OpenLinker CLI", Scopes: g.Scopes, ExpiresAt: &expires})
	if err != nil {
		var response *httpx.HTTPError
		if errors.As(err, &response) && response.Code == httpx.CodeConflict {
			return cliQuotaError()
		}
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE cli_login_requests SET status='consumed',code_hash=NULL WHERE device_hash=$1`, g.DeviceHash); err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return httpx.Internal("CLI authorization unavailable")
	}
	return c.JSON(200, map[string]any{"access_token": token.PlaintextToken, "token_type": "Bearer", "expires_in": int(cliTokenTTL.Seconds()), "token": cliSafeToken(token)})
}

func cliSafeToken(token *TokenResponse) TokenResponse {
	copy := *token
	copy.PlaintextToken = ""
	return copy
}

func cliQuotaError() error {
	return httpx.NewError(http.StatusConflict, "TOKEN_QUOTA_EXCEEDED", "Revoke an old User Token in website settings before retrying CLI login")
}

func (h *CLILoginHandler) current(c echo.Context) (*auth.AuthPrincipal, error) {
	header := c.Request().Header.Get("Authorization")
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || !strings.HasPrefix(parts[1], "ol_user_") {
		return nil, httpx.Unauthorized("")
	}
	p, err := h.svc.VerifyPrincipal(c.Request().Context(), parts[1])
	if err != nil || p == nil || p.TokenID == nil {
		return nil, httpx.Unauthorized("")
	}
	return p, nil
}

func (h *CLILoginHandler) Session(c echo.Context) error {
	p, err := h.current(c)
	if err != nil {
		return err
	}
	token, err := h.svc.Get(c.Request().Context(), p.UserID, *p.TokenID)
	if err != nil {
		return err
	}
	return c.JSON(200, map[string]any{"authenticated": true, "user": map[string]any{"id": p.UserID}, "token": cliSafeToken(token)})
}

func (h *CLILoginHandler) Logout(c echo.Context) error {
	p, err := h.current(c)
	if err != nil {
		return err
	}
	if err := h.svc.Revoke(c.Request().Context(), p.UserID, *p.TokenID); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}
