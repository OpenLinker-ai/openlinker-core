package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/markbates/goth/gothic"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// providerGoogle / providerGithub OAuth provider 名（goth 用作 key）。
const (
	providerGoogle          = "google"
	providerGithub          = "github"
	oauthStateVersion       = "ol2"
	oauthStateLegacyVersion = "ol1"
)

type oauthRedirectState struct {
	callbackPath   string
	frontendOrigin string
}

// Handler 认证模块 HTTP 入口。
//
// 主程序通过 NewHandler 构造，再用 SetConfig 注入 cfg（OAuth callback 重定向需要）。
// 单元测试可直接 NewHandler(svc) 不带 cfg。
type Handler struct {
	svc       authService
	validator *validator.Validate
	cfg       *config.Config
}

type authService interface {
	Register(context.Context, *RegisterRequest) (*AuthResponse, error)
	Login(context.Context, *LoginRequest) (*AuthResponse, error)
	FindOrCreateOAuthUser(context.Context, string, string, string, string, string) (*AuthResponse, error)
	IssueOAuthCode(context.Context, *AuthResponse) (string, error)
	ExchangeOAuthCode(context.Context, string) (*AuthResponse, error)
	RefreshToken(context.Context, uuid.UUID) (*AuthResponse, error)
	GetMe(context.Context, uuid.UUID) (*MeResponse, error)
	UpdateMe(context.Context, uuid.UUID, *UpdateMeRequest) (*MeResponse, error)
	ChangePassword(context.Context, uuid.UUID, *ChangePasswordRequest) error
}

// NewHandler 构造 Handler。
// cfg 可选：传入则启用 OAuth 回调重定向；不传则只能用作单元测试 / 邮箱注册登录场景。
func NewHandler(svc authService, cfg ...*config.Config) *Handler {
	h := &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
	if len(cfg) > 0 {
		h.cfg = cfg[0]
	}
	return h
}

// SetConfig 注入主配置（OAuth callback 用 FrontendURL 等）。
// 主程序在 NewHandler 后调用，测试不必调用。
func (h *Handler) SetConfig(cfg *config.Config) *Handler {
	h.cfg = cfg
	return h
}

// Register 注册公开认证路由（不需 JWT）。
//
//	POST /auth/login
//	POST /auth/register
//	GET  /auth/google
//	GET  /auth/google/callback
//	GET  /auth/github
//	GET  /auth/github/callback
func (h *Handler) Register(api *echo.Group) {
	auth := api.Group("/auth")
	auth.POST("/register", h.PostRegister)
	auth.POST("/login", h.PostLogin)
	auth.POST("/oauth/exchange", h.PostOAuthExchange)
	auth.GET("/google", h.GoogleStart)
	auth.GET("/google/callback", h.GoogleCallback)
	auth.GET("/github", h.GithubStart)
	auth.GET("/github/callback", h.GithubCallback)
}

// RegisterProtected 注册需要 JWT 的端点。
//
//	GET   /me
//	PATCH /me
//	POST  /me/password
//	POST  /auth/refresh
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	api.POST("/auth/refresh", h.PostRefresh, jwtMiddleware)
	api.GET("/me", h.GetMe, jwtMiddleware)
	api.PATCH("/me", h.PatchMe, jwtMiddleware)
	api.POST("/me/password", h.PostChangePassword, jwtMiddleware)
}

// PostRegister 邮箱注册。
func (h *Handler) PostRegister(c echo.Context) error {
	var req RegisterRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.Register(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

// PostLogin 邮箱登录。
func (h *Handler) PostLogin(c echo.Context) error {
	var req LoginRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.Login(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// PostRefresh 刷新当前网页登录 JWT。
func (h *Handler) PostRefresh(c echo.Context) error {
	userID, err := currentUserID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.RefreshToken(c.Request().Context(), userID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// GoogleStart 重定向到 Google OAuth 授权页。
func (h *Handler) GoogleStart(c echo.Context) error {
	return h.oauthStart(c, providerGoogle, h.googleConfigured())
}

// GoogleCallback Google OAuth 回调。
func (h *Handler) GoogleCallback(c echo.Context) error {
	return h.oauthCallback(c, providerGoogle, h.googleConfigured())
}

// GithubStart 重定向到 GitHub OAuth 授权页。
func (h *Handler) GithubStart(c echo.Context) error {
	return h.oauthStart(c, providerGithub, h.githubConfigured())
}

// GithubCallback GitHub OAuth 回调。
func (h *Handler) GithubCallback(c echo.Context) error {
	return h.oauthCallback(c, providerGithub, h.githubConfigured())
}

func (h *Handler) googleConfigured() bool {
	return h.cfg != nil && h.cfg.GoogleClientID != "" && h.cfg.GoogleClientSecret != ""
}

func (h *Handler) githubConfigured() bool {
	return h.cfg != nil && h.cfg.GithubClientID != "" && h.cfg.GithubClientSecret != ""
}

// oauthStart 把指定 provider 注入 gothic context，重定向到授权页。
//
// gothic 通过 query/route 参数定位 provider；这里把 provider 显式注入 context。
func (h *Handler) oauthStart(c echo.Context, provider string, configured bool) error {
	if !configured {
		return httpx.Internal(provider + " OAuth 未配置")
	}
	state, err := newOAuthState(c.QueryParam("callbackUrl"), h.safeOAuthFrontendOrigin(c.QueryParam("frontendOrigin")))
	if err != nil {
		log.Error().Err(err).Str("provider", provider).Msg("auth.oauthStart: create state")
		return httpx.Internal("启动 OAuth 失败")
	}
	req := gothic.GetContextWithProvider(requestWithOAuthState(c.Request(), state), provider)
	authURL, err := gothic.GetAuthURL(c.Response().Writer, req)
	if err != nil {
		log.Error().Err(err).Str("provider", provider).Msg("auth.oauthStart: GetAuthURL")
		return httpx.Internal("启动 OAuth 失败")
	}
	return c.Redirect(http.StatusTemporaryRedirect, authURL)
}

// oauthCallback 处理任意 OAuth provider 回调。
//
// 成功 -> 重定向到允许的前端 origin /auth/callback?provider=<provider>&code=<short-lived-code>
// 失败 -> 重定向到允许的前端 origin /auth/callback?provider=<provider>&error=<msg>
func (h *Handler) oauthCallback(c echo.Context, provider string, configured bool) error {
	if h.cfg == nil {
		return httpx.Internal(provider + " OAuth 未配置")
	}
	redirectState := redirectStateFromOAuthState(c.QueryParam("state"), h.cfg)
	if !configured {
		return h.redirectAuthError(c, provider, provider+" OAuth 未配置", redirectState)
	}
	req := gothic.GetContextWithProvider(c.Request(), provider)
	gu, err := gothic.CompleteUserAuth(c.Response().Writer, req)
	if err != nil {
		log.Warn().Err(err).Str("provider", provider).Msg("auth.oauthCallback: CompleteUserAuth")
		return h.redirectAuthError(c, provider, "OAuth 验证失败", redirectState)
	}

	resp, err := h.svc.FindOrCreateOAuthUser(
		c.Request().Context(),
		provider,
		gu.UserID,
		gu.Email,
		nonEmpty(gu.Name, gu.NickName),
		gu.AvatarURL,
	)
	if err != nil {
		// 业务错误（如 conflict）以友好消息回前端
		if he, ok := err.(*httpx.HTTPError); ok {
			return h.redirectAuthError(c, provider, he.Message, redirectState)
		}
		log.Error().Err(err).Str("provider", provider).Msg("auth.oauthCallback: FindOrCreateOAuthUser")
		return h.redirectAuthError(c, provider, "登录失败", redirectState)
	}

	code, err := h.svc.IssueOAuthCode(c.Request().Context(), resp)
	if err != nil {
		return err
	}

	target := h.authCallbackURL(provider, code, "", redirectState)
	return c.Redirect(http.StatusTemporaryRedirect, target)
}

func (h *Handler) PostOAuthExchange(c echo.Context) error {
	var req OAuthExchangeRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.ExchangeOAuthCode(c.Request().Context(), req.Code)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// GetMe 当前登录用户信息。
func (h *Handler) GetMe(c echo.Context) error {
	idStr := httpx.UserIDFrom(c)
	if idStr == "" {
		return httpx.Unauthorized("")
	}
	uid, err := uuid.Parse(idStr)
	if err != nil {
		return httpx.Unauthorized("token 无效")
	}
	resp, err := h.svc.GetMe(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// PatchMe 更新当前用户资料。
func (h *Handler) PatchMe(c echo.Context) error {
	uid, err := currentUserID(c)
	if err != nil {
		return err
	}
	var req UpdateMeRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.UpdateMe(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// PostChangePassword 修改当前用户密码。
func (h *Handler) PostChangePassword(c echo.Context) error {
	uid, err := currentUserID(c)
	if err != nil {
		return err
	}
	var req ChangePasswordRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	if err := h.svc.ChangePassword(c.Request().Context(), uid, &req); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

func currentUserID(c echo.Context) (uuid.UUID, error) {
	idStr := httpx.UserIDFrom(c)
	if idStr == "" {
		return uuid.Nil, httpx.Unauthorized("")
	}
	uid, err := uuid.Parse(idStr)
	if err != nil {
		return uuid.Nil, httpx.Unauthorized("token 无效")
	}
	return uid, nil
}

// redirectAuthError 把错误信息编码进 query 后重定向到前端。
func (h *Handler) redirectAuthError(c echo.Context, provider, msg string, redirectState oauthRedirectState) error {
	target := h.authCallbackURL(provider, "", msg, redirectState)
	return c.Redirect(http.StatusTemporaryRedirect, target)
}

func (h *Handler) authCallbackURL(provider, code, errorMessage string, redirectState oauthRedirectState) string {
	base := h.oauthFrontendBaseURL(redirectState.frontendOrigin) + "/auth/callback"
	q := url.Values{}
	if code != "" {
		q.Set("code", code)
	}
	if errorMessage != "" {
		q.Set("error", errorMessage)
	}
	if provider != "" {
		q.Set("provider", provider)
	}
	if safe := safeOAuthCallbackPath(redirectState.callbackPath); safe != "/" {
		q.Set("callbackUrl", safe)
	}
	return base + "?" + q.Encode()
}

func (h *Handler) oauthFrontendBaseURL(frontendOrigin string) string {
	if safe := safeOAuthFrontendOrigin(frontendOrigin, h.cfg); safe != "" {
		return safe
	}
	if h.cfg == nil {
		return ""
	}
	return strings.TrimRight(strings.TrimSpace(h.cfg.FrontendURL), "/")
}

// nonEmpty 选第一个非空字符串。
func nonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func newOAuthState(callbackPath, frontendOrigin string) (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	noncePart := base64.RawURLEncoding.EncodeToString(nonce)
	callbackPart := base64.RawURLEncoding.EncodeToString([]byte(safeOAuthCallbackPath(callbackPath)))
	originPart := base64.RawURLEncoding.EncodeToString([]byte(frontendOrigin))
	return strings.Join([]string{oauthStateVersion, noncePart, callbackPart, originPart}, "."), nil
}

func callbackFromOAuthState(state string) string {
	return redirectStateFromOAuthState(state, nil).callbackPath
}

func redirectStateFromOAuthState(state string, cfg *config.Config) oauthRedirectState {
	parts := strings.Split(state, ".")
	if len(parts) == 3 && parts[0] == oauthStateLegacyVersion {
		raw, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil {
			return oauthRedirectState{callbackPath: "/"}
		}
		return oauthRedirectState{callbackPath: safeOAuthCallbackPath(string(raw))}
	}
	if len(parts) != 4 || parts[0] != oauthStateVersion {
		return oauthRedirectState{callbackPath: "/"}
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return oauthRedirectState{callbackPath: "/"}
	}
	originRaw, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil {
		return oauthRedirectState{callbackPath: safeOAuthCallbackPath(string(raw))}
	}
	return oauthRedirectState{
		callbackPath:   safeOAuthCallbackPath(string(raw)),
		frontendOrigin: safeOAuthFrontendOrigin(string(originRaw), cfg),
	}
}

func safeOAuthCallbackPath(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" || !strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") {
		return "/"
	}
	return value
}

func requestWithOAuthState(req *http.Request, state string) *http.Request {
	clone := req.Clone(req.Context())
	copiedURL := *clone.URL
	q := copiedURL.Query()
	q.Set("state", state)
	copiedURL.RawQuery = q.Encode()
	clone.URL = &copiedURL
	return clone
}

func (h *Handler) safeOAuthFrontendOrigin(raw string) string {
	return safeOAuthFrontendOrigin(raw, h.cfg)
}

func safeOAuthFrontendOrigin(raw string, cfg *config.Config) string {
	value := strings.TrimRight(strings.TrimSpace(raw), "/")
	if value == "" || cfg == nil {
		return ""
	}
	origin, ok := normalizeOAuthOrigin(value)
	if !ok {
		return ""
	}
	if cfg.IsProduction() && !strings.HasPrefix(origin, "https://") {
		return ""
	}
	if originMatchesAllowedOAuthOrigin(origin, cfg.FrontendURL) {
		return origin
	}
	for _, allowed := range strings.Split(cfg.OAuthAllowedFrontendOrigins, ",") {
		if originMatchesAllowedOAuthOrigin(origin, allowed) {
			return origin
		}
	}
	return ""
}

func normalizeOAuthOrigin(raw string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
		return "", false
	}
	if u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false
	}
	return scheme + "://" + strings.ToLower(u.Host), true
}

func originMatchesAllowedOAuthOrigin(origin, allowed string) bool {
	normalized, ok := normalizeOAuthOrigin(origin)
	if !ok {
		return false
	}
	allowed = strings.TrimSpace(allowed)
	if allowed == "" {
		return false
	}
	if strings.Contains(allowed, "://*.") {
		return originMatchesWildcardOAuthOrigin(normalized, allowed)
	}
	allowedOrigin, ok := normalizeOAuthOrigin(strings.TrimRight(allowed, "/"))
	if !ok {
		return false
	}
	return normalized == allowedOrigin
}

func originMatchesWildcardOAuthOrigin(origin, allowed string) bool {
	originURL, err := url.Parse(origin)
	if err != nil {
		return false
	}
	parts := strings.SplitN(strings.ToLower(strings.TrimSpace(allowed)), "://*.", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	if originURL.Scheme != parts[0] {
		return false
	}
	if originURL.Port() != "" {
		return false
	}
	allowedHost := strings.TrimRight(parts[1], "/")
	if strings.ContainsAny(allowedHost, "/?#") {
		return false
	}
	originHost := strings.ToLower(originURL.Hostname())
	return originHost != allowedHost && strings.HasSuffix(originHost, "."+allowedHost)
}
