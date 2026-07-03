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
	providerGoogle    = "google"
	providerGithub    = "github"
	oauthStateVersion = "ol1"
)

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
	state, err := newOAuthState(c.QueryParam("callbackUrl"))
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
// 成功 -> 重定向到 FrontendURL/auth/callback?provider=<provider>&code=<short-lived-code>
// 失败 -> 重定向到 FrontendURL/auth/callback?provider=<provider>&error=<msg>
func (h *Handler) oauthCallback(c echo.Context, provider string, configured bool) error {
	if h.cfg == nil {
		return httpx.Internal(provider + " OAuth 未配置")
	}
	callbackPath := callbackFromOAuthState(c.QueryParam("state"))
	if !configured {
		return h.redirectAuthError(c, provider, provider+" OAuth 未配置", callbackPath)
	}
	req := gothic.GetContextWithProvider(c.Request(), provider)
	gu, err := gothic.CompleteUserAuth(c.Response().Writer, req)
	if err != nil {
		log.Warn().Err(err).Str("provider", provider).Msg("auth.oauthCallback: CompleteUserAuth")
		return h.redirectAuthError(c, provider, "OAuth 验证失败", callbackPath)
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
			return h.redirectAuthError(c, provider, he.Message, callbackPath)
		}
		log.Error().Err(err).Str("provider", provider).Msg("auth.oauthCallback: FindOrCreateOAuthUser")
		return h.redirectAuthError(c, provider, "登录失败", callbackPath)
	}

	code, err := h.svc.IssueOAuthCode(c.Request().Context(), resp)
	if err != nil {
		return err
	}

	target := h.authCallbackURL(provider, code, "", callbackPath)
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
func (h *Handler) redirectAuthError(c echo.Context, provider, msg, callbackPath string) error {
	target := h.authCallbackURL(provider, "", msg, callbackPath)
	return c.Redirect(http.StatusTemporaryRedirect, target)
}

func (h *Handler) authCallbackURL(provider, code, errorMessage, callbackPath string) string {
	base := strings.TrimRight(h.cfg.FrontendURL, "/") + "/auth/callback"
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
	if safe := safeOAuthCallbackPath(callbackPath); safe != "/" {
		q.Set("callbackUrl", safe)
	}
	return base + "?" + q.Encode()
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

func newOAuthState(callbackPath string) (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	noncePart := base64.RawURLEncoding.EncodeToString(nonce)
	callbackPart := base64.RawURLEncoding.EncodeToString([]byte(safeOAuthCallbackPath(callbackPath)))
	return strings.Join([]string{oauthStateVersion, noncePart, callbackPart}, "."), nil
}

func callbackFromOAuthState(state string) string {
	parts := strings.Split(state, ".")
	if len(parts) != 3 || parts[0] != oauthStateVersion {
		return "/"
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "/"
	}
	return safeOAuthCallbackPath(string(raw))
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
