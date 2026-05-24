package auth

import (
	"net/http"
	"net/url"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/markbates/goth/gothic"
	"github.com/rs/zerolog/log"

	"github.com/kinzhi/openlinker-core/pkg/config"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

// providerGoogle / providerGithub OAuth provider 名（goth 用作 key）。
const (
	providerGoogle = "google"
	providerGithub = "github"
)

// Handler 认证模块 HTTP 入口。
//
// 主程序通过 NewHandler 构造，再用 SetConfig 注入 cfg（OAuth callback 重定向需要）。
// 单元测试可直接 NewHandler(svc) 不带 cfg。
type Handler struct {
	svc       *Service
	validator *validator.Validate
	cfg       *config.Config
}

// NewHandler 构造 Handler。
// cfg 可选：传入则启用 OAuth 回调重定向；不传则只能用作单元测试 / 邮箱注册登录场景。
func NewHandler(svc *Service, cfg ...*config.Config) *Handler {
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
//	POST /auth/register
//	POST /auth/login
//	GET  /auth/google
//	GET  /auth/google/callback
//	GET  /auth/github
//	GET  /auth/github/callback
func (h *Handler) Register(api *echo.Group) {
	auth := api.Group("/auth")
	auth.POST("/register", h.PostRegister)
	auth.POST("/login", h.PostLogin)
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
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
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
	req := gothic.GetContextWithProvider(c.Request(), provider)
	authURL, err := gothic.GetAuthURL(c.Response().Writer, req)
	if err != nil {
		log.Error().Err(err).Str("provider", provider).Msg("auth.oauthStart: GetAuthURL")
		return httpx.Internal("启动 OAuth 失败")
	}
	return c.Redirect(http.StatusTemporaryRedirect, authURL)
}

// oauthCallback 处理任意 OAuth provider 回调。
//
// 成功 -> 重定向到 FrontendURL/auth/callback?token=<jwt>
// 失败 -> 重定向到 FrontendURL/auth/callback?error=<msg>
//
// Phase 1 简化版：token 直接放 URL（同源 redirect 到前端域，前端立即清除）。
// 阶段 3 整合时可改为短期 code + 后端换 token，参考 docs/13。
func (h *Handler) oauthCallback(c echo.Context, provider string, configured bool) error {
	if h.cfg == nil {
		return httpx.Internal(provider + " OAuth 未配置")
	}
	if !configured {
		return h.redirectAuthError(c, provider+" OAuth 未配置")
	}
	req := gothic.GetContextWithProvider(c.Request(), provider)
	gu, err := gothic.CompleteUserAuth(c.Response().Writer, req)
	if err != nil {
		log.Warn().Err(err).Str("provider", provider).Msg("auth.oauthCallback: CompleteUserAuth")
		return h.redirectAuthError(c, "OAuth 验证失败")
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
			return h.redirectAuthError(c, he.Message)
		}
		log.Error().Err(err).Str("provider", provider).Msg("auth.oauthCallback: FindOrCreateOAuthUser")
		return h.redirectAuthError(c, "登录失败")
	}

	target := h.cfg.FrontendURL + "/auth/callback?token=" + url.QueryEscape(resp.JWT)
	return c.Redirect(http.StatusTemporaryRedirect, target)
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
func (h *Handler) redirectAuthError(c echo.Context, msg string) error {
	target := h.cfg.FrontendURL + "/auth/callback?error=" + url.QueryEscape(msg)
	return c.Redirect(http.StatusTemporaryRedirect, target)
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
