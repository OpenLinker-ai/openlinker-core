// Package auth —— 认证模块。
//
// 提供：
//   - 邮箱密码注册 / 登录
//   - Google OAuth 登录
//   - JWT 签发与校验中间件
//   - GET /me 当前用户信息
//
// 见 docs/13-phase1-prd.md。
package auth

// RegisterRequest 邮箱注册请求。
type RegisterRequest struct {
	Email       string `json:"email" validate:"required,email,max=120"`
	Password    string `json:"password" validate:"required,min=8,max=72"`
	DisplayName string `json:"display_name" validate:"required,min=2,max=50"`
}

// LoginRequest 邮箱登录请求。
type LoginRequest struct {
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required"`
}

// OAuthExchangeRequest exchanges the short-lived OAuth redirect code for a JWT.
type OAuthExchangeRequest struct {
	Code string `json:"code" validate:"required,len=64"`
}

// AuthResponse 注册 / 登录 / OAuth 成功响应。
type AuthResponse struct {
	UserID      string `json:"user_id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	JWT         string `json:"jwt"`
}

// MeResponse GET /me 响应。
type MeResponse struct {
	UserID      string `json:"user_id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url,omitempty"`
	IsCreator   bool   `json:"is_creator"`
	IsAdmin     bool   `json:"is_admin"`
}

// UpdateMeRequest PATCH /me 请求。
type UpdateMeRequest struct {
	DisplayName string `json:"display_name" validate:"required,min=2,max=50"`
}

// ChangePasswordRequest POST /me/password 请求。
type ChangePasswordRequest struct {
	CurrentPassword string `json:"current_password" validate:"required"`
	NewPassword     string `json:"new_password" validate:"required,min=8,max=72"`
}
