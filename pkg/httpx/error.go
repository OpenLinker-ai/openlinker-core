package httpx

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
)

// ErrorCode 标准化错误码（见 docs/13 附录 A）。
type ErrorCode string

const (
	CodeBadRequest         ErrorCode = "BAD_REQUEST"
	CodeUnauthorized       ErrorCode = "UNAUTHORIZED"
	CodePaymentRequired    ErrorCode = "INSUFFICIENT_BALANCE"
	CodeForbidden          ErrorCode = "FORBIDDEN"
	CodeNotFound           ErrorCode = "NOT_FOUND"
	CodeConflict           ErrorCode = "CONFLICT"
	CodeUnprocessable      ErrorCode = "VALIDATION_FAILED"
	CodeRateLimited        ErrorCode = "RATE_LIMITED"
	CodeInternal           ErrorCode = "INTERNAL_ERROR"
	CodeServiceUnavailable ErrorCode = "SERVICE_UNAVAILABLE"
)

// ErrorResponse 统一错误响应体。
//
//	{ "error": { "code": "...", "message": "..." } }
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody 错误正文。
type ErrorBody struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Details any       `json:"details,omitempty"`
}

// HTTPError 业务级 HTTP 错误。
type HTTPError struct {
	Status  int
	Code    ErrorCode
	Message string
	Details any
}

func (e *HTTPError) Error() string {
	return e.Message
}

// NewError 构造业务错误。
func NewError(status int, code ErrorCode, message string) *HTTPError {
	return &HTTPError{Status: status, Code: code, Message: message}
}

// SendError 输出标准化错误响应。
func SendError(c echo.Context, err error) error {
	var he *HTTPError
	if errors.As(err, &he) {
		return c.JSON(he.Status, ErrorResponse{Error: ErrorBody{
			Code:    he.Code,
			Message: he.Message,
			Details: he.Details,
		}})
	}
	// 兼容 echo.HTTPError
	var ee *echo.HTTPError
	if errors.As(err, &ee) {
		return c.JSON(ee.Code, ErrorResponse{Error: ErrorBody{
			Code:    CodeInternal,
			Message: ee.Error(),
		}})
	}
	return c.JSON(http.StatusInternalServerError, ErrorResponse{Error: ErrorBody{
		Code:    CodeInternal,
		Message: "internal error",
	}})
}

// 常用 helper

func BadRequest(msg string) *HTTPError {
	return NewError(http.StatusBadRequest, CodeBadRequest, msg)
}
func Unauthorized(msg string) *HTTPError {
	if msg == "" {
		msg = "认证失败"
	}
	return NewError(http.StatusUnauthorized, CodeUnauthorized, msg)
}
func Forbidden(msg string) *HTTPError {
	return NewError(http.StatusForbidden, CodeForbidden, msg)
}
func NotFound(msg string) *HTTPError {
	if msg == "" {
		msg = "资源不存在"
	}
	return NewError(http.StatusNotFound, CodeNotFound, msg)
}
func Conflict(msg string) *HTTPError {
	return NewError(http.StatusConflict, CodeConflict, msg)
}
func Unprocessable(msg string) *HTTPError {
	return NewError(http.StatusUnprocessableEntity, CodeUnprocessable, msg)
}
func PaymentRequired(msg string) *HTTPError {
	if msg == "" {
		msg = "余额不足，请先充值"
	}
	return NewError(http.StatusPaymentRequired, CodePaymentRequired, msg)
}
func Internal(msg string) *HTTPError {
	return NewError(http.StatusInternalServerError, CodeInternal, msg)
}
func ServiceUnavailable(msg string) *HTTPError {
	if msg == "" {
		msg = "服务暂不可用"
	}
	return NewError(http.StatusServiceUnavailable, CodeServiceUnavailable, msg)
}

// CtxKey 类型安全的 context key。
type CtxKey string

const (
	CtxKeyUserID     CtxKey = "user_id"
	CtxKeyAdmin      CtxKey = "is_admin"
	CtxKeyAuthMethod CtxKey = "auth_method"
	CtxKeyAuthScopes CtxKey = "auth_scopes"
)

// UserIDFrom 从 echo.Context 取出 user_id（鉴权中间件设置）。
func UserIDFrom(c echo.Context) string {
	v, _ := c.Get(string(CtxKeyUserID)).(string)
	return v
}

// AuthMethodFrom 返回当前请求的鉴权方式：'jwt' / 'user_token' / ”（未鉴权）。
// HybridAuthMiddleware 命中后设置。
func AuthMethodFrom(c echo.Context) string {
	v, _ := c.Get(string(CtxKeyAuthMethod)).(string)
	return v
}

// HasScope reports whether the authenticated User Token carries a required permission.
func HasScope(c echo.Context, expected string) bool {
	scopes, _ := c.Get(string(CtxKeyAuthScopes)).([]string)
	for _, scope := range scopes {
		if scope == expected {
			return true
		}
	}
	return false
}

// IsAdmin 是否管理员。
func IsAdmin(c echo.Context) bool {
	v, _ := c.Get(string(CtxKeyAdmin)).(bool)
	return v
}
