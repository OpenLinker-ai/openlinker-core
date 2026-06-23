package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestAuthHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	authResp := &AuthResponse{
		UserID:      userID.String(),
		Email:       "user@example.com",
		DisplayName: "User",
		JWT:         "jwt-token",
	}
	meResp := &MeResponse{
		UserID:      userID.String(),
		Email:       "user@example.com",
		DisplayName: "User",
		IsCreator:   true,
		IsAdmin:     true,
	}

	t.Run("register", func(t *testing.T) {
		mock := &mockAuthService{registerResp: authResp}
		c, rec := newAuthRecorderContext(http.MethodPost, "/auth/register", `{"email":"user@example.com","password":"password123","display_name":"User"}`, "")

		if err := NewHandler(mock).PostRegister(c); err != nil {
			t.Fatalf("PostRegister error = %v", err)
		}
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.registerReq == nil || mock.registerReq.Email != "user@example.com" || mock.registerReq.DisplayName != "User" {
			t.Fatalf("captured register req = %#v", mock.registerReq)
		}
		var body AuthResponse
		decodeAuthDispatchJSON(t, rec, &body)
		if body.UserID != userID.String() || body.JWT == "" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("login", func(t *testing.T) {
		mock := &mockAuthService{loginResp: authResp}
		c, rec := newAuthRecorderContext(http.MethodPost, "/auth/login", `{"email":"user@example.com","password":"password123"}`, "")

		if err := NewHandler(mock).PostLogin(c); err != nil {
			t.Fatalf("PostLogin error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.loginReq == nil || mock.loginReq.Email != "user@example.com" {
			t.Fatalf("captured login req = %#v", mock.loginReq)
		}
	})

	t.Run("oauth exchange", func(t *testing.T) {
		code := strings.Repeat("a", 64)
		mock := &mockAuthService{exchangeResp: authResp}
		c, rec := newAuthRecorderContext(http.MethodPost, "/auth/oauth/exchange", `{"code":"`+code+`"}`, "")

		if err := NewHandler(mock).PostOAuthExchange(c); err != nil {
			t.Fatalf("PostOAuthExchange error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.exchangeCode != code {
			t.Fatalf("captured exchange code = %q", mock.exchangeCode)
		}
		var body AuthResponse
		decodeAuthDispatchJSON(t, rec, &body)
		if body.UserID != userID.String() || body.JWT == "" {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("get me", func(t *testing.T) {
		mock := &mockAuthService{getMeResp: meResp}
		c, rec := newAuthRecorderContext(http.MethodGet, "/me", "", userID.String())

		if err := NewHandler(mock).GetMe(c); err != nil {
			t.Fatalf("GetMe error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.getMeUserID != userID {
			t.Fatalf("captured get me user = %s", mock.getMeUserID)
		}
		var body MeResponse
		decodeAuthDispatchJSON(t, rec, &body)
		if body.UserID != userID.String() || !body.IsCreator || !body.IsAdmin {
			t.Fatalf("body = %#v", body)
		}
	})

	t.Run("patch me", func(t *testing.T) {
		mock := &mockAuthService{updateMeResp: &MeResponse{
			UserID:      userID.String(),
			Email:       "user@example.com",
			DisplayName: "New Name",
		}}
		c, rec := newAuthRecorderContext(http.MethodPatch, "/me", `{"display_name":"New Name"}`, userID.String())

		if err := NewHandler(mock).PatchMe(c); err != nil {
			t.Fatalf("PatchMe error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.updateMeUserID != userID || mock.updateMeReq == nil || mock.updateMeReq.DisplayName != "New Name" {
			t.Fatalf("captured update me = user %s req %#v", mock.updateMeUserID, mock.updateMeReq)
		}
	})

	t.Run("change password", func(t *testing.T) {
		mock := &mockAuthService{}
		c, rec := newAuthRecorderContext(
			http.MethodPost,
			"/me/password",
			`{"current_password":"old-password","new_password":"new-password-123"}`,
			userID.String(),
		)

		if err := NewHandler(mock).PostChangePassword(c); err != nil {
			t.Fatalf("PostChangePassword error = %v", err)
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
		}
		if mock.changePasswordUserID != userID || mock.changePasswordReq == nil || mock.changePasswordReq.NewPassword != "new-password-123" {
			t.Fatalf("captured change password = user %s req %#v", mock.changePasswordUserID, mock.changePasswordReq)
		}
	})
}

func TestAuthHandlerPropagatesServiceErrors(t *testing.T) {
	userID := uuid.New()

	t.Run("register", func(t *testing.T) {
		mock := &mockAuthService{registerErr: httpx.Conflict("邮箱已注册")}
		c, _ := newAuthRecorderContext(http.MethodPost, "/auth/register", `{"email":"user@example.com","password":"password123","display_name":"User"}`, "")
		requireAuthHTTPStatus(t, NewHandler(mock).PostRegister(c), http.StatusConflict)
	})

	t.Run("login", func(t *testing.T) {
		mock := &mockAuthService{loginErr: httpx.Unauthorized("bad login")}
		c, _ := newAuthRecorderContext(http.MethodPost, "/auth/login", `{"email":"user@example.com","password":"password123"}`, "")
		requireAuthHTTPStatus(t, NewHandler(mock).PostLogin(c), http.StatusUnauthorized)
	})

	t.Run("oauth exchange", func(t *testing.T) {
		mock := &mockAuthService{exchangeErr: httpx.Unauthorized("bad code")}
		c, _ := newAuthRecorderContext(http.MethodPost, "/auth/oauth/exchange", `{"code":"`+strings.Repeat("a", 64)+`"}`, "")
		requireAuthHTTPStatus(t, NewHandler(mock).PostOAuthExchange(c), http.StatusUnauthorized)
	})

	t.Run("get me", func(t *testing.T) {
		mock := &mockAuthService{getMeErr: httpx.NotFound("missing")}
		c, _ := newAuthRecorderContext(http.MethodGet, "/me", "", userID.String())
		requireAuthHTTPStatus(t, NewHandler(mock).GetMe(c), http.StatusNotFound)
	})

	t.Run("patch me", func(t *testing.T) {
		mock := &mockAuthService{updateMeErr: httpx.Unprocessable("bad name")}
		c, _ := newAuthRecorderContext(http.MethodPatch, "/me", `{"display_name":"New Name"}`, userID.String())
		requireAuthHTTPStatus(t, NewHandler(mock).PatchMe(c), http.StatusUnprocessableEntity)
	})

	t.Run("change password", func(t *testing.T) {
		mock := &mockAuthService{changePasswordErr: httpx.Unauthorized("bad password")}
		c, _ := newAuthRecorderContext(
			http.MethodPost,
			"/me/password",
			`{"current_password":"old-password","new_password":"new-password-123"}`,
			userID.String(),
		)
		requireAuthHTTPStatus(t, NewHandler(mock).PostChangePassword(c), http.StatusUnauthorized)
	})
}

type mockAuthService struct {
	registerReq  *RegisterRequest
	registerResp *AuthResponse
	registerErr  error

	loginReq  *LoginRequest
	loginResp *AuthResponse
	loginErr  error

	oauthProvider    string
	oauthID          string
	oauthEmail       string
	oauthDisplayName string
	oauthAvatarURL   string
	oauthResp        *AuthResponse
	oauthErr         error
	issuedOAuthResp  *AuthResponse
	issuedOAuthCode  string
	issuedOAuthErr   error
	exchangeCode     string
	exchangeResp     *AuthResponse
	exchangeErr      error

	getMeUserID uuid.UUID
	getMeResp   *MeResponse
	getMeErr    error

	updateMeUserID uuid.UUID
	updateMeReq    *UpdateMeRequest
	updateMeResp   *MeResponse
	updateMeErr    error

	changePasswordUserID uuid.UUID
	changePasswordReq    *ChangePasswordRequest
	changePasswordErr    error
}

func (m *mockAuthService) Register(_ context.Context, req *RegisterRequest) (*AuthResponse, error) {
	m.registerReq = req
	return m.registerResp, m.registerErr
}

func (m *mockAuthService) Login(_ context.Context, req *LoginRequest) (*AuthResponse, error) {
	m.loginReq = req
	return m.loginResp, m.loginErr
}

func (m *mockAuthService) FindOrCreateOAuthUser(_ context.Context, provider, oauthID, email, displayName, avatarURL string) (*AuthResponse, error) {
	m.oauthProvider = provider
	m.oauthID = oauthID
	m.oauthEmail = email
	m.oauthDisplayName = displayName
	m.oauthAvatarURL = avatarURL
	return m.oauthResp, m.oauthErr
}

func (m *mockAuthService) IssueOAuthCode(_ context.Context, resp *AuthResponse) (string, error) {
	m.issuedOAuthResp = resp
	return m.issuedOAuthCode, m.issuedOAuthErr
}

func (m *mockAuthService) ExchangeOAuthCode(_ context.Context, code string) (*AuthResponse, error) {
	m.exchangeCode = code
	return m.exchangeResp, m.exchangeErr
}

func (m *mockAuthService) GetMe(_ context.Context, userID uuid.UUID) (*MeResponse, error) {
	m.getMeUserID = userID
	return m.getMeResp, m.getMeErr
}

func (m *mockAuthService) UpdateMe(_ context.Context, userID uuid.UUID, req *UpdateMeRequest) (*MeResponse, error) {
	m.updateMeUserID = userID
	m.updateMeReq = req
	return m.updateMeResp, m.updateMeErr
}

func (m *mockAuthService) ChangePassword(_ context.Context, userID uuid.UUID, req *ChangePasswordRequest) error {
	m.changePasswordUserID = userID
	m.changePasswordReq = req
	return m.changePasswordErr
}

func newAuthRecorderContext(method, target, body, userID string) (echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if userID != "" {
		c.Set(string(httpx.CtxKeyUserID), userID)
	}
	return c, rec
}

func decodeAuthDispatchJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}
