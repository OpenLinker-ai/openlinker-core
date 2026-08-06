package usertoken

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type fakeTokenService struct {
	createCalled bool
	listOpts     ListOptions
}

func (f *fakeTokenService) Create(_ context.Context, userID uuid.UUID, req *CreateRequest) (*TokenResponse, error) {
	f.createCalled = true
	return &TokenResponse{ID: uuid.NewString(), UserID: userID.String(), Name: req.Name, Grants: []GrantResponse{}, Scopes: []string{}, PlaintextToken: "ol_user_once"}, nil
}
func (f *fakeTokenService) List(_ context.Context, _ uuid.UUID, opts ListOptions) (*ListResponse, error) {
	f.listOpts = opts
	return &ListResponse{}, nil
}
func (f *fakeTokenService) Get(context.Context, uuid.UUID, uuid.UUID) (*TokenResponse, error) {
	return nil, errors.New("not used")
}

func TestUserTokenListParsesStatusAndSearchBeforeServicePagination(t *testing.T) {
	userID := uuid.New()
	svc := &fakeTokenService{}
	h := NewHandler(svc)
	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/user-tokens?status=active&q=prod&limit=7&offset=14", nil), rec)
	c.Set(string(httpx.CtxKeyUserID), userID.String())
	c.Set(string(httpx.CtxKeyAuthMethod), auth.AuthMethodJWT)
	if err := h.List(c); err != nil {
		t.Fatalf("List error = %v", err)
	}
	if svc.listOpts.Status != "active" || svc.listOpts.Query != "prod" || svc.listOpts.Limit != 7 || svc.listOpts.Offset != 14 {
		t.Fatalf("list opts = %#v", svc.listOpts)
	}

	invalid := e.NewContext(httptest.NewRequest(http.MethodGet, "/api/v1/user-tokens?status=unknown", nil), httptest.NewRecorder())
	invalid.Set(string(httpx.CtxKeyUserID), userID.String())
	invalid.Set(string(httpx.CtxKeyAuthMethod), auth.AuthMethodJWT)
	err := h.List(invalid)
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusBadRequest {
		t.Fatalf("invalid status error = %#v", err)
	}
}
func (f *fakeTokenService) Update(context.Context, uuid.UUID, uuid.UUID, *UpdateRequest) (*TokenResponse, error) {
	return nil, errors.New("not used")
}
func (f *fakeTokenService) Revoke(context.Context, uuid.UUID, uuid.UUID) error { return nil }

func TestUserTokenCRUDIsJWTOnlyAndAllowsZeroCoreGrants(t *testing.T) {
	userID := uuid.New()
	for _, tc := range []struct {
		name       string
		method     string
		wantStatus int
		wantCall   bool
	}{
		{name: "jwt", method: auth.AuthMethodJWT, wantStatus: http.StatusCreated, wantCall: true},
		{name: "user token rejected", method: auth.AuthMethodUserToken, wantStatus: http.StatusForbidden, wantCall: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeTokenService{}
			h := NewHandler(svc)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(httptest.NewRequest(http.MethodPost, "/api/v1/user-tokens", bytes.NewBufferString(`{"name":"cloud only","grants":[]}`)), rec)
			c.Request().Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			c.Set(string(httpx.CtxKeyUserID), userID.String())
			c.Set(string(httpx.CtxKeyAuthMethod), tc.method)
			err := h.Create(c)
			if err != nil {
				var httpErr *httpx.HTTPError
				if !errors.As(err, &httpErr) || httpErr.Status != tc.wantStatus {
					t.Fatalf("error = %#v", err)
				}
			} else if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d", rec.Code)
			}
			if svc.createCalled != tc.wantCall {
				t.Fatalf("create called = %v", svc.createCalled)
			}
		})
	}
}

type fakeIntrospectionService struct {
	seen string
}

func (f *fakeIntrospectionService) Introspect(_ context.Context, token string) IntrospectionResponse {
	f.seen = token
	return IntrospectionResponse{Active: true, TokenID: uuid.NewString()}
}

func TestIntrospectionRequiresNonEmptyExactInternalSecret(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured string
		provided   string
		wantStatus int
	}{
		{name: "empty config stays closed", configured: "", provided: "", wantStatus: http.StatusUnauthorized},
		{name: "wrong secret", configured: "shared-secret", provided: "shared-secreu", wantStatus: http.StatusUnauthorized},
		{name: "exact secret", configured: "shared-secret", provided: "shared-secret", wantStatus: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeIntrospectionService{}
			h := NewIntrospectionHandler(svc, tc.configured)
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(httptest.NewRequest(http.MethodPost, "/internal/user-tokens/introspect", bytes.NewBufferString(`{"token":"ol_user_secret"}`)), rec)
			c.Request().Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			c.Request().Header.Set(InternalTokenHeader, tc.provided)
			err := h.Introspect(c)
			if tc.wantStatus == http.StatusUnauthorized {
				var httpErr *httpx.HTTPError
				if !errors.As(err, &httpErr) || httpErr.Status != tc.wantStatus || svc.seen != "" {
					t.Fatalf("error=%#v seen=%q", err, svc.seen)
				}
				return
			}
			if err != nil || rec.Code != http.StatusOK || svc.seen != "ol_user_secret" {
				t.Fatalf("err=%v status=%d seen=%q", err, rec.Code, svc.seen)
			}
		})
	}
}
