package httpx

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
)

func newTestContext() (echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	return e.NewContext(req, rec), rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) ErrorResponse {
	t.Helper()
	var got ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return got
}

func TestSendErrorWritesHTTPErrorResponse(t *testing.T) {
	c, rec := newTestContext()
	err := NewError(http.StatusForbidden, CodeForbidden, "nope")
	err.Details = map[string]string{"scope": "agent:pull"}

	if sendErr := SendError(c, err); sendErr != nil {
		t.Fatalf("SendError returned error: %v", sendErr)
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
	got := decodeError(t, rec)
	if got.Error.Code != CodeForbidden || got.Error.Message != "nope" {
		t.Fatalf("unexpected error body: %+v", got)
	}
	if got.Error.Details == nil {
		t.Fatalf("details should be included")
	}
}

func TestSendErrorHandlesEchoAndUnknownErrors(t *testing.T) {
	c, rec := newTestContext()
	if err := SendError(c, echo.NewHTTPError(http.StatusTeapot, "short and stout")); err != nil {
		t.Fatalf("SendError echo error: %v", err)
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("echo status = %d", rec.Code)
	}
	if got := decodeError(t, rec); got.Error.Code != CodeInternal {
		t.Fatalf("echo error code = %s", got.Error.Code)
	}

	c, rec = newTestContext()
	if err := SendError(c, errors.New("boom")); err != nil {
		t.Fatalf("SendError unknown error: %v", err)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unknown status = %d", rec.Code)
	}
	if got := decodeError(t, rec); got.Error.Message != "internal error" {
		t.Fatalf("unknown message = %q", got.Error.Message)
	}
}

func TestHelpersAndContextAccessors(t *testing.T) {
	if Unauthorized("").Message != "认证失败" {
		t.Fatalf("Unauthorized should use default message")
	}
	if NotFound("").Message != "资源不存在" {
		t.Fatalf("NotFound should use default message")
	}
	if PaymentRequired("").Code != CodePaymentRequired {
		t.Fatalf("PaymentRequired should set payment code")
	}
	if ServiceUnavailable("").Message != "服务暂不可用" {
		t.Fatalf("ServiceUnavailable should use default message")
	}

	c, _ := newTestContext()
	c.Set(string(CtxKeyUserID), "u_1")
	c.Set(string(CtxKeyAdmin), true)
	c.Set(string(CtxKeyAuthMethod), "apikey")
	c.Set(string(CtxKeyAuthScopes), []string{"tasks:write", "agent:pull"})
	if UserIDFrom(c) != "u_1" || !IsAdmin(c) || AuthMethodFrom(c) != "apikey" {
		t.Fatalf("context accessors returned unexpected values")
	}
	if !HasScope(c, "agent:pull") || HasScope(c, "missing") {
		t.Fatalf("scope lookup returned unexpected result")
	}
}
