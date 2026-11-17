package attachment

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func handlerFixture(t *testing.T) (*echo.Echo, uuid.UUID) {
	t.Helper()
	s := openStore(t, Config{})
	links, _ := NewLinks("https://example.com")
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	owner := uuid.New()
	authMw := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set(string(httpx.CtxKeyUserID), owner.String())
			c.Set(string(httpx.CtxKeyAuthMethod), "jwt")
			return next(c)
		}
	}
	NewHandler(s, links).Register(e.Group("/api/v1"), authMw)
	return e, owner
}

func upload(t *testing.T, e *echo.Echo, name string, body []byte) (int, map[string]any) {
	t.Helper()
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, RoutePath+"?filename="+name, bytes.NewReader(body)))
	var out map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &out)
	return recorder.Code, out
}

func TestHandlerUploadAndDownloadHeaders(t *testing.T) {
	e, _ := handlerFixture(t)
	code, image := upload(t, e, "cat.png", png)
	if code != http.StatusCreated || image["kind"] != KindImage || !strings.HasPrefix(image["url"].(string), "https://example.com"+RoutePath+"/") {
		t.Fatalf("image upload %d %v", code, image)
	}
	code, doc := upload(t, e, "notes.html", []byte("<script>alert(1)</script>"))
	if code != http.StatusCreated || doc["kind"] != KindFile {
		t.Fatalf("file upload %d %v", code, doc)
	}
	get := func(link string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		e.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, strings.TrimPrefix(link, "https://example.com"), nil))
		return recorder
	}
	img := get(image["url"].(string))
	if img.Code != 200 || img.Header().Get("Content-Type") != "image/png" || !bytes.Equal(img.Body.Bytes(), png) ||
		!strings.HasPrefix(img.Header().Get("Content-Disposition"), "inline") {
		t.Fatalf("image download %d %v", img.Code, img.Header())
	}
	file := get(doc["url"].(string))
	for header, want := range map[string]string{
		"Content-Type": "application/octet-stream", "X-Content-Type-Options": "nosniff", "Cache-Control": "private, no-store",
		"Content-Security-Policy": "default-src 'none'; sandbox",
	} {
		if got := file.Header().Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if !strings.HasPrefix(file.Header().Get("Content-Disposition"), "attachment") || !strings.Contains(file.Header().Get("Content-Disposition"), "notes.html") {
		t.Fatalf("disposition %q", file.Header().Get("Content-Disposition"))
	}
	if get("https://example.com"+RoutePath+"/"+strings.Repeat("c", 64)).Code != http.StatusNotFound {
		t.Fatal("unknown token served")
	}
	if code, _ := upload(t, e, "empty", nil); code != http.StatusUnprocessableEntity {
		t.Fatalf("empty upload %d", code)
	}
	recorder := httptest.NewRecorder()
	big := httptest.NewRequest(http.MethodPost, RoutePath, bytes.NewReader(make([]byte, MaxFileBytes+1)))
	e.ServeHTTP(recorder, big)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized upload %d", recorder.Code)
	}
}
