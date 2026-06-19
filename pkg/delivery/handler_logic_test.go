package delivery

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestDeliveryHandlerValidationAndRoutes(t *testing.T) {
	h := NewHandler(&Service{})
	userID := uuid.NewString()
	id := uuid.NewString()

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    *deliveryHandlerRequest
		want   int
	}{
		{name: "create target missing user", method: h.CreateTarget, req: &deliveryHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "create target invalid json", method: h.CreateTarget, req: &deliveryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: "{"}, want: http.StatusBadRequest},
		{name: "create target validation", method: h.CreateTarget, req: &deliveryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "list targets missing user", method: h.ListTargets, req: &deliveryHandlerRequest{method: http.MethodGet, target: "/"}, want: http.StatusUnauthorized},
		{name: "delete invalid id", method: h.DeleteTarget, req: &deliveryHandlerRequest{method: http.MethodDelete, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "default invalid id", method: h.SetDefault, req: &deliveryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "deliver invalid run id", method: h.DeliverRun, req: &deliveryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "deliver invalid target id", method: h.DeliverRun, req: &deliveryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{"target_id":"bad"}`, params: map[string]string{"id": id}}, want: http.StatusBadRequest},
		{name: "list deliveries invalid run id", method: h.ListDeliveries, req: &deliveryHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "retry invalid id", method: h.RetryDelivery, req: &deliveryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDeliveryTestContext(tc.req)
			requireDeliveryHTTPStatus(t, tc.method(c), tc.want)
		})
	}

	c := newDeliveryTestContext(&deliveryHandlerRequest{method: http.MethodGet, target: "/", userID: userID})
	if got, err := userIDFromCtx(c); err != nil || got.String() != userID {
		t.Fatalf("userIDFromCtx valid = %s %v", got, err)
	}
	c = newDeliveryTestContext(&deliveryHandlerRequest{method: http.MethodGet, target: "/", userID: "bad"})
	requireDeliveryHTTPStatus(t, userIDFromCtxOnly(c), http.StatusUnauthorized)
	c = newDeliveryTestContext(&deliveryHandlerRequest{method: http.MethodGet, target: "/", params: map[string]string{"id": id}})
	if got, err := pathID(c); err != nil || got.String() != id {
		t.Fatalf("pathID valid = %s %v", got, err)
	}

	e := echo.New()
	api := e.Group("/api/v1")
	noop := func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	h.RegisterProtected(api, noop)
	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/delivery-targets",
		"GET /api/v1/delivery-targets",
		"DELETE /api/v1/delivery-targets/:id",
		"POST /api/v1/delivery-targets/:id/default",
		"POST /api/v1/runs/:id/deliver",
		"GET /api/v1/runs/:id/deliveries",
		"POST /api/v1/deliveries/:id/retry",
	} {
		if !routes[route] {
			t.Fatalf("missing route %s", route)
		}
	}
}

type deliveryHandlerRequest struct {
	method  string
	target  string
	body    string
	userID  string
	params  map[string]string
	headers map[string]string
}

func newDeliveryTestContext(spec *deliveryHandlerRequest) echo.Context {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	if len(spec.params) > 0 {
		names := make([]string, 0, len(spec.params))
		values := make([]string, 0, len(spec.params))
		for name, value := range spec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c
}

func requireDeliveryHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if he.Status != want {
		t.Fatalf("HTTP status = %d (%s), want %d", he.Status, he.Message, want)
	}
}

func userIDFromCtxOnly(c echo.Context) error {
	_, err := userIDFromCtx(c)
	return err
}
