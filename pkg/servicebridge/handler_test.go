package servicebridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/usertoken"
)

func TestHandlerRejectsMissingWrongAndEmptyInternalSecret(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured string
		provided   string
	}{
		{name: "missing", configured: "secret"},
		{name: "wrong", configured: "secret", provided: "not-it"},
		{name: "fail closed", provided: "anything"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/internal/hosted/service-targets/validate", nil)
			req.Header.Set(usertoken.InternalTokenHeader, tc.provided)
			c := e.NewContext(req, httptest.NewRecorder())
			err := NewHandler(&fakeBridgeHandlerService{}, tc.configured).ValidateTarget(c)
			assertHTTPStatus(t, err, http.StatusUnauthorized)
		})
	}
}

type fakeBridgeHandlerService struct{}

func (*fakeBridgeHandlerService) ValidateTarget(context.Context, *TargetValidationRequest) (*TargetValidationResponse, error) {
	return nil, httpx.Internal("unexpected")
}

func (*fakeBridgeHandlerService) StartExecution(context.Context, *ExecutionRequest) (*ExecutionStartResponse, error) {
	return nil, httpx.Internal("unexpected")
}

func (*fakeBridgeHandlerService) GetExecution(context.Context, string) (*ExecutionStatusResponse, error) {
	return nil, httpx.Internal("unexpected")
}
