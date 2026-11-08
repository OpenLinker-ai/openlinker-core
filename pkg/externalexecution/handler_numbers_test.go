package externalexecution

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// Numbers decoded from real HTTP bodies are float64. Proofs and start-time
// enforcement must read them as their decimal text, not their binary value.
func TestHandlerDecodedNumbersUseDecimalSemantics(t *testing.T) {
	signer, authorizer := testServiceJWT(t, &memoryReplayStore{seen: map[string]struct{}{}})
	ownerID, buyerID, targetID := uuid.New(), uuid.New(), uuid.New()
	agents := callableAgent(targetID)
	var capabilitySchema map[string]interface{}
	if err := json.Unmarshal([]byte(`{"type":"object","properties":{"price":{"type":"number","multipleOf":0.1}}}`), &capabilitySchema); err != nil {
		t.Fatal(err)
	}
	agents.onboarding = &agent.OnboardingResponse{Capability: &agent.CapabilityResponse{
		Version: 1, InputSchema: capabilitySchema, OutputSchema: map[string]interface{}{"type": "object"},
	}}
	runID := uuid.New()
	runtimeSvc := &fakeRuntimeService{
		startResponse: &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
		getResponse:   &runtime.RunResponse{RunID: runID.String(), Status: "running", StartedAt: time.Now()},
	}
	handler := NewHandler(NewService(agents, runtimeSvc, &fakeWorkflowService{}, newMemoryStore(ownerID)), authorizer)

	post := func(path, scope string, actor uuid.UUID, body string, serve func(echo.Context) error) (*httptest.ResponseRecorder, error) {
		t.Helper()
		token, err := signer.Sign(scope, actor.String())
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set(echo.HeaderAuthorization, "Bearer "+token)
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		recorder := httptest.NewRecorder()
		return recorder, serve(echo.New().NewContext(req, recorder))
	}
	validate := func(listing string) TargetValidationResponse {
		t.Helper()
		recorder, err := post("/internal/external-execution-targets/validate", ScopeValidateTarget, ownerID,
			`{"target_type":"agent","target_id":"`+targetID.String()+`","input_schema":`+listing+`}`, handler.ValidateTarget)
		if err != nil {
			t.Fatal(err)
		}
		var resp TargetValidationResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// 0.3 is a multiple of 0.1, although 0.3/0.1 is not an integer in binary.
	listing := `{"type":"object","properties":{"price":{"type":"number","multipleOf":0.3}}}`
	resp := validate(listing)
	if !resp.Executable || resp.UnavailableDetail != nil {
		t.Fatalf("multipleOf 0.3 over 0.1 = %#v", resp)
	}
	if detail := validate(`{"type":"object","properties":{"price":{"type":"number","multipleOf":0.15}}}`).UnavailableDetail; detail == nil || detail.Rule != SchemaRuleMultipleOf {
		t.Fatalf("multipleOf 0.15 over 0.1 detail = %#v", detail)
	}

	// Start-time enforcement of a cents listing over an unconstrained field.
	capabilitySchema["properties"] = map[string]interface{}{"price": map[string]interface{}{"type": "number"}}
	listing = `{"type":"object","properties":{"price":{"type":"number","multipleOf":0.01}}}`
	contractHash := validate(listing).ContractHash
	start := func(price string) error {
		t.Helper()
		_, err := post("/internal/external-executions", ScopeStartExecution, buyerID,
			`{"external_request_id":"`+uuid.NewString()+`","target_type":"agent","target_id":"`+targetID.String()+
				`","input":{"price":`+price+`},"trace_id":"trace-cents","expected_contract_hash":"`+contractHash+
				`","input_schema":`+listing+`}`, handler.StartExecution)
		return err
	}
	for _, price := range []string{"1", "19.99", "0.07", "1e2", "0.010000000000000001"} {
		if err := start(price); err != nil {
			t.Fatalf("price %s rejected: %v", price, err)
		}
	}
	err := start("1.005")
	requireInputInvalid(t, err, executionInputSourceInputSchema, "price", SchemaRuleMultipleOf)
	assertHTTPStatusAndCode(t, err, http.StatusUnprocessableEntity, httpx.ErrorCode("EXECUTION_INPUT_INVALID"))
}
