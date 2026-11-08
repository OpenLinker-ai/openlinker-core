package externalexecution

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func decodeJSONObject(t *testing.T, raw string) map[string]interface{} {
	t.Helper()
	var value map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func requireInputInvalid(t *testing.T, err error, source, property, rule string) {
	t.Helper()
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusUnprocessableEntity || httpErr.Code != executionInputInvalidCode {
		t.Fatalf("error = %#v, want 422 %s", err, executionInputInvalidCode)
	}
	details, ok := httpErr.Details.(map[string]interface{})
	if !ok || details["source"] != source || details["rule"] != rule {
		t.Fatalf("details = %#v, want source=%s rule=%s", httpErr.Details, source, rule)
	}
	if got, _ := details["property"].(string); got != property {
		t.Fatalf("details.property = %q, want %q", got, property)
	}
}

func TestValidateExternalExecutionInput(t *testing.T) {
	cases := []struct {
		name, schema, input string
		property, rule      string // rule "" means accepted
	}{
		// Base keywords keep the run-time validator's semantics.
		{"no schema keywords", `{"type":"object"}`, `{"anything":1}`, "", ""},
		{"undeclared keys allowed by default", `{"type":"object","properties":{"a":{"type":"string"}}}`, `{"a":"x","extra":1}`, "", ""},
		{"type", `{"type":"object","properties":{"a":{"type":"string"}}}`, `{"a":1}`, "a", SchemaRuleType},
		{"null is not a string", `{"type":"object","properties":{"a":{"type":"string"}}}`, `{"a":null}`, "a", SchemaRuleType},
		{"integer", `{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":2.5}`, "n", SchemaRuleType},
		{"enum", `{"type":"object","properties":{"c":{"type":"string","enum":["a","b"]}}}`, `{"c":""}`, "c", SchemaRuleEnum},
		{"required", `{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}`, `{}`, "a", SchemaRuleRequired},
		{"additionalProperties false does not name the key", `{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}`, `{"a":"x","secret_key":1}`, "", SchemaRuleAdditionalProperties},

		// Scalar keywords.
		{"const ok", `{"type":"object","properties":{"m":{"const":"fast"}}}`, `{"m":"fast"}`, "", ""},
		{"const mismatch", `{"type":"object","properties":{"m":{"const":"fast"}}}`, `{"m":"slow"}`, "m", SchemaRuleConst},
		{"const number", `{"type":"object","properties":{"m":{"const":1}}}`, `{"m":1.0}`, "", ""},
		{"minLength runes", `{"type":"object","properties":{"s":{"type":"string","minLength":5}}}`, `{"s":"héllo"}`, "", ""},
		{"minLength short", `{"type":"object","properties":{"s":{"type":"string","minLength":5}}}`, `{"s":"hell"}`, "s", SchemaRuleMinLength},
		{"maxLength counts runes not bytes", `{"type":"object","properties":{"s":{"type":"string","maxLength":5}}}`, `{"s":"😀😀😀😀😀"}`, "", ""},
		{"maxLength long", `{"type":"object","properties":{"s":{"type":"string","maxLength":5}}}`, `{"s":"héllo!"}`, "s", SchemaRuleMaxLength},
		{"length ignores non-strings", `{"type":"object","properties":{"s":{"type":["string","number"],"maxLength":1}}}`, `{"s":12345}`, "", ""},
		{"maxLength malformed fails closed", `{"type":"object","properties":{"s":{"type":"string","maxLength":"5"}}}`, `{"s":"a"}`, "s", SchemaRuleMaxLength},
		{"pattern match", `{"type":"object","properties":{"p":{"type":"string","pattern":"^[a-z]+$"}}}`, `{"p":"abc"}`, "", ""},
		{"pattern unanchored", `{"type":"object","properties":{"p":{"type":"string","pattern":"abc"}}}`, `{"p":"xxabcxx"}`, "", ""},
		{"pattern mismatch", `{"type":"object","properties":{"p":{"type":"string","pattern":"^[a-z]+$"}}}`, `{"p":"ABC"}`, "p", SchemaRulePattern},
		{"pattern does not compile", `{"type":"object","properties":{"p":{"type":"string","pattern":"^(?=a)"}}}`, `{"p":"a"}`, "p", SchemaRulePattern},
		{"minimum equal", `{"type":"object","properties":{"x":{"type":"number","minimum":5}}}`, `{"x":5}`, "", ""},
		{"minimum below", `{"type":"object","properties":{"x":{"type":"number","minimum":5}}}`, `{"x":4.999}`, "x", SchemaRuleMinimum},
		{"exclusiveMinimum equal", `{"type":"object","properties":{"x":{"type":"number","exclusiveMinimum":5}}}`, `{"x":5}`, "x", SchemaRuleExclusiveMinimum},
		{"exclusiveMinimum above", `{"type":"object","properties":{"x":{"type":"number","exclusiveMinimum":5}}}`, `{"x":5.0001}`, "", ""},
		{"maximum equal", `{"type":"object","properties":{"x":{"type":"number","maximum":5}}}`, `{"x":5}`, "", ""},
		{"exclusiveMaximum equal", `{"type":"object","properties":{"x":{"type":"number","exclusiveMaximum":5}}}`, `{"x":5}`, "x", SchemaRuleExclusiveMaximum},
		{"draft-04 exclusive minimum", `{"type":"object","properties":{"x":{"type":"number","minimum":5,"exclusiveMinimum":true}}}`, `{"x":5}`, "x", SchemaRuleExclusiveMinimum},
		{"draft-04 false is inclusive", `{"type":"object","properties":{"x":{"type":"number","maximum":5,"exclusiveMaximum":false}}}`, `{"x":5}`, "", ""},
		{"mixed range", `{"type":"object","properties":{"x":{"type":"number","exclusiveMinimum":0,"maximum":10}}}`, `{"x":10}`, "", ""},
		{"multipleOf ok", `{"type":"object","properties":{"x":{"type":"number","multipleOf":0.5}}}`, `{"x":2.5}`, "", ""},
		{"multipleOf hundredths of an integer", `{"type":"object","properties":{"x":{"type":"number","multipleOf":0.01}}}`, `{"x":1}`, "", ""},
		{"multipleOf hundredths of a price", `{"type":"object","properties":{"x":{"type":"number","multipleOf":0.01}}}`, `{"x":19.99}`, "", ""},
		{"multipleOf tenths", `{"type":"object","properties":{"x":{"type":"number","multipleOf":0.1}}}`, `{"x":0.3}`, "", ""},
		{"multipleOf finer than allowed", `{"type":"object","properties":{"x":{"type":"number","multipleOf":0.01}}}`, `{"x":1.005}`, "x", SchemaRuleMultipleOf},
		{"maximum decimal text", `{"type":"object","properties":{"x":{"type":"number","maximum":0.3}}}`, `{"x":0.3}`, "", ""},
		{"multipleOf mismatch", `{"type":"object","properties":{"x":{"type":"number","multipleOf":2}}}`, `{"x":3}`, "x", SchemaRuleMultipleOf},
		{"multipleOf zero fails closed", `{"type":"object","properties":{"x":{"type":"number","multipleOf":0}}}`, `{"x":3}`, "x", SchemaRuleMultipleOf},
		{"bounds ignore non-numbers", `{"type":"object","properties":{"x":{"type":["number","boolean"],"maximum":1}}}`, `{"x":true}`, "", ""},

		// Nested schemas are enforced wherever the keyword appears.
		{"nested property", `{"type":"object","properties":{"doc":{"type":"object","properties":{"t":{"type":"string","maxLength":2}}}}}`, `{"doc":{"t":"abc"}}`, "doc", SchemaRuleMaxLength},
		{"array items", `{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string","pattern":"^#"}}}}`, `{"tags":["#a","b"]}`, "tags", SchemaRulePattern},
		{"additionalProperties schema", `{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":{"type":"string","maxLength":1}}`, `{"a":"x","other":"long"}`, "", SchemaRuleMaxLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateExternalExecutionInput(decodeJSONObject(t, tc.input), decodeJSONObject(t, tc.schema))
			if tc.rule == "" {
				if err != nil {
					t.Fatalf("want accepted, got %v", err)
				}
				return
			}
			requireInputInvalid(t, err, executionInputSourceInputSchema, tc.property, tc.rule)
		})
	}

	if err := validateExternalExecutionInput(map[string]interface{}{"a": json.Number("5")},
		decodeJSONObject(t, `{"type":"object","properties":{"a":{"type":"number","maximum":5}}}`)); err != nil {
		t.Fatalf("json.Number input rejected: %v", err)
	}
	if err := validateExternalExecutionInput(map[string]interface{}{"a": "anything"}, nil); err != nil {
		t.Fatalf("missing schema rejected: %v", err)
	}
}

func TestValidateExternalExecutionInputNeverEchoesInput(t *testing.T) {
	schema := decodeJSONObject(t, `{"type":"object","properties":{"note":{"type":"string","maxLength":3}},"additionalProperties":false}`)
	for _, input := range []map[string]interface{}{
		{"note": "buyer-secret-value"},
		{"note": "ok", "buyer-secret-key": "x"},
	} {
		err := validateExternalExecutionInput(input, schema)
		if err == nil {
			t.Fatalf("input %v accepted", input)
		}
		encoded, _ := json.Marshal(httpx.ErrorResponse{Error: httpx.ErrorBody{
			Code: err.(*httpx.HTTPError).Code, Message: err.(*httpx.HTTPError).Message, Details: err.(*httpx.HTTPError).Details,
		}})
		if strings.Contains(string(encoded), "buyer-secret") {
			t.Fatalf("rejection echoes input: %s", encoded)
		}
	}
}

func TestIsCapabilityInputRejection(t *testing.T) {
	if !isCapabilityInputRejection(&httpx.HTTPError{Status: http.StatusUnprocessableEntity, Code: httpx.ErrorCode("RUN_INPUT_SCHEMA_MISMATCH")}) {
		t.Fatal("RUN_INPUT_SCHEMA_MISMATCH 422 must be recognized")
	}
	for _, other := range []error{
		httpx.ServiceUnavailable("busy"),
		&httpx.HTTPError{Status: http.StatusConflict, Code: httpx.ErrorCode("RUN_INPUT_SCHEMA_MISMATCH")},
		&httpx.HTTPError{Status: http.StatusUnprocessableEntity, Code: httpx.CodeUnprocessable},
		errors.New("plain"),
	} {
		if isCapabilityInputRejection(other) {
			t.Fatalf("%v must not be treated as a capability rejection", other)
		}
	}
}
