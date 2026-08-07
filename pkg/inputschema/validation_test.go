package inputschema

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestValidateCapabilitySchemaBoundaries(t *testing.T) {
	valid := map[string]interface{}{
		"type": []interface{}{"object", "null"},
		"properties": map[string]interface{}{
			"name": map[string]interface{}{"type": "string", "enum": []interface{}{"alpha", "beta"}},
			"rows": map[string]interface{}{
				"type":  "array",
				"items": map[string]interface{}{"type": "integer"},
			},
		},
		"required":             []interface{}{"name"},
		"additionalProperties": map[string]interface{}{"type": "string"},
	}
	if err := ValidateCapabilitySchema(valid, "contract"); err != nil {
		t.Fatalf("valid capability schema rejected: %v", err)
	}
	if err := ValidateInputSchema(nestedObjectSchema(MaxDepth)); err != nil {
		t.Fatalf("schema at MaxDepth rejected: %v", err)
	}

	tests := []struct {
		name    string
		schema  map[string]interface{}
		wantErr string
	}{
		{name: "nil schema", schema: nil, wantErr: "必须是 JSON object"},
		{name: "root must allow object", schema: map[string]interface{}{"type": "string"}, wantErr: "必须包含 object"},
		{name: "type shape", schema: map[string]interface{}{"type": 42}, wantErr: "必须是 string 或 string array"},
		{name: "unsupported type", schema: map[string]interface{}{"type": []interface{}{"object", "date"}}, wantErr: "不支持"},
		{name: "required shape", schema: map[string]interface{}{"type": "object", "required": "name"}, wantErr: "必须是 string array"},
		{name: "required item", schema: map[string]interface{}{"type": "object", "required": []interface{}{1}}, wantErr: "数组项必须是 string"},
		{name: "empty required", schema: map[string]interface{}{"type": "object", "required": []interface{}{""}}, wantErr: "不能包含空字段名"},
		{name: "duplicate required", schema: map[string]interface{}{"type": "object", "required": []interface{}{"name", "name"}}, wantErr: "重复字段"},
		{name: "properties shape", schema: map[string]interface{}{"type": "object", "properties": []interface{}{}}, wantErr: "properties 必须是 object"},
		{name: "property schema shape", schema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"name": "string"}}, wantErr: "properties.name 必须是 object"},
		{name: "items shape", schema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"rows": map[string]interface{}{"type": "array", "items": "string"}}}, wantErr: "items 必须是 object"},
		{name: "additional properties shape", schema: map[string]interface{}{"type": "object", "additionalProperties": 1}, wantErr: "必须是 boolean 或 object"},
		{name: "enum shape", schema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"name": map[string]interface{}{"type": "string", "enum": []string{"alpha"}}}}, wantErr: "enum 必须是 array"},
		{name: "depth exceeded", schema: nestedObjectSchema(MaxDepth + 1), wantErr: "嵌套过深"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCapabilitySchema(test.schema, "contract")
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateCapabilitySchema() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestValidateInputReportsStablePathsAndReasons(t *testing.T) {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"name":  map[string]interface{}{"type": "string"},
			"count": map[string]interface{}{"type": "integer"},
			"mode":  map[string]interface{}{"type": "string", "enum": []interface{}{"fast", "safe"}},
			"items": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type":                 "object",
					"properties":           map[string]interface{}{"id": map[string]interface{}{"type": "string"}},
					"required":             []interface{}{"id"},
					"additionalProperties": false,
				},
			},
		},
		"required":             []interface{}{"name", "count", "mode", "items"},
		"additionalProperties": false,
	}

	valid := map[string]interface{}{
		"name": "report", "count": json.Number("2"), "mode": "safe",
		"items": []interface{}{map[string]interface{}{"id": "row-1"}},
	}
	if err := ValidateInput(valid, schema); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}

	tests := []struct {
		name       string
		input      interface{}
		wantPath   string
		wantReason string
	}{
		{name: "root type", input: []interface{}{}, wantPath: "input", wantReason: "type_mismatch"},
		{name: "missing required", input: map[string]interface{}{"name": "report"}, wantPath: "input.count", wantReason: "missing_required"},
		{name: "integer type", input: map[string]interface{}{"name": "report", "count": 1.5, "mode": "safe", "items": []interface{}{}}, wantPath: "input.count", wantReason: "type_mismatch"},
		{name: "enum", input: map[string]interface{}{"name": "report", "count": 1, "mode": "turbo", "items": []interface{}{}}, wantPath: "input.mode", wantReason: "enum_mismatch"},
		{name: "root additional property", input: map[string]interface{}{"name": "report", "count": 1, "mode": "safe", "items": []interface{}{}, "extra": true}, wantPath: "input.extra", wantReason: "additional_property"},
		{name: "array type", input: map[string]interface{}{"name": "report", "count": 1, "mode": "safe", "items": "row"}, wantPath: "input.items", wantReason: "type_mismatch"},
		{name: "array item required", input: map[string]interface{}{"name": "report", "count": 1, "mode": "safe", "items": []interface{}{map[string]interface{}{}}}, wantPath: "input.items[0].id", wantReason: "missing_required"},
		{name: "array item type", input: map[string]interface{}{"name": "report", "count": 1, "mode": "safe", "items": []interface{}{map[string]interface{}{"id": 7}}}, wantPath: "input.items[0].id", wantReason: "type_mismatch"},
		{name: "array item additional property", input: map[string]interface{}{"name": "report", "count": 1, "mode": "safe", "items": []interface{}{map[string]interface{}{"id": "row-1", "extra": true}}}, wantPath: "input.items[0].extra", wantReason: "additional_property"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertViolation(t, ValidateInput(test.input, schema), test.wantPath, test.wantReason)
		})
	}
}

func TestValidateInputAdditionalSchemasUnionsAndSensitiveValues(t *testing.T) {
	additionalSchema := map[string]interface{}{
		"type":                 "object",
		"properties":           map[string]interface{}{"known": map[string]interface{}{"type": "string"}},
		"additionalProperties": map[string]interface{}{"type": "integer"},
	}
	if err := ValidateInput(map[string]interface{}{"known": "yes", "retries": 2}, additionalSchema); err != nil {
		t.Fatalf("integer additional property rejected: %v", err)
	}
	assertViolation(
		t,
		ValidateInput(map[string]interface{}{"known": "yes", "retries": 2.5}, additionalSchema),
		"input.retries",
		"type_mismatch",
	)

	unionSchema := map[string]interface{}{"type": []interface{}{"string", "null"}}
	for _, value := range []interface{}{"text", nil} {
		if err := ValidateJSON(value, unionSchema, "payload"); err != nil {
			t.Fatalf("union rejected %#v: %v", value, err)
		}
	}
	assertViolation(t, ValidateJSON(true, unionSchema, "payload"), "payload", "type_mismatch")

	inferredObject := map[string]interface{}{
		"properties": map[string]interface{}{"name": map[string]interface{}{"type": "string"}},
		"required":   []interface{}{"name"},
	}
	if err := ValidateInput(map[string]interface{}{"name": "ok"}, inferredObject); err != nil {
		t.Fatalf("object-keyword inference rejected valid input: %v", err)
	}

	secret := "do-not-leak-this-value"
	err := ValidateInput(secret, map[string]interface{}{"type": "integer"})
	assertViolation(t, err, "input", "type_mismatch")
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("violation leaked rejected input value: %v", err)
	}
	if err := ValidateInput(secret, nil); err != nil {
		t.Fatalf("nil schema should preserve compatibility: %v", err)
	}
}

func TestJSONNumberClassification(t *testing.T) {
	for _, value := range []interface{}{1, int32(2), int64(3), float32(4), float64(5), json.Number("6")} {
		if !IsJSONNumber(value) {
			t.Fatalf("IsJSONNumber(%T(%v)) = false", value, value)
		}
		if !ValueMatchesJSONType(value, "integer") {
			t.Fatalf("integer match rejected %T(%v)", value, value)
		}
	}
	for _, value := range []interface{}{1.5, json.Number("2.5"), json.Number("invalid"), "3"} {
		if ValueMatchesJSONType(value, "integer") {
			t.Fatalf("integer match accepted %T(%v)", value, value)
		}
	}
	if !ValueMatchesJSONType(1.5, "number") || ValueMatchesJSONType("1.5", "number") {
		t.Fatal("number classification is inconsistent")
	}
}

func nestedObjectSchema(depth int) map[string]interface{} {
	if depth == 0 {
		return map[string]interface{}{"type": "string"}
	}
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"child": nestedObjectSchema(depth - 1),
		},
	}
}

func assertViolation(t *testing.T, err error, wantPath, wantReason string) {
	t.Helper()
	if err == nil {
		t.Fatalf("ValidateInput() error = nil, want %s/%s", wantPath, wantReason)
	}
	var violation *Violation
	if !errors.As(err, &violation) {
		t.Fatalf("ValidateInput() error type = %T, want *Violation: %v", err, err)
	}
	if violation.Path != wantPath || violation.Reason != wantReason {
		t.Fatalf("violation = %#v, want path=%q reason=%q", violation, wantPath, wantReason)
	}
}
