package externalexecution

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/inputschema"
)

func TestExternalInputSchemaCompatible(t *testing.T) {
	capability := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"topic": map[string]interface{}{"type": "string"},
			"count": map[string]interface{}{"type": "number"},
		},
		"required":             []interface{}{"topic"},
		"additionalProperties": false,
	}
	compatible := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"topic": map[string]interface{}{"type": "string"},
			"count": map[string]interface{}{"type": "integer"},
		},
		"required": []interface{}{"topic"},
	}
	if detail := externalInputSchemaIncompatibility(compatible, capability); detail != nil {
		t.Fatalf("expected controlled schema to be compatible, got %#v", detail)
	}

	listing := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"channel": map[string]interface{}{"type": "string", "enum": []interface{}{"email", "web"}},
			"source":  map[string]interface{}{"type": "string", "format": "uri"},
		},
	}
	enumAndFormat := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"channel": map[string]interface{}{"type": "string", "enum": []interface{}{"email", "web", "api"}, "description": "annotations are ignored"},
			"source":  map[string]interface{}{"type": "string", "format": "uri"},
		},
		"additionalProperties": false,
	}
	if detail := externalInputSchemaIncompatibility(listing, enumAndFormat); detail != nil {
		t.Fatalf("listing enum subset and matching format should be compatible, got %#v", detail)
	}
}

func TestNormalizeExternalInputSchemaRejectsHostedControlArrays(t *testing.T) {
	_, err := normalizeExternalInputSchema(json.RawMessage(`[{"key":"topic","type":"text"}]`))
	if err == nil {
		t.Fatal("Core External Execution must accept only generic JSON Schema objects")
	}
}

func objectSchema(properties map[string]interface{}, extra ...interface{}) map[string]interface{} {
	schema := map[string]interface{}{"type": "object", "properties": properties}
	for i := 0; i+1 < len(extra); i += 2 {
		schema[extra[i].(string)] = extra[i+1]
	}
	return schema
}

func prop(fields ...interface{}) map[string]interface{} {
	schema := map[string]interface{}{}
	for i := 0; i+1 < len(fields); i += 2 {
		schema[fields[i].(string)] = fields[i+1]
	}
	return schema
}

func requireDetail(t *testing.T, got *TargetUnavailableDetail, property, rule, capabilityValue string) {
	t.Helper()
	if got == nil {
		t.Fatalf("detail = nil, want property=%q rule=%q", property, rule)
	}
	if got.Property != property || got.Rule != rule {
		t.Fatalf("detail = {property:%q rule:%q value:%s}, want property=%q rule=%q", got.Property, got.Rule, got.CapabilityValue, property, rule)
	}
	if capabilityValue == "" {
		if len(got.CapabilityValue) != 0 {
			t.Fatalf("capability_value = %s, want omitted", got.CapabilityValue)
		}
		return
	}
	var want, have interface{}
	if err := json.Unmarshal([]byte(capabilityValue), &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got.CapabilityValue, &have); err != nil {
		t.Fatalf("capability_value %q is not JSON: %v", got.CapabilityValue, err)
	}
	if fmt.Sprint(want) != fmt.Sprint(have) {
		t.Fatalf("capability_value = %s, want %s", got.CapabilityValue, capabilityValue)
	}
}

func TestExternalInputSchemaIncompatibilityReportsFirstRule(t *testing.T) {
	str := prop("type", "string")
	cases := []struct {
		name       string
		listing    map[string]interface{}
		capability map[string]interface{}
		property   string
		rule       string
		value      string
	}{
		{name: "capability missing", listing: objectSchema(nil), capability: nil, rule: SchemaRuleCapabilityMissing},
		{name: "capability not an object", listing: objectSchema(nil), capability: map[string]interface{}{"type": "string"}, rule: SchemaRuleType, value: `"string"`},
		{name: "listing not an object", listing: map[string]interface{}{"type": "array"}, capability: objectSchema(nil), rule: SchemaRuleType, value: `"object"`},
		{name: "object keyword", listing: objectSchema(nil), capability: objectSchema(nil, "anyOf", []interface{}{}), rule: SchemaRuleUnsupportedKeyword, value: `"anyOf"`},
		{name: "object dependentSchemas", listing: objectSchema(nil), capability: objectSchema(nil, "dependentSchemas", map[string]interface{}{}), rule: SchemaRuleUnsupportedKeyword, value: `"dependentSchemas"`},
		{name: "additionalProperties schema", listing: objectSchema(nil), capability: objectSchema(nil, "additionalProperties", prop("type", "string")), rule: SchemaRuleUnsupportedKeyword, value: `"additionalProperties"`},
		{name: "capability properties malformed", listing: objectSchema(nil), capability: map[string]interface{}{"type": "object", "properties": []interface{}{}}, rule: SchemaRuleSchemaInvalid},
		{name: "listing properties malformed", listing: map[string]interface{}{"type": "object", "properties": "x"}, capability: objectSchema(nil), rule: SchemaRuleSchemaInvalid},
		{
			name:       "required property missing from listing",
			listing:    objectSchema(map[string]interface{}{}),
			capability: objectSchema(map[string]interface{}{"topic": str}, "required", []interface{}{"topic"}),
			property:   "topic", rule: SchemaRuleRequired,
		},
		{
			name:       "required property optional in listing",
			listing:    objectSchema(map[string]interface{}{"topic": str}),
			capability: objectSchema(map[string]interface{}{"topic": str}, "required", []interface{}{"topic"}),
			property:   "topic", rule: SchemaRuleRequired,
		},
		{
			name:       "undeclared property with additionalProperties false",
			listing:    objectSchema(map[string]interface{}{"extra": str}),
			capability: objectSchema(map[string]interface{}{"topic": str}, "additionalProperties", false),
			property:   "extra", rule: SchemaRuleAdditionalProperties, value: `false`,
		},
		{
			name:       "listing number for capability integer",
			listing:    objectSchema(map[string]interface{}{"count": prop("type", "number")}),
			capability: objectSchema(map[string]interface{}{"count": prop("type", "integer")}),
			property:   "count", rule: SchemaRuleType, value: `"integer"`,
		},
		{
			name:       "capability property without type",
			listing:    objectSchema(map[string]interface{}{"count": prop("type", "number")}),
			capability: objectSchema(map[string]interface{}{"count": prop()}),
			property:   "count", rule: SchemaRuleType,
		},
		{
			name:       "composition",
			listing:    objectSchema(map[string]interface{}{"topic": str}),
			capability: objectSchema(map[string]interface{}{"topic": prop("type", "string", "oneOf", []interface{}{})}),
			property:   "topic", rule: SchemaRuleUnsupportedKeyword, value: `"oneOf"`,
		},
		{
			name:       "format mismatch",
			listing:    objectSchema(map[string]interface{}{"source": prop("type", "string", "format", "uri")}),
			capability: objectSchema(map[string]interface{}{"source": prop("type", "string", "format", "email")}),
			property:   "source", rule: SchemaRuleFormat, value: `"email"`,
		},
		{
			name:       "enum not a subset",
			listing:    objectSchema(map[string]interface{}{"channel": prop("type", "string", "enum", []interface{}{"email", "fax"})}),
			capability: objectSchema(map[string]interface{}{"channel": prop("type", "string", "enum", []interface{}{"email", "web"})}),
			property:   "channel", rule: SchemaRuleEnum, value: `["email","web"]`,
		},
		{
			name:       "properties are checked in sorted order",
			listing:    objectSchema(map[string]interface{}{"b": prop("type", "boolean"), "a": prop("type", "boolean")}),
			capability: objectSchema(map[string]interface{}{"a": str, "b": str}),
			property:   "a", rule: SchemaRuleType, value: `"string"`,
		},
		{
			name:       "large capability value is omitted",
			listing:    objectSchema(map[string]interface{}{"channel": prop("type", "string", "enum", []interface{}{"x"})}),
			capability: objectSchema(map[string]interface{}{"channel": prop("type", "string", "enum", []interface{}{strings.Repeat("y", maxCapabilityValueBytes)})}),
			property:   "channel", rule: SchemaRuleEnum,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireDetail(t, externalInputSchemaIncompatibility(tc.listing, tc.capability), tc.property, tc.rule, tc.value)
		})
	}
}

// The pre-diagnostics checker ignored nested object/array keywords and
// unknown keywords; Core's run-time validator backstops the structural ones.
// They must not make an executable target unavailable.
func TestExternalInputSchemaKeepsPreviouslyExecutableTargets(t *testing.T) {
	str := prop("type", "string")
	cases := []struct {
		name                string
		listing, capability map[string]interface{}
	}{
		{"nested required", objectSchema(map[string]interface{}{"doc": prop("type", "object")}), objectSchema(map[string]interface{}{"doc": prop("type", "object", "required", []interface{}{"x"})})},
		{"array items", objectSchema(map[string]interface{}{"tags": prop("type", "array")}), objectSchema(map[string]interface{}{"tags": prop("type", "array", "items", str, "minItems", 1.0)})},
		{"vacuous keywords on a string", objectSchema(map[string]interface{}{"topic": str}), objectSchema(map[string]interface{}{"topic": prop("type", "string", "uniqueItems", true, "additionalProperties", false, "$ref", "#/$defs/x")})},
		{"top-level enum and ref", objectSchema(nil), objectSchema(nil, "enum", []interface{}{map[string]interface{}{}}, "$ref", "#/$defs/x", "patternProperties", map[string]interface{}{})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if detail := externalInputSchemaIncompatibility(tc.listing, tc.capability); detail != nil {
				t.Fatalf("previously executable target became unavailable: %#v", detail)
			}
		})
	}
}

// Production proves each scalar constraint from a stricter listing keyword,
// and reports the rule with the capability's value when the listing is looser.
func TestProductionSchemaCheckerProvesEnforcedScalarConstraints(t *testing.T) {
	cases := []struct {
		keyword                   string
		listing, looser, capabity map[string]interface{}
		rule, value               string
	}{
		{"const", prop("type", "string", "const", "x"), prop("type", "string", "const", "y"), prop("type", "string", "const", "x"), SchemaRuleConst, `"x"`},
		{"minLength", prop("type", "string", "minLength", 10.0), prop("type", "string"), prop("type", "string", "minLength", 1.0), SchemaRuleMinLength, `1`},
		{"maxLength", prop("type", "string", "maxLength", 100.0), prop("type", "string", "maxLength", 4001.0), prop("type", "string", "maxLength", 4000.0), SchemaRuleMaxLength, `4000`},
		{"pattern", prop("type", "string", "pattern", "^a+$"), prop("type", "string", "pattern", "^a*$"), prop("type", "string", "pattern", "^a+$"), SchemaRulePattern, `"^a+$"`},
		{"minimum", prop("type", "number", "minimum", 5.0), prop("type", "number", "minimum", -1.0), prop("type", "number", "minimum", 0.0), SchemaRuleMinimum, `0`},
		{"exclusiveMinimum", prop("type", "number", "minimum", 5.0), prop("type", "number", "minimum", 0.0), prop("type", "number", "exclusiveMinimum", 0.0), SchemaRuleExclusiveMinimum, `0`},
		{"maximum", prop("type", "number", "maximum", 5.0), prop("type", "number", "maximum", 11.0), prop("type", "number", "maximum", 10.0), SchemaRuleMaximum, `10`},
		{"exclusiveMaximum", prop("type", "number", "maximum", 5.0), prop("type", "number", "maximum", 10.0), prop("type", "number", "exclusiveMaximum", 10.0), SchemaRuleExclusiveMaximum, `10`},
		{"multipleOf", prop("type", "number", "multipleOf", 4.0), prop("type", "number", "multipleOf", 3.0), prop("type", "number", "multipleOf", 2.0), SchemaRuleMultipleOf, `2`},
	}
	if len(cases) != len(scalarConstraints) {
		t.Fatalf("cases cover %d of %d scalar constraints", len(cases), len(scalarConstraints))
	}
	for _, tc := range cases {
		t.Run(tc.keyword, func(t *testing.T) {
			capability := objectSchema(map[string]interface{}{"v": tc.capabity})
			if detail := externalInputSchemaIncompatibility(objectSchema(map[string]interface{}{"v": tc.listing}), capability); detail != nil {
				t.Fatalf("stricter listing should be compatible, got %#v", detail)
			}
			requireDetail(t, externalInputSchemaIncompatibility(objectSchema(map[string]interface{}{"v": tc.looser}), capability), "v", tc.rule, tc.value)
			disabled := schemaChecker{enforced: map[string]bool{}}
			requireDetail(t, disabled.incompatibility(objectSchema(map[string]interface{}{"v": tc.listing}), capability), "v", tc.rule, tc.value)
		})
	}
}

// The production proof gate must be exactly the set of keywords the start
// input enforcer rejects. Ordinary Runs keep the unchanged run-time validator,
// which still ignores these keywords.
func TestProductionSchemaGateMatchesStartInputEnforcement(t *testing.T) {
	probes := map[string]struct {
		schema, violating, satisfying string
	}{
		"const":            {`{"const":"x"}`, `"y"`, `"x"`},
		"minLength":        {`{"type":"string","minLength":3}`, `"ab"`, `"abc"`},
		"maxLength":        {`{"type":"string","maxLength":2}`, `"abc"`, `"日本"`},
		"pattern":          {`{"type":"string","pattern":"^a+$"}`, `"b"`, `"aa"`},
		"minimum":          {`{"type":"number","minimum":5}`, `4.5`, `5`},
		"exclusiveMinimum": {`{"type":"number","exclusiveMinimum":5}`, `5`, `5.5`},
		"maximum":          {`{"type":"number","maximum":5}`, `5.5`, `5`},
		"exclusiveMaximum": {`{"type":"number","exclusiveMaximum":5}`, `5`, `4.5`},
		"multipleOf":       {`{"type":"number","multipleOf":2}`, `3`, `4`},
	}
	if len(productionSchemaChecker.enforced) != len(scalarConstraints) {
		t.Fatalf("production gate enables %d keywords, want all %d", len(productionSchemaChecker.enforced), len(scalarConstraints))
	}
	decode := func(raw string) interface{} {
		var value interface{}
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	for _, constraint := range scalarConstraints {
		probe, ok := probes[constraint.keyword]
		if !ok {
			t.Fatalf("no enforcement probe for %s", constraint.keyword)
		}
		schema := decode(`{"type":"object","properties":{"v":` + probe.schema + `}}`).(map[string]interface{})
		violating := map[string]interface{}{"v": decode(probe.violating)}
		satisfying := map[string]interface{}{"v": decode(probe.satisfying)}

		rejected := validateExternalExecutionInput(violating, schema) != nil
		if rejected != productionSchemaChecker.enforced[constraint.keyword] {
			t.Fatalf("%s: start enforcer rejects=%v, production proof enabled=%v",
				constraint.keyword, rejected, productionSchemaChecker.enforced[constraint.keyword])
		}
		if err := validateExternalExecutionInput(satisfying, schema); err != nil {
			t.Fatalf("%s: satisfying input rejected: %v", constraint.keyword, err)
		}
		if err := inputschema.ValidateInput(violating, schema); err != nil {
			t.Fatalf("%s: ordinary run-time validation changed: %v", constraint.keyword, err)
		}
	}
}

func TestScalarConstraintProofs(t *testing.T) {
	n := func(value string) json.Number { return json.Number(value) }
	cases := []struct {
		name       string
		listing    map[string]interface{}
		capability map[string]interface{}
		rule       string // "" means compatible
	}{
		// const
		{"const equal", prop("type", "string", "const", "fast"), prop("type", "string", "const", "fast"), ""},
		{"const differs", prop("type", "string", "const", "slow"), prop("type", "string", "const", "fast"), SchemaRuleConst},
		{"const float equals json.Number", prop("type", "number", "const", 1.0), prop("type", "number", "const", n("1.0")), ""},
		{"const object deep-equal", prop("type", "object", "const", map[string]interface{}{"a": []interface{}{1.0, "x"}}), prop("type", "object", "const", map[string]interface{}{"a": []interface{}{n("1"), "x"}}), ""},
		{"const single enum value", prop("type", "string", "enum", []interface{}{"fast"}), prop("type", "string", "const", "fast"), ""},
		{"const enum with two values", prop("type", "string", "enum", []interface{}{"fast", "fast2"}), prop("type", "string", "const", "fast"), SchemaRuleConst},
		{"const unconstrained listing", prop("type", "string"), prop("type", "string", "const", "fast"), SchemaRuleConst},

		// minLength / maxLength
		{"minLength stricter", prop("type", "string", "minLength", 3.0), prop("type", "string", "minLength", 2.0), ""},
		{"minLength equal json.Number", prop("type", "string", "minLength", n("2")), prop("type", "string", "minLength", 2.0), ""},
		{"minLength looser", prop("type", "string", "minLength", 1.0), prop("type", "string", "minLength", 2.0), SchemaRuleMinLength},
		{"minLength missing", prop("type", "string"), prop("type", "string", "minLength", 1.0), SchemaRuleMinLength},
		{"minLength enum by runes", prop("type", "string", "enum", []interface{}{"héllo", "日本語ab"}), prop("type", "string", "minLength", 5.0), ""},
		{"minLength enum too short", prop("type", "string", "enum", []interface{}{"hello", "hi"}), prop("type", "string", "minLength", 3.0), SchemaRuleMinLength},
		{"minLength empty enum", prop("type", "string", "enum", []interface{}{}), prop("type", "string", "minLength", 1.0), SchemaRuleMinLength},
		{"minLength invalid capability", prop("type", "string", "minLength", 3.0), prop("type", "string", "minLength", 1.5), SchemaRuleSchemaInvalid},
		{"maxLength stricter", prop("type", "string", "maxLength", 100.0), prop("type", "string", "maxLength", 4000.0), ""},
		{"maxLength equal", prop("type", "string", "maxLength", 4000.0), prop("type", "string", "maxLength", n("4000")), ""},
		{"maxLength looser", prop("type", "string", "maxLength", 4001.0), prop("type", "string", "maxLength", 4000.0), SchemaRuleMaxLength},
		{"maxLength enum counts runes not bytes", prop("type", "string", "enum", []interface{}{"héllo", "😀😀😀😀😀"}), prop("type", "string", "maxLength", 5.0), ""},
		{"maxLength enum too long", prop("type", "string", "enum", []interface{}{"héllo!"}), prop("type", "string", "maxLength", 5.0), SchemaRuleMaxLength},
		{"maxLength mixed enum", prop("type", []interface{}{"string", "number"}, "enum", []interface{}{"a", 1.0}), prop("type", []interface{}{"string", "number"}, "maxLength", 5.0), SchemaRuleMaxLength},
		{"maxLength negative capability", prop("type", "string", "maxLength", 1.0), prop("type", "string", "maxLength", -1.0), SchemaRuleSchemaInvalid},
		{"maxLength string capability", prop("type", "string", "maxLength", 1.0), prop("type", "string", "maxLength", "5"), SchemaRuleSchemaInvalid},

		// pattern
		{"pattern identical", prop("type", "string", "pattern", `^[a-z]+$`), prop("type", "string", "pattern", `^[a-z]+$`), ""},
		{"pattern different", prop("type", "string", "pattern", `^[a-c]+$`), prop("type", "string", "pattern", `^[a-z]+$`), SchemaRulePattern},
		{"pattern enum matches", prop("type", "string", "enum", []interface{}{"abc", "zz"}), prop("type", "string", "pattern", `^[a-z]+$`), ""},
		{"pattern enum unanchored search", prop("type", "string", "enum", []interface{}{"x-abc-y"}), prop("type", "string", "pattern", `abc`), ""},
		{"pattern enum mismatch", prop("type", "string", "enum", []interface{}{"abc", "A"}), prop("type", "string", "pattern", `^[a-z]+$`), SchemaRulePattern},
		{"pattern does not compile", prop("type", "string", "pattern", `^(?=a)`), prop("type", "string", "pattern", `^(?=a)`), SchemaRulePattern},
		{"pattern enum but invalid regex", prop("type", "string", "enum", []interface{}{"a"}), prop("type", "string", "pattern", `(`), SchemaRulePattern},
		{"pattern not a string", prop("type", "string", "pattern", "a"), prop("type", "string", "pattern", 1.0), SchemaRuleSchemaInvalid},

		// minimum / exclusiveMinimum
		{"minimum equal", prop("type", "number", "minimum", 5.0), prop("type", "number", "minimum", 5.0), ""},
		{"minimum stricter", prop("type", "integer", "minimum", 6.0), prop("type", "number", "minimum", 5.0), ""},
		{"minimum looser", prop("type", "number", "minimum", 4.0), prop("type", "number", "minimum", 5.0), SchemaRuleMinimum},
		{"minimum from listing exclusive at same value", prop("type", "number", "exclusiveMinimum", 5.0), prop("type", "number", "minimum", 5.0), ""},
		{"minimum from listing exclusive below", prop("type", "number", "exclusiveMinimum", 4.0), prop("type", "number", "minimum", 5.0), SchemaRuleMinimum},
		{"minimum missing", prop("type", "number"), prop("type", "number", "minimum", 5.0), SchemaRuleMinimum},
		{"minimum json.Number vs float", prop("type", "number", "minimum", n("5.5")), prop("type", "number", "minimum", 5.5), ""},
		{"minimum json.Number exact beyond float", prop("type", "number", "minimum", n("0.30000000000000001")), prop("type", "number", "minimum", n("0.3")), ""},
		{"exclusiveMinimum needs exclusive at equality", prop("type", "number", "minimum", 5.0), prop("type", "number", "exclusiveMinimum", 5.0), SchemaRuleExclusiveMinimum},
		{"exclusiveMinimum exclusive equal", prop("type", "number", "exclusiveMinimum", 5.0), prop("type", "number", "exclusiveMinimum", 5.0), ""},
		{"exclusiveMinimum inclusive above", prop("type", "number", "minimum", 5.01), prop("type", "number", "exclusiveMinimum", 5.0), ""},
		{"exclusiveMinimum tightest of both listing bounds", prop("type", "number", "minimum", 5.0, "exclusiveMinimum", 5.0), prop("type", "number", "exclusiveMinimum", 5.0), ""},
		{"exclusiveMinimum draft-04 boolean capability", prop("type", "number", "minimum", 9.0), prop("type", "number", "minimum", 5.0, "exclusiveMinimum", true), SchemaRuleExclusiveMinimum},
		{"draft-04 boolean listing reads as inclusive", prop("type", "number", "minimum", 5.0, "exclusiveMinimum", true), prop("type", "number", "exclusiveMinimum", 5.0), SchemaRuleExclusiveMinimum},

		// maximum / exclusiveMaximum
		{"maximum equal", prop("type", "number", "maximum", 10.0), prop("type", "number", "maximum", 10.0), ""},
		{"maximum looser", prop("type", "number", "maximum", 11.0), prop("type", "number", "maximum", 10.0), SchemaRuleMaximum},
		{"maximum from listing exclusive at same value", prop("type", "number", "exclusiveMaximum", 10.0), prop("type", "number", "maximum", 10.0), ""},
		{"maximum from listing exclusive above", prop("type", "number", "exclusiveMaximum", 11.0), prop("type", "number", "maximum", 10.0), SchemaRuleMaximum},
		{"exclusiveMaximum needs exclusive at equality", prop("type", "number", "maximum", 10.0), prop("type", "number", "exclusiveMaximum", 10.0), SchemaRuleExclusiveMaximum},
		{"exclusiveMaximum exclusive equal", prop("type", "number", "exclusiveMaximum", n("10")), prop("type", "number", "exclusiveMaximum", 10.0), ""},
		{"exclusiveMaximum inclusive below", prop("type", "number", "maximum", 9.99), prop("type", "number", "exclusiveMaximum", 10.0), ""},
		{"mixed range", prop("type", "number", "minimum", 1.0, "exclusiveMaximum", 10.0), prop("type", "number", "exclusiveMinimum", 0.0, "maximum", 10.0), ""},
		{"mixed range upper fails", prop("type", "number", "minimum", 1.0, "maximum", 10.0), prop("type", "number", "exclusiveMinimum", 0.0, "exclusiveMaximum", 10.0), SchemaRuleExclusiveMaximum},
		{"bound type invalid", prop("type", "number", "maximum", 1.0), prop("type", "number", "maximum", "10"), SchemaRuleMaximum},

		// multipleOf
		{"multipleOf integer multiple", prop("type", "number", "multipleOf", 10.0), prop("type", "number", "multipleOf", 5.0), ""},
		{"multipleOf not a multiple", prop("type", "number", "multipleOf", 5.0), prop("type", "number", "multipleOf", 10.0), SchemaRuleMultipleOf},
		{"multipleOf exact decimals", prop("type", "number", "multipleOf", n("0.3")), prop("type", "number", "multipleOf", n("0.1")), ""},
		{"multipleOf decimal text of floats", prop("type", "number", "multipleOf", 0.3), prop("type", "number", "multipleOf", 0.1), ""},
		{"multipleOf hundredths", prop("type", "number", "multipleOf", 0.05), prop("type", "number", "multipleOf", 0.01), ""},
		{"multipleOf decimal not a multiple", prop("type", "number", "multipleOf", 0.15), prop("type", "number", "multipleOf", 0.1), SchemaRuleMultipleOf},
		{"bounds compare float text exactly", prop("type", "number", "maximum", 0.1), prop("type", "number", "maximum", n("0.1")), ""},
		{"exponent floats", prop("type", "number", "maximum", 1e21), prop("type", "number", "maximum", n("1e21")), ""},
		{"multipleOf integer listing and 1", prop("type", "integer"), prop("type", "integer", "multipleOf", 1.0), ""},
		{"multipleOf integer listing and 0.5", prop("type", "integer"), prop("type", "number", "multipleOf", 0.5), ""},
		{"multipleOf integer listing and 2", prop("type", "integer"), prop("type", "integer", "multipleOf", 2.0), SchemaRuleMultipleOf},
		{"multipleOf number listing and 1", prop("type", "number"), prop("type", "number", "multipleOf", 1.0), SchemaRuleMultipleOf},
		{"multipleOf zero", prop("type", "number", "multipleOf", 1.0), prop("type", "number", "multipleOf", 0.0), SchemaRuleSchemaInvalid},
		{"multipleOf negative", prop("type", "number", "multipleOf", 1.0), prop("type", "number", "multipleOf", -2.0), SchemaRuleSchemaInvalid},
		{"multipleOf rejects non-JSON number syntax", prop("type", "number", "multipleOf", n("1/3")), prop("type", "number", "multipleOf", n("1e1")), SchemaRuleMultipleOf},
		{"multipleOf rejects huge exponent", prop("type", "number", "multipleOf", n("1e100000")), prop("type", "number", "multipleOf", 1.0), SchemaRuleMultipleOf},
	}
	checker := productionSchemaChecker
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			listing := objectSchema(map[string]interface{}{"v": tc.listing})
			capability := objectSchema(map[string]interface{}{"v": tc.capability})
			detail := checker.incompatibility(listing, capability)
			if tc.rule == "" {
				if detail != nil {
					t.Fatalf("want compatible, got %#v (value %s)", detail, detail.CapabilityValue)
				}
				return
			}
			if detail == nil || detail.Property != "v" || detail.Rule != tc.rule {
				t.Fatalf("detail = %#v, want rule %q", detail, tc.rule)
			}
		})
	}
}

// Soundness check of the inclusive/exclusive bound logic: whenever the proof
// accepts a listing, every sampled value the start enforcer accepts for that
// listing must also be inside the capability range.
func TestNumericBoundProofsAreSound(t *testing.T) {
	type bound struct {
		key   string
		value float64
	}
	options := []*bound{nil}
	for _, value := range []float64{1, 2} {
		for _, key := range []string{"minimum", "exclusiveMinimum", "maximum", "exclusiveMaximum"} {
			options = append(options, &bound{key: key, value: value})
		}
	}
	samples := []float64{0, 0.5, 1, 1.5, 2, 2.5, 3}
	satisfies := func(sample float64, bounds []*bound) bool {
		for _, b := range bounds {
			if b == nil {
				continue
			}
			switch b.key {
			case "minimum":
				if sample < b.value {
					return false
				}
			case "exclusiveMinimum":
				if sample <= b.value {
					return false
				}
			case "maximum":
				if sample > b.value {
					return false
				}
			case "exclusiveMaximum":
				if sample >= b.value {
					return false
				}
			}
		}
		return true
	}
	checker := productionSchemaChecker
	proven := 0
	for _, l1 := range options {
		for _, l2 := range options {
			for _, c := range options[1:] {
				listing := prop("type", "number")
				for _, b := range []*bound{l1, l2} {
					if b != nil {
						listing[b.key] = b.value
					}
				}
				if l1 != nil && l2 != nil && l1.key == l2.key {
					continue
				}
				capability := prop("type", "number", c.key, c.value)
				detail := checker.incompatibility(objectSchema(map[string]interface{}{"v": listing}), objectSchema(map[string]interface{}{"v": capability}))
				if detail != nil {
					continue
				}
				proven++
				listingSchema := objectSchema(map[string]interface{}{"v": listing})
				for _, sample := range samples {
					accepted := validateExternalExecutionInput(map[string]interface{}{"v": sample}, listingSchema) == nil
					if accepted != satisfies(sample, []*bound{l1, l2}) {
						t.Fatalf("start enforcer for %v accepts %v = %v", listing, sample, accepted)
					}
					if accepted && !satisfies(sample, []*bound{c}) {
						t.Fatalf("unsound: listing %v accepts %v but capability %v rejects it", listing, sample, capability)
					}
				}
			}
		}
	}
	if proven == 0 {
		t.Fatal("no bound combination was proven")
	}
}
