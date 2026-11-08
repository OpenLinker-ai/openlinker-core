package externalexecution

import (
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// The External Execution schema check proves that every input an external
// (listing) schema accepts is also accepted by the target capability's input
// schema. It is a bounded subset proof: anything it cannot prove fails closed,
// and the first unproven capability rule is reported to the target owner.
//
// The listing is read as a closed form. Only its declared properties can be
// present; Hosted intake rejects undeclared keys, and Core's run-time
// validator enforces capability additionalProperties:false. Extra listing
// keywords only narrow the listing and are therefore ignored.
//
// Properties and required keys are walked in sorted order so the reported
// incompatibility is deterministic.

// maxCapabilityValueBytes bounds TargetUnavailableDetail.CapabilityValue. A
// larger capability value (for example a long enum) is omitted.
const maxCapabilityValueBytes = 2048

// unsupportedObjectKeywords are capability keywords at the top level of the
// input schema that the checker cannot prove. They are checked in this order.
var unsupportedObjectKeywords = []string{
	"$dynamicRef", "$ref", "allOf", "anyOf", "const", "dependencies",
	"dependentRequired", "dependentSchemas", "else", "enum", "if", "items",
	"maxProperties", "minProperties", "not", "oneOf", "patternProperties",
	"prefixItems", "propertyNames", "then", "unevaluatedProperties",
}

// unsupportedPropertyKeywords are capability property keywords the checker
// cannot prove. Nested object and array constraints are included: Core's
// run-time validator enforces properties, required, additionalProperties and
// items, so a listing that cannot prove them would fail at run time.
var unsupportedPropertyKeywords = []string{
	"$dynamicRef", "$ref", "additionalItems", "additionalProperties", "allOf",
	"anyOf", "contains", "dependencies", "dependentRequired", "dependentSchemas",
	"else", "if", "items", "maxContains", "maxItems", "maxProperties",
	"minContains", "minItems", "minProperties", "not", "oneOf",
	"patternProperties", "prefixItems", "properties", "propertyNames",
	"required", "then", "unevaluatedItems", "unevaluatedProperties",
	"uniqueItems",
}

// scalarConstraint is a capability property keyword with a subset proof.
type scalarConstraint struct {
	keyword string
	rule    string
	// prove returns "" when the listing property provably satisfies the
	// capability constraint, otherwise the rule to report.
	prove func(listing map[string]interface{}, capabilityValue interface{}) string
}

// scalarConstraints are checked in this order.
//
// A proof only holds end to end when Core also enforces the capability
// keyword at run time, because the listing schema itself is enforced outside
// Core. Core's run-time input validator (pkg/inputschema, the source of
// RUN_INPUT_SCHEMA_MISMATCH) enforces none of these keywords today, so the
// production checker keeps failing closed for all of them and reports the
// keyword's rule. A proof may be enabled only together with run-time
// enforcement of the same keyword with the same semantics: Unicode code-point
// (rune) lengths, Go RE2 patterns that compile, and exact decimal numbers.
// TestProductionSchemaCheckerMatchesRuntimeValidator pins the two together.
var scalarConstraints = []scalarConstraint{
	{keyword: "const", rule: SchemaRuleConst, prove: proveConst},
	{keyword: "minLength", rule: SchemaRuleMinLength, prove: proveMinLength},
	{keyword: "maxLength", rule: SchemaRuleMaxLength, prove: proveMaxLength},
	{keyword: "pattern", rule: SchemaRulePattern, prove: provePattern},
	{keyword: "minimum", rule: SchemaRuleMinimum, prove: proveMinimum},
	{keyword: "exclusiveMinimum", rule: SchemaRuleExclusiveMinimum, prove: proveExclusiveMinimum},
	{keyword: "maximum", rule: SchemaRuleMaximum, prove: proveMaximum},
	{keyword: "exclusiveMaximum", rule: SchemaRuleExclusiveMaximum, prove: proveExclusiveMaximum},
	{keyword: "multipleOf", rule: SchemaRuleMultipleOf, prove: proveMultipleOf},
}

// schemaChecker holds the set of scalar constraint keywords whose subset
// proofs are enabled. A keyword that is not enabled always fails closed.
type schemaChecker struct {
	runtimeEnforced map[string]bool
}

// productionSchemaChecker enables no scalar constraint proof: Core's run-time
// validator does not enforce any of them (see scalarConstraints).
var productionSchemaChecker = schemaChecker{runtimeEnforced: map[string]bool{}}

// externalInputSchemaIncompatibility returns the first reason the external
// schema is not provably accepted by the capability schema, or nil when it is.
func externalInputSchemaIncompatibility(external, capability map[string]interface{}) *TargetUnavailableDetail {
	return productionSchemaChecker.incompatibility(external, capability)
}

func (c schemaChecker) incompatibility(external, capability map[string]interface{}) *TargetUnavailableDetail {
	if capability == nil {
		return &TargetUnavailableDetail{Rule: SchemaRuleCapabilityMissing}
	}
	if !schemaAllowsType(capability["type"], "object") || !schemaAllowsType(external["type"], "object") {
		return schemaIncompatibility("", SchemaRuleType, capability["type"])
	}
	for _, keyword := range unsupportedObjectKeywords {
		if _, exists := capability[keyword]; exists {
			return schemaIncompatibility("", SchemaRuleUnsupportedKeyword, keyword)
		}
	}
	additionalAllowed := true
	if raw, exists := capability["additionalProperties"]; exists {
		allowed, ok := raw.(bool)
		if !ok {
			return schemaIncompatibility("", SchemaRuleUnsupportedKeyword, "additionalProperties")
		}
		additionalAllowed = allowed
	}
	externalProps, ok := schemaProperties(external)
	if !ok {
		return schemaIncompatibility("", SchemaRuleSchemaInvalid, nil)
	}
	capabilityProps, ok := schemaProperties(capability)
	if !ok {
		return schemaIncompatibility("", SchemaRuleSchemaInvalid, nil)
	}
	externalRequired := schemaRequiredSet(external)
	for _, key := range sortedSchemaKeys(schemaRequiredSet(capability)) {
		if _, exists := externalProps[key]; !exists {
			return schemaIncompatibility(key, SchemaRuleRequired, nil)
		}
		if _, required := externalRequired[key]; !required {
			return schemaIncompatibility(key, SchemaRuleRequired, nil)
		}
	}
	for _, key := range sortedSchemaKeys(externalProps) {
		capabilityProperty, exists := capabilityProps[key]
		if !exists {
			if !additionalAllowed {
				return schemaIncompatibility(key, SchemaRuleAdditionalProperties, false)
			}
			continue
		}
		externalSchema, externalOK := externalProps[key].(map[string]interface{})
		capabilitySchema, capabilityOK := capabilityProperty.(map[string]interface{})
		if !externalOK || !capabilityOK {
			return schemaIncompatibility(key, SchemaRuleSchemaInvalid, nil)
		}
		if detail := c.propertyIncompatibility(key, externalSchema, capabilitySchema); detail != nil {
			return detail
		}
	}
	return nil
}

func (c schemaChecker) propertyIncompatibility(property string, external, capability map[string]interface{}) *TargetUnavailableDetail {
	if !schemaTypeSubset(external["type"], capability["type"]) {
		return schemaIncompatibility(property, SchemaRuleType, capability["type"])
	}
	for _, keyword := range unsupportedPropertyKeywords {
		if _, exists := capability[keyword]; exists {
			return schemaIncompatibility(property, SchemaRuleUnsupportedKeyword, keyword)
		}
	}
	if capabilityFormat, constrained := capability["format"]; constrained {
		externalFormat, ok := external["format"].(string)
		expectedFormat, expectedOK := capabilityFormat.(string)
		if !ok || !expectedOK || externalFormat != expectedFormat {
			return schemaIncompatibility(property, SchemaRuleFormat, capabilityFormat)
		}
	}
	if capabilityEnumRaw, constrained := capability["enum"]; constrained {
		externalEnum, externalOK := schemaStringEnum(external["enum"])
		capabilityEnum, capabilityOK := schemaStringEnum(capabilityEnumRaw)
		if !externalOK || !capabilityOK || len(externalEnum) == 0 || len(capabilityEnum) == 0 {
			return schemaIncompatibility(property, SchemaRuleEnum, capabilityEnumRaw)
		}
		for value := range externalEnum {
			if _, allowed := capabilityEnum[value]; !allowed {
				return schemaIncompatibility(property, SchemaRuleEnum, capabilityEnumRaw)
			}
		}
	}
	for _, constraint := range scalarConstraints {
		capabilityValue, constrained := capability[constraint.keyword]
		if !constrained {
			continue
		}
		if !c.runtimeEnforced[constraint.keyword] {
			return schemaIncompatibility(property, constraint.rule, capabilityValue)
		}
		if rule := constraint.prove(external, capabilityValue); rule != "" {
			return schemaIncompatibility(property, rule, capabilityValue)
		}
	}
	return nil
}

// schemaIncompatibility builds a detail. capabilityValue must come from the
// capability schema (or name a capability keyword); never pass caller input.
func schemaIncompatibility(property, rule string, capabilityValue interface{}) *TargetUnavailableDetail {
	detail := &TargetUnavailableDetail{Property: property, Rule: rule}
	if capabilityValue == nil {
		return detail
	}
	encoded, err := json.Marshal(capabilityValue)
	if err == nil && len(encoded) <= maxCapabilityValueBytes {
		detail.CapabilityValue = encoded
	}
	return detail
}

func proveConst(listing map[string]interface{}, capabilityConst interface{}) string {
	if listingConst, ok := listing["const"]; ok && schemaJSONEqual(listingConst, capabilityConst) {
		return ""
	}
	if values, ok := schemaEnumValues(listing["enum"]); ok && len(values) == 1 && schemaJSONEqual(values[0], capabilityConst) {
		return ""
	}
	return SchemaRuleConst
}

func proveMinLength(listing map[string]interface{}, capabilityValue interface{}) string {
	bound, ok := schemaNonNegativeInteger(capabilityValue)
	if !ok {
		return SchemaRuleSchemaInvalid
	}
	if listingBound, ok := schemaNonNegativeInteger(listing["minLength"]); ok && listingBound.Cmp(bound) >= 0 {
		return ""
	}
	if values, ok := schemaNonEmptyStringEnum(listing["enum"]); ok {
		for _, value := range values {
			if big.NewRat(int64(utf8.RuneCountInString(value)), 1).Cmp(bound) < 0 {
				return SchemaRuleMinLength
			}
		}
		return ""
	}
	return SchemaRuleMinLength
}

func proveMaxLength(listing map[string]interface{}, capabilityValue interface{}) string {
	bound, ok := schemaNonNegativeInteger(capabilityValue)
	if !ok {
		return SchemaRuleSchemaInvalid
	}
	if listingBound, ok := schemaNonNegativeInteger(listing["maxLength"]); ok && listingBound.Cmp(bound) <= 0 {
		return ""
	}
	if values, ok := schemaNonEmptyStringEnum(listing["enum"]); ok {
		for _, value := range values {
			if big.NewRat(int64(utf8.RuneCountInString(value)), 1).Cmp(bound) > 0 {
				return SchemaRuleMaxLength
			}
		}
		return ""
	}
	return SchemaRuleMaxLength
}

func provePattern(listing map[string]interface{}, capabilityValue interface{}) string {
	pattern, ok := capabilityValue.(string)
	if !ok {
		return SchemaRuleSchemaInvalid
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return SchemaRulePattern
	}
	if listingPattern, ok := listing["pattern"].(string); ok && listingPattern == pattern {
		return ""
	}
	if values, ok := schemaNonEmptyStringEnum(listing["enum"]); ok {
		for _, value := range values {
			if !compiled.MatchString(value) {
				return SchemaRulePattern
			}
		}
		return ""
	}
	return SchemaRulePattern
}

// numericBound is one side of a listing's numeric range.
type numericBound struct {
	value     *big.Rat
	exclusive bool
}

// listingNumericBound returns the tightest lower (or upper) bound a listing
// declares. Only the numeric exclusiveMinimum/exclusiveMaximum form counts; a
// draft-04 boolean form is ignored, which reads minimum/maximum as inclusive
// and only widens the listing range, so the proof stays sound.
func listingNumericBound(listing map[string]interface{}, inclusiveKey, exclusiveKey string, lower bool) (numericBound, bool) {
	var bound numericBound
	found := false
	if value, ok := schemaNumber(listing[inclusiveKey]); ok {
		bound, found = numericBound{value: value}, true
	}
	if value, ok := schemaNumber(listing[exclusiveKey]); ok {
		tighter := !found
		if found {
			cmp := value.Cmp(bound.value)
			if !lower {
				cmp = -cmp
			}
			// An exclusive bound at the same value is tighter than an
			// inclusive one.
			tighter = cmp >= 0
		}
		if tighter {
			bound, found = numericBound{value: value, exclusive: true}, true
		}
	}
	return bound, found
}

func proveMinimum(listing map[string]interface{}, capabilityValue interface{}) string {
	return proveNumericBound(listing, capabilityValue, true, false, SchemaRuleMinimum)
}

func proveExclusiveMinimum(listing map[string]interface{}, capabilityValue interface{}) string {
	return proveNumericBound(listing, capabilityValue, true, true, SchemaRuleExclusiveMinimum)
}

func proveMaximum(listing map[string]interface{}, capabilityValue interface{}) string {
	return proveNumericBound(listing, capabilityValue, false, false, SchemaRuleMaximum)
}

func proveExclusiveMaximum(listing map[string]interface{}, capabilityValue interface{}) string {
	return proveNumericBound(listing, capabilityValue, false, true, SchemaRuleExclusiveMaximum)
}

// proveNumericBound proves a capability lower (or upper) bound from the
// listing's own bound. A capability exclusive bound c needs the listing value
// beyond c, or exactly c when the listing bound is exclusive too; an inclusive
// bound c needs the listing value at or beyond c.
func proveNumericBound(listing map[string]interface{}, capabilityValue interface{}, lower, capabilityExclusive bool, rule string) string {
	limit, ok := schemaNumber(capabilityValue)
	if !ok {
		// Includes the draft-04 boolean exclusiveMinimum/exclusiveMaximum form.
		return rule
	}
	inclusiveKey, exclusiveKey := "minimum", "exclusiveMinimum"
	if !lower {
		inclusiveKey, exclusiveKey = "maximum", "exclusiveMaximum"
	}
	bound, ok := listingNumericBound(listing, inclusiveKey, exclusiveKey, lower)
	if !ok {
		return rule
	}
	cmp := bound.value.Cmp(limit)
	if !lower {
		cmp = -cmp
	}
	switch {
	case cmp > 0:
		return ""
	case cmp == 0 && (!capabilityExclusive || bound.exclusive):
		return ""
	default:
		return rule
	}
}

func proveMultipleOf(listing map[string]interface{}, capabilityValue interface{}) string {
	divisor, ok := schemaNumber(capabilityValue)
	if !ok || divisor.Sign() <= 0 {
		return SchemaRuleSchemaInvalid
	}
	if listingMultiple, ok := schemaNumber(listing["multipleOf"]); ok && listingMultiple.Sign() > 0 &&
		new(big.Rat).Quo(listingMultiple, divisor).IsInt() {
		return ""
	}
	// Every integer is a multiple of 1 (and of 1/n).
	listingTypes := schemaTypes(listing["type"])
	if _, integer := listingTypes["integer"]; integer && len(listingTypes) == 1 && new(big.Rat).Inv(divisor).IsInt() {
		return ""
	}
	return SchemaRuleMultipleOf
}

var schemaJSONNumberPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]{0,99})(?:\.[0-9]{1,100})?(?:[eE][+-]?[0-9]{1,3})?$`)

// schemaNumber reads a JSON number as an exact rational. Both decode paths are
// accepted: encoding/json float64 and json.Number.
func schemaNumber(raw interface{}) (*big.Rat, bool) {
	switch value := raw.(type) {
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, false
		}
		return new(big.Rat).SetFloat64(value), true
	case json.Number:
		// Accept only JSON number syntax with a bounded exponent, so a
		// json.Number can neither use big.Rat's extra syntax (fractions, hex)
		// nor force a huge exact expansion.
		if !schemaJSONNumberPattern.MatchString(value.String()) {
			return nil, false
		}
		parsed, ok := new(big.Rat).SetString(value.String())
		return parsed, ok
	case int:
		return big.NewRat(int64(value), 1), true
	case int64:
		return big.NewRat(value, 1), true
	default:
		return nil, false
	}
}

func schemaNonNegativeInteger(raw interface{}) (*big.Rat, bool) {
	value, ok := schemaNumber(raw)
	if !ok || !value.IsInt() || value.Sign() < 0 {
		return nil, false
	}
	return value, true
}

// schemaJSONEqual compares decoded JSON values; numbers compare by exact value
// whichever decode path produced them.
func schemaJSONEqual(left, right interface{}) bool {
	if leftNumber, ok := schemaNumber(left); ok {
		rightNumber, ok := schemaNumber(right)
		return ok && leftNumber.Cmp(rightNumber) == 0
	}
	switch leftValue := left.(type) {
	case map[string]interface{}:
		rightValue, ok := right.(map[string]interface{})
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, item := range leftValue {
			other, exists := rightValue[key]
			if !exists || !schemaJSONEqual(item, other) {
				return false
			}
		}
		return true
	case []interface{}:
		rightValue, ok := right.([]interface{})
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for i := range leftValue {
			if !schemaJSONEqual(leftValue[i], rightValue[i]) {
				return false
			}
		}
		return true
	case string, bool, nil:
		return reflect.DeepEqual(left, right)
	default:
		return false
	}
}

func schemaEnumValues(raw interface{}) ([]interface{}, bool) {
	switch items := raw.(type) {
	case []interface{}:
		return items, true
	case []string:
		values := make([]interface{}, len(items))
		for i, item := range items {
			values[i] = item
		}
		return values, true
	default:
		return nil, false
	}
}

func schemaNonEmptyStringEnum(raw interface{}) ([]string, bool) {
	items, ok := schemaEnumValues(raw)
	if !ok || len(items) == 0 {
		return nil, false
	}
	values := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

func schemaStringEnum(raw interface{}) (map[string]struct{}, bool) {
	values := map[string]struct{}{}
	switch items := raw.(type) {
	case []interface{}:
		for _, item := range items {
			value, ok := item.(string)
			if !ok || strings.TrimSpace(value) == "" {
				return nil, false
			}
			values[value] = struct{}{}
		}
	case []string:
		for _, value := range items {
			if strings.TrimSpace(value) == "" {
				return nil, false
			}
			values[value] = struct{}{}
		}
	default:
		return nil, false
	}
	return values, true
}

func sortedSchemaKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func schemaProperties(schema map[string]interface{}) (map[string]interface{}, bool) {
	raw, exists := schema["properties"]
	if !exists {
		return map[string]interface{}{}, true
	}
	properties, ok := raw.(map[string]interface{})
	return properties, ok
}

func schemaRequiredSet(schema map[string]interface{}) map[string]struct{} {
	set := map[string]struct{}{}
	switch raw := schema["required"].(type) {
	case []interface{}:
		for _, item := range raw {
			if key, ok := item.(string); ok {
				set[key] = struct{}{}
			}
		}
	case []string:
		for _, key := range raw {
			set[key] = struct{}{}
		}
	}
	return set
}

func schemaTypeSubset(listingRaw, capabilityRaw interface{}) bool {
	listingTypes := schemaTypes(listingRaw)
	capabilityTypes := schemaTypes(capabilityRaw)
	if len(listingTypes) == 0 || len(capabilityTypes) == 0 {
		return false
	}
	for listingType := range listingTypes {
		if _, allowed := capabilityTypes[listingType]; allowed {
			continue
		}
		if listingType == "integer" {
			if _, allowed := capabilityTypes["number"]; allowed {
				continue
			}
		}
		return false
	}
	return true
}

func schemaAllowsType(raw interface{}, expected string) bool {
	_, ok := schemaTypes(raw)[expected]
	return ok
}

func schemaTypes(raw interface{}) map[string]struct{} {
	result := map[string]struct{}{}
	switch value := raw.(type) {
	case string:
		result[value] = struct{}{}
	case []interface{}:
		for _, item := range value {
			if itemType, ok := item.(string); ok {
				result[itemType] = struct{}{}
			}
		}
	case []string:
		for _, itemType := range value {
			result[itemType] = struct{}{}
		}
	}
	return result
}
