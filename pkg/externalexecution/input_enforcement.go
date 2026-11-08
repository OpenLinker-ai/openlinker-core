package externalexecution

import (
	"errors"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/inputschema"
)

// executionInputInvalidCode rejects an External Execution start whose input
// does not satisfy a schema: the caller-declared input_schema (checked by Core
// before anything is reserved) or the target capability (reported by Runtime
// at launch). The response carries a bounded rule, never the input.
const executionInputInvalidCode = httpx.ErrorCode("EXECUTION_INPUT_INVALID")

const (
	executionInputSourceInputSchema = "input_schema"
	executionInputSourceCapability  = "capability"
)

// externalInputEnforcedKeywords are the scalar constraint keywords
// validateExternalExecutionInput enforces on top of Core's run-time
// validator. The schema check enables exactly these subset proofs.
var externalInputEnforcedKeywords = map[string]bool{
	"const": true, "minLength": true, "maxLength": true, "pattern": true,
	"minimum": true, "exclusiveMinimum": true, "maximum": true,
	"exclusiveMaximum": true, "multipleOf": true,
}

// validateExternalExecutionInput enforces the caller-declared input_schema on
// the External Execution input. This is an External Execution-only path; the
// ordinary Run, A2A and Workflow validator (pkg/inputschema) is unchanged and
// only reused here for its base semantics.
//
//   - type, enum, required, properties, additionalProperties and items use
//     inputschema.ValidateInput, the same semantics as Core's run-time check.
//   - const, minLength/maxLength (Unicode code points), pattern (Go RE2,
//     unanchored, a pattern that does not compile rejects every string),
//     minimum/maximum, exclusiveMinimum/exclusiveMaximum (numeric form, or the
//     draft-04 boolean form on minimum/maximum) and multipleOf (exact rational
//     arithmetic) are enforced here, wherever they appear in the schema.
//   - A keyword whose value is malformed rejects every value it applies to.
//
// The error names only a property the schema itself declares and a rule; it
// never echoes input values or undeclared input keys.
func validateExternalExecutionInput(input, schema map[string]interface{}) error {
	if schema == nil {
		return nil
	}
	if input == nil {
		input = map[string]interface{}{}
	}
	if err := inputschema.ValidateInput(input, schema); err != nil {
		var violation *inputschema.Violation
		if !errors.As(err, &violation) {
			return executionInputInvalidError(executionInputSourceInputSchema, "", SchemaRuleSchemaInvalid)
		}
		return executionInputInvalidError(
			executionInputSourceInputSchema,
			declaredInputProperty(schema, violation.Path),
			inputschemaViolationRule(violation.Reason),
		)
	}
	enforcer := scalarInputEnforcer{patterns: map[string]*regexp.Regexp{}}
	if rule := enforcer.scalarViolation(input, schema); rule != "" {
		return executionInputInvalidError(executionInputSourceInputSchema, "", rule)
	}
	properties, _ := schema["properties"].(map[string]interface{})
	for _, key := range sortedSchemaKeys(input) {
		value := input[key]
		if subschema, declared := properties[key].(map[string]interface{}); declared {
			if rule := enforcer.violation(value, subschema, 1); rule != "" {
				return executionInputInvalidError(executionInputSourceInputSchema, key, rule)
			}
			continue
		}
		if _, declared := properties[key]; declared {
			continue
		}
		if additional, ok := schema["additionalProperties"].(map[string]interface{}); ok {
			if rule := enforcer.violation(value, additional, 1); rule != "" {
				return executionInputInvalidError(executionInputSourceInputSchema, "", rule)
			}
		}
	}
	return nil
}

func executionInputInvalidError(source, property, rule string) *httpx.HTTPError {
	details := map[string]interface{}{"source": source, "rule": rule}
	if property != "" {
		details["property"] = property
	}
	return &httpx.HTTPError{
		Status:  http.StatusUnprocessableEntity,
		Code:    executionInputInvalidCode,
		Message: "input 不匹配 input_schema",
		Details: details,
	}
}

// capabilityInputInvalidError maps Runtime's RUN_INPUT_SCHEMA_MISMATCH at
// launch. Runtime's path may name an undeclared input key, so only the bounded
// reason is kept.
func capabilityInputInvalidError(err error) (*httpx.HTTPError, bool) {
	var httpErr *httpx.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != httpx.ErrorCode("RUN_INPUT_SCHEMA_MISMATCH") ||
		httpErr.Status != http.StatusUnprocessableEntity {
		return nil, false
	}
	reason := ""
	if details, ok := httpErr.Details.(map[string]interface{}); ok {
		reason, _ = details["reason"].(string)
	}
	return executionInputInvalidError(executionInputSourceCapability, "", inputschemaViolationRule(reason)), true
}

func inputschemaViolationRule(reason string) string {
	switch reason {
	case "type_mismatch":
		return SchemaRuleType
	case "enum_mismatch":
		return SchemaRuleEnum
	case "missing_required":
		return SchemaRuleRequired
	case "additional_property":
		return SchemaRuleAdditionalProperties
	default:
		return SchemaRuleSchemaInvalid
	}
}

// declaredInputProperty returns the longest top-level property the schema
// declares that the violation path ("input.<key>...") falls under.
func declaredInputProperty(schema map[string]interface{}, path string) string {
	properties, _ := schema["properties"].(map[string]interface{})
	rest, ok := strings.CutPrefix(path, "input.")
	if !ok {
		return ""
	}
	best := ""
	for key := range properties {
		if (rest == key || strings.HasPrefix(rest, key+".") || strings.HasPrefix(rest, key+"[")) && len(key) > len(best) {
			best = key
		}
	}
	return best
}

type scalarInputEnforcer struct {
	patterns map[string]*regexp.Regexp
}

// violation applies the scalar keywords to value and recurses the way the
// base validator does. Base keywords were already checked.
func (e scalarInputEnforcer) violation(value interface{}, schema map[string]interface{}, depth int) string {
	if depth > inputschema.MaxDepth {
		return SchemaRuleSchemaInvalid
	}
	if rule := e.scalarViolation(value, schema); rule != "" {
		return rule
	}
	switch typed := value.(type) {
	case map[string]interface{}:
		properties, _ := schema["properties"].(map[string]interface{})
		additional, _ := schema["additionalProperties"].(map[string]interface{})
		for _, key := range sortedSchemaKeys(typed) {
			subschema, declared := properties[key]
			if !declared {
				if additional != nil {
					if rule := e.violation(typed[key], additional, depth+1); rule != "" {
						return rule
					}
				}
				continue
			}
			if child, ok := subschema.(map[string]interface{}); ok {
				if rule := e.violation(typed[key], child, depth+1); rule != "" {
					return rule
				}
			}
		}
	case []interface{}:
		if items, ok := schema["items"].(map[string]interface{}); ok {
			for _, item := range typed {
				if rule := e.violation(item, items, depth+1); rule != "" {
					return rule
				}
			}
		}
	}
	return ""
}

func (e scalarInputEnforcer) scalarViolation(value interface{}, schema map[string]interface{}) string {
	if constValue, constrained := schema["const"]; constrained && !schemaJSONEqual(value, constValue) {
		return SchemaRuleConst
	}
	if text, ok := value.(string); ok {
		return e.stringViolation(text, schema)
	}
	if _, isBool := value.(bool); isBool {
		return ""
	}
	if number, ok := schemaNumber(value); ok {
		return numberViolation(number, schema)
	}
	return ""
}

func (e scalarInputEnforcer) stringViolation(text string, schema map[string]interface{}) string {
	length := big.NewRat(int64(utf8.RuneCountInString(text)), 1)
	if raw, constrained := schema["minLength"]; constrained {
		bound, ok := schemaNonNegativeInteger(raw)
		if !ok || length.Cmp(bound) < 0 {
			return SchemaRuleMinLength
		}
	}
	if raw, constrained := schema["maxLength"]; constrained {
		bound, ok := schemaNonNegativeInteger(raw)
		if !ok || length.Cmp(bound) > 0 {
			return SchemaRuleMaxLength
		}
	}
	if raw, constrained := schema["pattern"]; constrained {
		pattern, ok := raw.(string)
		if !ok {
			return SchemaRulePattern
		}
		compiled, cached := e.patterns[pattern]
		if !cached {
			var err error
			if compiled, err = regexp.Compile(pattern); err != nil {
				compiled = nil
			}
			e.patterns[pattern] = compiled
		}
		if compiled == nil || !compiled.MatchString(text) {
			return SchemaRulePattern
		}
	}
	return ""
}

func numberViolation(number *big.Rat, schema map[string]interface{}) string {
	lowerExclusive, upperExclusive := false, false
	if raw, constrained := schema["exclusiveMinimum"]; constrained {
		switch value := raw.(type) {
		case bool:
			// Draft-04 form: makes "minimum" exclusive.
			lowerExclusive = value
		default:
			bound, ok := schemaNumber(value)
			if !ok || number.Cmp(bound) <= 0 {
				return SchemaRuleExclusiveMinimum
			}
		}
	}
	if raw, constrained := schema["exclusiveMaximum"]; constrained {
		switch value := raw.(type) {
		case bool:
			upperExclusive = value
		default:
			bound, ok := schemaNumber(value)
			if !ok || number.Cmp(bound) >= 0 {
				return SchemaRuleExclusiveMaximum
			}
		}
	}
	if raw, constrained := schema["minimum"]; constrained {
		bound, ok := schemaNumber(raw)
		if !ok {
			return SchemaRuleMinimum
		}
		if cmp := number.Cmp(bound); cmp < 0 || (lowerExclusive && cmp == 0) {
			if lowerExclusive {
				return SchemaRuleExclusiveMinimum
			}
			return SchemaRuleMinimum
		}
	}
	if raw, constrained := schema["maximum"]; constrained {
		bound, ok := schemaNumber(raw)
		if !ok {
			return SchemaRuleMaximum
		}
		if cmp := number.Cmp(bound); cmp > 0 || (upperExclusive && cmp == 0) {
			if upperExclusive {
				return SchemaRuleExclusiveMaximum
			}
			return SchemaRuleMaximum
		}
	}
	if raw, constrained := schema["multipleOf"]; constrained {
		divisor, ok := schemaNumber(raw)
		if !ok || divisor.Sign() <= 0 || !new(big.Rat).Quo(number, divisor).IsInt() {
			return SchemaRuleMultipleOf
		}
	}
	return ""
}
