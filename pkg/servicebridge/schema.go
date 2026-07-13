package servicebridge

import (
	"regexp"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

var hostedFieldKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

// controlledFieldsToJSONSchema accepts the marketplace's controlled field
// array and turns it into the small JSON-Schema subset needed for capability
// compatibility checks. Cloud remains responsible for full product-form
// validation.
func controlledFieldsToJSONSchema(fields []interface{}) (map[string]interface{}, error) {
	properties := map[string]interface{}{}
	required := make([]interface{}, 0)
	for _, raw := range fields {
		field, ok := raw.(map[string]interface{})
		if !ok {
			return nil, httpx.BadRequest("input_schema 字段必须是 JSON object")
		}
		key, _ := field["key"].(string)
		key = strings.TrimSpace(key)
		if !hostedFieldKeyPattern.MatchString(key) {
			return nil, httpx.BadRequest("input_schema 字段 key 无效")
		}
		if _, exists := properties[key]; exists {
			return nil, httpx.BadRequest("input_schema 字段 key 不能重复")
		}
		fieldType, _ := field["type"].(string)
		fieldType = strings.TrimSpace(fieldType)
		var jsonType string
		switch fieldType {
		case "text", "long_text":
			jsonType = "string"
		case "url":
			jsonType = "string"
		case "select":
			jsonType = "string"
		case "number":
			jsonType = "number"
		case "boolean":
			jsonType = "boolean"
		default:
			return nil, httpx.BadRequest("input_schema 包含不支持的字段类型")
		}
		property := map[string]interface{}{"type": jsonType}
		if fieldType == "url" {
			property["format"] = "uri"
		}
		if fieldType == "select" {
			options, ok := schemaStringEnum(field["options"])
			if !ok || len(options) == 0 {
				return nil, httpx.BadRequest("select 字段必须提供有效 options")
			}
			property["enum"] = stringSetValues(options)
		}
		properties[key] = property
		if isRequired, _ := field["required"].(bool); isRequired {
			required = append(required, key)
		}
	}
	return map[string]interface{}{
		"type":       "object",
		"properties": properties,
		"required":   required,
	}, nil
}

// hostedInputSchemaCompatible proves compatibility for the controlled object
// subset used by service listings. It fails closed when an Agent declares a
// shape outside that subset instead of pretending a potentially rejected order
// is executable.
func hostedInputSchemaCompatible(listing, capability map[string]interface{}) bool {
	if !schemaAllowsType(listing["type"], "object") || !schemaAllowsType(capability["type"], "object") {
		return false
	}
	listingProps, ok := schemaProperties(listing)
	if !ok {
		return false
	}
	capabilityProps, ok := schemaProperties(capability)
	if !ok {
		return false
	}
	listingRequired := schemaRequiredSet(listing)
	for key := range schemaRequiredSet(capability) {
		if _, exists := listingProps[key]; !exists {
			return false
		}
		if _, required := listingRequired[key]; !required {
			return false
		}
	}
	additionalAllowed := true
	if raw, exists := capability["additionalProperties"]; exists {
		allowed, ok := raw.(bool)
		if !ok {
			return false
		}
		additionalAllowed = allowed
	}
	if schemaHasUnsupportedConstraints(capability, "minProperties", "maxProperties", "propertyNames", "dependentRequired", "dependentSchemas", "allOf", "anyOf", "oneOf", "not", "if", "then", "else") {
		return false
	}
	for key, listingProperty := range listingProps {
		capabilityProperty, exists := capabilityProps[key]
		if !exists {
			if !additionalAllowed {
				return false
			}
			continue
		}
		listingSchema, listingOK := listingProperty.(map[string]interface{})
		capabilitySchema, capabilityOK := capabilityProperty.(map[string]interface{})
		if !listingOK || !capabilityOK || !schemaTypeSubset(listingSchema["type"], capabilitySchema["type"]) || !schemaConstraintsCompatible(listingSchema, capabilitySchema) {
			return false
		}
	}
	return true
}

func schemaConstraintsCompatible(listing, capability map[string]interface{}) bool {
	if schemaHasUnsupportedConstraints(capability, "const", "pattern", "minLength", "maxLength", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "allOf", "anyOf", "oneOf", "not", "if", "then", "else") {
		return false
	}
	if capabilityFormat, constrained := capability["format"]; constrained {
		listingFormat, ok := listing["format"].(string)
		expectedFormat, expectedOK := capabilityFormat.(string)
		if !ok || !expectedOK || listingFormat != expectedFormat {
			return false
		}
	}
	if _, constrained := capability["enum"]; constrained {
		listingEnum, listingOK := schemaStringEnum(listing["enum"])
		capabilityEnum, capabilityOK := schemaStringEnum(capability["enum"])
		if !listingOK || !capabilityOK || len(listingEnum) == 0 || len(capabilityEnum) == 0 {
			return false
		}
		for value := range listingEnum {
			if _, allowed := capabilityEnum[value]; !allowed {
				return false
			}
		}
	}
	return true
}

func schemaHasUnsupportedConstraints(schema map[string]interface{}, keys ...string) bool {
	for _, key := range keys {
		if _, exists := schema[key]; exists {
			return true
		}
	}
	return false
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

func stringSetValues(values map[string]struct{}) []interface{} {
	result := make([]interface{}, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	return result
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
