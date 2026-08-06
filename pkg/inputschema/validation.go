package inputschema

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

const MaxDepth = 16

var allowedJSONSchemaTypes = map[string]struct{}{
	"object":  {},
	"array":   {},
	"string":  {},
	"number":  {},
	"integer": {},
	"boolean": {},
	"null":    {},
}

// Violation is the stable, non-sensitive shape returned when an application
// input does not satisfy a capability schema. Callers may expose Path and
// Reason, but must not echo the rejected value or full schema.
type Violation struct {
	Path   string
	Reason string
	detail string
}

func (e *Violation) Error() string {
	if e == nil {
		return "input schema violation"
	}
	if e.detail == "" {
		return fmt.Sprintf("%s: %s", e.Path, e.Reason)
	}
	return fmt.Sprintf("%s %s", e.Path, e.detail)
}

func violation(path, reason, detail string) error {
	return &Violation{Path: path, Reason: reason, detail: detail}
}

func ValidateCapabilitySchema(schema map[string]interface{}, label string) error {
	if schema == nil {
		return fmt.Errorf("%s 必须是 JSON object", label)
	}
	if err := validateSchemaNode(schema, label, 0); err != nil {
		return err
	}
	if !AllowsType(schema, "object") {
		return fmt.Errorf("%s.type 必须包含 object", label)
	}
	return nil
}

func ValidateInputSchema(schema map[string]interface{}) error {
	return ValidateCapabilitySchema(schema, "input_schema")
}

func validateSchemaNode(schema map[string]interface{}, path string, depth int) error {
	if depth > MaxDepth {
		return fmt.Errorf("%s 嵌套过深", path)
	}
	if rawType, ok := schema["type"]; ok {
		types, err := SchemaTypes(rawType)
		if err != nil {
			return fmt.Errorf("%s.type %w", path, err)
		}
		for _, schemaType := range types {
			if _, ok := allowedJSONSchemaTypes[schemaType]; !ok {
				return fmt.Errorf("%s.type 不支持 %q", path, schemaType)
			}
		}
	}
	if rawRequired, ok := schema["required"]; ok {
		required, err := StringArray(rawRequired)
		if err != nil {
			return fmt.Errorf("%s.required %w", path, err)
		}
		seen := map[string]struct{}{}
		for _, key := range required {
			if key == "" {
				return fmt.Errorf("%s.required 不能包含空字段名", path)
			}
			if _, ok := seen[key]; ok {
				return fmt.Errorf("%s.required 重复字段 %q", path, key)
			}
			seen[key] = struct{}{}
		}
	}
	if rawProperties, ok := schema["properties"]; ok {
		properties, ok := rawProperties.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%s.properties 必须是 object", path)
		}
		for key, rawSubschema := range properties {
			subschema, ok := rawSubschema.(map[string]interface{})
			if !ok {
				return fmt.Errorf("%s.properties.%s 必须是 object", path, key)
			}
			if err := validateSchemaNode(subschema, path+".properties."+key, depth+1); err != nil {
				return err
			}
		}
	}
	if rawItems, ok := schema["items"]; ok {
		items, ok := rawItems.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%s.items 必须是 object", path)
		}
		if err := validateSchemaNode(items, path+".items", depth+1); err != nil {
			return err
		}
	}
	if rawAdditional, ok := schema["additionalProperties"]; ok {
		switch value := rawAdditional.(type) {
		case bool:
		case map[string]interface{}:
			if err := validateSchemaNode(value, path+".additionalProperties", depth+1); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s.additionalProperties 必须是 boolean 或 object", path)
		}
	}
	if rawEnum, ok := schema["enum"]; ok {
		if _, ok := rawEnum.([]interface{}); !ok {
			return fmt.Errorf("%s.enum 必须是 array", path)
		}
	}
	return nil
}

func ValidateJSON(value interface{}, schema map[string]interface{}, label string) error {
	if schema == nil {
		return nil
	}
	return validateJSONValue(value, schema, label, 0)
}

func ValidateInput(value interface{}, schema map[string]interface{}) error {
	return ValidateJSON(value, schema, "input")
}

func validateJSONValue(value interface{}, schema map[string]interface{}, path string, depth int) error {
	if depth > MaxDepth {
		return violation(path, "schema_depth_exceeded", "嵌套过深")
	}
	if enum, ok := schema["enum"].([]interface{}); ok {
		matched := false
		for _, candidate := range enum {
			if reflect.DeepEqual(value, candidate) {
				matched = true
				break
			}
		}
		if !matched {
			return violation(path, "enum_mismatch", "不在 enum 范围内")
		}
	}

	types, _ := SchemaTypes(schema["type"])
	if len(types) == 0 && hasObjectKeywords(schema) {
		types = []string{"object"}
	}
	if len(types) > 0 {
		matched := false
		for _, schemaType := range types {
			if ValueMatchesJSONType(value, schemaType) {
				matched = true
				break
			}
		}
		if !matched {
			return violation(path, "type_mismatch", fmt.Sprintf("类型不匹配，期望 %v", types))
		}
	}

	propertiesRaw, hasProperties := schema["properties"]
	if hasProperties || schema["required"] != nil {
		objectValue, ok := value.(map[string]interface{})
		if !ok {
			return violation(path, "type_mismatch", "必须是 object")
		}
		required, err := StringArray(schema["required"])
		if err == nil {
			for _, key := range required {
				if _, ok := objectValue[key]; !ok {
					return violation(path+"."+key, "missing_required", "缺少必填字段")
				}
			}
		}
		properties, _ := propertiesRaw.(map[string]interface{})
		for key, rawSubschema := range properties {
			childValue, exists := objectValue[key]
			if !exists {
				continue
			}
			subschema, ok := rawSubschema.(map[string]interface{})
			if !ok {
				continue
			}
			if err := validateJSONValue(childValue, subschema, path+"."+key, depth+1); err != nil {
				return err
			}
		}
		if additional, ok := schema["additionalProperties"]; ok {
			if allow, ok := additional.(bool); ok && !allow {
				for key := range objectValue {
					if _, declared := properties[key]; !declared {
						return violation(path+"."+key, "additional_property", "包含未声明字段")
					}
				}
			}
			if additionalSchema, ok := additional.(map[string]interface{}); ok {
				for key, childValue := range objectValue {
					if _, declared := properties[key]; declared {
						continue
					}
					if err := validateJSONValue(childValue, additionalSchema, path+"."+key, depth+1); err != nil {
						return err
					}
				}
			}
		}
	}

	if itemsRaw, ok := schema["items"]; ok {
		arrayValue, ok := value.([]interface{})
		if !ok {
			return violation(path, "type_mismatch", "必须是 array")
		}
		items, ok := itemsRaw.(map[string]interface{})
		if !ok {
			return nil
		}
		for index, item := range arrayValue {
			if err := validateJSONValue(item, items, fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func SchemaTypes(raw interface{}) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	if value, ok := raw.(string); ok {
		return []string{value}, nil
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("必须是 string 或 string array")
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("数组项必须是 string")
		}
		out = append(out, value)
	}
	return out, nil
}

func StringArray(raw interface{}) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("必须是 string array")
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("数组项必须是 string")
		}
		out = append(out, value)
	}
	return out, nil
}

func AllowsType(schema map[string]interface{}, want string) bool {
	types, err := SchemaTypes(schema["type"])
	if err != nil {
		return false
	}
	for _, schemaType := range types {
		if schemaType == want {
			return true
		}
	}
	return false
}

func hasObjectKeywords(schema map[string]interface{}) bool {
	return schema["properties"] != nil || schema["required"] != nil || schema["additionalProperties"] != nil
}

func ValueMatchesJSONType(value interface{}, want string) bool {
	switch want {
	case "object":
		_, ok := value.(map[string]interface{})
		return ok
	case "array":
		_, ok := value.([]interface{})
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		return IsJSONNumber(value)
	case "integer":
		number, ok := numericValue(value)
		return ok && math.Trunc(number) == number
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	default:
		return true
	}
}

func IsJSONNumber(value interface{}) bool {
	_, ok := numericValue(value)
	return ok
}

func numericValue(value interface{}) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case float32:
		return float64(number), true
	case float64:
		return number, true
	case json.Number:
		parsed, err := number.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}
