package agent

import (
	"fmt"
	"math"
	"reflect"
)

const maxSchemaDepth = 16

var allowedJSONSchemaTypes = map[string]struct{}{
	"object":  {},
	"array":   {},
	"string":  {},
	"number":  {},
	"integer": {},
	"boolean": {},
	"null":    {},
}

func validateCapabilitySchema(schema map[string]interface{}, label string) error {
	if schema == nil {
		return fmt.Errorf("%s 必须是 JSON object", label)
	}
	if err := validateSchemaNode(schema, label, 0); err != nil {
		return err
	}
	if !schemaAllowsType(schema, "object") {
		return fmt.Errorf("%s.type 必须包含 object", label)
	}
	return nil
}

func validateSchemaNode(schema map[string]interface{}, path string, depth int) error {
	if depth > maxSchemaDepth {
		return fmt.Errorf("%s 嵌套过深", path)
	}
	if rawType, ok := schema["type"]; ok {
		types, err := schemaTypes(rawType)
		if err != nil {
			return fmt.Errorf("%s.type %w", path, err)
		}
		for _, t := range types {
			if _, ok := allowedJSONSchemaTypes[t]; !ok {
				return fmt.Errorf("%s.type 不支持 %q", path, t)
			}
		}
	}
	if rawRequired, ok := schema["required"]; ok {
		required, err := stringArray(rawRequired)
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
		switch v := rawAdditional.(type) {
		case bool:
		case map[string]interface{}:
			if err := validateSchemaNode(v, path+".additionalProperties", depth+1); err != nil {
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

func validateJSONAgainstSchema(value interface{}, schema map[string]interface{}, label string) error {
	if schema == nil {
		return nil
	}
	return validateJSONValue(value, schema, label, 0)
}

// ValidateInputAgainstSchema lets Core orchestration layers validate the
// concrete input they are about to send to an Agent. Capability schema
// ownership stays in the agent package; callers do not duplicate a partial
// validator or reinterpret the Agent contract.
func ValidateInputAgainstSchema(value interface{}, schema map[string]interface{}) error {
	return validateJSONAgainstSchema(value, schema, "input")
}

func validateJSONValue(value interface{}, schema map[string]interface{}, path string, depth int) error {
	if depth > maxSchemaDepth {
		return fmt.Errorf("%s 嵌套过深", path)
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
			return fmt.Errorf("%s 不在 enum 范围内", path)
		}
	}

	types, _ := schemaTypes(schema["type"])
	if len(types) == 0 && hasObjectKeywords(schema) {
		types = []string{"object"}
	}
	if len(types) > 0 {
		matched := false
		for _, t := range types {
			if valueMatchesJSONType(value, t) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s 类型不匹配，期望 %v", path, types)
		}
	}

	if propertiesRaw, ok := schema["properties"]; ok || schema["required"] != nil {
		objectValue, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%s 必须是 object", path)
		}
		required, err := stringArray(schema["required"])
		if err == nil {
			for _, key := range required {
				if _, ok := objectValue[key]; !ok {
					return fmt.Errorf("%s 缺少必填字段 %q", path, key)
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
						return fmt.Errorf("%s 包含未声明字段 %q", path, key)
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
			return fmt.Errorf("%s 必须是 array", path)
		}
		items, ok := itemsRaw.(map[string]interface{})
		if !ok {
			return nil
		}
		for i, item := range arrayValue {
			if err := validateJSONValue(item, items, fmt.Sprintf("%s[%d]", path, i), depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func schemaTypes(raw interface{}) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	if s, ok := raw.(string); ok {
		return []string{s}, nil
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("必须是 string 或 string array")
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("数组项必须是 string")
		}
		out = append(out, s)
	}
	return out, nil
}

func stringArray(raw interface{}) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("必须是 string array")
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("数组项必须是 string")
		}
		out = append(out, s)
	}
	return out, nil
}

func schemaAllowsType(schema map[string]interface{}, want string) bool {
	types, err := schemaTypes(schema["type"])
	if err != nil {
		return false
	}
	for _, t := range types {
		if t == want {
			return true
		}
	}
	return false
}

func hasObjectKeywords(schema map[string]interface{}) bool {
	return schema["properties"] != nil || schema["required"] != nil || schema["additionalProperties"] != nil
}

func valueMatchesJSONType(value interface{}, want string) bool {
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
		return isJSONNumber(value)
	case "integer":
		n, ok := numericValue(value)
		return ok && math.Trunc(n) == n
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	default:
		return true
	}
}

func isJSONNumber(value interface{}) bool {
	_, ok := numericValue(value)
	return ok
}

func numericValue(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}
