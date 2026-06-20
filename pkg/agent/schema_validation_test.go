package agent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateCapabilitySchema(t *testing.T) {
	valid := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query": map[string]interface{}{"type": "string"},
		},
		"required": []interface{}{"query"},
	}
	require.NoError(t, validateCapabilitySchema(valid, "input_schema"))

	invalidType := map[string]interface{}{
		"type": "string",
	}
	assert.Error(t, validateCapabilitySchema(invalidType, "input_schema"))

	badRequired := map[string]interface{}{
		"type":     "object",
		"required": []interface{}{"query", 1},
	}
	assert.Error(t, validateCapabilitySchema(badRequired, "input_schema"))
}

func TestValidateCapabilitySchemaBranches(t *testing.T) {
	validNested := map[string]interface{}{
		"type": []interface{}{"object", "null"},
		"properties": map[string]interface{}{
			"query": map[string]interface{}{"type": "string"},
			"items": map[string]interface{}{
				"type":  "array",
				"items": map[string]interface{}{"type": []interface{}{"string", "null"}},
			},
		},
		"required": []interface{}{"query"},
		"additionalProperties": map[string]interface{}{
			"type": "number",
		},
		"enum": []interface{}{map[string]interface{}{"query": "example"}},
	}
	require.NoError(t, validateCapabilitySchema(validNested, "input_schema"))

	deep := map[string]interface{}{"type": "object"}
	cursor := deep
	for i := 0; i < maxSchemaDepth+2; i++ {
		child := map[string]interface{}{"type": "object"}
		cursor["properties"] = map[string]interface{}{"next": child}
		cursor = child
	}

	for _, tc := range []struct {
		name   string
		schema map[string]interface{}
	}{
		{name: "nil schema", schema: nil},
		{name: "unsupported type", schema: map[string]interface{}{"type": "objectish"}},
		{name: "bad type array item", schema: map[string]interface{}{"type": []interface{}{"object", 7}}},
		{name: "required empty", schema: map[string]interface{}{"type": "object", "required": []interface{}{""}}},
		{name: "required duplicate", schema: map[string]interface{}{"type": "object", "required": []interface{}{"q", "q"}}},
		{name: "properties not object", schema: map[string]interface{}{"type": "object", "properties": []interface{}{}}},
		{name: "property schema not object", schema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"q": "string"}}},
		{name: "items not object", schema: map[string]interface{}{"type": "object", "items": "string"}},
		{name: "additionalProperties invalid", schema: map[string]interface{}{"type": "object", "additionalProperties": "no"}},
		{name: "enum not array", schema: map[string]interface{}{"type": "object", "enum": "x"}},
		{name: "too deep", schema: deep},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Error(t, validateCapabilitySchema(tc.schema, "input_schema"))
		})
	}
}

func TestValidateJSONAgainstSchema(t *testing.T) {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query": map[string]interface{}{"type": "string"},
			"limit": map[string]interface{}{"type": "integer"},
			"tags": map[string]interface{}{
				"type":  "array",
				"items": map[string]interface{}{"type": "string"},
			},
		},
		"required":             []interface{}{"query"},
		"additionalProperties": false,
	}

	valid := map[string]interface{}{
		"query": "hello",
		"limit": float64(3),
		"tags":  []interface{}{"a", "b"},
	}
	require.NoError(t, validateJSONAgainstSchema(valid, schema, "input_json"))

	missingRequired := map[string]interface{}{"limit": float64(3)}
	assert.Error(t, validateJSONAgainstSchema(missingRequired, schema, "input_json"))

	wrongType := map[string]interface{}{"query": 7}
	assert.Error(t, validateJSONAgainstSchema(wrongType, schema, "input_json"))

	extraField := map[string]interface{}{"query": "hello", "extra": true}
	assert.Error(t, validateJSONAgainstSchema(extraField, schema, "input_json"))
}

func TestValidateJSONAgainstSchemaExtendedTypes(t *testing.T) {
	schema := map[string]interface{}{
		"properties": map[string]interface{}{
			"payload": map[string]interface{}{
				"properties": map[string]interface{}{
					"status": map[string]interface{}{"enum": []interface{}{"ok", "queued"}},
					"count":  map[string]interface{}{"type": "integer"},
					"maybe":  map[string]interface{}{"type": []interface{}{"string", "null"}},
				},
				"required": []interface{}{"status"},
				"additionalProperties": map[string]interface{}{
					"type": "number",
				},
			},
			"flags": map[string]interface{}{
				"type":  "array",
				"items": map[string]interface{}{"type": "boolean"},
			},
		},
		"required": []interface{}{"payload"},
	}

	valid := map[string]interface{}{
		"payload": map[string]interface{}{
			"status": "ok",
			"count":  int64(3),
			"maybe":  nil,
			"score":  float32(7.5),
		},
		"flags": []interface{}{true, false},
	}
	require.NoError(t, validateJSONAgainstSchema(valid, schema, "input_json"))

	enumMismatch := map[string]interface{}{"payload": map[string]interface{}{"status": "bad"}}
	assert.Error(t, validateJSONAgainstSchema(enumMismatch, schema, "input_json"))

	additionalMismatch := map[string]interface{}{
		"payload": map[string]interface{}{"status": "ok", "score": "high"},
	}
	assert.Error(t, validateJSONAgainstSchema(additionalMismatch, schema, "input_json"))

	arrayMismatch := map[string]interface{}{
		"payload": map[string]interface{}{"status": "ok"},
		"flags":   []interface{}{true, "false"},
	}
	assert.Error(t, validateJSONAgainstSchema(arrayMismatch, schema, "input_json"))

	require.True(t, valueMatchesJSONType(map[string]interface{}{}, "object"))
	require.True(t, valueMatchesJSONType([]interface{}{}, "array"))
	require.True(t, valueMatchesJSONType("x", "string"))
	require.True(t, valueMatchesJSONType(float64(1.5), "number"))
	require.True(t, valueMatchesJSONType(int32(2), "integer"))
	require.False(t, valueMatchesJSONType(float64(1.5), "integer"))
	require.True(t, valueMatchesJSONType(true, "boolean"))
	require.True(t, valueMatchesJSONType(nil, "null"))
	require.True(t, valueMatchesJSONType("anything", "future-type"))
	require.True(t, isJSONNumber(int(1)))
	require.False(t, isJSONNumber("1"))
}
