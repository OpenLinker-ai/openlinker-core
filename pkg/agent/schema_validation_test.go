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
