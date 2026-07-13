package servicebridge

import "testing"

func TestHostedInputSchemaCompatible(t *testing.T) {
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
			"count": map[string]interface{}{"type": "number"},
		},
		"required": []interface{}{"topic"},
	}
	if !hostedInputSchemaCompatible(compatible, capability) {
		t.Fatalf("expected controlled schema to be compatible")
	}

	missingRequired := map[string]interface{}{
		"type":       "object",
		"properties": map[string]interface{}{"topic": map[string]interface{}{"type": "string"}},
	}
	if hostedInputSchemaCompatible(missingRequired, capability) {
		t.Fatalf("Agent-required field must also be required by the listing")
	}

	wrongType := map[string]interface{}{
		"type":       "object",
		"properties": map[string]interface{}{"topic": map[string]interface{}{"type": "number"}},
		"required":   []interface{}{"topic"},
	}
	if hostedInputSchemaCompatible(wrongType, capability) {
		t.Fatalf("incompatible field type should be rejected")
	}
}

func TestControlledFieldsToJSONSchema(t *testing.T) {
	schema, err := controlledFieldsToJSONSchema([]interface{}{
		map[string]interface{}{"key": "topic", "type": "text", "required": true},
		map[string]interface{}{"key": "count", "type": "number"},
		map[string]interface{}{"key": "channel", "type": "select", "options": []interface{}{"email", "web"}},
		map[string]interface{}{"key": "source", "type": "url"},
	})
	if err != nil || !schemaAllowsType(schema["type"], "object") {
		t.Fatalf("controlledFieldsToJSONSchema() = %#v, %v", schema, err)
	}
	if _, ok := schemaRequiredSet(schema)["topic"]; !ok {
		t.Fatalf("required field missing from schema: %#v", schema)
	}
	properties, _ := schemaProperties(schema)
	channel := properties["channel"].(map[string]interface{})
	if enum, ok := schemaStringEnum(channel["enum"]); !ok || len(enum) != 2 {
		t.Fatalf("select options were not preserved as enum: %#v", channel)
	}
	if source := properties["source"].(map[string]interface{}); source["format"] != "uri" {
		t.Fatalf("url field must preserve its uri guarantee: %#v", source)
	}
}

func TestHostedInputSchemaCompatibleRejectsUnprovenConstraints(t *testing.T) {
	listing := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"channel": map[string]interface{}{"type": "string", "enum": []interface{}{"email", "web"}},
			"source":  map[string]interface{}{"type": "string", "format": "uri"},
		},
	}
	compatible := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"channel": map[string]interface{}{"type": "string", "enum": []interface{}{"email", "web", "api"}},
			"source":  map[string]interface{}{"type": "string", "format": "uri"},
		},
		"additionalProperties": false,
	}
	if !hostedInputSchemaCompatible(listing, compatible) {
		t.Fatal("listing enum subset and matching format should be compatible")
	}

	restricted := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"channel": map[string]interface{}{"type": "string", "pattern": "^email$"},
			"source":  map[string]interface{}{"type": "string", "format": "email"},
		},
		"additionalProperties": false,
	}
	if hostedInputSchemaCompatible(listing, restricted) {
		t.Fatal("constraints not guaranteed by the controlled form must fail closed")
	}
}
