package agent

import "github.com/OpenLinker-ai/openlinker-core/pkg/inputschema"

const maxSchemaDepth = inputschema.MaxDepth

// InputSchemaViolation remains the Agent package's public validation error
// contract while the implementation lives in a dependency-neutral package
// shared with Runtime Run creation.
type InputSchemaViolation = inputschema.Violation

func validateCapabilitySchema(schema map[string]interface{}, label string) error {
	return inputschema.ValidateCapabilitySchema(schema, label)
}

// ValidateInputSchema validates the JSON Schema subset accepted for Agent
// application inputs.
func ValidateInputSchema(schema map[string]interface{}) error {
	return inputschema.ValidateInputSchema(schema)
}

func validateJSONAgainstSchema(value interface{}, schema map[string]interface{}, label string) error {
	return inputschema.ValidateJSON(value, schema, label)
}

// ValidateInputAgainstSchema lets Core orchestration layers validate the
// concrete input they are about to send to an Agent. Capability schema
// ownership stays in the shared contract package; callers do not duplicate a
// partial validator or reinterpret the Agent contract.
func ValidateInputAgainstSchema(value interface{}, schema map[string]interface{}) error {
	return inputschema.ValidateInput(value, schema)
}

func schemaTypes(raw interface{}) ([]string, error) {
	return inputschema.SchemaTypes(raw)
}

func stringArray(raw interface{}) ([]string, error) {
	return inputschema.StringArray(raw)
}

func schemaAllowsType(schema map[string]interface{}, want string) bool {
	return inputschema.AllowsType(schema, want)
}

func valueMatchesJSONType(value interface{}, want string) bool {
	return inputschema.ValueMatchesJSONType(value, want)
}

func isJSONNumber(value interface{}) bool {
	return inputschema.IsJSONNumber(value)
}
