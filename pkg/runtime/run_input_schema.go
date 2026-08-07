package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/inputschema"
)

const runInputSchemaMismatchCode = httpx.ErrorCode("RUN_INPUT_SCHEMA_MISMATCH")

func validateRunTargetApplicationInput(
	target db.AgentRunTarget,
	input map[string]interface{},
) (map[string]interface{}, error) {
	if target.CapabilityVersion == nil {
		return nil, nil
	}
	if *target.CapabilityVersion < 1 || len(target.CapabilityInputSchema) == 0 {
		return nil, fmt.Errorf("agent %s capability input schema is incomplete", target.Agent.ID)
	}

	var schema map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(target.CapabilityInputSchema))
	decoder.UseNumber()
	if err := decoder.Decode(&schema); err != nil || schema == nil {
		return nil, fmt.Errorf("agent %s capability input schema is invalid JSON", target.Agent.ID)
	}
	if err := inputschema.ValidateInputSchema(schema); err != nil {
		return nil, fmt.Errorf("agent %s capability input schema is invalid: %w", target.Agent.ID, err)
	}
	if err := inputschema.ValidateInput(input, schema); err != nil {
		var violation *inputschema.Violation
		if !errors.As(err, &violation) {
			return nil, fmt.Errorf("agent %s input schema validation failed: %w", target.Agent.ID, err)
		}
		return nil, &httpx.HTTPError{
			Status:  http.StatusUnprocessableEntity,
			Code:    runInputSchemaMismatchCode,
			Message: "input 不匹配 Agent input_schema",
			Details: map[string]interface{}{
				"agent_id":           target.Agent.ID.String(),
				"capability_version": *target.CapabilityVersion,
				"path":               violation.Path,
				"reason":             violation.Reason,
			},
		}
	}
	return schema, nil
}

func inputSchemaAllowsServerField(schema map[string]interface{}, field string) bool {
	if schema == nil {
		return true
	}
	if properties, ok := schema["properties"].(map[string]interface{}); ok {
		if _, declared := properties[field]; declared {
			return true
		}
	}
	additional, declared := schema["additionalProperties"]
	if !declared {
		return true
	}
	switch value := additional.(type) {
	case bool:
		return value
	case map[string]interface{}:
		return true
	default:
		return false
	}
}
