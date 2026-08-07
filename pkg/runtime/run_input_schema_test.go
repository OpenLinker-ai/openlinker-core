package runtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestValidateRunTargetApplicationInput(t *testing.T) {
	agentID := uuid.New()
	version := int32(3)
	target := db.AgentRunTarget{
		Agent:             db.Agent{ID: agentID},
		CapabilityVersion: &version,
		CapabilityInputSchema: []byte(`{
			"type":"object",
			"properties":{
				"query":{"type":"string"},
				"budget":{"type":"integer"},
				"mode":{"enum":["fast","deep"]},
				"tags":{"type":"array","items":{"type":"string"}}
			},
			"required":["query","budget"],
			"additionalProperties":false
		}`),
	}

	schema, err := validateRunTargetApplicationInput(target, map[string]interface{}{
		"query":  "research",
		"budget": json.Number("5"),
		"mode":   "deep",
		"tags":   []interface{}{"finance"},
	})
	require.NoError(t, err)
	require.Equal(t, "object", schema["type"])

	_, err = validateRunTargetApplicationInput(target, map[string]interface{}{
		"query":  "research",
		"budget": "five",
	})
	var httpErr *httpx.HTTPError
	require.True(t, errors.As(err, &httpErr), "error = %v", err)
	require.Equal(t, http.StatusUnprocessableEntity, httpErr.Status)
	require.Equal(t, runInputSchemaMismatchCode, httpErr.Code)
	require.Equal(t, map[string]interface{}{
		"agent_id":           agentID.String(),
		"capability_version": version,
		"path":               "input.budget",
		"reason":             "type_mismatch",
	}, httpErr.Details)
}

func TestValidateRunTargetApplicationInputLegacyAndCorruptCapabilities(t *testing.T) {
	agentID := uuid.New()
	schema, err := validateRunTargetApplicationInput(
		db.AgentRunTarget{Agent: db.Agent{ID: agentID}},
		map[string]interface{}{"legacy": true},
	)
	require.NoError(t, err)
	require.Nil(t, schema)

	version := int32(1)
	_, err = validateRunTargetApplicationInput(db.AgentRunTarget{
		Agent:                 db.Agent{ID: agentID},
		CapabilityVersion:     &version,
		CapabilityInputSchema: []byte(`{"type":"future"}`),
	}, map[string]interface{}{})
	require.Error(t, err)
	var httpErr *httpx.HTTPError
	require.False(t, errors.As(err, &httpErr))
}

func TestAttachRunA2AContextHonorsStrictApplicationSchema(t *testing.T) {
	context := &RunA2AContextRequest{
		ProtocolContextID: "context-1",
		ProtocolTaskID:    "task-1",
		RootContextID:     "root-1",
		ReferenceTaskIDs:  []string{"reference-1"},
	}
	strictSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query":          map[string]interface{}{"type": "string"},
			"a2a_context_id": map[string]interface{}{"type": "string"},
		},
		"additionalProperties": false,
	}
	strictInput := map[string]interface{}{"query": "hello"}
	attachRunA2AContextToInput(strictInput, context, strictSchema)
	require.Equal(t, map[string]interface{}{
		"query":          "hello",
		"a2a_context_id": "context-1",
	}, strictInput)

	legacyInput := map[string]interface{}{"query": "hello"}
	attachRunA2AContextToInput(legacyInput, context, nil)
	require.Equal(t, "context-1", legacyInput["a2a_context_id"])
	require.Equal(t, "task-1", legacyInput["a2a_task_id"])
	require.Equal(t, "root-1", legacyInput["a2a_root_context_id"])
	require.Equal(t, []string{"reference-1"}, legacyInput["a2a_reference_task_ids"])
}
