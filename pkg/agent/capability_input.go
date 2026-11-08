package agent

import (
	"encoding/json"
	"strings"

	"github.com/google/uuid"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Capability and example validation shared by the creator endpoints
// (PUT /creator/agents/:id/capabilities, POST /creator/agents/:id/examples)
// and self-registration, so both accept exactly the same shapes and limits.

const maxCapabilitySummaryLen = 1000

// preparedCapability is a validated capability declaration ready to store.
type preparedCapability struct {
	inputSchema  map[string]interface{}
	outputSchema map[string]interface{}
	inputJSON    []byte
	outputJSON   []byte
	summary      string
}

func prepareCapability(req *UpsertCapabilityRequest) (*preparedCapability, error) {
	if req == nil || req.InputSchema == nil || req.OutputSchema == nil {
		return nil, httpx.Unprocessable("input_schema 和 output_schema 必填")
	}
	summary := strings.TrimSpace(req.Summary)
	if len(summary) > maxCapabilitySummaryLen {
		return nil, httpx.Unprocessable("summary 不能超过 1000 字符")
	}
	if err := validateCapabilitySchema(req.InputSchema, "input_schema"); err != nil {
		return nil, httpx.Unprocessable(err.Error())
	}
	if err := validateCapabilitySchema(req.OutputSchema, "output_schema"); err != nil {
		return nil, httpx.Unprocessable(err.Error())
	}
	inputJSON, err := json.Marshal(req.InputSchema)
	if err != nil {
		return nil, httpx.BadRequest("input_schema 不是合法 JSON")
	}
	outputJSON, err := json.Marshal(req.OutputSchema)
	if err != nil {
		return nil, httpx.BadRequest("output_schema 不是合法 JSON")
	}
	return &preparedCapability{
		inputSchema:  req.InputSchema,
		outputSchema: req.OutputSchema,
		inputJSON:    inputJSON,
		outputJSON:   outputJSON,
		summary:      summary,
	}, nil
}

func (c *preparedCapability) upsertParams(agentID, creatorID uuid.UUID) db.UpsertAgentCapabilityParams {
	return db.UpsertAgentCapabilityParams{
		AgentID:      agentID,
		CreatorID:    creatorID,
		InputSchema:  c.inputJSON,
		OutputSchema: c.outputJSON,
		Summary:      c.summary,
	}
}

// preparedExample is a validated example ready to store.
type preparedExample struct {
	title        string
	inputJSON    []byte
	expectedJSON []byte
	sortOrder    int32
}

func errAgentExampleLimit() error {
	return httpx.BadRequest("每个 Agent 最多 20 条示例")
}

func validateExampleShape(req *CreateExampleRequest) error {
	if req == nil || req.InputJSON == nil {
		return httpx.Unprocessable("input_json 必填")
	}
	title := strings.TrimSpace(req.Title)
	if title == "" || len(title) > 120 {
		return httpx.Unprocessable("title 长度需在 1-120 字符之间")
	}
	return nil
}

// validateExampleAgainstSchemas checks an example against a capability's
// schemas. A nil schema map (no capability declared yet) accepts any object,
// as the creator endpoint does before a capability exists.
func validateExampleAgainstSchemas(
	input map[string]interface{},
	expectedOutput map[string]interface{},
	inputSchema map[string]interface{},
	outputSchema map[string]interface{},
) error {
	if inputSchema != nil {
		if err := validateJSONAgainstSchema(input, inputSchema, "input_json"); err != nil {
			return httpx.Unprocessable("input_json 不匹配 input_schema：" + err.Error())
		}
	}
	if expectedOutput != nil && outputSchema != nil {
		if err := validateJSONAgainstSchema(expectedOutput, outputSchema, "expected_output_json"); err != nil {
			return httpx.Unprocessable("expected_output_json 不匹配 output_schema：" + err.Error())
		}
	}
	return nil
}

// prepareExample marshals an example whose shape and schema were validated.
func prepareExample(req *CreateExampleRequest) (*preparedExample, error) {
	inputJSON, err := json.Marshal(req.InputJSON)
	if err != nil {
		return nil, httpx.BadRequest("input_json 不是合法 JSON")
	}
	var expectedJSON []byte
	if req.ExpectedOutputJSON != nil {
		expectedJSON, err = json.Marshal(req.ExpectedOutputJSON)
		if err != nil {
			return nil, httpx.BadRequest("expected_output_json 不是合法 JSON")
		}
	}
	return &preparedExample{
		title:        strings.TrimSpace(req.Title),
		inputJSON:    inputJSON,
		expectedJSON: expectedJSON,
		sortOrder:    req.SortOrder,
	}, nil
}

func (e *preparedExample) createParams(agentID, creatorID uuid.UUID) db.CreateAgentExampleParams {
	return db.CreateAgentExampleParams{
		AgentID:            agentID,
		CreatorID:          creatorID,
		Title:              e.title,
		InputJSON:          e.inputJSON,
		ExpectedOutputJSON: e.expectedJSON,
		SortOrder:          e.sortOrder,
	}
}

// prepareRegistrationOnboarding validates the optional capability and examples
// a self-registering Agent declares, with the creator endpoints' rules. Examples
// are checked against the capability declared in the same request (none
// declared means no schema check, as on the creator endpoint). When every
// example omits sort_order, request order is kept by using the array index:
// examples inserted in one transaction share created_at, the secondary key.
func prepareRegistrationOnboarding(
	capability *UpsertCapabilityRequest,
	examples []CreateExampleRequest,
) (*preparedCapability, []*preparedExample, error) {
	var preparedCap *preparedCapability
	if capability != nil {
		var err error
		preparedCap, err = prepareCapability(capability)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(examples) > maxAgentExamples {
		return nil, nil, errAgentExampleLimit()
	}
	var inputSchema, outputSchema map[string]interface{}
	if preparedCap != nil {
		inputSchema, outputSchema = preparedCap.inputSchema, preparedCap.outputSchema
	}
	useIndexOrder := true
	for i := range examples {
		if examples[i].SortOrder != 0 {
			useIndexOrder = false
			break
		}
	}
	prepared := make([]*preparedExample, 0, len(examples))
	for i := range examples {
		example := &examples[i]
		if err := validateExampleShape(example); err != nil {
			return nil, nil, err
		}
		if err := validateExampleAgainstSchemas(example.InputJSON, example.ExpectedOutputJSON, inputSchema, outputSchema); err != nil {
			return nil, nil, err
		}
		item, err := prepareExample(example)
		if err != nil {
			return nil, nil, err
		}
		if useIndexOrder {
			item.sortOrder = int32(i) // #nosec G115 -- bounded by maxAgentExamples.
		}
		prepared = append(prepared, item)
	}
	return preparedCap, prepared, nil
}
