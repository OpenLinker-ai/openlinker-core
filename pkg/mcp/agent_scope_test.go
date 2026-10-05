package mcp

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"
)

func TestScopedToolsUseOnlyValidBoundedObjectSchemas(t *testing.T) {
	valid := map[string]interface{}{"type": "object", "properties": map[string]interface{}{"text": map[string]interface{}{"type": "string"}}}
	oversized := map[string]interface{}{"type": "object", "description": strings.Repeat("x", maxScopedSchemaBytes)}
	for name, tc := range map[string]struct {
		schema map[string]interface{}
		custom bool
	}{
		"valid":      {valid, true},
		"missing":    {nil, false},
		"not object": {map[string]interface{}{"type": "string"}, false},
		"malformed":  {map[string]interface{}{"type": "object", "properties": "bad"}, false},
		"oversized":  {oversized, false},
	} {
		agent := &ScopedAgent{ID: uuid.New(), Slug: "a", Name: "A", Description: strings.Repeat("描", 1000), InputSchema: tc.schema}
		tools := scopedTools(agent)
		if len(tools) != len(scopedToolNames) {
			t.Fatalf("%s: tools=%d", name, len(tools))
		}
		input := tools[0].InputSchema["properties"].(map[string]interface{})["input"].(map[string]interface{})
		if _, custom := input["properties"]; custom != tc.custom {
			t.Fatalf("%s: custom schema used=%v", name, custom)
		}
		if utf8.RuneCountInString(tools[0].Description) > 600 {
			t.Fatalf("%s: description was not truncated", name)
		}
	}
}

func TestBindScopedAgent(t *testing.T) {
	scope := &ScopedAgent{ID: uuid.New()}
	req := RunAgentRequest{}
	if err := bindScopedAgent(&req, scope); err != nil || req.AgentID != scope.ID.String() {
		t.Fatalf("omitted agent_id was not injected: %v %q", err, req.AgentID)
	}
	req.AgentID = strings.ToUpper(scope.ID.String())
	if err := bindScopedAgent(&req, scope); err != nil {
		t.Fatalf("equal agent_id rejected: %v", err)
	}
	for _, other := range []string{uuid.NewString(), "not-a-uuid"} {
		req.AgentID = other
		if err := bindScopedAgent(&req, scope); err == nil || err.Code != -32602 {
			t.Fatalf("agent_id %q accepted", other)
		}
	}
}
