package workflow

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestWorkflowStepInputLegacyEnvelopeIsUnchanged(t *testing.T) {
	original := map[string]interface{}{"topic": "runtime"}
	outputs := map[string]map[string]interface{}{"research": {"summary": "done"}}

	root := workflowStepInput(original, outputs, nil, db.WorkflowNode{NodeKey: "root", Config: []byte(`{"source":"legacy"}`)})
	if root.Err != nil || root.Mapped {
		t.Fatalf("root = %#v", root)
	}
	wantRoot := map[string]interface{}{"workflow_input": original, "node_key": "root"}
	if !reflect.DeepEqual(root.Value, wantRoot) {
		t.Fatalf("root.Value = %#v, want %#v", root.Value, wantRoot)
	}

	child := workflowStepInput(original, outputs, []string{"research"}, db.WorkflowNode{NodeKey: "review"})
	wantChild := map[string]interface{}{
		"workflow_input": original,
		"dependencies":   map[string]interface{}{"research": outputs["research"]},
		"node_key":       "review", "previous_node": "research", "previous_output": outputs["research"],
	}
	if child.Err != nil || child.Mapped || !reflect.DeepEqual(child.Value, wantChild) {
		t.Fatalf("child = %#v, want %#v", child, wantChild)
	}
}

func TestWorkflowStepInputMappingEvaluatesExplicitSources(t *testing.T) {
	config := map[string]interface{}{
		"input_mapping": map[string]interface{}{
			"version": "v1",
			"fields": map[string]interface{}{
				"task":    map[string]interface{}{"source": "workflow_input", "path": []interface{}{"request", "task"}},
				"summary": map[string]interface{}{"source": "dependency", "node_key": "research", "path": []interface{}{"summary"}},
				"context": map[string]interface{}{"source": "previous_output", "path": []interface{}{}},
				"format":  map[string]interface{}{"source": "constant", "value": "markdown"},
			},
		},
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	original := map[string]interface{}{"request": map[string]interface{}{"task": "audit"}}
	outputs := map[string]map[string]interface{}{"research": {"summary": "evidence", "score": float64(9)}}
	got := workflowStepInput(original, outputs, []string{"research"}, db.WorkflowNode{NodeKey: "review", Config: configJSON})
	if got.Err != nil || !got.Mapped {
		t.Fatalf("mapped input = %#v", got)
	}
	want := map[string]interface{}{
		"task": "audit", "summary": "evidence", "context": outputs["research"], "format": "markdown",
	}
	if !reflect.DeepEqual(got.Value, want) {
		t.Fatalf("mapped value = %#v, want %#v", got.Value, want)
	}
}

func TestWorkflowInputMappingRejectsAmbiguousOrUnsafeSources(t *testing.T) {
	tests := []struct {
		name    string
		mapping string
		parents []string
		want    string
	}{
		{name: "unknown version", mapping: `{"version":"v2","fields":{"task":{"source":"workflow_input","path":["text"]}}}`, want: "version"},
		{name: "unknown property", mapping: `{"version":"v1","extra":true,"fields":{"task":{"source":"workflow_input","path":["text"]}}}`, want: "unknown field"},
		{name: "dependency is not parent", mapping: `{"version":"v1","fields":{"task":{"source":"dependency","node_key":"other","path":[]}}}`, parents: []string{"research"}, want: "直接父节点"},
		{name: "previous requires one parent", mapping: `{"version":"v1","fields":{"task":{"source":"previous_output","path":[]}}}`, parents: []string{"a", "b"}, want: "恰有一个"},
		{name: "constant cannot have path", mapping: `{"version":"v1","fields":{"task":{"source":"constant","value":"x","path":[]}}}`, want: "不允许 path"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeWorkflowInputMapping([]byte(tc.mapping), tc.parents)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestWorkflowInputMappingRequiredCoverage(t *testing.T) {
	mapping, err := decodeWorkflowInputMapping([]byte(`{
		"version":"v1",
		"fields":{"task":{"source":"workflow_input","path":["text"]}}
	}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]interface{}{"required": []interface{}{"task", "weeks"}}
	err = workflowInputMappingCoversRequired(mapping, schema)
	if err == nil || !strings.Contains(err.Error(), "weeks") {
		t.Fatalf("coverage error = %v", err)
	}
}

func TestWorkflowStepInputMappingReportsMissingRuntimePath(t *testing.T) {
	config := []byte(`{"input_mapping":{"version":"v1","fields":{"task":{"source":"workflow_input","path":["missing"]}}}}`)
	got := workflowStepInput(map[string]interface{}{"text": "present"}, nil, nil, db.WorkflowNode{NodeKey: "run", Config: config})
	if got.Err == nil || !got.Mapped || !strings.Contains(got.Err.Error(), "路径不存在") {
		t.Fatalf("mapped input = %#v", got)
	}
}
