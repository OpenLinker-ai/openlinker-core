package workflow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustJSON(t *testing.T, raw string) interface{} {
	t.Helper()
	value, err := decodeJSONValue(json.RawMessage(raw))
	require.NoError(t, err)
	return value
}

func condition(t *testing.T, raw string) *workflowEdgeCondition {
	t.Helper()
	c, err := parseWorkflowEdgeCondition(mustJSON(t, raw))
	require.NoError(t, err)
	return c
}

// The evaluation table of the design, cell by cell.
func TestWorkflowEdgeConditionEvaluationTable(t *testing.T) {
	output := map[string]interface{}{
		"category": "refund", "nullable": nil, "score": json.Number("1"), "count": float64(3),
		"empty": "", "list": []interface{}{}, "obj": map[string]interface{}{}, "flag": false,
		"nested": map[string]interface{}{"items": []interface{}{map[string]interface{}{"x": "y"}}},
	}
	cases := []struct {
		when string
		want bool
	}{
		// missing path
		{`{"path":["missing"],"op":"exists"}`, false},
		{`{"path":["missing"],"op":"eq","value":"refund"}`, false},
		{`{"path":["missing"],"op":"ne","value":"refund"}`, true},
		{`{"path":["missing"],"op":"in","value":["refund"]}`, false},
		{`{"path":["missing"],"op":"gt","value":0}`, false},
		{`{"path":["missing"],"op":"truthy"}`, false},
		// null
		{`{"path":["nullable"],"op":"exists"}`, true},
		{`{"path":["nullable"],"op":"eq","value":null}`, true},
		{`{"path":["nullable"],"op":"ne","value":null}`, false},
		{`{"path":["nullable"],"op":"in","value":[1,null]}`, true},
		{`{"path":["nullable"],"op":"in","value":[1]}`, false},
		{`{"path":["nullable"],"op":"lt","value":1}`, false},
		{`{"path":["nullable"],"op":"truthy"}`, false},
		// type differs from value
		{`{"path":["category"],"op":"eq","value":1}`, false},
		{`{"path":["category"],"op":"ne","value":1}`, true},
		{`{"path":["category"],"op":"gt","value":1}`, false},
		// same type; 1 and 1.0 are equal
		{`{"path":["category"],"op":"eq","value":"refund"}`, true},
		{`{"path":["score"],"op":"eq","value":1.0}`, true},
		{`{"path":["count"],"op":"eq","value":3}`, true},
		{`{"path":["count"],"op":"gte","value":3}`, true},
		{`{"path":["count"],"op":"gt","value":3}`, false},
		{`{"path":["count"],"op":"lte","value":2.5}`, false},
		{`{"path":["category"],"op":"in","value":["a","refund"]}`, true},
		// truthiness
		{`{"path":["empty"],"op":"truthy"}`, false},
		{`{"path":["list"],"op":"truthy"}`, false},
		{`{"path":["obj"],"op":"truthy"}`, false},
		{`{"path":["flag"],"op":"truthy"}`, false},
		{`{"path":["score"],"op":"truthy"}`, true},
		{`{"path":["category"],"op":"truthy"}`, true},
		// paths: integer indexes, whole value
		{`{"path":["nested","items",0,"x"],"op":"eq","value":"y"}`, true},
		{`{"path":["nested","items",1,"x"],"op":"exists"}`, false},
		{`{"path":[],"op":"exists"}`, true},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, condition(t, tc.when).evaluate(output), tc.when)
	}
}

func TestWorkflowEdgeConditionDefinitionErrors(t *testing.T) {
	for _, raw := range []string{
		`{"path":["a"],"op":"in","value":"x"}`,
		`{"path":["a"],"op":"gt","value":"1"}`,
		`{"path":["a"],"op":"exists","value":1}`,
		`{"path":["a"],"op":"truthy","value":true}`,
		`{"path":["a"],"op":"eq"}`,
		`{"path":["a"],"op":"like","value":"x"}`,
		`{"path":["1"],"op":"exists","extra":1}`,
		`{"path":[-1],"op":"exists"}`,
		`{"path":[1.5],"op":"exists"}`,
	} {
		_, err := parseWorkflowEdgeCondition(mustJSON(t, raw))
		require.Error(t, err, raw)
	}
}

// default distinguishes "absent", "null" and a value that is itself null.
func TestWorkflowMappingV2DefaultsAndAvailability(t *testing.T) {
	mapping, err := parseWorkflowMappingV2(mustJSON(t, `{"version":"v2","fields":{
		"query":   {"source":"workflow_input","path":["question"]},
		"summary": {"source":"dependency","node":"research","path":["items",0,"text"],"default":""},
		"tag":     {"source":"dependency","node":"classify","path":["label"],"default":null},
		"raw":     {"source":"dependency","node":"classify","path":["nullable"]},
		"mode":    {"source":"constant","value":"fast"}}}`), false)
	require.NoError(t, err)
	require.Len(t, mapping.Fields["tag"].Default, 4, `"default": null is kept as the bytes null`)
	require.Len(t, mapping.Fields["raw"].Default, 0)

	value, err := mapping.evaluate(workflowMappingContext{
		input: map[string]interface{}{"question": "q"},
		outputs: map[string]map[string]interface{}{
			"classify": {"nullable": nil},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "q", value["query"])
	require.Equal(t, "", value["summary"], "unavailable source uses its default")
	require.Nil(t, value["tag"], "missing path uses default null")
	require.Contains(t, value, "raw")
	require.Nil(t, value["raw"], "a value that is null is a value, not missing")
	require.Equal(t, "fast", value["mode"])

	_, err = mapping.evaluate(workflowMappingContext{input: map[string]interface{}{}, outputs: map[string]map[string]interface{}{}})
	require.ErrorContains(t, err, "query")
}

func TestWorkflowNodePolicyValidation(t *testing.T) {
	spec, err := parseWorkflowNodePolicy([]byte(`{"policy":{"timeout_seconds":60,"retry":{"max_attempts":2,"backoff_seconds":5,"on":["failed"]},"on_failure":"continue"}}`))
	require.NoError(t, err)
	require.EqualValues(t, 2, spec.Retry.MaxAttempts)
	for _, bad := range []string{
		`{"policy":{"retry":{"max_attempts":2,"on":["timeout"]}}}`,
		`{"policy":{"retry":{"max_attempts":0}}}`,
		`{"policy":{"on_failure":"ignore"}}`,
		`{"policy":{"timeout_seconds":-1}}`,
		`{"policy":{"unknown":1}}`,
	} {
		_, err := parseWorkflowNodePolicy([]byte(bad))
		require.Error(t, err, bad)
	}
	// v1 ignores policies; v2 applies them.
	s := &Service{}
	require.EqualValues(t, 1, s.workflowNodePolicy(workflowSemanticsV1, []byte(`{"policy":{"retry":{"max_attempts":3}}}`)).maxAttempts)
	require.EqualValues(t, 3, s.workflowNodePolicy(workflowSemanticsV2, []byte(`{"policy":{"retry":{"max_attempts":3}}}`)).maxAttempts)
}

func v2Nodes(configs map[string]string) []workflowV2NodeInput {
	nodes := []workflowV2NodeInput{}
	for _, key := range []string{"a", "b", "c"} {
		if config, ok := configs[key]; ok {
			nodes = append(nodes, workflowV2NodeInput{key: key, config: []byte(config)})
		}
	}
	return nodes
}

func TestWorkflowV2DefinitionValidation(t *testing.T) {
	edge := func(from, to string, when string) map[string]interface{} {
		e := map[string]interface{}{"from": from, "to": to}
		if when != "" {
			var w interface{}
			_ = json.Unmarshal([]byte(when), &w)
			e["when"] = w
		}
		return e
	}
	when := `{"path":["ok"],"op":"truthy"}`
	mapB := `{"input_mapping":{"version":"v2","fields":{"x":{"source":"dependency","node":"a","path":["v"]}}}}`

	// A direct unconditional parent under join all is guaranteed: no default needed.
	_, err := parseWorkflowV2Definition(v2Nodes(map[string]string{"a": `{}`, "b": mapB}), []map[string]interface{}{edge("a", "b", "")}, nil)
	require.NoError(t, err)

	// Through a condition the source may be unavailable to c.
	mapC := `{"input_mapping":{"version":"v2","fields":{"x":{"source":"dependency","node":"b","path":["v"]}}},"join":"any"}`
	_, err = parseWorkflowV2Definition(v2Nodes(map[string]string{"a": `{}`, "b": `{}`, "c": mapC}),
		[]map[string]interface{}{edge("a", "b", when), edge("b", "c", ""), edge("a", "c", "")}, nil)
	require.ErrorContains(t, err, "必须设置 default")

	// Root nodes cannot set join.
	_, err = parseWorkflowV2Definition(v2Nodes(map[string]string{"a": `{"join":"any"}`}), nil, nil)
	require.ErrorContains(t, err, "根节点")

	// dependency must be an ancestor.
	_, err = parseWorkflowV2Definition(v2Nodes(map[string]string{"a": `{}`, "b": `{}`, "c": `{"input_mapping":{"version":"v2","fields":{"x":{"source":"dependency","node":"b","path":[]}}}}`}),
		[]map[string]interface{}{edge("a", "c", "")}, nil)
	require.ErrorContains(t, err, "祖先")

	// output_mapping rejects previous_output and needs defaults for unavailable sources.
	_, err = parseWorkflowV2Definition(v2Nodes(map[string]string{"a": `{}`}), nil,
		mustJSON(t, `{"version":"v2","fields":{"x":{"source":"previous_output","path":[]}}}`))
	require.ErrorContains(t, err, "previous_output")
	_, err = parseWorkflowV2Definition(v2Nodes(map[string]string{"a": `{"policy":{"on_failure":"continue"}}`}), nil,
		mustJSON(t, `{"version":"v2","fields":{"x":{"source":"dependency","node":"a","path":[]}}}`))
	require.ErrorContains(t, err, "default")
}

func TestWorkflowV2ActivationJoinAndContinue(t *testing.T) {
	def, err := parseWorkflowV2Definition(
		v2Nodes(map[string]string{"a": `{"policy":{"on_failure":"continue"}}`, "b": `{}`, "c": `{"join":"any"}`}),
		[]map[string]interface{}{
			{"from": "a", "to": "c"},
			{"from": "b", "to": "c", "when": map[string]interface{}{"path": []interface{}{"go"}, "op": "truthy"}},
		}, nil)
	require.NoError(t, err)
	snap := func(a, b string) workflowRunSnapshot {
		return workflowRunSnapshot{nodes: map[string]workflowNodeSnapshot{
			"a": {nodeKey: "a", state: a}, "b": {nodeKey: "b", state: b}, "c": {nodeKey: "c", state: nodeWaiting},
		}}
	}
	got, _ := def.activation("c", snap(nodeRunning, nodeSuccess), nil)
	require.Equal(t, "unresolved", got)
	got, _ = def.activation("c", snap(nodeFailed, nodeSuccess), map[string]map[string]interface{}{"b": {"go": true}})
	require.Equal(t, "ready", got, "join any: one active edge is enough")
	got, reason := def.activation("c", snap(nodeFailed, nodeSuccess), map[string]map[string]interface{}{"b": {"go": false}})
	require.Equal(t, "skip", got)
	require.Contains(t, reason, "join any")
}
