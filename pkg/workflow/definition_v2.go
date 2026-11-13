package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
)

// Semantics v2 definitions interpret node policies, conditional edges, join
// modes, input mapping v2 and an output mapping. v1 definitions ignore all of
// these keys, exactly as before.

const (
	onFailureContinue = "continue"
	joinAll           = "all"
	joinAny           = "any"

	maxWorkflowNodeTimeoutSeconds = 7 * 24 * 3600
	maxWorkflowRetryAttempts      = 10
	maxWorkflowRetryBackoff       = 3600
)

// ---------------------------------------------------------------- policy

type workflowNodePolicySpec struct {
	TimeoutSeconds int64                  `json:"timeout_seconds,omitempty"`
	Retry          *workflowNodeRetrySpec `json:"retry,omitempty"`
	OnFailure      string                 `json:"on_failure,omitempty"`
}

type workflowNodeRetrySpec struct {
	MaxAttempts    int32    `json:"max_attempts"`
	BackoffSeconds int64    `json:"backoff_seconds,omitempty"`
	On             []string `json:"on,omitempty"`
}

// parseWorkflowNodePolicy reads and validates config.policy (nil when absent).
func parseWorkflowNodePolicy(config []byte) (*workflowNodePolicySpec, error) {
	raw, ok, err := workflowConfigKey(config, "policy")
	if err != nil || !ok {
		return nil, err
	}
	var spec workflowNodePolicySpec
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return nil, fmt.Errorf("policy 无效: %w", err)
	}
	if spec.TimeoutSeconds < 0 || spec.TimeoutSeconds > maxWorkflowNodeTimeoutSeconds {
		return nil, fmt.Errorf("policy.timeout_seconds 必须在 1 到 %d 之间", maxWorkflowNodeTimeoutSeconds)
	}
	if spec.Retry != nil {
		if spec.Retry.MaxAttempts < 1 || spec.Retry.MaxAttempts > maxWorkflowRetryAttempts {
			return nil, fmt.Errorf("policy.retry.max_attempts 必须在 1 到 %d 之间", maxWorkflowRetryAttempts)
		}
		if spec.Retry.BackoffSeconds < 0 || spec.Retry.BackoffSeconds > maxWorkflowRetryBackoff {
			return nil, fmt.Errorf("policy.retry.backoff_seconds 必须在 0 到 %d 之间", maxWorkflowRetryBackoff)
		}
		for _, on := range spec.Retry.On {
			switch on {
			case "failed":
			case "timeout":
				return nil, fmt.Errorf("policy.retry.on 不支持 timeout：超时重试需要执行方提供后端停止证明")
			default:
				return nil, fmt.Errorf("policy.retry.on 只接受 failed")
			}
		}
	}
	switch spec.OnFailure {
	case "", onFailureFailWorkflow, onFailureContinue:
	default:
		return nil, fmt.Errorf("policy.on_failure 只接受 fail_workflow 或 continue")
	}
	return &spec, nil
}

func workflowNodeJoin(config []byte) (string, error) {
	raw, ok, err := workflowConfigKey(config, "join")
	if err != nil || !ok {
		return joinAll, err
	}
	var join string
	if err := json.Unmarshal(raw, &join); err != nil || (join != joinAll && join != joinAny) {
		return "", fmt.Errorf("join 只接受 all 或 any")
	}
	return join, nil
}

func workflowConfigKey(config []byte, key string) (json.RawMessage, bool, error) {
	if len(config) == 0 {
		return nil, false, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(config, &object); err != nil {
		return nil, false, fmt.Errorf("config 不是 JSON 对象")
	}
	raw, ok := object[key]
	return raw, ok, nil
}

// ---------------------------------------------------------------- paths

// workflowPathV2 is a JSON path: strings are object keys, non-negative
// integers are array indexes. Empty means the whole value.
type workflowPathV2 []interface{}

func parseWorkflowPathV2(raw json.RawMessage) (workflowPathV2, error) {
	if len(raw) == 0 {
		return workflowPathV2{}, nil
	}
	var segments []json.RawMessage
	if err := json.Unmarshal(raw, &segments); err != nil {
		return nil, fmt.Errorf("path 必须是数组")
	}
	path := make(workflowPathV2, 0, len(segments))
	for _, segment := range segments {
		var key string
		if json.Unmarshal(segment, &key) == nil {
			path = append(path, key)
			continue
		}
		var number json.Number
		decoder := json.NewDecoder(bytes.NewReader(segment))
		decoder.UseNumber()
		if decoder.Decode(&number) != nil {
			return nil, fmt.Errorf("path 段只能是字符串或非负整数")
		}
		index, err := number.Int64()
		if err != nil || index < 0 || strings.ContainsAny(number.String(), ".eE") {
			return nil, fmt.Errorf("path 下标必须是非负整数")
		}
		path = append(path, int(index))
	}
	return path, nil
}

// resolve walks the path; ok=false means the path is missing.
func (p workflowPathV2) resolve(value interface{}) (interface{}, bool) {
	current := value
	for _, segment := range p {
		switch key := segment.(type) {
		case string:
			object, ok := current.(map[string]interface{})
			if !ok {
				return nil, false
			}
			next, exists := object[key]
			if !exists {
				return nil, false
			}
			current = next
		case int:
			array, ok := current.([]interface{})
			if !ok || key >= len(array) {
				return nil, false
			}
			current = array[key]
		}
	}
	return current, true
}

// ---------------------------------------------------------------- conditions

type workflowEdgeCondition struct {
	Path     workflowPathV2
	Op       string
	Value    interface{}
	HasValue bool
}

type workflowEdgeConditionWire struct {
	Path  json.RawMessage `json:"path"`
	Op    string          `json:"op"`
	Value json.RawMessage `json:"value,omitempty"`
}

func parseWorkflowEdgeCondition(raw interface{}) (*workflowEdgeCondition, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("when 不是合法 JSON")
	}
	var wire workflowEdgeConditionWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("when 无效: %w", err)
	}
	path, err := parseWorkflowPathV2(wire.Path)
	if err != nil {
		return nil, fmt.Errorf("when.%w", err)
	}
	condition := &workflowEdgeCondition{Path: path, Op: wire.Op, HasValue: len(wire.Value) > 0}
	if condition.HasValue {
		value, err := decodeJSONValue(wire.Value)
		if err != nil {
			return nil, fmt.Errorf("when.value 无效")
		}
		condition.Value = value
	}
	switch wire.Op {
	case "exists", "truthy":
		if condition.HasValue {
			return nil, fmt.Errorf("when.op %s 不接受 value", wire.Op)
		}
	case "eq", "ne":
		if !condition.HasValue {
			return nil, fmt.Errorf("when.op %s 需要 value", wire.Op)
		}
	case "in":
		if _, ok := condition.Value.([]interface{}); !ok {
			return nil, fmt.Errorf("when.op in 的 value 必须是数组")
		}
	case "gt", "gte", "lt", "lte":
		if _, ok := condition.Value.(json.Number); !ok {
			return nil, fmt.Errorf("when.op %s 的 value 必须是数值", wire.Op)
		}
	default:
		return nil, fmt.Errorf("when.op 只接受 exists/eq/ne/in/gt/gte/lt/lte/truthy")
	}
	return condition, nil
}

// evaluate never errors: the evaluation table of the design (missing path:
// only ne is true; ordering needs numbers on both sides; deep equality
// compares numbers numerically).
func (c *workflowEdgeCondition) evaluate(output interface{}) bool {
	left, present := c.Path.resolve(normalizeJSONNumbers(output))
	switch c.Op {
	case "exists":
		return present
	case "eq":
		return present && jsonDeepEqual(left, c.Value)
	case "ne":
		return !present || !jsonDeepEqual(left, c.Value)
	case "in":
		if !present {
			return false
		}
		for _, item := range c.Value.([]interface{}) {
			if jsonDeepEqual(left, item) {
				return true
			}
		}
		return false
	case "gt", "gte", "lt", "lte":
		l, ok := left.(json.Number)
		if !present || !ok {
			return false
		}
		cmp := compareJSONNumbers(l, c.Value.(json.Number))
		switch c.Op {
		case "gt":
			return cmp > 0
		case "gte":
			return cmp >= 0
		case "lt":
			return cmp < 0
		default:
			return cmp <= 0
		}
	case "truthy":
		return present && jsonTruthy(left)
	}
	return false
}

func decodeJSONValue(raw json.RawMessage) (interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// normalizeJSONNumbers re-decodes a value so every number is a json.Number.
func normalizeJSONNumbers(value interface{}) interface{} {
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	normalized, err := decodeJSONValue(data)
	if err != nil {
		return value
	}
	return normalized
}

func compareJSONNumbers(a, b json.Number) int {
	left, okA := new(big.Rat).SetString(a.String())
	right, okB := new(big.Rat).SetString(b.String())
	if !okA || !okB {
		return 0
	}
	return left.Cmp(right)
}

func jsonDeepEqual(a, b interface{}) bool {
	switch left := a.(type) {
	case json.Number:
		right, ok := b.(json.Number)
		return ok && compareJSONNumbers(left, right) == 0
	case []interface{}:
		right, ok := b.([]interface{})
		if !ok || len(left) != len(right) {
			return false
		}
		for i := range left {
			if !jsonDeepEqual(left[i], right[i]) {
				return false
			}
		}
		return true
	case map[string]interface{}:
		right, ok := b.(map[string]interface{})
		if !ok || len(left) != len(right) {
			return false
		}
		for key, value := range left {
			other, exists := right[key]
			if !exists || !jsonDeepEqual(value, other) {
				return false
			}
		}
		return true
	case nil:
		return b == nil
	default:
		return a == b
	}
}

func jsonTruthy(value interface{}) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case json.Number:
		return compareJSONNumbers(v, json.Number("0")) != 0
	case string:
		return v != ""
	case []interface{}:
		return len(v) > 0
	case map[string]interface{}:
		return len(v) > 0
	}
	return true
}

// ---------------------------------------------------------------- mapping v2

type workflowMappingV2Field struct {
	Source  string
	Node    string
	Path    workflowPathV2
	Value   interface{}
	Default json.RawMessage // property absent: len 0; "default": null: bytes null
}

type workflowMappingV2 struct {
	Fields map[string]workflowMappingV2Field
}

type workflowMappingV2FieldWire struct {
	Source  string          `json:"source"`
	Node    string          `json:"node,omitempty"`
	Path    json.RawMessage `json:"path,omitempty"`
	Value   json.RawMessage `json:"value,omitempty"`
	Default json.RawMessage `json:"default,omitempty"`
}

// parseWorkflowMappingV2 decodes {"version":"v2","fields":{...}}. output
// marks the output mapping, which rejects previous_output.
func parseWorkflowMappingV2(raw interface{}, output bool) (*workflowMappingV2, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("不是合法 JSON")
	}
	var wire struct {
		Version string                                `json:"version"`
		Fields  map[string]workflowMappingV2FieldWire `json:"fields"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("结构无效: %w", err)
	}
	if wire.Version != "v2" {
		return nil, fmt.Errorf("version 必须是 v2")
	}
	if len(wire.Fields) == 0 {
		return nil, fmt.Errorf("fields 不能为空")
	}
	mapping := &workflowMappingV2{Fields: map[string]workflowMappingV2Field{}}
	for name, field := range wire.Fields {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("字段名不能为空")
		}
		parsed := workflowMappingV2Field{Source: field.Source, Node: strings.TrimSpace(field.Node), Default: field.Default}
		if len(field.Default) > 0 {
			if _, err := decodeJSONValue(field.Default); err != nil {
				return nil, fmt.Errorf("字段 %s 的 default 无效", name)
			}
		}
		switch field.Source {
		case "constant":
			if len(field.Value) == 0 || len(field.Path) > 0 || parsed.Node != "" {
				return nil, fmt.Errorf("字段 %s: constant 只接受 value", name)
			}
			value, err := decodeJSONValue(field.Value)
			if err != nil {
				return nil, fmt.Errorf("字段 %s 的 value 无效", name)
			}
			parsed.Value = value
		case "workflow_input", "dependency", "previous_output":
			if len(field.Value) > 0 {
				return nil, fmt.Errorf("字段 %s: %s 不接受 value", name, field.Source)
			}
			if (field.Source == "dependency") != (parsed.Node != "") {
				return nil, fmt.Errorf("字段 %s: 只有 dependency 需要 node", name)
			}
			if output && field.Source == "previous_output" {
				return nil, fmt.Errorf("字段 %s: output_mapping 不能使用 previous_output", name)
			}
			path, err := parseWorkflowPathV2(field.Path)
			if err != nil {
				return nil, fmt.Errorf("字段 %s 的 %w", name, err)
			}
			parsed.Path = path
		default:
			return nil, fmt.Errorf("字段 %s: source 只接受 workflow_input/dependency/previous_output/constant", name)
		}
		mapping.Fields[name] = parsed
	}
	return mapping, nil
}

type workflowMappingContext struct {
	input     map[string]interface{}
	outputs   map[string]map[string]interface{} // only successful nodes
	parents   []string
	available func(nodeKey string) bool
}

// evaluate computes the mapped object. A missing path or an unavailable
// source uses the field's default; without one the evaluation fails naming
// the field and the reason.
func (m *workflowMappingV2) evaluate(ctx workflowMappingContext) (map[string]interface{}, error) {
	names := make([]string, 0, len(m.Fields))
	for name := range m.Fields {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make(map[string]interface{}, len(m.Fields))
	for _, name := range names {
		field := m.Fields[name]
		value, state := field.resolve(ctx)
		if state != "value" {
			if len(field.Default) == 0 {
				return nil, fmt.Errorf("字段 %s: %s，且没有 default", name, map[string]string{
					"missing": "路径缺失", "unavailable": "来源节点没有成功输出",
				}[state])
			}
			value, _ = decodeJSONValue(field.Default)
		}
		result[name] = value
	}
	return result, nil
}

func (f workflowMappingV2Field) resolve(ctx workflowMappingContext) (interface{}, string) {
	var source interface{}
	switch f.Source {
	case "constant":
		return f.Value, "value"
	case "workflow_input":
		source = ctx.input
	case "previous_output":
		if len(ctx.parents) != 1 {
			return nil, "unavailable"
		}
		output, ok := ctx.outputs[ctx.parents[0]]
		if !ok {
			return nil, "unavailable"
		}
		source = output
	case "dependency":
		output, ok := ctx.outputs[f.Node]
		if !ok {
			return nil, "unavailable"
		}
		source = output
	}
	value, present := f.Path.resolve(normalizeJSONNumbers(source))
	if !present {
		return nil, "missing"
	}
	return value, "value"
}

// ---------------------------------------------------------------- definition

type workflowV2Edge struct {
	from, to string
	when     *workflowEdgeCondition
}

// workflowV2Definition is the parsed v2 structure of a definition.
type workflowV2Definition struct {
	edges    []workflowV2Edge
	incoming map[string][]workflowV2Edge
	join     map[string]string
	policy   map[string]workflowNodePolicySpec
	mappings map[string]*workflowMappingV2
	output   *workflowMappingV2
}

type workflowV2NodeInput struct {
	key    string
	config []byte
}

// parseWorkflowV2Definition validates a v2 definition: node policies, join,
// conditional edges, input mappings v2 (dependency must be an ancestor) and
// the output mapping, plus the availability rule: a field whose source may
// be unavailable must have a default.
func parseWorkflowV2Definition(nodes []workflowV2NodeInput, edges []map[string]interface{}, outputMapping interface{}) (*workflowV2Definition, error) {
	def := &workflowV2Definition{
		incoming: map[string][]workflowV2Edge{}, join: map[string]string{},
		policy: map[string]workflowNodePolicySpec{}, mappings: map[string]*workflowMappingV2{},
	}
	keys := map[string]bool{}
	for _, node := range nodes {
		keys[node.key] = true
	}
	parents := map[string][]string{}
	for _, raw := range edges {
		edge := workflowV2Edge{from: workflowEdgeEndpoint(raw, "from"), to: workflowEdgeEndpoint(raw, "to")}
		if when, ok := raw["when"]; ok {
			condition, err := parseWorkflowEdgeCondition(when)
			if err != nil {
				return nil, fmt.Errorf("edge %s->%s: %w", edge.from, edge.to, err)
			}
			edge.when = condition
		}
		for key := range raw {
			if !isWorkflowEndpointKey(key) && key != "when" {
				return nil, fmt.Errorf("edge %s->%s: 不支持的字段 %s", edge.from, edge.to, key)
			}
		}
		def.edges = append(def.edges, edge)
		def.incoming[edge.to] = append(def.incoming[edge.to], edge)
		parents[edge.to] = append(parents[edge.to], edge.from)
	}
	ancestors := workflowAncestors(nodes, parents)
	for _, node := range nodes {
		if spec, err := parseWorkflowNodePolicy(node.config); err != nil {
			return nil, fmt.Errorf("node %s: %w", node.key, err)
		} else if spec != nil {
			def.policy[node.key] = *spec
		}
		_, hasJoin, err := workflowConfigKey(node.config, "join")
		if err != nil {
			return nil, fmt.Errorf("node %s: %w", node.key, err)
		}
		join, err := workflowNodeJoin(node.config)
		if err != nil {
			return nil, fmt.Errorf("node %s: %w", node.key, err)
		}
		if hasJoin && len(def.incoming[node.key]) == 0 {
			return nil, fmt.Errorf("node %s: 根节点不能配置 join", node.key)
		}
		def.join[node.key] = join
		if raw, ok, _ := workflowConfigKey(node.config, "input_mapping"); ok {
			var decoded interface{}
			_ = json.Unmarshal(raw, &decoded)
			mapping, err := parseWorkflowMappingV2(decoded, false)
			if err != nil {
				return nil, fmt.Errorf("node %s input_mapping: %w", node.key, err)
			}
			for name, field := range mapping.Fields {
				if field.Source == "dependency" && !ancestors[node.key][field.Node] {
					return nil, fmt.Errorf("node %s input_mapping 字段 %s: dependency %s 必须是祖先节点", node.key, name, field.Node)
				}
				if field.Source == "previous_output" && len(parents[node.key]) != 1 {
					return nil, fmt.Errorf("node %s input_mapping 字段 %s: previous_output 需要唯一父节点", node.key, name)
				}
			}
			def.mappings[node.key] = mapping
		}
	}
	if outputMapping != nil {
		mapping, err := parseWorkflowMappingV2(outputMapping, true)
		if err != nil {
			return nil, fmt.Errorf("output_mapping: %w", err)
		}
		for name, field := range mapping.Fields {
			if field.Source == "dependency" && !keys[field.Node] {
				return nil, fmt.Errorf("output_mapping 字段 %s: node %s 不存在", name, field.Node)
			}
		}
		def.output = mapping
	}
	// Availability: a field whose source may not be successful needs a default.
	mayBeUnavailable := def.mayBeUnavailable(nodes, ancestors)
	for _, node := range nodes {
		mapping := def.mappings[node.key]
		if mapping == nil {
			continue
		}
		for name, field := range mapping.Fields {
			source := field.Node
			if field.Source == "previous_output" {
				source = parents[node.key][0]
			}
			if source == "" || len(field.Default) > 0 || !mayBeUnavailable[source] {
				continue
			}
			if def.guaranteedFor(node.key, source) {
				continue
			}
			return nil, fmt.Errorf("node %s input_mapping 字段 %s: 来源 %s 可能不可用，必须设置 default", node.key, name, source)
		}
	}
	if def.output != nil {
		for name, field := range def.output.Fields {
			if field.Source == "dependency" && len(field.Default) == 0 && mayBeUnavailable[field.Node] {
				return nil, fmt.Errorf("output_mapping 字段 %s: 来源 %s 可能不可用，必须设置 default", name, field.Node)
			}
		}
	}
	return def, nil
}

// mayBeUnavailable: a node may end without success when it may continue
// after failing, may be skipped (a conditional incoming edge, join any, or a
// parent that may be unavailable under join all).
func (d *workflowV2Definition) mayBeUnavailable(nodes []workflowV2NodeInput, ancestors map[string]map[string]bool) map[string]bool {
	result := map[string]bool{}
	order := make([]workflowV2NodeInput, len(nodes))
	copy(order, nodes)
	sort.SliceStable(order, func(i, j int) bool { return len(ancestors[order[i].key]) < len(ancestors[order[j].key]) })
	for _, node := range order {
		unavailable := d.policy[node.key].OnFailure == onFailureContinue || d.join[node.key] == joinAny
		for _, edge := range d.incoming[node.key] {
			if edge.when != nil || result[edge.from] {
				unavailable = true
			}
		}
		result[node.key] = unavailable
	}
	return result
}

// guaranteedFor: when target runs under join all, a direct unconditional
// parent is necessarily successful.
func (d *workflowV2Definition) guaranteedFor(target, source string) bool {
	if d.join[target] != joinAll {
		return false
	}
	for _, edge := range d.incoming[target] {
		if edge.from == source && edge.when == nil {
			return true
		}
	}
	return false
}

func workflowAncestors(nodes []workflowV2NodeInput, parents map[string][]string) map[string]map[string]bool {
	result := map[string]map[string]bool{}
	var visit func(key string, seen map[string]bool) map[string]bool
	visit = func(key string, seen map[string]bool) map[string]bool {
		if cached, ok := result[key]; ok {
			return cached
		}
		if seen[key] {
			return map[string]bool{}
		}
		seen[key] = true
		set := map[string]bool{}
		for _, parent := range parents[key] {
			set[parent] = true
			for ancestor := range visit(parent, seen) {
				set[ancestor] = true
			}
		}
		result[key] = set
		return set
	}
	for _, node := range nodes {
		visit(node.key, map[string]bool{})
	}
	return result
}
