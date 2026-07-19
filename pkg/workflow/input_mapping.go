package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const workflowInputMappingVersion = "v1"

type workflowInputMapping struct {
	Version string
	Fields  map[string]workflowInputSource
}

type workflowInputSource struct {
	Source  string
	Path    []string
	NodeKey string
	Value   interface{}
}

type workflowInputMappingWire struct {
	Version string                             `json:"version"`
	Fields  map[string]workflowInputSourceWire `json:"fields"`
}

type workflowInputSourceWire struct {
	Source  string          `json:"source"`
	Path    *[]string       `json:"path,omitempty"`
	NodeKey *string         `json:"node_key,omitempty"`
	Value   json.RawMessage `json:"value,omitempty"`
}

type workflowStepInputResult struct {
	Value  map[string]interface{}
	Mapped bool
	Err    error
}

func workflowStepInput(
	original map[string]interface{},
	outputsByNode map[string]map[string]interface{},
	parents []string,
	node db.WorkflowNode,
) workflowStepInputResult {
	legacy := legacyWorkflowStepInput(original, outputsByNode, parents, node.NodeKey)
	mapping, configured, err := decodeStoredWorkflowInputMapping(node.Config, parents)
	if err != nil {
		return workflowStepInputResult{Value: legacy, Mapped: true, Err: fmt.Errorf("workflow node %s input_mapping 无效: %w", node.NodeKey, err)}
	}
	if !configured {
		return workflowStepInputResult{Value: legacy}
	}
	value, err := evaluateWorkflowInputMapping(mapping, original, outputsByNode, parents)
	if err != nil {
		return workflowStepInputResult{Value: value, Mapped: true, Err: fmt.Errorf("workflow node %s input_mapping 求值失败: %w", node.NodeKey, err)}
	}
	return workflowStepInputResult{Value: value, Mapped: true}
}

func legacyWorkflowStepInput(
	original map[string]interface{},
	outputsByNode map[string]map[string]interface{},
	parents []string,
	nodeKey string,
) map[string]interface{} {
	if len(parents) == 0 {
		return map[string]interface{}{
			"workflow_input": original,
			"node_key":       nodeKey,
		}
	}
	dependencies := map[string]interface{}{}
	for _, key := range parents {
		dependencies[key] = outputsByNode[key]
	}
	input := map[string]interface{}{
		"workflow_input": original,
		"dependencies":   dependencies,
		"node_key":       nodeKey,
	}
	if len(parents) == 1 {
		input["previous_node"] = parents[0]
		input["previous_output"] = outputsByNode[parents[0]]
	}
	return input
}

func decodeRequestWorkflowInputMapping(config map[string]interface{}, parents []string) (*workflowInputMapping, bool, error) {
	if config == nil {
		return nil, false, nil
	}
	raw, exists := config["input_mapping"]
	if !exists {
		return nil, false, nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, true, fmt.Errorf("不是合法 JSON: %w", err)
	}
	mapping, err := decodeWorkflowInputMapping(data, parents)
	return mapping, true, err
}

func decodeStoredWorkflowInputMapping(config json.RawMessage, parents []string) (*workflowInputMapping, bool, error) {
	if len(config) == 0 {
		return nil, false, nil
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(config, &decoded); err != nil {
		return nil, false, fmt.Errorf("node config 不是合法 JSON")
	}
	return decodeRequestWorkflowInputMapping(decoded, parents)
}

func decodeWorkflowInputMapping(data []byte, parents []string) (*workflowInputMapping, error) {
	var wire workflowInputMappingWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("格式错误: %w", err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("只能包含一个 JSON object")
	}
	if wire.Version != workflowInputMappingVersion {
		return nil, fmt.Errorf("version 必须是 %s", workflowInputMappingVersion)
	}
	if len(wire.Fields) == 0 {
		return nil, fmt.Errorf("fields 不能为空")
	}
	parentSet := make(map[string]struct{}, len(parents))
	for _, parent := range parents {
		parentSet[parent] = struct{}{}
	}
	result := &workflowInputMapping{Version: wire.Version, Fields: make(map[string]workflowInputSource, len(wire.Fields))}
	for target, source := range wire.Fields {
		if target == "" || target != strings.TrimSpace(target) {
			return nil, fmt.Errorf("fields 目标字段不能为空或包含首尾空格")
		}
		if len(target) > 160 {
			return nil, fmt.Errorf("fields.%s 目标字段过长", target)
		}
		parsed, err := validateWorkflowInputSource(target, source, parentSet, len(parents))
		if err != nil {
			return nil, err
		}
		result.Fields[target] = parsed
	}
	return result, nil
}

func validateWorkflowInputSource(
	target string,
	wire workflowInputSourceWire,
	parents map[string]struct{},
	parentCount int,
) (workflowInputSource, error) {
	result := workflowInputSource{Source: wire.Source}
	switch wire.Source {
	case "workflow_input":
		if err := validateWorkflowInputPath(target, wire.Path); err != nil {
			return result, err
		}
		if wire.NodeKey != nil || wire.Value != nil {
			return result, fmt.Errorf("fields.%s workflow_input 不允许 node_key 或 value", target)
		}
		result.Path = append([]string{}, (*wire.Path)...)
	case "dependency":
		if err := validateWorkflowInputPath(target, wire.Path); err != nil {
			return result, err
		}
		if wire.NodeKey == nil || *wire.NodeKey == "" || *wire.NodeKey != strings.TrimSpace(*wire.NodeKey) {
			return result, fmt.Errorf("fields.%s dependency 必须提供有效 node_key", target)
		}
		if _, ok := parents[*wire.NodeKey]; !ok {
			return result, fmt.Errorf("fields.%s 只能引用直接父节点 %q", target, *wire.NodeKey)
		}
		if wire.Value != nil {
			return result, fmt.Errorf("fields.%s dependency 不允许 value", target)
		}
		result.Path = append([]string{}, (*wire.Path)...)
		result.NodeKey = *wire.NodeKey
	case "previous_output":
		if err := validateWorkflowInputPath(target, wire.Path); err != nil {
			return result, err
		}
		if parentCount != 1 {
			return result, fmt.Errorf("fields.%s previous_output 要求节点恰有一个直接父节点", target)
		}
		if wire.NodeKey != nil || wire.Value != nil {
			return result, fmt.Errorf("fields.%s previous_output 不允许 node_key 或 value", target)
		}
		result.Path = append([]string{}, (*wire.Path)...)
	case "constant":
		if wire.Value == nil {
			return result, fmt.Errorf("fields.%s constant 必须提供 value", target)
		}
		if wire.Path != nil || wire.NodeKey != nil {
			return result, fmt.Errorf("fields.%s constant 不允许 path 或 node_key", target)
		}
		if err := json.Unmarshal(wire.Value, &result.Value); err != nil {
			return result, fmt.Errorf("fields.%s constant value 不是合法 JSON", target)
		}
	default:
		return result, fmt.Errorf("fields.%s source 不受支持: %q", target, wire.Source)
	}
	return result, nil
}

func validateWorkflowInputPath(target string, path *[]string) error {
	if path == nil {
		return fmt.Errorf("fields.%s 必须提供 path", target)
	}
	if len(*path) > 32 {
		return fmt.Errorf("fields.%s path 过深", target)
	}
	for _, segment := range *path {
		if segment == "" || segment != strings.TrimSpace(segment) || len(segment) > 160 {
			return fmt.Errorf("fields.%s path 包含无效字段", target)
		}
	}
	return nil
}

func evaluateWorkflowInputMapping(
	mapping *workflowInputMapping,
	original map[string]interface{},
	outputsByNode map[string]map[string]interface{},
	parents []string,
) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(mapping.Fields))
	targets := make([]string, 0, len(mapping.Fields))
	for target := range mapping.Fields {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	for _, target := range targets {
		source := mapping.Fields[target]
		var value interface{}
		switch source.Source {
		case "workflow_input":
			value = original
		case "dependency":
			value = outputsByNode[source.NodeKey]
		case "previous_output":
			value = outputsByNode[parents[0]]
		case "constant":
			result[target] = source.Value
			continue
		}
		resolved, ok := workflowObjectPath(value, source.Path)
		if !ok {
			return result, fmt.Errorf("fields.%s 路径不存在", target)
		}
		result[target] = resolved
	}
	return result, nil
}

func workflowObjectPath(value interface{}, path []string) (interface{}, bool) {
	current := value
	for _, segment := range path {
		object, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func workflowInputMappingCoversRequired(mapping *workflowInputMapping, schema map[string]interface{}) error {
	raw, ok := schema["required"].([]interface{})
	if !ok {
		return nil
	}
	missing := make([]string, 0)
	for _, item := range raw {
		field, ok := item.(string)
		if !ok || field == "" {
			continue
		}
		if _, exists := mapping.Fields[field]; !exists {
			missing = append(missing, field)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("缺少 Agent input_schema 必填字段映射: %s", strings.Join(missing, ", "))
}

func (s *Service) validateWorkflowRequestInputMappings(
	ctx context.Context,
	nodes []WorkflowNodeRequest,
	edges []map[string]interface{},
) error {
	parentsByNode := make(map[string][]string, len(nodes))
	for _, edge := range edges {
		from := workflowEdgeEndpoint(edge, "from", "source", "source_key", "sourceKey")
		to := workflowEdgeEndpoint(edge, "to", "target", "target_key", "targetKey")
		parentsByNode[to] = append(parentsByNode[to], from)
	}
	for _, node := range nodes {
		key := strings.TrimSpace(node.Key)
		parents := append([]string{}, parentsByNode[key]...)
		sort.Strings(parents)
		mapping, configured, err := decodeRequestWorkflowInputMapping(node.Config, parents)
		if err != nil {
			return workflowInputMappingHTTPError(key, err)
		}
		if !configured {
			continue
		}
		capability, err := s.queries.GetAgentCapabilityByAgentID(ctx, node.AgentID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return workflowInputMappingHTTPError(key, errors.New("Agent 缺少 capability"))
			}
			return httpx.Internal("查询 workflow node Agent capability 失败")
		}
		var schema map[string]interface{}
		if err := json.Unmarshal(capability.InputSchema, &schema); err != nil {
			return workflowInputMappingHTTPError(key, errors.New("Agent input_schema 无效"))
		}
		if err := workflowInputMappingCoversRequired(mapping, schema); err != nil {
			return workflowInputMappingHTTPError(key, err)
		}
	}
	return nil
}

func workflowInputMappingHTTPError(nodeKey string, err error) error {
	message := fmt.Sprintf("workflow node %s input_mapping 无效: %v", nodeKey, err)
	return httpx.NewError(http.StatusUnprocessableEntity, workflowInputMappingInvalidCode, message)
}

func (s *Service) validateMappedWorkflowNodeInput(
	ctx context.Context,
	node db.WorkflowNode,
	input map[string]interface{},
) error {
	capability, err := s.queries.GetAgentCapabilityByAgentID(ctx, node.AgentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%s: workflow node %s Agent 缺少 capability", workflowNodeInputSchemaMismatchCode, node.NodeKey)
		}
		return fmt.Errorf("查询 workflow node %s Agent capability 失败: %w", node.NodeKey, err)
	}
	var schema map[string]interface{}
	if err := json.Unmarshal(capability.InputSchema, &schema); err != nil {
		return fmt.Errorf("%s: workflow node %s Agent input_schema 无效", workflowNodeInputSchemaMismatchCode, node.NodeKey)
	}
	if err := agent.ValidateInputAgainstSchema(input, schema); err != nil {
		return fmt.Errorf("%s: workflow node %s input 不匹配 Agent input_schema: %v", workflowNodeInputSchemaMismatchCode, node.NodeKey, err)
	}
	return nil
}
