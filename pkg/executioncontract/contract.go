package executioncontract

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const Prefix = "hct:v1:"

var hashPattern = regexp.MustCompile(`^hct:v1:[a-f0-9]{64}$`)

type Agent struct {
	ID                string
	ConnectionMode    string
	EndpointURL       string
	MCPToolName       string
	CapabilityVersion int32
	InputSchema       map[string]interface{}
	OutputSchema      map[string]interface{}
}

type WorkflowNode struct {
	ID                string
	Key               string
	Type              string
	AgentID           string
	Config            map[string]interface{}
	Position          int32
	AgentContractHash string
}

type Workflow struct {
	ID    string
	Edges []map[string]interface{}
	Nodes []WorkflowNode
}

func Valid(value string) bool {
	return hashPattern.MatchString(strings.TrimSpace(value))
}

func AgentHash(input Agent) (string, error) {
	mode := strings.TrimSpace(input.ConnectionMode)
	if mode == "" {
		mode = "direct_http"
	}
	tool := ""
	if mode == "mcp_server" {
		tool = strings.TrimSpace(input.MCPToolName)
	}
	return hash(map[string]interface{}{
		"contract_schema":    "hct:v1",
		"target_type":        "agent",
		"target_id":          strings.TrimSpace(input.ID),
		"connection_mode":    mode,
		"endpoint_url":       strings.TrimSpace(input.EndpointURL),
		"mcp_tool_name":      tool,
		"capability_version": input.CapabilityVersion,
		"input_schema":       nonNilMap(input.InputSchema),
		"output_schema":      nonNilMap(input.OutputSchema),
	})
}

func WorkflowHash(input Workflow) (string, error) {
	edges := append([]map[string]interface{}(nil), input.Edges...)
	sort.Slice(edges, func(i, j int) bool {
		return canonicalSortKey(edges[i]) < canonicalSortKey(edges[j])
	})
	nodes := append([]WorkflowNode(nil), input.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Position != nodes[j].Position {
			return nodes[i].Position < nodes[j].Position
		}
		if nodes[i].Key != nodes[j].Key {
			return nodes[i].Key < nodes[j].Key
		}
		return nodes[i].ID < nodes[j].ID
	})
	canonicalNodes := make([]map[string]interface{}, 0, len(nodes))
	for _, node := range nodes {
		if !Valid(node.AgentContractHash) {
			return "", fmt.Errorf("invalid node agent contract hash")
		}
		canonicalNodes = append(canonicalNodes, map[string]interface{}{
			"id":                  strings.TrimSpace(node.ID),
			"key":                 strings.TrimSpace(node.Key),
			"type":                strings.TrimSpace(node.Type),
			"agent_id":            strings.TrimSpace(node.AgentID),
			"config":              nonNilMap(node.Config),
			"position":            node.Position,
			"agent_contract_hash": strings.TrimSpace(node.AgentContractHash),
		})
	}
	contract := map[string]interface{}{
		"contract_schema": "hct:v1",
		"target_type":     "workflow",
		"target_id":       strings.TrimSpace(input.ID),
		"edges":           edges,
		"nodes":           canonicalNodes,
	}
	if len(edges) == 0 && len(nodes) > 1 {
		// Older Core versions executed [] as a chain. Fence their external
		// execution snapshots before applying independent-root semantics.
		contract["edge_semantics"] = "independent_roots"
	}
	return hash(contract)
}

func hash(value interface{}) (string, error) {
	canonical, err := runtime.CanonicalizeRFC8785(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return Prefix + hex.EncodeToString(digest[:]), nil
}

func canonicalSortKey(value interface{}) string {
	canonical, err := runtime.CanonicalizeRFC8785(value)
	if err != nil {
		return fmt.Sprintf("invalid:%#v", value)
	}
	return string(canonical)
}

func nonNilMap(value map[string]interface{}) map[string]interface{} {
	if value == nil {
		return map[string]interface{}{}
	}
	return value
}
