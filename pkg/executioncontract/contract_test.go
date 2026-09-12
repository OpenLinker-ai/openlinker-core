package executioncontract

import (
	"strings"
	"testing"
)

func TestAgentHashIsCanonicalAndCoversExecutionContract(t *testing.T) {
	base := Agent{
		ID: "agent-1", ConnectionMode: "direct_http", EndpointURL: "https://agent.example/run",
		CapabilityVersion: 2,
		InputSchema:       map[string]interface{}{"type": "object", "properties": map[string]interface{}{"topic": map[string]interface{}{"type": "string"}}},
		OutputSchema:      map[string]interface{}{"type": "object"},
	}
	first, err := AgentHash(base)
	if err != nil || !Valid(first) {
		t.Fatalf("AgentHash() = %q, %v", first, err)
	}
	reordered := base
	reordered.InputSchema = map[string]interface{}{"properties": map[string]interface{}{"topic": map[string]interface{}{"type": "string"}}, "type": "object"}
	second, err := AgentHash(reordered)
	if err != nil || second != first {
		t.Fatalf("canonical AgentHash() = %q, want %q (%v)", second, first, err)
	}
	defaultMode := base
	defaultMode.ConnectionMode = ""
	directMode := base
	directMode.ConnectionMode = "direct_http"
	defaultHash, _ := AgentHash(defaultMode)
	directHash, _ := AgentHash(directMode)
	if defaultHash != directHash {
		t.Fatal("empty connection mode must normalize to direct_http")
	}
	changed := base
	changed.CapabilityVersion++
	third, _ := AgentHash(changed)
	if third == first {
		t.Fatal("capability version must change hash")
	}
	changed = base
	changed.EndpointURL = "https://agent.example/v2"
	fourth, _ := AgentHash(changed)
	if fourth == first {
		t.Fatal("endpoint must change hash")
	}
	changed = base
	changed.ConnectionMode = "mcp_server"
	changed.MCPToolName = "execute"
	fifth, _ := AgentHash(changed)
	if fifth == first {
		t.Fatal("connection contract must change hash")
	}
	changed = base
	changed.InputSchema = map[string]interface{}{"type": "object", "required": []interface{}{"topic"}}
	inputChanged, _ := AgentHash(changed)
	if inputChanged == first {
		t.Fatal("input schema must change hash")
	}
	changed = base
	changed.OutputSchema = map[string]interface{}{"type": "string"}
	sixth, _ := AgentHash(changed)
	if sixth == first {
		t.Fatal("output schema must change hash")
	}
	mcpBase := base
	mcpBase.ConnectionMode = "mcp_server"
	mcpBase.MCPToolName = "execute"
	mcpHash, _ := AgentHash(mcpBase)
	mcpChanged := mcpBase
	mcpChanged.MCPToolName = "execute_v2"
	mcpChangedHash, _ := AgentHash(mcpChanged)
	if mcpChangedHash == mcpHash {
		t.Fatal("MCP tool name must change hash")
	}
}

func TestWorkflowHashNormalizesOrderAndCoversDependencies(t *testing.T) {
	agentHash, _ := AgentHash(Agent{ID: "agent-1", ConnectionMode: "runtime", CapabilityVersion: 1})
	base := Workflow{ID: "workflow-1",
		Edges: []map[string]interface{}{{"from": "a", "to": "b"}, {"from": "b", "to": "c"}},
		Nodes: []WorkflowNode{
			{ID: "node-b", Key: "b", Type: "agent", AgentID: "agent-1", Position: 1, AgentContractHash: agentHash},
			{ID: "node-a", Key: "a", Type: "agent", AgentID: "agent-1", Position: 0, Config: map[string]interface{}{"prompt": "x"}, AgentContractHash: agentHash},
		},
	}
	first, err := WorkflowHash(base)
	if err != nil || !Valid(first) {
		t.Fatalf("WorkflowHash() = %q, %v", first, err)
	}
	reordered := base
	reordered.Edges = []map[string]interface{}{base.Edges[1], base.Edges[0]}
	reordered.Nodes = []WorkflowNode{base.Nodes[1], base.Nodes[0]}
	second, err := WorkflowHash(reordered)
	if err != nil || second != first {
		t.Fatalf("canonical WorkflowHash() = %q, want %q (%v)", second, first, err)
	}
	changed := base
	changed.Nodes = append([]WorkflowNode(nil), base.Nodes...)
	changed.Nodes[0].AgentContractHash, _ = AgentHash(Agent{ID: "agent-1", ConnectionMode: "runtime", CapabilityVersion: 2})
	third, _ := WorkflowHash(changed)
	if third == first {
		t.Fatal("node agent contract must change workflow hash")
	}
	changed = base
	changed.Nodes = append([]WorkflowNode(nil), base.Nodes...)
	changed.Nodes[1].Config = map[string]interface{}{"prompt": "changed"}
	fourth, _ := WorkflowHash(changed)
	if fourth == first {
		t.Fatal("node config must change workflow hash")
	}
	changed = base
	changed.Edges = []map[string]interface{}{{"from": "a", "to": "c"}, {"from": "b", "to": "c"}}
	edgeChanged, _ := WorkflowHash(changed)
	if edgeChanged == first {
		t.Fatal("workflow edge must change hash")
	}
	changed = base
	changed.Nodes = append([]WorkflowNode(nil), base.Nodes...)
	changed.Nodes[0].Position++
	positionChanged, _ := WorkflowHash(changed)
	if positionChanged == first {
		t.Fatal("workflow node position must change hash")
	}
}

func TestIndependentWorkflowFencesLegacyImplicitChainContract(t *testing.T) {
	input := Workflow{ID: "workflow-1", Edges: []map[string]interface{}{}, Nodes: []WorkflowNode{
		{ID: "node-a", Key: "a", Type: "agent", AgentID: "agent-1", Position: 0, AgentContractHash: Prefix + strings.Repeat("0", 64)},
		{ID: "node-b", Key: "b", Type: "agent", AgentID: "agent-1", Position: 1, AgentContractHash: Prefix + strings.Repeat("0", 64)},
	}}
	independent, err := WorkflowHash(input)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := hash(map[string]interface{}{
		"contract_schema": "hct:v1", "target_type": "workflow", "target_id": "workflow-1",
		"edges": []map[string]interface{}(nil),
		"nodes": []map[string]interface{}{
			{"id": "node-a", "key": "a", "type": "agent", "agent_id": "agent-1", "config": map[string]interface{}{}, "position": 0, "agent_contract_hash": Prefix + strings.Repeat("0", 64)},
			{"id": "node-b", "key": "b", "type": "agent", "agent_id": "agent-1", "config": map[string]interface{}{}, "position": 1, "agent_contract_hash": Prefix + strings.Repeat("0", 64)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if independent == legacy {
		t.Fatal("parallel graph must not accept an old implicit-chain service contract")
	}
}
