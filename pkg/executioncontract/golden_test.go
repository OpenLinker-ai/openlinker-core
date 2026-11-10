package executioncontract

import "testing"

// Golden values were computed from the contract code before mcp_transport and
// endpoint_auth_header_name existed. Default transport settings must keep
// every existing contract hash byte-for-byte, so stored External Execution
// fences and Workflow contracts stay valid.
const (
	goldenMCPAgentHash      = "hct:v1:a8894ad4a0bcfc80c122d30d76412af257ed6dadbb7b1f8f9a4bfb961d07a6f2"
	goldenDirectAgentHash   = "hct:v1:126d1b587893bbeedbfea89cb063c4c06d6f0c43f6b6d4b653ea8853ad08c363"
	goldenWorkflowHash      = "hct:v1:5d19f27b9d20e0ce23979bd52d5ef63b9d64b0061801d25d33a50560f540d1fa"
	goldenMCPAgentID        = "6a1f2a7e-2f43-4b2e-9a36-0d8c8b7f0a11"
	goldenDirectAgentID     = "2c9f8f0e-0b6c-4f38-8d0e-6e3c3f1f9b22"
	goldenWorkflowID        = "9d4c3b2a-1f0e-4d5c-8b7a-6f5e4d3c2b33"
	goldenWorkflowNodeID    = "0f1e2d3c-4b5a-4968-8776-655443322144"
	goldenWorkflowNodeKey   = "lookup"
	goldenMCPEndpointURL    = "https://mcp.example/mcp"
	goldenDirectEndpointURL = "https://agent.example/run"
)

func goldenMCPAgent() Agent {
	return Agent{
		ID: goldenMCPAgentID, ConnectionMode: "mcp_server", EndpointURL: goldenMCPEndpointURL,
		MCPToolName: "lookup", CapabilityVersion: 3,
		InputSchema: map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{"query": map[string]interface{}{"type": "string"}},
		},
		OutputSchema: map[string]interface{}{"type": "object"},
	}
}

func goldenDirectAgent() Agent {
	return Agent{
		ID: goldenDirectAgentID, ConnectionMode: "direct_http", EndpointURL: goldenDirectEndpointURL,
		CapabilityVersion: 1,
		InputSchema:       map[string]interface{}{"type": "object"},
		OutputSchema:      map[string]interface{}{"type": "object"},
	}
}

func goldenWorkflow(agentHash string) Workflow {
	return Workflow{
		ID: goldenWorkflowID,
		Nodes: []WorkflowNode{{
			ID: goldenWorkflowNodeID, Key: goldenWorkflowNodeKey, Type: "agent", AgentID: goldenMCPAgentID,
			Config: map[string]interface{}{"timeout_seconds": float64(30)}, Position: 0, AgentContractHash: agentHash,
		}},
	}
}

func TestDefaultTransportContractHashesMatchPreTransportGolden(t *testing.T) {
	mcpHash, err := AgentHash(goldenMCPAgent())
	if err != nil {
		t.Fatal(err)
	}
	directHash, err := AgentHash(goldenDirectAgent())
	if err != nil {
		t.Fatal(err)
	}
	workflowHash, err := WorkflowHash(goldenWorkflow(mcpHash))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mcp=%s direct=%s workflow=%s", mcpHash, directHash, workflowHash)
	if mcpHash != goldenMCPAgentHash {
		t.Fatalf("default MCP AgentHash = %s, want %s", mcpHash, goldenMCPAgentHash)
	}
	if directHash != goldenDirectAgentHash {
		t.Fatalf("direct AgentHash = %s, want %s", directHash, goldenDirectAgentHash)
	}
	if workflowHash != goldenWorkflowHash {
		t.Fatalf("WorkflowHash = %s, want %s", workflowHash, goldenWorkflowHash)
	}
}

func TestNonDefaultMCPTransportChangesContractHashes(t *testing.T) {
	base := goldenMCPAgent()
	explicitDefault := base
	explicitDefault.MCPTransport = "legacy_json"
	if hash, _ := AgentHash(explicitDefault); hash != goldenMCPAgentHash {
		t.Fatalf("explicit legacy_json hash = %s, want golden %s", hash, goldenMCPAgentHash)
	}
	seen := map[string]bool{goldenMCPAgentHash: true}
	for _, variant := range []Agent{
		func() Agent { a := base; a.MCPTransport = "streamable_http"; return a }(),
		func() Agent { a := base; a.MCPTransport = "legacy_sse"; return a }(),
		func() Agent { a := base; a.AuthHeaderName = "X-Api-Key"; return a }(),
		func() Agent { a := base; a.AuthHeaderName = "X-Other-Key"; return a }(),
	} {
		hash, err := AgentHash(variant)
		if err != nil {
			t.Fatal(err)
		}
		if seen[hash] {
			t.Fatalf("variant %+v did not change the contract hash", variant)
		}
		seen[hash] = true
		workflowHash, err := WorkflowHash(goldenWorkflow(hash))
		if err != nil || workflowHash == goldenWorkflowHash {
			t.Fatalf("workflow hash did not follow the node Agent contract: %s %v", workflowHash, err)
		}
	}
	// Transport fields never apply to non-MCP Agents.
	direct := goldenDirectAgent()
	direct.MCPTransport = "streamable_http"
	direct.AuthHeaderName = "X-Api-Key"
	if hash, _ := AgentHash(direct); hash != goldenDirectAgentHash {
		t.Fatalf("direct_http hash changed: %s", hash)
	}
}
