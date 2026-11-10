package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcpclient"
)

// DiscoverMCPToolsRequest asks Core to list the tools of an MCP server with
// the endpoint and credential the creator is about to register. Nothing in
// it is stored.
type DiscoverMCPToolsRequest struct {
	EndpointURL            string `json:"endpoint_url" validate:"required,max=500"`
	MCPTransport           string `json:"mcp_transport" validate:"required,oneof=streamable_http legacy_sse"`
	EndpointAuthHeader     string `json:"endpoint_auth_header" validate:"max=500"`
	EndpointAuthHeaderName string `json:"endpoint_auth_header_name" validate:"max=128"`
}

// DiscoveredMCPTool is one tools/list entry. InputSchemaError is set when the
// schema would be rejected by the same capability validation manual
// registration uses; the tool is still listed so the creator sees why.
type DiscoveredMCPTool struct {
	Name             string                 `json:"name"`
	Title            string                 `json:"title,omitempty"`
	Description      string                 `json:"description,omitempty"`
	InputSchema      map[string]interface{} `json:"input_schema"`
	InputSchemaError string                 `json:"input_schema_error,omitempty"`
}

type DiscoverMCPToolsResponse struct {
	Tools []DiscoveredMCPTool `json:"tools"`
}

const mcpDiscoveryTimeout = 30 * time.Second

// mcpToolLister is replaced in tests.
type mcpToolLister func(context.Context, mcpclient.Config) ([]mcpclient.Tool, error)

// DiscoverMCPTools runs initialize and tools/list once against the supplied
// endpoint. It is limited to creators, applies the outbound endpoint policy,
// and keeps neither the credential nor the result.
func (s *Service) DiscoverMCPTools(ctx context.Context, creatorID uuid.UUID, req *DiscoverMCPToolsRequest) (*DiscoverMCPToolsResponse, error) {
	user, err := s.queries.GetUserByID(ctx, creatorID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, httpx.NotFound("用户不存在")
		}
		log.Error().Err(err).Msg("agent.DiscoverMCPTools: GetUserByID")
		return nil, httpx.Internal("查询用户失败")
	}
	if user.DisabledAt != nil {
		return nil, httpx.Unauthorized("账号已禁用")
	}
	if !user.IsCreator {
		return nil, httpx.Forbidden("仅创作者可发现 MCP 工具")
	}
	return s.discoverMCPTools(ctx, req, mcpclient.ListTools)
}

func (s *Service) discoverMCPTools(ctx context.Context, req *DiscoverMCPToolsRequest, list mcpToolLister) (*DiscoverMCPToolsResponse, error) {
	allowLocalHTTP := s.cfg != nil && s.cfg.AllowLocalHTTPEndpoints
	endpoint := strings.TrimSpace(req.EndpointURL)
	if err := endpointurl.Validate(endpoint, allowLocalHTTP); err != nil {
		return nil, httpx.Unprocessable(err.Error())
	}
	transport := strings.TrimSpace(req.MCPTransport)
	if transport != mcpclient.TransportStreamableHTTP && transport != mcpclient.TransportLegacySSE {
		return nil, httpx.Unprocessable("mcp_transport 必须是 streamable_http 或 legacy_sse")
	}
	var headerName *string
	if name := strings.TrimSpace(req.EndpointAuthHeaderName); name != "" {
		if err := mcpclient.ValidateAuthHeaderName(name); err != nil {
			return nil, httpx.Unprocessable(err.Error())
		}
		headerName = &name
	}
	header := http.Header{}
	header.Set("User-Agent", "OpenLinker/1.0")
	if name, value, ok := mcpclient.CredentialHeader(req.EndpointAuthHeader, headerName); ok {
		header.Set(name, value)
	}
	tools, err := list(ctx, mcpclient.Config{
		Transport:      transport,
		EndpointURL:    endpoint,
		Header:         header,
		HTTPClient:     mcpclient.NewHTTPClient(endpointurl.NewHTTPClient(mcpDiscoveryTimeout, allowLocalHTTP), allowLocalHTTP),
		AllowLocalHTTP: allowLocalHTTP,
		Limits:         mcpclient.Limits{TotalTimeout: mcpDiscoveryTimeout},
	})
	if err != nil {
		return nil, mcpDiscoveryError(err)
	}
	resp := &DiscoverMCPToolsResponse{Tools: make([]DiscoveredMCPTool, 0, len(tools))}
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		discovered := DiscoveredMCPTool{Name: name, Title: tool.Title, Description: tool.Description}
		schema := map[string]interface{}{}
		if len(tool.InputSchema) == 0 || json.Unmarshal(tool.InputSchema, &schema) != nil {
			discovered.InputSchemaError = "inputSchema 不是 JSON object"
		} else if err := validateCapabilitySchema(schema, "input_schema"); err != nil {
			discovered.InputSchemaError = err.Error()
		}
		discovered.InputSchema = schema
		resp.Tools = append(resp.Tools, discovered)
	}
	return resp, nil
}

// mcpDiscoveryError reports why discovery failed without echoing response
// bodies, which could reflect the supplied credential.
func mcpDiscoveryError(err error) error {
	var status *mcpclient.HTTPStatusError
	var rpc *mcpclient.RPCError
	var protocol *mcpclient.ProtocolError
	message := "MCP 服务不可达"
	switch {
	case errors.As(err, &status):
		message = fmt.Sprintf("MCP 服务在 %s 阶段返回 HTTP %d", status.Stage, status.StatusCode)
	case errors.As(err, &rpc):
		message = fmt.Sprintf("MCP 服务在 %s 阶段返回 JSON-RPC 错误 %d", rpc.Stage, rpc.Code)
	case errors.As(err, &protocol):
		message = fmt.Sprintf("MCP 服务在 %s 阶段的响应不符合协议: %s", protocol.Stage, protocol.Message)
	case errors.Is(err, mcpclient.ErrEndpointRejected):
		message = "MCP 服务的消息地址不在 endpoint_url 同源或未通过出站策略"
	case errors.Is(err, endpointurl.ErrCrossOriginRedirect):
		message = "MCP 服务重定向到其他来源，已拒绝"
	case errors.Is(err, context.DeadlineExceeded):
		message = "MCP 服务响应超时"
	}
	return httpx.NewError(http.StatusBadGateway, "MCP_DISCOVERY_FAILED", message)
}
