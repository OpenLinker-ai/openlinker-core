package agent

import (
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcpclient"
)

const (
	ConnectionModeDirectHTTP = "direct_http"
	ConnectionModeMCPServer  = "mcp_server"
	ConnectionModeRuntime    = "runtime"
)

const runtimeEndpointPrefix = "openlinker-runtime://"

type connectionSettings struct {
	Mode        string
	EndpointURL string
	MCPToolName *string
	// MCPTransport and EndpointAuthHeaderName are set by withMCPCallShape;
	// only mcp_server Agents may use non-default values.
	MCPTransport           string
	EndpointAuthHeaderName *string
}

// withMCPCallShape validates how an mcp_server Agent is called: its transport
// and the optional credential header name. Other connection modes keep the
// defaults and reject explicit non-default values.
func (c connectionSettings) withMCPCallShape(transport string, headerName *string) (connectionSettings, error) {
	transport = strings.TrimSpace(transport)
	name := ""
	if headerName != nil {
		name = strings.TrimSpace(*headerName)
	}
	if c.Mode != ConnectionModeMCPServer {
		if transport != "" && transport != mcpclient.TransportLegacyJSON {
			return connectionSettings{}, httpx.Unprocessable("mcp_transport 仅适用于 mcp_server 接入")
		}
		if name != "" {
			return connectionSettings{}, httpx.Unprocessable("endpoint_auth_header_name 仅适用于 mcp_server 接入")
		}
		c.MCPTransport = mcpclient.TransportLegacyJSON
		c.EndpointAuthHeaderName = nil
		return c, nil
	}
	transport = mcpclient.NormalizeTransport(transport)
	if !mcpclient.ValidTransport(transport) {
		return connectionSettings{}, httpx.Unprocessable("mcp_transport 不支持")
	}
	c.MCPTransport = transport
	c.EndpointAuthHeaderName = nil
	if name != "" {
		if err := mcpclient.ValidateAuthHeaderName(name); err != nil {
			return connectionSettings{}, httpx.Unprocessable(err.Error())
		}
		c.EndpointAuthHeaderName = &name
	}
	return c, nil
}

func normalizeConnectionSettings(slug, endpointURL, mode, mcpToolName string, allowLocalHTTP bool) (connectionSettings, error) {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = ConnectionModeDirectHTTP
	}
	endpointURL = strings.TrimSpace(endpointURL)
	mcpToolName = strings.TrimSpace(mcpToolName)

	switch mode {
	case ConnectionModeDirectHTTP:
		if endpointURL == "" {
			return connectionSettings{}, httpx.Unprocessable("direct_http 接入必须填写 endpoint_url")
		}
		if err := endpointurl.Validate(endpointURL, allowLocalHTTP); err != nil {
			return connectionSettings{}, httpx.Unprocessable(err.Error())
		}
		return connectionSettings{Mode: mode, EndpointURL: endpointURL}, nil
	case ConnectionModeMCPServer:
		if endpointURL == "" {
			return connectionSettings{}, httpx.Unprocessable("mcp_server 接入必须填写 MCP endpoint_url")
		}
		if mcpToolName == "" {
			return connectionSettings{}, httpx.Unprocessable("mcp_server 接入必须填写 mcp_tool_name")
		}
		if err := endpointurl.Validate(endpointURL, allowLocalHTTP); err != nil {
			return connectionSettings{}, httpx.Unprocessable(err.Error())
		}
		return connectionSettings{Mode: mode, EndpointURL: endpointURL, MCPToolName: &mcpToolName}, nil
	case ConnectionModeRuntime:
		if endpointURL == "" || !strings.HasPrefix(endpointURL, runtimeEndpointPrefix) {
			endpointURL = runtimeEndpointPrefix + slug
		}
		return connectionSettings{Mode: mode, EndpointURL: endpointURL}, nil
	default:
		return connectionSettings{}, httpx.Unprocessable("connection_mode 不支持")
	}
}
