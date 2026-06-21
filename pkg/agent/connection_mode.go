package agent

import (
	"strings"

	"github.com/kinzhi/openlinker-core/pkg/endpointurl"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const (
	ConnectionModeDirectHTTP  = "direct_http"
	ConnectionModeMCPServer   = "mcp_server"
	ConnectionModeRuntimePull = "runtime_pull"
	ConnectionModeRuntimeWS   = "runtime_ws"
)

const runtimePullEndpointPrefix = "openlinker-runtime-pull://"
const runtimeWSEndpointPrefix = "openlinker-runtime-ws://"

type connectionSettings struct {
	Mode        string
	EndpointURL string
	MCPToolName *string
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
	case ConnectionModeRuntimePull:
		if endpointURL == "" || !strings.HasPrefix(endpointURL, runtimePullEndpointPrefix) {
			endpointURL = runtimePullEndpointPrefix + slug
		}
		return connectionSettings{Mode: mode, EndpointURL: endpointURL}, nil
	case ConnectionModeRuntimeWS:
		if endpointURL == "" || !strings.HasPrefix(endpointURL, runtimeWSEndpointPrefix) {
			endpointURL = runtimeWSEndpointPrefix + slug
		}
		return connectionSettings{Mode: mode, EndpointURL: endpointURL}, nil
	default:
		return connectionSettings{}, httpx.Unprocessable("connection_mode 不支持")
	}
}
