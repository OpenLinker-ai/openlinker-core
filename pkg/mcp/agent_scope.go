package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/inputschema"
)

// Agent-scoped MCP: POST /api/v1/mcp/agents/:agentId serves the same JSON-RPC
// handler and tool dispatch as /api/v1/mcp, restricted to one Agent and a fixed
// six-tool set. It does not add a second Runtime or SDK path.

const (
	maxScopedSchemaBytes      = 16 * 1024
	maxScopedDescriptionRunes = 300
	maxScopedNameRunes        = 120
)

var scopedToolNames = []string{"run_agent", "start_agent_run", "get_run", "list_run_events", "list_run_artifacts", "cancel_run"}

// ScopedAgent is the visible Agent identity behind a scoped endpoint.
type ScopedAgent struct {
	ID          uuid.UUID
	Slug        string
	Name        string
	Description string
	Visibility  string
	InputSchema map[string]interface{}
}

type agentScopeResolver interface {
	ResolveAgent(ctx context.Context, userID, agentID uuid.UUID) (*ScopedAgent, error)
}

// Directory reads Agent identities for scoped MCP endpoints and the public
// mcp_server catalog. It never returns endpoint URLs or credentials.
type Directory struct{ pool *pgxpool.Pool }

func NewDirectory(pool *pgxpool.Pool) *Directory { return &Directory{pool: pool} }

// ResolveAgent returns an active Agent that is public/unlisted or owned by the
// caller; every other state is the same 404.
func (d *Directory) ResolveAgent(ctx context.Context, userID, agentID uuid.UUID) (*ScopedAgent, error) {
	agent := &ScopedAgent{ID: agentID}
	var schema []byte
	err := d.pool.QueryRow(ctx, `SELECT a.slug,a.name,a.description,a.visibility,
 (SELECT c.input_schema FROM agent_capabilities c WHERE c.agent_id=a.id ORDER BY c.version DESC,c.updated_at DESC LIMIT 1)
 FROM agents a WHERE a.id=$1 AND a.lifecycle_status='active' AND (a.visibility IN ('public','unlisted') OR a.creator_id=$2)`, agentID, userID).
		Scan(&agent.Slug, &agent.Name, &agent.Description, &agent.Visibility, &schema)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询 Agent 失败")
	}
	if len(schema) > 0 && len(schema) <= maxScopedSchemaBytes {
		var parsed map[string]interface{}
		if json.Unmarshal(schema, &parsed) == nil && inputschema.ValidateInputSchema(parsed) == nil {
			agent.InputSchema = parsed
		}
	}
	return agent, nil
}

// ServiceListItem is one public mcp_server Agent in the MCP catalog.
type ServiceListItem struct {
	ID             string `json:"id"`
	Slug           string `json:"slug"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	ConnectionMode string `json:"connection_mode"`
	MCPToolName    string `json:"mcp_tool_name"`
}

type ServiceListResponse struct {
	Items []ServiceListItem `json:"items"`
	Total int64             `json:"total"`
	Page  int               `json:"page"`
	Size  int               `json:"size"`
}

// Filtering happens in SQL before pagination, with the market's hidden-tag rule.
const serviceCatalogFilter = ` FROM agents a WHERE a.visibility='public' AND a.lifecycle_status='active' AND a.connection_mode='mcp_server'
 AND NOT EXISTS (SELECT 1 FROM unnest(a.tags) AS tag WHERE lower(tag) IN ('internal','test','testing','validation') OR tag IN ('内部','测试','验收'))
 AND ($1='' OR a.slug ILIKE $2 ESCAPE '\' OR a.name ILIKE $2 ESCAPE '\' OR a.description ILIKE $2 ESCAPE '\')`

func (d *Directory) ListServices(ctx context.Context, q string, page, size int) (*ServiceListResponse, error) {
	pattern := "%" + strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(q) + "%"
	resp := &ServiceListResponse{Items: []ServiceListItem{}, Page: page, Size: size}
	if err := d.pool.QueryRow(ctx, `SELECT count(*)`+serviceCatalogFilter, q, pattern).Scan(&resp.Total); err != nil {
		return nil, httpx.Internal("查询 MCP 服务失败")
	}
	rows, err := d.pool.Query(ctx, `SELECT a.id::text,a.slug,a.name,a.description,a.connection_mode,COALESCE(a.mcp_tool_name,'')`+serviceCatalogFilter+
		` ORDER BY a.created_at DESC,a.id LIMIT $3 OFFSET $4`, q, pattern, size, (page-1)*size)
	if err != nil {
		return nil, httpx.Internal("查询 MCP 服务失败")
	}
	defer rows.Close()
	for rows.Next() {
		var item ServiceListItem
		if err := rows.Scan(&item.ID, &item.Slug, &item.Name, &item.Description, &item.ConnectionMode, &item.MCPToolName); err != nil {
			return nil, httpx.Internal("查询 MCP 服务失败")
		}
		resp.Items = append(resp.Items, item)
	}
	if rows.Err() != nil {
		return nil, httpx.Internal("查询 MCP 服务失败")
	}
	return resp, nil
}

// CatalogHandler serves GET /api/v1/mcp-services without authentication.
type CatalogHandler struct{ directory *Directory }

func NewCatalogHandler(directory *Directory) *CatalogHandler {
	return &CatalogHandler{directory: directory}
}

func (h *CatalogHandler) Register(api *echo.Group) {
	api.GET("/mcp-services", h.List)
}

func (h *CatalogHandler) List(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "no-store")
	q := strings.TrimSpace(c.QueryParam("q"))
	if utf8.RuneCountInString(q) > 200 {
		return httpx.BadRequest("q is too long")
	}
	page := boundedQueryInt(c.QueryParam("page"), 1, 10000)
	size := boundedQueryInt(c.QueryParam("size"), 12, 50)
	resp, err := h.directory.ListServices(c.Request().Context(), q, page, size)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func boundedQueryInt(raw string, fallback, limit int) int {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 1 {
		return fallback
	}
	return min(n, limit)
}

// SetAgentScopeResolver enables /mcp/agents/:agentId. Without it the scoped
// endpoint reports every Agent as not found.
func (h *Handler) SetAgentScopeResolver(resolver agentScopeResolver) {
	h.scopes = resolver
}

// GetAgentEndpointInfo describes the scoped endpoint without reading the Agent,
// so it cannot disclose private names or schemas. SSE streams are not offered.
func (h *Handler) GetAgentEndpointInfo(c echo.Context) error {
	if acceptsEventStream(c.Request()) {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	agentID, err := uuid.Parse(c.Param("agentId"))
	if err != nil {
		return httpx.NotFound("Agent 不存在")
	}
	return c.JSON(http.StatusOK, map[string]interface{}{
		"name":             "openlinker-agent-mcp",
		"transport":        "streamable_http_json_response",
		"protocol_version": mcpProtocolVersion,
		"endpoint":         "/api/v1/mcp/agents/" + agentID.String(),
		"auth":             "Authorization: Bearer ol_user_...",
		"methods":          []string{"initialize", "tools/list", "tools/call"},
		"tool_names":       scopedToolNames,
	})
}

// PostAgentRPC is the Agent-scoped JSON-RPC entry.
func (h *Handler) PostAgentRPC(c echo.Context) error {
	agentID, err := uuid.Parse(c.Param("agentId"))
	if err != nil {
		return writeRPCError(c, nil, http.StatusNotFound, -32000, "Agent 不存在")
	}
	return h.serveRPC(c, &agentID)
}

// resolveScope runs after User Token authentication. A token narrowed to other
// resources cannot read a private Agent's schema even though its owner can.
func (h *Handler) resolveScope(c echo.Context, agentID uuid.UUID) (*ScopedAgent, error) {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return nil, err
	}
	if h.scopes == nil {
		return nil, httpx.NotFound("Agent 不存在")
	}
	agent, err := h.scopes.ResolveAgent(c.Request().Context(), uid, agentID)
	if err != nil {
		return nil, err
	}
	if agent.Visibility != "public" && agent.Visibility != "unlisted" &&
		auth.RequirePermission(c, "agents:run", "agent", &agentID) != nil &&
		auth.RequirePermission(c, "agents:read", "agent", &agentID) != nil {
		return nil, httpx.NotFound("Agent 不存在")
	}
	return agent, nil
}

// bindScopedAgent injects the endpoint's Agent ID. A different explicit ID is
// a parameter error rather than a silent redirect.
func bindScopedAgent(req *RunAgentRequest, scope *ScopedAgent) *rpcError {
	if scope == nil {
		return nil
	}
	if req.AgentID != "" {
		if parsed, err := uuid.Parse(req.AgentID); err != nil || parsed != scope.ID {
			return &rpcError{Code: -32602, Message: "Invalid arguments: agent_id must be omitted or equal this endpoint's Agent ID"}
		}
	}
	req.AgentID = scope.ID.String()
	return nil
}

// requireRunInScope keeps the ordinary run authorization and then requires the
// run to belong to the endpoint's Agent, reporting a mismatch as not found.
func (h *Handler) requireRunInScope(c echo.Context, uid, runID uuid.UUID, scope *ScopedAgent) error {
	if scope == nil {
		return nil
	}
	resp, err := h.svc.GetRun(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	if resp == nil || resp.AgentID != scope.ID.String() {
		return httpx.NotFound("调用记录不存在")
	}
	return nil
}

func truncateRunes(value string, limit int) string {
	value = strings.TrimSpace(value)
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return strings.TrimSpace(string(runes[:limit])) + "…"
}

func scopedInstructions(agent *ScopedAgent) string {
	return "Tools in this server invoke only the OpenLinker Agent " + truncateRunes(agent.Name, maxScopedNameRunes) + " (" + agent.Slug + ")."
}

// scopedTools keeps input nested under `input`. The Agent's own schema is used
// only when it is a valid, bounded object schema.
func scopedTools(agent *ScopedAgent) []ToolDescriptor {
	input := map[string]interface{}{"type": "object", "description": "Input object sent to this Agent."}
	if agent.InputSchema != nil {
		if raw, err := json.Marshal(agent.InputSchema); err == nil && len(raw) <= maxScopedSchemaBytes && inputschema.ValidateInputSchema(agent.InputSchema) == nil {
			input = agent.InputSchema
		}
	}
	label := truncateRunes(agent.Name, maxScopedNameRunes) + " (" + agent.Slug + ")"
	about := ""
	if description := truncateRunes(agent.Description, maxScopedDescriptionRunes); description != "" {
		about = " Agent description: " + description
	}
	runSchema := func() map[string]interface{} {
		return map[string]interface{}{
			"type":     "object",
			"required": []string{"input", "idempotency_key"},
			"properties": map[string]interface{}{
				"agent_id":        map[string]interface{}{"type": "string", "format": "uuid", "enum": []string{agent.ID.String()}, "description": "Optional. When present it must equal this endpoint's Agent ID."},
				"input":           input,
				"metadata":        map[string]interface{}{"type": "object", "description": "Optional run metadata. Include task_id to associate the run with a task."},
				"idempotency_key": map[string]interface{}{"type": "string", "minLength": 1, "maxLength": 255, "pattern": "^[ -~]{1,255}$", "description": "Caller-generated printable ASCII key. Reuse it only when retrying the same request."},
			},
		}
	}
	tools := []ToolDescriptor{
		{
			Name:        "run_agent",
			Description: "Invoke " + label + ". If the returned run is still pending or running, use get_run or list_run_events to read its progress and result. Repeating an identical request with the same idempotency key returns the original run." + about,
			Annotations: toolAnnotations(false, true, true, true),
			InputSchema: runSchema(),
		},
		{
			Name:        "start_agent_run",
			Description: "Start " + label + " and return immediately with a run that can be inspected while it executes. Repeating an identical request with the same idempotency key returns the original run." + about,
			Annotations: toolAnnotations(false, true, true, true),
			InputSchema: runSchema(),
		},
	}
	for _, name := range scopedToolNames[2:] {
		for _, tool := range mcpTools {
			if tool.Name == name {
				tool.Description += " Only runs of " + label + " are accessible."
				tools = append(tools, tool)
			}
		}
	}
	return tools
}
