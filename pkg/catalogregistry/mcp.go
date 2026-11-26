package catalogregistry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
)

type serverDocument struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Version     string `json:"version"`
	Remotes     []struct {
		Type    string `json:"type"`
		URL     string `json:"url"`
		Headers []struct {
			Name     string `json:"name"`
			IsSecret bool   `json:"isSecret"`
		} `json:"headers"`
	} `json:"remotes"`
	Packages []json.RawMessage `json:"packages"`
}

type mcpVersion struct {
	Version string          `json:"version"`
	Status  string          `json:"status"`
	Server  json.RawMessage `json:"server"`
}

type Remote struct {
	Type      string `json:"type"`
	URL       string `json:"url"`
	Transport string `json:"mcp_transport"`
}

type Draft struct {
	Registry               string   `json:"registry"`
	Server                 string   `json:"server"`
	Version                string   `json:"version"`
	Status                 string   `json:"status"`
	Name                   string   `json:"name"`
	Slug                   string   `json:"slug"`
	Description            string   `json:"description"`
	ConnectionMode         string   `json:"connection_mode"`
	EndpointURL            string   `json:"endpoint_url"`
	MCPTransport           string   `json:"mcp_transport"`
	EndpointAuthHeaderName string   `json:"endpoint_auth_header_name"`
	Remotes                []Remote `json:"remotes"`
}

var credentialHeader = regexp.MustCompile(`(?i)(authorization|api[-_]?key|token|secret|password)`)
var slugUnsafe = regexp.MustCompile(`[^a-z0-9-]+`)

func transportFor(kind string) string {
	switch kind {
	case "streamable-http":
		return "streamable_http"
	case "sse":
		return "legacy_sse"
	}
	return ""
}

func suggestSlug(server string) string {
	_, name, _ := strings.Cut(server, "/")
	s := strings.Trim(slugUnsafe.ReplaceAllString(strings.ToLower(name), "-"), "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	if len(s) > 80 {
		s = strings.Trim(s[:80], "-")
	}
	for len(s) < 3 {
		s += "-mcp"
		s = strings.Trim(s, "-")
	}
	return s
}

func BuildDraft(registry string, v mcpVersion, remoteIndex int) (Draft, error) {
	var doc serverDocument
	if err := json.Unmarshal(v.Server, &doc); err != nil {
		return Draft{}, catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog returned an invalid server document")
	}
	if v.Status == "deprecated" || v.Status == "deleted" {
		return Draft{}, catalogError(http.StatusConflict, "CATALOG_MCP_INACTIVE", "this MCP server version is "+v.Status+" in the catalog; it cannot be registered")
	}
	d := Draft{Registry: registry, Server: doc.Name, Version: v.Version, Status: v.Status, ConnectionMode: "mcp_server", Slug: suggestSlug(doc.Name)}
	d.Name = strings.TrimSpace(doc.Title)
	if d.Name == "" {
		_, d.Name, _ = strings.Cut(doc.Name, "/")
	}
	if len([]rune(d.Name)) > 80 {
		d.Name = string([]rune(d.Name)[:80])
	}
	for len([]rune(d.Name)) < 3 {
		d.Name += " MCP"
	}
	d.Description = doc.Description
	if len([]rune(d.Description)) > 500 {
		d.Description = string([]rune(d.Description)[:500])
	}
	usable := -1
	for i, r := range doc.Remotes {
		t := transportFor(r.Type)
		if t == "" {
			continue
		}
		d.Remotes = append(d.Remotes, Remote{Type: r.Type, URL: r.URL, Transport: t})
		if (remoteIndex < 0 && usable < 0) || i == remoteIndex {
			usable = i
		}
	}
	if len(d.Remotes) == 0 {
		if len(doc.Packages) > 0 {
			return Draft{}, catalogError(http.StatusUnprocessableEntity, "CATALOG_MCP_SELF_HOST_REQUIRED", "this MCP server is only published as a package; host it yourself, then register its address")
		}
		return Draft{}, catalogError(http.StatusUnprocessableEntity, "CATALOG_MCP_NO_REMOTE", "this MCP server has no remote endpoint OpenLinker can call")
	}
	if usable < 0 {
		return Draft{}, catalogError(http.StatusBadRequest, "CATALOG_MCP_REMOTE_INVALID", "the selected remote is not a supported transport")
	}
	remote := doc.Remotes[usable]
	if strings.ContainsAny(remote.URL, "{}") {
		return Draft{}, catalogError(http.StatusUnprocessableEntity, "CATALOG_MCP_TEMPLATE_URL", "this remote endpoint needs values filled into its address; register it manually")
	}
	u, err := url.Parse(remote.URL)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return Draft{}, catalogError(http.StatusUnprocessableEntity, "CATALOG_MCP_REMOTE_INVALID", "this remote endpoint is not a usable URL")
	}
	d.EndpointURL = remote.URL
	d.MCPTransport = transportFor(remote.Type)
	for _, hd := range remote.Headers {
		if hd.IsSecret || credentialHeader.MatchString(hd.Name) {
			d.EndpointAuthHeaderName = hd.Name
			break
		}
	}
	return d, nil
}

func remoteIndex(c echo.Context) (int, error) {
	raw := c.QueryParam("remote")
	if raw == "" {
		return -1, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > 32 {
		return 0, httpx.BadRequest("invalid remote")
	}
	return n, nil
}

func (h *Handler) loadDraft(ctx context.Context, c echo.Context, remote int) (Draft, Registry, caller, error) {
	reg, who, err := h.registryFor(c)
	if err != nil {
		return Draft{}, Registry{}, caller{}, err
	}
	if !who.present {
		return Draft{}, Registry{}, caller{}, httpx.Unauthorized("authentication required")
	}
	_, _, base, err := entryPath(c, "mcp/servers")
	if err != nil {
		return Draft{}, Registry{}, caller{}, err
	}
	v, err := version(c, "mcp/servers")
	if err != nil {
		return Draft{}, Registry{}, caller{}, err
	}
	res, err := h.client.Get(ctx, reg.Name, base+"/versions/"+url.PathEscape(v), who.user, []string{"read"})
	if err != nil {
		return Draft{}, Registry{}, caller{}, upstreamError(err)
	}
	if res.Status == http.StatusNotFound {
		return Draft{}, Registry{}, caller{}, httpx.NotFound("catalog entry not found")
	}
	if res.Status != http.StatusOK {
		return Draft{}, Registry{}, caller{}, catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog is unavailable right now")
	}
	var doc mcpVersion
	if err := json.Unmarshal(res.Body, &doc); err != nil {
		return Draft{}, Registry{}, caller{}, catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog returned an invalid response")
	}
	d, err := BuildDraft(reg.Name, doc, remote)
	return d, reg, who, err
}

func (h *Handler) mcpDraft(c echo.Context) error {
	remote, err := remoteIndex(c)
	if err != nil {
		return err
	}
	d, _, _, err := h.loadDraft(c.Request().Context(), c, remote)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, d)
}

type registrationRequest struct {
	Remote             *int     `json:"remote"`
	Slug               string   `json:"slug"`
	Name               string   `json:"name"`
	Description        *string  `json:"description"`
	Tags               []string `json:"tags"`
	MCPToolName        string   `json:"mcp_tool_name"`
	EndpointAuthHeader string   `json:"endpoint_auth_header"`
	Visibility         string   `json:"visibility"`
}

func (h *Handler) registerMCP(c echo.Context) error {
	if httpx.UserIDFrom(c) == "" {
		return httpx.Unauthorized("authentication required")
	}
	var req registrationRequest
	if err := decodeBody(c, &req); err != nil {
		return err
	}
	remote := -1
	if req.Remote != nil {
		if *req.Remote < 0 || *req.Remote > 32 {
			return httpx.BadRequest("invalid remote")
		}
		remote = *req.Remote
	}
	ctx := c.Request().Context()
	d, reg, who, err := h.loadDraft(ctx, c, remote)
	if err != nil {
		return err
	}
	description := d.Description
	if req.Description != nil {
		description = *req.Description
	}
	create := &agent.CreateAgentRequest{Slug: req.Slug, Name: req.Name, Description: description, EndpointURL: d.EndpointURL,
		EndpointAuthHeader: req.EndpointAuthHeader, Tags: req.Tags, Visibility: req.Visibility, ConnectionMode: "mcp_server",
		MCPToolName: req.MCPToolName, MCPTransport: d.MCPTransport, EndpointAuthHeaderName: d.EndpointAuthHeaderName}
	if err := validateCreate(create); err != nil {
		return err
	}
	resp, err := h.agents.CreateAgentWithin(ctx, who.user.ID, create, func(ctx context.Context, tx pgx.Tx, agentID uuid.UUID) error {
		_, err := tx.Exec(ctx, `INSERT INTO agent_catalog_sources(agent_id,registry_url,server_name,server_version) VALUES($1,$2,$3,$4)`, agentID, reg.URL, d.Server, d.Version)
		return err
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, map[string]any{"agent": resp, "source": map[string]any{"registry": reg.Name, "server": d.Server, "version": d.Version}})
}

var createValidator = validator.New(validator.WithRequiredStructEnabled())

func validateCreate(req *agent.CreateAgentRequest) error {
	if err := createValidator.Struct(req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	return nil
}
