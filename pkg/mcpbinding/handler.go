package mcpbinding

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
)

type Handler struct {
	pool         *pgxpool.Pool
	digestSecret string
}

func NewHandler(p *pgxpool.Pool, digestSecret string) *Handler { return &Handler{p, digestSecret} }

// Selecting tools grants execution authority and is owner JWT-only, like other
// privileged Agent settings. Public Agent/catalog reads never join these rows.
func (h *Handler) Register(api *echo.Group, jwt echo.MiddlewareFunc) {
	g := api.Group("/creator/agents/:id/mcp-bindings", jwt)
	g.GET("", h.List)
	g.GET("/candidates", h.Candidates)
	g.GET("/candidates/:targetId", h.Preview)
	g.PUT("/:targetId", h.Bind)
	g.DELETE("/:targetId", h.Unbind)
}
func identity(c echo.Context) (uuid.UUID, uuid.UUID, error) {
	c.Response().Header().Set("Cache-Control", "private, no-store")
	owner, err := uuid.Parse(httpx.UserIDFrom(c))
	if err != nil || owner == uuid.Nil {
		return uuid.Nil, uuid.Nil, httpx.Unauthorized("authentication required")
	}
	agent, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return owner, agent, httpx.BadRequest("invalid Agent")
	}
	return owner, agent, nil
}
func failure(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.NotFound("Agent or tool not found")
	}
	var he *httpx.HTTPError
	if errors.As(err, &he) {
		return he
	}
	return httpx.Internal("MCP configuration could not be saved")
}
func (h *Handler) List(c echo.Context) error {
	owner, agent, err := identity(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	var mode string
	if err = h.pool.QueryRow(ctx, `SELECT connection_mode FROM agents WHERE id=$1 AND creator_id=$2`, agent, owner).Scan(&mode); err != nil {
		return failure(err)
	}
	var raw json.RawMessage
	err = h.pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(b.tool || jsonb_build_object('binding_id',b.binding_id,'status',b.status,'last_run_id',b.last_run_id) ORDER BY b.target_agent_id),'[]'::jsonb) FROM agent_mcp_bindings b WHERE agent_id=$1`, agent).Scan(&raw)
	if err != nil {
		return failure(err)
	}
	var items []map[string]any
	if json.Unmarshal(raw, &items) != nil {
		return httpx.Internal("Invalid MCP configuration")
	}
	for _, item := range items {
		id, _ := uuid.Parse(item["target_agent_id"].(string))
		t, e := Resolve(ctx, h.pool, owner, id, h.digestSecret)
		item["needs_review"] = e != nil || t.Digest != item["digest"]
	}
	var known, supported bool
	err = h.pool.QueryRow(ctx, `SELECT count(*)>0,COALESCE(bool_or($2=ANY(features)),false) FROM runtime_sessions WHERE agent_id=$1 AND status NOT IN ('closed','revoked')`, agent, Feature).Scan(&known, &supported)
	if err != nil {
		return failure(err)
	}
	status := "offline"
	if known {
		status = "incompatible"
	}
	if supported && mode == "runtime" {
		status = "compatible"
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items, "host_status": status, "max_bindings": MaxBindings})
}
func (h *Handler) Candidates(c echo.Context) error {
	owner, agent, err := identity(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	var id uuid.UUID
	if err = h.pool.QueryRow(ctx, `SELECT id FROM agents WHERE id=$1 AND creator_id=$2`, agent, owner).Scan(&id); err != nil {
		return failure(err)
	}
	// Search is server-side; pagination never silently hides services after 200.
	rows, err := h.pool.Query(ctx, `SELECT a.id,a.name,a.description,a.slug,a.mcp_tool_name,a.creator_id=$1 AS owned FROM agents a LEFT JOIN agent_call_policies p ON p.agent_id=a.id
 WHERE a.connection_mode='mcp_server' AND a.lifecycle_status='active' AND a.id<>$2
 AND (a.creator_id=$1 OR (a.visibility='public' AND COALESCE(p.callable_by,'public')='public'))
 AND ($3='' OR a.name ILIKE '%'||$3||'%' OR a.slug ILIKE '%'||$3||'%') ORDER BY (a.creator_id=$1) DESC,a.name,a.id LIMIT 51`, owner, agent, c.QueryParam("q"))
	if err != nil {
		return failure(err)
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id uuid.UUID
		var name, desc, slug, tool string
		var owned bool
		if err = rows.Scan(&id, &name, &desc, &slug, &tool, &owned); err != nil {
			return failure(err)
		}
		items = append(items, map[string]any{"id": id, "name": name, "description": desc, "slug": slug, "tool_name": tool, "owned": owned})
	}
	if err = rows.Err(); err != nil {
		return failure(err)
	}
	more := len(items) > 50
	if more {
		items = items[:50]
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items, "has_more": more})
}

type editRequest struct {
	ExpectedToolDigest string `json:"expected_tool_digest"`
	Expected           string `json:"expected_binding_id"`
	Acknowledged       bool   `json:"acknowledge_tool_access"`
}

func decodeEdit(c echo.Context) (editRequest, error) {
	var r editRequest
	d := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, 4096))
	d.DisallowUnknownFields()
	if d.Decode(&r) != nil || d.Decode(new(any)) != io.EOF {
		return r, httpx.BadRequest("invalid MCP binding request")
	}
	return r, nil
}
func (h *Handler) Bind(c echo.Context) error   { return h.edit(c, false) }
func (h *Handler) Unbind(c echo.Context) error { return h.edit(c, true) }
func (h *Handler) edit(c echo.Context, remove bool) error {
	owner, agent, err := identity(c)
	if err != nil {
		return err
	}
	target, err := uuid.Parse(c.Param("targetId"))
	if err != nil || target == agent {
		return httpx.BadRequest("invalid MCP service")
	}
	req, err := decodeEdit(c)
	if err != nil {
		return err
	}
	if !remove && !req.Acknowledged {
		return httpx.BadRequest("Confirm that callers may use this tool through your Agent")
	}
	ctx := c.Request().Context()
	var result Tool
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		var mode string
		if e := tx.QueryRow(ctx, `SELECT connection_mode FROM agents WHERE id=$1 AND creator_id=$2 FOR UPDATE`, agent, owner).Scan(&mode); e != nil {
			return e
		}
		if !remove && mode != "runtime" {
			return httpx.BadRequest("Only Runtime Agents can load MCP tools")
		}
		var current string
		e := tx.QueryRow(ctx, `SELECT binding_id::text FROM agent_mcp_bindings WHERE agent_id=$1 AND target_agent_id=$2`, agent, target).Scan(&current)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		if current != req.Expected {
			return httpx.NewError(409, "MCP_BINDING_STALE", "MCP selection changed; refresh before saving")
		}
		if remove {
			_, e = tx.Exec(ctx, `DELETE FROM agent_mcp_bindings WHERE agent_id=$1 AND target_agent_id=$2`, agent, target)
			return e
		}
		var count int
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM agent_mcp_bindings WHERE agent_id=$1`, agent).Scan(&count); e != nil {
			return e
		}
		if current == "" && count >= MaxBindings {
			return httpx.NewError(400, "MCP_BINDING_LIMIT", "At most 16 MCP tools can be selected")
		}
		result, e = Resolve(ctx, tx, owner, target, h.digestSecret)
		if e != nil {
			return e
		}
		if result.Digest != req.ExpectedToolDigest {
			return httpx.NewError(409, "MCP_SERVICE_CHANGED", "MCP service changed; review its current tool before saving")
		}
		result.BindingID = uuid.New()
		raw, _ := json.Marshal(result)
		_, e = tx.Exec(ctx, `INSERT INTO agent_mcp_bindings(agent_id,target_agent_id,binding_id,owner_user_id,digest,tool) VALUES($1,$2,$3,$4,$5,$6)
 ON CONFLICT(agent_id,target_agent_id) DO UPDATE SET binding_id=EXCLUDED.binding_id,owner_user_id=EXCLUDED.owner_user_id,digest=EXCLUDED.digest,tool=EXCLUDED.tool,status='pending',last_run_id=NULL,updated_at=now()`, agent, target, result.BindingID, owner, result.Digest, raw)
		return e
	})
	if err != nil {
		return failure(err)
	}
	if remove {
		return c.NoContent(http.StatusNoContent)
	}
	return c.JSON(http.StatusOK, result)
}

func (h *Handler) Preview(c echo.Context) error {
	owner, agent, err := identity(c)
	if err != nil {
		return err
	}
	target, err := uuid.Parse(c.Param("targetId"))
	if err != nil {
		return httpx.BadRequest("Invalid MCP service")
	}
	ctx := c.Request().Context()
	var id uuid.UUID
	if err = h.pool.QueryRow(ctx, `SELECT id FROM agents WHERE id=$1 AND creator_id=$2`, agent, owner).Scan(&id); err != nil {
		return failure(err)
	}
	tool, err := Resolve(ctx, h.pool, owner, target, h.digestSecret)
	if err != nil {
		return failure(err)
	}
	return c.JSON(http.StatusOK, tool)
}
