package catalogregistry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
)

type AgentCreator interface {
	CreateAgentWithin(ctx context.Context, creatorID uuid.UUID, req *agent.CreateAgentRequest, within func(context.Context, pgx.Tx, uuid.UUID) error) (*agent.AgentResponse, error)
}

type Handler struct {
	client   *Client
	pool     *pgxpool.Pool
	packages *skillpackage.Handler
	agents   AgentCreator
	now      func() time.Time
	limiter  writeLimiter
}

func NewHandler(client *Client, pool *pgxpool.Pool, packages *skillpackage.Handler, agents AgentCreator) *Handler {
	return &Handler{client: client, pool: pool, packages: packages, agents: agents, now: time.Now}
}

var (
	segmentPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	mcpNamespace      = regexp.MustCompile(`^[A-Za-z0-9.-]{1,198}$`)
	mcpName           = regexp.MustCompile(`^[A-Za-z0-9._-]{1,198}$`)
	mcpVersionPattern = regexp.MustCompile(`^[^\x00-\x1f\x7f]{1,255}$`)
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

const (
	maxQueryValue  = 200
	maxCursorValue = 4096
)

func catalogError(status int, code, message string) error {
	return httpx.NewError(status, httpx.ErrorCode(code), message)
}

func (h *Handler) Register(api *echo.Group, jwt echo.MiddlewareFunc) {
	if h == nil || h.client == nil || len(h.client.order) == 0 {
		return
	}
	optional := func(next echo.HandlerFunc) echo.HandlerFunc {
		withJWT := jwt(next)
		return func(c echo.Context) error {
			if c.Request().Header.Get("Authorization") != "" {
				return withJWT(c)
			}
			return next(c)
		}
	}
	g := api.Group("/catalog", optional, noStore)
	g.GET("/registries", h.listRegistries)
	g.GET("/registries/:registry/skills", h.listSkills)
	g.GET("/registries/:registry/skills/:ns/:name", h.getSkill)
	g.GET("/registries/:registry/skills/:ns/:name/versions/:version", h.getSkillVersion)
	g.GET("/registries/:registry/skills/:ns/:name/versions/:version/files/*", h.getSkillFile)
	g.GET("/registries/:registry/mcp/servers", h.listMCP)
	g.GET("/registries/:registry/mcp/servers/:ns/:name", h.getMCP)
	g.GET("/registries/:registry/mcp/servers/:ns/:name/versions/:version", h.getMCPVersion)
	g.GET("/registries/:registry/mcp/servers/:ns/:name/versions/:version/registration-draft", h.mcpDraft)
	g.POST("/registries/:registry/mcp/servers/:ns/:name/versions/:version/registrations", h.registerMCP)
	h.registerWrites(g)
	api.POST("/creator/skill-packages/registry-imports", h.importSkill, jwt, noStore)
}

func noStore(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "private, no-store")
		c.Response().Header().Set("X-Content-Type-Options", "nosniff")
		return next(c)
	}
}

type caller struct {
	user    *User
	admin   bool
	present bool
}

func (h *Handler) caller(c echo.Context) (caller, error) {
	raw := httpx.UserIDFrom(c)
	if raw == "" {
		return caller{}, nil
	}
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return caller{}, httpx.Unauthorized("authentication required")
	}
	var admin bool
	var provider, oauthID *string
	err = h.pool.QueryRow(c.Request().Context(), `SELECT is_admin,oauth_provider,oauth_id FROM users WHERE id=$1 AND disabled_at IS NULL`, id).Scan(&admin, &provider, &oauthID)
	if errors.Is(err, pgx.ErrNoRows) {
		return caller{}, httpx.Unauthorized("authentication required")
	}
	if err != nil {
		return caller{}, httpx.Internal("catalog caller lookup failed")
	}
	u := &User{ID: id}
	if provider != nil && *provider == "github" && oauthID != nil && githubIDPattern.MatchString(*oauthID) {
		u.GitHubID = *oauthID
	}
	return caller{user: u, admin: admin, present: true}, nil
}

var githubIDPattern = regexp.MustCompile(`^[1-9][0-9]{0,18}$`)

func allows(reg Registry, who caller) bool {
	switch reg.Audience {
	case AudiencePublic:
		return true
	case AudienceMembers:
		return who.present
	case AudienceAdmins:
		return who.present && who.admin
	}
	return false
}

func (h *Handler) registryFor(c echo.Context) (Registry, caller, error) {
	who, err := h.caller(c)
	if err != nil {
		return Registry{}, caller{}, err
	}
	reg, ok := h.client.Registry(c.Param("registry"))
	if !ok || !allows(reg, who) {
		return Registry{}, caller{}, httpx.NotFound("catalog not found")
	}
	return reg, who, nil
}

func (h *Handler) listRegistries(c echo.Context) error {
	who, err := h.caller(c)
	if err != nil {
		return err
	}
	items := []map[string]any{}
	for _, reg := range h.client.Registries() {
		if allows(reg, who) {
			items = append(items, map[string]any{"name": reg.Name, "audience": reg.Audience, "personalized": who.present && reg.CanSign()})
		}
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func rawParam(c echo.Context, name string) (string, error) {
	v := c.Param(name)
	if c.Request().URL.RawPath == "" {
		return v, nil
	}
	return url.PathUnescape(v)
}

func param(c echo.Context, name string, pattern *regexp.Regexp) (string, error) {
	v, err := rawParam(c, name)
	if err != nil || !pattern.MatchString(v) || v == "." || v == ".." {
		return "", httpx.BadRequest("invalid catalog path")
	}
	return v, nil
}

func segment(c echo.Context, name string) (string, error) {
	return param(c, name, segmentPattern)
}

func version(c echo.Context, kind string) (string, error) {
	if kind == "mcp/servers" {
		return param(c, "version", mcpVersionPattern)
	}
	return segment(c, "version")
}

func entryPath(c echo.Context, kind string) (string, string, string, error) {
	nsPattern, namePattern := segmentPattern, segmentPattern
	if kind == "mcp/servers" {
		nsPattern, namePattern = mcpNamespace, mcpName
	}
	ns, err := param(c, "ns", nsPattern)
	if err != nil {
		return "", "", "", err
	}
	name, err := param(c, "name", namePattern)
	if err != nil {
		return "", "", "", err
	}
	if kind == "mcp/servers" && len(ns)+1+len(name) > 200 {
		return "", "", "", httpx.BadRequest("invalid catalog path")
	}
	base := "/v1/" + kind + "/" + url.PathEscape(ns) + "/" + url.PathEscape(name)
	return ns, name, base, nil
}

func passQuery(c echo.Context, keys ...string) (string, error) {
	q := url.Values{}
	for _, k := range keys {
		v := c.QueryParam(k)
		if v == "" {
			continue
		}
		limit := maxQueryValue
		if k == "cursor" {
			limit = maxCursorValue
		}
		if len(v) > limit {
			return "", httpx.BadRequest("invalid catalog query")
		}
		q.Set(k, v)
	}
	if len(q) == 0 {
		return "", nil
	}
	return "?" + q.Encode(), nil
}

func stripRegistryURLs(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, item := range t {
			if (k == "artifact" || k == "archive") && item != nil {
				if m, ok := item.(map[string]any); ok {
					delete(m, "url")
				}
			}
			t[k] = stripRegistryURLs(t[k])
		}
	case []any:
		for i := range t {
			t[i] = stripRegistryURLs(t[i])
		}
	}
	return v
}

func upstreamError(err error) error {
	if errors.Is(err, ErrInvalidTarget) {
		return httpx.BadRequest("invalid catalog request")
	}
	return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog is unavailable right now")
}

func (h *Handler) proxyJSON(c echo.Context, target string) error {
	reg, who, err := h.registryFor(c)
	if err != nil {
		return err
	}
	res, err := h.client.GetCached(c.Request().Context(), reg.Name, target, who.user)
	if err != nil {
		return upstreamError(err)
	}
	return relayJSON(c, res)
}

func relayJSON(c echo.Context, res Response) error {
	switch res.Status {
	case http.StatusOK:
		var doc any
		if err := json.Unmarshal(res.Body, &doc); err != nil {
			return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog returned an invalid response")
		}
		return c.JSON(http.StatusOK, stripRegistryURLs(doc))
	case http.StatusNotFound:
		return httpx.NotFound("catalog entry not found")
	case http.StatusBadRequest:
		return httpx.BadRequest("invalid catalog request")
	case http.StatusGone:
		return catalogError(http.StatusGone, "CATALOG_ENTRY_YANKED", "this catalog entry was withdrawn")
	}
	return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog is unavailable right now")
}

func (h *Handler) listSkills(c echo.Context) error {
	query, err := passQuery(c, "q", "namespace", "mode", "compatible", "limit", "cursor")
	if err != nil {
		return err
	}
	return h.proxyJSON(c, "/v1/skills"+query)
}

func (h *Handler) getSkill(c echo.Context) error {
	_, _, base, err := entryPath(c, "skills")
	if err != nil {
		return err
	}
	return h.proxyJSON(c, base)
}

func (h *Handler) getSkillVersion(c echo.Context) error {
	_, _, base, err := entryPath(c, "skills")
	if err != nil {
		return err
	}
	version, err := segment(c, "version")
	if err != nil {
		return err
	}
	return h.proxyJSON(c, base+"/versions/"+url.PathEscape(version))
}

func (h *Handler) getSkillFile(c echo.Context) error {
	reg, who, err := h.registryFor(c)
	if err != nil {
		return err
	}
	_, _, base, err := entryPath(c, "skills")
	if err != nil {
		return err
	}
	version, err := segment(c, "version")
	if err != nil {
		return err
	}
	file, err := rawParam(c, "*")
	if err != nil || !safePath(file) || len(file) > 180 {
		return httpx.BadRequest("invalid file path")
	}
	parts := strings.Split(file, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	res, err := h.client.GetCached(c.Request().Context(), reg.Name, base+"/versions/"+url.PathEscape(version)+"/files/"+strings.Join(parts, "/"), who.user)
	if err != nil {
		return upstreamError(err)
	}
	switch res.Status {
	case http.StatusOK:
		c.Response().Header().Set("Content-Security-Policy", "sandbox; default-src 'none'")
		return c.Blob(http.StatusOK, "text/plain; charset=utf-8", res.Body)
	case http.StatusForbidden:
		var body struct {
			Error struct {
				Code      string `json:"code"`
				SourceURL string `json:"source_url"`
			} `json:"error"`
		}
		_ = json.Unmarshal(res.Body, &body)
		return c.JSON(http.StatusForbidden, map[string]any{"error": map[string]any{"code": "CATALOG_CONTENT_WITHHELD",
			"message": "this file is not redistributed by the catalog; open the source instead", "reason": body.Error.Code, "source_url": body.Error.SourceURL}})
	}
	return relayJSON(c, res)
}

func (h *Handler) listMCP(c echo.Context) error {
	query, err := passQuery(c, "q", "namespace", "mode", "limit", "cursor")
	if err != nil {
		return err
	}
	return h.proxyJSON(c, "/v1/mcp/servers"+query)
}

func (h *Handler) getMCP(c echo.Context) error {
	_, _, base, err := entryPath(c, "mcp/servers")
	if err != nil {
		return err
	}
	return h.proxyJSON(c, base)
}

func (h *Handler) getMCPVersion(c echo.Context) error {
	_, _, base, err := entryPath(c, "mcp/servers")
	if err != nil {
		return err
	}
	v, err := version(c, "mcp/servers")
	if err != nil {
		return err
	}
	return h.proxyJSON(c, base+"/versions/"+url.PathEscape(v))
}

type importRequest struct {
	Registry         string   `json:"registry"`
	Namespace        string   `json:"namespace"`
	Name             string   `json:"name"`
	Version          string   `json:"version"`
	Digest           string   `json:"digest"`
	Providers        []string `json:"providers"`
	CapabilityIDs    []string `json:"capability_ids"`
	RequiredCommands []string `json:"required_commands"`
}

type versionDetail struct {
	Mode           string          `json:"mode"`
	Yanked         json.RawMessage `json:"yanked"`
	CoreCompatible bool            `json:"core_compatible"`
	Artifact       struct {
		Type   string `json:"type"`
		Digest string `json:"digest"`
	} `json:"artifact"`
	License struct {
		Kind     string `json:"kind"`
		ID       string `json:"id"`
		Approved bool   `json:"approved"`
	} `json:"license"`
}

func decodeBody(c echo.Context, out any) error {
	dec := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil || dec.More() {
		return httpx.BadRequest("invalid or oversized request")
	}
	return nil
}

func licenseLabel(kind, id string) string {
	switch kind {
	case "spdx":
		return "spdx:" + id
	case "custom":
		return "custom"
	}
	return "unknown"
}

func (h *Handler) importSkill(c echo.Context) error {
	var req importRequest
	if err := decodeBody(c, &req); err != nil {
		return err
	}
	who, err := h.caller(c)
	if err != nil {
		return err
	}
	if !who.present {
		return httpx.Unauthorized("authentication required")
	}
	reg, ok := h.client.Registry(req.Registry)
	if !ok || !allows(reg, who) {
		return httpx.NotFound("catalog not found")
	}
	if !segmentPattern.MatchString(req.Namespace) || !segmentPattern.MatchString(req.Name) || !segmentPattern.MatchString(req.Version) || !digestPattern.MatchString(req.Digest) {
		return catalogError(http.StatusBadRequest, "SKILL_PACKAGE_REQUEST_INVALID", "registry, namespace, name, version and a sha256 digest are required")
	}
	ctx := c.Request().Context()
	base := "/v1/skills/" + url.PathEscape(req.Namespace) + "/" + url.PathEscape(req.Name) + "/versions/" + url.PathEscape(req.Version)
	res, err := h.client.Get(ctx, reg.Name, base, who.user, []string{"read"})
	if err != nil {
		return upstreamError(err)
	}
	if res.Status == http.StatusNotFound {
		return httpx.NotFound("catalog entry not found")
	}
	if res.Status != http.StatusOK {
		return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog is unavailable right now")
	}
	var detail versionDetail
	if err := json.Unmarshal(res.Body, &detail); err != nil {
		return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog returned an invalid response")
	}
	if len(detail.Yanked) > 0 && string(detail.Yanked) != "null" {
		return catalogError(http.StatusConflict, "SKILL_PACKAGE_SOURCE_YANKED", "this version was withdrawn from the catalog")
	}
	if !detail.CoreCompatible {
		return catalogError(http.StatusUnprocessableEntity, "SKILL_PACKAGE_INCOMPATIBLE", "this catalog entry does not meet the Core package limits")
	}
	if detail.Artifact.Digest != req.Digest {
		return catalogError(http.StatusConflict, "SKILL_PACKAGE_DIGEST_MISMATCH", "the catalog version does not match the expected digest")
	}
	var acts []string
	if reg.CanSign() {
		acts = []string{"read", "import"}
	}
	art, err := h.client.GetArtifact(ctx, reg.Name, base+"/artifact?purpose=import", who.user, acts)
	if err != nil {
		return upstreamError(err)
	}
	switch art.Status {
	case http.StatusOK:
	case http.StatusForbidden:
		return catalogError(http.StatusConflict, "SKILL_PACKAGE_SOURCE_UNAVAILABLE", "the catalog cannot hand this content to this Core; import it from its source")
	case http.StatusConflict:
		return catalogError(http.StatusConflict, "SKILL_PACKAGE_SOURCE_CHANGED", "the source of this catalog entry changed or is unavailable")
	case http.StatusGone:
		return catalogError(http.StatusConflict, "SKILL_PACKAGE_SOURCE_YANKED", "this version was withdrawn from the catalog")
	case http.StatusNotFound:
		return httpx.NotFound("catalog entry not found")
	default:
		return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog is unavailable right now")
	}
	sum := sha256.Sum256(art.Body)
	if "sha256:"+hex.EncodeToString(sum[:]) != req.Digest {
		return catalogError(http.StatusConflict, "SKILL_PACKAGE_DIGEST_MISMATCH", "the catalog returned bytes that do not match the digest")
	}
	decoded, err := DecodeArtifact(detail.Artifact.Type, art.Body)
	if err != nil {
		return catalogError(http.StatusUnprocessableEntity, "SKILL_PACKAGE_INCOMPATIBLE", "this catalog entry is not a valid Core skill package")
	}
	ir := skillpackage.ImportRequest{Version: req.Version, Files: decoded.Files, Providers: req.Providers, CapabilityIDs: req.CapabilityIDs, RequiredCommands: req.RequiredCommands}
	if len(ir.Providers) == 0 {
		ir.Providers = decoded.Providers
	}
	if len(ir.CapabilityIDs) == 0 {
		ir.CapabilityIDs = decoded.CapabilityIDs
	}
	if len(ir.RequiredCommands) == 0 {
		ir.RequiredCommands = decoded.RequiredCommands
	}
	mode := detail.Mode
	if mode != "hosted" && mode != "indexed" {
		mode = "indexed"
	}
	src := skillpackage.RegistrySource{URL: reg.URL, Entry: req.Namespace + "/" + req.Name, Version: req.Version, Digest: req.Digest, Mode: mode,
		License:         licenseLabel(detail.License.Kind, detail.License.ID),
		Redistributable: detail.License.Kind == "spdx" || (detail.License.Kind == "custom" && detail.License.Approved), CheckedAt: h.now().UTC()}
	result, err := h.packages.ImportFromRegistry(ctx, who.user.ID, ir, src)
	if err != nil {
		return err
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	return c.JSON(status, map[string]any{"id": result.PackageID, "version_id": result.VersionID, "digest": result.Digest,
		"source": map[string]any{"registry": reg.Name, "entry": src.Entry, "version": src.Version, "license": src.License, "redistributable": src.Redistributable}})
}
