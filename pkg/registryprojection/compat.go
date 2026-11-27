package registryprojection

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
)

var (
	legacyPackagePath = regexp.MustCompile(`^/api/v1/skill-packages/([0-9a-fA-F-]{36})(/|$)`)
	legacyServicePath = regexp.MustCompile(`^/api/v1/mcp-services/([^/]+)/metadata$`)
)

type mapping struct {
	Namespace string
	Name      string
	URL       string
}

func (p *Projector) skillMapping(ctx context.Context, id uuid.UUID) (mapping, bool, error) {
	var m mapping
	var registryURL string
	err := p.pool.QueryRow(ctx, `SELECT l.registry_url, l.registry_namespace, l.registry_entry FROM registry_projection_links l
		JOIN registry_projection_settings s ON s.mode = 'registry' AND s.registry_url = l.registry_url
		JOIN skill_packages pk ON pk.id = l.subject_id
		WHERE l.kind = 'skill' AND l.subject_id = $1 AND NOT l.applied_hidden AND l.registry_namespace <> ''
		AND pk.visibility IN ('public','unlisted')
		AND EXISTS (SELECT 1 FROM skill_package_versions v WHERE v.package_id = pk.id AND v.published_at IS NOT NULL)
		AND EXISTS (SELECT 1 FROM users u WHERE u.id = pk.owner_user_id AND u.deleted_at IS NULL AND u.disabled_at IS NULL)`, id).
		Scan(&registryURL, &m.Namespace, &m.Name)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, false, nil
	}
	if err != nil {
		return m, false, err
	}
	m.URL = registryURL + "/v1/skills/" + url.PathEscape(m.Namespace) + "/" + url.PathEscape(m.Name)
	return m, true, nil
}

func (p *Projector) serviceMapping(ctx context.Context, slug string) (mapping, bool, error) {
	var m mapping
	var registryURL string
	err := p.pool.QueryRow(ctx, `SELECT l.registry_url, l.registry_namespace, l.registry_entry FROM agents a
		JOIN registry_projection_links l ON l.kind = 'mcp' AND l.subject_id = a.id
		JOIN registry_projection_settings s ON s.mode = 'registry' AND s.registry_url = l.registry_url
		WHERE a.slug = $1 AND NOT l.applied_hidden AND l.registry_namespace <> '' AND `+serviceEligible, slug).
		Scan(&registryURL, &m.Namespace, &m.Name)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, false, nil
	}
	if err != nil {
		return m, false, err
	}
	m.URL = registryURL + "/v0.1/servers/" + url.PathEscape(m.Namespace+"/"+m.Name) + "/versions/latest"
	return m, true, nil
}

func (p *Projector) CompatMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			req := c.Request()
			if req.Method != http.MethodGet && req.Method != http.MethodHead {
				return next(c)
			}
			var m mapping
			found := false
			if sub := legacyPackagePath.FindStringSubmatch(req.URL.Path); sub != nil {
				if id, err := uuid.Parse(sub[1]); err == nil {
					m, found, _ = p.skillMapping(req.Context(), id)
				}
			} else if sub := legacyServicePath.FindStringSubmatch(req.URL.Path); sub != nil {
				m, found, _ = p.serviceMapping(req.Context(), sub[1])
			}
			if found {
				c.Response().Header().Set("Deprecation", "true")
				c.Response().Header().Set("Link", "<"+m.URL+">; rel=\"alternate\"")
			}
			return next(c)
		}
	}
}

func (p *Projector) RegisterLegacy(api *echo.Group) {
	g := api.Group("/catalog/legacy", func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Response().Header().Set("Cache-Control", "private, no-store")
			return next(c)
		}
	})
	g.GET("/skill-packages/:id", func(c echo.Context) error {
		id, err := uuid.Parse(c.Param("id"))
		if err != nil {
			return httpx.NotFound("no catalog entry for this package")
		}
		m, ok, err := p.skillMapping(c.Request().Context(), id)
		if err != nil {
			return httpx.Internal("catalog lookup failed")
		}
		if !ok {
			return httpx.NotFound("no catalog entry for this package")
		}
		return c.JSON(http.StatusOK, map[string]string{"registry": p.registry.Name, "kind": "skills", "namespace": m.Namespace, "name": m.Name})
	})
	g.GET("/mcp-services/:slug", func(c echo.Context) error {
		slug := strings.TrimSpace(c.Param("slug"))
		m, ok, err := p.serviceMapping(c.Request().Context(), slug)
		if err != nil {
			return httpx.Internal("catalog lookup failed")
		}
		if !ok {
			return httpx.NotFound("no catalog entry for this service")
		}
		return c.JSON(http.StatusOK, map[string]string{"registry": p.registry.Name, "kind": "mcp", "namespace": m.Namespace, "name": m.Name})
	})
}
