package catalogregistry

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

const (
	maxWriteJSON     = 64 << 10
	maxWriteUpload   = 8 << 20
	writesPerMinute  = 30
	writeLimitWindow = time.Minute
)

var (
	uploadTypes   = []string{"text/markdown", "application/x-tar", "application/gzip", "application/zip", "application/vnd.openlinker.skill-bundle.v1+json"}
	registryCode  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	relayedStatus = map[int]bool{400: true, 403: true, 404: true, 409: true, 413: true, 415: true, 422: true, 429: true}
)

type writeLimiter struct {
	mu      sync.Mutex
	windows map[uuid.UUID][]time.Time
}

func (l *writeLimiter) allow(user uuid.UUID, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.windows == nil {
		l.windows = map[uuid.UUID][]time.Time{}
	}
	kept := l.windows[user][:0]
	for _, at := range l.windows[user] {
		if now.Sub(at) < writeLimitWindow {
			kept = append(kept, at)
		}
	}
	if len(kept) >= writesPerMinute {
		l.windows[user] = kept
		return false
	}
	l.windows[user] = append(kept, now)
	return true
}

func (h *Handler) registerWrites(g *echo.Group) {
	r := "/registries/:registry"
	g.GET(r+"/me", h.me)
	g.POST(r+"/namespaces", h.write(func(c echo.Context) (string, error) { return "/v1/namespaces", nil }, false))
	g.POST(r+"/reports", h.write(func(c echo.Context) (string, error) { return "/v1/reports", nil }, false))
	g.POST(r+"/skills/:ns/:name/versions", h.write(h.uploadTarget, true))
	for _, action := range []string{"github-imports/preview", "github-imports", "indexes/preview", "indexes"} {
		action := action
		g.POST(r+"/skills/:ns/"+action, h.write(func(c echo.Context) (string, error) {
			ns, err := segment(c, "ns")
			if err != nil {
				return "", err
			}
			return "/v1/skills/" + url.PathEscape(ns) + "/" + action, nil
		}, false))
	}
	for _, action := range []string{"deprecate", "yank", "claim"} {
		action := action
		g.POST(r+"/skills/:ns/:name/"+action, h.write(func(c echo.Context) (string, error) {
			_, _, base, err := entryPath(c, "skills")
			return base + "/" + action, err
		}, false))
	}
	g.POST(r+"/skills/:ns/:name/versions/:version/yank", h.write(func(c echo.Context) (string, error) {
		_, _, base, err := entryPath(c, "skills")
		if err != nil {
			return "", err
		}
		v, err := version(c, "skills")
		return base + "/versions/" + url.PathEscape(v) + "/yank", err
	}, false))
	g.PATCH(r+"/skills/:ns/:name", h.write(func(c echo.Context) (string, error) {
		_, _, base, err := entryPath(c, "skills")
		return base, err
	}, false))
	g.POST(r+"/mcp/servers/:ns", h.write(func(c echo.Context) (string, error) {
		ns, err := param(c, "ns", mcpNamespace)
		if err != nil {
			return "", err
		}
		query, err := passQuery(c, "visibility", "listed", "indexed")
		return "/v1/mcp/servers/" + url.PathEscape(ns) + query, err
	}, false))
	g.POST(r+"/mcp/servers/:ns/:name/status", h.write(func(c echo.Context) (string, error) {
		_, _, base, err := entryPath(c, "mcp/servers")
		return base + "/status", err
	}, false))
	g.POST(r+"/mcp/servers/:ns/:name/versions/:version/status", h.write(func(c echo.Context) (string, error) {
		_, _, base, err := entryPath(c, "mcp/servers")
		if err != nil {
			return "", err
		}
		v, err := version(c, "mcp/servers")
		return base + "/versions/" + url.PathEscape(v) + "/status", err
	}, false))
	g.PATCH(r+"/mcp/servers/:ns/:name", h.write(func(c echo.Context) (string, error) {
		_, _, base, err := entryPath(c, "mcp/servers")
		return base, err
	}, false))
}

func (h *Handler) uploadTarget(c echo.Context) (string, error) {
	_, _, base, err := entryPath(c, "skills")
	if err != nil {
		return "", err
	}
	query, err := passQuery(c, "version", "visibility", "listed", "license")
	return base + "/versions" + query, err
}

func (h *Handler) writer(c echo.Context) (Registry, *User, error) {
	reg, who, err := h.registryFor(c)
	if err != nil {
		return Registry{}, nil, err
	}
	if !reg.Writes || !reg.CanSign() {
		return Registry{}, nil, httpx.NotFound("catalog writes are not enabled")
	}
	if !who.present {
		return Registry{}, nil, httpx.Unauthorized("authentication required")
	}
	return reg, who.user, nil
}

func (h *Handler) write(target func(echo.Context) (string, error), upload bool) echo.HandlerFunc {
	return func(c echo.Context) error {
		reg, user, err := h.writer(c)
		if err != nil {
			return err
		}
		path, err := target(c)
		if err != nil {
			return err
		}
		contentType := "application/json"
		limit := int64(maxWriteJSON)
		if upload {
			mediaType, _, perr := mime.ParseMediaType(c.Request().Header.Get("Content-Type"))
			if perr != nil || !contains(uploadTypes, mediaType) {
				return catalogError(http.StatusUnsupportedMediaType, "CATALOG_CONTENT_TYPE", "unsupported skill upload type")
			}
			contentType, limit = mediaType, maxWriteUpload
		} else if ct := c.Request().Header.Get("Content-Type"); ct != "" {
			mediaType, _, perr := mime.ParseMediaType(ct)
			if perr != nil || mediaType != "application/json" {
				return catalogError(http.StatusUnsupportedMediaType, "CATALOG_CONTENT_TYPE", "expected application/json")
			}
		}
		body, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, limit))
		if err != nil {
			var mbe *http.MaxBytesError
			if errors.As(err, &mbe) {
				return catalogError(http.StatusRequestEntityTooLarge, "CATALOG_BODY_TOO_LARGE", "request body too large")
			}
			return httpx.BadRequest("request body could not be read")
		}
		if !upload && len(body) > 0 && !json.Valid(body) {
			return httpx.BadRequest("invalid JSON body")
		}
		if !h.limiter.allow(user.ID, h.now()) {
			c.Response().Header().Set("Retry-After", "60")
			return catalogError(http.StatusTooManyRequests, "CATALOG_RATE_LIMITED", "too many catalog writes; retry later")
		}
		res, err := h.client.Send(c.Request().Context(), reg.Name, c.Request().Method, path, contentType, body, user)
		if err != nil {
			return upstreamError(err)
		}
		return relayWrite(c, res)
	}
}

func contains(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

func relayWrite(c echo.Context, res Response) error {
	if res.Status >= 200 && res.Status < 300 {
		var doc any
		if err := json.Unmarshal(res.Body, &doc); err != nil {
			return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog returned an invalid response")
		}
		return c.JSON(res.Status, stripRegistryURLs(doc))
	}
	if !relayedStatus[res.Status] {
		return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog is unavailable right now")
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(res.Body, &body)
	code := "CATALOG_REQUEST_REFUSED"
	if registryCode.MatchString(body.Error.Code) {
		code = "CATALOG_" + strings.ToUpper(body.Error.Code)
	}
	message := body.Error.Message
	if message == "" || len(message) > 300 {
		message = http.StatusText(res.Status)
	}
	if res.Status == http.StatusTooManyRequests {
		if wait := res.Header.Get("Retry-After"); wait != "" {
			c.Response().Header().Set("Retry-After", wait)
		}
	}
	return catalogError(res.Status, code, message)
}

func (h *Handler) me(c echo.Context) error {
	reg, user, err := h.writer(c)
	if err != nil {
		return err
	}
	res, err := h.client.Get(c.Request().Context(), reg.Name, "/v1/whoami", user, []string{"read"})
	if err != nil {
		return upstreamError(err)
	}
	if res.Status != http.StatusOK {
		return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog is unavailable right now")
	}
	var who struct {
		Mode string `json:"mode"`
		User *struct {
			GitHubID string            `json:"github_id"`
			Roles    map[string]string `json:"roles"`
		} `json:"user"`
	}
	if err := json.Unmarshal(res.Body, &who); err != nil || who.User == nil {
		return catalogError(http.StatusBadGateway, "CATALOG_UNAVAILABLE", "the catalog returned an invalid response")
	}
	names := make([]string, 0, len(who.User.Roles))
	for ns := range who.User.Roles {
		names = append(names, ns)
	}
	sort.Strings(names)
	namespaces := make([]map[string]string, 0, len(names))
	for _, ns := range names {
		namespaces = append(namespaces, map[string]string{"name": ns, "role": who.User.Roles[ns]})
	}
	return c.JSON(http.StatusOK, map[string]any{"writes": true, "mode": who.Mode, "github": who.User.GitHubID != "", "namespaces": namespaces})
}
