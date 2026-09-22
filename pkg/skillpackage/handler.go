package skillpackage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
)

type Handler struct{ pool *pgxpool.Pool }

func NewHandler(pool *pgxpool.Pool) *Handler { return &Handler{pool: pool} }
func (h *Handler) Register(api *echo.Group, auth echo.MiddlewareFunc) {
	g := api.Group("/creator", auth)
	g.GET("/skill-packages", h.List)
	g.POST("/skill-packages", h.Import)
	g.GET("/skill-packages/:id", h.Detail)
	g.POST("/skill-packages/:id/versions", h.Import)
	g.GET("/skill-packages/:id/versions/:versionId", h.Version)
	g.GET("/agents/:id/skill-packages", h.Bindings)
	g.PUT("/agents/:id/skill-packages/:packageId", h.Bind)
	g.DELETE("/agents/:id/skill-packages/:packageId", h.Unbind)
}

func owner(c echo.Context) (uuid.UUID, error) {
	id, parseErr := uuid.Parse(httpx.UserIDFrom(c))
	if parseErr != nil || id == uuid.Nil {
		return uuid.Nil, httpx.Unauthorized("authentication required")
	}
	c.Response().Header().Set("Cache-Control", "private, no-store")
	return id, nil
}
func parameter(c echo.Context, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(c.Param(name))
	if err != nil {
		return uuid.Nil, httpx.BadRequest("invalid identifier")
	}
	return id, nil
}
func readRequest(c echo.Context, out any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, 128*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return packageError("REQUEST_INVALID", "invalid or oversized request")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return packageError("REQUEST_INVALID", "expected one JSON object")
	}
	return nil
}
func packageError(code, message string, status ...int) error {
	n := http.StatusBadRequest
	if len(status) > 0 {
		n = status[0]
	}
	return httpx.NewError(n, httpx.ErrorCode("SKILL_PACKAGE_"+code), message)
}

func databaseError(err error) error {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return httpx.NewError(http.StatusBadRequest, httpx.ErrorCode(ve.Code), ve.Message)
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		return packageError("VERSION_CONFLICT", "this version already exists; use a new version", http.StatusConflict)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.NotFound("resource not found")
	}
	var he *httpx.HTTPError
	if errors.As(err, &he) {
		return err
	}
	return httpx.Internal("skill package operation failed")
}

const packageJSON = `jsonb_build_object('id',p.id,'name',p.name,'description',p.description,'created_at',p.created_at,'updated_at',p.updated_at,
 'versions',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',v.id,'version',v.version,'digest',v.digest,
 'capability_ids',v.capability_ids,'providers',v.providers,'created_at',v.created_at) ORDER BY v.created_at DESC,v.id)
 FROM skill_package_versions v WHERE v.package_id=p.id),'[]'::jsonb))`

func (h *Handler) List(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	rows, err := h.pool.Query(c.Request().Context(), `SELECT `+packageJSON+` FROM skill_packages p WHERE p.owner_user_id=$1 ORDER BY p.updated_at DESC,p.id LIMIT 200`, uid)
	if err != nil {
		return databaseError(err)
	}
	defer rows.Close()
	items := []json.RawMessage{}
	for rows.Next() {
		var raw json.RawMessage
		if err = rows.Scan(&raw); err != nil {
			return databaseError(err)
		}
		items = append(items, raw)
	}
	if rows.Err() != nil {
		return databaseError(rows.Err())
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) Detail(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	id, err := parameter(c, "id")
	if err != nil {
		return err
	}
	var raw json.RawMessage
	err = h.pool.QueryRow(c.Request().Context(), `SELECT `+packageJSON+` FROM skill_packages p WHERE p.id=$1 AND p.owner_user_id=$2`, id, uid).Scan(&raw)
	if err != nil {
		return databaseError(err)
	}
	return c.JSONBlob(http.StatusOK, raw)
}

func (h *Handler) Version(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	id, err := parameter(c, "id")
	if err != nil {
		return err
	}
	versionID, err := parameter(c, "versionId")
	if err != nil {
		return err
	}
	var raw []byte
	err = h.pool.QueryRow(c.Request().Context(), `SELECT v.payload FROM skill_package_versions v JOIN skill_packages p ON p.id=v.package_id WHERE p.id=$1 AND p.owner_user_id=$2 AND v.id=$3`, id, uid, versionID).Scan(&raw)
	if err != nil {
		return databaseError(err)
	}
	return c.JSONBlob(http.StatusOK, raw)
}

func (h *Handler) Import(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	var req ImportRequest
	if err = readRequest(c, &req); err != nil {
		return err
	}
	bundle, payload, digest, err := ValidateImport(req)
	if err != nil {
		return databaseError(err)
	}
	packageID := uuid.New()
	existing := c.Param("id") != ""
	if existing {
		packageID, err = parameter(c, "id")
		if err != nil {
			return err
		}
	}
	versionID := uuid.New()
	ctx := c.Request().Context()
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Serialize imports per owner to make the visible 200-package limit explicit.
		if _, err := tx.Exec(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, uid); err != nil {
			return err
		}
		var count int
		if !existing {
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM skill_packages WHERE owner_user_id=$1`, uid).Scan(&count); err != nil {
				return err
			}
			if count >= 200 {
				return packageError("PACKAGE_LIMIT", "package limit reached")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO skill_packages(id,owner_user_id,name,description) VALUES($1,$2,$3,$4)`, packageID, uid, bundle.Name, bundle.Description); err != nil {
				return err
			}
		} else {
			var found uuid.UUID
			if err := tx.QueryRow(ctx, `SELECT id FROM skill_packages WHERE id=$1 AND owner_user_id=$2 FOR UPDATE`, packageID, uid).Scan(&found); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM skill_package_versions WHERE package_id=$1`, packageID).Scan(&count); err != nil {
				return err
			}
			if count >= 50 {
				return packageError("VERSION_LIMIT", "version limit reached")
			}
		}
		for _, id := range bundle.CapabilityIDs {
			var found string
			if err := tx.QueryRow(ctx, `SELECT id FROM skills WHERE id=$1`, id).Scan(&found); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return packageError("CAPABILITY_UNKNOWN", "unknown capability: "+id)
				}
				return err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO skill_package_versions(id,package_id,version,digest,payload,capability_ids,providers) VALUES($1,$2,$3,$4,$5,$6,$7)`, versionID, packageID, req.Version, digest, payload, bundle.CapabilityIDs, bundle.Providers); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE skill_packages SET name=$2,description=$3,updated_at=now() WHERE id=$1`, packageID, bundle.Name, bundle.Description)
		return err
	})
	if err != nil {
		return databaseError(err)
	}
	return c.JSON(http.StatusCreated, map[string]any{"id": packageID, "version_id": versionID, "digest": digest})
}

func ownedAgent(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, agentID, uid uuid.UUID, lock bool) (string, error) {
	var mode string
	sql := `SELECT connection_mode FROM agents WHERE id=$1 AND creator_id=$2`
	if lock {
		sql += " FOR UPDATE"
	}
	err := db.QueryRow(ctx, sql, agentID, uid).Scan(&mode)
	return mode, err
}

func (h *Handler) Bindings(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	agentID, err := parameter(c, "id")
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	mode, err := ownedAgent(ctx, h.pool, agentID, uid, false)
	if err != nil {
		return databaseError(err)
	}
	var features []string
	err = h.pool.QueryRow(ctx, `SELECT features FROM runtime_sessions WHERE agent_id=$1 AND status NOT IN ('revoked','closed') ORDER BY created_at DESC LIMIT 1`, agentID).Scan(&features)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return databaseError(err)
	}
	providers := []string{}
	for _, p := range []string{"codex", "claude"} {
		if slices.Contains(features, "skill_packages."+p+".v1") {
			providers = append(providers, p)
		}
	}
	var items json.RawMessage
	err = h.pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('package_id',p.id,'name',v.payload::jsonb->>'name','providers',v.providers,'latest_version_id',(SELECT id FROM skill_package_versions WHERE package_id=p.id ORDER BY created_at DESC,id LIMIT 1),'version_id',v.id,'version',v.version,'digest',v.digest,'capability_ids',v.capability_ids,'binding_id',b.binding_id,'status',b.status,'error_code',b.error_code,'last_run_id',b.last_run_id,'loaded_at',b.loaded_at) ORDER BY p.name,p.id),'[]'::jsonb)
 FROM agent_skill_package_bindings b JOIN skill_packages p ON p.id=b.package_id JOIN skill_package_versions v ON v.id=b.version_id WHERE b.agent_id=$1`, agentID).Scan(&items)
	if err != nil {
		return databaseError(err)
	}
	return c.JSON(http.StatusOK, map[string]any{"items": items, "supported": mode == "runtime" && slices.Contains(features, Feature) && len(providers) > 0, "providers": providers})
}

func (h *Handler) Bind(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	agentID, err := parameter(c, "id")
	if err != nil {
		return err
	}
	packageID, err := parameter(c, "packageId")
	if err != nil {
		return err
	}
	var req struct {
		VersionID uuid.UUID `json:"version_id"`
	}
	if err = readRequest(c, &req); err != nil {
		return err
	}
	ctx := c.Request().Context()
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		mode, err := ownedAgent(ctx, tx, agentID, uid, true)
		if err != nil {
			return err
		}
		var active bool
		if err := tx.QueryRow(ctx, `SELECT lifecycle_status='active' FROM agents WHERE id=$1`, agentID).Scan(&active); err != nil {
			return err
		}
		if !active {
			return packageError("AGENT_DISABLED", "enable the Agent before associating a package")
		}
		if mode != "runtime" {
			return packageError("HOST_INCOMPATIBLE", "this Agent does not support managed skill packages")
		}
		var providers []string
		if err = tx.QueryRow(ctx, `SELECT v.providers FROM skill_package_versions v JOIN skill_packages p ON p.id=v.package_id WHERE p.id=$1 AND p.owner_user_id=$2 AND v.id=$3`, packageID, uid, req.VersionID).Scan(&providers); err != nil {
			return err
		}
		var features []string
		if err = tx.QueryRow(ctx, `SELECT features FROM runtime_sessions WHERE agent_id=$1 AND status NOT IN ('revoked','closed') ORDER BY created_at DESC LIMIT 1`, agentID).Scan(&features); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return packageError("HOST_INCOMPATIBLE", "connect a compatible Plugin host before binding packages")
			}
			return err
		}
		compatible := false
		for _, p := range providers {
			compatible = compatible || slices.Contains(features, "skill_packages."+p+".v1")
		}
		if !slices.Contains(features, Feature) || !compatible {
			return packageError("HOST_INCOMPATIBLE", "the Agent's execution environment is incompatible with this version")
		}
		var count int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM agent_skill_package_bindings WHERE agent_id=$1 AND package_id<>$2`, agentID, packageID).Scan(&count); err != nil {
			return err
		}
		if count >= MaxBindings {
			return packageError("BINDING_LIMIT", "an Agent can bind at most 5 packages")
		}
		_, err = tx.Exec(ctx, `INSERT INTO agent_skill_package_bindings(agent_id,package_id,version_id,binding_id) VALUES($1,$2,$3,$4)
   ON CONFLICT(agent_id,package_id) DO UPDATE SET version_id=EXCLUDED.version_id,binding_id=EXCLUDED.binding_id,status='pending',error_code='',last_run_id=NULL,loaded_at=NULL,updated_at=now()
   WHERE agent_skill_package_bindings.version_id<>EXCLUDED.version_id`, agentID, packageID, req.VersionID, uuid.New())
		return err
	})
	if err != nil {
		return databaseError(err)
	}
	return h.Bindings(c)
}

func (h *Handler) Unbind(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	agentID, err := parameter(c, "id")
	if err != nil {
		return err
	}
	packageID, err := parameter(c, "packageId")
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		if _, err := ownedAgent(ctx, tx, agentID, uid, true); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM agent_skill_package_bindings WHERE agent_id=$1 AND package_id=$2`, agentID, packageID)
		return err
	})
	if err != nil {
		return databaseError(err)
	}
	return c.NoContent(http.StatusNoContent)
}
