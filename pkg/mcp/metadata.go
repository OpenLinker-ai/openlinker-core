package mcp

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/resourcemetadata"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
)

type metadataResponse struct {
	Metadata  json.RawMessage `json:"metadata"`
	UpdatedAt *time.Time      `json:"updated_at"`
}
type ownerMetadataResponse struct {
	metadataResponse
	Revision int64 `json:"revision"`
}

// RegisterProtected accepts the same JWT owner middleware as Agent settings.
func (h *CatalogHandler) RegisterProtected(api *echo.Group, auth echo.MiddlewareFunc) {
	g := api.Group("/creator", auth)
	g.GET("/agents/:id/mcp-metadata", h.OwnerMetadata)
	g.PUT("/agents/:id/mcp-metadata", h.SaveMetadata)
}

func metadataError(err error) error {
	var he *httpx.HTTPError
	if errors.As(err, &he) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.NotFound("resource not found")
	}
	return httpx.Internal("resource metadata operation failed")
}
func metadataOwner(c echo.Context) (uuid.UUID, uuid.UUID, error) {
	c.Response().Header().Set("Cache-Control", "private, no-store")
	owner, err := uuid.Parse(httpx.UserIDFrom(c))
	if err != nil || owner == uuid.Nil {
		return uuid.Nil, uuid.Nil, httpx.Unauthorized("authentication required")
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return uuid.Nil, uuid.Nil, httpx.NotFound("resource not found")
	}
	return owner, id, nil
}
func (h *CatalogHandler) OwnerMetadata(c echo.Context) error {
	owner, id, err := metadataOwner(c)
	if err != nil {
		return err
	}
	var out ownerMetadataResponse
	err = h.directory.pool.QueryRow(c.Request().Context(), `SELECT COALESCE(m.metadata,'{}'::jsonb),m.updated_at,COALESCE(m.revision,0) FROM agents a LEFT JOIN mcp_service_metadata m ON m.agent_id=a.id WHERE a.id=$1 AND a.creator_id=$2 AND a.connection_mode='mcp_server'`, id, owner).Scan(&out.Metadata, &out.UpdatedAt, &out.Revision)
	if err != nil {
		return metadataError(err)
	}
	return c.JSON(http.StatusOK, out)
}
func (h *CatalogHandler) PublicMetadata(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "no-store")
	c.Response().Header().Set("X-Content-Type-Options", "nosniff")
	var out metadataResponse
	// Keep the existing public Agent detail visibility contract. No private
	// credentials, revision, or owner records are projected into this response.
	err := h.directory.pool.QueryRow(c.Request().Context(), `SELECT COALESCE(m.metadata,'{}'::jsonb),m.updated_at FROM agents a LEFT JOIN mcp_service_metadata m ON m.agent_id=a.id WHERE a.slug=$1 AND a.lifecycle_status='active' AND a.visibility IN ('public','unlisted') AND a.connection_mode='mcp_server'`, c.Param("slug")).Scan(&out.Metadata, &out.UpdatedAt)
	if err != nil {
		return metadataError(err)
	}
	return c.JSON(http.StatusOK, out)
}
func (h *CatalogHandler) SaveMetadata(c echo.Context) error {
	owner, id, err := metadataOwner(c)
	if err != nil {
		return err
	}
	var req struct {
		ExpectedRevision *int64          `json:"expected_revision"`
		Metadata         json.RawMessage `json:"metadata"`
	}
	raw, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, resourcemetadata.MaxBytes))
	if err != nil || resourcemetadata.DecodeObject(raw, &req) != nil || req.ExpectedRevision == nil || *req.ExpectedRevision < 0 {
		return httpx.BadRequest("invalid metadata request")
	}
	data, err := resourcemetadata.Parse(req.Metadata)
	if err != nil {
		return httpx.BadRequest(err.Error())
	}
	encoded, _ := json.Marshal(data)
	var out ownerMetadataResponse
	ctx := c.Request().Context()
	err = pgx.BeginFunc(ctx, h.directory.pool, func(tx pgx.Tx) error {
		var found uuid.UUID
		// SHARE stabilizes mode/ownership while remaining compatible with run FK
		// KEY SHARE locks. Metadata CAS serializes edits without locking execution.
		if err := tx.QueryRow(ctx, `SELECT id FROM agents WHERE id=$1 AND creator_id=$2 AND connection_mode='mcp_server' FOR SHARE`, id, owner).Scan(&found); err != nil {
			return err
		}
		var err error
		if *req.ExpectedRevision == 0 {
			err = tx.QueryRow(ctx, `INSERT INTO mcp_service_metadata(agent_id,metadata,revision) VALUES($1,$2,1) ON CONFLICT DO NOTHING RETURNING metadata,updated_at,revision`, id, encoded).Scan(&out.Metadata, &out.UpdatedAt, &out.Revision)
		} else {
			err = tx.QueryRow(ctx, `UPDATE mcp_service_metadata SET metadata=$2,revision=revision+1,updated_at=now() WHERE agent_id=$1 AND revision=$3 RETURNING metadata,updated_at,revision`, id, encoded, *req.ExpectedRevision).Scan(&out.Metadata, &out.UpdatedAt, &out.Revision)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return httpx.NewError(http.StatusConflict, "RESOURCE_METADATA_CONFLICT", "metadata changed; reload before saving")
		}
		return err
	})
	if err != nil {
		return metadataError(err)
	}
	return c.JSON(http.StatusOK, out)
}
