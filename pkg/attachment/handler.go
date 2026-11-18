package attachment

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// Handler serves uploads and capability-link downloads.
type Handler struct {
	store *Store
	links Links
}

func NewHandler(store *Store, links Links) *Handler { return &Handler{store: store, links: links} }

// Register adds POST /attachments (authenticated, same permission as
// starting a Run) and GET /attachments/:token (capability link).
func (h *Handler) Register(api *echo.Group, authMw echo.MiddlewareFunc) {
	api.POST("/attachments", h.Upload, authMw)
	api.GET("/attachments/:token", h.Download)
}

type uploadResponse struct {
	URL       string    `json:"url"`
	Kind      string    `json:"kind"`
	MediaType string    `json:"media_type"`
	Filename  string    `json:"filename"`
	Size      int64     `json:"size"`
	SHA256    string    `json:"sha256"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Upload stores the raw request body. The optional ?filename= is display
// metadata only and is never used as a path.
func (h *Handler) Upload(c echo.Context) error {
	if err := auth.RequireAnyPermission(c, "agents:run", "agent"); err != nil {
		return err
	}
	owner, err := uuid.Parse(httpx.UserIDFrom(c))
	if err != nil {
		return httpx.Unauthorized("")
	}
	if c.Request().ContentLength > MaxFileBytes {
		return httpx.NewError(http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "attachment must be at most 30 MB")
	}
	body := http.MaxBytesReader(c.Response(), c.Request().Body, MaxFileBytes+1)
	upload, err := h.store.Put(c.Request().Context(), owner, c.QueryParam("filename"), body)
	var tooLarge *http.MaxBytesError
	switch {
	case errors.Is(err, ErrTooLarge) || errors.As(err, &tooLarge):
		return httpx.NewError(http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "attachment must be at most 30 MB")
	case errors.Is(err, ErrEmpty):
		return httpx.Unprocessable("attachment is empty")
	case errors.Is(err, ErrOwnerQuota):
		return httpx.RateLimited("too many live attachments; finish or remove earlier ones first")
	case errors.Is(err, ErrInstanceFull), errors.Is(err, ErrDiskLow):
		return httpx.ServiceUnavailable("attachment storage is temporarily full")
	case err != nil:
		log.Error().Err(err).Msg("attachment upload failed")
		return httpx.ServiceUnavailable("attachment storage is temporarily unavailable")
	}
	return c.JSON(http.StatusCreated, uploadResponse{
		URL: h.links.Link(upload.Token), Kind: upload.Kind, MediaType: upload.MediaType, Filename: upload.Filename,
		Size: upload.Size, SHA256: upload.SHA256, ExpiresAt: upload.ExpiresAt,
	})
}

// Download streams a live attachment to whoever holds the capability link
// (the executing Agent, or the uploader's preview). Non-images are always
// served as opaque downloads so a browser never renders or runs them.
func (h *Handler) Download(c echo.Context) error {
	meta, file, err := h.store.Open(c.Param("token"))
	if errors.Is(err, ErrNotFound) {
		return httpx.NotFound("attachment not found or expired")
	}
	if err != nil {
		return httpx.ServiceUnavailable("attachment storage is temporarily unavailable")
	}
	defer file.Close()
	header := c.Response().Header()
	header.Set("Cache-Control", "private, no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Content-Security-Policy", "default-src 'none'; sandbox")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("Cross-Origin-Resource-Policy", "same-site")
	header.Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	contentType := "application/octet-stream"
	if meta.Kind == KindImage {
		contentType = meta.MediaType
		header.Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": meta.Filename}))
	} else {
		header.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": meta.Filename}))
	}
	header.Set(echo.HeaderContentType, contentType)
	header.Set("X-Attachment-SHA256", meta.SHA256)
	c.Response().WriteHeader(http.StatusOK)
	_, err = io.Copy(c.Response(), file)
	return err
}
