package skillpackage

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
)

// Anonymous reads share one predicate: an enabled owner, a public or unlisted
// package and a published version. Every other state is the same 404, and
// names always come from a published payload, never from the package row that
// a later private draft may have renamed.
const publicOwnerPredicate = `EXISTS (SELECT 1 FROM users u WHERE u.id=p.owner_user_id AND u.deleted_at IS NULL AND u.disabled_at IS NULL)`
const publicPackagePredicate = `p.visibility IN ('public','unlisted') AND ` + publicOwnerPredicate
const publicVersionPredicate = publicPackagePredicate + ` AND v.published_at IS NOT NULL`

const publicVersionsJSON = `COALESCE((SELECT jsonb_agg(jsonb_build_object('id',v.id,'version',v.version,'digest',v.digest,
 'capability_ids',v.capability_ids,'providers',v.providers,'created_at',v.created_at,'published_at',v.published_at) ORDER BY v.created_at DESC,v.id DESC)
 FROM skill_package_versions v WHERE v.package_id=p.id AND v.published_at IS NOT NULL),'[]'::jsonb)`

// The newest published version supplies the public title and description.
const publicPackageFrom = ` FROM skill_packages p CROSS JOIN LATERAL (SELECT v.payload::jsonb AS manifest,v.created_at FROM skill_package_versions v
 WHERE v.package_id=p.id AND v.published_at IS NOT NULL ORDER BY v.created_at DESC,v.id DESC LIMIT 1) latest WHERE ` + publicPackagePredicate

const publicPackageJSON = `jsonb_build_object('id',p.id,'name',latest.manifest->>'name','description',latest.manifest->>'description',
 'visibility',p.visibility,'versions',` + publicVersionsJSON + `)`

const maxPublicQueryRunes = 200

var errIntegrity = errors.New("stored skill package failed verification")

func (h *Handler) RegisterPublic(api *echo.Group) {
	g := api.Group("/skill-packages", publicHeaders)
	g.GET("", h.PublicList)
	g.GET("/:id", h.PublicDetail)
	g.GET("/:id/versions/:versionId", h.PublicVersion)
	g.GET("/:id/versions/:versionId/bundle.json", h.PublicBundle)
	g.GET("/:id/versions/:versionId/archive.zip", h.PublicArchive)
	g.GET("/:id/versions/:versionId/files/*", h.PublicFile)
}

func publicHeaders(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		c.Response().Header().Set("X-Content-Type-Options", "nosniff")
		return next(c)
	}
}

func notFound() error { return httpx.NotFound("resource not found") }

func publicParameter(c echo.Context, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(c.Param(name))
	if err != nil {
		return uuid.Nil, notFound()
	}
	return id, nil
}

func positiveQuery(raw string, fallback, limit int) int {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 1 {
		return fallback
	}
	return min(n, limit)
}

func likePattern(q string) string {
	return "%" + strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(q) + "%"
}

func (h *Handler) PublicList(c echo.Context) error {
	q := strings.TrimSpace(c.QueryParam("q"))
	if utf8.RuneCountInString(q) > maxPublicQueryRunes {
		return packageError("REQUEST_INVALID", "search text is too long")
	}
	page := positiveQuery(c.QueryParam("page"), 1, 10000)
	size := positiveQuery(c.QueryParam("size"), 12, 50)
	ctx := c.Request().Context()
	filter := publicPackageFrom + ` AND p.visibility='public' AND ($1='' OR latest.manifest->>'name' ILIKE $2 ESCAPE '\' OR latest.manifest->>'description' ILIKE $2 ESCAPE '\')`
	var total int
	if err := h.pool.QueryRow(ctx, `SELECT count(*)`+filter, q, likePattern(q)).Scan(&total); err != nil {
		return databaseError(err)
	}
	rows, err := h.pool.Query(ctx, `SELECT `+publicPackageJSON+filter+` ORDER BY latest.created_at DESC,p.id LIMIT $3 OFFSET $4`, q, likePattern(q), size, (page-1)*size)
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
	return c.JSON(http.StatusOK, map[string]any{"items": items, "total": total, "page": page, "size": size})
}

func (h *Handler) PublicDetail(c echo.Context) error {
	id, err := publicParameter(c, "id")
	if err != nil {
		return err
	}
	var raw json.RawMessage
	if err = h.pool.QueryRow(c.Request().Context(), `SELECT `+publicPackageJSON+publicPackageFrom+` AND p.id=$1`, id).Scan(&raw); err != nil {
		return databaseError(err)
	}
	return c.JSONBlob(http.StatusOK, raw)
}

type publicVersion struct {
	ID            uuid.UUID  `json:"id"`
	PackageID     uuid.UUID  `json:"package_id"`
	Version       string     `json:"version"`
	Digest        string     `json:"digest"`
	CapabilityIDs []string   `json:"capability_ids"`
	Providers     []string   `json:"providers"`
	CreatedAt     time.Time  `json:"created_at"`
	PublishedAt   *time.Time `json:"published_at"`
	Visibility    string     `json:"visibility"`
	payload       string
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// loadPublicVersion is the single public version read. Imports pass a lock
// clause so withdrawal and visibility changes linearize with the copy.
func loadPublicVersion(ctx context.Context, db rowQuerier, packageID, versionID uuid.UUID, lock string) (publicVersion, error) {
	var v publicVersion
	err := db.QueryRow(ctx, `SELECT v.id,v.package_id,v.version,v.digest,v.capability_ids,v.providers,v.created_at,v.published_at,p.visibility,v.payload
 FROM skill_package_versions v JOIN skill_packages p ON p.id=v.package_id WHERE p.id=$1 AND v.id=$2 AND `+publicVersionPredicate+lock, packageID, versionID).
		Scan(&v.ID, &v.PackageID, &v.Version, &v.Digest, &v.CapabilityIDs, &v.Providers, &v.CreatedAt, &v.PublishedAt, &v.Visibility, &v.payload)
	return v, err
}

// verifyStoredVersion re-runs import validation over the exact stored bytes:
// they must hash to the stored digest and re-encode to the same payload.
func verifyStoredVersion(version, payload, digest string) (Bundle, error) {
	sum := sha256.Sum256([]byte(payload))
	if hex.EncodeToString(sum[:]) != digest {
		return Bundle{}, errIntegrity
	}
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var stored Bundle
	if decoder.Decode(&stored) != nil {
		return Bundle{}, errIntegrity
	}
	bundle, canonical, canonicalDigest, err := ValidateImport(ImportRequest{
		Version: version, Files: stored.Files, CapabilityIDs: stored.CapabilityIDs,
		Providers: stored.Providers, RequiredCommands: stored.RequiredCommands,
	})
	if err != nil || canonical != payload || canonicalDigest != digest || bundle.Name != stored.Name || bundle.Description != stored.Description {
		return Bundle{}, errIntegrity
	}
	return bundle, nil
}

func (h *Handler) verifiedPublicVersion(c echo.Context) (publicVersion, Bundle, error) {
	packageID, err := publicParameter(c, "id")
	if err != nil {
		return publicVersion{}, Bundle{}, err
	}
	versionID, err := publicParameter(c, "versionId")
	if err != nil {
		return publicVersion{}, Bundle{}, err
	}
	v, err := loadPublicVersion(c.Request().Context(), h.pool, packageID, versionID, "")
	if err != nil {
		return publicVersion{}, Bundle{}, databaseError(err)
	}
	bundle, err := verifyStoredVersion(v.Version, v.payload, v.Digest)
	if err != nil {
		return publicVersion{}, Bundle{}, httpx.Internal("skill package integrity check failed")
	}
	return v, bundle, nil
}

func (h *Handler) PublicVersion(c echo.Context) error {
	v, _, err := h.verifiedPublicVersion(c)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, struct {
		publicVersion
		Contents json.RawMessage `json:"contents"`
	}{v, json.RawMessage(v.payload)})
}

// PublicBundle returns the exact stored bytes so `sha256sum` matches digest.
// It is an archival/verification format, not an owner import request.
func (h *Handler) PublicBundle(c echo.Context) error {
	v, bundle, err := h.verifiedPublicVersion(c)
	if err != nil {
		return err
	}
	c.Response().Header().Set("Content-Disposition", `attachment; filename="`+ArchiveDirectory(bundle.Name, v.PackageID)+"-"+v.Version+`.json"`)
	return c.Blob(http.StatusOK, "application/json", []byte(v.payload))
}

func (h *Handler) PublicFile(c echo.Context) error {
	_, bundle, err := h.verifiedPublicVersion(c)
	if err != nil {
		return err
	}
	name, err := url.PathUnescape(c.Param("*"))
	if err != nil {
		return notFound()
	}
	// Exact lookup in the verified file map; paths are never resolved.
	content, ok := bundle.Files[name]
	if !ok {
		return notFound()
	}
	return c.Blob(http.StatusOK, "text/plain; charset=utf-8", []byte(content))
}

func (h *Handler) PublicArchive(c echo.Context) error {
	v, bundle, err := h.verifiedPublicVersion(c)
	if err != nil {
		return err
	}
	directory := ArchiveDirectory(bundle.Name, v.PackageID)
	archive, err := BuildArchive(directory, bundle.Files)
	if err != nil {
		return httpx.Internal("skill package archive failed")
	}
	c.Response().Header().Set("Content-Disposition", `attachment; filename="`+directory+"-"+v.Version+`.zip"`)
	return c.Blob(http.StatusOK, "application/zip", archive)
}

// Agent Skills clients accept lowercase hyphenated names up to 64 characters
// and reserve vendor words. Other names keep SKILL.md untouched and use an
// identifier-derived directory instead.
var archiveNamePattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

func ArchiveDirectory(name string, packageID uuid.UUID) string {
	if len(name) <= 64 && archiveNamePattern.MatchString(name) && !strings.Contains(name, "anthropic") && !strings.Contains(name, "claude") {
		return name
	}
	return "skill-" + packageID.String()
}

var archiveEpoch = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)

// BuildArchive writes only the package files, sorted, stored uncompressed and
// with fixed metadata so identical versions produce identical bytes.
func BuildArchive(directory string, files map[string]string) ([]byte, error) {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	for _, name := range names {
		header := &zip.FileHeader{Name: directory + "/" + name, Method: zip.Store, Modified: archiveEpoch}
		header.SetMode(0o644)
		entry, err := writer.CreateHeader(header)
		if err != nil {
			return nil, err
		}
		if _, err = entry.Write([]byte(files[name])); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
