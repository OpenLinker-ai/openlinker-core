package skillpackage

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"regexp"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
)

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Lock order: importer user row, then source package, then source version.
// Publishers lock only their package row (visibility) or version row
// (publication), so no path waits in the opposite direction.

func (h *Handler) SetVisibility(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	id, err := parameter(c, "id")
	if err != nil {
		return err
	}
	var req struct {
		Visibility string `json:"visibility"`
	}
	if err = readRequest(c, &req); err != nil {
		return err
	}
	if req.Visibility != "private" && req.Visibility != "unlisted" && req.Visibility != "public" {
		return packageError("VISIBILITY_INVALID", "visibility must be private, unlisted or public")
	}
	var found uuid.UUID
	err = h.pool.QueryRow(c.Request().Context(), `UPDATE skill_packages SET visibility=$3,updated_at=now() WHERE id=$1 AND owner_user_id=$2 RETURNING id`, id, uid, req.Visibility).Scan(&found)
	if err != nil {
		return databaseError(err)
	}
	return h.ownedDetail(c, id, uid)
}

// readEmptyObject accepts no body or `{}` so publication stays an explicit,
// parameterless state transition.
func readEmptyObject(c echo.Context) error {
	raw, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, 1024))
	if err != nil {
		return packageError("REQUEST_INVALID", "invalid or oversized request")
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	c.Request().Body = io.NopCloser(bytes.NewReader(raw))
	return readRequest(c, &struct{}{})
}

func (h *Handler) Publish(c echo.Context) error {
	return h.setPublication(c, true)
}

func (h *Handler) Withdraw(c echo.Context) error {
	return h.setPublication(c, false)
}

func (h *Handler) setPublication(c echo.Context, publish bool) error {
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
	if publish {
		if err = readEmptyObject(c); err != nil {
			return err
		}
	}
	ctx := c.Request().Context()
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		var version, payload, digest string
		if err := tx.QueryRow(ctx, `SELECT v.version,v.payload,v.digest FROM skill_package_versions v JOIN skill_packages p ON p.id=v.package_id
 WHERE p.id=$1 AND p.owner_user_id=$2 AND v.id=$3 FOR UPDATE OF v`, id, uid, versionID).Scan(&version, &payload, &digest); err != nil {
			return err
		}
		if !publish {
			_, err := tx.Exec(ctx, `UPDATE skill_package_versions SET published_at=NULL WHERE id=$1`, versionID)
			return err
		}
		// Publishing exposes exact stored bytes; refuse anything that would
		// fail the same verification public readers and importers apply.
		if _, err := verifyStoredVersion(version, payload, digest); err != nil {
			return packageError("SOURCE_INVALID", "this version failed integrity verification", http.StatusConflict)
		}
		_, err := tx.Exec(ctx, `UPDATE skill_package_versions SET published_at=COALESCE(published_at,now()) WHERE id=$1`, versionID)
		return err
	})
	if err != nil {
		return databaseError(err)
	}
	return h.ownedDetail(c, id, uid)
}

// ImportPublished creates the caller's own private copy of a published
// version. Exact stored bytes are copied after re-validation; the copy keeps
// provenance IDs but no foreign key, so later withdrawal never affects it.
// Retrying the same source version returns the existing copy with 200.
func (h *Handler) ImportPublished(c echo.Context) error {
	uid, err := owner(c)
	if err != nil {
		return err
	}
	var req struct {
		SourcePackageID uuid.UUID `json:"source_package_id"`
		SourceVersionID uuid.UUID `json:"source_version_id"`
		ExpectedDigest  string    `json:"expected_digest"`
	}
	if err = readRequest(c, &req); err != nil {
		return err
	}
	if req.SourcePackageID == uuid.Nil || req.SourceVersionID == uuid.Nil || !digestPattern.MatchString(req.ExpectedDigest) {
		return packageError("REQUEST_INVALID", "source_package_id, source_version_id and a lowercase SHA-256 expected_digest are required")
	}
	ctx := c.Request().Context()
	status := http.StatusCreated
	packageID, versionID := uuid.New(), uuid.New()
	var digest string
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		// Same per-owner serialization and quota as ordinary imports.
		if _, err := tx.Exec(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, uid); err != nil {
			return err
		}
		source, err := loadPublicVersion(ctx, tx, req.SourcePackageID, req.SourceVersionID, ` FOR SHARE OF p,v`)
		if err != nil {
			return err
		}
		if source.Digest != req.ExpectedDigest {
			return packageError("DIGEST_MISMATCH", "the published version does not match the expected digest", http.StatusConflict)
		}
		bundle, err := verifyStoredVersion(source.Version, source.payload, source.Digest)
		if err != nil {
			return packageError("SOURCE_INVALID", "the published version failed integrity verification", http.StatusConflict)
		}
		var existingPackage, existingVersion uuid.UUID
		err = tx.QueryRow(ctx, `SELECT p.id,v.id,v.digest FROM skill_packages p JOIN skill_package_versions v ON v.package_id=p.id
 WHERE p.owner_user_id=$1 AND p.source_package_id=$2 AND v.source_version_id=$3 ORDER BY v.created_at,v.id LIMIT 1`, uid, source.PackageID, source.ID).Scan(&existingPackage, &existingVersion, &digest)
		if err == nil {
			packageID, versionID, status = existingPackage, existingVersion, http.StatusOK
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM skill_packages WHERE owner_user_id=$1`, uid).Scan(&count); err != nil {
			return err
		}
		if count >= 200 {
			return packageError("PACKAGE_LIMIT", "package limit reached")
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
		if _, err := tx.Exec(ctx, `INSERT INTO skill_packages(id,owner_user_id,name,description,source_package_id) VALUES($1,$2,$3,$4,$5)`, packageID, uid, bundle.Name, bundle.Description, source.PackageID); err != nil {
			return err
		}
		digest = source.Digest
		_, err = tx.Exec(ctx, `INSERT INTO skill_package_versions(id,package_id,version,digest,payload,capability_ids,providers,source_version_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
			versionID, packageID, source.Version, source.Digest, source.payload, bundle.CapabilityIDs, bundle.Providers, source.ID)
		return err
	})
	if err != nil {
		return databaseError(err)
	}
	return c.JSON(status, map[string]any{"id": packageID, "version_id": versionID, "digest": digest})
}
