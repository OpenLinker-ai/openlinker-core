package skillpackage

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type RegistrySource struct {
	URL             string
	Entry           string
	Version         string
	Digest          string
	Mode            string
	License         string
	Redistributable bool
	CheckedAt       time.Time
}

type ImportResult struct {
	PackageID uuid.UUID
	VersionID uuid.UUID
	Digest    string
	Created   bool
}

func checkCapabilities(ctx context.Context, tx pgx.Tx, ids []string) error {
	for _, id := range ids {
		var found string
		if err := tx.QueryRow(ctx, `SELECT id FROM skills WHERE id=$1`, id).Scan(&found); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return packageError("CAPABILITY_UNKNOWN", "unknown capability: "+id)
			}
			return err
		}
	}
	return nil
}

func (h *Handler) ImportFromRegistry(ctx context.Context, uid uuid.UUID, req ImportRequest, src RegistrySource) (ImportResult, error) {
	bundle, payload, digest, err := ValidateImport(req)
	if err != nil {
		return ImportResult{}, databaseError(err)
	}
	result := ImportResult{PackageID: uuid.New(), VersionID: uuid.New(), Digest: digest, Created: true}
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, uid); err != nil {
			return err
		}
		var existingPackage, existingVersion uuid.UUID
		var existingDigest string
		err := tx.QueryRow(ctx, `SELECT p.id,v.id,v.digest FROM skill_packages p JOIN skill_package_versions v ON v.package_id=p.id
 WHERE p.owner_user_id=$1 AND v.source_kind='registry' AND v.source_registry_url=$2 AND v.source_registry_entry=$3 AND v.source_registry_version=$4
 ORDER BY v.created_at,v.id LIMIT 1`, uid, src.URL, src.Entry, src.Version).Scan(&existingPackage, &existingVersion, &existingDigest)
		if err == nil {
			if existingDigest != digest {
				return packageError("IMPORT_CONFLICT", "this registry version was already imported with different options", http.StatusConflict)
			}
			result = ImportResult{PackageID: existingPackage, VersionID: existingVersion, Digest: existingDigest}
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
		if err := checkCapabilities(ctx, tx, bundle.CapabilityIDs); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO skill_packages(id,owner_user_id,name,description) VALUES($1,$2,$3,$4)`, result.PackageID, uid, bundle.Name, bundle.Description); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO skill_package_versions(id,package_id,version,digest,payload,capability_ids,providers,
 source_kind,source_registry_url,source_registry_entry,source_registry_version,source_registry_digest,source_registry_mode,source_license,source_checked_at,source_redistributable)
 VALUES($1,$2,$3,$4,$5,$6,$7,'registry',$8,$9,$10,$11,$12,$13,$14,$15)`,
			result.VersionID, result.PackageID, req.Version, digest, payload, bundle.CapabilityIDs, bundle.Providers,
			src.URL, src.Entry, src.Version, src.Digest, src.Mode, src.License, src.CheckedAt, src.Redistributable)
		return err
	})
	if err != nil {
		return ImportResult{}, databaseError(err)
	}
	return result, nil
}

func notRedistributable() error {
	return packageError("SOURCE_NOT_REDISTRIBUTABLE", "a version imported from a source whose license does not allow redistribution cannot be shared", http.StatusConflict)
}
