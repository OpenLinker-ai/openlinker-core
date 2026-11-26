package migrationinit

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestCatalogSourcesMigrationKeepsContentAndGuardsSources(t *testing.T) {
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, baseURL)
	migrateTestDatabaseToVersion(t, databaseURL, 106)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	ownerID, packageID, versionID := uuid.New(), uuid.New(), uuid.New()
	payload := `{"files":{"SKILL.md":"---\nname: existing\ndescription: Existing content\n---\nKeep it.\n"},"providers":["codex"]}`
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(payload)))
	if _, err := conn.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'synthetic','Synthetic owner')`, ownerID, ownerID.String()+"@example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO skill_packages(id,owner_user_id,name,description,visibility) VALUES($1,$2,'existing','Existing content','public')`, packageID, ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO skill_package_versions(id,package_id,version,digest,payload,providers,published_at,publication_metadata) VALUES($1,$2,'1.0.0',$3,$4,ARRAY['codex'],now(),'{}'::jsonb)`, versionID, packageID, digest, payload); err != nil {
		t.Fatal(err)
	}
	migrateTestDatabaseToCurrent(t, databaseURL)
	var storedPayload, storedDigest string
	var kind *string
	var published *time.Time
	if err := conn.QueryRow(ctx, `SELECT payload,digest,source_kind,published_at FROM skill_package_versions WHERE id=$1`, versionID).Scan(&storedPayload, &storedDigest, &kind, &published); err != nil {
		t.Fatal(err)
	}
	if storedPayload != payload || storedDigest != digest || kind != nil || published == nil {
		t.Fatal("migration changed existing published content or gave it a source")
	}
	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if current, err := snapshot.ValidateCoreUp(); err != nil || !current {
		t.Fatalf("populated upgrade failed fingerprint validation: current=%v err=%v", current, err)
	}
	registryDigest := "sha256:" + strings.Repeat("a", 64)
	insert := func(columns, values string, args ...any) error {
		_, err := conn.Exec(ctx, `INSERT INTO skill_package_versions(id,package_id,version,digest,payload,providers,`+columns+`) VALUES(gen_random_uuid(),$1,$2,$3,$4,ARRAY['codex'],`+values+`)`, args...)
		return err
	}
	if err := insert(`source_kind,source_registry_url`, `'registry','https://r.example'`, packageID, "2", digest, payload); err == nil {
		t.Fatal("incomplete registry source accepted")
	}
	if err := insert(`source_kind`, `'github'`, packageID, "3", digest, payload); err == nil {
		t.Fatal("incomplete github source accepted")
	}
	if err := insert(`source_kind,source_registry_url,source_registry_entry,source_registry_version,source_registry_digest,source_registry_mode,source_redistributable`,
		`'registry','https://r.example','io.github.a/b','1','sha256:BAD','hosted',true`, packageID, "4", digest, payload); err == nil {
		t.Fatal("malformed registry digest accepted")
	}
	if err := insert(`source_kind,source_registry_url,source_registry_entry,source_registry_version,source_registry_digest,source_registry_mode,source_redistributable,published_at,publication_metadata`,
		`'registry','https://r.example','io.github.a/b','1',$5,'indexed',false,now(),'{}'::jsonb`, packageID, "5", digest, payload, registryDigest); err == nil {
		t.Fatal("published version from a non-redistributable source accepted")
	}
	if err := insert(`source_kind,source_registry_url,source_registry_entry,source_registry_version,source_registry_digest,source_registry_mode,source_redistributable`,
		`'registry','https://r.example','io.github.a/b','1',$5,'indexed',false`, packageID, "6", digest, payload, registryDigest); err != nil {
		t.Fatalf("private registry source rejected: %v", err)
	}
	migrationDir := "../../migrations/107_registry_catalog_sources.down.sql"
	down, err := os.ReadFile(migrationDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(down)); err == nil || !strings.Contains(err.Error(), "source records") {
		t.Fatalf("down migration with source records: %v", err)
	}
}
