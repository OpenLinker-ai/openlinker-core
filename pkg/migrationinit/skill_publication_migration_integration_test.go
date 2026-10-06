package migrationinit

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Existing private skills must not become published or have their immutable
// bytes rewritten when a populated version-95 database is upgraded.
func TestSkillPublicationMigrationPreservesExistingPrivateContent(t *testing.T) {
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, baseURL)
	migrateTestDatabaseToVersion(t, databaseURL, 95)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	ownerID, packageID, versionID := uuid.New(), uuid.New(), uuid.New()
	payload := `{"files":{"SKILL.md":"---\nname: private-example\ndescription: Synthetic private content\n---\nKeep this content private.\n"},"providers":["codex"]}`
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(payload)))
	if _, err := conn.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'synthetic','Synthetic migration owner')`, ownerID, ownerID.String()+"@example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO skill_packages(id,owner_user_id,name,description) VALUES($1,$2,'private-example','Synthetic private content')`, packageID, ownerID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO skill_package_versions(id,package_id,version,digest,payload,providers) VALUES($1,$2,'1.0.0',$3,$4,ARRAY['codex'])`, versionID, packageID, digest, payload); err != nil {
		t.Fatal(err)
	}
	migrateTestDatabaseToCurrent(t, databaseURL)
	var visibility, storedPayload, storedDigest string
	var publishedAt *time.Time
	var sourcePackage, sourceVersion *uuid.UUID
	if err := conn.QueryRow(ctx, `SELECT p.visibility,p.source_package_id,v.published_at,v.source_version_id,v.payload,v.digest FROM skill_packages p JOIN skill_package_versions v ON v.package_id=p.id WHERE p.id=$1 AND v.id=$2`, packageID, versionID).Scan(&visibility, &sourcePackage, &publishedAt, &sourceVersion, &storedPayload, &storedDigest); err != nil {
		t.Fatal(err)
	}
	if visibility != "private" || publishedAt != nil || sourcePackage != nil || sourceVersion != nil {
		t.Fatalf("existing content exposed or given provenance: visibility=%q published=%v sourcePackage=%v sourceVersion=%v", visibility, publishedAt, sourcePackage, sourceVersion)
	}
	if storedPayload != payload || storedDigest != digest {
		t.Fatal("migration changed stored bytes or digest")
	}
	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if current, err := snapshot.ValidateCoreUp(); err != nil || !current {
		t.Fatalf("populated upgrade failed fingerprint validation: current=%v err=%v", current, err)
	}
}
