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

func TestRegistryProjectionMigrationStartsLocalAndGuardsRecords(t *testing.T) {
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	databaseURL := createMigrationTestDatabase(t, baseURL)
	migrateTestDatabaseToVersion(t, databaseURL, 107)
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
	var storedPayload string
	if err := conn.QueryRow(ctx, `SELECT payload FROM skill_package_versions WHERE id=$1`, versionID).Scan(&storedPayload); err != nil || storedPayload != payload {
		t.Fatalf("migration changed existing content: %v", err)
	}
	snapshot, err := Inspect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if current, err := snapshot.ValidateCoreUp(); err != nil || !current {
		t.Fatalf("populated upgrade failed fingerprint validation: current=%v err=%v", current, err)
	}
	var mode string
	if err := conn.QueryRow(ctx, `SELECT mode FROM registry_projection_settings`).Scan(&mode); err != nil || mode != "local" {
		t.Fatalf("mode %q %v", mode, err)
	}
	if _, err := conn.Exec(ctx, `UPDATE skill_packages SET visibility='unlisted' WHERE id=$1`, packageID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `UPDATE users SET disabled_at=now() WHERE id=$1`, ownerID); err != nil {
		t.Fatal(err)
	}
	var dirty int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM registry_projection_dirty`).Scan(&dirty); err != nil || dirty != 0 {
		t.Fatalf("local mode marked %d subjects: %v", dirty, err)
	}
	if _, err := conn.Exec(ctx, `UPDATE registry_projection_settings SET mode='registry'`); err == nil {
		t.Fatal("registry mode without a destination accepted")
	}
	down, err := os.ReadFile("../../migrations/108_registry_projection.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO skill_package_registry_links(package_id,version_id,registry_url,registry_entry,registry_version,registry_digest) VALUES($1,$2,'https://r.example','ai.openlinker.u1/p-x','1.0.0',$3)`,
		packageID, versionID, "sha256:"+digest); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(down)); err == nil || !strings.Contains(err.Error(), "records exist") {
		t.Fatalf("down migration with projection records: %v", err)
	}
}
