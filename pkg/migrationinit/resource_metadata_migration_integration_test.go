package migrationinit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestResourceMetadataMigrationPreservesPublicationHistory(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	dsn := createMigrationTestDatabase(t, base)
	migrateTestDatabaseToVersion(t, dsn, 96)
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	owner, pkg := uuid.New(), uuid.New()
	if _, err = c.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'synthetic','Synthetic')`, owner, owner.String()+"@example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO skill_packages(id,owner_user_id,name,description) VALUES($1,$2,'migration','Synthetic')`, pkg, owner); err != nil {
		t.Fatal(err)
	}
	payload := `{"files":{"SKILL.md":"synthetic"}}`
	sum := sha256.Sum256([]byte(payload))
	digest := hex.EncodeToString(sum[:])
	for _, version := range []string{"published", "withdrawn", "private"} {
		if _, err = c.Exec(ctx, `INSERT INTO skill_package_versions(id,package_id,version,digest,payload,providers,published_at) VALUES($1,$2,$3,$4,$5,ARRAY['codex'],CASE WHEN $3='private' THEN NULL ELSE now() END)`, uuid.New(), pkg, version, digest, payload); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = c.Exec(ctx, `UPDATE skill_package_versions SET published_at=NULL WHERE version='withdrawn'`); err != nil {
		t.Fatal(err)
	}
	migrateTestDatabaseToCurrent(t, dsn)
	rows, err := c.Query(ctx, `SELECT version,payload,digest,publication_metadata::text FROM skill_package_versions WHERE package_id=$1`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var v, p, d string
		var metadata *string
		if err = rows.Scan(&v, &p, &d, &metadata); err != nil {
			t.Fatal(err)
		}
		if p != payload || d != digest {
			t.Fatal("execution bytes changed")
		}
		if v == "published" {
			if metadata == nil || *metadata != "{}" {
				t.Fatal("published legacy version not frozen")
			}
		} else if metadata != nil {
			t.Fatal("private/previously withdrawn version assigned invented history")
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	rows.Close()
	// A historical withdrawn version freezes on its next publication, atomically.
	if _, err = c.Exec(ctx, `UPDATE skill_package_versions SET published_at=now(),publication_metadata='{"license":"MIT"}'::jsonb WHERE version='withdrawn'`); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `UPDATE skill_package_versions SET published_at=now() WHERE version='private'`); err == nil {
		t.Fatal("published-without-metadata invariant accepted")
	}
	if _, err = c.Exec(ctx, `UPDATE skill_package_versions SET publication_metadata='[]'::jsonb WHERE version='private'`); err == nil {
		t.Fatal("non-object metadata accepted")
	}
	snap, err := Inspect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if current, err := snap.ValidateCoreUp(); err != nil || !current {
		t.Fatalf("fresh/upgrade shape: %v %v", current, err)
	}
}
