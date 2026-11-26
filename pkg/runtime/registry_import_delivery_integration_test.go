package runtime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestRegistryImportedPackageIsDeliveredWithoutTheRegistry(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, skillpackage.Feature, "skill_packages.codex.v1")
	agentID := fixture.identity.AgentID
	var creatorID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id=$1`, agentID).Scan(&creatorID))
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime',endpoint_url='openlinker-runtime://' || id::text WHERE id=$1`, agentID)
	require.NoError(t, err)

	artifact := []byte("---\nname: registry-skill\ndescription: Imported from a registry\n---\nREGISTRY-INSTRUCTION-CONTENT\n")
	sum := sha256.Sum256(artifact)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/registry/v1/skills/io.github.acme/reg/versions/1.0.0":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"mode": "hosted", "yanked": nil, "core_compatible": true,
				"artifact": map[string]any{"type": "skill-md", "digest": digest}, "license": map[string]any{"kind": "spdx", "id": "MIT", "approved": false}})
		case "/registry/v1/skills/io.github.acme/reg/versions/1.0.0/artifact":
			_, _ = w.Write(artifact)
		default:
			http.NotFound(w, r)
		}
	}))
	registries, err := catalogregistry.Parse(`[{"name":"pub","url":"` + fake.URL + `/registry","consumer":"core-a","token":"olr_abcdefghijklmnop_` + strings.Repeat("s", 40) + `","audience":"public","allow_http":true,"allow_private_network":true}]`)
	require.NoError(t, err)

	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	api := e.Group("/api/v1")
	jwt := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { c.Set(string(httpx.CtxKeyUserID), creatorID.String()); return next(c) }
	}
	packages := skillpackage.NewHandler(pool)
	packages.Register(api, jwt)
	catalogregistry.NewHandler(catalogregistry.NewClient(registries, catalogregistry.Options{}), pool, packages,
		agent.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true, JWTSecret: "x"})).Register(api, jwt)
	call := func(method, path string, body any, want int) []byte {
		t.Helper()
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(method, path, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer test")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, want, rec.Code, rec.Body.String())
		return rec.Body.Bytes()
	}
	var imported struct {
		ID        uuid.UUID `json:"id"`
		VersionID uuid.UUID `json:"version_id"`
		Digest    string    `json:"digest"`
	}
	require.NoError(t, json.Unmarshal(call(http.MethodPost, "/api/v1/creator/skill-packages/registry-imports",
		map[string]any{"registry": "pub", "namespace": "io.github.acme", "name": "reg", "version": "1.0.0", "digest": digest, "providers": []string{"codex"}}, 201), &imported))
	call(http.MethodPut, "/api/v1/creator/agents/"+agentID.String()+"/skill-packages/"+imported.ID.String(), map[string]any{"version_id": imported.VersionID}, 200)
	fake.Close()

	svc := newTestService(t, pool)
	run, err := svc.Run(ctx, insertRuntimeUser(t, pool), makeRunReq(agentID, map[string]any{"text": "test"}), "api")
	require.NoError(t, err)
	var snapshotDigest string
	require.NoError(t, pool.QueryRow(ctx, `SELECT digest FROM run_skill_package_snapshots WHERE run_id=$1`, run.RunID).Scan(&snapshotDigest))
	require.Equal(t, imported.Digest, snapshotDigest)
	candidate, err := db.New(pool).LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agentID, SkillPackageProviders: []string{"codex"}})
	require.NoError(t, err)
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(candidate.RequestMetadata, &metadata))
	delivered, _ := json.Marshal(metadata[skillpackage.MetadataKey])
	require.Contains(t, string(delivered), "REGISTRY-INSTRUCTION-CONTENT")
	require.Contains(t, string(delivered), imported.Digest)
	var envelope struct {
		SchemaVersion int `json:"schema_version"`
		Bundles       []struct {
			VersionID uuid.UUID `json:"version_id"`
			Digest    string    `json:"digest"`
			Payload   string    `json:"payload"`
		} `json:"bundles"`
	}
	require.NoError(t, json.Unmarshal(delivered, &envelope))
	require.Equal(t, 1, envelope.SchemaVersion)
	require.Len(t, envelope.Bundles, 1)
	require.Equal(t, imported.VersionID, envelope.Bundles[0].VersionID)
	recomputed := sha256.Sum256([]byte(envelope.Bundles[0].Payload))
	require.Equal(t, envelope.Bundles[0].Digest, hex.EncodeToString(recomputed[:]))
}
