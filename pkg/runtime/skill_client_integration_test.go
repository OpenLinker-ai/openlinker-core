package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/coreapi"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
	"github.com/OpenLinker-ai/openlinker-core/pkg/usertoken"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// Exercise the actual production Register entry, Core token verification and
// durable ownership/status checks. Background workers use a cancelled context;
// no alternate synthetic principal is substituted into the HTTP routes.
func TestSkillPlatformClientAuthorityIntegration(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, skillpackage.Feature, "skill_packages.codex.v1")
	agentID := fixture.identity.AgentID
	var owner uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id=$1`, agentID).Scan(&owner))
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime',endpoint_url='openlinker-runtime://' || id::text WHERE id=$1`, agentID)
	require.NoError(t, err)
	otherAgent := insertAgent(t, pool, owner, "https://example.test/unused", 0, "approved")
	outsider := insertRuntimeUser(t, pool)
	secret := "synthetic-skill-client-jwt-secret-32-bytes"
	t.Setenv("DATABASE_URL", os.Getenv("TEST_DATABASE_URL"))
	t.Setenv("JWT_SECRET", secret)
	cfg, err := config.Load()
	require.NoError(t, err)
	cfg.FrontendURL = "https://platform.example.test"
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	workers, stop := context.WithCancel(ctx)
	stop()
	coreapi.Register(workers, e, pool, cfg, coreapi.Options{})
	jwt, err := auth.GenerateToken(owner.String(), secret, time.Hour)
	require.NoError(t, err)
	call := func(method, path, token string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		raw, _ := json.Marshal(body)
		if body == nil {
			raw = nil
		}
		req := httptest.NewRequest(method, "/api/v1"+path, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, want, rec.Code, "%s %s: %s", method, path, rec.Body.String())
		return rec
	}
	svc := usertoken.NewService(pool)
	issue := func(uid uuid.UUID, grants ...usertoken.GrantRequest) *usertoken.TokenResponse {
		t.Helper()
		token, err := svc.Create(ctx, uid, &usertoken.CreateRequest{Name: "Skill client test", Grants: grants})
		require.NoError(t, err)
		return token
	}
	grant := func(permission, kind string) usertoken.GrantRequest {
		return usertoken.GrantRequest{Permission: permission, ResourceType: kind}
	}
	all := []usertoken.GrantRequest{grant("skill-packages:read", "skill_package"), grant("skill-packages:import", "skill_package"), grant("skill-bindings:read", "agent"), grant("skill-bindings:manage", "agent")}
	token := issue(owner, all...)
	legacy := issue(owner, grant("agents:read", "agent"), grant("agents:run", "agent"), grant("runs:read", "run"), grant("runs:cancel", "run"), grant("tasks:create", "task"))
	read := issue(owner, all[0], all[2])
	imports := issue(owner, all[1])
	unrelated := issue(outsider, all...)
	scopedGrants := []usertoken.GrantRequest{all[2], all[3]}
	scopeID := agentID.String()
	for i := range scopedGrants {
		scopedGrants[i].ResourceID = &scopeID
	}
	scoped := issue(owner, scopedGrants...)
	foreign := otherAgent.String()
	_, err = svc.Create(ctx, outsider, &usertoken.CreateRequest{Name: "Invalid foreign range", Grants: []usertoken.GrantRequest{{Permission: "skill-bindings:manage", ResourceType: "agent", ResourceID: &foreign}}})
	require.Error(t, err)
	pkgScope := uuid.NewString()
	_, err = svc.Create(ctx, owner, &usertoken.CreateRequest{Name: "Invalid package range", Grants: []usertoken.GrantRequest{{Permission: "skill-packages:read", ResourceType: "skill_package", ResourceID: &pkgScope}}})
	require.Error(t, err)
	request := skillpackage.ImportRequest{Version: "1.0.0", Providers: []string{"codex"}, Files: map[string]string{"SKILL.md": "---\nname: scoped-skill\ndescription: Synthetic private instructions\n---\nPRIVATE-SKILL-MARKER\n", "references/example.txt": "synthetic"}}
	var imported struct {
		ID, VersionID uuid.UUID
		Digest        string `json:"digest"`
	}
	var ids map[string]string
	require.NoError(t, json.Unmarshal(call("POST", "/creator/skill-packages", jwt, request, 201).Body.Bytes(), &ids))
	imported.ID = uuid.MustParse(ids["id"])
	imported.VersionID = uuid.MustParse(ids["version_id"])
	imported.Digest = ids["digest"]
	path := "/creator/skill-packages/" + imported.ID.String()
	versionPath := path + "/versions/" + imported.VersionID.String()
	publicVersion := "/skill-packages/" + imported.ID.String() + "/versions/" + imported.VersionID.String()
	bindingsPath := "/creator/agents/" + agentID.String() + "/skill-packages"
	bindPath := bindingsPath + "/" + imported.ID.String()
	body := map[string]any{"version_id": imported.VersionID}
	importBody := map[string]any{"source_package_id": imported.ID, "source_version_id": imported.VersionID, "expected_digest": imported.Digest}
	call("GET", "/creator/skill-packages", legacy.PlaintextToken, nil, 403)
	call("GET", path, legacy.PlaintextToken, nil, 403)
	call("GET", versionPath, legacy.PlaintextToken, nil, 403)
	call("GET", bindingsPath, legacy.PlaintextToken, nil, 403)
	call("PUT", bindPath, legacy.PlaintextToken, body, 403)
	call("DELETE", bindPath, legacy.PlaintextToken, nil, 403)
	call("POST", "/creator/skill-packages/imports", legacy.PlaintextToken, importBody, 403)
	require.Contains(t, call("GET", versionPath, read.PlaintextToken, nil, 200).Body.String(), "PRIVATE-SKILL-MARKER")
	require.NotContains(t, call("GET", path, read.PlaintextToken, nil, 200).Body.String(), "PRIVATE-SKILL-MARKER")
	call("GET", "/creator/skill-packages", token.PlaintextToken, nil, 200)
	call("GET", path, unrelated.PlaintextToken, nil, 404)
	call("GET", versionPath, unrelated.PlaintextToken, nil, 404)
	call("GET", bindingsPath, unrelated.PlaintextToken, nil, 404)
	call("PUT", bindPath, unrelated.PlaintextToken, body, 404)
	call("PUT", bindPath, read.PlaintextToken, body, 403)
	call("POST", "/creator/skill-packages/imports", read.PlaintextToken, importBody, 403)
	call("GET", path, imports.PlaintextToken, nil, 403)
	call("GET", bindingsPath, scoped.PlaintextToken, nil, 200)
	call("PUT", bindPath, scoped.PlaintextToken, body, 200)
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		target := "/creator/agents/" + otherAgent.String() + "/skill-packages"
		if method != "GET" {
			target += "/" + imported.ID.String()
		}
		call(method, target, scoped.PlaintextToken, body, 403)
	}
	call("DELETE", bindPath, scoped.PlaintextToken, nil, 204)
	// Every authoring route remains JWT-only even with all new grants.
	for _, tc := range []struct {
		method, path string
		body         any
	}{{"POST", "/creator/skill-packages", request}, {"POST", path + "/versions", request}, {"PATCH", path, map[string]string{"visibility": "public"}}, {"PUT", versionPath + "/publication", map[string]string{}}, {"DELETE", versionPath + "/publication", nil}} {
		call(tc.method, tc.path, token.PlaintextToken, tc.body, 401)
	}
	call("GET", publicVersion+"/metadata", "", nil, 404)
	call("PATCH", path, jwt, map[string]string{"visibility": "public"}, 200)
	call("GET", publicVersion+"/metadata", "", nil, 404)
	call("PUT", versionPath+"/publication", jwt, map[string]any{"metadata": map[string]string{"publisher_name": "Demo", "repository_url": "https://github.com/example/demo", "license": "MIT", "release_notes": "PUBLIC-RELEASE-NOTES"}}, 200)
	metadata := call("GET", publicVersion+"/metadata", "", nil, 200)
	require.Equal(t, "no-store", metadata.Header().Get("Cache-Control"))
	require.NotContains(t, metadata.Body.String(), "PRIVATE-SKILL-MARKER")
	require.Contains(t, metadata.Body.String(), "PUBLIC-RELEASE-NOTES")
	var dto map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadata.Body.Bytes(), &dto))
	keys := []string{}
	for k := range dto {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"id", "package_id", "version", "digest", "providers", "capability_ids", "created_at", "published_at", "visibility", "publication_metadata", "contents", "local_install_compatible"}, keys)
	var contents map[string]any
	require.NoError(t, json.Unmarshal(dto["contents"], &contents))
	require.Len(t, contents, 3)
	require.Equal(t, "scoped-skill", contents["name"])
	require.NotContains(t, contents, "files")
	require.Equal(t, json.RawMessage("true"), dto["local_install_compatible"])
	call("POST", "/creator/skill-packages/imports", imports.PlaintextToken, importBody, 201)
	call("POST", "/creator/skill-packages/imports", imports.PlaintextToken, importBody, 200)
	call("GET", publicVersion, "", nil, 200) // full-content endpoint remains compatible
	call("DELETE", versionPath+"/publication", jwt, nil, 200)
	call("GET", publicVersion+"/metadata", "", nil, 404)
	call("PUT", versionPath+"/publication", jwt, map[string]string{}, 200)
	_, err = pool.Exec(ctx, `UPDATE users SET disabled_at=now() WHERE id=$1`, owner)
	require.NoError(t, err)
	call("GET", path, token.PlaintextToken, nil, 401)
	call("GET", publicVersion+"/metadata", "", nil, 404)
	_, err = pool.Exec(ctx, `UPDATE users SET disabled_at=NULL WHERE id=$1`, owner)
	require.NoError(t, err)
	require.NoError(t, svc.Revoke(ctx, owner, uuid.MustParse(token.ID)))
	call("GET", path, token.PlaintextToken, nil, 401)
	_, err = pool.Exec(ctx, `UPDATE skill_package_versions SET payload=replace(payload,'PRIVATE-SKILL-MARKER','TAMPERED') WHERE id=$1`, imported.VersionID)
	require.NoError(t, err)
	response := call("GET", publicVersion+"/metadata", "", nil, 500)
	require.NotContains(t, response.Body.String(), "TAMPERED")
	require.False(t, strings.Contains(response.Body.String(), "PRIVATE-SKILL-MARKER"))
}
