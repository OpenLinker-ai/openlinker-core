package runtime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcp"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestResourceMetadataPublicationPrivacyAndDirectory(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	owner, other := insertCreator(t, pool), insertCreator(t, pool)
	cfg := newTestConfig()
	token, err := auth.GenerateTokenWithVersion(owner.String(), cfg.JWTSecret, time.Hour, 0)
	require.NoError(t, err)
	outsider, err := auth.GenerateTokenWithVersion(other.String(), cfg.JWTSecret, time.Hour, 0)
	require.NoError(t, err)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	api := e.Group("/api/v1")
	mw := auth.JWTMiddlewareWithUserStatus(cfg.JWTSecret, auth.NewDBUserStatusChecker(pool))
	skills := skillpackage.NewHandler(pool)
	skills.Register(api, mw)
	skills.RegisterPublic(api)
	catalog := mcp.NewCatalogHandler(mcp.NewDirectory(pool))
	catalog.Register(api)
	catalog.RegisterProtected(api, mw)
	send := func(method, path, bearer, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/api/v1"+path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			r.Header.Set("Authorization", "Bearer "+bearer)
		}
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		return w
	}
	check := func(method, path, bearer, body string, status int) *httptest.ResponseRecorder {
		t.Helper()
		w := send(method, path, bearer, body)
		require.Equal(t, status, w.Code, w.Body.String())
		return w
	}
	create := func(name, version, provider string, packageID string) map[string]string {
		t.Helper()
		req := skillpackage.ImportRequest{Version: version, Providers: []string{provider}, CapabilityIDs: []string{"data/analysis"}, Files: map[string]string{"SKILL.md": "---\nname: " + name + "\ndescription: Synthetic metadata acceptance\n---\nRead-only instructions.\n"}}
		raw, _ := json.Marshal(req)
		path := "/creator/skill-packages"
		if packageID != "" {
			path += "/" + packageID + "/versions"
		}
		var out map[string]string
		require.NoError(t, json.Unmarshal(check("POST", path, token, string(raw), 201).Body.Bytes(), &out))
		return out
	}
	src := create("metadata-alpha", "1.0.0", "codex", "")
	base := "/creator/skill-packages/" + src["id"]
	pub := base + "/versions/" + src["version_id"] + "/publication"
	public := "/skill-packages/" + src["id"] + "/versions/" + src["version_id"]
	for _, bad := range []string{`{"metadata":{"repository_url":"https://example.com/a b"}}`, `{"metadata":{"publisher_name":"team\u202e"}}`, `null`, `{"metadata":null}`, `{"metadata":{"repository_url":"https://example.com?secret=yes"}}`, `{"metadata":{"unknown":"x"}}`, `{} {}`} {
		check("PUT", pub, token, bad, 400)
	}
	check("PUT", pub, "", `{}`, 401)
	check("PUT", pub, outsider, `{}`, 404)
	notes := strings.Repeat("字", 4000)
	body, _ := json.Marshal(map[string]any{"metadata": map[string]string{"publisher_name": " Synthetic author ", "repository_url": "https://example.com/repo", "license": "MIT", "release_notes": notes}})
	check("PUT", pub, token, string(body), 200)
	check("GET", public, "", "", 404)
	check("PATCH", base, token, `{"visibility":"public"}`, 200)
	detail := check("GET", public, "", "", 200)
	require.Contains(t, detail.Body.String(), notes)
	require.NotContains(t, detail.Body.String(), "password_hash")
	bundle := check("GET", public+"/bundle.json", "", "", 200)
	require.Equal(t, src["digest"], fmt.Sprintf("%x", sha256.Sum256(bundle.Body.Bytes())))
	require.NotContains(t, bundle.Body.String(), "Synthetic author")
	check("PUT", pub, token, string(body), 200)
	check("PUT", pub, token, `{}`, 200)
	check("PUT", pub, token, `{"metadata":{}}`, 409)
	check("DELETE", pub, token, "", 200)
	check("GET", public, "", "", 404)
	check("PUT", pub, token, `{"metadata":{"publisher_name":"changed"}}`, 409)
	check("PUT", pub, token, "", 200)
	list := check("GET", "/skill-packages?provider=codex&capability=data/analysis&size=1", "", "", 200)
	require.Contains(t, list.Body.String(), src["id"])
	require.NotContains(t, list.Body.String(), "release_notes")
	require.Contains(t, list.Body.String(), "Synthetic author")
	ownedList := check("GET", "/creator/skill-packages", token, "", 200)
	require.NotContains(t, ownedList.Body.String(), "release_notes")
	require.Contains(t, check("GET", base, token, "", 200).Body.String(), notes)
	draft := create("private-new-name", "2.0.0", "claude", src["id"])
	require.Contains(t, check("GET", "/skill-packages?provider=codex", "", "", 200).Body.String(), src["id"])
	check("PUT", base+"/versions/"+draft["version_id"]+"/publication", token, `{}`, 200)
	require.NotContains(t, check("GET", "/skill-packages?provider=codex", "", "", 200).Body.String(), src["id"])
	require.Contains(t, check("GET", "/skill-packages?provider=claude", "", "", 200).Body.String(), draft["version_id"])
	check("GET", "/skill-packages?provider=codex&provider=claude", "", "", 400)
	require.Contains(t, check("GET", "/skill-packages?capability=unknown/valid-id", "", "", 200).Body.String(), `"total":0`)
	// Imported copies keep identical execution bytes, never inherited claims.
	importBody, _ := json.Marshal(map[string]string{"source_package_id": src["id"], "source_version_id": src["version_id"], "expected_digest": src["digest"]})
	var imported map[string]string
	require.NoError(t, json.Unmarshal(check("POST", "/creator/skill-packages/imports", outsider, string(importBody), 201).Body.Bytes(), &imported))
	copied := check("GET", "/creator/skill-packages/"+imported["id"], outsider, "", 200)
	require.Contains(t, copied.Body.String(), `"publication_metadata": null`)
	require.NotContains(t, copied.Body.String(), "Synthetic author")
	// Selective filters run before page size; private/draft/hidden resources do not count.
	beta := create("metadata-beta", "1.0.0", "claude", "")
	betaBase := "/creator/skill-packages/" + beta["id"]
	check("PUT", betaBase+"/versions/"+beta["version_id"]+"/publication", token, `{}`, 200)
	check("PATCH", betaBase, token, `{"visibility":"public"}`, 200)
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		Total int `json:"total"`
	}
	require.NoError(t, json.Unmarshal(check("GET", "/skill-packages?provider=claude&sort=name&size=1&page=2", "", "", 200).Body.Bytes(), &page))
	require.Equal(t, 2, page.Total)
	require.Len(t, page.Items, 1)
	require.Equal(t, src["id"], page.Items[0].ID)

	agentID := insertAgent(t, pool, owner, "https://synthetic.invalid/mcp", 0, "approved")
	var slug string
	require.NoError(t, pool.QueryRow(ctx, `UPDATE agents SET connection_mode='mcp_server',mcp_tool_name='analyze',visibility='public',tags=ARRAY['analytics'] WHERE id=$1 RETURNING slug`, agentID).Scan(&slug))
	_, err = pool.Exec(ctx, `INSERT INTO agent_skills(agent_id,skill_id) VALUES($1,'data/analysis')`, agentID)
	require.NoError(t, err)
	path := "/creator/agents/" + agentID.String() + "/mcp-metadata"
	publicMCP := "/mcp-services/" + slug + "/metadata"
	require.Contains(t, check("GET", path, token, "", 200).Body.String(), `"revision":0`)
	require.Contains(t, check("GET", publicMCP, "", "", 200).Body.String(), `"metadata":{}`)
	check("GET", path, outsider, "", 404)
	check("GET", path, "", "", 401)
	for _, bad := range []string{`{}`, `null`, `{"metadata":{},"expected_revision":null}`, `{"metadata":{},"expected_revision":-1}`, `{"metadata":null,"expected_revision":0}`, `{"metadata":{},"expected_revision":0,"extra":1}`} {
		check("PUT", path, token, bad, 400)
	}
	write := `{"metadata":{"publisher_name":"MCP Synthetic","release_notes":"SERVICE NOTES"},"expected_revision":0}`
	// Concurrent first writes have exactly one winner, no read-modify-write loss.
	var wg sync.WaitGroup
	results := make(chan int, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); results <- send("PUT", path, token, write).Code }()
	}
	wg.Wait()
	close(results)
	codes := []int{}
	for code := range results {
		codes = append(codes, code)
	}
	require.ElementsMatch(t, []int{200, 409}, codes)
	require.NotContains(t, check("GET", publicMCP, "", "", 200).Body.String(), "revision")
	// A metadata write must not wait on the KEY SHARE lock used by run FKs.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SELECT id FROM agents WHERE id=$1 FOR KEY SHARE`, agentID)
	require.NoError(t, err)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- send("PUT", path, token, `{"metadata":{"license":"MIT"},"expected_revision":1}`) }()
	select {
	case result := <-done:
		require.Equal(t, 200, result.Code, result.Body.String())
	case <-time.After(2 * time.Second):
		_ = tx.Rollback(ctx)
		t.Fatal("metadata write blocks run FK lock")
	}
	require.NoError(t, tx.Rollback(ctx))
	check("PUT", path, token, write, 409)
	filtered := check("GET", "/mcp-services?capability=data/analysis&tag=analytics&size=1", "", "", 200)
	require.Contains(t, filtered.Body.String(), agentID.String())
	require.Contains(t, filtered.Body.String(), `"total":1`)
	require.Contains(t, check("GET", "/mcp-services?tag=Analytics", "", "", 200).Body.String(), `"total":0`)
	_, err = pool.Exec(ctx, `UPDATE agents SET visibility='private' WHERE id=$1`, agentID)
	require.NoError(t, err)
	check("GET", publicMCP, "", "", 404)
	check("GET", path, token, "", 200)
	_, err = pool.Exec(ctx, `UPDATE agents SET visibility='unlisted' WHERE id=$1`, agentID)
	require.NoError(t, err)
	check("GET", publicMCP, "", "", 200)
	require.NotContains(t, check("GET", "/mcp-services", "", "", 200).Body.String(), agentID.String())
	_, err = pool.Exec(ctx, `UPDATE agents SET connection_mode='direct_http' WHERE id=$1`, agentID)
	require.NoError(t, err)
	check("GET", path, token, "", 404)
	check("PUT", path, token, write, 404)
	check("GET", publicMCP, "", "", 404)
	// No authorization/metadata state entered the pinned bundle.
	require.True(t, bytes.Equal(bundle.Body.Bytes(), check("GET", public+"/bundle.json", "", "", 200).Body.Bytes()))
}
