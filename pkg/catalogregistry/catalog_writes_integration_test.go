package catalogregistry_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func (c coreEnv) raw(method, path string, user uuid.UUID, contentType string, body []byte) (int, map[string]any) {
	c.t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if user != uuid.Nil {
		req.Header.Set("Authorization", "Bearer "+user.String())
	}
	rec := httptest.NewRecorder()
	c.e.ServeHTTP(rec, req)
	var doc map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &doc)
	return rec.Code, doc
}

func errorCode(doc map[string]any) string {
	e, _ := doc["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func TestCatalogWritesThroughRealRegistry(t *testing.T) {
	reg := startRegistry(t, "REGISTRY_REVIEW=true", "REGISTRY_REPORT_THRESHOLD=2")
	gw := reg.expect(t, 201, "POST", "/v1/admin/tokens", registryAdmin, "application/json", jsonBody(map[string]any{"name": "core", "scopes": []string{"skills:read", "skills:write"}}))
	reg.expect(t, 201, "POST", "/v1/admin/gateways", registryAdmin, "application/json", jsonBody(map[string]any{"consumer": "core-w", "token_id": gw["id"], "identities": []string{"openlinker", "github"}}))
	pubKey, privKey, _ := ed25519.GenerateKey(nil)
	reg.expect(t, 201, "POST", "/v1/admin/gateways/core-w/keys", registryAdmin, "application/json", jsonBody(map[string]any{"kid": "k1", "public_key": base64.StdEncoding.EncodeToString(pubKey)}))
	ro := reg.expect(t, 201, "POST", "/v1/admin/tokens", registryAdmin, "application/json", jsonBody(map[string]any{"name": "core-ro", "scopes": []string{"skills:read"}}))
	reg.expect(t, 201, "POST", "/v1/admin/gateways", registryAdmin, "application/json", jsonBody(map[string]any{"consumer": "core-ro", "token_id": ro["id"], "identities": []string{"openlinker"}}))
	reg.expect(t, 201, "POST", "/v1/admin/gateways/core-ro/keys", registryAdmin, "application/json", jsonBody(map[string]any{"kid": "k1", "public_key": base64.StdEncoding.EncodeToString(pubKey)}))

	entry := func(name, consumer, token string, writes bool) string {
		return `{"name":"` + name + `","url":"` + reg.base + `","consumer":"` + consumer + `","token":"` + token + `","audience":"public","allow_http":true,"allow_private_network":true` +
			`,"assertion_audience":"` + assertionAudience + `","signing_key_id":"k1","signing_key":"` + base64.StdEncoding.EncodeToString(privKey.Seed()) + `"` +
			map[bool]string{true: `,"writes":true`, false: ``}[writes] + `}`
	}
	registries, err := catalogregistry.Parse("[" + entry("w", "core-w", gw["token"].(string), true) + "," + entry("r", "core-w", gw["token"].(string), false) + "," +
		entry("ro", "core-ro", ro["token"].(string), true) + "]")
	require.NoError(t, err)
	core := newCore(t, registries)
	alice := insertUser(t, core.pool, "", true, false)
	bob := insertUser(t, core.pool, "", true, false)
	carol := insertUser(t, core.pool, "", false, false)
	const api = "/api/v1/catalog/registries/"

	t.Run("writes need a signed-in user and an enabled catalog", func(t *testing.T) {
		status, _ := core.raw("POST", api+"w/namespaces", uuid.Nil, "application/json", []byte(`{"name":"ai.openlinker.anon"}`))
		require.Equal(t, 401, status)
		status, _ = core.raw("POST", api+"r/namespaces", alice, "application/json", []byte(`{"name":"ai.openlinker.alice"}`))
		require.Equal(t, 404, status)
		status, _ = core.raw("GET", api+"r/me", alice, "", nil)
		require.Equal(t, 404, status)
		status, doc := core.raw("POST", api+"ro/namespaces", alice, "application/json", []byte(`{"name":"ai.openlinker.alice"}`))
		require.Equal(t, 403, status)
		require.Equal(t, "CATALOG_FORBIDDEN", errorCode(doc))
	})

	t.Run("namespace and publishing", func(t *testing.T) {
		status, doc := core.raw("POST", api+"w/namespaces", alice, "application/json", []byte(`{"name":"ai.openlinker.alice"}`))
		require.Equal(t, 201, status, "%v", doc)
		status, doc = core.raw("POST", api+"w/namespaces", bob, "application/json", []byte(`{"name":"ai.openlinker.alice"}`))
		require.Equal(t, 409, status)
		require.Equal(t, "CATALOG_NAMESPACE_EXISTS", errorCode(doc))
		status, doc = core.raw("GET", api+"w/me", alice, "", nil)
		require.Equal(t, 200, status)
		require.Equal(t, []any{map[string]any{"name": "ai.openlinker.alice", "role": "owner"}}, doc["namespaces"])
		require.NotContains(t, doc, "subject")

		status, _ = core.raw("POST", api+"w/skills/ai.openlinker.alice/first/versions?version=1.0.0", alice, "text/html", []byte("<script>"))
		require.Equal(t, 415, status)
		status, _ = core.raw("POST", api+"w/skills/ai.openlinker.alice/first/versions?version=1.0.0", alice, "text/markdown", bytes.Repeat([]byte("x"), 17<<20))
		require.Equal(t, 413, status)
		status, _ = core.raw("POST", api+"w/skills/ai.openlinker.alice/..%2Fx/versions?version=1", alice, "text/markdown", skillMD("x", "X", "MIT"))
		require.Equal(t, 400, status)
		status, doc = core.raw("POST", api+"w/skills/ai.openlinker.alice/first/versions?version=1.0.0&license=MIT&smuggled=1", alice, "text/markdown", skillMD("first", "First", "MIT"))
		require.Equal(t, 201, status, "%v", doc)
		require.Equal(t, "pending", doc["review"].(map[string]any)["state"])
		require.NotContains(t, doc["artifact"].(map[string]any), "url")

		core.expect(404, "GET", api+"w/skills/ai.openlinker.alice/first", uuid.Nil, nil)
		core.expect(404, "GET", api+"w/skills/ai.openlinker.alice/first", bob, nil)
		own := core.expect(200, "GET", api+"w/skills/ai.openlinker.alice/first", alice, nil)
		revision := own["review"].(map[string]any)["revision"]
		reg.expect(t, 200, "POST", "/v1/admin/review/skills/ai.openlinker.alice/first", registryAdmin, "application/json",
			jsonBody(map[string]any{"decision": "approve", "revision": revision, "trust_namespace": true}))
		core.expect(200, "GET", api+"w/skills/ai.openlinker.alice/first", uuid.Nil, nil)

		status, doc = core.raw("POST", api+"w/skills/ai.openlinker.alice/first/versions?version=1.0.0", bob, "text/markdown", skillMD("first", "First", "MIT"))
		require.Equal(t, 403, status)
		require.Equal(t, "CATALOG_FORBIDDEN", errorCode(doc))

		bundle := []byte(`{"required_commands":[],"name":"bundled","description":"A Core bundle","files":{"SKILL.md":"---\nname: bundled\ndescription: A Core bundle\n---\nBody.\n"},"capability_ids":[],"providers":["codex"]}`)
		status, doc = core.raw("POST", api+"w/skills/ai.openlinker.alice/bundled/versions?version=1&license=MIT", alice, "application/vnd.openlinker.skill-bundle.v1+json", bundle)
		require.Equal(t, 201, status, "%v", doc)
		require.Nil(t, doc["review"])
		status, doc = core.raw("PATCH", api+"w/skills/ai.openlinker.alice/bundled", alice, "application/json", []byte(`{"listed":false}`))
		require.Equal(t, 200, status, "%v", doc)
		status, _ = core.raw("POST", api+"w/skills/ai.openlinker.alice/bundled/deprecate", alice, "application/json", []byte(`{"reason":"old"}`))
		require.Equal(t, 200, status)
		status, _ = core.raw("POST", api+"w/skills/ai.openlinker.alice/bundled/versions/1/yank", alice, "application/json", []byte(`{"reason":"bad"}`))
		require.Equal(t, 200, status)

		server := `{"name":"ai.openlinker.alice/weather","description":"Weather","version":"1.0.0","remotes":[{"type":"streamable-http","url":"https://mcp.alice.example/mcp"}]}`
		status, doc = core.raw("POST", api+"w/mcp/servers/ai.openlinker.alice", alice, "application/json", []byte(server))
		require.Equal(t, 201, status, "%v", doc)
		status, _ = core.raw("POST", api+"w/mcp/servers/ai.openlinker.alice/weather/versions/1.0.0/status", alice, "application/json", []byte(`{"status":"deprecated"}`))
		require.Equal(t, 200, status)
		status, _ = core.raw("PATCH", api+"w/mcp/servers/ai.openlinker.alice/weather", alice, "application/json", []byte(`{"listed":false}`))
		require.Equal(t, 200, status)
		status, doc = core.raw("POST", api+"w/skills/ai.openlinker.alice/github-imports/preview", alice, "application/json", []byte(`{"url":"https://example.com/not-github"}`))
		require.Equal(t, 400, status, "%v", doc)
	})

	t.Run("reports reopen review", func(t *testing.T) {
		report := []byte(`{"kind":"skill","namespace":"ai.openlinker.alice","name":"first","category":"spam","details":""}`)
		status, _ := core.raw("POST", api+"w/reports", uuid.Nil, "application/json", report)
		require.Equal(t, 401, status)
		status, doc := core.raw("POST", api+"w/reports", bob, "application/json", report)
		require.Equal(t, 201, status, "%v", doc)
		core.expect(200, "GET", api+"w/skills/ai.openlinker.alice/first", uuid.Nil, nil)
		status, _ = core.raw("POST", api+"w/reports", carol, "application/json", report)
		require.Equal(t, 201, status)
		require.Equal(t, 404, reg.expectStatus(t, "GET", "/v1/skills/ai.openlinker.alice/first"))
	})

	t.Run("writes are limited per user", func(t *testing.T) {
		bad := []byte(`{"kind":"skill","namespace":"ai.openlinker.alice","name":"first","category":"nonsense","details":""}`)
		limited := false
		for i := 0; i < 40 && !limited; i++ {
			status, _ := core.raw("POST", api+"w/reports", carol, "application/json", bad)
			limited = status == http.StatusTooManyRequests
		}
		require.True(t, limited)
		status, _ := core.raw("POST", api+"w/reports", bob, "application/json", bad)
		require.NotEqual(t, http.StatusTooManyRequests, status)
	})

}

func (r *registryProcess) expectStatus(t *testing.T, method, path string) int {
	t.Helper()
	status, _ := r.do(t, method, path, "", "", nil)
	return status
}
