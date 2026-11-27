package catalogregistry_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

const registryAdmin = "registry-admin-token-0123456789abcdefgh"
const assertionAudience = "https://registry.test/registry"

type registryProcess struct {
	base string
	cmd  *exec.Cmd
}

func (r *registryProcess) stop() {
	if r.cmd.Process != nil {
		_ = r.cmd.Process.Kill()
		_, _ = r.cmd.Process.Wait()
	}
}

func (r *registryProcess) do(t *testing.T, method, path, token, contentType string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, r.base+path, bytes.NewReader(body))
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, out
}

func (r *registryProcess) expect(t *testing.T, want int, method, path, token, contentType string, body []byte) map[string]any {
	t.Helper()
	status, out := r.do(t, method, path, token, contentType, body)
	require.Equal(t, want, status, "%s %s: %s", method, path, out)
	var doc map[string]any
	_ = json.Unmarshal(out, &doc)
	return doc
}

func jsonBody(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().String()
}

func createDatabase(t *testing.T, baseURL string) string {
	t.Helper()
	parsed, err := url.Parse(baseURL)
	require.NoError(t, err)
	name := "registry_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	admin := *parsed
	admin.Path = "/postgres"
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin.String())
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	require.NoError(t, err)
	conn.Close(ctx)
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin.String())
		if err == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
			c.Close(context.Background())
		}
	})
	target := *parsed
	target.Path = "/" + name
	return target.String()
}

func startRegistry(t *testing.T, extraEnv ...string) *registryProcess {
	t.Helper()
	binary := os.Getenv("REGISTRY_TEST_BINARY")
	dbURL := os.Getenv("REGISTRY_TEST_DATABASE_URL")
	if binary == "" || dbURL == "" {
		t.Skip("REGISTRY_TEST_BINARY and REGISTRY_TEST_DATABASE_URL are required")
	}
	addr := freePort(t)
	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(),
		"REGISTRY_LISTEN="+addr, "REGISTRY_BASE_PATH=/registry", "REGISTRY_MODE=public",
		"REGISTRY_PUBLIC_URL=http://"+addr+"/registry", "REGISTRY_ASSERTION_AUDIENCE="+assertionAudience,
		"REGISTRY_DATABASE_URL="+createDatabase(t, dbURL), "REGISTRY_STORAGE_DIR="+t.TempDir(),
		"REGISTRY_ADMIN_TOKEN="+registryAdmin, "REGISTRY_UPSTREAM_ALLOW_HTTP=true", "REGISTRY_UPSTREAM_ALLOW_CIDRS=127.0.0.0/8",
		"REGISTRY_RATE_LIMIT_READ=0", "REGISTRY_RATE_LIMIT_WRITE=0", "REGISTRY_RATE_LIMIT_FORWARD=0", "REGISTRY_BACKGROUND_SYNC=false")
	if len(extraEnv) == 0 {
		extraEnv = []string{"REGISTRY_REVIEW=false"}
	}
	cmd.Env = append(cmd.Env, extraEnv...)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	require.NoError(t, cmd.Start())
	p := &registryProcess{base: "http://" + addr + "/registry", cmd: cmd}
	t.Cleanup(p.stop)
	deadline := time.Now().Add(20 * time.Second)
	for {
		res, err := http.Get(p.base + "/readyz")
		if err == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				return p
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry did not start: %s", logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func skillMD(name, description, license string) []byte {
	s := "---\nname: " + name + "\ndescription: " + description + "\n"
	if license != "" {
		s += "license: " + license + "\n"
	}
	return []byte(s + "---\nInstructions for " + name + ".\n")
}

func tgz(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: n, Mode: 0o644, Size: int64(len(files[n])), Typeflag: tar.TypeReg}))
		_, err := tw.Write(files[n])
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func shaTag(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

type coreEnv struct {
	t    *testing.T
	pool *pgxpool.Pool
	e    *echo.Echo
}

func (c coreEnv) call(method, path string, user uuid.UUID, body any) (int, []byte, http.Header) {
	c.t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(jsonBody(body))
	}
	req := httptest.NewRequest(method, path, rd)
	req.Header.Set("Content-Type", "application/json")
	if user != uuid.Nil {
		req.Header.Set("Authorization", "Bearer "+user.String())
	}
	rec := httptest.NewRecorder()
	c.e.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes(), rec.Header()
}

func (c coreEnv) expect(want int, method, path string, user uuid.UUID, body any) map[string]any {
	c.t.Helper()
	status, out, _ := c.call(method, path, user, body)
	require.Equal(c.t, want, status, "%s %s: %s", method, path, out)
	var doc map[string]any
	_ = json.Unmarshal(out, &doc)
	return doc
}

func names(doc map[string]any, key string) string {
	var out []string
	items, _ := doc[key].([]any)
	for _, it := range items {
		m := it.(map[string]any)
		out = append(out, fmt.Sprint(m["namespace"])+"/"+fmt.Sprint(m["name"]))
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func newCore(t *testing.T, registries []catalogregistry.Registry) coreEnv {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	api := e.Group("/api/v1")
	jwt := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			raw := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
			if _, err := uuid.Parse(raw); err != nil {
				return httpx.Unauthorized("invalid token")
			}
			c.Set(string(httpx.CtxKeyUserID), raw)
			return next(c)
		}
	}
	packages := skillpackage.NewHandler(pool)
	packages.Register(api, jwt)
	agents := agent.NewService(pool, &config.Config{AllowLocalHTTPEndpoints: true, JWTSecret: "catalog-test"})
	catalogregistry.NewHandler(catalogregistry.NewClient(registries, catalogregistry.Options{}), pool, packages, agents).Register(api, jwt)
	return coreEnv{t: t, pool: pool, e: e}
}

func insertUser(t *testing.T, pool *pgxpool.Pool, githubID string, creator, admin bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var err error
	if githubID != "" {
		_, err = pool.Exec(context.Background(), `INSERT INTO users(id,email,display_name,oauth_provider,oauth_id,is_creator,is_admin) VALUES($1,$2,'Catalog user','github',$3,$4,$5)`,
			id, "catalog-"+id.String()[:8]+"@example.com", githubID, creator, admin)
	} else {
		_, err = pool.Exec(context.Background(), `INSERT INTO users(id,email,password_hash,display_name,is_creator,is_admin) VALUES($1,$2,'x','Catalog user',$3,$4)`,
			id, "catalog-"+id.String()[:8]+"@example.com", creator, admin)
	}
	require.NoError(t, err)
	return id
}

func TestCatalogUnconfiguredAddsNoRoutes(t *testing.T) {
	core := newCore(t, nil)
	core.expect(404, "GET", "/api/v1/catalog/registries", uuid.Nil, nil)
	core.expect(404, "POST", "/api/v1/creator/skill-packages/registry-imports", uuid.New(), map[string]any{})
}

func TestCatalogThroughRealRegistry(t *testing.T) {
	reg := startRegistry(t)
	githubID := fmt.Sprint(4_000_000_000 + rand.IntN(1_000_000_000))
	reg.expect(t, 201, "POST", "/v1/admin/namespaces", registryAdmin, "application/json", jsonBody(map[string]any{"name": "io.github.acme", "kind": "github", "github_id": json.Number(githubID)}))
	reg.expect(t, 201, "POST", "/v1/admin/namespaces", registryAdmin, "application/json", jsonBody(map[string]any{"name": "io.github.other", "kind": "github", "github_id": 5}))
	pub := reg.expect(t, 201, "POST", "/v1/admin/tokens", registryAdmin, "application/json", jsonBody(map[string]any{"name": "pub", "scopes": []string{"skills:read", "skills:write"},
		"roles": []map[string]string{{"namespace": "io.github.acme", "role": "publisher"}, {"namespace": "io.github.other", "role": "publisher"}}}))["token"].(string)
	upload := func(ns, name, query, contentType string, body []byte) string {
		doc := reg.expect(t, 201, "POST", "/v1/skills/"+ns+"/"+name+"/versions?"+query, pub, contentType, body)
		return doc["artifact"].(map[string]any)["digest"].(string)
	}
	helloDigest := upload("io.github.acme", "hello", "version=1.0.0", "text/markdown", skillMD("hello", "Says hello", "MIT"))
	bundle := []byte(`{"required_commands":["git"],"name":"bundled","description":"A Core bundle","files":{"SKILL.md":"---\nname: bundled\ndescription: A Core bundle\n---\nUse git.\n"},"capability_ids":[],"providers":["codex"]}`)
	bundleDigest := upload("io.github.acme", "bundled", "version=2&license=MIT", "application/vnd.openlinker.skill-bundle.v1+json", bundle)
	upload("io.github.acme", "secret", "version=1&visibility=private", "text/markdown", skillMD("secret", "Private one", "MIT"))
	upload("io.github.acme", "inside", "version=1&visibility=internal", "text/markdown", skillMD("inside", "Internal one", "MIT"))
	upload("io.github.other", "locked", "version=1&visibility=private", "text/markdown", skillMD("locked", "Someone else's", "MIT"))
	tarDigest := upload("io.github.acme", "tarred", "version=3", "application/gzip", tgz(t, map[string][]byte{"tarred/SKILL.md": skillMD("tarred", "Two files", "Apache-2.0"), "tarred/references/notes.md": []byte("notes")}))
	bigDigest := upload("io.github.acme", "big", "version=1", "text/markdown", append(skillMD("big", "Too big for Core", "MIT"), bytes.Repeat([]byte("x"), 70<<10)...))

	site := httptest.NewServer(nil)
	defer site.Close()
	archive := tgz(t, map[string][]byte{"SKILL.md": skillMD("nolic", "No license", ""), "extra.md": []byte("extra")})
	site.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/agent-skills/index.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(jsonBody(map[string]any{"$schema": "https://schemas.agentskills.io/discovery/0.2.0/schema.json",
				"skills": []map[string]any{{"name": "nolic", "type": "archive", "description": "No license", "url": site.URL + "/nolic.tar.gz", "digest": shaTag(archive)}}}))
		case "/nolic.tar.gz":
			_, _ = w.Write(archive)
		default:
			http.NotFound(w, r)
		}
	})
	reg.expect(t, 201, "POST", "/v1/admin/upstreams", registryAdmin, "application/json", jsonBody(map[string]any{"name": "site", "kind": "agent-skills",
		"base_url": site.URL, "target_namespace": "com.example.site", "visibility": "public"}))
	reg.expect(t, 200, "POST", "/v1/admin/upstreams/site/sync?wait=true", registryAdmin, "", nil)
	nolicDoc := reg.expect(t, 200, "GET", "/v1/skills/com.example.site/nolic", "", "", nil)
	nolicVersion := nolicDoc["latest_version"].(string)
	nolicDigest := reg.expect(t, 200, "GET", "/v1/skills/com.example.site/nolic/versions/"+nolicVersion, "", "", nil)["artifact"].(map[string]any)["digest"].(string)

	server := func(name, kind, endpoint string) []byte {
		doc := map[string]any{"$schema": "https://static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json",
			"name": "io.github.acme/" + name, "description": "The " + name + " server", "version": "1.0.0"}
		if kind == "packages" {
			doc["packages"] = []map[string]any{{"registryType": "npm", "identifier": "@acme/" + name, "version": "1.0.0", "transport": map[string]any{"type": "stdio"}}}
		} else {
			doc["remotes"] = []map[string]any{{"type": kind, "url": endpoint, "headers": []map[string]any{{"name": "X-API-Key", "isSecret": true, "isRequired": true, "description": "API key"}}}}
		}
		return jsonBody(doc)
	}
	reg.expect(t, 201, "POST", "/v1/mcp/servers/io.github.acme", pub, "application/json", server("weather", "streamable-http", "https://127.0.0.1:9/mcp"))
	reg.expect(t, 201, "POST", "/v1/mcp/servers/io.github.acme", pub, "application/json", server("legacy", "sse", "https://127.0.0.1:9/sse"))
	reg.expect(t, 201, "POST", "/v1/mcp/servers/io.github.acme", pub, "application/json", server("local", "packages", ""))
	reg.expect(t, 201, "POST", "/v1/mcp/servers/io.github.acme", pub, "application/json", server("old", "streamable-http", "https://127.0.0.1:9/old"))
	built := map[string]any{}
	require.NoError(t, json.Unmarshal(server("_built", "streamable-http", "https://127.0.0.1:9/built"), &built))
	built["version"] = "1.0.0+build.1"
	reg.expect(t, 201, "POST", "/v1/mcp/servers/io.github.acme", pub, "application/json", jsonBody(built))
	reg.expect(t, 200, "POST", "/v1/mcp/servers/io.github.acme/old/status", pub, "application/json", jsonBody(map[string]any{"status": "deprecated"}))

	gw := reg.expect(t, 201, "POST", "/v1/admin/tokens", registryAdmin, "application/json", jsonBody(map[string]any{"name": "core", "scopes": []string{"skills:read"}}))
	reg.expect(t, 201, "POST", "/v1/admin/gateways", registryAdmin, "application/json", jsonBody(map[string]any{"consumer": "core-it", "token_id": gw["id"], "identities": []string{"openlinker", "github"}}))
	pubKey, privKey, _ := ed25519.GenerateKey(nil)
	reg.expect(t, 201, "POST", "/v1/admin/gateways/core-it/keys", registryAdmin, "application/json", jsonBody(map[string]any{"kid": "k1", "public_key": base64.StdEncoding.EncodeToString(pubKey)}))

	entry := func(name, audience string, signed bool) string {
		s := `{"name":"` + name + `","url":"` + reg.base + `","consumer":"core-it","token":"` + gw["token"].(string) + `","audience":"` + audience + `","allow_http":true,"allow_private_network":true`
		if signed {
			s += `,"assertion_audience":"` + assertionAudience + `","signing_key_id":"k1","signing_key":"` + base64.StdEncoding.EncodeToString(privKey.Seed()) + `"`
		}
		return s + `}`
	}
	registries, err := catalogregistry.Parse("[" + entry("pub", "public", true) + "," + entry("mem", "members", false) + "," + entry("adm", "admins", true) + "]")
	require.NoError(t, err)
	core := newCore(t, registries)
	alice := insertUser(t, core.pool, githubID, true, false)
	bob := insertUser(t, core.pool, "", true, false)
	carol := insertUser(t, core.pool, "", false, false)
	admin := insertUser(t, core.pool, "", false, true)

	t.Run("audience tiers", func(t *testing.T) {
		list := func(user uuid.UUID) string {
			doc := core.expect(200, "GET", "/api/v1/catalog/registries", user, nil)
			var out []string
			for _, it := range doc["items"].([]any) {
				out = append(out, it.(map[string]any)["name"].(string))
			}
			return strings.Join(out, ",")
		}
		require.Equal(t, "pub", list(uuid.Nil))
		require.Equal(t, "pub,mem", list(bob))
		require.Equal(t, "pub,mem,adm", list(admin))
		core.expect(404, "GET", "/api/v1/catalog/registries/mem/skills", uuid.Nil, nil)
		core.expect(404, "GET", "/api/v1/catalog/registries/adm/skills", bob, nil)
		core.expect(200, "GET", "/api/v1/catalog/registries/adm/skills", admin, nil)
		core.expect(401, "GET", "/api/v1/catalog/registries", uuid.New(), nil)
	})

	t.Run("browsing respects who is asking and caches only unsigned answers", func(t *testing.T) {
		require.Equal(t, "com.example.site/nolic,io.github.acme/big,io.github.acme/bundled,io.github.acme/hello,io.github.acme/tarred",
			names(core.expect(200, "GET", "/api/v1/catalog/registries/pub/skills", uuid.Nil, nil), "skills"))
		require.Equal(t, "com.example.site/nolic,io.github.acme/big,io.github.acme/bundled,io.github.acme/hello,io.github.acme/inside,io.github.acme/secret,io.github.acme/tarred",
			names(core.expect(200, "GET", "/api/v1/catalog/registries/pub/skills", alice, nil), "skills"))
		core.expect(404, "GET", "/api/v1/catalog/registries/pub/skills/io.github.other/locked", alice, nil)
		core.expect(404, "GET", "/api/v1/catalog/registries/pub/skills/io.github.acme/secret", uuid.Nil, nil)
		upload("io.github.acme", "late", "version=1", "text/markdown", skillMD("late", "Arrives later", "MIT"))
		require.NotContains(t, names(core.expect(200, "GET", "/api/v1/catalog/registries/pub/skills", uuid.Nil, nil), "skills"), "late")
		require.Contains(t, names(core.expect(200, "GET", "/api/v1/catalog/registries/pub/skills", alice, nil), "skills"), "late")
		status, body, header := core.call("GET", "/api/v1/catalog/registries/pub/skills/io.github.acme/hello/versions/1.0.0", uuid.Nil, nil)
		require.Equal(t, 200, status)
		require.Equal(t, "private, no-store", header.Get("Cache-Control"))
		require.NotContains(t, string(body), reg.base)
		require.Contains(t, string(body), helloDigest)
		status, body, header = core.call("GET", "/api/v1/catalog/registries/pub/skills/io.github.acme/tarred/versions/3/files/references/notes.md", uuid.Nil, nil)
		require.Equal(t, 200, status, string(body))
		require.Equal(t, "notes", string(body))
		require.Equal(t, "nosniff", header.Get("X-Content-Type-Options"))
		require.Contains(t, header.Get("Content-Security-Policy"), "sandbox")
		status, body, _ = core.call("GET", "/api/v1/catalog/registries/pub/skills/com.example.site/nolic/versions/"+nolicVersion+"/files/extra.md", uuid.Nil, nil)
		require.Equal(t, 403, status)
		require.Contains(t, string(body), "CATALOG_CONTENT_WITHHELD")
		core.expect(400, "GET", "/api/v1/catalog/registries/pub/skills/io.github.acme/tarred/versions/3/files/..%2Fsecret", uuid.Nil, nil)
		core.expect(400, "GET", "/api/v1/catalog/registries/pub/skills?cursor="+strings.Repeat("a", 5000), uuid.Nil, nil)
		page := core.expect(200, "GET", "/api/v1/catalog/registries/pub/skills?limit=1", uuid.Nil, nil)
		cursor, _ := page["next_cursor"].(string)
		require.NotEmpty(t, cursor)
		next := core.expect(200, "GET", "/api/v1/catalog/registries/pub/skills?limit=1&cursor="+url.QueryEscape(cursor), uuid.Nil, nil)
		require.NotEqual(t, names(page, "skills"), names(next, "skills"))
		var servers []string
		for _, it := range core.expect(200, "GET", "/api/v1/catalog/registries/pub/mcp/servers", uuid.Nil, nil)["servers"].([]any) {
			servers = append(servers, it.(map[string]any)["name"].(string))
		}
		sort.Strings(servers)
		require.Equal(t, "io.github.acme/_built,io.github.acme/legacy,io.github.acme/local,io.github.acme/old,io.github.acme/weather", strings.Join(servers, ","))
	})

	importBody := func(registry, ns, name, version, digest string, providers ...string) map[string]any {
		return map[string]any{"registry": registry, "namespace": ns, "name": name, "version": version, "digest": digest, "providers": providers}
	}
	var helloCopy map[string]any

	t.Run("import hosted entries", func(t *testing.T) {
		core.expect(401, "POST", "/api/v1/creator/skill-packages/registry-imports", uuid.Nil, importBody("pub", "io.github.acme", "hello", "1.0.0", helloDigest, "codex"))
		helloCopy = core.expect(201, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("pub", "io.github.acme", "hello", "1.0.0", helloDigest, "codex"))
		again := core.expect(200, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("pub", "io.github.acme", "hello", "1.0.0", helloDigest, "codex"))
		require.Equal(t, helloCopy["version_id"], again["version_id"])
		conflict := core.expect(409, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("pub", "io.github.acme", "hello", "1.0.0", helloDigest, "claude"))
		require.Contains(t, fmt.Sprint(conflict), "SKILL_PACKAGE_IMPORT_CONFLICT")
		mismatch := core.expect(409, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("pub", "io.github.acme", "hello", "1.0.0", "sha256:"+strings.Repeat("0", 64), "codex"))
		require.Contains(t, fmt.Sprint(mismatch), "SKILL_PACKAGE_DIGEST_MISMATCH")
		var kind, entry, version, digest, mode, license string
		var redistributable bool
		var visibility string
		require.NoError(t, core.pool.QueryRow(context.Background(), `SELECT v.source_kind,v.source_registry_entry,v.source_registry_version,v.source_registry_digest,v.source_registry_mode,v.source_license,v.source_redistributable,p.visibility
 FROM skill_package_versions v JOIN skill_packages p ON p.id=v.package_id WHERE v.id=$1`, helloCopy["version_id"]).Scan(&kind, &entry, &version, &digest, &mode, &license, &redistributable, &visibility))
		require.Equal(t, []any{"registry", "io.github.acme/hello", "1.0.0", helloDigest, "hosted", "spdx:MIT", true, "private"}, []any{kind, entry, version, digest, mode, license, redistributable, visibility})

		b := core.expect(201, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("pub", "io.github.acme", "bundled", "2", bundleDigest))
		require.Equal(t, strings.TrimPrefix(bundleDigest, "sha256:"), b["digest"], "a Core bundle keeps its Core digest")
		var providers []string
		var payload string
		require.NoError(t, core.pool.QueryRow(context.Background(), `SELECT providers,payload FROM skill_package_versions WHERE id=$1`, b["version_id"]).Scan(&providers, &payload))
		require.Equal(t, []string{"codex"}, providers)
		require.Equal(t, string(bundle), payload)

		tr := core.expect(201, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("pub", "io.github.acme", "tarred", "3", tarDigest, "claude"))
		require.NoError(t, core.pool.QueryRow(context.Background(), `SELECT payload FROM skill_package_versions WHERE id=$1`, tr["version_id"]).Scan(&payload))
		require.Contains(t, payload, `"references/notes.md":"notes"`)

		big := core.expect(422, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("pub", "io.github.acme", "big", "1", bigDigest, "codex"))
		require.Contains(t, fmt.Sprint(big), "SKILL_PACKAGE_INCOMPATIBLE")
		core.expect(404, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("pub", "io.github.acme", "secret", "1", helloDigest, "codex"))
		core.expect(404, "POST", "/api/v1/creator/skill-packages/registry-imports", carol, importBody("adm", "io.github.acme", "hello", "1.0.0", helloDigest, "codex"))
	})

	t.Run("indexed content without a redistribution license stays private", func(t *testing.T) {
		unsigned := core.expect(409, "POST", "/api/v1/creator/skill-packages/registry-imports", bob, importBody("mem", "com.example.site", "nolic", nolicVersion, nolicDigest, "codex"))
		require.Contains(t, fmt.Sprint(unsigned), "SKILL_PACKAGE_SOURCE_UNAVAILABLE")
		copyDoc := core.expect(201, "POST", "/api/v1/creator/skill-packages/registry-imports", alice, importBody("pub", "com.example.site", "nolic", nolicVersion, nolicDigest, "codex"))
		var redistributable bool
		var license, mode string
		require.NoError(t, core.pool.QueryRow(context.Background(), `SELECT source_redistributable,source_license,source_registry_mode FROM skill_package_versions WHERE id=$1`, copyDoc["version_id"]).Scan(&redistributable, &license, &mode))
		require.False(t, redistributable)
		require.Equal(t, "unknown", license)
		require.Equal(t, "indexed", mode)
		pkg := "/api/v1/creator/skill-packages/" + copyDoc["id"].(string)
		for _, visibility := range []string{"public", "unlisted"} {
			doc := core.expect(409, "PATCH", pkg, alice, map[string]any{"visibility": visibility})
			require.Contains(t, fmt.Sprint(doc), "SKILL_PACKAGE_SOURCE_NOT_REDISTRIBUTABLE")
		}
		doc := core.expect(409, "PUT", pkg+"/versions/"+copyDoc["version_id"].(string)+"/publication", alice, nil)
		require.Contains(t, fmt.Sprint(doc), "SKILL_PACKAGE_SOURCE_NOT_REDISTRIBUTABLE")
		core.expect(200, "PATCH", pkg, alice, map[string]any{"visibility": "private"})
	})

	t.Run("copies of published registry imports keep their source", func(t *testing.T) {
		pkg := "/api/v1/creator/skill-packages/" + helloCopy["id"].(string)
		core.expect(200, "PUT", pkg+"/versions/"+helloCopy["version_id"].(string)+"/publication", bob, nil)
		core.expect(200, "PATCH", pkg, bob, map[string]any{"visibility": "public"})
		copied := core.expect(201, "POST", "/api/v1/creator/skill-packages/imports", carol, map[string]any{"source_package_id": helloCopy["id"], "source_version_id": helloCopy["version_id"], "expected_digest": helloCopy["digest"]})
		var kind, entry string
		var redistributable bool
		require.NoError(t, core.pool.QueryRow(context.Background(), `SELECT source_kind,source_registry_entry,source_redistributable FROM skill_package_versions WHERE id=$1`, copied["version_id"]).Scan(&kind, &entry, &redistributable))
		require.Equal(t, []any{"registry", "io.github.acme/hello", true}, []any{kind, entry, redistributable})
	})

	t.Run("mcp registration drafts", func(t *testing.T) {
		base := "/api/v1/catalog/registries/pub/mcp/servers/io.github.acme/"
		core.expect(401, "GET", base+"weather/versions/1.0.0/registration-draft", uuid.Nil, nil)
		d := core.expect(200, "GET", base+"weather/versions/1.0.0/registration-draft", alice, nil)
		require.Equal(t, "streamable_http", d["mcp_transport"])
		require.Equal(t, "https://127.0.0.1:9/mcp", d["endpoint_url"])
		require.Equal(t, "X-API-Key", d["endpoint_auth_header_name"])
		require.Equal(t, "mcp_server", d["connection_mode"])
		require.Equal(t, "legacy_sse", core.expect(200, "GET", base+"legacy/versions/1.0.0/registration-draft", alice, nil)["mcp_transport"])
		require.Contains(t, fmt.Sprint(core.expect(422, "GET", base+"local/versions/1.0.0/registration-draft", alice, nil)), "CATALOG_MCP_SELF_HOST_REQUIRED")
		require.Contains(t, fmt.Sprint(core.expect(409, "GET", base+"old/versions/1.0.0/registration-draft", alice, nil)), "CATALOG_MCP_INACTIVE")
		require.Equal(t, "1.0.0+build.1", core.expect(200, "GET", base+"_built/versions/1.0.0%2Bbuild.1/registration-draft", alice, nil)["version"])
	})

	t.Run("mcp registration rechecks status and records provenance atomically", func(t *testing.T) {
		base := "/api/v1/catalog/registries/pub/mcp/servers/io.github.acme/"
		body := func(slug string) map[string]any {
			return map[string]any{"slug": slug, "name": "Weather MCP", "tags": []string{"weather"}, "mcp_tool_name": "forecast", "endpoint_auth_header": "secret-value"}
		}
		slug := "weather-" + uuid.NewString()[:8]
		doc := core.expect(201, "POST", base+"weather/versions/1.0.0/registrations", alice, body(slug))
		agentID := doc["agent"].(map[string]any)["id"].(string)
		var registryURL, serverName, serverVersion, transport, headerName string
		require.NoError(t, core.pool.QueryRow(context.Background(), `SELECT s.registry_url,s.server_name,s.server_version,a.mcp_transport,a.endpoint_auth_header_name FROM agent_catalog_sources s JOIN agents a ON a.id=s.agent_id WHERE s.agent_id=$1`, agentID).
			Scan(&registryURL, &serverName, &serverVersion, &transport, &headerName))
		require.Equal(t, []any{reg.base, "io.github.acme/weather", "1.0.0", "streamable_http", "X-API-Key"}, []any{registryURL, serverName, serverVersion, transport, headerName})
		require.NotContains(t, fmt.Sprint(doc), "secret-value")
		core.expect(403, "POST", base+"weather/versions/1.0.0/registrations", carol, body("weather-"+uuid.NewString()[:8]))

		_, err := core.pool.Exec(context.Background(), `CREATE OR REPLACE FUNCTION catalog_test_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused by test'; END $$;
 CREATE TRIGGER catalog_test_refuse BEFORE INSERT ON agent_catalog_sources FOR EACH ROW EXECUTE FUNCTION catalog_test_refuse()`)
		require.NoError(t, err)
		failing := "weather-" + uuid.NewString()[:8]
		status, _, _ := core.call("POST", base+"weather/versions/1.0.0/registrations", alice, body(failing))
		_, dropErr := core.pool.Exec(context.Background(), `DROP TRIGGER catalog_test_refuse ON agent_catalog_sources; DROP FUNCTION catalog_test_refuse()`)
		require.NoError(t, dropErr)
		require.Equal(t, 500, status)
		var count int
		require.NoError(t, core.pool.QueryRow(context.Background(), `SELECT count(*) FROM agents WHERE slug=$1`, failing).Scan(&count))
		require.Zero(t, count, "the Agent must roll back with its provenance")

		reg.expect(t, 200, "POST", "/v1/mcp/servers/io.github.acme/weather/status", pub, "application/json", jsonBody(map[string]any{"status": "deprecated"}))
		late := core.expect(409, "POST", base+"weather/versions/1.0.0/registrations", alice, body("weather-"+uuid.NewString()[:8]))
		require.Contains(t, fmt.Sprint(late), "CATALOG_MCP_INACTIVE")
	})

	t.Run("registry outage leaves imported copies intact", func(t *testing.T) {
		reg.stop()
		down := core.expect(502, "GET", "/api/v1/catalog/registries/pub/skills/io.github.acme/hello", alice, nil)
		require.Contains(t, fmt.Sprint(down), "CATALOG_UNAVAILABLE")
		var payload, digest string
		require.NoError(t, core.pool.QueryRow(context.Background(), `SELECT payload,digest FROM skill_package_versions WHERE id=$1`, helloCopy["version_id"]).Scan(&payload, &digest))
		sum := sha256.Sum256([]byte(payload))
		require.Equal(t, hex.EncodeToString(sum[:]), digest)
	})
}
