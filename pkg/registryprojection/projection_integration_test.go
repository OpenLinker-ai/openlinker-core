package registryprojection_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcp"
	"github.com/OpenLinker-ai/openlinker-core/pkg/registryprojection"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

const registryAdmin = "registry-admin-token-0123456789abcdefgh"
const audience = "https://registry.test/registry"

type registryProcess struct {
	base string
	cmd  *exec.Cmd
	env  []string
}

func (r *registryProcess) call(t *testing.T, method, path, token string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, r.base+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	var doc map[string]any
	_ = json.NewDecoder(res.Body).Decode(&doc)
	return res.StatusCode, doc
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().String()
}

func createDatabase(t *testing.T, baseURL, prefix string) string {
	t.Helper()
	parsed, err := url.Parse(baseURL)
	require.NoError(t, err)
	name := prefix + strings.ReplaceAll(uuid.NewString(), "-", "")
	admin := *parsed
	admin.Path = "/postgres"
	conn, err := pgx.Connect(context.Background(), admin.String())
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'")
	conn.Close(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin.String())
		if err == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
			c.Close(context.Background())
		}
	})
	out := *parsed
	out.Path = "/" + name
	return out.String()
}

func startRegistry(t *testing.T, env []string) *registryProcess {
	t.Helper()
	cmd := exec.Command(os.Getenv("REGISTRY_TEST_BINARY"))
	cmd.Env = append(os.Environ(), env...)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	require.NoError(t, cmd.Start())
	base := ""
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "REGISTRY_LISTEN="); ok {
			base = "http://" + v + "/registry"
		}
	}
	p := &registryProcess{base: base, cmd: cmd, env: env}
	t.Cleanup(p.stop)
	deadline := time.Now().Add(20 * time.Second)
	for {
		if res, err := http.Get(base + "/readyz"); err == nil {
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

func (r *registryProcess) stop() {
	if r.cmd.Process != nil {
		_ = r.cmd.Process.Kill()
		_, _ = r.cmd.Process.Wait()
	}
}

type fixture struct {
	t       *testing.T
	pool    *pgxpool.Pool
	reg     *registryProcess
	proj    *registryprojection.Projector
	gateway string
	e       *echo.Echo
}

func (f fixture) exec(sql string, args ...any) {
	f.t.Helper()
	_, err := f.pool.Exec(context.Background(), sql, args...)
	require.NoError(f.t, err)
}

func (f fixture) drain() {
	f.t.Helper()
	require.NoError(f.t, f.proj.Drain(context.Background(), 30*time.Second))
}

func (f fixture) state(kind, name string, owner uuid.UUID) (int, map[string]any) {
	f.t.Helper()
	client := f.proj.Client()
	res, err := client.Project(context.Background(), "projection", http.MethodGet, "/v1/projections/"+kind+"/"+name, "", nil, &catalogregistry.User{ID: owner})
	require.NoError(f.t, err)
	var doc map[string]any
	_ = json.Unmarshal(res.Body, &doc)
	return res.Status, doc
}

func strs(v any) string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, x := range list {
		out = append(out, fmt.Sprint(x))
	}
	return strings.Join(out, ",")
}

func bundle(name, description string) (string, string) {
	files, _ := json.Marshal(map[string]string{"SKILL.md": "---\nname: " + name + "\ndescription: " + description + "\n---\nBody.\n"})
	payload := `{"required_commands":[],"name":"` + name + `","description":"` + description + `","files":` + string(files) + `,"capability_ids":[],"providers":["codex"]}`
	sum := sha256.Sum256([]byte(payload))
	return payload, hex.EncodeToString(sum[:])
}

func setup(t *testing.T) fixture {
	t.Helper()
	if os.Getenv("REGISTRY_TEST_BINARY") == "" || os.Getenv("REGISTRY_TEST_DATABASE_URL") == "" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("REGISTRY_TEST_BINARY, REGISTRY_TEST_DATABASE_URL and TEST_DATABASE_URL are required")
	}
	coreURL := createDatabase(t, os.Getenv("TEST_DATABASE_URL"), "core_projection_")
	dir, _ := filepath.Abs(filepath.Join("..", "..", "migrations"))
	m, err := migrate.New("file://"+dir, coreURL)
	require.NoError(t, err)
	require.NoError(t, m.Up())
	_, _ = m.Close()
	addr := freePort(t)
	reg := startRegistry(t, []string{"REGISTRY_LISTEN=" + addr, "REGISTRY_BASE_PATH=/registry", "REGISTRY_MODE=public", "REGISTRY_REVIEW=true",
		"REGISTRY_PUBLIC_URL=http://" + addr + "/registry", "REGISTRY_ASSERTION_AUDIENCE=" + audience,
		"REGISTRY_DATABASE_URL=" + createDatabase(t, os.Getenv("REGISTRY_TEST_DATABASE_URL"), "registry_projection_"),
		"REGISTRY_STORAGE_DIR=" + t.TempDir(), "REGISTRY_ADMIN_TOKEN=" + registryAdmin,
		"REGISTRY_RATE_LIMIT_READ=0", "REGISTRY_RATE_LIMIT_WRITE=0", "REGISTRY_BACKGROUND_SYNC=false"})
	_, tok := reg.call(t, "POST", "/v1/admin/tokens", registryAdmin, map[string]any{"name": "core", "scopes": []string{"skills:read", "skills:write"}})
	reg.call(t, "POST", "/v1/admin/gateways", registryAdmin, map[string]any{"consumer": "core-p", "token_id": tok["id"], "identities": []string{"openlinker"}})
	pub, priv, _ := ed25519.GenerateKey(nil)
	reg.call(t, "POST", "/v1/admin/gateways/core-p/keys", registryAdmin, map[string]any{"kid": "k1", "public_key": base64.StdEncoding.EncodeToString(pub)})
	status, _ := reg.call(t, "PATCH", "/v1/admin/gateways/core-p", registryAdmin, map[string]any{"projection": true})
	require.Equal(t, 200, status)
	regs, err := catalogregistry.Parse(`[{"name":"projection","url":"` + reg.base + `","consumer":"core-p","token":"` + tok["token"].(string) +
		`","audience":"admins","allow_http":true,"allow_private_network":true,"assertion_audience":"` + audience +
		`","signing_key_id":"k1","signing_key":"` + base64.StdEncoding.EncodeToString(priv.Seed()) + `"}]`)
	require.NoError(t, err)
	pool, err := pgxpool.New(context.Background(), coreURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	proj, err := registryprojection.New(pool, catalogregistry.NewClient(regs, catalogregistry.Options{}), "projection", "https://openlinker.example")
	require.NoError(t, err)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	e.Use(proj.CompatMiddleware())
	api := e.Group("/api/v1")
	skillpackage.NewHandler(pool).RegisterPublic(api)
	mcp.NewCatalogHandler(mcp.NewDirectory(pool)).Register(api)
	proj.RegisterLegacy(api)
	return fixture{t: t, pool: pool, reg: reg, proj: proj, gateway: tok["token"].(string), e: e}
}

func (f fixture) get(path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	f.e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestProjectionThroughRealRegistry(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	alice, bob := uuid.New(), uuid.New()
	for _, u := range []uuid.UUID{alice, bob} {
		f.exec(`INSERT INTO users(id,email,password_hash,display_name) VALUES($1,$2,'x','User')`, u, u.String()+"@example.invalid")
	}
	pkg := func(owner uuid.UUID, name, visibility string) uuid.UUID {
		id := uuid.New()
		f.exec(`INSERT INTO skill_packages(id,owner_user_id,name,description,visibility) VALUES($1,$2,$3,'Draft display text','`+visibility+`')`, id, owner, name)
		return id
	}
	version := func(pkgID uuid.UUID, label, name, description string, published bool) uuid.UUID {
		id := uuid.New()
		payload, digest := bundle(name, description)
		pub := "NULL"
		if published {
			pub = "now()"
		}
		f.exec(`INSERT INTO skill_package_versions(id,package_id,version,digest,payload,providers,published_at,publication_metadata,created_at)
			VALUES($1,$2,$3,$4,$5,ARRAY['codex'],`+pub+`,'{}'::jsonb,clock_timestamp())`, id, pkgID, label, digest, payload)
		return id
	}
	p1 := pkg(alice, "pdf-tools", "public")
	p1v1 := version(p1, "1.0.0", "pdf-tools", "Read PDFs", true)
	version(p1, "1.1.0", "pdf-tools", "Read and split PDFs", true)
	p2 := pkg(alice, "notes", "unlisted")
	version(p2, "1", "notes", "Take notes", true)
	p3 := pkg(alice, "secret", "private")
	version(p3, "1", "secret", "Private", true)
	p4 := pkg(bob, "draft", "public")
	version(p4, "1", "draft", "Draft only", false)
	p5 := pkg(bob, "odd", "public")
	version(p5, "bad version", "odd", "Odd label", true)
	agent := uuid.New()
	f.exec(`INSERT INTO agents(id,creator_id,slug,name,description,endpoint_url,connection_mode,visibility,lifecycle_status,mcp_tool_name,tags,price_per_call_cents)
		VALUES($1,$2,'weather','Weather','Forecasts','https://mcp.example/mcp','mcp_server','public','active','forecast',ARRAY['weather'],0)`, agent, alice)
	hidden := uuid.New()
	f.exec(`INSERT INTO agents(id,creator_id,slug,name,description,endpoint_url,connection_mode,visibility,lifecycle_status,mcp_tool_name,price_per_call_cents)
		VALUES($1,$2,'private-mcp','Private','Hidden','https://mcp.example/p','mcp_server','private','active','x',0)`, hidden, alice)
	inst, err := f.proj.Instance(ctx)
	require.NoError(t, err)
	name := func(prefix string, id uuid.UUID) string {
		return prefix + "-" + strings.ReplaceAll(inst.String(), "-", "")[:8] + "-" + strings.ReplaceAll(id.String(), "-", "")
	}

	f.exec(`UPDATE skill_package_versions SET publication_metadata = '{"license":"MIT"}'::jsonb WHERE id = $1`, p1v1)

	t.Run("a target without projection permission blocks preflight", func(t *testing.T) {
		status, _ := f.reg.call(t, "PATCH", "/v1/admin/gateways/core-p", registryAdmin, map[string]any{"projection": false})
		require.Equal(t, 200, status)
		pre, err := f.proj.Preflight(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, pre.Blockers)
		require.Equal(t, "target_unusable", pre.Blockers[0].Code)
		status, _ = f.reg.call(t, "PATCH", "/v1/admin/gateways/core-p", registryAdmin, map[string]any{"projection": true})
		require.Equal(t, 200, status)
	})

	t.Run("inventory and preflight", func(t *testing.T) {
		inv, err := f.proj.Inventory(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, inv.SkillPackages)
		require.Equal(t, 4, inv.SkillVersions)
		require.Equal(t, 1, inv.MCPServices)
		pre, err := f.proj.Preflight(ctx)
		require.NoError(t, err)
		require.Len(t, pre.Blockers, 1)
		require.Equal(t, p5.String(), pre.Blockers[0].Subject)
		_, err = f.proj.Cutover(ctx, time.Minute)
		require.ErrorContains(t, err, "blocker")
		require.NoError(t, f.proj.Exclude(ctx, "skill", p5, nil, "label is not a Registry version"))
		pre, err = f.proj.Preflight(ctx)
		require.NoError(t, err)
		require.Empty(t, pre.Blockers)
	})

	t.Run("preflight works with a single database connection", func(t *testing.T) {
		cfg := f.pool.Config().Copy()
		cfg.MaxConns = 1
		single, err := pgxpool.NewWithConfig(ctx, cfg)
		require.NoError(t, err)
		defer single.Close()
		proj, err := registryprojection.New(single, f.proj.Client(), "projection", "https://openlinker.example")
		require.NoError(t, err)
		bounded, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		_, err = proj.Preflight(bounded)
		require.NoError(t, err)
	})

	t.Run("cutover projects exactly the public catalog", func(t *testing.T) {
		report, err := f.proj.Cutover(ctx, time.Minute)
		require.NoError(t, err)
		require.True(t, report.Complete, "%+v", report)
		status, doc := f.state("skills", name("p", p1), alice)
		require.Equal(t, 200, status)
		require.Equal(t, false, doc["hidden"])
		require.Equal(t, "1.0.0,1.1.0", strs(doc["versions"]))
		require.Equal(t, "1.1.0", doc["current_version"])
		status, doc = f.state("skills", name("p", p2), alice)
		require.Equal(t, 200, status)
		require.Equal(t, false, doc["listed"])
		status, _ = f.state("skills", name("p", p3), alice)
		require.Equal(t, 404, status)
		status, _ = f.state("skills", name("p", p4), bob)
		require.Equal(t, 404, status)
		status, doc = f.state("mcp", name("m", agent), alice)
		require.Equal(t, 200, status)
		require.Equal(t, false, doc["hidden"])
		status, _ = f.state("mcp", name("m", hidden), alice)
		require.Equal(t, 404, status)
		var linked int
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM skill_package_registry_links l JOIN skill_package_versions v ON v.id=l.version_id
			WHERE l.registry_digest = 'sha256:' || v.digest`).Scan(&linked))
		require.Equal(t, 3, linked)
		ns := doc["namespace"]
		pub, _ := f.reg.call(t, "GET", fmt.Sprintf("/v1/skills/%s/%s", ns, name("p", p1)), "", nil)
		require.Equal(t, 200, pub)
		pub, body := f.reg.call(t, "GET", "/v0.1/servers?search="+url.QueryEscape(name("m", agent)), "", nil)
		require.Equal(t, 200, pub)
		raw, _ := json.Marshal(body)
		require.Contains(t, string(raw), "https://openlinker.example/mcp/agents/"+agent.String())
		require.NotContains(t, string(raw), "mcp.example/mcp")
	})

	t.Run("declared licenses and display names are kept", func(t *testing.T) {
		ns := ""
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT registry_namespace FROM registry_projection_links WHERE kind = 'skill' AND subject_id = $1`, p1).Scan(&ns))
		status, doc := f.reg.call(t, "GET", fmt.Sprintf("/v1/skills/%s/%s/versions/1.0.0", ns, name("p", p1)), registryAdmin, nil)
		require.Equal(t, 200, status)
		lic, _ := doc["license"].(map[string]any)
		require.Equal(t, "spdx", lic["kind"], "%v", doc["license"])
		require.Equal(t, "MIT", lic["id"], "%v", doc["license"])
		status, doc = f.reg.call(t, "GET", fmt.Sprintf("/v1/skills/%s/%s", ns, name("p", p1)), "", nil)
		require.Equal(t, 200, status)
		require.Equal(t, "pdf-tools", doc["display_name"])
	})

	t.Run("compatibility headers and legacy lookups", func(t *testing.T) {
		rec := f.get("/api/v1/skill-packages/" + p1.String())
		require.Equal(t, 200, rec.Code)
		require.Equal(t, "true", rec.Header().Get("Deprecation"))
		require.Contains(t, rec.Header().Get("Link"), name("p", p1))
		require.True(t, strings.HasPrefix(rec.Header().Get("Link"), "<"+audience+"/v1/skills/"), rec.Header().Get("Link"))
		rec = f.get("/api/v1/catalog/legacy/skill-packages/" + p1.String())
		require.Equal(t, 200, rec.Code)
		require.Contains(t, rec.Body.String(), name("p", p1))
		require.Equal(t, 404, f.get("/api/v1/catalog/legacy/skill-packages/"+p3.String()).Code)
	})

	t.Run("withdrawing the only published version drops the legacy mapping at once", func(t *testing.T) {
		require.Equal(t, 200, f.get("/api/v1/catalog/legacy/skill-packages/"+p2.String()).Code)
		f.exec(`UPDATE skill_package_versions SET published_at = NULL WHERE package_id = $1`, p2)
		require.Equal(t, 404, f.get("/api/v1/catalog/legacy/skill-packages/"+p2.String()).Code)
		f.exec(`UPDATE skill_package_versions SET published_at = now() WHERE package_id = $1`, p2)
		f.drain()
	})

	t.Run("withdrawal and owner shutdown", func(t *testing.T) {
		f.exec(`UPDATE skill_package_versions SET published_at=NULL WHERE id=$1`, p1v1)
		f.drain()
		_, doc := f.state("skills", name("p", p1), alice)
		require.Equal(t, "1.1.0", strs(doc["versions"]))
		f.exec(`UPDATE users SET disabled_at=now() WHERE id=$1`, alice)
		f.drain()
		for _, s := range []struct {
			kind, name string
		}{{"skills", name("p", p1)}, {"skills", name("p", p2)}, {"mcp", name("m", agent)}} {
			_, doc := f.state(s.kind, s.name, alice)
			require.Equal(t, true, doc["hidden"], s.name)
		}
		require.Equal(t, 404, f.get("/api/v1/catalog/legacy/skill-packages/"+p1.String()).Code)
		require.NotContains(t, f.get("/api/v1/mcp-services").Body.String(), "weather")
		require.Equal(t, 404, f.get("/api/v1/mcp-services/weather/metadata").Code)
		f.exec(`UPDATE users SET disabled_at=NULL WHERE id=$1`, alice)
		f.drain()
		_, doc = f.state("skills", name("p", p1), alice)
		require.Equal(t, false, doc["hidden"])
	})

	t.Run("an MCP Agent moved to another owner is listed under the new owner", func(t *testing.T) {
		f.exec(`UPDATE agents SET creator_id = $1 WHERE id = $2`, bob, agent)
		f.drain()
		_, doc := f.state("mcp", name("m", agent), alice)
		require.Equal(t, true, doc["hidden"])
		status, doc := f.state("mcp", name("m", agent), bob)
		require.Equal(t, 200, status)
		require.Equal(t, false, doc["hidden"])
		f.exec(`UPDATE agents SET creator_id = $1 WHERE id = $2`, alice, agent)
		f.drain()
		_, doc = f.state("mcp", name("m", agent), alice)
		require.Equal(t, false, doc["hidden"])
	})

	t.Run("an MCP Agent with Core-valid text outside the Registry limits is still listed", func(t *testing.T) {
		odd := uuid.New()
		f.exec(`INSERT INTO agents(id,creator_id,slug,name,description,endpoint_url,connection_mode,visibility,lifecycle_status,mcp_tool_name,price_per_call_cents)
			VALUES($1,$2,'odd-mcp',$3,'','https://mcp.example/odd','mcp_server','public','active','x',0)`, odd, alice, strings.Repeat("n", 150))
		f.drain()
		status, doc := f.state("mcp", name("m", odd), alice)
		require.Equal(t, 200, status)
		require.Equal(t, false, doc["hidden"])
	})

	t.Run("a name conflict suspends the package without failing its versions", func(t *testing.T) {
		ns := ""
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT registry_namespace FROM registry_projection_links WHERE kind = 'skill' AND subject_id = $1`, p1).Scan(&ns))
		taken := uuid.New()
		md := []byte("---\nname: squatter\ndescription: Not from Core\nlicense: MIT\n---\nBody.\n")
		req, _ := http.NewRequest("POST", f.reg.base+"/v1/skills/"+ns+"/"+name("p", taken)+"/versions?version=1&license=MIT", bytes.NewReader(md))
		req.Header.Set("Authorization", "Bearer "+registryAdmin)
		req.Header.Set("Content-Type", "text/markdown")
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		res.Body.Close()
		require.Equal(t, 201, res.StatusCode)
		f.exec(`INSERT INTO skill_packages(id,owner_user_id,name,description,visibility) VALUES($1,$2,'squatted','d','public')`, taken, alice)
		version(taken, "1", "squatted", "Conflicting name", true)
		_ = f.proj.Drain(ctx, 3*time.Second)
		var suspended bool
		var lastError string
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT suspended, last_error FROM registry_projection_dirty WHERE subject_id = $1`, taken).Scan(&suspended, &lastError))
		require.True(t, suspended)
		require.Contains(t, lastError, "name_conflict")
		var failed int
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM registry_projection_version_errors WHERE package_id = $1`, taken).Scan(&failed))
		require.Zero(t, failed)
		f.exec(`UPDATE skill_packages SET visibility = 'private' WHERE id = $1`, taken)
		f.drain()
	})

	t.Run("unrelated updates mark nothing", func(t *testing.T) {
		f.exec(`UPDATE agents SET total_calls = total_calls + 1, updated_at = now() WHERE id=$1`, agent)
		f.exec(`UPDATE users SET password_hash='y', display_name='Renamed' WHERE id=$1`, alice)
		var dirty int
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM registry_projection_dirty`).Scan(&dirty))
		require.Zero(t, dirty)
	})

	t.Run("reconciliation repairs a lost mark", func(t *testing.T) {
		f.exec(`UPDATE skill_packages SET visibility='private' WHERE id=$1`, p2)
		f.exec(`DELETE FROM registry_projection_dirty`)
		rep, err := f.proj.Reconcile(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, rep.Marked)
		f.drain()
		_, doc := f.state("skills", name("p", p2), alice)
		require.Equal(t, true, doc["hidden"])
	})

	t.Run("an outage keeps work queued", func(t *testing.T) {
		f.reg.stop()
		version(p1, "1.2.0", "pdf-tools", "Read, split and merge PDFs", true)
		processed, err := f.proj.RunOnce(ctx)
		require.Error(t, err)
		require.False(t, processed)
		var dirty int
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM registry_projection_dirty WHERE claim_token IS NULL`).Scan(&dirty))
		require.Equal(t, 1, dirty)
	})

	t.Run("rollback refuses pending work and then returns to local", func(t *testing.T) {
		_, err := f.proj.Rollback(ctx, 2*time.Second)
		require.Error(t, err)
		st, err := f.proj.Status(ctx)
		require.NoError(t, err)
		require.Equal(t, "draining", st.Mode)
		f.reg = startRegistry(f.t, f.reg.env)
		_, err = f.proj.Rollback(ctx, 30*time.Second)
		require.NoError(t, err)
		st, err = f.proj.Status(ctx)
		require.NoError(t, err)
		require.Equal(t, "local", st.Mode)
		_, doc := f.state("skills", name("p", p1), alice)
		require.Equal(t, "1.1.0,1.2.0", strs(doc["versions"]))
		f.exec(`UPDATE skill_packages SET visibility='public' WHERE id=$1`, p2)
		var dirty int
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM registry_projection_dirty`).Scan(&dirty))
		require.Zero(t, dirty)
		require.Empty(t, f.get("/api/v1/skill-packages/"+p1.String()).Header().Get("Deprecation"))
		f.exec(`UPDATE users SET disabled_at=now() WHERE id=$1`, alice)
		require.Contains(t, f.get("/api/v1/mcp-services").Body.String(), "weather")
		require.Equal(t, 200, f.get("/api/v1/mcp-services/weather/metadata").Code)
	})

	t.Run("a new cutover hides what changed while local", func(t *testing.T) {
		f.exec(`UPDATE skill_packages SET visibility = 'private' WHERE id = $1`, p1)
		var dirty int
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM registry_projection_dirty`).Scan(&dirty))
		require.Zero(t, dirty)
		pre, err := f.proj.Preflight(ctx)
		require.NoError(t, err)
		require.Empty(t, pre.Blockers)
		report, err := f.proj.Cutover(ctx, time.Minute)
		require.NoError(t, err)
		require.True(t, report.Complete, "%+v", report)
		_, doc := f.state("skills", name("p", p1), alice)
		require.Equal(t, true, doc["hidden"])
	})
}
