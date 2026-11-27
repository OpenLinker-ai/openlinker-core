package catalogregistry_test

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type fixedResolver map[string][]net.IPAddr

func (f fixedResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if addrs, ok := f[host]; ok {
		return addrs, nil
	}
	return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
}

func registryJSON(name, url string, extra string) string {
	return `[{"name":"` + name + `","url":"` + url + `","consumer":"core-a","token":"olr_abcdefghijklmnop_` + strings.Repeat("s", 40) + `","audience":"public"` + extra + `}]`
}

func newClient(t *testing.T, raw string, opts catalogregistry.Options) *catalogregistry.Client {
	t.Helper()
	regs, err := catalogregistry.Parse(raw)
	require.NoError(t, err)
	return catalogregistry.NewClient(regs, opts)
}

func TestClientOutboundPolicy(t *testing.T) {
	var hits atomic.Int32
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		gotAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"skills":[]}`))
	}))
	defer srv.Close()
	ctx := context.Background()

	private := newClient(t, registryJSON("r", srv.URL+"/registry", `,"allow_http":true`), catalogregistry.Options{})
	_, err := private.Get(ctx, "r", "/v1/skills", nil, nil)
	require.ErrorIs(t, err, catalogregistry.ErrUnavailable)
	require.Zero(t, hits.Load())

	allowed := newClient(t, registryJSON("r", srv.URL+"/registry", `,"allow_http":true,"allow_private_network":true`), catalogregistry.Options{})
	res, err := allowed.Get(ctx, "r", "/v1/skills", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 200, res.Status)
	require.True(t, strings.HasPrefix(gotAuth.Load().(string), "Bearer olr_"))

	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	rebinding := newClient(t, registryJSON("r", "http://registry.test:"+port+"/registry", `,"allow_http":true`),
		catalogregistry.Options{Resolver: fixedResolver{"registry.test": {{IP: net.ParseIP("127.0.0.1")}}}})
	before := hits.Load()
	_, err = rebinding.Get(ctx, "r", "/v1/skills", nil, nil)
	require.ErrorIs(t, err, catalogregistry.ErrUnavailable)
	require.Equal(t, before, hits.Load())

	metadata := newClient(t, registryJSON("r", "http://metadata.test:"+port+"/registry", `,"allow_http":true,"allow_private_network":true`),
		catalogregistry.Options{Resolver: fixedResolver{"metadata.test": {{IP: net.ParseIP("169.254.169.254")}}}})
	_, err = metadata.Get(ctx, "r", "/v1/skills", nil, nil)
	require.ErrorIs(t, err, catalogregistry.ErrUnavailable)

	_, err = allowed.Get(ctx, "missing", "/v1/skills", nil, nil)
	require.ErrorIs(t, err, catalogregistry.ErrUnknownRegistry)
	_, err = allowed.Get(ctx, "r", "//evil.example/v1/skills", nil, nil)
	require.Error(t, err)
	_, err = allowed.Get(ctx, "r", "/v1/skills?q=a&q=b", nil, nil)
	require.Error(t, err)
}

func TestClientDoesNotFollowRedirectsOrLeakTheToken(t *testing.T) {
	var leaked atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Add(1)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer other.Close()
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/v1/skills", http.StatusFound)
	}))
	defer redirecting.Close()
	c := newClient(t, registryJSON("r", redirecting.URL, `,"allow_http":true,"allow_private_network":true`), catalogregistry.Options{})
	_, err := c.Get(context.Background(), "r", "/v1/skills", nil, nil)
	require.ErrorIs(t, err, catalogregistry.ErrUnavailable)
	require.Zero(t, leaked.Load())
}

func TestClientBoundsResponses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`"` + strings.Repeat("a", 2<<20) + `"`))
	}))
	defer srv.Close()
	c := newClient(t, registryJSON("r", srv.URL, `,"allow_http":true,"allow_private_network":true`), catalogregistry.Options{})
	_, err := c.Get(context.Background(), "r", "/v1/skills", nil, nil)
	require.ErrorIs(t, err, catalogregistry.ErrUnavailable)
}

func TestAssertionIsBoundToTheRequest(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	var assertion atomic.Value
	var target atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertion.Store(r.Header.Get("OpenLinker-User-Assertion"))
		target.Store(r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	extra := `,"allow_http":true,"allow_private_network":true,"assertion_audience":"https://openlinker.ai/registry","signing_key_id":"k1","signing_key":"` + base64.StdEncoding.EncodeToString(priv.Seed()) + `"`
	c := newClient(t, registryJSON("r", srv.URL+"/registry", extra), catalogregistry.Options{})
	user := &catalogregistry.User{ID: uuid.MustParse("8f14e45f-ceea-467a-a866-051b2ad0d4a1"), GitHubID: "4242"}
	_, err := c.Get(context.Background(), "r", "/v1/skills?q=hello world&limit=5", user, []string{"read", "import"})
	require.NoError(t, err)
	require.Equal(t, "/registry/v1/skills?limit=5&q=hello+world", target.Load())
	parts := strings.Split(assertion.Load().(string), ".")
	require.Len(t, parts, 3)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	require.True(t, ed25519.Verify(pub, []byte(parts[0]+"."+parts[1]), sig))
	var header map[string]string
	raw, _ := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, json.Unmarshal(raw, &header))
	require.Equal(t, map[string]string{"alg": "EdDSA", "typ": "openlinker-user-assertion+jwt", "kid": "k1"}, header)
	var claims struct {
		Aud string            `json:"aud"`
		Cid string            `json:"cid"`
		Sub string            `json:"sub"`
		GH  string            `json:"gh"`
		Act []string          `json:"act"`
		Req map[string]string `json:"req"`
		Iat int64             `json:"iat"`
		Exp int64             `json:"exp"`
		Jti string            `json:"jti"`
	}
	raw, _ = base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, json.Unmarshal(raw, &claims))
	empty := sha256.Sum256(nil)
	require.Equal(t, "https://openlinker.ai/registry", claims.Aud)
	require.Equal(t, "core-a", claims.Cid)
	require.Equal(t, user.ID.String(), claims.Sub)
	require.Equal(t, "4242", claims.GH)
	require.Equal(t, []string{"read", "import"}, claims.Act)
	require.Equal(t, map[string]string{"m": "GET", "t": "/registry/v1/skills?limit=5&q=hello+world", "bh": hex.EncodeToString(empty[:])}, claims.Req)
	require.LessOrEqual(t, claims.Exp-claims.Iat, int64(60))
	require.InDelta(t, time.Now().Unix(), claims.Iat, 5)
	require.GreaterOrEqual(t, len(claims.Jti), 16)

	unsigned := newClient(t, registryJSON("r", srv.URL+"/registry", `,"allow_http":true,"allow_private_network":true`), catalogregistry.Options{})
	_, err = unsigned.Get(context.Background(), "r", "/v1/skills", user, []string{"read"})
	require.NoError(t, err)
	require.Equal(t, "", assertion.Load())
}

func TestClientEscapesPathSegmentsOnce(t *testing.T) {
	var target atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target.Store(r.URL.EscapedPath())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := newClient(t, registryJSON("r", srv.URL+"/registry", `,"allow_http":true,"allow_private_network":true`), catalogregistry.Options{})
	_, err := c.Get(context.Background(), "r", "/v1/skills/a/b/versions/1/files/my%20file.md", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "/registry/v1/skills/a/b/versions/1/files/my%20file.md", target.Load())
	_, err = c.Get(context.Background(), "r", "/v1/mcp/servers/io.github.a/w/versions/1.0.0%2Bbuild.1", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "/registry/v1/mcp/servers/io.github.a/w/versions/1.0.0%2Bbuild.1", target.Load())
	_, err = c.Get(context.Background(), "r", "/v1/skills/a/b/versions/1/files/%zz", nil, nil)
	require.ErrorIs(t, err, catalogregistry.ErrInvalidTarget)
	_, err = c.Get(context.Background(), "r", "/v1/mcp/servers/io.github.a/w/versions/release%2F2026", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "/registry/v1/mcp/servers/io.github.a/w/versions/release%2F2026", target.Load())
	for _, target := range []string{"/v1/skills/a/%2e%2e/x", "/v1/skills/a/../x", "/v1/./skills", "/v1/skills/a/%2E%2E", "/v1/skills/a%5Cb/x"} {
		_, err = c.Get(context.Background(), "r", target, nil, nil)
		require.ErrorIs(t, err, catalogregistry.ErrInvalidTarget, target)
	}
}

func assertionClaims(t *testing.T, raw string) map[string]any {
	t.Helper()
	parts := strings.Split(raw, ".")
	require.Len(t, parts, 3)
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var claims map[string]any
	require.NoError(t, json.Unmarshal(payload, &claims))
	return claims
}

func signingExtra(priv ed25519.PrivateKey) string {
	return `,"allow_http":true,"allow_private_network":true,"writes":true,"assertion_audience":"https://openlinker.ai/registry","signing_key_id":"k1","signing_key":"` + base64.StdEncoding.EncodeToString(priv.Seed()) + `"`
}

func TestClientFollowsRedirectsWithinTheRegistry(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	type seen struct{ target, assertion string }
	var mu = make(chan seen, 16)
	var other atomic.Int32
	outside := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { other.Add(1) }))
	defer outside.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu <- seen{r.URL.RequestURI(), r.Header.Get("OpenLinker-User-Assertion")}
		switch r.URL.Path {
		case "/registry/v1/skills/old/demo/versions/1/artifact":
			w.Header().Set("Location", "/registry/v1/skills/io.github.acme/demo/versions/1/artifact?purpose=import")
			w.WriteHeader(http.StatusPermanentRedirect)
		case "/registry/v1/skills/io.github.acme/demo/versions/1/artifact":
			_, _ = w.Write([]byte("bytes"))
		case "/registry/v1/skills/loop/a":
			w.Header().Set("Location", "/registry/v1/skills/loop/a")
			w.WriteHeader(http.StatusPermanentRedirect)
		case "/registry/v1/skills/out/a":
			w.Header().Set("Location", outside.URL+"/registry/v1/skills/x/y")
			w.WriteHeader(http.StatusPermanentRedirect)
		case "/registry/v1/skills/up/a":
			w.Header().Set("Location", "/elsewhere/v1/skills/x/y")
			w.WriteHeader(http.StatusPermanentRedirect)
		case "/registry/v1/skills/dots/a":
			w.Header().Set("Location", "/registry/v1/%2e%2e/%2e%2e/admin")
			w.WriteHeader(http.StatusPermanentRedirect)
		case "/registry/v1/skills/purpose/a/versions/1/artifact":
			w.Header().Set("Location", "/registry/v1/skills/io.github.acme/demo/versions/1/artifact")
			w.WriteHeader(http.StatusPermanentRedirect)
		case "/registry/v1/skills/slash/a":
			w.Header().Set("Location", "/registry/v1/skills/x%2F..%2F..%2Fadmin/y")
			w.WriteHeader(http.StatusPermanentRedirect)
		case "/registry/v1/skills/found/a":
			w.Header().Set("Location", "/registry/v1/skills/x/y")
			w.WriteHeader(http.StatusFound)
		}
	}))
	defer srv.Close()
	c := newClient(t, registryJSON("r", srv.URL+"/registry", signingExtra(priv)), catalogregistry.Options{})
	user := &catalogregistry.User{ID: uuid.MustParse("8f14e45f-ceea-467a-a866-051b2ad0d4a1"), GitHubID: "4242"}
	res, err := c.GetArtifact(context.Background(), "r", "/v1/skills/old/demo/versions/1/artifact?purpose=import", user, []string{"read", "import"})
	require.NoError(t, err)
	require.Equal(t, "bytes", string(res.Body))
	first, second := <-mu, <-mu
	require.Equal(t, "/registry/v1/skills/io.github.acme/demo/versions/1/artifact?purpose=import", second.target)
	a, b := assertionClaims(t, first.assertion), assertionClaims(t, second.assertion)
	require.NotEqual(t, a["jti"], b["jti"])
	require.Equal(t, "/registry/v1/skills/io.github.acme/demo/versions/1/artifact?purpose=import", b["req"].(map[string]any)["t"])
	require.Equal(t, []any{"read", "import"}, b["act"])
	res, err = c.GetArtifact(context.Background(), "r", "/v1/skills/purpose/a/versions/1/artifact?purpose=import", user, []string{"read", "import"})
	require.NoError(t, err)
	require.Equal(t, "bytes", string(res.Body))
	<-mu
	kept := <-mu
	require.Equal(t, "/registry/v1/skills/io.github.acme/demo/versions/1/artifact?purpose=import", kept.target)
	for _, target := range []string{"/v1/skills/loop/a", "/v1/skills/out/a", "/v1/skills/up/a", "/v1/skills/found/a", "/v1/skills/dots/a", "/v1/skills/slash/a"} {
		_, err := c.Get(context.Background(), "r", target, user, []string{"read"})
		require.ErrorIs(t, err, catalogregistry.ErrUnavailable, target)
	}
	require.Zero(t, other.Load())
}

func TestClientSendSignsWritesAndNeverFollowsRedirects(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		got.Store([]string{r.Method, r.URL.RequestURI(), r.Header.Get("Content-Type"), string(body), r.Header.Get("OpenLinker-User-Assertion")})
		if strings.HasSuffix(r.URL.Path, "/moved") {
			w.Header().Set("Location", "/registry/v1/namespaces")
			w.WriteHeader(http.StatusPermanentRedirect)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"ai.openlinker.alice"}`))
	}))
	defer srv.Close()
	c := newClient(t, registryJSON("r", srv.URL+"/registry", signingExtra(priv)), catalogregistry.Options{})
	user := &catalogregistry.User{ID: uuid.MustParse("8f14e45f-ceea-467a-a866-051b2ad0d4a1")}
	body := []byte(`{"name":"ai.openlinker.alice"}`)
	res, err := c.Send(context.Background(), "r", http.MethodPost, "/v1/namespaces", "application/json", body, user)
	require.NoError(t, err)
	require.Equal(t, 201, res.Status)
	seen := got.Load().([]string)
	require.Equal(t, []string{"POST", "/registry/v1/namespaces", "application/json", string(body)}, seen[:4])
	claims := assertionClaims(t, seen[4])
	h := sha256.Sum256(body)
	require.Equal(t, map[string]any{"m": "POST", "t": "/registry/v1/namespaces", "bh": hex.EncodeToString(h[:])}, claims["req"])
	require.Equal(t, []any{"read", "write"}, claims["act"])
	_, err = c.Send(context.Background(), "r", http.MethodPost, "/v1/skills/a/moved", "application/json", body, user)
	require.ErrorIs(t, err, catalogregistry.ErrUnavailable)
	_, err = c.Send(context.Background(), "r", http.MethodPost, "/v1/mcp/servers/a/b/versions/release%2F2026/status", "application/json", body, user)
	require.ErrorIs(t, err, catalogregistry.ErrInvalidTarget)
	_, err = c.Send(context.Background(), "r", http.MethodPost, "/v1/namespaces", "application/json", body, nil)
	require.Error(t, err)
	readOnly := newClient(t, registryJSON("r", srv.URL+"/registry", `,"allow_http":true,"allow_private_network":true`), catalogregistry.Options{})
	_, err = readOnly.Send(context.Background(), "r", http.MethodPost, "/v1/namespaces", "application/json", body, user)
	require.Error(t, err)
}

func TestClientProjectSignsTheProjectAction(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store([]string{r.Method, r.URL.RequestURI(), r.Header.Get("OpenLinker-User-Assertion")})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"applied":true}`))
	}))
	defer srv.Close()
	extra := `,"allow_http":true,"allow_private_network":true,"assertion_audience":"https://openlinker.ai/registry","signing_key_id":"k1","signing_key":"` + base64.StdEncoding.EncodeToString(priv.Seed()) + `"`
	c := newClient(t, registryJSON("r", srv.URL+"/registry", extra), catalogregistry.Options{})
	owner := &catalogregistry.User{ID: uuid.MustParse("8f14e45f-ceea-467a-a866-051b2ad0d4a1")}
	res, err := c.Project(context.Background(), "r", http.MethodPut, "/v1/projections/skills/p-x", "application/json", []byte(`{}`), owner)
	require.NoError(t, err)
	require.Equal(t, 200, res.Status)
	seen := got.Load().([]string)
	require.Equal(t, []string{"PUT", "/registry/v1/projections/skills/p-x"}, seen[:2])
	require.Equal(t, []any{"project"}, assertionClaims(t, seen[2])["act"])
	_, err = c.Project(context.Background(), "r", http.MethodGet, "/v1/projections/skills/p%2Fx", "", nil, owner)
	require.ErrorIs(t, err, catalogregistry.ErrInvalidTarget)
	unsigned := newClient(t, registryJSON("r", srv.URL+"/registry", `,"allow_http":true,"allow_private_network":true`), catalogregistry.Options{})
	_, err = unsigned.Project(context.Background(), "r", http.MethodPut, "/v1/projections/skills/p-x", "application/json", []byte(`{}`), owner)
	require.Error(t, err)
}
