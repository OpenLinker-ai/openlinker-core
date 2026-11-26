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
