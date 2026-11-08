package endpointurl

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

var credentialTestHeaders = map[string]string{
	"X-OpenLinker-Token":          "agent-secret",
	"X-OpenLinker-Internal-Token": "internal-secret",
	"Authorization":               "Bearer bearer-secret",
	"Proxy-Authorization":         "Basic proxy-secret",
	"Cookie":                      "session=cookie-secret",
}

type hit struct {
	path   string
	method string
	header http.Header
	body   string
}

type hitRecorder struct {
	mu   sync.Mutex
	hits []hit
}

func (r *hitRecorder) record(req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hits = append(r.hits, hit{path: req.URL.Path, method: req.Method, header: req.Header.Clone(), body: string(body)})
}

func (r *hitRecorder) find(t *testing.T, path string) hit {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range r.hits {
		if h.path == path {
			return h
		}
	}
	t.Fatalf("no request reached %s; got %+v", path, r.hits)
	return hit{}
}

// redirectServer answers /landing with 200 and redirects every other path to
// the URL its routes map gives it, using 307 so the POST body is replayed.
func redirectServer(t *testing.T, rec *hitRecorder, routes func() map[string]string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		if target, ok := routes()[r.URL.Path]; ok {
			http.Redirect(w, r, target, http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(server.Close)
	return server
}

func postWithCredentials(t *testing.T, client *http.Client, target string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, target, strings.NewReader(`{"input":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range credentialTestHeaders {
		req.Header.Set(name, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-OpenLinker-Run-Id", "run-1")
	resp, err := client.Do(req)
	if resp != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	return resp, err
}

func assertNoCredentials(t *testing.T, h hit) {
	t.Helper()
	for name := range credentialTestHeaders {
		if got := h.header.Get(name); got != "" {
			t.Fatalf("%s leaked to %s: %q", name, h.path, got)
		}
	}
	if got := h.header.Get("Referer"); got != "" {
		t.Fatalf("Referer leaked to %s: %q", h.path, got)
	}
}

func assertCredentials(t *testing.T, h hit) {
	t.Helper()
	for name, value := range credentialTestHeaders {
		if got := h.header.Get(name); got != value {
			t.Fatalf("%s at %s = %q, want %q", name, h.path, got, value)
		}
	}
}

func TestNewHTTPClientDropsCredentialsOnCrossOriginRedirect(t *testing.T) {
	var targetURL string
	targetRec := &hitRecorder{}
	target := redirectServer(t, targetRec, func() map[string]string { return nil })
	targetURL = target.URL
	originRec := &hitRecorder{}
	origin := redirectServer(t, originRec, func() map[string]string {
		return map[string]string{"/start": targetURL + "/landing?from=origin"}
	})

	resp, err := postWithCredentials(t, NewHTTPClient(5*time.Second, true), origin.URL+"/start?code=query-secret")
	if err != nil {
		t.Fatalf("redirect should still be followed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	assertCredentials(t, originRec.find(t, "/start"))
	landing := targetRec.find(t, "/landing")
	assertNoCredentials(t, landing)
	if landing.method != http.MethodPost || landing.body != `{"input":"x"}` {
		t.Fatalf("307 redirect should replay the POST body: %+v", landing)
	}
	if landing.header.Get("X-OpenLinker-Run-Id") != "run-1" || landing.header.Get("Content-Type") != "application/json" {
		t.Fatalf("non-credential headers should still follow the redirect: %#v", landing.header)
	}
}

func TestNewHTTPClientKeepsCredentialsOnSameOriginRedirect(t *testing.T) {
	rec := &hitRecorder{}
	server := redirectServer(t, rec, func() map[string]string {
		return map[string]string{"/start": "/landing", "/moved": "/landing"}
	})

	if _, err := postWithCredentials(t, NewHTTPClient(5*time.Second, true), server.URL+"/start"); err != nil {
		t.Fatal(err)
	}
	landing := rec.find(t, "/landing")
	assertCredentials(t, landing)
	if landing.body != `{"input":"x"}` {
		t.Fatalf("same-origin 307 should replay the body: %+v", landing)
	}

	// An absolute Location with the explicit default host spelling is the same
	// origin as well.
	rec2 := &hitRecorder{}
	var selfURL string
	self := redirectServer(t, rec2, func() map[string]string {
		return map[string]string{"/start": selfURL + "/landing"}
	})
	selfURL = self.URL
	if _, err := postWithCredentials(t, NewHTTPClient(5*time.Second, true), self.URL+"/start"); err != nil {
		t.Fatal(err)
	}
	assertCredentials(t, rec2.find(t, "/landing"))

	// A leading-zero spelling of the same port is the same effective origin.
	rec3 := &hitRecorder{}
	var zeroPortURL string
	zero := redirectServer(t, rec3, func() map[string]string {
		return map[string]string{"/start": zeroPortURL + "/landing"}
	})
	parsed, err := url.Parse(zero.URL)
	if err != nil {
		t.Fatal(err)
	}
	zeroPortURL = "http://" + parsed.Hostname() + ":0" + parsed.Port()
	if _, err := postWithCredentials(t, NewHTTPClient(5*time.Second, true), zero.URL+"/start"); err != nil {
		t.Fatal(err)
	}
	assertCredentials(t, rec3.find(t, "/landing"))
}

func TestNewHTTPClientKeepsCredentialsStrippedAfterReturningToOrigin(t *testing.T) {
	var originURL, otherURL string
	originRec := &hitRecorder{}
	origin := redirectServer(t, originRec, func() map[string]string {
		return map[string]string{"/start": otherURL + "/bounce"}
	})
	originURL = origin.URL
	otherRec := &hitRecorder{}
	other := redirectServer(t, otherRec, func() map[string]string {
		return map[string]string{"/bounce": originURL + "/landing"}
	})
	otherURL = other.URL

	if _, err := postWithCredentials(t, NewHTTPClient(5*time.Second, true), origin.URL+"/start"); err != nil {
		t.Fatal(err)
	}
	assertNoCredentials(t, otherRec.find(t, "/bounce"))
	assertNoCredentials(t, originRec.find(t, "/landing"))
}

func TestNewHTTPClientTreatsHostSpellingAndSchemeAsOtherOrigins(t *testing.T) {
	// localhost and 127.0.0.1 on the same port are different origins.
	var aliasURL string
	rec := &hitRecorder{}
	server := redirectServer(t, rec, func() map[string]string {
		return map[string]string{"/start": aliasURL + "/landing"}
	})
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	aliasURL = "http://localhost:" + parsed.Port()
	if _, err := postWithCredentials(t, NewHTTPClient(5*time.Second, true), server.URL+"/start"); err != nil {
		t.Fatal(err)
	}
	assertCredentials(t, rec.find(t, "/start"))
	assertNoCredentials(t, rec.find(t, "/landing"))

	// https -> http on loopback (only reachable with local HTTP opted in) is a
	// scheme change and therefore another origin.
	plainRec := &hitRecorder{}
	plain := redirectServer(t, plainRec, func() map[string]string { return nil })
	tlsRec := &hitRecorder{}
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tlsRec.record(r)
		http.Redirect(w, r, plain.URL+"/landing", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(tlsServer.Close)
	client := NewHTTPClient(5*time.Second, true)
	client.Transport.(*http.Transport).TLSClientConfig = tlsServer.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	if _, err := postWithCredentials(t, client, tlsServer.URL+"/start"); err != nil {
		t.Fatal(err)
	}
	assertCredentials(t, tlsRec.find(t, "/start"))
	assertNoCredentials(t, plainRec.find(t, "/landing"))
}

func TestNewHTTPClientRedirectStillEnforcesEndpointPolicy(t *testing.T) {
	for _, location := range []string{
		"http://10.0.0.5/internal",
		"https://169.254.169.254/latest/meta-data",
		"http://agent.example.com/plain",
		"https://user:pass@agent.example.com/userinfo",
	} {
		t.Run(location, func(t *testing.T) {
			rec := &hitRecorder{}
			server := redirectServer(t, rec, func() map[string]string {
				return map[string]string{"/start": location}
			})
			_, err := postWithCredentials(t, NewHTTPClient(5*time.Second, true), server.URL+"/start")
			if err == nil {
				t.Fatalf("redirect to %s should be rejected", location)
			}
		})
	}
}

func TestRedirectPolicyRejectsHTTPSDowngrade(t *testing.T) {
	policy := RedirectPolicy(false)
	first := mustRequest(t, "https://public.example.com/invoke")
	next := mustRequest(t, "http://public.example.com/invoke")
	if err := policy(next, []*http.Request{first}); err == nil {
		t.Fatal("https -> http downgrade must be rejected")
	}
	// Even with local HTTP opted in, a downgrade to a remote host is rejected.
	if err := RedirectPolicy(true)(next, []*http.Request{first}); err == nil {
		t.Fatal("https -> remote http downgrade must be rejected with local HTTP enabled")
	}
}

func TestRedirectPolicyStopsAfterTenRedirects(t *testing.T) {
	via := make([]*http.Request, 10)
	for i := range via {
		via[i] = mustRequest(t, "https://public.example.com/hop")
	}
	if err := RedirectPolicy(true)(mustRequest(t, "https://public.example.com/next"), via); err == nil {
		t.Fatal("expected redirect limit")
	}
	if err := CredentialRedirectPolicy()(mustRequest(t, "https://public.example.com/next"), via); err == nil {
		t.Fatal("expected redirect limit")
	}
}

func TestCredentialRedirectPolicyStripsWithoutEndpointValidation(t *testing.T) {
	policy := CredentialRedirectPolicy()
	first := mustRequest(t, "http://llm.internal:8080/complete")

	same := withCredentialHeaders(mustRequest(t, "http://LLM.internal:8080/v2/complete"))
	if err := policy(same, []*http.Request{first}); err != nil {
		t.Fatalf("operator endpoints on private networks are allowed: %v", err)
	}
	for name, value := range credentialTestHeaders {
		if same.Header.Get(name) != value {
			t.Fatalf("same-origin redirect dropped %s", name)
		}
	}

	other := withCredentialHeaders(mustRequest(t, "http://llm.internal:9090/complete"))
	other.Header.Set("Referer", "http://llm.internal:8080/complete?key=secret")
	if err := policy(other, []*http.Request{first}); err != nil {
		t.Fatal(err)
	}
	assertNoCredentials(t, hit{path: other.URL.Path, header: other.Header})
}

// Each case goes through StripCrossOriginCredentials, the decision the
// production CheckRedirect makes: same=true means the credentials are kept.
func TestCrossOriginDecisionMatchesTransportOrigin(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		same bool
	}{
		{"implicit https port", "https://agent.example.com/a", "https://agent.example.com:443/b", true},
		{"implicit http port", "http://agent.example.com/a", "http://agent.example.com:80/b", true},
		{"leading-zero port", "https://agent.example.com:443/a", "https://agent.example.com:0443/b", true},
		{"leading-zero default port", "https://agent.example.com/a", "https://agent.example.com:00443/b", true},
		{"leading-zero custom port", "http://127.0.0.1:8080/a", "http://127.0.0.1:08080/b", true},
		{"ascii case", "https://Agent.Example.com/a", "HTTPS://agent.example.com/b?x=1", true},
		{"ipv6 literal", "https://[::1]:8443/a", "https://[::1]:8443/b", true},
		{"unicode and punycode spelling", "https://b\u00fccher.example/a", "https://xn--bcher-kva.example/b", true},
		{"punycode ascii case", "https://XN--BCHER-KVA.example/a", "https://b\u00fccher.example/b", true},
		// The Host header keeps Unicode case, so a different virtual host may answer.
		{"unicode case", "https://B\u00dcCHER.example/a", "https://b\u00fccher.example/b", false},
		{"lookup and host header disagree", "https://xn--i-9bb.example/a", "https://\u0130.example/b", false},
		{"compatibility mapping", "https://k.example/a", "https://\u212a.example/b", false},
		{"dotted capital I is not i", "https://i.example/a", "https://\u0130.example/b", false},
		{"dotted capital I is not I", "https://I.example/a", "https://\u0130.example/b", false},
		{"scheme", "https://agent.example.com/a", "http://agent.example.com/a", false},
		{"port", "https://agent.example.com/a", "https://agent.example.com:8443/a", false},
		{"subdomain", "https://agent.example.com/a", "https://sub.agent.example.com/a", false},
		{"trailing dot", "https://agent.example.com/a", "https://agent.example.com./a", false},
		{"scheme with explicit port", "https://agent.example.com:443/a", "http://agent.example.com:443/a", false},
		{"port out of range", "https://agent.example.com:443/a", "https://agent.example.com:65979/a", false},
		{"port overflow", "https://agent.example.com/a", "https://agent.example.com:4294967739/a", false},
		{"invalid IDN even when identical", "https://\u2488.example/a", "https://\u2488.example/b", false},
		{"invalid IDN target", "https://agent.example.com/a", "https://\u0080.example/b", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := mustRequest(t, tc.a)
			next := withCredentialHeaders(mustRequest(t, tc.b))
			StripCrossOriginCredentials(next, []*http.Request{first})
			if tc.same {
				assertCredentials(t, hit{path: tc.b, header: next.Header})
			} else {
				assertNoCredentials(t, hit{path: tc.b, header: next.Header})
			}
		})
	}
}

func TestStripCrossOriginCredentialsFailsClosed(t *testing.T) {
	req := withCredentialHeaders(mustRequest(t, "https://agent.example.com/next"))
	StripCrossOriginCredentials(req, []*http.Request{nil})
	assertNoCredentials(t, hit{path: "/next", header: req.Header})

	initial := withCredentialHeaders(mustRequest(t, "https://agent.example.com/first"))
	StripCrossOriginCredentials(initial, nil)
	assertCredentials(t, hit{path: "/first", header: initial.Header})
}

func mustRequest(t *testing.T, raw string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func withCredentialHeaders(req *http.Request) *http.Request {
	for name, value := range credentialTestHeaders {
		req.Header.Set(name, value)
	}
	return req
}
