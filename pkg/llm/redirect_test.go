package llm

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type llmRedirectFixture struct {
	mu     sync.Mutex
	seen   map[string]http.Header
	origin *httptest.Server
	other  *httptest.Server
}

func newLLMRedirectFixture(t *testing.T, body string) *llmRedirectFixture {
	t.Helper()
	f := &llmRedirectFixture{seen: map[string]http.Header{}}
	record := func(name string, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.seen[name+r.URL.Path] = r.Header.Clone()
	}
	f.other = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("other", r)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(f.other.Close)
	f.origin = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("origin", r)
		switch r.URL.Path {
		case "/cross/chat/completions", "/cross":
			http.Redirect(w, r, f.other.URL+"/landing", http.StatusTemporaryRedirect)
		case "/same/chat/completions", "/same":
			http.Redirect(w, r, "/landing", http.StatusTemporaryRedirect)
		default:
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(f.origin.Close)
	return f
}

func (f *llmRedirectFixture) header(key string) http.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen[key]
}

func TestRemoteClientKeepsInternalTokenOnConfiguredOrigin(t *testing.T) {
	f := newLLMRedirectFixture(t, `{"text":"ok"}`)

	if _, err := NewRemoteClient(f.origin.URL+"/cross", "internal-secret").Complete(context.Background(), "", "hi"); err != nil {
		t.Fatal(err)
	}
	if got := f.header("origin/cross").Get("X-OpenLinker-Internal-Token"); got != "internal-secret" {
		t.Fatalf("first hop token = %q", got)
	}
	if got := f.header("other/landing"); got == nil || got.Get("X-OpenLinker-Internal-Token") != "" {
		t.Fatalf("internal token reached another origin: %#v", got)
	}

	if _, err := NewRemoteClient(f.origin.URL+"/same", "internal-secret").Complete(context.Background(), "", "hi"); err != nil {
		t.Fatal(err)
	}
	if got := f.header("origin/landing").Get("X-OpenLinker-Internal-Token"); got != "internal-secret" {
		t.Fatalf("same-origin redirect token = %q", got)
	}
}

func TestOpenAIClientKeepsAPIKeyOnConfiguredOrigin(t *testing.T) {
	f := newLLMRedirectFixture(t, `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)

	if _, err := NewOpenAIClient(f.origin.URL+"/cross", "sk-test", "m").Complete(context.Background(), "", "hi"); err != nil {
		t.Fatal(err)
	}
	if got := f.header("origin/cross/chat/completions").Get("Authorization"); got != "Bearer sk-test" {
		t.Fatalf("first hop authorization = %q", got)
	}
	// Both servers are 127.0.0.1 on different ports: net/http alone keeps
	// Authorization here, the origin check must not.
	if got := f.header("other/landing"); got == nil || got.Get("Authorization") != "" {
		t.Fatalf("API key reached another origin: %#v", got)
	}

	if _, err := NewOpenAIClient(f.origin.URL+"/same", "sk-test", "m").Complete(context.Background(), "", "hi"); err != nil {
		t.Fatal(err)
	}
	if got := f.header("origin/landing").Get("Authorization"); got != "Bearer sk-test" {
		t.Fatalf("same-origin redirect authorization = %q", got)
	}
}

// "İ.example" lowercases to "i.example" under Unicode case mapping, but
// the transport resolves and sends it as "xn--i-9bb.example": another host.
// Every name is dialed to one test server so only the host differs.
func TestRemoteClientDropsInternalTokenOnIDNLookalikeRedirect(t *testing.T) {
	var mu sync.Mutex
	type landingHit struct {
		host  string
		token string
	}
	var landings []landingHit
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cross":
			w.Header().Set("Location", "http://İ.example:"+portOf(r.Host)+"/landing")
			w.WriteHeader(http.StatusTemporaryRedirect)
		case "/same":
			w.Header().Set("Location", "http://xn--bcher-kva.example:"+portOf(r.Host)+"/landing")
			w.WriteHeader(http.StatusTemporaryRedirect)
		default:
			mu.Lock()
			landings = append(landings, landingHit{host: r.Host, token: r.Header.Get("X-OpenLinker-Internal-Token")})
			mu.Unlock()
			_, _ = w.Write([]byte(`{"text":"ok"}`))
		}
	}))
	defer server.Close()
	port := portOf(server.Listener.Addr().String())
	dialAll := func(ctx context.Context, network, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, server.Listener.Addr().String())
	}

	client := NewRemoteClient("http://i.example:"+port+"/cross", "internal-secret")
	client.http.Transport = &http.Transport{DialContext: dialAll}
	if _, err := client.Complete(context.Background(), "", "hi"); err != nil {
		t.Fatal(err)
	}
	// The production CheckRedirect is kept; only dialing is redirected.
	client = NewRemoteClient("http://b\u00fccher.example:"+port+"/same", "internal-secret")
	client.http.Transport = &http.Transport{DialContext: dialAll}
	if _, err := client.Complete(context.Background(), "", "hi"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(landings) != 2 {
		t.Fatalf("landings = %+v", landings)
	}
	if landings[0].host == "i.example:"+port || landings[0].token != "" {
		t.Fatalf("internal token followed a redirect to another IDN host: %+v", landings[0])
	}
	if landings[1].host != "xn--bcher-kva.example:"+port || landings[1].token != "internal-secret" {
		t.Fatalf("the same IDN host in its punycode spelling lost the token: %+v", landings[1])
	}
}

func portOf(hostport string) string {
	_, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return ""
	}
	return port
}
