package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

type endpointRedirectHit struct {
	path   string
	header http.Header
}

type endpointRedirectRecorder struct {
	mu   sync.Mutex
	hits []endpointRedirectHit
}

func (r *endpointRedirectRecorder) handler(redirects map[string]func() string, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.hits = append(r.hits, endpointRedirectHit{path: req.URL.Path, header: req.Header.Clone()})
		r.mu.Unlock()
		if target, ok := redirects[req.URL.Path]; ok {
			http.Redirect(w, req, target(), http.StatusTemporaryRedirect)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
}

func (r *endpointRedirectRecorder) header(t *testing.T, path string) http.Header {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, hit := range r.hits {
		if hit.path == path {
			return hit.header
		}
	}
	t.Fatalf("no request reached %s", path)
	return nil
}

// newLocalEndpointRuntimeService keeps the production Agent endpoint client
// (endpointurl policy plus the runtime pool tuning) and only opts into the
// loopback HTTP development switch so httptest servers are reachable.
func newLocalEndpointRuntimeService() *Service {
	return NewService(nil, &config.Config{
		APIURL:                  "https://api.example.test",
		RuntimePKIMasterSecret:  thirdPartyTransportTestMasterSecret,
		AllowLocalHTTPEndpoints: true,
	})
}

func TestAgentEndpointRedirectDropsCredentialsAcrossOrigins(t *testing.T) {
	const directBody = `{"output":{"answer":"ok"}}`
	const mcpBody = `{"jsonrpc":"2.0","id":"1","result":{"structuredContent":{"answer":"ok"}}}`
	for _, tc := range []struct {
		name       string
		mode       string
		auth       string
		authHeader string
		body       string
	}{
		{name: "direct_http token", mode: connectionModeDirectHTTP, auth: "direct-secret", authHeader: "X-OpenLinker-Token", body: directBody},
		{name: "mcp_server token", mode: connectionModeMCPServer, auth: "mcp-secret", authHeader: "X-OpenLinker-Token", body: mcpBody},
		{name: "mcp_server bearer", mode: connectionModeMCPServer, auth: "Bearer mcp-bearer", authHeader: "Authorization", body: mcpBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			otherRec := &endpointRedirectRecorder{}
			other := httptest.NewServer(otherRec.handler(nil, tc.body))
			defer other.Close()
			originRec := &endpointRedirectRecorder{}
			origin := httptest.NewServer(originRec.handler(map[string]func() string{
				"/cross": func() string { return other.URL + "/landing" },
				"/same":  func() string { return "/landing" },
			}, tc.body))
			defer origin.Close()

			svc := newLocalEndpointRuntimeService()
			auth := tc.auth
			toolName := "answer"
			agent := &db.Agent{
				ID:                 uuid.New(),
				EndpointAuthHeader: &auth,
				ConnectionMode:     tc.mode,
				MCPToolName:        &toolName,
			}

			agent.EndpointURL = origin.URL + "/cross"
			output, _, agentErr, callErr := svc.callAgent(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{Input: map[string]interface{}{"q": "x"}}, nil)
			require.NoError(t, callErr)
			require.Nil(t, agentErr)
			require.Equal(t, "ok", output["answer"])
			require.Equal(t, tc.auth, originRec.header(t, "/cross").Get(tc.authHeader))
			landing := otherRec.header(t, "/landing")
			require.Empty(t, landing.Get("X-OpenLinker-Token"), "Agent token must not reach another origin")
			require.Empty(t, landing.Get("Authorization"), "Agent bearer must not reach another origin")
			require.Empty(t, landing.Get("Referer"), "endpoint URL must not reach another origin")
			require.NotEmpty(t, landing.Get("X-OpenLinker-Run-Id"))

			agent.EndpointURL = origin.URL + "/same"
			_, _, agentErr, callErr = svc.callAgent(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{Input: map[string]interface{}{"q": "x"}}, nil)
			require.NoError(t, callErr)
			require.Nil(t, agentErr)
			require.Equal(t, tc.auth, originRec.header(t, "/landing").Get(tc.authHeader), "same-origin redirects keep the credential")
		})
	}
}

func TestAgentEndpointRedirectStillRejectsPrivateTargets(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	svc := newLocalEndpointRuntimeService()
	token := "direct-secret"
	agent := &db.Agent{
		ID:                 uuid.New(),
		EndpointURL:        origin.URL,
		EndpointAuthHeader: &token,
		ConnectionMode:     connectionModeDirectHTTP,
	}
	_, _, _, callErr := svc.callAgent(context.Background(), agent, uuid.New(), uuid.New(), &RunRequest{Input: map[string]interface{}{}}, nil)
	require.Error(t, callErr)

	output, dryErr := svc.DryRun(context.Background(), agent, map[string]interface{}{"ping": true})
	require.Nil(t, output)
	require.Contains(t, dryErr, "endpoint 调用失败")
}
