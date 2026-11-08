package registry_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/registry"
)

type registryRedirectRecorder struct {
	mu   sync.Mutex
	auth map[string][]string
}

func (r *registryRedirectRecorder) record(server string, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.auth == nil {
		r.auth = map[string][]string{}
	}
	key := server + req.URL.Path
	r.auth[key] = append(r.auth[key], req.Header.Get("Authorization"))
}

func (r *registryRedirectRecorder) only(t *testing.T, server, path string) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	values := r.auth[server+path]
	require.Len(t, values, 1, "requests to %s%s", server, path)
	return values[0]
}

func writeRemoteProxyRun(t *testing.T, w http.ResponseWriter, listingID uuid.UUID) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	now := time.Now().UTC().Format(time.RFC3339)
	require.NoError(t, json.NewEncoder(w).Encode(registry.ProxyRunResponse{
		ID: uuid.NewString(), RegistryRunID: uuid.NewString(), RegistryListingLinkID: uuid.NewString(),
		RegistryListingID: listingID.String(), RegistryNodeID: uuid.NewString(), LocalAgentID: uuid.NewString(),
		RequestingUserID: uuid.NewString(), IdempotencyKey: "redirect-route-1", Status: "pending",
		PayloadPolicy: "metadata_only", MaxAttempts: 3, CreatedAt: now, UpdatedAt: now,
	}))
}

// CreateRemoteProxyRun sends the peer's Bearer token through the production
// Registry client. A redirect may move the call, but never the token, to
// another origin; the strip is sticky; same-origin redirects keep it.
func TestCreateRemoteProxyRunRedirectKeepsBearerTokenOnOrigin(t *testing.T) {
	const token = "Bearer remote-token-123"
	listingID := uuid.New()
	rec := &registryRedirectRecorder{}
	var originURL string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record("other", r)
		if r.URL.Path == "/bounce" {
			http.Redirect(w, r, originURL+"/api/v1/proxy/landing", http.StatusTemporaryRedirect)
			return
		}
		writeRemoteProxyRun(t, w, listingID)
	}))
	defer other.Close()
	var route atomic.Value
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record("origin", r)
		if r.URL.Path != "/api/v1/proxy/runs" {
			writeRemoteProxyRun(t, w, listingID)
			return
		}
		switch route.Load() {
		case "cross":
			http.Redirect(w, r, other.URL+"/api/v1/proxy/runs", http.StatusTemporaryRedirect)
		case "sticky":
			http.Redirect(w, r, other.URL+"/bounce", http.StatusTemporaryRedirect)
		case "same":
			http.Redirect(w, r, "/api/v1/proxy/landing", http.StatusTemporaryRedirect)
		}
	}))
	defer origin.Close()
	originURL = origin.URL

	for _, tc := range []struct {
		route   string
		landing [2]string
		want    string
	}{
		{route: "cross", landing: [2]string{"other", "/api/v1/proxy/runs"}, want: ""},
		{route: "sticky", landing: [2]string{"origin", "/api/v1/proxy/landing"}, want: ""},
		{route: "same", landing: [2]string{"origin", "/api/v1/proxy/landing"}, want: token},
	} {
		t.Run(tc.route, func(t *testing.T) {
			rec.mu.Lock()
			rec.auth = nil
			rec.mu.Unlock()
			route.Store(tc.route)
			// The production client: endpoint policy, only loopback opted in.
			svc := newLocalRegistryService(nil)
			resp, err := svc.CreateRemoteProxyRun(context.Background(), uuid.Nil, &registry.CreateRemoteProxyRunRequest{
				RemoteAPIBaseURL:        origin.URL,
				RemoteBearerToken:       "remote-token-123",
				RemoteRegistryListingID: listingID.String(),
				IdempotencyKey:          "redirect-route-1",
				Input:                   map[string]any{"task": "redirect"},
			})
			require.NoError(t, err)
			require.Equal(t, listingID.String(), resp.RemoteRun.RegistryListingID)
			require.Equal(t, token, rec.only(t, "origin", "/api/v1/proxy/runs"))
			if tc.route == "sticky" {
				require.Empty(t, rec.only(t, "other", "/bounce"))
			}
			require.Equal(t, tc.want, rec.only(t, tc.landing[0], tc.landing[1]))
		})
	}
}
