package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

type deliveryRedirectHit struct {
	referer   string
	signature string
	delivery  string
}

type deliveryRedirectRecorder struct {
	mu   sync.Mutex
	hits map[string][]deliveryRedirectHit
}

func (r *deliveryRedirectRecorder) record(server string, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hits == nil {
		r.hits = map[string][]deliveryRedirectHit{}
	}
	key := server + req.URL.Path
	r.hits[key] = append(r.hits[key], deliveryRedirectHit{
		referer:   req.Header.Get("Referer"),
		signature: req.Header.Get("X-OpenLinker-Signature"),
		delivery:  req.Header.Get("X-OpenLinker-Delivery"),
	})
}

func (r *deliveryRedirectRecorder) only(t *testing.T, server, path string) deliveryRedirectHit {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	hits := r.hits[server+path]
	require.Len(t, hits, 1, "requests to %s%s", server, path)
	return hits[0]
}

// A Slack incoming-webhook URL is itself the secret, and a webhook target URL
// may carry a token in its query. net/http puts the previous URL in Referer
// on every redirect hop, so AttemptDelivery through the production client
// must not let it reach another origin, must keep it stripped for the rest
// of the chain, and keeps same-origin redirects unchanged.
func TestAttemptDeliveryRedirectKeepsTargetURLOnOrigin(t *testing.T) {
	rec := &deliveryRedirectRecorder{}
	var originURL string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record("other", r)
		if r.URL.Path == "/bounce" {
			http.Redirect(w, r, originURL+"/landing", http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()
	var route atomic.Value
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record("origin", r)
		if r.URL.Path == "/landing" {
			w.WriteHeader(http.StatusOK)
			return
		}
		switch route.Load() {
		case "cross":
			http.Redirect(w, r, other.URL+"/landing", http.StatusTemporaryRedirect)
		case "sticky":
			http.Redirect(w, r, other.URL+"/bounce", http.StatusTemporaryRedirect)
		case "same":
			http.Redirect(w, r, "/landing", http.StatusTemporaryRedirect)
		}
	}))
	defer origin.Close()
	originURL = origin.URL

	payload, err := json.Marshal(DeliveryPayload{Event: eventRunCompleted, AgentName: "Helper", Status: "success"})
	require.NoError(t, err)
	secret := "target-secret"
	targetPath := "/services/T000/B000/slack-path-secret"
	for _, targetType := range []string{targetTypeSlack, targetTypeWebhook} {
		for _, tc := range []struct {
			route   string
			landing [2]string
			keeps   bool
		}{
			{route: "cross", landing: [2]string{"other", "/landing"}},
			{route: "sticky", landing: [2]string{"origin", "/landing"}},
			{route: "same", landing: [2]string{"origin", "/landing"}, keeps: true},
		} {
			t.Run(targetType+"/"+tc.route, func(t *testing.T) {
				rec.mu.Lock()
				rec.hits = nil
				rec.mu.Unlock()
				route.Store(tc.route)
				deliveryID := uuid.New()
				targetURL := origin.URL + targetPath + "?token=query-secret"
				queries := &fakeDeliveryQueries{runDeliveryRow: db.GetRunDeliveryRow{
					RunDelivery: db.RunDelivery{
						ID: deliveryID, TargetType: targetType, TargetURL: targetURL,
						Payload: payload, Status: "pending",
					},
					TargetSecret: &secret,
				}}
				// The production client: endpoint policy, only loopback opted in.
				svc := NewService(nil, &config.Config{AllowLocalHTTPEndpoints: true})
				svc.queries = queries
				require.NoError(t, svc.AttemptDelivery(context.Background(), deliveryID))
				require.Equal(t, deliveryID, queries.successArg.ID, "redirected delivery should succeed")

				first := rec.only(t, "origin", targetPath)
				require.Equal(t, deliveryID.String(), first.delivery)
				if tc.route == "sticky" {
					require.Empty(t, rec.only(t, "other", "/bounce").referer)
				}
				landing := rec.only(t, tc.landing[0], tc.landing[1])
				require.Equal(t, deliveryID.String(), landing.delivery, "non-credential headers still follow")
				if targetType == targetTypeWebhook {
					require.Equal(t, first.signature, landing.signature)
				}
				if tc.keeps {
					require.Equal(t, targetURL, landing.referer)
					return
				}
				require.Empty(t, landing.referer, "target URL leaked to another origin")
				require.False(t, strings.Contains(landing.referer, "secret"))
			})
		}
	}
}
