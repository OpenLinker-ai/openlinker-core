package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
)

// The production webhook client (task callbacks and A2A push included) must
// not forward the subscriber's Authorization credential when the callback URL
// redirects to another origin, while a same-origin redirect keeps it.
func TestDoDeliverWithEventDropsAuthorizationOnCrossOriginRedirect(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]http.Header{}
	record := func(r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		seen[r.Host+r.URL.Path] = r.Header.Clone()
	}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record(r)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer other.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record(r)
		switch r.URL.Path {
		case "/cross":
			http.Redirect(w, r, other.URL+"/landing", http.StatusPermanentRedirect)
		case "/same":
			http.Redirect(w, r, "/landing", http.StatusPermanentRedirect)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer origin.Close()

	svc := NewService(nil, &config.Config{AllowLocalHTTPEndpoints: true})
	payload := []byte(`{"event_type":"task.completed"}`)
	scheme, credentials := "Bearer", "push-credential"

	status, _, err := svc.doDeliverWithEvent(context.Background(), origin.URL+"/cross", "secret", uuid.New(), payload, "task.completed", &scheme, &credentials)
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("cross-origin redirect delivery = %d %v", status, err)
	}
	mu.Lock()
	landing := seen[other.Listener.Addr().String()+"/landing"]
	first := seen[origin.Listener.Addr().String()+"/cross"]
	mu.Unlock()
	if first.Get("Authorization") != "Bearer push-credential" {
		t.Fatalf("first hop authorization = %q", first.Get("Authorization"))
	}
	if landing == nil || landing.Get("Authorization") != "" {
		t.Fatalf("authorization reached another origin: %#v", landing)
	}
	if landing.Get("X-OpenLinker-Delivery") == "" || landing.Get("X-OpenLinker-Signature") == "" {
		t.Fatalf("delivery metadata should still follow the redirect: %#v", landing)
	}

	status, _, err = svc.doDeliverWithEvent(context.Background(), origin.URL+"/same", "secret", uuid.New(), payload, "task.completed", &scheme, &credentials)
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("same-origin redirect delivery = %d %v", status, err)
	}
	mu.Lock()
	same := seen[origin.Listener.Addr().String()+"/landing"]
	mu.Unlock()
	if same.Get("Authorization") != "Bearer push-credential" {
		t.Fatalf("same-origin redirect should keep authorization, got %q", same.Get("Authorization"))
	}
}
