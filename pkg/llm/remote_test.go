package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewRemoteClientReturnsNilWithoutEndpoint(t *testing.T) {
	if got := NewRemoteClient(" ", "secret"); got != nil {
		t.Fatalf("NewRemoteClient(empty) = %#v, want nil", got)
	}
}

func TestRemoteClientCompleteSendsInternalRequest(t *testing.T) {
	var gotToken string
	var gotReq remoteCompleteRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-OpenLinker-Internal-Token")
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(`{"text":"{\"skills\":[\"data\"]}"}`))
	}))
	defer server.Close()

	client := NewRemoteClient(server.URL, "internal-secret")
	client.http = server.Client()
	got, err := client.Complete(context.Background(), "system prompt", "user prompt")
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if got != `{"skills":["data"]}` {
		t.Fatalf("Complete = %q", got)
	}
	if gotToken != "internal-secret" {
		t.Fatalf("internal token = %q", gotToken)
	}
	if gotReq.System != "system prompt" || gotReq.User != "user prompt" {
		t.Fatalf("request = %#v", gotReq)
	}
}

func TestRemoteClientCompleteReturnsErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "remote status", status: http.StatusServiceUnavailable, body: `{"error":"disabled"}`},
		{name: "malformed", status: http.StatusOK, body: `not-json`},
		{name: "empty", status: http.StatusOK, body: `{"text":""}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			client := NewRemoteClient(server.URL, "")
			client.http = server.Client()
			if _, err := client.Complete(context.Background(), "", "hello"); err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}
