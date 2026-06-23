package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewAnthropicClientReturnsNilWithoutKey(t *testing.T) {
	if got := NewAnthropicClient(" "); got != nil {
		t.Fatalf("NewAnthropicClient(empty) = %#v, want nil", got)
	}
}

func TestAnthropicClientCompleteSendsMessagesRequest(t *testing.T) {
	var sawAPIKey, sawVersion string
	var sawBody anthropicMessageRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAPIKey = r.Header.Get("x-api-key")
		sawVersion = r.Header.Get("anthropic-version")
		if err := json.NewDecoder(r.Body).Decode(&sawBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"[\"data/analysis\"]"}]}`))
	}))
	defer srv.Close()

	client := newAnthropicClient("sk-ant-test", srv.URL, "claude-test", srv.Client())
	got, err := client.Complete(context.Background(), "system prompt", "user prompt")
	if err != nil {
		t.Fatalf("Complete error = %v", err)
	}
	if got != `["data/analysis"]` {
		t.Fatalf("Complete = %q", got)
	}
	if sawAPIKey != "sk-ant-test" || sawVersion != anthropicAPIVersion {
		t.Fatalf("headers api=%q version=%q", sawAPIKey, sawVersion)
	}
	if sawBody.Model != "claude-test" || sawBody.System != "system prompt" ||
		len(sawBody.Messages) != 1 || sawBody.Messages[0].Content != "user prompt" {
		t.Fatalf("unexpected body = %#v", sawBody)
	}
}

func TestAnthropicClientCompleteReturnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"type":"authentication_error","message":"bad key"}}`))
	}))
	defer srv.Close()

	client := newAnthropicClient("sk-ant-test", srv.URL, "claude-test", srv.Client())
	if _, err := client.Complete(context.Background(), "", "hi"); err == nil {
		t.Fatal("Complete error = nil, want API error")
	}
}
