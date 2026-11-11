package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A server may leave an SSE response open after the matching result. The
// client must return at once instead of draining until the session deadline.
func TestStreamableHTTPReturnsWhenSSEResponseStaysOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&m)
		if strings.HasPrefix(m.Method, "notifications/") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		result := `{"structuredContent":{"answer":1}}`
		if m.Method == "initialize" {
			result = `{"protocolVersion":"2025-06-18"}`
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":%s}\n\n", m.ID, result)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	cfg := testConfig(TransportStreamableHTTP, server.URL)
	cfg.Limits.TotalTimeout = 5 * time.Second
	started := time.Now()
	result, err := CallTool(context.Background(), cfg, "run-1", toolParams())
	if err != nil {
		t.Fatal(err)
	}
	if result["structuredContent"] == nil {
		t.Fatalf("result = %#v", result)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("open SSE responses held the call for %s", elapsed)
	}
}

// JSON-RPC ids keep their type: the number 7 does not answer the string "7".
func TestStreamableHTTPDoesNotMatchNumberIDToStringID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&m)
		w.Header().Set("Content-Type", "application/json")
		switch m.Method {
		case "initialize":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18"}}`, m.ID)
		case "tools/call":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":7,"result":{"structuredContent":{}}}`)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer server.Close()
	_, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, server.URL), "7", toolParams())
	var protocol *ProtocolError
	if !errors.As(err, &protocol) || !strings.Contains(protocol.Message, "does not answer") {
		t.Fatalf("err = %v", err)
	}
}

func TestLegacySSEDoesNotMatchNumberIDToStringID(t *testing.T) {
	server := newLegacySSEServer(t, "custom")
	ts := httptest.NewServer(server)
	defer ts.Close()
	server.onToolCall = func(id json.RawMessage) {
		server.stream <- "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"structuredContent\":{\"from\":\"number\"}}}\n\n"
		server.push(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"structuredContent": map[string]any{"from": "string"}}})
	}
	result, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/sse"), "7", toolParams())
	if err != nil {
		t.Fatal(err)
	}
	if result["structuredContent"].(map[string]any)["from"] != "string" {
		t.Fatalf("number id answered a string-id request: %#v", result)
	}
}

// A flood of server requests is answered by one bounded worker, so it can
// neither exhaust goroutines nor send unbounded POSTs.
func TestLegacySSEBoundsRepliesToServerRequests(t *testing.T) {
	server := newLegacySSEServer(t, "custom")
	ts := httptest.NewServer(server)
	defer ts.Close()
	server.onToolCall = func(id json.RawMessage) {
		var b strings.Builder
		for i := 0; i < 500; i++ {
			fmt.Fprintf(&b, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"flood-%d\",\"method\":\"ping\"}\n\n", i)
		}
		server.stream <- b.String()
		time.Sleep(200 * time.Millisecond)
		server.push(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"structuredContent": map[string]any{"ok": true}}})
	}
	if _, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/sse"), "run-1", toolParams()); err != nil {
		t.Fatal(err)
	}
	replies := 0
	for _, m := range server.recordedPosts() {
		if m.Method == "" {
			replies++
		}
	}
	if replies == 0 || replies > maxServerRequestReplies {
		t.Fatalf("replies to 500 server requests = %d, want 1..%d", replies, maxServerRequestReplies)
	}
}

// Server requests that arrive before the endpoint event are not answered
// until the message URL is validated, and never if it is rejected.
func TestLegacySSEAnswersServerRequestsOnlyAfterEndpointValidation(t *testing.T) {
	other := newLegacySSEServer(t, "result")
	otherTS := httptest.NewServer(other)
	defer otherTS.Close()
	for _, crossOrigin := range []bool{false, true} {
		t.Run(fmt.Sprintf("crossOrigin=%t", crossOrigin), func(t *testing.T) {
			server := newLegacySSEServer(t, "result")
			server.preEndpoint = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"early\",\"method\":\"ping\"}\n\n"
			ts := httptest.NewServer(server)
			defer ts.Close()
			if crossOrigin {
				server.endpoint = otherTS.URL + "/messages?sessionId=s1"
			}
			_, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/sse"), "run-1", toolParams())
			early := 0
			for _, m := range server.recordedPosts() {
				if m.ID == "early" && m.Method == "" {
					early++
				}
			}
			if crossOrigin {
				if !errors.Is(err, ErrEndpointRejected) || early != 0 || len(other.recordedPosts()) != 0 {
					t.Fatalf("err=%v early=%d other=%d", err, early, len(other.recordedPosts()))
				}
				return
			}
			if err != nil || early != 1 {
				t.Fatalf("err=%v early replies=%d", err, early)
			}
		})
	}
}

func TestSSEReaderAcceptsCRLFCRAndLeadingBOM(t *testing.T) {
	for name, stream := range map[string]string{
		"lf":   "\xEF\xBB\xBFevent: endpoint\ndata: /a\n\ndata: x\ndata: y\n\n",
		"crlf": "event: endpoint\r\ndata: /a\r\n\r\ndata: x\r\ndata: y\r\n\r\n",
		"cr":   "\xEF\xBB\xBFevent: endpoint\rdata: /a\r\rdata: x\rdata: y\r\r",
	} {
		t.Run(name, func(t *testing.T) {
			r := newSSEReader(strings.NewReader(stream), 1024, 4096)
			first, err := r.next()
			if err != nil || first.Event != "endpoint" || string(first.Data) != "/a" {
				t.Fatalf("first = %q %q %v", first.Event, first.Data, err)
			}
			second, err := r.next()
			if err != nil || second.Event != "message" || string(second.Data) != "x\ny" {
				t.Fatalf("second = %q %q %v", second.Event, second.Data, err)
			}
			if _, err := r.next(); !errors.Is(err, io.EOF) {
				t.Fatalf("end err = %v", err)
			}
		})
	}
}

// A CR-terminated event is dispatched without waiting for a following byte.
func TestSSEReaderDispatchesCRTerminatedEventWithoutMoreBytes(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	r := newSSEReader(pr, 1024, 4096)
	got := make(chan sseEvent, 1)
	go func() {
		event, err := r.next()
		if err == nil {
			got <- event
		}
	}()
	go func() { _, _ = pw.Write([]byte("data: ready\r\r")) }()
	select {
	case event := <-got:
		if string(event.Data) != "ready" {
			t.Fatalf("event = %q", event.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("CR-terminated event waited for more bytes")
	}
}

// The server writes a ping, then a result larger than the socket buffers,
// and acknowledges Core's ping reply only after the result stream has been
// consumed. Replying inline would leave both sides waiting until the
// timeout; the reply worker keeps the stream read.
func TestStreamableHTTPRepliesDoNotBlockAResultStream(t *testing.T) {
	resultWritten := make(chan struct{})
	replyArrived := make(chan struct{}, 1)
	var replyAcked atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&m)
		switch {
		case m.Method == "initialize":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18"}}`, m.ID)
		case m.Method == "tools/call":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"srv-ping\",\"method\":\"ping\"}\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-replyArrived:
			case <-r.Context().Done():
				return
			}
			big := strings.Repeat("r", 3<<20)
			fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"structuredContent\":{\"blob\":\"%s\"}}}\n\n", m.ID, big)
			w.(http.Flusher).Flush()
			close(resultWritten)
		case m.Method == "" && string(m.ID) == `"srv-ping"`:
			replyArrived <- struct{}{}
			select {
			case <-resultWritten:
				replyAcked.Store(true)
				w.WriteHeader(http.StatusAccepted)
			case <-r.Context().Done():
			}
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer server.Close()
	cfg := testConfig(TransportStreamableHTTP, server.URL)
	cfg.Limits.TotalTimeout = 10 * time.Second
	started := time.Now()
	result, err := CallTool(context.Background(), cfg, "run-1", toolParams())
	if err != nil {
		t.Fatal(err)
	}
	if blob, _ := result["structuredContent"].(map[string]any)["blob"].(string); len(blob) != 3<<20 {
		t.Fatalf("result blob length = %d", len(blob))
	}
	// A deadlock holds both sides until TotalTimeout (10s) and fails CallTool
	// above. This bound only catches a near-deadlock; it leaves room for the
	// race detector on shared CI runners, where the 3 MiB result alone has
	// taken over 3s (locally it takes under 1s).
	if elapsed := time.Since(started); elapsed > 8*time.Second {
		t.Fatalf("reply POST and result stream deadlocked for %s", elapsed)
	}
	if !replyAcked.Load() {
		t.Fatal("ping reply was not delivered once the result was consumed")
	}
}

// Replies already queued when the result arrives still go out.
func TestStreamableHTTPDeliversQueuedRepliesAfterResult(t *testing.T) {
	server := newStreamableServer(t, "sse")
	ts := httptest.NewServer(server)
	defer ts.Close()
	// streamableServer buffers four tool-call signals; stay within them.
	for i := 0; i < 4; i++ {
		before := len(server.recorded())
		if _, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts.URL), "run-1", toolParams()); err != nil {
			t.Fatal(err)
		}
		replies := 0
		for _, m := range server.recorded()[before:] {
			if m.Method == "" {
				replies++
			}
		}
		if replies != 2 {
			t.Fatalf("run %d: replies = %d, want ping and sampling", i, replies)
		}
	}
}
