package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
)

// legacySSEServer is a minimal 2024-11-05 HTTP+SSE MCP server for tests.
type legacySSEServer struct {
	t *testing.T

	// endpoint is the endpoint event data; empty sends none.
	endpoint string
	// toolMode: "result", "out-of-order", "error", "hang", "huge".
	toolMode string
	// postStatus is the status for POSTs; 0 means 202.
	postStatus     int
	ignoreCanceled bool
	// preEndpoint is written before the endpoint event.
	preEndpoint string
	// onToolCall replaces the toolMode answer when toolMode is "custom".
	onToolCall func(id json.RawMessage)

	mu       sync.Mutex
	stream   chan string
	posts    []recordedMessage
	gets     []http.Header
	canceled chan recordedMessage
	toolCall chan struct{}
	closed   chan struct{}
}

func newLegacySSEServer(t *testing.T, mode string) *legacySSEServer {
	return &legacySSEServer{
		t: t, endpoint: "/messages?sessionId=s1", toolMode: mode,
		stream: make(chan string, 64), canceled: make(chan recordedMessage, 4),
		toolCall: make(chan struct{}, 4), closed: make(chan struct{}, 4),
	}
}

func (s *legacySSEServer) push(v any) {
	raw, _ := json.Marshal(v)
	s.stream <- fmt.Sprintf("event: message\ndata: %s\n\n", raw)
}

func (s *legacySSEServer) recordedPosts() []recordedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedMessage(nil), s.posts...)
}

func (s *legacySSEServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/sse":
		s.mu.Lock()
		s.gets = append(s.gets, r.Header.Clone())
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		fmt.Fprint(w, s.preEndpoint)
		if s.endpoint != "" {
			fmt.Fprintf(w, ": hello\n\nevent: endpoint\ndata: %s\n\n", s.endpoint)
		}
		flusher.Flush()
		for {
			select {
			case chunk := <-s.stream:
				fmt.Fprint(w, chunk)
				flusher.Flush()
			case <-r.Context().Done():
				s.closed <- struct{}{}
				return
			}
		}
	case r.Method == http.MethodPost && r.URL.Path == "/messages":
		if r.URL.Query().Get("sessionId") != "s1" {
			http.Error(w, "unknown session", http.StatusNotFound)
			return
		}
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
			Result json.RawMessage `json:"result"`
			Error  *rpcErrorObject `json:"error"`
		}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		rec := recordedMessage{Method: m.Method, ID: idKey(m.ID), Params: m.Params, Result: m.Result, Error: m.Error, Header: r.Header.Clone()}
		s.mu.Lock()
		s.posts = append(s.posts, rec)
		s.mu.Unlock()
		if m.Method == "notifications/cancelled" {
			s.canceled <- rec
			if s.ignoreCanceled {
				<-r.Context().Done()
				return
			}
		}
		if s.postStatus != 0 {
			http.Error(w, "refused", s.postStatus)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("Accepted"))
		go s.answer(m.ID, m.Method)
	default:
		http.NotFound(w, r)
	}
}

func (s *legacySSEServer) answer(id json.RawMessage, method string) {
	result := map[string]any{"structuredContent": map[string]any{"answer": 7}}
	switch method {
	case "initialize":
		s.push(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"protocolVersion": LegacySSEProtocolVersion, "capabilities": map[string]any{}}})
	case "tools/list":
		s.push(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"tools": []any{map[string]any{"name": "lookup", "inputSchema": map[string]any{"type": "object"}}}}})
	case "tools/call":
		s.toolCall <- struct{}{}
		switch s.toolMode {
		case "custom":
			s.onToolCall(id)
		case "result":
			s.push(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
		case "out-of-order":
			s.push(map[string]any{"jsonrpc": "2.0", "id": "unrelated", "result": map[string]any{"wrong": true}})
			s.push(map[string]any{"jsonrpc": "2.0", "method": "notifications/message", "params": map[string]any{"level": "info"}})
			s.push(map[string]any{"jsonrpc": "2.0", "id": 99, "method": "ping"})
			s.push(map[string]any{"jsonrpc": "2.0", "id": "srv-roots", "method": "roots/list"})
			time.Sleep(50 * time.Millisecond)
			s.push(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
		case "error":
			s.push(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32002, "message": "no such tool"}})
		case "huge":
			s.stream <- "event: message\ndata: " + strings.Repeat("y", 4096) + "\n\n"
		case "flood":
			for i := 0; i < 40; i++ {
				s.push(map[string]any{"jsonrpc": "2.0", "method": "notifications/message", "params": map[string]any{"data": strings.Repeat("z", 200)}})
			}
		case "hang":
		}
	}
}

func TestLegacySSERelativeAndAbsoluteEndpointAndPOST202(t *testing.T) {
	for _, absolute := range []bool{false, true} {
		t.Run(fmt.Sprintf("absolute=%t", absolute), func(t *testing.T) {
			server := newLegacySSEServer(t, "result")
			ts := httptest.NewServer(server)
			defer ts.Close()
			if absolute {
				server.endpoint = ts.URL + "/messages?sessionId=s1"
			}
			result, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/sse"), "run-1", toolParams())
			if err != nil {
				t.Fatal(err)
			}
			if result["structuredContent"].(map[string]any)["answer"] != float64(7) {
				t.Fatalf("result = %#v", result)
			}
			posts := server.recordedPosts()
			if len(posts) != 3 || posts[0].Method != "initialize" || posts[1].Method != "notifications/initialized" || posts[2].Method != "tools/call" {
				t.Fatalf("posts = %#v", posts)
			}
			var init map[string]any
			_ = json.Unmarshal(posts[0].Params, &init)
			if init["protocolVersion"] != LegacySSEProtocolVersion {
				t.Fatalf("initialize params = %s", posts[0].Params)
			}
			if posts[2].Header.Get("Authorization") != "Bearer secret-token" || posts[2].ID != "run-1" {
				t.Fatalf("tools/call id=%q headers=%v", posts[2].ID, posts[2].Header)
			}
			var params map[string]any
			_ = json.Unmarshal(posts[2].Params, &params)
			if params["arguments"].(map[string]any)["query"] != "q" || params["_meta"].(map[string]any)["platform"] != "openlinker" {
				t.Fatalf("tools/call params = %s", posts[2].Params)
			}
			server.mu.Lock()
			getHeader := server.gets[0]
			server.mu.Unlock()
			if getHeader.Get("Accept") != "text/event-stream" || getHeader.Get("Authorization") != "Bearer secret-token" {
				t.Fatalf("GET headers = %v", getHeader)
			}
			select {
			case <-server.closed:
			case <-time.After(2 * time.Second):
				t.Fatal("event stream was not closed after the result")
			}
		})
	}
}

func TestLegacySSERejectsCrossOriginEndpoint(t *testing.T) {
	other := newLegacySSEServer(t, "result")
	otherTS := httptest.NewServer(other)
	defer otherTS.Close()
	server := newLegacySSEServer(t, "result")
	server.endpoint = otherTS.URL + "/messages?sessionId=s1"
	ts := httptest.NewServer(server)
	defer ts.Close()
	_, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/sse"), "run-1", toolParams())
	if !errors.Is(err, ErrEndpointRejected) {
		t.Fatalf("err = %v", err)
	}
	if posts := other.recordedPosts(); len(posts) != 0 {
		t.Fatalf("credentials reached another origin: %#v", posts)
	}
	// A same-origin URL that fails the outbound policy is also rejected.
	server2 := newLegacySSEServer(t, "result")
	ts2 := httptest.NewServer(server2)
	defer ts2.Close()
	cfg := testConfig(TransportLegacySSE, ts2.URL+"/sse")
	cfg.ValidateURL = func(string) error { return errors.New("policy says no") }
	if _, err := CallTool(context.Background(), cfg, "run-1", toolParams()); !errors.Is(err, ErrEndpointRejected) {
		t.Fatalf("policy rejection err = %v", err)
	}
	if posts := server2.recordedPosts(); len(posts) != 0 {
		t.Fatalf("posted to a rejected message URL: %#v", posts)
	}
}

func TestLegacySSEEndpointTimeout(t *testing.T) {
	server := newLegacySSEServer(t, "result")
	server.endpoint = ""
	ts := httptest.NewServer(server)
	defer ts.Close()
	cfg := testConfig(TransportLegacySSE, ts.URL+"/sse")
	cfg.Limits.EndpointTimeout = 200 * time.Millisecond
	started := time.Now()
	_, err := CallTool(context.Background(), cfg, "run-1", toolParams())
	var protocol *ProtocolError
	if !errors.As(err, &protocol) || !strings.Contains(protocol.Message, "endpoint event") {
		t.Fatalf("err = %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("endpoint timeout took %s", time.Since(started))
	}
}

func TestLegacySSEOutOfOrderResultsAndServerRequests(t *testing.T) {
	server := newLegacySSEServer(t, "out-of-order")
	ts := httptest.NewServer(server)
	defer ts.Close()
	result, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/sse"), "run-1", toolParams())
	if err != nil {
		t.Fatal(err)
	}
	if result["structuredContent"].(map[string]any)["answer"] != float64(7) {
		t.Fatalf("result = %#v", result)
	}
	replies := map[string]recordedMessage{}
	for _, m := range server.recordedPosts() {
		if m.Method == "" {
			replies[m.ID] = m
		}
	}
	if ping, ok := replies["99"]; !ok || string(ping.Result) != "{}" {
		t.Fatalf("ping reply = %#v", replies)
	}
	if roots, ok := replies["srv-roots"]; !ok || roots.Error == nil || roots.Error.Code != -32601 {
		t.Fatalf("roots/list reply = %#v", replies)
	}
	if len(replies) != 2 {
		t.Fatalf("unexpected replies: %#v", replies)
	}
}

func TestLegacySSEErrorResultAndPOSTFailure(t *testing.T) {
	server := newLegacySSEServer(t, "error")
	ts := httptest.NewServer(server)
	defer ts.Close()
	_, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/sse"), "run-1", toolParams())
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Code != -32002 || rpc.Stage != "tools/call" {
		t.Fatalf("err = %v", err)
	}

	refused := newLegacySSEServer(t, "result")
	refused.postStatus = http.StatusForbidden
	ts2 := httptest.NewServer(refused)
	defer ts2.Close()
	_, err = CallTool(context.Background(), testConfig(TransportLegacySSE, ts2.URL+"/sse"), "run-1", toolParams())
	var status *HTTPStatusError
	if !errors.As(err, &status) || status.StatusCode != http.StatusForbidden || status.Stage != "initialize" {
		t.Fatalf("err = %v", err)
	}
}

func TestLegacySSEByteLimits(t *testing.T) {
	server := newLegacySSEServer(t, "huge")
	ts := httptest.NewServer(server)
	defer ts.Close()
	cfg := testConfig(TransportLegacySSE, ts.URL+"/sse")
	cfg.Limits.MaxEventBytes = 1024
	_, err := CallTool(context.Background(), cfg, "run-1", toolParams())
	var protocol *ProtocolError
	if !errors.As(err, &protocol) || !strings.Contains(protocol.Message, "event exceeds") {
		t.Fatalf("event limit err = %v", err)
	}

	flood := newLegacySSEServer(t, "flood")
	ts2 := httptest.NewServer(flood)
	defer ts2.Close()
	cfg = testConfig(TransportLegacySSE, ts2.URL+"/sse")
	cfg.Limits.MaxEventBytes = 1024
	cfg.Limits.MaxStreamBytes = 2048
	_, err = CallTool(context.Background(), cfg, "run-1", toolParams())
	if !errors.As(err, &protocol) || !strings.Contains(protocol.Message, "stream exceeds") {
		t.Fatalf("stream limit err = %v", err)
	}
}

func TestLegacySSECancellationSendsCancelledBeforeClosing(t *testing.T) {
	server := newLegacySSEServer(t, "hang")
	ts := httptest.NewServer(server)
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-server.toolCall
		cancel()
	}()
	_, err := CallTool(ctx, testConfig(TransportLegacySSE, ts.URL+"/sse"), "run-1", toolParams())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	var canceled recordedMessage
	select {
	case canceled = <-server.canceled:
	default:
		t.Fatal("notifications/cancelled was not sent")
	}
	var params map[string]any
	_ = json.Unmarshal(canceled.Params, &params)
	if params["requestId"] != "run-1" {
		t.Fatalf("cancelled params = %s", canceled.Params)
	}
	select {
	case <-server.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("event stream was not closed after cancellation")
	}
}

func TestLegacySSECancellationGivesUpWithinGrace(t *testing.T) {
	server := newLegacySSEServer(t, "hang")
	server.ignoreCanceled = true
	ts := httptest.NewServer(server)
	defer ts.Close()
	cfg := testConfig(TransportLegacySSE, ts.URL+"/sse")
	cfg.Limits.CancelGrace = 300 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-server.toolCall
		cancel()
	}()
	started := time.Now()
	_, err := CallTool(ctx, cfg, "run-1", toolParams())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("unresponsive upstream held cancellation for %s", elapsed)
	}
	select {
	case <-server.canceled:
	default:
		t.Fatal("notifications/cancelled was not attempted")
	}
}

func TestLegacySSERejectsCrossOriginRedirect(t *testing.T) {
	other := newLegacySSEServer(t, "result")
	otherTS := httptest.NewServer(other)
	defer otherTS.Close()
	server := newLegacySSEServer(t, "result")
	mux := http.NewServeMux()
	mux.Handle("/", server)
	mux.HandleFunc("/old-sse", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, otherTS.URL+"/sse", http.StatusFound)
	})
	mux.HandleFunc("/moved-sse", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/sse", http.StatusFound)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	_, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/old-sse"), "run-1", toolParams())
	if !errors.Is(err, endpointurl.ErrCrossOriginRedirect) {
		t.Fatalf("err = %v", err)
	}
	other.mu.Lock()
	gets := len(other.gets)
	other.mu.Unlock()
	if gets != 0 {
		t.Fatal("the event stream request reached another origin")
	}
	if _, err := CallTool(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/moved-sse"), "run-1", toolParams()); err != nil {
		t.Fatalf("same-origin redirect: %v", err)
	}
}

func TestListToolsOverLegacySSE(t *testing.T) {
	server := newLegacySSEServer(t, "result")
	ts := httptest.NewServer(server)
	defer ts.Close()
	tools, err := ListTools(context.Background(), testConfig(TransportLegacySSE, ts.URL+"/sse"))
	if err != nil || len(tools) != 1 || tools[0].Name != "lookup" {
		t.Fatalf("tools=%#v err=%v", tools, err)
	}
}
