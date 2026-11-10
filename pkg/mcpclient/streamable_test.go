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
	"sync"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
)

type recordedMessage struct {
	Method    string
	ID        string
	Params    json.RawMessage
	Result    json.RawMessage
	Error     *rpcErrorObject
	SessionID string
	Protocol  string
	Header    http.Header
}

// streamableServer is a minimal Streamable HTTP MCP server for tests.
type streamableServer struct {
	t *testing.T

	// toolMode: "json", "sse", "error", "hang", "huge", "is-error".
	toolMode       string
	initStatus     int
	initRPCError   bool
	noSession      bool
	ignoreCanceled bool

	mu        sync.Mutex
	messages  []recordedMessage
	deletes   []string
	canceled  chan recordedMessage
	toolCalls chan struct{}
}

func newStreamableServer(t *testing.T, mode string) *streamableServer {
	return &streamableServer{t: t, toolMode: mode, canceled: make(chan recordedMessage, 4), toolCalls: make(chan struct{}, 4)}
}

func (s *streamableServer) recorded() []recordedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedMessage(nil), s.messages...)
}

func (s *streamableServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		s.mu.Lock()
		s.deletes = append(s.deletes, r.Header.Get(HeaderSessionID))
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
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
	rec := recordedMessage{
		Method: m.Method, ID: idKey(m.ID), Params: m.Params, Result: m.Result, Error: m.Error,
		SessionID: r.Header.Get(HeaderSessionID), Protocol: r.Header.Get(HeaderProtocolVersion), Header: r.Header.Clone(),
	}
	s.mu.Lock()
	s.messages = append(s.messages, rec)
	s.mu.Unlock()

	switch {
	case m.Method == "initialize":
		if s.initStatus != 0 {
			http.Error(w, "init refused", s.initStatus)
			return
		}
		if s.initRPCError {
			writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": m.ID, "error": map[string]any{"code": -32602, "message": "unsupported version"}})
			return
		}
		if !s.noSession {
			w.Header().Set(HeaderSessionID, "session-abc")
		}
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": m.ID, "result": map[string]any{
			"protocolVersion": "2025-03-26", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "test"},
		}})
	case m.Method == "notifications/cancelled":
		s.canceled <- rec
		if s.ignoreCanceled {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusAccepted)
	case m.Method == "" || strings.HasPrefix(m.Method, "notifications/"):
		w.WriteHeader(http.StatusAccepted)
	case m.Method == "tools/list":
		var params struct {
			Cursor string `json:"cursor"`
		}
		_ = json.Unmarshal(m.Params, &params)
		if params.Cursor == "" {
			writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": m.ID, "result": map[string]any{
				"tools":      []any{map[string]any{"name": "lookup", "description": "Look up", "inputSchema": map[string]any{"type": "object"}}},
				"nextCursor": "page-2",
			}})
			return
		}
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": m.ID, "result": map[string]any{
			"tools": []any{map[string]any{"name": "translate", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}}}},
		}})
	case m.Method == "tools/call":
		s.toolCalls <- struct{}{}
		s.serveToolCall(w, r, m.ID)
	default:
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": m.ID, "error": map[string]any{"code": -32601, "message": "nope"}})
	}
}

func (s *streamableServer) serveToolCall(w http.ResponseWriter, r *http.Request, id json.RawMessage) {
	result := map[string]any{"structuredContent": map[string]any{"answer": 42}, "content": []any{}}
	switch s.toolMode {
	case "json":
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	case "is-error":
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"isError": true, "content": []any{}}})
	case "error":
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32000, "message": "tool exploded"}})
	case "sse":
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		write := func(v any) {
			raw, _ := json.Marshal(v)
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", raw)
			flusher.Flush()
		}
		fmt.Fprint(w, ": keepalive comment\n\n")
		write(map[string]any{"jsonrpc": "2.0", "method": "notifications/progress", "params": map[string]any{"progress": 1}})
		write(map[string]any{"jsonrpc": "2.0", "id": "srv-ping", "method": "ping"})
		write(map[string]any{"jsonrpc": "2.0", "id": "srv-sample", "method": "sampling/createMessage", "params": map[string]any{}})
		write(map[string]any{"jsonrpc": "2.0", "id": "someone-else", "result": map[string]any{"wrong": true}})
		// A multi-line data field is joined with newlines.
		raw, _ := json.MarshalIndent(map[string]any{"jsonrpc": "2.0", "id": id, "result": result}, "", " ")
		fmt.Fprint(w, "event: message\n")
		for _, line := range strings.Split(string(raw), "\n") {
			fmt.Fprintf(w, "data: %s\n", line)
		}
		fmt.Fprint(w, "\n")
		flusher.Flush()
	case "huge":
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", strings.Repeat("x", 4096))
	case "hang":
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func testConfig(transport, endpoint string) Config {
	header := http.Header{}
	header.Set("Authorization", "Bearer secret-token")
	header.Set("X-OpenLinker-Run-Id", "run-1")
	return Config{
		Transport:      transport,
		EndpointURL:    endpoint,
		Header:         header,
		HTTPClient:     NewHTTPClient(nil, true),
		AllowLocalHTTP: true,
		Limits:         Limits{TotalTimeout: 5 * time.Second, EndpointTimeout: 2 * time.Second, CancelGrace: 500 * time.Millisecond},
	}
}

func toolParams() map[string]any {
	return map[string]any{
		"name":      "lookup",
		"arguments": map[string]any{"query": "q"},
		"_meta":     map[string]any{"run_id": "run-1", "platform": "openlinker"},
	}
}

func TestStreamableHTTPJSONResponseUsesSessionAndReleasesIt(t *testing.T) {
	server := newStreamableServer(t, "json")
	ts := httptest.NewServer(server)
	defer ts.Close()

	result, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts.URL+"/mcp"), "run-1", toolParams())
	if err != nil {
		t.Fatal(err)
	}
	if structured := result["structuredContent"].(map[string]any); structured["answer"] != float64(42) {
		t.Fatalf("result = %#v", result)
	}
	msgs := server.recorded()
	if len(msgs) != 3 || msgs[0].Method != "initialize" || msgs[1].Method != "notifications/initialized" || msgs[2].Method != "tools/call" {
		t.Fatalf("message order = %#v", msgs)
	}
	if msgs[0].SessionID != "" || msgs[0].Protocol != StreamableHTTPProtocolVersion {
		t.Fatalf("initialize headers session=%q protocol=%q", msgs[0].SessionID, msgs[0].Protocol)
	}
	var initParams map[string]any
	_ = json.Unmarshal(msgs[0].Params, &initParams)
	if initParams["protocolVersion"] != StreamableHTTPProtocolVersion {
		t.Fatalf("initialize params = %v", initParams)
	}
	for _, m := range msgs[1:] {
		if m.SessionID != "session-abc" || m.Protocol != "2025-03-26" {
			t.Fatalf("%s headers session=%q protocol=%q", m.Method, m.SessionID, m.Protocol)
		}
	}
	call := msgs[2]
	if call.ID != "run-1" || call.Header.Get("Authorization") != "Bearer secret-token" || call.Header.Get("X-OpenLinker-Run-Id") != "run-1" {
		t.Fatalf("tools/call id=%q headers=%v", call.ID, call.Header)
	}
	if accept := call.Header.Get("Accept"); !strings.Contains(accept, "application/json") || !strings.Contains(accept, "text/event-stream") {
		t.Fatalf("Accept = %q", accept)
	}
	var params map[string]any
	_ = json.Unmarshal(call.Params, &params)
	if params["name"] != "lookup" || params["arguments"].(map[string]any)["query"] != "q" || params["_meta"].(map[string]any)["run_id"] != "run-1" {
		t.Fatalf("tools/call params = %s", call.Params)
	}
	server.mu.Lock()
	deletes := append([]string(nil), server.deletes...)
	server.mu.Unlock()
	if len(deletes) != 1 || deletes[0] != "session-abc" {
		t.Fatalf("DELETE sessions = %v", deletes)
	}
}

func TestStreamableHTTPSSEResponseAnswersServerRequestsAndMatchesID(t *testing.T) {
	server := newStreamableServer(t, "sse")
	ts := httptest.NewServer(server)
	defer ts.Close()

	result, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts.URL), "run-1", toolParams())
	if err != nil {
		t.Fatal(err)
	}
	if result["structuredContent"].(map[string]any)["answer"] != float64(42) {
		t.Fatalf("result = %#v", result)
	}
	replies := map[string]recordedMessage{}
	for _, m := range server.recorded() {
		if m.Method == "" {
			replies[m.ID] = m
		}
	}
	if ping, ok := replies["srv-ping"]; !ok || string(ping.Result) != "{}" || ping.SessionID != "session-abc" {
		t.Fatalf("ping reply = %#v", replies["srv-ping"])
	}
	if sample, ok := replies["srv-sample"]; !ok || sample.Error == nil || sample.Error.Code != -32601 {
		t.Fatalf("sampling reply = %#v", replies["srv-sample"])
	}
	if len(replies) != 2 {
		t.Fatalf("Core answered notifications or responses: %#v", replies)
	}
}

func TestStreamableHTTPWithoutSessionIDOmitsHeaderAndDelete(t *testing.T) {
	server := newStreamableServer(t, "json")
	server.noSession = true
	ts := httptest.NewServer(server)
	defer ts.Close()
	if _, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts.URL), "run-1", toolParams()); err != nil {
		t.Fatal(err)
	}
	for _, m := range server.recorded() {
		if m.SessionID != "" {
			t.Fatalf("%s sent session %q", m.Method, m.SessionID)
		}
	}
	if len(server.deletes) != 0 {
		t.Fatalf("DELETE without a session: %v", server.deletes)
	}
}

func TestStreamableHTTPInitializeFailures(t *testing.T) {
	server := newStreamableServer(t, "json")
	server.initStatus = http.StatusUnauthorized
	ts := httptest.NewServer(server)
	defer ts.Close()
	_, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts.URL), "run-1", toolParams())
	var status *HTTPStatusError
	if !errors.As(err, &status) || status.Stage != "initialize" || status.StatusCode != http.StatusUnauthorized {
		t.Fatalf("err = %v", err)
	}

	server2 := newStreamableServer(t, "json")
	server2.initRPCError = true
	ts2 := httptest.NewServer(server2)
	defer ts2.Close()
	_, err = CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts2.URL), "run-1", toolParams())
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Stage != "initialize" || rpc.Code != -32602 {
		t.Fatalf("err = %v", err)
	}
	for _, m := range server2.recorded() {
		if m.Method == "tools/call" {
			t.Fatal("tools/call sent after a failed initialize")
		}
	}
}

func TestStreamableHTTPErrorResult(t *testing.T) {
	server := newStreamableServer(t, "error")
	ts := httptest.NewServer(server)
	defer ts.Close()
	_, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts.URL), "run-1", toolParams())
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Stage != "tools/call" || rpc.Code != -32000 || rpc.Message != "tool exploded" {
		t.Fatalf("err = %v", err)
	}
}

func TestStreamableHTTPEventSizeLimit(t *testing.T) {
	server := newStreamableServer(t, "huge")
	ts := httptest.NewServer(server)
	defer ts.Close()
	cfg := testConfig(TransportStreamableHTTP, ts.URL)
	cfg.Limits.MaxEventBytes = 1024
	_, err := CallTool(context.Background(), cfg, "run-1", toolParams())
	var protocol *ProtocolError
	if !errors.As(err, &protocol) || !strings.Contains(protocol.Message, "size limit") {
		t.Fatalf("err = %v", err)
	}
}

func TestStreamableHTTPCancellationSendsCancelledThenReleases(t *testing.T) {
	server := newStreamableServer(t, "hang")
	ts := httptest.NewServer(server)
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-server.toolCalls
		cancel()
	}()
	started := time.Now()
	_, err := CallTool(ctx, testConfig(TransportStreamableHTTP, ts.URL), "run-1", toolParams())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	select {
	case canceled := <-server.canceled:
		var params map[string]any
		_ = json.Unmarshal(canceled.Params, &params)
		if params["requestId"] != "run-1" || canceled.SessionID != "session-abc" {
			t.Fatalf("notifications/cancelled = %s session=%q", canceled.Params, canceled.SessionID)
		}
	default:
		t.Fatal("notifications/cancelled was not sent")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("cancellation took %s", elapsed)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.deletes) != 1 {
		t.Fatalf("session not released after cancellation: %v", server.deletes)
	}
}

func TestStreamableHTTPTotalTimeoutCancelsUpstreamWithinGrace(t *testing.T) {
	server := newStreamableServer(t, "hang")
	server.ignoreCanceled = true
	ts := httptest.NewServer(server)
	defer ts.Close()
	cfg := testConfig(TransportStreamableHTTP, ts.URL)
	cfg.Limits.TotalTimeout = 300 * time.Millisecond
	cfg.Limits.CancelGrace = 300 * time.Millisecond
	started := time.Now()
	_, err := CallTool(context.Background(), cfg, "run-1", toolParams())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("timeout plus bounded cancel took %s", elapsed)
	}
	select {
	case <-server.canceled:
	default:
		t.Fatal("notifications/cancelled was not attempted after the timeout")
	}
}

func TestStreamableHTTPRedirects(t *testing.T) {
	other := newStreamableServer(t, "json")
	otherTS := httptest.NewServer(other)
	defer otherTS.Close()
	server := newStreamableServer(t, "json")
	mux := http.NewServeMux()
	mux.Handle("/mcp", server)
	mux.HandleFunc("/moved", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/mcp", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/elsewhere", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, otherTS.URL+"/mcp", http.StatusTemporaryRedirect)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	if _, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts.URL+"/moved"), "run-1", toolParams()); err != nil {
		t.Fatalf("same-origin redirect: %v", err)
	}
	_, err := CallTool(context.Background(), testConfig(TransportStreamableHTTP, ts.URL+"/elsewhere"), "run-1", toolParams())
	if !errors.Is(err, endpointurl.ErrCrossOriginRedirect) {
		t.Fatalf("cross-origin redirect err = %v", err)
	}
	if got := other.recorded(); len(got) != 0 {
		t.Fatalf("credentials reached another origin: %#v", got)
	}
}

func TestListToolsPagesThroughStreamableHTTP(t *testing.T) {
	server := newStreamableServer(t, "json")
	ts := httptest.NewServer(server)
	defer ts.Close()
	tools, err := ListTools(context.Background(), testConfig(TransportStreamableHTTP, ts.URL))
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 || tools[0].Name != "lookup" || tools[0].Description != "Look up" || tools[1].Name != "translate" {
		t.Fatalf("tools = %#v", tools)
	}
	if !strings.Contains(string(tools[1].InputSchema), `"text"`) {
		t.Fatalf("schema = %s", tools[1].InputSchema)
	}
}

func TestSSEReaderLimits(t *testing.T) {
	stream := "data: " + strings.Repeat("a", 10) + "\n\n"
	r := newSSEReader(strings.NewReader(stream), 5, 1024)
	if _, err := r.next(); !errors.Is(err, errEventTooLarge) {
		t.Fatalf("event limit err = %v", err)
	}
	r = newSSEReader(strings.NewReader(strings.Repeat("data: ab\n\n", 50)), 64, 100)
	var err error
	for err == nil {
		_, err = r.next()
	}
	if !errors.Is(err, errStreamTooLarge) {
		t.Fatalf("stream limit err = %v", err)
	}
	r = newSSEReader(strings.NewReader("data: ok\n\n"), 64, 10)
	if event, err := r.next(); err != nil || string(event.Data) != "ok" {
		t.Fatalf("stream exactly at limit: %q %v", event.Data, err)
	}
	if _, err := r.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end of stream err = %v", err)
	}
}

func TestValidateAuthHeaderName(t *testing.T) {
	for _, ok := range []string{"Authorization", "X-Api-Key", "x-goog-api-key", "Api_Key"} {
		if err := ValidateAuthHeaderName(ok); err != nil {
			t.Fatalf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Bad Header", "X-Key:", "Host", "cookie", "Content-Type", "Mcp-Session-Id", "X-OpenLinker-Token",
		"Proxy-Authorization", "Transfer-Encoding", "Connection", "Accept", strings.Repeat("a", 129), "Ünicode"} {
		if err := ValidateAuthHeaderName(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestCredentialHeaderRouting(t *testing.T) {
	cases := []struct {
		value      string
		headerName *string
		wantName   string
	}{
		{"Bearer abc", nil, "Authorization"},
		{"abc", nil, "X-OpenLinker-Token"},
		{"abc", strPtr("X-Api-Key"), "X-Api-Key"},
		{"Bearer abc", strPtr("X-Api-Key"), "X-Api-Key"},
		{"abc", strPtr(""), "X-OpenLinker-Token"},
	}
	for _, c := range cases {
		name, value, ok := CredentialHeader(c.value, c.headerName)
		if !ok || name != c.wantName || value != c.value {
			t.Fatalf("CredentialHeader(%q,%v) = %q %q %v", c.value, c.headerName, name, value, ok)
		}
	}
	if _, _, ok := CredentialHeader("  ", strPtr("X-Api-Key")); ok {
		t.Fatal("empty credential produced a header")
	}
}

func strPtr(v string) *string { return &v }
