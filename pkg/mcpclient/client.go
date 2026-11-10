package mcpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const (
	initializeRequestID = "openlinker-initialize"
	jsonRPCVersion      = "2.0"
	methodNotFound      = -32601
)

// message is one JSON-RPC 2.0 message in either direction.
type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  any             `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcErrorObject `json:"error,omitempty"`
}

type incoming struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Result json.RawMessage `json:"result"`
	Error  *rpcErrorObject `json:"error"`
}

type rpcErrorObject struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (m incoming) isResponse() bool {
	return m.Method == "" && len(m.ID) > 0 && string(m.ID) != "null"
}

func (m incoming) isRequest() bool {
	return m.Method != "" && len(m.ID) > 0 && string(m.ID) != "null"
}

// idKey normalizes a JSON-RPC id: a string id is its value, a number id is
// its literal text. Core sends string ids, so a server echoing them matches.
func idKey(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

func rawID(id string) json.RawMessage {
	encoded, _ := json.Marshal(id)
	return encoded
}

// replyToServerRequest answers a request the server sent: ping succeeds,
// everything else is method-not-found. Core does not offer sampling, roots or
// elicitation.
func replyToServerRequest(req incoming) message {
	reply := message{JSONRPC: jsonRPCVersion, ID: req.ID}
	if req.Method == "ping" {
		reply.Result = json.RawMessage(`{}`)
		return reply
	}
	reply.Error = &rpcErrorObject{Code: methodNotFound, Message: "Method not found"}
	return reply
}

// conn is one transport session. request returns the response message whose
// id matches; notify sends a notification; cancel sends
// notifications/cancelled for an outstanding request; close releases the
// session. cancel and close are best effort and bounded by their context.
type conn interface {
	request(ctx context.Context, stage, id, method string, params any) (incoming, error)
	notify(ctx context.Context, method string, params any) error
	setProtocolVersion(version string)
	close(ctx context.Context)
}

// Tool is one entry of tools/list.
type Tool struct {
	Name        string          `json:"name"`
	Title       string          `json:"title,omitempty"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

const (
	maxToolListPages = 20
	maxToolsListed   = 500
)

// CallTool opens a session, calls tools/call with id and params, and closes
// the session. It returns the JSON-RPC result object. When ctx is canceled
// or the total timeout passes while the call is outstanding, the server is
// sent notifications/cancelled for id within Limits.CancelGrace before the
// session is closed, because closing a stream alone does not stop work the
// server already accepted.
func CallTool(ctx context.Context, cfg Config, id string, params any) (map[string]any, error) {
	var result map[string]any
	err := withSession(ctx, cfg, func(ctx context.Context, c conn) error {
		response, err := c.request(ctx, "tools/call", id, "tools/call", params)
		if err != nil {
			return err
		}
		if len(response.Result) == 0 || string(response.Result) == "null" {
			return &ProtocolError{Stage: "tools/call", Message: "MCP endpoint returned no tool result"}
		}
		if json.Unmarshal(response.Result, &result) != nil || result == nil {
			return &ProtocolError{Stage: "tools/call", Message: "MCP tool result is not an object"}
		}
		return nil
	}, id)
	return result, err
}

// ListTools opens a session, pages through tools/list, and closes it.
func ListTools(ctx context.Context, cfg Config) ([]Tool, error) {
	var tools []Tool
	err := withSession(ctx, cfg, func(ctx context.Context, c conn) error {
		cursor := ""
		for page := 1; page <= maxToolListPages; page++ {
			var params any
			if cursor != "" {
				params = map[string]any{"cursor": cursor}
			}
			id := "openlinker-tools-list-" + strconv.Itoa(page)
			response, err := c.request(ctx, "tools/list", id, "tools/list", params)
			if err != nil {
				return err
			}
			var listed struct {
				Tools      []Tool `json:"tools"`
				NextCursor string `json:"nextCursor"`
			}
			if json.Unmarshal(response.Result, &listed) != nil {
				return &ProtocolError{Stage: "tools/list", Message: "tools/list result is malformed"}
			}
			tools = append(tools, listed.Tools...)
			if len(tools) > maxToolsListed {
				return &ProtocolError{Stage: "tools/list", Message: fmt.Sprintf("MCP endpoint lists more than %d tools", maxToolsListed)}
			}
			if listed.NextCursor == "" {
				return nil
			}
			cursor = listed.NextCursor
		}
		return &ProtocolError{Stage: "tools/list", Message: "tools/list did not finish paging"}
	}, "")
	return tools, err
}

// withSession runs one initialized session. cancelID names the request that
// is canceled on the server if ctx ends while fn runs; empty means none.
func withSession(parent context.Context, cfg Config, fn func(context.Context, conn) error, cancelID string) error {
	cfg.Limits = cfg.Limits.withDefaults()
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = NewHTTPClient(nil, cfg.AllowLocalHTTP)
	}
	ctx, cancel := context.WithTimeout(parent, cfg.Limits.TotalTimeout)
	defer cancel()

	var c conn
	var err error
	requestedVersion := StreamableHTTPProtocolVersion
	switch cfg.Transport {
	case TransportStreamableHTTP:
		c = newStreamableConn(cfg)
	case TransportLegacySSE:
		requestedVersion = LegacySSEProtocolVersion
		c, err = dialLegacySSE(ctx, cfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("mcpclient: unsupported transport %q", cfg.Transport)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(parent), cfg.Limits.CancelGrace)
		defer closeCancel()
		c.close(closeCtx)
	}()

	initialized, err := c.request(ctx, "initialize", initializeRequestID, "initialize", map[string]any{
		"protocolVersion": requestedVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "openlinker-core", "version": "1.0"},
	})
	if err != nil {
		return err
	}
	var init struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if len(initialized.Result) == 0 || json.Unmarshal(initialized.Result, &init) != nil {
		return &ProtocolError{Stage: "initialize", Message: "initialize result is malformed"}
	}
	if init.ProtocolVersion == "" {
		init.ProtocolVersion = requestedVersion
	}
	c.setProtocolVersion(init.ProtocolVersion)
	if err := c.notify(ctx, "notifications/initialized", nil); err != nil {
		return err
	}

	err = fn(ctx, c)
	if err != nil && ctx.Err() != nil {
		if cancelID != "" {
			cancelCtx, cancelCancel := context.WithTimeout(context.WithoutCancel(parent), cfg.Limits.CancelGrace)
			_ = c.notify(cancelCtx, "notifications/cancelled", map[string]any{
				"requestId": cancelID,
				"reason":    "OpenLinker Run was canceled or timed out",
			})
			cancelCancel()
		}
		return ctx.Err()
	}
	return err
}

// newJSONPost builds a JSON POST with Core's headers.
func newJSONPost(ctx context.Context, cfg Config, target string, body any) (*http.Request, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	copyHeader(req.Header, cfg.Header)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func copyHeader(dst, src http.Header) {
	for name, values := range src {
		dst.Del(name)
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func readLimited(body io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, errEventTooLarge
	}
	return raw, nil
}

func drain(body io.ReadCloser, limit int64) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, limit))
	_ = body.Close()
}

func statusError(stage string, resp *http.Response, limit int64) error {
	raw, _ := readLimited(resp.Body, min(limit, 4096))
	return &HTTPStatusError{Stage: stage, StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(raw))}
}

func isEventStream(resp *http.Response) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type"))), "text/event-stream")
}

// decodeIncoming parses one JSON-RPC message, or a batch, and returns every
// message in it.
func decodeIncoming(raw []byte) ([]incoming, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '[' {
		var batch []incoming
		if err := json.Unmarshal(raw, &batch); err != nil {
			return nil, err
		}
		return batch, nil
	}
	var single incoming
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, err
	}
	return []incoming{single}, nil
}

func rpcErrorFor(stage string, response incoming) error {
	if response.Error == nil {
		return nil
	}
	return &RPCError{Stage: stage, Code: response.Error.Code, Message: response.Error.Message}
}
