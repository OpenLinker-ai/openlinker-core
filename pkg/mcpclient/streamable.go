package mcpclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
)

// streamableConn is one Streamable HTTP session: every message is a POST to
// the endpoint, a request's response is either one JSON body or an SSE stream
// that carries it, and Mcp-Session-Id ties the messages together.
type streamableConn struct {
	cfg Config

	mu              sync.Mutex
	sessionID       string
	protocolVersion string
}

func newStreamableConn(cfg Config) *streamableConn {
	return &streamableConn{cfg: cfg, protocolVersion: StreamableHTTPProtocolVersion}
}

func (c *streamableConn) setProtocolVersion(version string) {
	c.mu.Lock()
	c.protocolVersion = version
	c.mu.Unlock()
}

func (c *streamableConn) headers(req *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(HeaderProtocolVersion, c.protocolVersion)
	if c.sessionID != "" {
		req.Header.Set(HeaderSessionID, c.sessionID)
	}
}

func (c *streamableConn) post(ctx context.Context, body any) (*http.Response, error) {
	req, err := newJSONPost(ctx, c.cfg, c.cfg.EndpointURL, body)
	if err != nil {
		return nil, err
	}
	c.headers(req)
	return c.cfg.HTTPClient.Do(req)
}

func (c *streamableConn) request(ctx context.Context, stage, id, method string, params any) (incoming, error) {
	resp, err := c.post(ctx, message{JSONRPC: jsonRPCVersion, ID: rawID(id), Method: method, Params: params})
	if err != nil {
		return incoming{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer drain(resp.Body, c.cfg.Limits.MaxEventBytes)
		return incoming{}, statusError(stage, resp)
	}
	// Close as soon as the response is read: a server may keep an SSE
	// response open after the matching result, and draining it would hold
	// the call until the stream ends or the session times out.
	defer resp.Body.Close()
	if method == "initialize" {
		if session := resp.Header.Get(HeaderSessionID); session != "" {
			c.mu.Lock()
			c.sessionID = session
			c.mu.Unlock()
		}
	}
	var response incoming
	if isEventStream(resp) {
		response, err = c.readStream(ctx, stage, id, resp.Body)
	} else {
		response, err = c.readJSON(stage, id, resp.Body)
	}
	if err != nil {
		return incoming{}, err
	}
	return response, rpcErrorFor(stage, response)
}

func (c *streamableConn) readJSON(stage, id string, body io.Reader) (incoming, error) {
	raw, err := readLimited(body, c.cfg.Limits.MaxEventBytes)
	if err != nil {
		if errors.Is(err, errEventTooLarge) {
			return incoming{}, &ProtocolError{Stage: stage, Message: "response exceeds the size limit"}
		}
		return incoming{}, err
	}
	messages, err := decodeIncoming(raw)
	if err != nil {
		return incoming{}, &ProtocolError{Stage: stage, Message: "response is not JSON-RPC"}
	}
	for _, m := range messages {
		if m.answers(id) {
			return m, nil
		}
	}
	return incoming{}, &ProtocolError{Stage: stage, Message: "response does not answer the request"}
}

// readStream reads SSE events until the response for id. Server requests on
// the stream are answered with a POST; notifications and responses to other
// ids are ignored.
func (c *streamableConn) readStream(ctx context.Context, stage, id string, body io.Reader) (incoming, error) {
	events := newSSEReader(body, c.cfg.Limits.MaxEventBytes, c.cfg.Limits.MaxStreamBytes)
	for {
		event, err := events.next()
		if err != nil {
			if ctx.Err() != nil {
				return incoming{}, ctx.Err()
			}
			return incoming{}, protocolErrorFromStream(stage, err)
		}
		if event.Event != "message" {
			continue
		}
		messages, err := decodeIncoming(event.Data)
		if err != nil {
			return incoming{}, &ProtocolError{Stage: stage, Message: "event is not JSON-RPC"}
		}
		for _, m := range messages {
			switch {
			case m.answers(id):
				return m, nil
			case m.isRequest():
				if err := c.send(ctx, replyToServerRequest(m)); err != nil && ctx.Err() != nil {
					return incoming{}, ctx.Err()
				}
			}
		}
	}
}

// send posts a notification or a response, which the server acknowledges
// with 202 Accepted.
func (c *streamableConn) send(ctx context.Context, body message) error {
	resp, err := c.post(ctx, body)
	if err != nil {
		return err
	}
	defer drain(resp.Body, c.cfg.Limits.MaxEventBytes)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return statusError(body.Method, resp)
	}
	return nil
}

func (c *streamableConn) notify(ctx context.Context, method string, params any) error {
	return c.send(ctx, message{JSONRPC: jsonRPCVersion, Method: method, Params: params})
}

// close asks the server to release the session. Servers that do not allow
// client termination answer 405, which is fine.
func (c *streamableConn) close(ctx context.Context) {
	c.mu.Lock()
	session := c.sessionID
	c.mu.Unlock()
	if session == "" {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.cfg.EndpointURL, nil)
	if err != nil {
		return
	}
	copyHeader(req.Header, c.cfg.Header)
	c.headers(req)
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return
	}
	drain(resp.Body, 4096)
}
