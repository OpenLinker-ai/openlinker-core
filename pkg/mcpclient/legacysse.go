package mcpclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
)

// legacySSEConn is one 2024-11-05 HTTP+SSE session: a long-lived GET event
// stream carries every server message, and client messages are POSTed to the
// message URL announced by the first endpoint event.
type legacySSEConn struct {
	cfg        Config
	messageURL string
	cancel     context.CancelFunc

	mu      sync.Mutex
	pending map[string]chan incoming
	done    chan struct{}
	readErr error
	replies sync.WaitGroup
	// replyCtx bounds replies to server requests to the stream's lifetime.
	replyCtx context.Context
}

// dialLegacySSE opens the event stream and waits for the endpoint event. The
// message URL must have the endpoint_url origin and pass the outbound policy,
// so a server cannot steer Core, and the credentials, to another host.
func dialLegacySSE(ctx context.Context, cfg Config) (*legacySSEConn, error) {
	base, err := url.Parse(cfg.EndpointURL)
	if err != nil {
		return nil, fmt.Errorf("mcp connect: %w", err)
	}
	// The stream outlives ctx on purpose: after a cancellation Core still
	// sends notifications/cancelled before it closes the stream. Until the
	// endpoint event arrives, ctx ending still aborts the connect.
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopConnectAbort := context.AfterFunc(ctx, cancel)
	defer stopConnectAbort()
	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, cfg.EndpointURL, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	copyHeader(req.Header, cfg.Header)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := cfg.HTTPClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		err := statusError("connect", resp, cfg.Limits.MaxEventBytes)
		drain(resp.Body, cfg.Limits.MaxEventBytes)
		cancel()
		return nil, err
	}
	if !isEventStream(resp) {
		drain(resp.Body, cfg.Limits.MaxEventBytes)
		cancel()
		return nil, &ProtocolError{Stage: "connect", Message: "endpoint did not return an event stream"}
	}

	c := &legacySSEConn{
		cfg:      cfg,
		cancel:   cancel,
		pending:  map[string]chan incoming{},
		done:     make(chan struct{}),
		replyCtx: streamCtx,
	}
	endpoint := make(chan string, 1)
	go c.readLoop(resp, endpoint)

	timer := time.NewTimer(cfg.Limits.EndpointTimeout)
	defer timer.Stop()
	var raw string
	select {
	case raw = <-endpoint:
	case <-c.done:
		c.close(ctx)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, protocolErrorFromStream("connect", c.readErr)
	case <-timer.C:
		c.close(ctx)
		return nil, &ProtocolError{Stage: "connect", Message: "no endpoint event before the timeout"}
	case <-ctx.Done():
		c.close(ctx)
		return nil, ctx.Err()
	}
	target, err := base.Parse(strings.TrimSpace(raw))
	if err != nil || !endpointurl.SameOrigin(base, target) {
		c.close(ctx)
		return nil, fmt.Errorf("%w: message URL must have the endpoint origin", ErrEndpointRejected)
	}
	if err := cfg.validateURL(target.String()); err != nil {
		c.close(ctx)
		return nil, fmt.Errorf("%w: %v", ErrEndpointRejected, err)
	}
	c.messageURL = target.String()
	return c, nil
}

func (c *legacySSEConn) readLoop(resp *http.Response, endpoint chan<- string) {
	defer resp.Body.Close()
	events := newSSEReader(resp.Body, c.cfg.Limits.MaxEventBytes, c.cfg.Limits.MaxStreamBytes)
	sentEndpoint := false
	var err error
	for {
		var event sseEvent
		event, err = events.next()
		if err != nil {
			break
		}
		switch event.Event {
		case "endpoint":
			if !sentEndpoint {
				sentEndpoint = true
				endpoint <- string(event.Data)
			}
		case "message":
			messages, decodeErr := decodeIncoming(event.Data)
			if decodeErr != nil {
				err = errNotJSONRPC
				break
			}
			for _, m := range messages {
				c.dispatch(m)
			}
		}
		if err != nil {
			break
		}
	}
	c.mu.Lock()
	c.readErr = err
	c.mu.Unlock()
	close(c.done)
}

func (c *legacySSEConn) dispatch(m incoming) {
	switch {
	case m.isResponse():
		c.mu.Lock()
		waiter, ok := c.pending[idKey(m.ID)]
		if ok {
			delete(c.pending, idKey(m.ID))
		}
		c.mu.Unlock()
		if ok {
			waiter <- m
		}
	case m.isRequest():
		// Reply off the read loop so a server that writes to the stream
		// while handling the POST cannot stall it.
		c.replies.Add(1)
		go func() {
			defer c.replies.Done()
			_ = c.send(c.replyCtx, replyToServerRequest(m))
		}()
	}
}

func (c *legacySSEConn) setProtocolVersion(string) {}

func (c *legacySSEConn) request(ctx context.Context, stage, id, method string, params any) (incoming, error) {
	waiter := make(chan incoming, 1)
	c.mu.Lock()
	c.pending[id] = waiter
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()
	if err := c.send(ctx, message{JSONRPC: jsonRPCVersion, ID: rawID(id), Method: method, Params: params}); err != nil {
		if ctx.Err() != nil {
			return incoming{}, ctx.Err()
		}
		var status *HTTPStatusError
		if errors.As(err, &status) {
			status.Stage = stage
		}
		return incoming{}, err
	}
	select {
	case response := <-waiter:
		return response, rpcErrorFor(stage, response)
	case <-c.done:
		// A response dispatched just before the stream ended still counts.
		select {
		case response := <-waiter:
			return response, rpcErrorFor(stage, response)
		default:
		}
		if ctx.Err() != nil {
			return incoming{}, ctx.Err()
		}
		c.mu.Lock()
		readErr := c.readErr
		c.mu.Unlock()
		return incoming{}, protocolErrorFromStream(stage, readErr)
	case <-ctx.Done():
		return incoming{}, ctx.Err()
	}
}

// send POSTs one message to the message URL. Results arrive on the stream, so
// the body is ignored; servers answer 202 Accepted (or 200).
func (c *legacySSEConn) send(ctx context.Context, body message) error {
	req, err := newJSONPost(ctx, c.cfg, c.messageURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer drain(resp.Body, c.cfg.Limits.MaxEventBytes)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return statusError(body.Method, resp, c.cfg.Limits.MaxEventBytes)
	}
	return nil
}

func (c *legacySSEConn) notify(ctx context.Context, method string, params any) error {
	if c.messageURL == "" {
		return nil
	}
	return c.send(ctx, message{JSONRPC: jsonRPCVersion, Method: method, Params: params})
}

// close ends the event stream, which is how a 2024-11-05 session ends, and
// waits for in-flight replies to server requests, which the stream context
// bounds.
func (c *legacySSEConn) close(ctx context.Context) {
	c.cancel()
	select {
	case <-c.done:
	case <-ctx.Done():
	}
	c.replies.Wait()
}
