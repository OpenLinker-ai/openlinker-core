// Package mcpclient is Core's outbound client for mcp_server Agents that use
// a standard MCP transport: Streamable HTTP, or the 2024-11-05 HTTP+SSE
// transport (legacy_sse). Each call opens its own session, so nothing is
// shared across Runs. The legacy single JSON-RPC POST (legacy_json) stays in
// the runtime package unchanged; this package only owns its names and the
// credential header rules shared by every transport.
package mcpclient

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
)

const (
	// TransportLegacyJSON is one stateless JSON-RPC POST per call: no
	// initialize, no session and no SSE. It is the stored default.
	TransportLegacyJSON = "legacy_json"
	// TransportStreamableHTTP is the MCP Streamable HTTP transport.
	TransportStreamableHTTP = "streamable_http"
	// TransportLegacySSE is the MCP 2024-11-05 HTTP+SSE transport.
	TransportLegacySSE = "legacy_sse"
)

// Protocol versions requested in initialize. The server may answer with
// another version it supports; Core then uses the server's answer.
const (
	StreamableHTTPProtocolVersion = "2025-06-18"
	LegacySSEProtocolVersion      = "2024-11-05"
)

const (
	// HeaderSessionID carries the Streamable HTTP session.
	HeaderSessionID = "Mcp-Session-Id"
	// HeaderProtocolVersion carries the negotiated protocol version.
	HeaderProtocolVersion = "MCP-Protocol-Version"
)

// NormalizeTransport maps the empty value, which rows read without the column
// carry, to the stored default.
func NormalizeTransport(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return TransportLegacyJSON
	}
	return value
}

// ValidTransport reports whether value names a supported transport.
func ValidTransport(value string) bool {
	switch value {
	case TransportLegacyJSON, TransportStreamableHTTP, TransportLegacySSE:
		return true
	default:
		return false
	}
}

const maxAuthHeaderNameLen = 128

// reservedAuthHeaderNames are headers Core or the HTTP stack owns, or that
// would change how the request is routed or framed.
var reservedAuthHeaderNames = map[string]struct{}{
	"host": {}, "cookie": {}, "cookie2": {}, "set-cookie": {},
	"connection": {}, "keep-alive": {}, "te": {}, "trailer": {},
	"transfer-encoding": {}, "upgrade": {}, "expect": {},
	"accept": {}, "user-agent": {}, "referer": {}, "origin": {},
	"via": {}, "forwarded": {}, "max-forwards": {}, "last-event-id": {},
	"http2-settings": {}, "date": {},
}

// reservedAuthHeaderPrefixes cover header families with the same concern.
var reservedAuthHeaderPrefixes = []string{
	"content-", "mcp-", "x-openlinker-", "proxy-", "accept-",
	"x-forwarded-", "sec-", "if-",
}

// ValidateAuthHeaderName checks an owner-chosen credential header name: RFC
// 9110 token syntax, at most 128 bytes, and not a reserved header.
func ValidateAuthHeaderName(name string) error {
	if name == "" || len(name) > maxAuthHeaderNameLen {
		return fmt.Errorf("endpoint_auth_header_name 长度必须在 1..%d 之间", maxAuthHeaderNameLen)
	}
	for i := 0; i < len(name); i++ {
		if !isTokenChar(name[i]) {
			return errors.New("endpoint_auth_header_name 必须是合法的 HTTP 头名称")
		}
	}
	lower := strings.ToLower(name)
	if _, reserved := reservedAuthHeaderNames[lower]; reserved {
		return fmt.Errorf("endpoint_auth_header_name 不能使用保留头 %s", name)
	}
	for _, prefix := range reservedAuthHeaderPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return fmt.Errorf("endpoint_auth_header_name 不能使用保留头 %s", name)
		}
	}
	return nil
}

func isTokenChar(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
}

// CredentialHeader returns the header that carries an endpoint credential.
// Without a header name the existing routing applies: a Bearer value goes in
// Authorization and anything else in X-OpenLinker-Token. ok is false when
// there is no credential.
func CredentialHeader(value string, headerName *string) (name, headerValue string, ok bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", false
	}
	if headerName != nil && strings.TrimSpace(*headerName) != "" {
		return strings.TrimSpace(*headerName), value, true
	}
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return "Authorization", value, true
	}
	return "X-OpenLinker-Token", value, true
}

// Limits bound one session. Zero fields take the defaults below.
type Limits struct {
	// EndpointTimeout bounds the wait for the legacy_sse endpoint event.
	EndpointTimeout time.Duration
	// TotalTimeout bounds the whole session, from connect to result.
	TotalTimeout time.Duration
	// MaxEventBytes bounds one SSE event or one JSON response body.
	MaxEventBytes int64
	// MaxStreamBytes bounds all bytes read from one event stream.
	MaxStreamBytes int64
	// CancelGrace bounds the best-effort notifications/cancelled and session
	// release sent after the caller cancels.
	CancelGrace time.Duration
}

const (
	DefaultEndpointTimeout = 10 * time.Second
	DefaultTotalTimeout    = 60 * time.Second
	DefaultMaxEventBytes   = 4 << 20
	DefaultMaxStreamBytes  = 8 << 20
	DefaultCancelGrace     = 2 * time.Second
)

func (l Limits) withDefaults() Limits {
	if l.EndpointTimeout <= 0 {
		l.EndpointTimeout = DefaultEndpointTimeout
	}
	if l.TotalTimeout <= 0 {
		l.TotalTimeout = DefaultTotalTimeout
	}
	if l.MaxEventBytes <= 0 {
		l.MaxEventBytes = DefaultMaxEventBytes
	}
	if l.MaxStreamBytes <= 0 {
		l.MaxStreamBytes = DefaultMaxStreamBytes
	}
	if l.MaxStreamBytes < l.MaxEventBytes {
		l.MaxStreamBytes = l.MaxEventBytes
	}
	if l.CancelGrace <= 0 {
		l.CancelGrace = DefaultCancelGrace
	}
	return l
}

// Config describes one MCP endpoint.
type Config struct {
	// Transport is TransportStreamableHTTP or TransportLegacySSE.
	Transport string
	// EndpointURL is the Streamable HTTP endpoint, or the legacy_sse event
	// stream URL.
	EndpointURL string
	// Header is sent on every request: credentials and Core's identifying
	// headers. Content-Type, Accept and the MCP headers are set by the client.
	Header http.Header
	// HTTPClient must refuse cross-origin redirects; use NewHTTPClient.
	HTTPClient *http.Client
	// AllowLocalHTTP is the outbound endpoint policy switch used to
	// re-validate the legacy_sse message URL.
	AllowLocalHTTP bool
	// ValidateURL overrides the outbound policy check of the legacy_sse
	// message URL; tests use it. Nil means endpointurl.Validate.
	ValidateURL func(string) error
	Limits      Limits
}

func (c Config) validateURL(raw string) error {
	if c.ValidateURL != nil {
		return c.ValidateURL(raw)
	}
	return endpointurl.Validate(raw, c.AllowLocalHTTP)
}

// NewHTTPClient returns a client for MCP sessions. It reuses base's
// transport, so the outbound dial policy and connection pool stay the same,
// refuses any redirect that leaves the first request's origin (custom
// credential headers and the session id are not on the cross-origin strip
// list), and has no client timeout: an event stream outlives a single
// response, so Limits.TotalTimeout bounds the session through its context.
func NewHTTPClient(base *http.Client, allowLocalHTTP bool) *http.Client {
	var transport http.RoundTripper = http.DefaultTransport
	if base != nil && base.Transport != nil {
		transport = base.Transport
	}
	return &http.Client{
		Transport:     transport,
		CheckRedirect: endpointurl.SameOriginRedirectPolicy(allowLocalHTTP),
	}
}

// HTTPStatusError is a non-success HTTP status from the MCP endpoint.
type HTTPStatusError struct {
	Stage      string
	StatusCode int
	Body       string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("mcp %s: HTTP %d", e.Stage, e.StatusCode)
}

// RPCError is a JSON-RPC error answered by the MCP server.
type RPCError struct {
	Stage   string
	Code    int
	Message string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("mcp %s: JSON-RPC error %d: %s", e.Stage, e.Code, e.Message)
}

// ProtocolError is a response that does not follow the transport: malformed
// JSON-RPC, a missing result, an oversized event or stream, or a stream that
// ended before the result.
type ProtocolError struct {
	Stage   string
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("mcp %s: %s", e.Stage, e.Message)
}

// ErrEndpointRejected reports a legacy_sse endpoint event that pointed to
// another origin or failed the outbound endpoint policy.
var ErrEndpointRejected = errors.New("mcp legacy_sse endpoint event rejected")
