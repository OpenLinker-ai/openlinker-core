package endpointurl

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/net/idna"
)

var errHTTPSRequired = errors.New("endpoint_url 必须使用 HTTPS；本地开发仅可在开启 ALLOW_LOCAL_HTTP_ENDPOINTS 后使用 loopback HTTP")
var errPublicEndpointRequired = errors.New("endpoint_url 必须解析到公网地址；loopback、内网、link-local、metadata 地址仅允许本地开发显式放行")

var blockedPublicEndpointPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// Validate applies the outbound Agent endpoint policy shared by registration and invocation.
// Plain HTTP is never accepted unless it targets the local machine and is explicitly enabled.
func Validate(raw string, allowLocalHTTP bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return ValidateWithResolver(ctx, raw, allowLocalHTTP, net.DefaultResolver)
}

func ValidateWithResolver(ctx context.Context, raw string, allowLocalHTTP bool, resolver Resolver) error {
	parsed, err := parseEndpointURL(raw)
	if err != nil {
		return err
	}
	switch {
	case strings.EqualFold(parsed.Scheme, "https"):
		if allowLocalHTTP && isLoopbackHost(ctx, parsed.Hostname(), resolver, true) {
			return nil
		}
		return validatePublicHost(ctx, parsed.Hostname(), resolver, true)
	case strings.EqualFold(parsed.Scheme, "http"):
		if allowLocalHTTP && isLoopbackHost(ctx, parsed.Hostname(), resolver, true) {
			return nil
		}
		return errHTTPSRequired
	default:
		return errHTTPSRequired
	}
}

func NewHTTPClient(timeout time.Duration, allowLocalHTTP bool) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		addrs, err := resolveHost(ctx, host, net.DefaultResolver, false)
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			if err := validatePublicAddr(addr, allowLocalHTTP && addr.IsLoopback()); err != nil {
				return nil, err
			}
		}
		var lastErr error
		for _, addr := range addrs {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(addr.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, errPublicEndpointRequired
	}
	return &http.Client{
		Timeout:       timeout,
		Transport:     transport,
		CheckRedirect: RedirectPolicy(allowLocalHTTP),
	}
}

// maxRedirects matches net/http's default redirect limit.
const maxRedirects = 10

// crossOriginCredentialHeaders carry credentials (or, for Referer, the
// previous URL, whose query string may hold a key) that must not follow a
// redirect to another origin. net/http drops Authorization, Proxy-Authorization
// and Cookie only when the host leaves the original domain and its subdomains
// (a port or scheme change keeps them), and never drops custom headers such
// as X-OpenLinker-Token.
var crossOriginCredentialHeaders = []string{
	"X-OpenLinker-Token",
	"X-OpenLinker-Internal-Token",
	"Authorization",
	"Proxy-Authorization",
	"Cookie",
	"Cookie2",
	"Referer",
}

// RedirectPolicy is the CheckRedirect for outbound Agent, webhook and registry
// calls. Every hop is re-validated against the public-endpoint policy (so an
// https endpoint can never be downgraded to plain HTTP outside opt-in loopback
// development), and credential headers are removed once the chain leaves the
// origin of the first request.
func RedirectPolicy(allowLocalHTTP bool) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return errors.New("stopped after 10 redirects")
		}
		if err := Validate(req.URL.String(), allowLocalHTTP); err != nil {
			return err
		}
		StripCrossOriginCredentials(req, via)
		return nil
	}
}

// ErrCrossOriginRedirect reports a redirect that left the origin of the first
// request under SameOriginRedirectPolicy.
var ErrCrossOriginRedirect = errors.New("endpoint redirected to another origin; the request was not forwarded")

// SameOriginRedirectPolicy is the CheckRedirect for outbound calls that carry
// headers the fixed cross-origin strip list does not know about, such as a
// custom credential header name or an MCP session id. A hop that leaves the
// origin of the first request (scheme, host and effective port) is refused
// instead of stripped; a same-origin hop is still re-validated against the
// public-endpoint policy.
func SameOriginRedirectPolicy(allowLocalHTTP bool) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return errors.New("stopped after 10 redirects")
		}
		if len(via) > 0 && redirectChainCrossesOrigin(req, via) {
			return ErrCrossOriginRedirect
		}
		return Validate(req.URL.String(), allowLocalHTTP)
	}
}

// SameOrigin reports whether a and b have the same RFC 6454 origin as the
// HTTP transport would connect to it. A URL whose origin cannot be normalized
// is never the same origin as anything.
func SameOrigin(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	left, ok := originOf(a)
	if !ok {
		return false
	}
	right, ok := originOf(b)
	return ok && left == right
}

// CredentialRedirectPolicy is the CheckRedirect for operator-configured
// endpoints (for example internal LLM services) that may legitimately live on
// a private network. It keeps net/http's redirect limit and removes credential
// headers once the chain leaves the first request's origin, but does not
// apply the public-endpoint policy. Use RedirectPolicy for any URL supplied
// by a user or an Agent.
func CredentialRedirectPolicy() func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return errors.New("stopped after 10 redirects")
		}
		StripCrossOriginCredentials(req, via)
		return nil
	}
}

// StripCrossOriginCredentials removes credential headers from a redirect hop
// when that hop, or any earlier hop, targets a different origin (scheme, host
// and effective port) than the first request in via. net/http copies the
// first request's headers onto every hop before calling CheckRedirect, so the
// check covers the whole chain: once a chain has crossed origins it stays
// stripped, even if a later hop returns to the original origin, because that
// hop was chosen by the other origin.
func StripCrossOriginCredentials(req *http.Request, via []*http.Request) {
	if req == nil || len(via) == 0 {
		return
	}
	if !redirectChainCrossesOrigin(req, via) {
		return
	}
	for _, name := range crossOriginCredentialHeaders {
		req.Header.Del(name)
	}
}

func redirectChainCrossesOrigin(req *http.Request, via []*http.Request) bool {
	if via[0] == nil || via[0].URL == nil || req.URL == nil {
		return true
	}
	initial, ok := originOf(via[0].URL)
	if !ok {
		return true
	}
	if next, ok := originOf(req.URL); !ok || next != initial {
		return true
	}
	for _, hop := range via[1:] {
		if hop == nil || hop.URL == nil {
			return true
		}
		if previous, ok := originOf(hop.URL); !ok || previous != initial {
			return true
		}
	}
	return false
}

type urlOrigin struct {
	scheme string
	host   string
	// port is the numeric effective port, or -1 for a scheme without a
	// default port and no explicit one.
	port int
}

// originOf returns the RFC 6454 origin tuple as the HTTP transport will
// connect to it: the host goes through the IDNA conversions net/http applies
// before dialing and sending Host, then ASCII case folding, and the port is
// compared as a number with the scheme default filled in. ok is false
// when any part cannot be normalized; callers then treat the URL as another
// origin, which can only strip credentials, never forward them.
func originOf(u *url.URL) (urlOrigin, bool) {
	scheme, ok := asciiLower(u.Scheme)
	if !ok || scheme == "" {
		return urlOrigin{}, false
	}
	host, ok := transportHost(u.Hostname())
	if !ok {
		return urlOrigin{}, false
	}
	port, ok := effectivePort(scheme, u.Port())
	if !ok {
		return urlOrigin{}, false
	}
	return urlOrigin{scheme: scheme, host: host, port: port}, true
}

// transportHost mirrors net/http, which converts a non-ASCII host twice: the
// dial address and TLS name use idna.Lookup.ToASCII, while the Host header
// uses the Punycode profile (idna.ToASCII, via httpguts.PunycodeHostPort). An
// ASCII host is used as written by both. A non-ASCII host is accepted only
// when both conversions agree, so the connection and the virtual host are
// the same; confusable spellings such as "\u0130.example" (Lookup
// "xn--i-9bb.example", Host "xn--bfa.example", while strings.ToLower gives
// "i.example") are treated as another origin.
func transportHost(host string) (string, bool) {
	if host == "" {
		return "", false
	}
	if lowered, ok := asciiLower(host); ok {
		return lowered, true
	}
	dialHost, err := idna.Lookup.ToASCII(host)
	if err != nil {
		return "", false
	}
	headerHost, err := idna.ToASCII(host)
	if err != nil {
		return "", false
	}
	dialHost, dialOK := asciiLower(dialHost)
	headerHost, headerOK := asciiLower(headerHost)
	if !dialOK || !headerOK || dialHost != headerHost {
		return "", false
	}
	return dialHost, true
}

func effectivePort(scheme, port string) (int, bool) {
	if port == "" {
		switch scheme {
		case "https":
			return 443, true
		case "http":
			return 80, true
		default:
			return -1, true
		}
	}
	// net/url accepts only digits here; anything outside 0..65535 is not a
	// port the transport could dial.
	value, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return 0, false
	}
	return int(value), true
}

// asciiLower folds A-Z only and reports false for any non-ASCII byte.
func asciiLower(value string) (string, bool) {
	out := []byte(value)
	for i, c := range out {
		if c >= utf8.RuneSelf {
			return "", false
		}
		if 'A' <= c && c <= 'Z' {
			out[i] = c + ('a' - 'A')
		}
	}
	return string(out), true
}

func parseEndpointURL(raw string) (*url.URL, error) {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return nil, errHTTPSRequired
	}
	return parsed, nil
}

func isLoopbackHost(ctx context.Context, host string, resolver Resolver, allowDocumentationHosts bool) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	addrs, err := resolveHost(ctx, host, resolver, allowDocumentationHosts)
	if err != nil || len(addrs) == 0 {
		return false
	}
	for _, addr := range addrs {
		if !addr.IsLoopback() {
			return false
		}
	}
	return true
}

func validatePublicHost(ctx context.Context, host string, resolver Resolver, allowDocumentationHosts bool) error {
	addrs, err := resolveHost(ctx, host, resolver, allowDocumentationHosts)
	if err != nil || len(addrs) == 0 {
		return errPublicEndpointRequired
	}
	for _, addr := range addrs {
		if err := validatePublicAddr(addr, false); err != nil {
			return err
		}
	}
	return nil
}

func resolveHost(ctx context.Context, host string, resolver Resolver, allowDocumentationHosts bool) ([]netip.Addr, error) {
	host = strings.Trim(strings.TrimSpace(host), ".")
	if host == "" {
		return nil, errPublicEndpointRequired
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{addr.Unmap()}, nil
	}
	if strings.EqualFold(host, "localhost") {
		return []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("::1")}, nil
	}
	if allowDocumentationHosts && isReservedDocumentationHost(host) {
		return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	addrs := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		addr, ok := netip.AddrFromSlice(ip.IP)
		if !ok {
			return nil, errPublicEndpointRequired
		}
		addrs = append(addrs, addr.Unmap())
	}
	return addrs, nil
}

func validatePublicAddr(addr netip.Addr, allowLoopback bool) error {
	addr = addr.Unmap()
	if allowLoopback && addr.IsLoopback() {
		return nil
	}
	for _, prefix := range blockedPublicEndpointPrefixes {
		if prefix.Contains(addr) {
			return errPublicEndpointRequired
		}
	}
	if !addr.IsGlobalUnicast() {
		return errPublicEndpointRequired
	}
	return nil
}

func isReservedDocumentationHost(host string) bool {
	host = strings.ToLower(strings.Trim(host, "."))
	return host == "example.com" ||
		host == "example.net" ||
		host == "example.org" ||
		host == "example" ||
		host == "test" ||
		strings.HasSuffix(host, ".example.com") ||
		strings.HasSuffix(host, ".example.net") ||
		strings.HasSuffix(host, ".example.org") ||
		strings.HasSuffix(host, ".example") ||
		strings.HasSuffix(host, ".test")
}
