package endpointurl

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
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

func RedirectPolicy(allowLocalHTTP bool) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return Validate(req.URL.String(), allowLocalHTTP)
	}
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
