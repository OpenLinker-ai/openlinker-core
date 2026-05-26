package endpointurl

import (
	"errors"
	"net"
	"net/url"
	"strings"
)

var errHTTPSRequired = errors.New("endpoint_url 必须使用 HTTPS；本地开发仅可在开启 ALLOW_LOCAL_HTTP_ENDPOINTS 后使用 loopback HTTP")

// Validate applies the outbound Agent endpoint policy shared by registration and invocation.
// Plain HTTP is never accepted unless it targets the local machine and is explicitly enabled.
func Validate(raw string, allowLocalHTTP bool) error {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return errHTTPSRequired
	}
	if strings.EqualFold(parsed.Scheme, "https") {
		return nil
	}
	if !allowLocalHTTP || !strings.EqualFold(parsed.Scheme, "http") || !isLoopback(parsed.Hostname()) {
		return errHTTPSRequired
	}
	return nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
