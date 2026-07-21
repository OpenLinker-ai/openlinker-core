package config

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// DeriveRuntimePublicOrigin keeps the default deployment zero-touch: it uses
// the public API hostname and the dedicated Runtime listener port, while the
// Runtime listener itself always speaks HTTPS.
func DeriveRuntimePublicOrigin(apiOrigin string, port int) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(apiOrigin))
	if err != nil || parsed.Hostname() == "" || port < 1 || port > 65535 {
		return "", fmt.Errorf("RUNTIME_MTLS_API_URL is required when API_URL cannot be used to derive it")
	}
	parsed.Scheme = "https"
	parsed.Host = net.JoinHostPort(parsed.Hostname(), strconv.Itoa(port))
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.User = nil
	return NormalizeRuntimePublicOrigin(parsed.String())
}

// NormalizeRuntimePublicOrigin validates the public HTTPS origin that Core
// publishes for its dedicated mTLS Runtime listener. Keeping this validator in
// config makes startup validation and discovery serialization share exactly
// the same fail-closed boundary.
func NormalizeRuntimePublicOrigin(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	parsed, err := url.Parse(value)
	if err != nil || parsed.Opaque != "" || parsed.Host == "" || parsed.Hostname() == "" {
		return "", fmt.Errorf("Runtime public URL must be an absolute HTTPS origin")
	}
	if parsed.Scheme != "https" {
		return "", fmt.Errorf("Runtime public URL must be an absolute HTTPS origin")
	}
	if parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" ||
		parsed.RawFragment != "" || strings.Contains(value, "#") {
		return "", fmt.Errorf("Runtime public URL must not include credentials, a path, query, or fragment")
	}
	if strings.HasSuffix(parsed.Host, ":") {
		return "", fmt.Errorf("Runtime public URL has an invalid port")
	}
	if portText := parsed.Port(); portText != "" {
		port, err := strconv.Atoi(portText)
		if err != nil || port < 1 || port > 65535 {
			return "", fmt.Errorf("Runtime public URL has an invalid port")
		}
	}
	return parsed.String(), nil
}
