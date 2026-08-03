package browserpolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
)

const (
	ContractID = "openlinker.browser.v2"
	Restricted = "restricted"
	Full       = "full"
	MaxOrigins = 32
)

var ErrInvalid = errors.New("browser interaction policy is invalid")

// Canonicalize returns the exact sorted origin list and the lowercase SHA-256
// of its compact JSON encoding. It deliberately does not resolve DNS; public
// address and rebinding enforcement remains the Egress Gateway's boundary.
func Canonicalize(policy string, origins []string) ([]string, string, error) {
	if origins == nil || len(origins) > MaxOrigins {
		return nil, "", ErrInvalid
	}
	canonical := make([]string, 0, len(origins))
	seen := make(map[string]struct{}, len(origins))
	for _, raw := range origins {
		origin, err := canonicalOrigin(raw)
		if err != nil {
			return nil, "", err
		}
		if _, exists := seen[origin]; exists {
			continue
		}
		seen[origin] = struct{}{}
		canonical = append(canonical, origin)
	}
	sort.Strings(canonical)
	switch policy {
	case Restricted:
		if len(canonical) != 0 {
			return nil, "", ErrInvalid
		}
	case Full:
		if len(canonical) == 0 {
			return nil, "", ErrInvalid
		}
	default:
		return nil, "", ErrInvalid
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", ErrInvalid
	}
	digest := sha256.Sum256(encoded)
	return canonical, hex.EncodeToString(digest[:]), nil
}

// ValidateCanonical rejects semantically equivalent but non-canonical wire
// lists. This keeps the stored list, authority envelope and lease digest
// byte-stable instead of accepting multiple encodings of the same scope.
func ValidateCanonical(policy string, origins []string) (string, error) {
	canonical, digest, err := Canonicalize(policy, origins)
	if err != nil || len(canonical) != len(origins) {
		return "", ErrInvalid
	}
	for index := range canonical {
		if canonical[index] != origins[index] {
			return "", ErrInvalid
		}
	}
	return digest, nil
}

func canonicalOrigin(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.Contains(raw, "%") {
		return "", ErrInvalid
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil ||
		parsed.Opaque != "" || parsed.Host == "" || parsed.Path != "" ||
		parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		parsed.ForceQuery {
		return "", ErrInvalid
	}
	host := parsed.Hostname()
	if host == "" || strings.HasSuffix(host, ".") {
		return "", ErrInvalid
	}
	port := parsed.Port()
	if port != "" {
		portNumber, parseErr := strconv.ParseUint(port, 10, 16)
		if parseErr != nil || portNumber == 0 {
			return "", ErrInvalid
		}
		if portNumber == 443 {
			port = ""
		} else {
			port = strconv.FormatUint(portNumber, 10)
		}
	}

	canonicalHost := ""
	if address, parseErr := netip.ParseAddr(host); parseErr == nil {
		if address.Is4In6() {
			return "", ErrInvalid
		}
		if address.Is6() {
			canonicalHost = "[" + address.String() + "]"
		} else {
			canonicalHost = address.String()
		}
	} else {
		ascii, idnaErr := idna.Lookup.ToASCII(strings.ToLower(host))
		if idnaErr != nil || !validDomain(ascii) || ambiguousIPv4Domain(ascii) {
			return "", ErrInvalid
		}
		canonicalHost = strings.ToLower(ascii)
	}
	if port != "" {
		canonicalHost += ":" + port
	}
	return "https://" + canonicalHost, nil
}

// ambiguousIPv4Domain rejects legacy WHATWG IPv4 spellings before the Browser
// can reinterpret a value that Core otherwise treated as a DNS name. Examples
// include 127.1, a single integer, and hexadecimal or octal-looking parts.
func ambiguousIPv4Domain(host string) bool {
	parts := strings.Split(strings.ToLower(host), ".")
	if len(parts) == 0 || len(parts) > 4 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		digits := part
		base := byte('0')
		if strings.HasPrefix(part, "0x") {
			digits = part[2:]
			base = 'a'
		}
		if digits == "" {
			return false
		}
		for index := 0; index < len(digits); index++ {
			character := digits[index]
			if character >= '0' && character <= '9' {
				continue
			}
			if base == 'a' && character >= 'a' && character <= 'f' {
				continue
			}
			return false
		}
	}
	return true
}

func validDomain(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character >= 'a' && character <= 'z') ||
				(character >= '0' && character <= '9') || character == '-' {
				continue
			}
			return false
		}
	}
	return true
}
