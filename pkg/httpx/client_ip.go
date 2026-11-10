package httpx

import (
	"net"

	"github.com/labstack/echo/v4"
)

// TrustedProxyIPExtractor identifies the client from X-Forwarded-For only when
// the request arrives from one of the explicitly configured proxy peers, and
// takes the nearest address those proxies did not add themselves. Loopback,
// link-local and private networks are not trusted by default: other
// containers on the same network could otherwise choose their own client IP.
// With no trusted proxies the connection peer is the client.
func TrustedProxyIPExtractor(trustedProxies []*net.IPNet) echo.IPExtractor {
	if len(trustedProxies) == 0 {
		return echo.ExtractIPDirect()
	}
	options := []echo.TrustOption{echo.TrustLoopback(false), echo.TrustLinkLocal(false), echo.TrustPrivateNet(false)}
	for _, network := range trustedProxies {
		options = append(options, echo.TrustIPRange(network))
	}
	return echo.ExtractIPFromXFFHeader(options...)
}
