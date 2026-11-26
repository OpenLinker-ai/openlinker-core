package catalogregistry

import (
	"net"
	"net/http"
	"testing"
)

func TestTransportIgnoresEnvironmentProxies(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://proxy.invalid:3128")
	t.Setenv("HTTP_PROXY", "http://proxy.invalid:3128")
	c := newHTTPClient(Registry{}, net.DefaultResolver)
	if c.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("registry requests must not use environment proxies")
	}
}
