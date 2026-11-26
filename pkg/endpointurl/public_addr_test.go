package endpointurl

import (
	"net/netip"
	"testing"
)

func TestIsPublicAddr(t *testing.T) {
	for _, s := range []string{"127.0.0.1", "10.1.2.3", "169.254.169.254", "192.168.1.1", "::1", "fd00::1", "224.0.0.1", "0.0.0.0", "::ffff:10.0.0.1"} {
		if IsPublicAddr(netip.MustParseAddr(s)) {
			t.Fatalf("%s accepted as public", s)
		}
	}
	for _, s := range []string{"93.184.216.34", "2606:4700::1111"} {
		if !IsPublicAddr(netip.MustParseAddr(s)) {
			t.Fatalf("%s rejected", s)
		}
	}
}
