package endpointurl

import (
	"context"
	"net"
	"net/http"
	"testing"
)

func TestValidate(t *testing.T) {
	resolver := staticResolver{
		"public.openlinker.dev":  []string{"93.184.216.34"},
		"private.openlinker.dev": []string{"10.0.0.5"},
		"mixed.openlinker.dev":   []string{"93.184.216.34", "10.0.0.5"},
	}
	tests := []struct {
		name       string
		url        string
		allowLocal bool
		wantErr    bool
	}{
		{name: "public https is accepted", url: "https://public.openlinker.dev/invoke"},
		{name: "https scheme is case insensitive", url: "HTTPS://public.openlinker.dev/invoke"},
		{name: "local http is opt in", url: "http://127.0.0.1:9191/invoke", allowLocal: true},
		{name: "localhost is opt in", url: "http://localhost:9191/invoke", allowLocal: true},
		{name: "ipv6 loopback is opt in", url: "http://[::1]:9191/invoke", allowLocal: true},
		{name: "local https is opt in", url: "https://127.0.0.1:9191/invoke", allowLocal: true},
		{name: "local http disabled by default", url: "http://127.0.0.1:9191/invoke", wantErr: true},
		{name: "local https disabled by default", url: "https://127.0.0.1:9191/invoke", wantErr: true},
		{name: "remote http remains forbidden", url: "http://agent.example.com/invoke", allowLocal: true, wantErr: true},
		{name: "lookalike hostname remains forbidden", url: "http://localhost.example.com/invoke", allowLocal: true, wantErr: true},
		{name: "private ip literal is forbidden", url: "https://10.0.0.5/invoke", wantErr: true},
		{name: "metadata ip literal is forbidden", url: "https://169.254.169.254/latest/meta-data", wantErr: true},
		{name: "private dns result is forbidden", url: "https://private.openlinker.dev/invoke", wantErr: true},
		{name: "mixed public private dns result is forbidden", url: "https://mixed.openlinker.dev/invoke", wantErr: true},
		{name: "userinfo is forbidden", url: "https://user:pass@agent.example.com/invoke", wantErr: true},
		{name: "empty url is forbidden", url: " ", wantErr: true},
		{name: "relative url is forbidden", url: "/invoke", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateWithResolver(context.Background(), tt.url, tt.allowLocal, resolver)
			if tt.wantErr && err == nil {
				t.Fatal("expected endpoint validation to fail")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected endpoint validation to pass: %v", err)
			}
		})
	}
}

func TestRedirectPolicyRevalidatesRedirectTarget(t *testing.T) {
	policy := RedirectPolicy(false)
	req, err := http.NewRequest(http.MethodGet, "https://10.0.0.5/internal", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := policy(req, nil); err == nil {
		t.Fatal("expected redirect to private address to fail")
	}
}

type staticResolver map[string][]string

func (s staticResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	raw, ok := s[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host}
	}
	out := make([]net.IPAddr, 0, len(raw))
	for _, value := range raw {
		out = append(out, net.IPAddr{IP: net.ParseIP(value)})
	}
	return out, nil
}
