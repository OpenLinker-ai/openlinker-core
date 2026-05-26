package endpointurl

import "testing"

func TestValidate(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		allowLocal bool
		wantErr    bool
	}{
		{name: "https is always accepted", url: "https://agent.example.com/invoke"},
		{name: "local http is opt in", url: "http://127.0.0.1:9191/invoke", allowLocal: true},
		{name: "localhost is opt in", url: "http://localhost:9191/invoke", allowLocal: true},
		{name: "ipv6 loopback is opt in", url: "http://[::1]:9191/invoke", allowLocal: true},
		{name: "local http disabled by default", url: "http://127.0.0.1:9191/invoke", wantErr: true},
		{name: "remote http remains forbidden", url: "http://agent.example.com/invoke", allowLocal: true, wantErr: true},
		{name: "lookalike hostname remains forbidden", url: "http://localhost.example.com/invoke", allowLocal: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.url, tt.allowLocal)
			if tt.wantErr && err == nil {
				t.Fatal("expected endpoint validation to fail")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected endpoint validation to pass: %v", err)
			}
		})
	}
}
