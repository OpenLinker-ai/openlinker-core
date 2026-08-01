package coreapi

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/a2a"
	"github.com/OpenLinker-ai/openlinker-core/pkg/agent"
	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
)

func TestStartA2AGRPCServerRejectsMissingUserStatusBeforeListening(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		checker auth.UserStatusChecker
	}{
		{name: "nil"},
		{name: "typed-nil", checker: (*auth.DBUserStatusChecker)(nil)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			port := availableTCPPort(t)
			cfg := &config.Config{
				A2AGRPCEnabled: true,
				A2AGRPCPort:    port,
				JWTSecret:      "test-secret",
			}
			services := &Services{
				A2A:         &a2a.Service{},
				AgentMarket: &agent.MarketService{},
				UserStatus:  testCase.checker,
			}

			shutdown, err := StartA2AGRPCServer(context.Background(), cfg, services)
			if err == nil || !strings.Contains(err.Error(), "user status checker is required") {
				t.Fatalf("StartA2AGRPCServer() shutdown=%v err=%v", shutdown, err)
			}
			if shutdown != nil {
				t.Fatal("failed startup returned a shutdown function")
			}

			listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
			if err != nil {
				t.Fatalf("authentication failure leaked a listener on port %d: %v", port, err)
			}
			listener.Close()
		})
	}
}

func availableTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
