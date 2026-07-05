package coreapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	"github.com/OpenLinker-ai/openlinker-core/pkg/a2a"
	a2apb "github.com/OpenLinker-ai/openlinker-core/pkg/a2a/pb"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
)

type ShutdownFunc func(context.Context) error

func configureA2AGRPCAgentCard(cfg *config.Config, services *Services) {
	if cfg == nil || services == nil || services.AgentMarket == nil || !cfg.A2AGRPCEnabled {
		return
	}
	publicURL := strings.TrimRight(strings.TrimSpace(cfg.A2AGRPCPublicURL), "/")
	if publicURL == "" {
		publicURL = deriveA2AGRPCPublicURL(cfg)
	}
	services.AgentMarket.SetA2AGRPCInterface(publicURL)
}

func StartA2AGRPCServer(rootCtx context.Context, cfg *config.Config, services *Services, opts Options) (ShutdownFunc, error) {
	if cfg == nil || !cfg.A2AGRPCEnabled {
		return nil, nil
	}
	if services == nil || services.A2A == nil || services.AgentMarket == nil {
		return nil, errors.New("a2a grpc services are not initialized")
	}
	addr := ":" + strconv.Itoa(cfg.A2AGRPCPort)
	publicURL := strings.TrimRight(strings.TrimSpace(cfg.A2AGRPCPublicURL), "/")
	if publicURL == "" {
		publicURL = deriveA2AGRPCPublicURL(cfg)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen a2a grpc %s: %w", addr, err)
	}
	server := grpc.NewServer()
	a2apb.RegisterA2AServiceServer(server, a2a.NewGRPCServer(
		services.A2A,
		services.AgentMarket,
		a2a.NewBearerGRPCAuthenticator(cfg.JWTSecret, opts.APIKeyVerifier),
	))

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	log.Info().Int("port", cfg.A2AGRPCPort).Str("public_url", publicURL).Msg("a2a grpc listening")

	var stopOnce sync.Once
	stopGracefully := func() {
		stopOnce.Do(func() {
			server.GracefulStop()
		})
	}
	go func() {
		<-rootCtx.Done()
		stopGracefully()
	}()

	return func(ctx context.Context) error {
		stopped := make(chan struct{})
		go func() {
			stopGracefully()
			close(stopped)
		}()
		select {
		case <-stopped:
			if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				return err
			}
			return nil
		case <-ctx.Done():
			server.Stop()
			return ctx.Err()
		}
	}, nil
}

func deriveA2AGRPCPublicURL(cfg *config.Config) string {
	port := 9090
	if cfg != nil && cfg.A2AGRPCPort > 0 {
		port = cfg.A2AGRPCPort
	}
	if cfg != nil && strings.TrimSpace(cfg.APIURL) != "" {
		if parsed, err := url.Parse(strings.TrimSpace(cfg.APIURL)); err == nil && parsed.Hostname() != "" {
			scheme := parsed.Scheme
			if scheme == "" {
				scheme = "http"
			}
			return (&url.URL{Scheme: scheme, Host: net.JoinHostPort(parsed.Hostname(), strconv.Itoa(port))}).String()
		}
	}
	return (&url.URL{Scheme: "http", Host: net.JoinHostPort("localhost", strconv.Itoa(port))}).String()
}
