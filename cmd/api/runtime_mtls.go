package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

const runtimeV2PathPrefix = "/api/v1/agent-runtime/v2/"

func startRuntimeMTLSListener(cfg *config.Config, application http.Handler) (*http.Server, net.Listener, error) {
	if cfg == nil || !cfg.RuntimeMTLSEnabled {
		return nil, nil, nil
	}
	tlsConfig, err := buildRuntimeMTLSConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.RuntimeMTLSPort))
	if err != nil {
		return nil, nil, fmt.Errorf("listen runtime mTLS: %w", err)
	}
	server := newHTTPServer(cfg.RuntimeMTLSPort)
	server.Handler = runtimeV2OnlyHandler(application)
	return server, tls.NewListener(listener, tlsConfig), nil
}

func buildRuntimeMTLSConfig(cfg *config.Config) (*tls.Config, error) {
	if err := validateRuntimeMTLSConfig(cfg); err != nil {
		return nil, err
	}
	certificate, err := tls.LoadX509KeyPair(cfg.RuntimeMTLSCertFile, cfg.RuntimeMTLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load runtime mTLS server certificate: %w", err)
	}
	clientCAPEM, err := os.ReadFile(cfg.RuntimeMTLSClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read runtime mTLS client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		return nil, fmt.Errorf("runtime mTLS client CA contains no certificates")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		// This listener is served through http.Server.Serve over an already
		// configured TLS listener. Advertise only the protocol it serves; h2
		// must not be negotiated without an HTTP/2 server configuration.
		NextProtos: []string{"http/1.1"},
	}, nil
}

func validateRuntimeMTLSConfig(cfg *config.Config) error {
	if cfg == nil || !cfg.RuntimeMTLSEnabled {
		return nil
	}
	if cfg.RuntimeMTLSPort < 1 || cfg.RuntimeMTLSPort > 65535 {
		return fmt.Errorf("RUNTIME_MTLS_PORT must be between 1 and 65535")
	}
	if cfg.RuntimeMTLSPort == cfg.Port {
		return fmt.Errorf("RUNTIME_MTLS_PORT must differ from PORT")
	}
	for name, value := range map[string]string{
		"RUNTIME_MTLS_CERT_FILE":      cfg.RuntimeMTLSCertFile,
		"RUNTIME_MTLS_KEY_FILE":       cfg.RuntimeMTLSKeyFile,
		"RUNTIME_MTLS_CLIENT_CA_FILE": cfg.RuntimeMTLSClientCAFile,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required when RUNTIME_MTLS_ENABLED=true", name)
		}
	}
	if _, err := runtime.NewRuntimeInvocationSignerWithPrevious(
		cfg.RuntimeInvocationSigningKeyID,
		cfg.RuntimeInvocationSigningSecret,
		cfg.RuntimeInvocationPreviousSigningKeyID,
		cfg.RuntimeInvocationPreviousSigningSecret,
	); err != nil {
		return fmt.Errorf("RUNTIME_INVOCATION_SIGNING_SECRET is invalid: %w", err)
	}
	return nil
}

func runtimeV2OnlyHandler(application http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if application == nil || !strings.HasPrefix(r.URL.Path, runtimeV2PathPrefix) {
			http.NotFound(w, r)
			return
		}
		application.ServeHTTP(w, r)
	})
}
