package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const (
	runtimePathPrefix                = "/api/v1/agent-runtime/"
	minimumRuntimeMTLSMaxConnections = 1
	maximumRuntimeMTLSMaxConnections = 65535
)

type runtimeListenerContextKey struct{}

func startRuntimeMTLSListener(cfg *config.Config, application http.Handler, automatic ...*tls.Config) (*http.Server, net.Listener, error) {
	if cfg == nil || !cfg.RuntimeMTLSEnabled {
		return nil, nil, nil
	}
	tlsConfig, err := buildRuntimeMTLSConfig(cfg, automatic...)
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.RuntimeMTLSPort))
	if err != nil {
		return nil, nil, fmt.Errorf("listen runtime mTLS: %w", err)
	}
	listener = newRuntimeConnectionLimitListener(listener, cfg.RuntimeMTLSMaxConnections, func() {
		log.Warn().
			Int("max_connections", cfg.RuntimeMTLSMaxConnections).
			Msg("agent runtime mTLS connection limit reached")
	})
	server := newHTTPServer(cfg.RuntimeMTLSPort)
	server.Handler = runtimeOnlyHandler(application)
	return server, tls.NewListener(listener, tlsConfig), nil
}

type runtimeConnectionLimitListener struct {
	net.Listener
	permits        chan struct{}
	onLimitReached func()
	warnOnce       sync.Once
}

func newRuntimeConnectionLimitListener(listener net.Listener, maxConnections int, onLimitReached func()) net.Listener {
	return &runtimeConnectionLimitListener{
		Listener:       listener,
		permits:        make(chan struct{}, maxConnections),
		onLimitReached: onLimitReached,
	}
}

func (listener *runtimeConnectionLimitListener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case listener.permits <- struct{}{}:
			return &runtimeConnectionLimitConnection{
				Conn:    connection,
				release: listener.release,
			}, nil
		default:
			_ = connection.Close()
			if listener.onLimitReached != nil {
				listener.warnOnce.Do(listener.onLimitReached)
			}
		}
	}
}

func (listener *runtimeConnectionLimitListener) release() {
	<-listener.permits
}

type runtimeConnectionLimitConnection struct {
	net.Conn
	release     func()
	releaseOnce sync.Once
}

func (connection *runtimeConnectionLimitConnection) Close() error {
	err := connection.Conn.Close()
	connection.releaseOnce.Do(connection.release)
	return err
}

func buildRuntimeMTLSConfig(cfg *config.Config, automatic ...*tls.Config) (*tls.Config, error) {
	if err := validateRuntimeMTLSConfig(cfg); err != nil {
		return nil, err
	}
	if len(automatic) > 0 && automatic[0] != nil {
		cloned := automatic[0].Clone()
		cloned.MinVersion = tls.VersionTLS13
		cloned.ClientAuth = tls.RequireAndVerifyClientCert
		cloned.NextProtos = []string{"http/1.1"}
		if cloned.ClientCAs == nil || (len(cloned.Certificates) == 0 && cloned.GetCertificate == nil) {
			return nil, errors.New("automatic runtime mTLS configuration is incomplete")
		}
		return cloned, nil
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
	if cfg.RuntimeMTLSMaxConnections < minimumRuntimeMTLSMaxConnections ||
		cfg.RuntimeMTLSMaxConnections > maximumRuntimeMTLSMaxConnections {
		return fmt.Errorf("RUNTIME_MTLS_MAX_CONNECTIONS must be between 1 and 65535")
	}
	required := map[string]string{"RUNTIME_MTLS_API_URL": cfg.RuntimeMTLSAPIURL}
	if cfg.RuntimePKIMode == "files" {
		required["RUNTIME_MTLS_CERT_FILE"] = cfg.RuntimeMTLSCertFile
		required["RUNTIME_MTLS_KEY_FILE"] = cfg.RuntimeMTLSKeyFile
		required["RUNTIME_MTLS_CLIENT_CA_FILE"] = cfg.RuntimeMTLSClientCAFile
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required when RUNTIME_MTLS_ENABLED=true", name)
		}
	}
	if _, err := config.NormalizeRuntimePublicOrigin(cfg.RuntimeMTLSAPIURL); err != nil {
		return fmt.Errorf("RUNTIME_MTLS_API_URL must be an HTTPS origin without credentials, path, query, or fragment")
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

func runtimeOnlyHandler(application http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if application == nil || !strings.HasPrefix(r.URL.Path, runtimePathPrefix) {
			http.NotFound(w, r)
			return
		}
		ctx := context.WithValue(r.Context(), runtimeListenerContextKey{}, true)
		application.ServeHTTP(w, r.WithContext(ctx))
	})
}

// runtimeListenerIsolation rejects Runtime traffic on the ordinary API
// listener. The marker is written directly into request context by the
// dedicated mTLS listener after TLS client-certificate verification, so it
// cannot be supplied by an external header.
func runtimeListenerIsolation(next echo.HandlerFunc) echo.HandlerFunc {
	return runtimeListenerIsolationForConfig(&config.Config{RuntimeMTLSEnabled: true})(next)
}

func runtimeListenerIsolationForConfig(cfg *config.Config) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			path := c.Request().URL.Path
			isRuntimePath := strings.HasPrefix(path, runtimePathPrefix)
			fromRuntimeListener, _ := c.Request().Context().Value(runtimeListenerContextKey{}).(bool)
			mtlsRequired := cfg == nil || cfg.RuntimeMTLSEnabled
			if mtlsRequired && isRuntimePath && !fromRuntimeListener {
				return echo.ErrNotFound
			}
			return next(c)
		}
	}
}
