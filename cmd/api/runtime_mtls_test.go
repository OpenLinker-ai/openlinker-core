package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/labstack/echo/v4"
)

func TestBuildRuntimeMTLSConfigRequiresVerifiedClientCertificates(t *testing.T) {
	certFile, keyFile := writeRuntimeTestCertificate(t)
	cfg := &config.Config{
		Port:                           8080,
		RuntimeMTLSEnabled:             true,
		RuntimeMTLSPort:                8443,
		RuntimeMTLSMaxConnections:      4096,
		RuntimeMTLSAPIURL:              "https://runtime.example.test:8443",
		RuntimeMTLSCertFile:            certFile,
		RuntimeMTLSKeyFile:             keyFile,
		RuntimeMTLSClientCAFile:        certFile,
		RuntimeInvocationSigningKeyID:  "current",
		RuntimeInvocationSigningSecret: "runtime-test-signing-secret-00000000",
	}
	tlsConfig, err := buildRuntimeMTLSConfig(cfg)
	if err != nil {
		t.Fatalf("buildRuntimeMTLSConfig() error = %v", err)
	}
	if tlsConfig.MinVersion != tls.VersionTLS13 || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.ClientCAs == nil {
		t.Fatalf("unsafe runtime TLS config: %#v", tlsConfig)
	}
}

func TestRuntimeMTLSConfigAndPathFailClosed(t *testing.T) {
	cfg := &config.Config{Port: 8080, RuntimeMTLSEnabled: true, RuntimeMTLSPort: 8080, RuntimeMTLSMaxConnections: 4096}
	if err := validateRuntimeMTLSConfig(cfg); err == nil {
		t.Fatal("same public/runtime port must fail")
	}
	cfg.RuntimeMTLSPort = 8443
	if err := validateRuntimeMTLSConfig(cfg); err == nil {
		t.Fatal("missing certificate paths must fail")
	}
	cfg.RuntimeMTLSCertFile = "server.pem"
	cfg.RuntimeMTLSKeyFile = "server-key.pem"
	cfg.RuntimeMTLSClientCAFile = "client-ca.pem"
	cfg.RuntimeMTLSAPIURL = "http://runtime.example.test:8443"
	if err := validateRuntimeMTLSConfig(cfg); err == nil {
		t.Fatal("non-HTTPS runtime public URL must fail")
	}
	cfg.RuntimeMTLSAPIURL = "https://runtime.example.test:8443"
	cfg.RuntimeInvocationSigningKeyID = "current"
	cfg.RuntimeInvocationSigningSecret = "too-short"
	if err := validateRuntimeMTLSConfig(cfg); err == nil {
		t.Fatal("weak runtime invocation key must fail")
	}

	called := 0
	application := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called++
		w.WriteHeader(http.StatusNoContent)
	})
	handler := runtimeOnlyHandler(application)

	public := httptest.NewRecorder()
	handler.ServeHTTP(public, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if public.Code != http.StatusNotFound || called != 0 {
		t.Fatalf("non-runtime path code=%d called=%v", public.Code, called)
	}
	runtimeRequest := httptest.NewRecorder()
	handler.ServeHTTP(runtimeRequest, httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/sessions", nil))
	if runtimeRequest.Code != http.StatusNoContent || called != 1 {
		t.Fatalf("runtime path code=%d called=%v", runtimeRequest.Code, called)
	}
	e := echo.New()
	e.Use(runtimeListenerIsolation)
	e.POST("/api/v1/agent-runtime/sessions", func(c echo.Context) error {
		called++
		return c.NoContent(http.StatusNoContent)
	})
	e.GET("/healthz", func(c echo.Context) error {
		called++
		return c.NoContent(http.StatusNoContent)
	})
	for _, path := range []string{"/api/v1/agent-runtime/sessions"} {
		recorder := httptest.NewRecorder()
		e.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))
		if recorder.Code != http.StatusNotFound || called != 1 {
			t.Fatalf("public runtime path %s code=%d called=%v", path, recorder.Code, called)
		}
	}
	health := httptest.NewRecorder()
	e.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusNoContent || called != 2 {
		t.Fatalf("public health path code=%d called=%v", health.Code, called)
	}

	dedicated := runtimeOnlyHandler(e)
	runtimeRequest = httptest.NewRecorder()
	dedicated.ServeHTTP(runtimeRequest, httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/sessions", nil))
	if runtimeRequest.Code != http.StatusNoContent || called != 3 {
		t.Fatalf("dedicated runtime path code=%d called=%v", runtimeRequest.Code, called)
	}
}

func TestRuntimeListenerPortsMustNotConflict(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
	}{
		{
			name: "a2a grpc conflicts with api",
			cfg:  config.Config{Port: 8080, A2AGRPCEnabled: true, A2AGRPCPort: 8080},
		},
		{
			name: "runtime mtls conflicts with a2a grpc",
			cfg: config.Config{
				Port: 8080, A2AGRPCEnabled: true, A2AGRPCPort: 9443,
				RuntimeMTLSEnabled: true, RuntimeMTLSPort: 9443,
			},
		},
		{
			name: "invalid a2a grpc port",
			cfg:  config.Config{Port: 8080, A2AGRPCEnabled: true, A2AGRPCPort: 0},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := validateRuntimeMTLSConfig(&testCase.cfg); err == nil {
				t.Fatal("listener conflict must fail before startup")
			}
		})
	}
}

func TestProductionA2AGRPCRequiresExplicitHTTPSPublicOrigin(t *testing.T) {
	cfg := &config.Config{Env: "production", Port: 8080, A2AGRPCEnabled: true, A2AGRPCPort: 9090}
	if err := validateRuntimeMTLSConfig(cfg); err == nil {
		t.Fatal("missing production A2A gRPC public URL succeeded")
	}
	cfg.A2AGRPCPublicURL = "http://twv1.kinzhi.net:8443"
	if err := validateRuntimeMTLSConfig(cfg); err == nil {
		t.Fatal("plaintext production A2A gRPC public URL succeeded")
	}
	cfg.A2AGRPCPublicURL = "https://twv1.kinzhi.net:8443"
	if err := validateRuntimeMTLSConfig(cfg); err != nil {
		t.Fatalf("valid production A2A gRPC public URL failed: %v", err)
	}
}

func TestValidateRuntimeMTLSConfigRejectsNonOriginPublicURL(t *testing.T) {
	cfg := &config.Config{
		Port:                           8080,
		RuntimeMTLSEnabled:             true,
		RuntimeMTLSPort:                8443,
		RuntimeMTLSMaxConnections:      4096,
		RuntimeMTLSCertFile:            "server.pem",
		RuntimeMTLSKeyFile:             "server-key.pem",
		RuntimeMTLSClientCAFile:        "client-ca.pem",
		RuntimeInvocationSigningKeyID:  "current",
		RuntimeInvocationSigningSecret: "runtime-test-signing-secret-00000000",
	}
	for _, rawURL := range []string{
		"http://runtime.example.test:8443",
		"https://:8443",
		"https://user:secret@runtime.example.test:8443",
		"https://runtime.example.test:8443/",
		"https://runtime.example.test:8443/api/v1/agent-runtime",
		"https://runtime.example.test:8443/%2F",
		"https://runtime.example.test:8443?token=secret",
		"https://runtime.example.test:8443?",
		"https://runtime.example.test:8443#runtime",
		"https://runtime.example.test:8443#",
		"https://runtime.example.test:",
		"https://runtime.example.test:0",
		"https://runtime.example.test:65536",
	} {
		t.Run(rawURL, func(t *testing.T) {
			cfg.RuntimeMTLSAPIURL = rawURL
			if err := validateRuntimeMTLSConfig(cfg); err == nil {
				t.Fatalf("validateRuntimeMTLSConfig(%q) succeeded", rawURL)
			}
		})
	}
	cfg.RuntimeMTLSAPIURL = "https://runtime.example.test:8443"
	if err := validateRuntimeMTLSConfig(cfg); err != nil {
		t.Fatalf("valid Runtime origin rejected: %v", err)
	}
}

func TestValidateRuntimeMTLSConfigRejectsInvalidConnectionLimit(t *testing.T) {
	cfg := &config.Config{
		Port:                           8080,
		RuntimeMTLSEnabled:             true,
		RuntimeMTLSPort:                8443,
		RuntimeMTLSAPIURL:              "https://runtime.example.test:8443",
		RuntimeMTLSCertFile:            "server.pem",
		RuntimeMTLSKeyFile:             "server-key.pem",
		RuntimeMTLSClientCAFile:        "client-ca.pem",
		RuntimeInvocationSigningKeyID:  "current",
		RuntimeInvocationSigningSecret: "runtime-test-signing-secret-00000000",
	}
	for _, value := range []int{-1, 0, 65536} {
		cfg.RuntimeMTLSMaxConnections = value
		if err := validateRuntimeMTLSConfig(cfg); err == nil {
			t.Fatalf("RUNTIME_MTLS_MAX_CONNECTIONS=%d succeeded", value)
		}
	}
	for _, value := range []int{1, 4096, 65535} {
		cfg.RuntimeMTLSMaxConnections = value
		if err := validateRuntimeMTLSConfig(cfg); err != nil {
			t.Fatalf("RUNTIME_MTLS_MAX_CONNECTIONS=%d rejected: %v", value, err)
		}
	}
}

func TestRuntimeConnectionLimitListenerRejectsExcessAndReleases(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var limitWarnings atomic.Int32
	listener := newRuntimeConnectionLimitListener(raw, 1, func() {
		limitWarnings.Add(1)
	})
	t.Cleanup(func() { _ = listener.Close() })

	firstAccept := acceptRuntimeTestConnection(listener)
	firstClient := dialRuntimeTestConnection(t, listener.Addr().String())
	firstServer := awaitRuntimeTestAccept(t, firstAccept)

	replacementAccept := acceptRuntimeTestConnection(listener)
	for range 2 {
		excess := dialRuntimeTestConnection(t, listener.Addr().String())
		assertRuntimeTestPeerClosed(t, excess)
	}
	if got := limitWarnings.Load(); got != 1 {
		t.Fatalf("connection-limit warnings = %d, want 1", got)
	}

	if err := firstServer.Close(); err != nil {
		t.Fatal(err)
	}
	_ = firstClient.Close()
	replacementClient := dialRuntimeTestConnection(t, listener.Addr().String())
	replacementServer := awaitRuntimeTestAccept(t, replacementAccept)

	closedTwice := make(chan struct{})
	go func() {
		_ = replacementServer.Close()
		_ = replacementServer.Close()
		close(closedTwice)
	}()
	select {
	case <-closedTwice:
	case <-time.After(2 * time.Second):
		t.Fatal("closing an admitted connection twice blocked while releasing its permit")
	}
	_ = replacementClient.Close()

	finalAccept := acceptRuntimeTestConnection(listener)
	finalClient := dialRuntimeTestConnection(t, listener.Addr().String())
	finalServer := awaitRuntimeTestAccept(t, finalAccept)
	_ = finalServer.Close()
	_ = finalClient.Close()
}

func TestRuntimeConnectionLimitListenerCloseUnblocksAccept(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := newRuntimeConnectionLimitListener(raw, 1, nil)

	firstAccept := acceptRuntimeTestConnection(listener)
	firstClient := dialRuntimeTestConnection(t, listener.Addr().String())
	firstServer := awaitRuntimeTestAccept(t, firstAccept)
	blockedAccept := acceptRuntimeTestConnection(listener)

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case accepted := <-blockedAccept:
		if accepted.err == nil {
			_ = accepted.connection.Close()
			t.Fatal("Accept succeeded after the listener was closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listener Close did not unblock Accept")
	}
	_ = firstServer.Close()
	_ = firstClient.Close()
}

type runtimeTestAcceptResult struct {
	connection net.Conn
	err        error
}

func acceptRuntimeTestConnection(listener net.Listener) <-chan runtimeTestAcceptResult {
	result := make(chan runtimeTestAcceptResult, 1)
	go func() {
		connection, err := listener.Accept()
		result <- runtimeTestAcceptResult{connection: connection, err: err}
	}()
	return result
}

func awaitRuntimeTestAccept(t *testing.T, result <-chan runtimeTestAcceptResult) net.Conn {
	t.Helper()
	select {
	case accepted := <-result:
		if accepted.err != nil {
			t.Fatalf("Accept returned error: %v", accepted.err)
		}
		if accepted.connection == nil {
			t.Fatal("Accept returned a nil connection")
		}
		return accepted.connection
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return")
		return nil
	}
}

func dialRuntimeTestConnection(t *testing.T, address string) net.Conn {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	return connection
}

func assertRuntimeTestPeerClosed(t *testing.T, connection net.Conn) {
	t.Helper()
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Read(make([]byte, 1)); err == nil {
		t.Fatal("excess connection remained open")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("excess connection was not closed before the deadline")
	}
}

func writeRuntimeTestCertificate(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "runtime-test-ca"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "runtime-cert.pem")
	keyFile := filepath.Join(dir, "runtime-key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
