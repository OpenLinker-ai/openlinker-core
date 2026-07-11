package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
)

func TestBuildRuntimeMTLSConfigRequiresVerifiedClientCertificates(t *testing.T) {
	certFile, keyFile := writeRuntimeTestCertificate(t)
	cfg := &config.Config{
		Port:                           8080,
		RuntimeMTLSEnabled:             true,
		RuntimeMTLSPort:                8443,
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
	cfg := &config.Config{Port: 8080, RuntimeMTLSEnabled: true, RuntimeMTLSPort: 8080}
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
	cfg.RuntimeInvocationSigningKeyID = "current"
	cfg.RuntimeInvocationSigningSecret = "too-short"
	if err := validateRuntimeMTLSConfig(cfg); err == nil {
		t.Fatal("weak runtime invocation key must fail")
	}

	called := false
	handler := runtimeV2OnlyHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	public := httptest.NewRecorder()
	handler.ServeHTTP(public, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if public.Code != http.StatusNotFound || called {
		t.Fatalf("non-runtime path code=%d called=%v", public.Code, called)
	}
	runtimeRequest := httptest.NewRecorder()
	handler.ServeHTTP(runtimeRequest, httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/v2/sessions", nil))
	if runtimeRequest.Code != http.StatusNoContent || !called {
		t.Fatalf("runtime path code=%d called=%v", runtimeRequest.Code, called)
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
