package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestCoreShutdownClosesServedListenerAndDrainsHTTPBeforeDetach(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Listener = listener
	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	e.GET("/blocked", func(c echo.Context) error {
		close(entered)
		<-release
		return c.NoContent(http.StatusNoContent)
	})
	srv := newHTTPServer(context.Background(), 0)
	defer srv.Close()
	served := make(chan error, 1)
	go func() { served <- e.StartServer(srv) }()
	address := listener.Addr().String()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	requested := make(chan error, 1)
	go func() {
		res, err := client.Get("http://" + address + "/blocked")
		if err == nil {
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				err = errors.New("in-flight response failed")
			}
		}
		requested <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not reach the served listener")
	}
	plan := newCoreShutdownPlan(srv, true, func(context.Context) error { return nil })
	plan.ShutdownRuntimeController = func(context.Context) error { return nil }
	detached := make(chan struct{})
	plan.DetachSessions = func(context.Context) (int64, error) { close(detached); return 9, nil }
	stopped := make(chan error, 1)
	go func() {
		result, err := runCoreShutdown(plan)
		if err == nil && (!result.DetachCompleted || result.DetachedSessions != 9) {
			err = errors.New("detach not completed")
		}
		stopped <- err
	}()
	// Serve must stop accepting while the existing HTTP handler is still active.
	select {
	case err := <-served:
		require.ErrorIs(t, err, http.ErrServerClosed)
	case <-time.After(3 * time.Second):
		t.Fatal("actual HTTP listener remained open during shutdown")
	}
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if conn != nil {
		_ = conn.Close()
	}
	require.Error(t, err, "new requests must not reach an old Core after its listener closes")
	select {
	case <-detached:
		t.Fatal("Session detach raced an admitted HTTP request")
	default:
	}
	close(release)
	require.NoError(t, <-requested)
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not drain")
	}
}

func TestCoreShutdownCancelsAdmittedLongPoll(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := newHTTPServer(ctx, 0)
	defer srv.Close()
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Listener = listener
	entered := make(chan struct{})
	finished := make(chan struct{})
	e.GET("/long-poll", func(c echo.Context) error {
		close(entered)
		select {
		case <-c.Request().Context().Done():
			close(finished)
			return c.NoContent(http.StatusServiceUnavailable)
		case <-time.After(30 * time.Second):
			return c.NoContent(http.StatusNoContent)
		}
	})
	served := make(chan error, 1)
	go func() { served <- e.StartServer(srv) }()
	requested := make(chan error, 1)
	go func() {
		client := http.Client{Timeout: 5 * time.Second}
		res, err := client.Get("http://" + listener.Addr().String() + "/long-poll")
		if err == nil {
			_ = res.Body.Close()
			if res.StatusCode != http.StatusServiceUnavailable {
				err = errors.New("long poll ignored shutdown")
			}
		}
		requested <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not start")
	}
	// main cancels the shared process context before running the shutdown plan.
	cancel()
	plan := newCoreShutdownPlan(srv, true, func(context.Context) error { return nil })
	plan.PhaseTimeout = time.Second
	plan.ShutdownRuntimeController = func(context.Context) error { return nil }
	plan.DetachSessions = func(context.Context) (int64, error) {
		select {
		case <-finished:
			return 1, nil
		default:
			return 0, errors.New("long poll still active during detach")
		}
	}
	result, err := runCoreShutdown(plan)
	require.NoError(t, err)
	require.True(t, result.DetachCompleted)
	require.NoError(t, <-requested)
	require.ErrorIs(t, <-served, http.ErrServerClosed)
}

func TestRuntimeMTLSListenerInheritsShutdownCancellation(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := reserved.Addr().(*net.TCPAddr).Port
	require.NoError(t, reserved.Close())
	certFile, keyFile := writeRuntimeTestCertificate(t)
	cfg := &config.Config{Port: 0, RuntimeMTLSEnabled: true, RuntimeMTLSPort: port, RuntimeMTLSMaxConnections: 4, RuntimeMTLSAPIURL: fmt.Sprintf("https://localhost:%d", port), RuntimeMTLSCertFile: certFile, RuntimeMTLSKeyFile: keyFile, RuntimeMTLSClientCAFile: certFile, RuntimeInvocationSigningKeyID: "current", RuntimeInvocationSigningSecret: "runtime-test-signing-secret-00000000"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	application := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		close(entered)
		<-r.Context().Done()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv, listener, err := startRuntimeMTLSListener(ctx, cfg, application)
	require.NoError(t, err)
	defer listener.Close()
	defer srv.Close()
	served := make(chan error, 1)
	go func() { served <- srv.Serve(listener) }()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	pem, err := os.ReadFile(certFile)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(pem))
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: roots, Certificates: []tls.Certificate{cert}}}
	defer transport.CloseIdleConnections()
	client := http.Client{Transport: transport, Timeout: 5 * time.Second}
	requested := make(chan error, 1)
	go func() {
		res, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/api/v1/agent-runtime/commands", port))
		if err == nil {
			_ = res.Body.Close()
			if res.StatusCode != http.StatusServiceUnavailable {
				err = errors.New("mTLS request ignored shutdown")
			}
		}
		requested <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("verified mTLS request did not start")
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	require.NoError(t, srv.Shutdown(shutdownCtx))
	require.NoError(t, <-requested)
	require.ErrorIs(t, <-served, http.ErrServerClosed)
}
