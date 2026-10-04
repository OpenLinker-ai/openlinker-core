package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/credential"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRuntimeAuthenticationUnavailableDoesNotBecomeUnauthorized(t *testing.T) {
	for _, stage := range []string{"token", "device", "binding", "token_only_binding"} {
		for _, cause := range []error{NewRuntimeAuthenticationUnavailableError(errors.New("private database detail")), context.Canceled, context.DeadlineExceeded, errors.New("revoked credential")} {
			t.Run(stage+"/"+cause.Error(), func(t *testing.T) {
				fixture := newRuntimeHandlerFixture()
				controller := fixture.controller()
				switch stage {
				case "token":
					fixture.tokens.err = cause
				case "device":
					fixture.devices.err = cause
				case "binding", "token_only_binding":
					controller.dependencies.PrincipalBinder = &runtimePrincipalBinderFake{err: cause}
					controller.dependencies.TokenOnlyTransport = stage == "token_only_binding"
				}
				for _, path := range []string{"/api/v1/agent-runtime/sessions", "/api/v1/agent-runtime/ws"} {
					e := echo.New()
					controller.Register(e.Group("/api/v1"))
					method := http.MethodPost
					if strings.HasSuffix(path, "/ws") {
						method = http.MethodGet
					}
					req := httptest.NewRequest(method, path, strings.NewReader("{}"))
					req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
					req.Header.Set(RuntimeNodeIDHeader, fixture.authenticated.Device.NodeID.String())
					rec := httptest.NewRecorder()
					e.ServeHTTP(rec, req)
					var body RuntimeError
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
					if cause.Error() == "revoked credential" {
						require.Equal(t, http.StatusUnauthorized, rec.Code)
						require.Equal(t, RuntimeErrorUnauthorized, body.Error.Code)
						require.False(t, body.Error.Retryable)
					} else {
						require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
						require.Equal(t, RuntimeErrorServiceUnavailable, body.Error.Code)
						require.True(t, body.Error.Retryable)
					}
					require.NotContains(t, rec.Body.String(), "private database detail")
					require.Zero(t, fixture.sessions.createCalls)
				}
			})
		}
	}
}

func TestRuntimeTokenClosedPoolIsRetryable(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://unused:unused@127.0.0.1:1/unused?sslmode=disable")
	require.NoError(t, err)
	pool.Close()
	token, _, err := credential.GenerateAgentToken()
	require.NoError(t, err)
	fixture := newRuntimeHandlerFixture()
	controller := fixture.controller()
	controller.dependencies.TokenValidator = &Service{queries: db.New(pool)}
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/sessions", strings.NewReader("{}"))
	req.Header.Set(echo.HeaderAuthorization, "Bearer "+token)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	requireRuntimeResponseCode(t, rec, RuntimeErrorServiceUnavailable)
	require.Contains(t, rec.Body.String(), `"retryable":true`)
	require.Zero(t, fixture.devices.calls)
	require.Zero(t, fixture.sessions.createCalls)
}

func TestRuntimeCertificateStoreFailurePreservesRetryabilityThroughMTLS(t *testing.T) {
	now := time.Now()
	leaf := &x509.Certificate{SerialNumber: big.NewInt(0xabc), Raw: []byte("verified-leaf"), RawSubjectPublicKeyInfo: []byte("verified-key"), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
	presented := runtimePresentedCertificate(leaf)
	databaseErr := errors.New("private database detail")
	for _, stage := range []string{"node_query", "database_clock", "missing_node", "revoked_node"} {
		t.Run(stage, func(t *testing.T) {
			queries := &sessionNodeCredentialQueriesFake{node: db.RuntimeNode{NodeID: uuid.New(), DeviceCertificateSerial: presented.Serial, DevicePublicKeyThumbprint: presented.PublicKeyThumbprintSHA256, Status: "active"}}
			clock := &sessionClockFake{now: now}
			switch stage {
			case "node_query":
				queries.err = databaseErr
			case "database_clock":
				clock.err = databaseErr
			case "missing_node":
				queries.err = pgx.ErrNoRows
			case "revoked_node":
				queries.node.Status = "revoked"
			}
			authenticator := NewMTLSRuntimeDeviceAuthenticator(newDBRuntimeNodeCredentialVerifier(queries, clock))
			req := httptest.NewRequest(http.MethodPost, "https://core.test/runtime", nil)
			req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
			_, err := authenticator.AuthenticateHTTP(context.Background(), req)
			require.Error(t, err)
			if stage == "node_query" || stage == "database_clock" {
				var transportErr *RuntimeTransportError
				require.ErrorAs(t, err, &transportErr)
				require.Equal(t, RuntimeErrorServiceUnavailable, transportErr.Body.Code)
				require.True(t, transportErr.Body.Retryable)
				require.ErrorIs(t, err, databaseErr)
			} else {
				require.True(t, IsRuntimeSessionError(err, RuntimeSessionErrorAuthenticationFailed))
				require.Equal(t, RuntimeErrorUnauthorized, runtimeAuthenticationError(err).Body.Code)
			}
		})
	}
}

func TestRuntimeShutdownRejectsHTTPFallbackBeforeAuthentication(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	controller := fixture.controller()
	require.NoError(t, controller.Shutdown(context.Background()))
	// Even a failing credential store must not turn shutdown into permanent 401.
	fixture.tokens.err = errors.New("pool is closed")
	for _, path := range []string{"/api/v1/agent-runtime/sessions", "/api/v1/agent-runtime/ws"} {
		method := http.MethodPost
		if strings.HasSuffix(path, "/ws") {
			method = http.MethodGet
		}
		rec := serveRuntimeRaw(t, controller, method, path, "{}")
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		requireRuntimeResponseCode(t, rec, RuntimeErrorServiceUnavailable)
	}
	require.Empty(t, fixture.tokens.plaintext)
	require.Zero(t, fixture.sessions.createCalls)
	// A principal cached by an earlier middleware cannot bypass shutdown.
	e := echo.New()
	ctx := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), httptest.NewRecorder())
	ctx.Set(runtimeAuthenticatedPrincipalContextKey, fixture.authenticated)
	_, err := controller.authenticate(ctx)
	require.Equal(t, RuntimeErrorServiceUnavailable, err.Body.Code)
}

func TestRuntimeDelegationAuthenticationAndShutdownFailClosed(t *testing.T) {
	for _, tokenOnly := range []bool{false, true} {
		for _, stopping := range []bool{false, true} {
			for _, unavailable := range []bool{false, true} {
				for _, path := range []string{"/api/v1/agent-runtime/call-agent", "/api/v1/agent-runtime/delegated-runs/read"} {
					fixture := newRuntimeHandlerFixture()
					controller := fixture.controller()
					controller.dependencies.TokenOnlyTransport = tokenOnly
					cause := error(errors.New("invalid credential"))
					if unavailable {
						cause = NewRuntimeAuthenticationUnavailableError(errors.New("private database detail"))
					}
					fixture.devices.err = cause
					fixture.delegation.resolveErr = cause
					if stopping {
						require.NoError(t, controller.Shutdown(context.Background()))
					}
					rec := serveRuntimeRaw(t, controller, http.MethodPost, path, "{}")
					if unavailable || stopping {
						require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
						requireRuntimeResponseCode(t, rec, RuntimeErrorServiceUnavailable)
					} else {
						require.Equal(t, http.StatusUnauthorized, rec.Code)
						requireRuntimeResponseCode(t, rec, RuntimeErrorUnauthorized)
					}
					require.Zero(t, fixture.delegation.calls)
					if stopping {
						require.Zero(t, fixture.delegation.resolveCalls)
						require.Zero(t, fixture.devices.calls)
					}
				}
			}
		}
	}
}

func TestRuntimeMalformedAgentTokenRejectedBeforeStoreLookup(t *testing.T) {
	// A nil query store makes any accidental lookup fail the test immediately.
	service := &Service{}
	for _, token := range []string{
		credential.AgentTokenPrefix + "abé" + strings.Repeat("a", 60),
		credential.AgentTokenPrefix + strings.Repeat("g", 64),
		credential.AgentTokenPrefix + strings.Repeat("a", 63) + "\x00",
	} {
		_, err := service.ValidateRuntimeToken(context.Background(), token, "agent:pull")
		require.Error(t, err)
		require.Equal(t, RuntimeErrorUnauthorized, mapRuntimeHTTPError(err).Body.Code)
	}
}

func TestRuntimePullClaimCancellationReturnsRetryable503(t *testing.T) {
	fixture := newRuntimeHandlerFixture()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture.leases.claim = func(context.Context, RuntimeSessionPrincipal) (*RunAssignedPayload, error) { cancel(); return nil, nil }
	controller := fixture.controller()
	e := echo.New()
	controller.Register(e.Group("/api/v1"))
	body, err := json.Marshal(RuntimeClaimRequest{RuntimeSessionID: fixture.acting.RuntimeSessionID, Capacity: 1})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/runs/claim?wait=30", strings.NewReader(string(body))).WithContext(ctx)
	req.Header.Set(echo.HeaderAuthorization, "Bearer runtime-secret")
	req.Header.Set(RuntimeAttachmentIDHeader, runtimeTestAttachmentID)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	requireRuntimeResponseCode(t, rec, RuntimeErrorServiceUnavailable)
	require.Contains(t, rec.Body.String(), `"retryable":true`)
}

type closePoolAfterInvocationVerification struct {
	RuntimeInvocationVerifier
	pool *pgxpool.Pool
}

func (v closePoolAfterInvocationVerification) VerifyInvocationToken(raw string, now time.Time) (RuntimeInvocationCapability, error) {
	capability, err := v.RuntimeInvocationVerifier.VerifyInvocationToken(raw, now)
	if err == nil {
		v.pool.Close()
	}
	return capability, err
}

func TestRuntimeInvocationBindingQueryFailureIsUnavailable(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	defer pool.Close()
	signer, err := NewRuntimeInvocationSigner(runtimeInvocationTestSecret)
	require.NoError(t, err)
	capability := runtimeInvocationCapabilityFixture()
	capability.IssuedAt = time.Now().UTC().Add(-time.Second)
	capability.ExpiresAt = capability.IssuedAt.Add(5 * time.Minute)
	_, token, err := signer.Issue(capability)
	require.NoError(t, err)
	service := NewRuntimeDelegationService(pool, nil, signer)
	_, err = service.ResolveInvocationDevice(context.Background(), token)
	require.Equal(t, RuntimeErrorUnauthorized, mapRuntimeHTTPError(err).Body.Code, "a genuinely absent binding is unauthorized")
	// The actual clock query succeeds; close the pool only after verifying the
	// capability to force the subsequent binding lookup's infrastructure branch.
	service.verifier = closePoolAfterInvocationVerification{RuntimeInvocationVerifier: signer, pool: pool}
	_, err = service.ResolveInvocationDevice(context.Background(), token)
	var transportErr *RuntimeTransportError
	require.ErrorAs(t, err, &transportErr)
	require.Equal(t, RuntimeErrorServiceUnavailable, transportErr.Body.Code)
	require.True(t, transportErr.Body.Retryable)
}
