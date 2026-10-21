package runtime_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtimepki"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type renewalTestToken struct{ token db.AgentRuntimeToken }

func (v renewalTestToken) ValidateRuntimeToken(_ context.Context, raw string, _ ...string) (db.AgentRuntimeToken, error) {
	if raw != "synthetic-renewal-token" {
		return db.AgentRuntimeToken{}, httpx.Unauthorized("invalid token")
	}
	return v.token, nil
}

// Exercise enrollment and renewal through the HTTP handler, real certificate
// signing, controlled upgrade, exact-feature Hello, and PostgreSQL constraints.
func TestRuntimeNodeControlledUpgradeMTLSRenewal(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	owner := insertCreator(t, pool)
	agent := insertAgent(t, pool, owner, "openlinker-runtime://renewal", 0, "approved")
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime',visibility='private' WHERE id=$1`, agent)
	require.NoError(t, err)
	f := runtimeNodeAdminFixture{nodeID: uuid.New(), agentID: agent, credentialID: uuid.New(), coreInstanceID: uuid.New()}
	_, err = pool.Exec(ctx, `INSERT INTO agent_tokens(id,agent_id,creator_user_id,name,prefix,token_hash,scopes,status,redeemed_at)
 VALUES($1,$2,$3,'renewal','ol_agent_123456789abc','synthetic',ARRAY['agent:pull'],'active_runtime',clock_timestamp())`, f.credentialID, agent, owner)
	require.NoError(t, err)
	manager, err := runtimepki.NewManager(ctx, pool, newTestConfig())
	require.NoError(t, err)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	runtimepki.NewCredentialService(pool, manager, renewalTestToken{db.AgentRuntimeToken{ID: f.credentialID, AgentID: agent}}).Register(e.Group("/api/v1"))
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	require.NoError(t, err)
	request := runtimepki.CredentialRequest{NodeID: f.nodeID.String(), DisplayName: "mTLS renewal", NodeVersion: upgradeVersionA,
		ProtocolVersion: runtime.RuntimeProtocolVersion, RuntimeContractID: runtime.RuntimeContractID, RuntimeContractDigest: runtime.RuntimeContractDigest,
		Features: runtime.RuntimeRequiredFeatures(), Capacity: 4, CSRPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr}))}
	call := func(path string, r runtimepki.CredentialRequest, status int) runtimepki.CredentialResponse {
		t.Helper()
		raw, err := json.Marshal(r)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/runtime-credentials"+path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer synthetic-renewal-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, status, rec.Code, rec.Body.String())
		var response runtimepki.CredentialResponse
		if status == 200 {
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		}
		return response
	}
	call("/renew", request, 409) // renewal cannot enroll a new identity
	unapproved := request
	unapproved.Features = append(slices.Clone(request.Features), "mcp_bindings.v1")
	call("", unapproved, 409) // preserve initial-enrollment gate
	first := call("", request, 200)
	principal := runtime.AuthenticatedRuntimePrincipal{AgentID: agent, CredentialID: f.credentialID,
		Device: runtime.RuntimeDeviceIdentity{NodeID: f.nodeID, CertificateSerial: first.CertificateSerial, PublicKeyThumbprintSHA256: first.PublicKeyThumbprint}}
	block, _ := pem.Decode([]byte(first.CertificatePEM))
	require.NotNil(t, block)
	fingerprint := sha256.Sum256(block.Bytes)
	principal.Device.CertificateFingerprintSHA256 = hex.EncodeToString(fingerprint[:])
	sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
	hello := upgradedSessionRequest(f, upgradeVersionA, 1)
	state, err := sessions.CreateOrAttachSession(ctx, principal, hello)
	require.NoError(t, err)
	f.sessionID = state.Session.RuntimeSessionID
	svc := newTestService(t, pool)
	svc.ConfigureCoreRuntime(f.coreInstanceID)
	registerUpgradeFixtureCluster(t, pool, f)
	_, err = svc.DrainRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	detachUpgradeSession(t, pool, f, true)
	permit := upgradeRequest("version_change", upgradeVersionA, upgradeVersionB, 0)
	permit.TargetFeatures = append(slices.Clone(request.Features), "mcp_bindings.v1", "delegated_run_read.v1")
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, owner, permit)
	require.NoError(t, err)
	request.NodeVersion, request.Features = upgradeVersionB, slices.Clone(permit.TargetFeatures)
	// Renewal while an exact upgrade permit is pending must not mutate it.
	replay := call("/renew", request, 200)
	require.Equal(t, first.CertificateSerial, replay.CertificateSerial)
	hello = upgradedSessionRequest(f, upgradeVersionB, 2)
	hello.Features = slices.Clone(request.Features)
	state, err = sessions.CreateOrAttachSession(ctx, principal, hello)
	require.NoError(t, err)
	f.sessionID = state.Session.RuntimeSessionID
	_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	for _, offline := range []bool{false, true} {
		if offline {
			detachUpgradeSession(t, pool, f, true)
		}
		var before []byte
		require.NoError(t, pool.QueryRow(ctx, `SELECT to_jsonb(n) FROM runtime_nodes n WHERE node_id=$1`, f.nodeID).Scan(&before))
		changed := request
		changed.Features = slices.Clone(request.Features)
		changed.DisplayName = "must not overwrite"
		changed.Capacity = 1
		slices.Reverse(changed.Features)
		got := call("/renew", changed, 200)
		require.Equal(t, first.CertificateSerial, got.CertificateSerial)
		legacy := changed
		legacy.Features = runtime.RuntimeRequiredFeatures()
		require.Equal(t, first.CertificateSerial, call("", legacy, 200).CertificateSerial)
		require.Equal(t, first.CertificateSerial, call("/renew", legacy, 200).CertificateSerial)
		for _, mutate := range []func(*runtimepki.CredentialRequest){
			func(r *runtimepki.CredentialRequest) { r.Features = r.Features[:len(r.Features)-1] },
			func(r *runtimepki.CredentialRequest) {
				r.Features = append(slices.Clone(r.Features), "unapproved.extension")
			},
			func(r *runtimepki.CredentialRequest) { r.Features = append(slices.Clone(r.Features), r.Features[0]) },
			func(r *runtimepki.CredentialRequest) { r.NodeVersion = upgradeVersionA },
			func(r *runtimepki.CredentialRequest) { r.RuntimeContractDigest = "invalid" },
			func(r *runtimepki.CredentialRequest) { r.NodeID = uuid.NewString() },
		} {
			r := request
			mutate(&r)
			status := 409
			if len(r.Features) > len(request.Features) && r.Features[len(r.Features)-1] == r.Features[0] {
				status = 400
			}
			call("/renew", r, status)
			call("", r, status) // issuance alias must not bypass renewal fences
		}
		var after []byte
		require.NoError(t, pool.QueryRow(ctx, `SELECT to_jsonb(n) FROM runtime_nodes n WHERE node_id=$1`, f.nodeID).Scan(&after))
		require.JSONEq(t, string(before), string(after), "renewal must preserve all Node metadata")
	}
	// Legacy renewal is certificate compatibility, not a feature-change permit.
	wrongHello := upgradedSessionRequest(f, upgradeVersionB, 3)
	wrongHello.Features = runtime.RuntimeRequiredFeatures()
	_, err = sessions.CreateOrAttachSession(ctx, principal, wrongHello)
	require.Error(t, err, "baseline renewal must not allow a baseline Hello after extension approval")
	_, err = pool.Exec(ctx, `UPDATE runtime_node_certificates SET issued_at=clock_timestamp()-INTERVAL '12 hours 1 second' WHERE node_id=$1`, f.nodeID)
	require.NoError(t, err)
	legacyRenewal := request
	legacyRenewal.Features = runtime.RuntimeRequiredFeatures()
	renewed := call("", legacyRenewal, 200) // published SDK request shape and issuance alias
	require.Equal(t, renewed.CertificateSerial, call("/renew", request, 200).CertificateSerial)
	require.NotEqual(t, first.CertificateSerial, renewed.CertificateSerial)
	require.Equal(t, first.PublicKeyThumbprint, renewed.PublicKeyThumbprint)
	block, _ = pem.Decode([]byte(renewed.CertificatePEM))
	require.NotNil(t, block)
	leaf, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM([]byte(renewed.TrustBundlePEM)))
	intermediates := x509.NewCertPool()
	require.True(t, intermediates.AppendCertsFromPEM([]byte(renewed.CertificateChainPEM)))
	_, err = leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, CurrentTime: time.Now()})
	require.NoError(t, err)
	// A real mutually authenticated TLS connection must preserve the original
	// credential binding while presenting the newly signed leaf.
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certificate, err := tls.X509KeyPair([]byte(renewed.CertificateChainPEM), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	require.NoError(t, err)
	authenticator := runtime.NewMTLSRuntimeDeviceAuthenticator(runtime.NewDBRuntimeNodeCredentialVerifier(pool))
	verified := make(chan runtime.RuntimeDeviceIdentity, 1)
	listener := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		device, authErr := authenticator.AuthenticateHTTP(r.Context(), r)
		if authErr != nil {
			http.Error(w, authErr.Error(), http.StatusUnauthorized)
			return
		}
		verified <- device
		w.WriteHeader(http.StatusOK)
	}))
	listener.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	listener.StartTLS()
	defer listener.Close()
	client := listener.Client()
	client.Timeout = 5 * time.Second
	client.Transport.(*http.Transport).TLSClientConfig.Certificates = []tls.Certificate{certificate}
	response, err := client.Get(listener.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	device := <-verified
	require.Equal(t, first.CertificateSerial, device.CertificateSerial)
	require.Equal(t, renewed.CertificateSerial, device.PresentedCertificateSerial)
	require.NoError(t, runtimepki.NewBindingVerifier(pool).VerifyRuntimePrincipalBinding(ctx, f.credentialID, device))
	// Renewed identity can reconnect with the exact approved feature set.
	principal.Device = device
	hello = upgradedSessionRequest(f, upgradeVersionB, 3)
	hello.Features = slices.Clone(request.Features)
	_, err = sessions.CreateOrAttachSession(ctx, principal, hello)
	require.NoError(t, err)
}
