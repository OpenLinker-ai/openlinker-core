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
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/auth"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtimepki"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestRuntimeControlledMTLSTransitionAndRotation(t *testing.T) {
	for _, extensions := range []bool{false, true} {
		name := "baseline"
		if extensions {
			name = "skill_and_mcp_extensions"
		}
		t.Run(name, func(t *testing.T) { testControlledMTLSTransitionAndRotation(t, extensions) })
	}
}

func testControlledMTLSTransitionAndRotation(t *testing.T, extensions bool) {
	features := runtime.RuntimeRequiredFeatures()
	if extensions {
		features = append(features, "mcp_bindings.v1", "skill_packages.v1")
	}
	pool, f, _, actor := upgradeFixtureWithBinding(t, false, true, true, features...)
	ctx := context.Background()
	sessionRequest := func(epoch int64) runtime.RuntimeSessionRequest {
		r := upgradedSessionRequest(f, upgradeVersionA, epoch)
		r.Features = slices.Clone(features)
		return r
	}
	cfg := newTestConfig()
	cfg.RuntimeMTLSTransitionEnabled = true
	cfg.RuntimePKIMode = "auto"
	cfg.RuntimeMTLSAPIURL = "https://runtime.example.test:9443"
	cfg.APIURL = "https://platform.example.test"
	svc := runtime.NewService(pool, cfg)
	svc.ConfigureCoreRuntime(f.coreInstanceID)
	_, err := pool.Exec(ctx, `UPDATE agents SET visibility='private' WHERE id=$1`, f.agentID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO runtime_cluster_authentication_policies SELECT instance_id,$1 FROM runtime_cluster_members`, cfg.RuntimeAuthenticationPolicyID())
	require.NoError(t, err)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	require.NoError(t, err)
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	digest := sha256.Sum256(spki)
	thumb := hex.EncodeToString(digest[:])
	manager, err := runtimepki.NewManager(ctx, pool, cfg)
	require.NoError(t, err)
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	credentials := runtimepki.NewCredentialService(pool, manager, renewalTestToken{db.AgentRuntimeToken{ID: f.credentialID, AgentID: f.agentID}}, f.coreInstanceID)
	credentials.Register(e.Group("/api/v1"))
	credentials.RegisterSecurityPolicy(e.Group("/api/v1"), cfg)
	runtime.NewHandler(svc).RegisterAdmin(e.Group("/api/v1"), auth.JWTMiddlewareWithUserStatus(cfg.JWTSecret, auth.NewDBUserStatusChecker(pool)), auth.AdminMiddleware(db.New(pool)))
	request := runtimepki.CredentialRequest{NodeID: f.nodeID.String(), DisplayName: "transition", NodeVersion: upgradeVersionA, ProtocolVersion: runtime.RuntimeProtocolVersion,
		RuntimeContractID: runtime.RuntimeContractID, RuntimeContractDigest: runtime.RuntimeContractDigest, Features: features, Capacity: 4, CSRPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr}))}
	call := func(path string, body any, status int) []byte {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/"+path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer synthetic-renewal-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, status, rec.Code, rec.Body.String())
		return rec.Body.Bytes()
	}
	issue := func(r runtimepki.CredentialRequest, status int) runtimepki.CredentialResponse {
		t.Helper()
		raw := call("runtime-credentials", r, status)
		var out runtimepki.CredentialResponse
		if status == 200 {
			require.NoError(t, json.Unmarshal(raw, &out))
		}
		return out
	}
	policy := func(mode string) map[string]any {
		t.Helper()
		raw := call("runtime-security-policy", map[string]string{"node_id": f.nodeID.String()}, 200)
		var out map[string]any
		require.NoError(t, json.Unmarshal(raw, &out))
		require.Equal(t, mode, out["authentication_mode"])
		return out
	}
	policy("token_only")
	assertTransitionDelegationFence(t, pool, f, true)
	issue(request, 409)
	permit := upgradeRequest("token_to_mtls", upgradeVersionA, upgradeVersionA, 0)
	permit.CredentialID = f.credentialID
	permit.PublicKeyThumbprint = thumb
	second := uuid.New()
	_, err = pool.Exec(ctx, `UPDATE runtime_cluster_control SET expected_replicas=2 WHERE singleton_id=1`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE runtime_cluster_control SET expected_replicas=1 WHERE singleton_id=1`)
	})
	_, err = pool.Exec(ctx, `INSERT INTO runtime_cluster_members(instance_id,release_version,release_commit,schema_version,schema_checksum,runtime_contract_id,runtime_contract_digest,ready)
 VALUES($1,'upgrade-test','synthetic',$2,$3,$4,$5,true)`, second, runtime.RuntimeSchemaVersion, runtime.RuntimeSchemaChecksum, runtime.RuntimeContractID, runtime.RuntimeContractDigest)
	require.NoError(t, err)
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, permit)
	requireUpgradeCode(t, err, "RUNTIME_NODE_MTLS_POLICY_MISMATCH")
	_, err = pool.Exec(ctx, `INSERT INTO runtime_cluster_authentication_policies(instance_id,authentication_policy_id) VALUES($2,$1)`, cfg.RuntimeAuthenticationPolicyID(), second)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE users SET is_admin=true WHERE id=$1`, actor)
	require.NoError(t, err)
	adminJWT, err := auth.GenerateToken(actor.String(), cfg.JWTSecret, time.Hour)
	require.NoError(t, err)
	adminBody, err := json.Marshal(permit)
	require.NoError(t, err)
	adminRequest := httptest.NewRequest(http.MethodPost, "/api/v1/admin/runtime/nodes/"+f.nodeID.String()+"/upgrade", bytes.NewReader(adminBody))
	adminRequest.Header.Set("Authorization", "Bearer "+adminJWT)
	adminRequest.Header.Set("Content-Type", "application/json")
	adminResponse := httptest.NewRecorder()
	e.ServeHTTP(adminResponse, adminRequest)
	require.Equal(t, 200, adminResponse.Code, adminResponse.Body.String())
	var receipt runtime.RuntimeNodeUpgradeReceipt
	require.NoError(t, json.Unmarshal(adminResponse.Body.Bytes(), &receipt))
	require.Equal(t, &f.credentialID, receipt.CredentialID)
	require.Equal(t, &thumb, receipt.PublicKeyThumbprint)
	replay, err := svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, permit)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, permit.OperationID.String(), policy("mtls")["operation_id"])
	binder := runtimepki.NewBindingVerifier(pool)
	_, err = binder.ResolveTokenOnlyRuntimeDeviceIdentity(ctx, f.credentialID, f.nodeID)
	require.Error(t, err, "authorization must fence old clients")
	assertTransitionDelegationFence(t, pool, f, false)
	sessions := runtime.NewRuntimeSessionService(pool, f.coreInstanceID)
	oldPrincipal := runtimeNodeAdminPrincipal(f)
	_, err = sessions.CreateOrAttachSession(ctx, oldPrincipal, sessionRequest(2))
	require.Error(t, err, "cached old principal cannot enter an uncompleted migration")
	request.OperationID = permit.OperationID.String()
	wrong := request
	wrong.Features = append(slices.Clone(request.Features), "unapproved.extension")
	issue(wrong, 409)
	wrong = request
	wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	wrongCSR, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, wrongKey)
	require.NoError(t, err)
	wrong.CSRPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: wrongCSR}))
	issue(wrong, 409)
	wrong = request
	wrong.OperationID = uuid.NewString()
	issue(wrong, 409)
	wrong = request
	wrong.NodeID = uuid.NewString()
	issue(wrong, 409)
	first := issue(request, 200)
	assertTransitionDelegationFence(t, pool, f, false)
	require.Equal(t, first.CertificateSerial, issue(request, 200).CertificateSerial)
	require.Equal(t, thumb, first.PublicKeyThumbprint)
	require.Equal(t, f.nodeID.String(), first.NodeID)
	require.Equal(t, f.agentID.String(), first.AgentID)
	_, err = pool.Exec(ctx, `DELETE FROM runtime_node_bindings WHERE credential_id=$1`, f.credentialID)
	require.Error(t, err, "binding history cannot be deleted")
	_, err = pool.Exec(ctx, `UPDATE runtime_node_credential_operations SET completed_at=clock_timestamp() WHERE operation_id=$1`, permit.OperationID)
	require.Error(t, err, "credential receipt must be immutable")
	_, err = pool.Exec(ctx, `UPDATE runtime_node_bindings SET binding_mode='token_only' WHERE credential_id=$1`, f.credentialID)
	require.Error(t, err)
	_, err = pool.Exec(ctx, `UPDATE runtime_nodes SET device_public_key_thumbprint=$2 WHERE node_id=$1`, f.nodeID, strings.Repeat("e", 64))
	require.Error(t, err)
	_, err = binder.ResolveTokenOnlyRuntimeDeviceIdentity(ctx, f.credentialID, f.nodeID)
	require.Error(t, err)
	authenticate := func(issued runtimepki.CredentialResponse) runtime.RuntimeDeviceIdentity {
		t.Helper()
		roots := x509.NewCertPool()
		require.True(t, roots.AppendCertsFromPEM([]byte(issued.TrustBundlePEM)))
		keyDER, err := x509.MarshalECPrivateKey(key)
		require.NoError(t, err)
		pair, err := tls.X509KeyPair([]byte(issued.CertificateChainPEM), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
		require.NoError(t, err)
		authenticator := runtime.NewMTLSRuntimeDeviceAuthenticator(runtime.NewDBRuntimeNodeCredentialVerifier(pool))
		verified := make(chan runtime.RuntimeDeviceIdentity, 1)
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, uint16(tls.VersionTLS13), r.TLS.Version)
			device, err := authenticator.AuthenticateHTTP(r.Context(), r)
			if err != nil {
				http.Error(w, err.Error(), 401)
				return
			}
			verified <- device
			w.WriteHeader(200)
		}))
		server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
		server.StartTLS()
		defer server.Close()
		client := server.Client()
		client.Timeout = 5 * time.Second
		_, err = client.Get(server.URL)
		require.Error(t, err, "missing client certificate must fail TLS")
		client.Transport.(*http.Transport).TLSClientConfig.Certificates = []tls.Certificate{pair}
		response, err := client.Get(server.URL)
		require.NoError(t, err)
		response.Body.Close()
		require.Equal(t, 200, response.StatusCode)
		device := <-verified
		require.Equal(t, f.nodeID, device.NodeID)
		require.Equal(t, issued.CertificateSerial, device.PresentedCertificateSerial)
		require.Equal(t, oldPrincipal.Device.CertificateSerial, device.CertificateSerial)
		require.NoError(t, binder.VerifyRuntimePrincipalBinding(ctx, f.credentialID, device))
		return device
	}
	principal := oldPrincipal
	principal.Device = authenticate(first)
	old := sessionRequest(1)
	old.RuntimeSessionID = f.sessionID
	_, err = sessions.CreateOrAttachSession(ctx, principal, old)
	require.Error(t, err)
	state, err := sessions.CreateOrAttachSession(ctx, principal, sessionRequest(2))
	require.NoError(t, err)
	require.Equal(t, "admin-worker", state.Session.WorkerID)
	require.Equal(t, "draining", state.Session.Status)
	require.Zero(t, state.Session.Capacity)
	f.sessionID = state.Session.RuntimeSessionID
	_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	require.NotContains(t, policy("mtls"), "operation_id", "activation must clear the credential operation on future startups")
	ordinary := request
	ordinary.OperationID = ""
	require.Equal(t, first.CertificateSerial, issue(ordinary, 200).CertificateSerial, "active migrated identity must support ordinary credential refresh")
	// Rotate the actual leaf, without changing the DB clock or the replay interval.
	_, err = svc.DrainRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	detachUpgradeSession(t, pool, f, true)
	rotate := upgradeRequest("certificate_rotation", upgradeVersionA, upgradeVersionA, 1)
	rotate.CredentialID = f.credentialID
	rotate.PublicKeyThumbprint = thumb
	_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, rotate)
	require.NoError(t, err)
	request.OperationID = rotate.OperationID.String()
	renewed := issue(request, 200)
	require.NotEqual(t, first.CertificateSerial, renewed.CertificateSerial)
	require.Equal(t, thumb, renewed.PublicKeyThumbprint)
	require.Equal(t, renewed.CertificateSerial, issue(request, 200).CertificateSerial)
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_certificates WHERE node_id=$1`, f.nodeID).Scan(&count))
	require.Equal(t, 2, count)
	principal.Device = authenticate(renewed)
	state, err = sessions.CreateOrAttachSession(ctx, principal, sessionRequest(3))
	require.NoError(t, err)
	require.Zero(t, state.Session.Capacity)
	_, err = svc.ActivateRuntimeNode(ctx, f.nodeID)
	require.NoError(t, err)
	var visibility string
	require.NoError(t, pool.QueryRow(ctx, `SELECT visibility FROM agents WHERE id=$1`, f.agentID).Scan(&visibility))
	require.Equal(t, "private", visibility)
	require.NotContains(t, policy("mtls"), "operation_id")
}
