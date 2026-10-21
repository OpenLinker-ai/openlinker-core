package runtime_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtimepki"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestRuntimeControlledMTLSRejectsInvalidAuthorization(t *testing.T) {
	for _, scenario := range []string{"unbound", "wrong_credential", "not_quiescent", "expired_operation", "superseded_operation", "expired_credential", "revoked_credential", "changed_cluster_policy"} {
		t.Run(scenario, func(t *testing.T) {
			pool, f, _, actor := upgradeFixtureWithBinding(t, true, true, scenario != "unbound")
			ctx := context.Background()
			cfg := newTestConfig()
			cfg.RuntimeMTLSTransitionEnabled = true
			cfg.RuntimePKIMode = "auto"
			cfg.RuntimeMTLSAPIURL = "https://runtime.example.test:9443"
			cfg.APIURL = "https://platform.example.test"
			svc := runtime.NewService(pool, cfg)
			svc.ConfigureCoreRuntime(f.coreInstanceID)
			_, err := pool.Exec(ctx, `INSERT INTO runtime_cluster_authentication_policies SELECT instance_id,$1 FROM runtime_cluster_members`, cfg.RuntimeAuthenticationPolicyID())
			require.NoError(t, err)
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			require.NoError(t, err)
			der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
			require.NoError(t, err)
			spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
			require.NoError(t, err)
			digest := sha256.Sum256(spki)
			permit := upgradeRequest("token_to_mtls", upgradeVersionA, upgradeVersionA, 0)
			permit.CredentialID = f.credentialID
			permit.PublicKeyThumbprint = hex.EncodeToString(digest[:])
			request := runtimepki.CredentialRequest{NodeID: f.nodeID.String(), DisplayName: "negative transition", NodeVersion: upgradeVersionA, ProtocolVersion: runtime.RuntimeProtocolVersion, RuntimeContractID: runtime.RuntimeContractID, RuntimeContractDigest: runtime.RuntimeContractDigest, Features: runtime.RuntimeRequiredFeatures(), Capacity: 1, CSRPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))}
			if scenario == "wrong_credential" {
				permit.CredentialID = uuid.New()
			}
			if scenario == "not_quiescent" {
				_, err = pool.Exec(ctx, `UPDATE runtime_nodes SET inflight=1 WHERE node_id=$1`, f.nodeID)
				require.NoError(t, err)
			}
			if scenario == "expired_operation" {
				permit.DeadlineAt = time.Now().Add(300 * time.Millisecond)
			}
			if scenario != "unbound" {
				_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, permit)
				if scenario == "wrong_credential" || scenario == "not_quiescent" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				request.OperationID = permit.OperationID.String()
			}
			status := http.StatusConflict
			switch scenario {
			case "expired_operation":
				time.Sleep(time.Until(permit.DeadlineAt) + 20*time.Millisecond)
			case "superseded_operation":
				_, err = svc.UpgradeRuntimeNode(ctx, f.nodeID, actor, upgradeRequest("restart_after_drain", upgradeVersionA, upgradeVersionA, 1))
				require.NoError(t, err)
			case "expired_credential":
				_, err = pool.Exec(ctx, `UPDATE agent_tokens SET expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1`, f.credentialID)
				require.NoError(t, err)
				status = 401
			case "revoked_credential":
				_, err = pool.Exec(ctx, `UPDATE agent_tokens SET status='revoked',revoked_at=clock_timestamp(),revocation_kind='manual' WHERE id=$1`, f.credentialID)
				require.NoError(t, err)
				status = 401
			case "changed_cluster_policy":
				_, err = pool.Exec(ctx, `UPDATE runtime_cluster_authentication_policies SET authentication_policy_id=$1`, strings.Repeat("f", 64))
				require.NoError(t, err)
			}
			manager, err := runtimepki.NewManager(ctx, pool, cfg)
			require.NoError(t, err)
			credentials := runtimepki.NewCredentialService(pool, manager, renewalTestToken{db.AgentRuntimeToken{ID: f.credentialID, AgentID: f.agentID}}, f.coreInstanceID)
			e := echo.New()
			e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
			credentials.Register(e.Group("/api/v1"))
			credentials.RegisterSecurityPolicy(e.Group("/api/v1"), cfg)
			body, err := json.Marshal(request)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/runtime-credentials", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer synthetic-renewal-token")
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			require.Equal(t, status, rec.Code, rec.Body.String())
			var certificates int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runtime_node_certificates WHERE node_id=$1`, f.nodeID).Scan(&certificates))
			require.Zero(t, certificates)
			if scenario != "unbound" {
				var mode string
				require.NoError(t, pool.QueryRow(ctx, `SELECT binding_mode FROM runtime_node_bindings WHERE credential_id=$1`, f.credentialID).Scan(&mode))
				require.Equal(t, "token_only", mode)
			}
			if scenario == "expired_operation" || scenario == "superseded_operation" {
				_, err = runtimepki.NewBindingVerifier(pool).ResolveTokenOnlyRuntimeDeviceIdentity(ctx, f.credentialID, f.nodeID)
				require.Error(t, err, "authorized migration must never regain token-only access")
				assertTransitionDelegationFence(t, pool, f, false)
				down, err := os.ReadFile(filepath.Join("..", "..", "migrations", "100_runtime_mtls_transition.down.sql"))
				require.NoError(t, err)
				conn, err := pool.Acquire(ctx)
				require.NoError(t, err)
				_, err = conn.Exec(ctx, string(down))
				require.ErrorContains(t, err, "authentication operations exist")
				_, err = conn.Exec(ctx, "ROLLBACK")
				require.NoError(t, err)
				conn.Release()
			}
		})
	}
}

// Use a real signed capability and the production PostgreSQL resolver/HTTP route.
// The token remains valid; only Core's authentication history changes admission.
func assertTransitionDelegationFence(t *testing.T, pool *pgxpool.Pool, f runtimeNodeAdminFixture, allowed bool) {
	t.Helper()
	signer, err := runtime.NewRuntimeInvocationSigner("transition-delegation-synthetic-secret-00000000")
	require.NoError(t, err)
	now := time.Now().UTC().Add(-time.Second)
	_, token, err := signer.Issue(runtime.RuntimeInvocationCapability{
		RunID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(), FencingToken: 1,
		AgentID: f.agentID, CredentialID: f.credentialID, NodeID: f.nodeID,
		WorkerID: "admin-worker", RuntimeSessionID: f.sessionID, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	delegation := runtime.NewRuntimeDelegationService(pool, nil, signer)
	device, err := delegation.ResolveInvocationDevice(context.Background(), token)
	if allowed {
		require.NoError(t, err)
		require.Equal(t, runtime.RuntimeAuthenticationTokenOnly, device.AuthenticationMode)
		return
	}
	require.Error(t, err)
	e := echo.New()
	runtime.NewRuntimeHTTPController(runtime.RuntimeHTTPDependencies{TokenOnlyTransport: true, MTLSTransition: true, Delegation: delegation}).Register(e.Group("/api/v1"))
	for _, path := range []string{"call-agent", "delegated-runs/read"} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/agent-runtime/"+path, strings.NewReader("{}"))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		e.ServeHTTP(response, request)
		require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
	}
}
