package runtime_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	runtime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtimepki"
)

func TestTokenOnlyFirstSessionAtomicallyEnrollsDurableNodeBinding(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	if err = pool.Ping(ctx); err != nil {
		t.Fatalf("ping test database: %v", err)
	}

	creatorID := uuid.New()
	agentID := uuid.New()
	credentialID := uuid.New()
	otherCredentialID := uuid.New()
	nodeID := uuid.New()
	sessionID := uuid.New()
	coreInstanceID := uuid.New()
	prefix := "ol_agent_" + strings.ReplaceAll(credentialID.String(), "-", "")[:12]
	otherPrefix := "ol_agent_" + strings.ReplaceAll(otherCredentialID.String(), "-", "")[:12]

	var originalMode string
	if err = pool.QueryRow(ctx, `SELECT mode FROM runtime_cluster_control WHERE singleton_id = 1`).Scan(&originalMode); err != nil {
		t.Fatalf("read cluster mode: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupTokenOnlyIntegrationFixture(
			cleanupCtx, pool, originalMode, creatorID, agentID, nodeID,
			credentialID, otherCredentialID,
		)
		pool.Close()
	})

	if _, err = pool.Exec(ctx, `
INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
VALUES ($1, $2, 'x', 'Token-only integration creator', TRUE, TRUE)`,
		creatorID, "token-only-"+creatorID.String()+"@example.test"); err != nil {
		t.Fatalf("insert token-only creator: %v", err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, tags, lifecycle_status, visibility, certification_status,
    connection_mode
) VALUES ($1, $2, $3, 'Token-only integration Agent', 'test',
          'openlinker-runtime://token-only-integration', 0, '{}',
          'active', 'private', 'unreviewed', 'runtime')`,
		agentID, creatorID, "token-only-"+agentID.String()); err != nil {
		t.Fatalf("insert token-only Agent: %v", err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    status, redeemed_at
) VALUES
    ($1, $2, $3, 'token-only-primary', $4, 'test-hash-primary',
     ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp()),
	    ($5, $2, $3, 'token-only-other', $6, 'test-hash-other',
	     ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		credentialID, agentID, creatorID, prefix, otherCredentialID, otherPrefix); err != nil {
		t.Fatalf("insert token-only Tokens: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE runtime_cluster_control SET mode = 'normal' WHERE singleton_id = 1`); err != nil {
		t.Fatalf("enable Session admission: %v", err)
	}

	binder := runtimepki.NewBindingVerifier(pool)
	device, err := binder.ResolveTokenOnlyRuntimeDeviceIdentity(ctx, credentialID, nodeID)
	if err != nil {
		t.Fatalf("resolve pending token-only identity: %v", err)
	}
	if device.AuthenticationMode != runtime.RuntimeAuthenticationTokenOnly || device.NodeID != nodeID {
		t.Fatalf("pending token-only identity = %#v", device)
	}
	principal := runtime.AuthenticatedRuntimePrincipal{
		AgentID: agentID, CredentialID: credentialID, Device: device,
	}
	features := runtime.RuntimeRequiredFeatures()
	sort.Strings(features)
	request := runtime.RuntimeSessionRequest{
		RuntimeSessionIdentity: runtime.RuntimeSessionIdentity{
			RuntimeSessionID: sessionID,
			NodeID:           nodeID,
			AgentID:          agentID,
			WorkerID:         "token-only-integration-worker",
			SessionEpoch:     1,
		},
		NodeVersion:           "token-only-integration-v1",
		ProtocolVersion:       runtime.RuntimeProtocolVersion,
		RuntimeContractID:     runtime.RuntimeContractID,
		RuntimeContractDigest: runtime.RuntimeContractDigest,
		Features:              features,
		Capacity:              1,
		Transport:             runtime.RuntimeTransportWebSocket,
	}
	service := runtime.NewRuntimeSessionService(pool, coreInstanceID)
	state, err := service.CreateOrAttachSession(ctx, principal, request)
	if err != nil {
		t.Fatalf("create first token-only Session: %v", err)
	}
	if state, err = service.CreateOrAttachSession(ctx, principal, request); err != nil {
		t.Fatalf("retry first token-only Session: %v", err)
	}
	if state.Attachment == nil {
		t.Fatal("created token-only Session has no active attachment")
	}
	registration := runtime.RuntimeConnectionRegistration{
		Identity: runtime.RuntimeConnectionIdentity{
			RuntimeSessionID: state.Session.RuntimeSessionID,
			SessionEpoch:     state.Session.SessionEpoch,
			AttachmentID:     state.Attachment.ID,
		},
		CredentialID: credentialID,
	}
	validator := runtime.NewPostgresRuntimeCredentialConnectionValidator(pool, coreInstanceID)
	validation, err := validator.Validate(ctx, []runtime.RuntimeConnectionRegistration{registration})
	if err != nil || len(validation) != 1 || !validation[0].Valid {
		t.Fatalf("active token-only connection validation = %#v, err=%v", validation, err)
	}

	var bindingMode string
	var boundNodeID, boundAgentID uuid.UUID
	var sessionCount int
	if err = pool.QueryRow(ctx, `
SELECT binding.binding_mode, binding.node_id, binding.agent_id,
       (SELECT count(*) FROM runtime_sessions session
        WHERE session.runtime_session_id = $2
          AND session.credential_id = binding.credential_id)
FROM runtime_node_bindings binding
WHERE binding.credential_id = $1`, credentialID, sessionID).Scan(
		&bindingMode, &boundNodeID, &boundAgentID, &sessionCount,
	); err != nil {
		t.Fatalf("read committed token-only binding: %v", err)
	}
	if bindingMode != "token_only" || boundNodeID != nodeID || boundAgentID != agentID || sessionCount != 1 {
		t.Fatalf("committed binding = mode=%q node=%s agent=%s sessions=%d",
			bindingMode, boundNodeID, boundAgentID, sessionCount)
	}

	if _, err = binder.ResolveTokenOnlyRuntimeDeviceIdentity(ctx, credentialID, uuid.New()); err == nil {
		t.Fatal("bound Token selected a different Node")
	}
	if _, err = binder.ResolveTokenOnlyRuntimeDeviceIdentity(ctx, otherCredentialID, nodeID); err == nil {
		t.Fatal("new Token borrowed an existing Node identity")
	}

	signer, err := runtime.NewRuntimeInvocationSigner("token-only-delegation-test-secret-00000000")
	if err != nil {
		t.Fatalf("create Runtime invocation signer: %v", err)
	}
	issuedAt := time.Now().UTC().Add(-time.Second)
	_, invocationToken, err := signer.Issue(runtime.RuntimeInvocationCapability{
		RunID:            uuid.New(),
		AttemptID:        uuid.New(),
		LeaseID:          uuid.New(),
		FencingToken:     1,
		AgentID:          agentID,
		CredentialID:     credentialID,
		NodeID:           nodeID,
		WorkerID:         "token-only-integration-worker",
		RuntimeSessionID: sessionID,
		IssuedAt:         issuedAt,
		ExpiresAt:        issuedAt.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("issue Runtime invocation capability: %v", err)
	}
	invocationDevice, err := runtime.NewRuntimeDelegationService(pool, nil, signer).
		ResolveInvocationDevice(ctx, invocationToken)
	if err != nil {
		t.Fatalf("resolve token-only Runtime delegation device: %v", err)
	}
	if invocationDevice.AuthenticationMode != runtime.RuntimeAuthenticationTokenOnly ||
		invocationDevice.NodeID != nodeID ||
		invocationDevice.CertificateSerial != device.CertificateSerial ||
		invocationDevice.PublicKeyThumbprintSHA256 != device.PublicKeyThumbprintSHA256 ||
		invocationDevice.CertificateFingerprintSHA256 != device.CertificateFingerprintSHA256 {
		t.Fatalf("token-only Runtime delegation device = %#v; want %#v", invocationDevice, device)
	}
}

func TestTokenOnlyEnrollmentRollsBackWhenSessionAdmissionFails(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	if err = pool.Ping(ctx); err != nil {
		t.Fatalf("ping test database: %v", err)
	}

	creatorID := uuid.New()
	agentID := uuid.New()
	credentialID := uuid.New()
	nodeID := uuid.New()
	var originalMode string
	if err = pool.QueryRow(ctx, `SELECT mode FROM runtime_cluster_control WHERE singleton_id = 1`).Scan(&originalMode); err != nil {
		t.Fatalf("read cluster mode: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupTokenOnlyIntegrationFixture(
			cleanupCtx, pool, originalMode, creatorID, agentID, nodeID, credentialID,
		)
		pool.Close()
	})

	prefix := "ol_agent_" + strings.ReplaceAll(credentialID.String(), "-", "")[:12]
	if _, err = pool.Exec(ctx, `
INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
VALUES ($1, $2, 'x', 'Token-only rollback creator', TRUE, TRUE)`,
		creatorID, "token-only-rollback-"+creatorID.String()+"@example.test"); err != nil {
		t.Fatalf("insert rollback creator: %v", err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, tags, lifecycle_status, visibility, certification_status,
    connection_mode
) VALUES ($1, $2, $3, 'Token-only rollback Agent', 'test',
          'openlinker-runtime://token-only-rollback', 0, '{}',
          'active', 'private', 'unreviewed', 'runtime')`,
		agentID, creatorID, "token-only-rollback-"+agentID.String()); err != nil {
		t.Fatalf("insert rollback Agent: %v", err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    status, redeemed_at
) VALUES ($1, $2, $3, 'token-only-rollback', $4, 'test-hash-rollback',
          ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		credentialID, agentID, creatorID, prefix); err != nil {
		t.Fatalf("insert rollback Token: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE runtime_cluster_control SET mode = 'hard_maintenance' WHERE singleton_id = 1`); err != nil {
		t.Fatalf("disable Session admission: %v", err)
	}

	binder := runtimepki.NewBindingVerifier(pool)
	device, err := binder.ResolveTokenOnlyRuntimeDeviceIdentity(ctx, credentialID, nodeID)
	if err != nil {
		t.Fatalf("resolve pending token-only identity: %v", err)
	}
	features := runtime.RuntimeRequiredFeatures()
	request := runtime.RuntimeSessionRequest{
		RuntimeSessionIdentity: runtime.RuntimeSessionIdentity{
			RuntimeSessionID: uuid.New(), NodeID: nodeID, AgentID: agentID,
			WorkerID: "token-only-rollback-worker", SessionEpoch: 1,
		},
		NodeVersion: "token-only-rollback-v1", ProtocolVersion: runtime.RuntimeProtocolVersion,
		RuntimeContractID: runtime.RuntimeContractID, RuntimeContractDigest: runtime.RuntimeContractDigest,
		Features: features, Capacity: 1, Transport: runtime.RuntimeTransportWebSocket,
	}
	_, err = runtime.NewRuntimeSessionService(pool, uuid.New()).CreateOrAttachSession(ctx,
		runtime.AuthenticatedRuntimePrincipal{AgentID: agentID, CredentialID: credentialID, Device: device}, request)
	var httpErr *httpx.HTTPError
	if err == nil || !errors.As(err, &httpErr) || httpErr.Status != http.StatusServiceUnavailable {
		t.Fatalf("hard-maintenance admission error = %v", err)
	}

	var nodeCount, bindingCount int
	if err = pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM runtime_nodes WHERE node_id = $1),
       (SELECT count(*) FROM runtime_node_bindings WHERE credential_id = $2)`,
		nodeID, credentialID).Scan(&nodeCount, &bindingCount); err != nil {
		t.Fatalf("read rolled-back enrollment: %v", err)
	}
	if nodeCount != 0 || bindingCount != 0 {
		t.Fatalf("failed Session left enrollment rows: nodes=%d bindings=%d", nodeCount, bindingCount)
	}
}

func cleanupTokenOnlyIntegrationFixture(
	ctx context.Context,
	pool *pgxpool.Pool,
	originalMode string,
	creatorID, agentID, nodeID uuid.UUID,
	credentialIDs ...uuid.UUID,
) {
	if pool == nil {
		return
	}
	_, _ = pool.Exec(ctx, `
DELETE FROM runtime_session_attachments
WHERE runtime_session_id IN (
    SELECT runtime_session_id FROM runtime_sessions
    WHERE credential_id = ANY($1::uuid[])
)`, credentialIDs)
	_, _ = pool.Exec(ctx, `DELETE FROM runtime_sessions WHERE credential_id = ANY($1::uuid[])`, credentialIDs)
	_, _ = pool.Exec(ctx, `DELETE FROM runtime_node_certificates WHERE node_id = $1`, nodeID)
	_, _ = pool.Exec(ctx, `DELETE FROM runtime_node_bindings WHERE credential_id = ANY($1::uuid[])`, credentialIDs)
	_, _ = pool.Exec(ctx, `DELETE FROM runtime_nodes WHERE node_id = $1`, nodeID)
	_, _ = pool.Exec(ctx, `DELETE FROM agent_tokens WHERE id = ANY($1::uuid[])`, credentialIDs)
	_, _ = pool.Exec(ctx, `DELETE FROM agents WHERE id = $1`, agentID)
	_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, creatorID)
	_, _ = pool.Exec(ctx, `UPDATE runtime_cluster_control SET mode = $1 WHERE singleton_id = 1`, originalMode)
}
