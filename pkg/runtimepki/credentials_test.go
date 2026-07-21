package runtimepki

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestValidateCredentialRequestRequiresClientPersistedNodeID(t *testing.T) {
	_, _, err := validateCredentialRequest(&CredentialRequest{})
	if err == nil || !strings.Contains(err.Error(), "node_id 必填") {
		t.Fatalf("empty node_id error = %v", err)
	}
}

func TestCredentialIssuanceReplaysCommittedLeafWithinMinimumInterval(t *testing.T) {
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
		pool.Close()
		t.Fatalf("ping test database: %v", err)
	}

	creatorID := uuid.New()
	agentID := uuid.New()
	credentialID := uuid.New()
	nodeID := uuid.New()
	prefix := "ol_agent_" + strings.ReplaceAll(credentialID.String(), "-", "")[:12]
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM runtime_node_certificates WHERE node_id = $1`, nodeID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM runtime_node_bindings WHERE credential_id = $1`, credentialID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM runtime_nodes WHERE node_id = $1`, nodeID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM agent_tokens WHERE id = $1`, credentialID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM agents WHERE id = $1`, agentID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = $1`, creatorID)
		pool.Close()
	})

	if _, err = pool.Exec(ctx, `
INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
VALUES ($1, $2, 'x', 'Credential replay creator', TRUE, TRUE)`,
		creatorID, "credential-replay-"+creatorID.String()+"@example.test"); err != nil {
		t.Fatalf("insert replay creator: %v", err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO agents (
    id, creator_id, slug, name, description, endpoint_url,
    price_per_call_cents, tags, lifecycle_status, visibility, certification_status,
    connection_mode
) VALUES ($1, $2, $3, 'Credential replay Agent', 'test',
          'openlinker-runtime://credential-replay', 0, '{}',
          'active', 'private', 'unreviewed', 'runtime')`,
		agentID, creatorID, "credential-replay-"+agentID.String()); err != nil {
		t.Fatalf("insert replay Agent: %v", err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO agent_tokens (
    id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
    status, redeemed_at
) VALUES ($1, $2, $3, 'credential-replay', $4, 'test-hash-replay',
          ARRAY['agent:pull']::text[], 'active_runtime', clock_timestamp())`,
		credentialID, agentID, creatorID, prefix); err != nil {
		t.Fatalf("insert replay Token: %v", err)
	}

	root, client, server, err := createAuthorityHierarchy(time.Now().UTC())
	if err != nil {
		t.Fatalf("create test authority: %v", err)
	}
	manager := &Manager{root: root, client: client, server: server}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	features := coreruntime.RuntimeRequiredFeatures()
	sort.Strings(features)
	request := CredentialRequest{
		NodeID: nodeID.String(), DisplayName: "Credential replay Node",
		NodeVersion: "credential-replay-v1", ProtocolVersion: coreruntime.RuntimeProtocolVersion,
		RuntimeContractID: coreruntime.RuntimeContractID, RuntimeContractDigest: coreruntime.RuntimeContractDigest,
		Features: features, Capacity: 1,
	}
	token := db.AgentRuntimeToken{ID: credentialID, AgentID: agentID}
	service := NewCredentialService(pool, manager, nil)
	first, err := service.issueOrReplayCredential(ctx, token, nodeID, request, csr)
	if err != nil {
		t.Fatalf("issue first credential: %v", err)
	}
	second, err := service.issueOrReplayCredential(ctx, token, nodeID, request, csr)
	if err != nil {
		t.Fatalf("replay credential: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("retry did not replay exact committed response:\nfirst=%#v\nsecond=%#v", first, second)
	}

	var certificateCount int
	if err = pool.QueryRow(ctx, `
SELECT count(*) FROM runtime_node_certificates WHERE node_id = $1`, nodeID).Scan(&certificateCount); err != nil {
		t.Fatalf("count replayed certificates: %v", err)
	}
	if certificateCount != 1 {
		t.Fatalf("retry inserted %d certificate rows, want 1", certificateCount)
	}

	if _, err = pool.Exec(ctx, `
UPDATE runtime_node_certificates
SET issued_at = clock_timestamp() - INTERVAL '12 hours 1 second'
WHERE certificate_serial = $1`, first.CertificateSerial); err != nil {
		t.Fatalf("age replay evidence: %v", err)
	}
	third, err := service.issueOrReplayCredential(ctx, token, nodeID, request, csr)
	if err != nil {
		t.Fatalf("renew after minimum interval: %v", err)
	}
	if third.CertificateSerial == first.CertificateSerial || third.CertificatePEM == first.CertificatePEM {
		t.Fatal("renewal after minimum interval replayed the prior leaf")
	}
	if err = pool.QueryRow(ctx, `
SELECT count(*) FROM runtime_node_certificates WHERE node_id = $1`, nodeID).Scan(&certificateCount); err != nil {
		t.Fatalf("count renewed certificates: %v", err)
	}
	if certificateCount != 2 {
		t.Fatalf("renewal inserted %d certificate rows, want 2", certificateCount)
	}
}
