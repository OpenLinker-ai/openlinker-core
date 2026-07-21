package runtimepki

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestPruneExpiredRuntimeCertificatesUsesBoundedAuditPolicy(t *testing.T) {
	store := &runtimeCertificateRetentionStoreFake{rows: []int64{1000, 1000, 12}}
	deleted, err := pruneExpiredRuntimeCertificates(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2012 || store.calls != 3 {
		t.Fatalf("deleted=%d calls=%d", deleted, store.calls)
	}
	if !strings.Contains(store.query, "INTERVAL '30 days'") ||
		!strings.Contains(store.query, "FOR UPDATE OF certificate SKIP LOCKED") ||
		!strings.Contains(store.query, "node.status = 'revoked'") ||
		!strings.Contains(store.query, "ORDER BY latest.not_after DESC") {
		t.Fatalf("retention query does not preserve the approved policy: %s", store.query)
	}
}

func TestPruneExpiredRuntimeCertificatesStopsAtFourBatches(t *testing.T) {
	store := &runtimeCertificateRetentionStoreFake{rows: []int64{1000, 1000, 1000, 1000, 1000}}
	deleted, err := pruneExpiredRuntimeCertificates(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 4000 || store.calls != runtimeCertificateRetentionMaxBatch {
		t.Fatalf("deleted=%d calls=%d", deleted, store.calls)
	}
}

func TestPruneExpiredRuntimeCertificatesReturnsFailure(t *testing.T) {
	want := errors.New("database unavailable")
	store := &runtimeCertificateRetentionStoreFake{err: want}
	if _, err := pruneExpiredRuntimeCertificates(context.Background(), store); !errors.Is(err, want) {
		t.Fatalf("error=%v, want %v", err, want)
	}
}

func TestCertificateRetentionPreservesLatestActiveIdentityThenPrunesRevokedNode(t *testing.T) {
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
	nodeID := uuid.New()
	serialOld := strings.ReplaceAll(uuid.NewString(), "-", "")
	serialLatest := strings.ReplaceAll(uuid.NewString(), "-", "")
	thumbprint := retentionTestDigest(nodeID.String() + "/key")
	fingerprintOld := retentionTestDigest(nodeID.String() + "/old")
	fingerprintLatest := retentionTestDigest(nodeID.String() + "/latest")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM runtime_node_certificates WHERE node_id = $1`, nodeID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM runtime_nodes WHERE node_id = $1`, nodeID)
		pool.Close()
	})

	if _, err = pool.Exec(ctx, `
INSERT INTO runtime_nodes (
    node_id, display_name, device_certificate_serial,
    device_public_key_thumbprint, node_version, protocol_version,
    runtime_contract_id, runtime_contract_digest, features, capacity
) VALUES ($1, 'Retention integration Node', $2, $3, 'retention-v1', $4,
          $5, $6, $7, 1)`,
		nodeID, serialLatest, thumbprint, coreruntime.RuntimeProtocolVersion,
		coreruntime.RuntimeContractID, coreruntime.RuntimeContractDigest,
		coreruntime.RuntimeRequiredFeatures()); err != nil {
		t.Fatalf("insert retention Node: %v", err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO runtime_node_certificates (
    certificate_serial, node_id, public_key_thumbprint,
    certificate_fingerprint, not_before, not_after, issued_at
) VALUES
    ($1, $2, $3, $4, clock_timestamp() - INTERVAL '100 days',
     clock_timestamp() - INTERVAL '90 days', clock_timestamp() - INTERVAL '95 days'),
    ($5, $2, $3, $6, clock_timestamp() - INTERVAL '99 days',
     clock_timestamp() - INTERVAL '89 days', clock_timestamp() - INTERVAL '94 days')`,
		serialOld, nodeID, thumbprint, fingerprintOld, serialLatest, fingerprintLatest); err != nil {
		t.Fatalf("insert retention certificates: %v", err)
	}

	if _, err = pruneExpiredRuntimeCertificates(ctx, pool); err != nil {
		t.Fatalf("prune active Node inventory: %v", err)
	}
	var remaining []string
	rows, err := pool.Query(ctx, `
SELECT certificate_serial
FROM runtime_node_certificates
WHERE node_id = $1
ORDER BY certificate_serial`, nodeID)
	if err != nil {
		t.Fatalf("query retained certificates: %v", err)
	}
	for rows.Next() {
		var serial string
		if err = rows.Scan(&serial); err != nil {
			rows.Close()
			t.Fatalf("scan retained certificate: %v", err)
		}
		remaining = append(remaining, serial)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		t.Fatalf("iterate retained certificates: %v", err)
	}
	if len(remaining) != 1 || remaining[0] != serialLatest {
		t.Fatalf("active Node retained certificates = %#v, want latest %s", remaining, serialLatest)
	}

	if _, err = pool.Exec(ctx, `
UPDATE runtime_nodes
SET status = 'revoked', revoked_at = clock_timestamp(), revoke_reason = 'retention integration'
WHERE node_id = $1`, nodeID); err != nil {
		t.Fatalf("revoke retention Node: %v", err)
	}
	if _, err = pruneExpiredRuntimeCertificates(ctx, pool); err != nil {
		t.Fatalf("prune revoked Node inventory: %v", err)
	}
	var remainingCount int
	if err = pool.QueryRow(ctx, `
SELECT count(*) FROM runtime_node_certificates WHERE node_id = $1`, nodeID).Scan(&remainingCount); err != nil {
		t.Fatalf("count revoked Node inventory: %v", err)
	}
	if remainingCount != 0 {
		t.Fatalf("revoked Node retained %d expired certificates", remainingCount)
	}
}

func retentionTestDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

type runtimeCertificateRetentionStoreFake struct {
	rows  []int64
	err   error
	calls int
	query string
}

func (f *runtimeCertificateRetentionStoreFake) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	f.calls++
	f.query = query
	if f.err != nil {
		return pgconn.CommandTag{}, f.err
	}
	var rows int64
	if len(f.rows) > 0 {
		rows = f.rows[0]
		f.rows = f.rows[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", rows)), nil
}
