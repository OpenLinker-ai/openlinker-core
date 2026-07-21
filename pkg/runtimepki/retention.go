package runtimepki

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

const (
	runtimeCertificateRetentionInterval = 6 * time.Hour
	runtimeCertificateRetentionBatch    = int64(1000)
	runtimeCertificateRetentionMaxBatch = 4
)

type runtimeCertificateRetentionStore interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// StartCertificateRetentionWorker prunes only certificate audit inventory.
// It is intentionally independent from the optional automatic PKI manager so
// deployments that disable mTLS still converge historical certificate rows.
func StartCertificateRetentionWorker(ctx context.Context, pool *pgxpool.Pool) {
	if ctx == nil || pool == nil {
		return
	}
	go func() {
		prune := func() {
			deleted, err := pruneExpiredRuntimeCertificates(ctx, pool)
			if err != nil {
				if ctx.Err() == nil {
					log.Error().Err(err).Msg("prune Runtime certificate audit inventory failed")
				}
				return
			}
			if deleted > 0 {
				log.Info().Int64("deleted", deleted).Msg("Runtime certificate audit inventory pruned")
			}
		}

		prune()
		ticker := time.NewTicker(runtimeCertificateRetentionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				prune()
			}
		}
	}()
}

func pruneExpiredRuntimeCertificates(ctx context.Context, store runtimeCertificateRetentionStore) (int64, error) {
	if store == nil {
		return 0, nil
	}
	var deleted int64
	for batch := 0; batch < runtimeCertificateRetentionMaxBatch; batch++ {
		tag, err := store.Exec(ctx, `
WITH candidates AS (
    SELECT certificate.certificate_serial
    FROM runtime_node_certificates certificate
    JOIN runtime_nodes node ON node.node_id = certificate.node_id
    WHERE GREATEST(certificate.not_after, COALESCE(certificate.revoked_at, certificate.not_after))
              < clock_timestamp() - INTERVAL '30 days'
      AND (
          node.status = 'revoked'
          OR certificate.certificate_serial <> (
              SELECT latest.certificate_serial
              FROM runtime_node_certificates latest
              WHERE latest.node_id = certificate.node_id
              ORDER BY latest.not_after DESC, latest.issued_at DESC, latest.certificate_serial DESC
              LIMIT 1
          )
      )
    ORDER BY certificate.not_after ASC, certificate.certificate_serial ASC
    FOR UPDATE OF certificate SKIP LOCKED
    LIMIT $1
)
DELETE FROM runtime_node_certificates certificate
USING candidates
WHERE certificate.certificate_serial = candidates.certificate_serial`, runtimeCertificateRetentionBatch)
		if err != nil {
			return deleted, err
		}
		rows := tag.RowsAffected()
		deleted += rows
		if rows < runtimeCertificateRetentionBatch {
			break
		}
	}
	return deleted, nil
}
