BEGIN;

ALTER TABLE proxy_runs
    DROP CONSTRAINT IF EXISTS proxy_runs_cloud_listing_id_fkey;

ALTER TABLE cloud_listing_links
    DROP CONSTRAINT IF EXISTS cloud_listing_links_cloud_listing_unique;

CREATE INDEX IF NOT EXISTS idx_cloud_listing_links_cloud_listing
    ON cloud_listing_links (cloud_listing_id);

ALTER TABLE proxy_runs
    ADD CONSTRAINT proxy_runs_listing_idempotency_unique UNIQUE (cloud_listing_id, idempotency_key);

COMMIT;
