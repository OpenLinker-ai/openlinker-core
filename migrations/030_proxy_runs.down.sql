BEGIN;

DROP TABLE IF EXISTS proxy_runs;

ALTER TABLE cloud_listing_links
    DROP CONSTRAINT IF EXISTS cloud_listing_links_cloud_listing_unique;

CREATE INDEX IF NOT EXISTS idx_cloud_listing_links_cloud_listing
    ON cloud_listing_links (cloud_listing_id);

COMMIT;
