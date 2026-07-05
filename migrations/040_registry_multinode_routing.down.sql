BEGIN;

ALTER TABLE proxy_runs
    DROP CONSTRAINT IF EXISTS proxy_runs_listing_idempotency_unique;

DROP INDEX IF EXISTS idx_cloud_listing_links_cloud_listing;

ALTER TABLE cloud_listing_links
    ADD CONSTRAINT cloud_listing_links_cloud_listing_unique UNIQUE (cloud_listing_id);

ALTER TABLE proxy_runs
    ADD CONSTRAINT proxy_runs_cloud_listing_id_fkey
    FOREIGN KEY (cloud_listing_id)
    REFERENCES cloud_listing_links(cloud_listing_id)
    ON DELETE CASCADE;

COMMIT;
