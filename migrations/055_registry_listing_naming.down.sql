BEGIN;

ALTER TABLE IF EXISTS proxy_run_artifacts
    RENAME COLUMN registry_run_id TO cloud_run_id;

ALTER INDEX IF EXISTS idx_proxy_runs_registry_run
    RENAME TO idx_proxy_runs_cloud_run;

ALTER TABLE IF EXISTS proxy_runs
    RENAME COLUMN registry_run_id TO cloud_run_id;

ALTER TABLE IF EXISTS proxy_runs
    RENAME COLUMN registry_listing_link_id TO cloud_listing_link_id;

ALTER TABLE IF EXISTS proxy_runs
    RENAME COLUMN registry_listing_id TO cloud_listing_id;

ALTER TABLE IF EXISTS proxy_runs
    DROP CONSTRAINT IF EXISTS proxy_runs_listing_idempotency_unique;

ALTER TABLE IF EXISTS proxy_runs
    ADD CONSTRAINT proxy_runs_listing_idempotency_unique UNIQUE (cloud_listing_id, idempotency_key);

ALTER INDEX IF EXISTS idx_registry_listing_links_registry_listing
    RENAME TO idx_cloud_listing_links_cloud_listing;

ALTER INDEX IF EXISTS idx_registry_listing_links_agent
    RENAME TO idx_cloud_listing_links_agent;

ALTER INDEX IF EXISTS idx_registry_listing_links_node
    RENAME TO idx_cloud_listing_links_node;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT registry_listing_links_node_agent_unique TO cloud_listing_links_node_agent_unique;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT registry_listing_links_synced_slug_len TO cloud_listing_links_synced_slug_len;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT registry_listing_links_synced_name_len TO cloud_listing_links_synced_name_len;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT registry_listing_links_synced_description_len TO cloud_listing_links_synced_description_len;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT registry_listing_links_synced_availability_valid TO cloud_listing_links_synced_availability_valid;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT registry_listing_links_metadata_sync_error_len TO cloud_listing_links_metadata_sync_error_len;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT registry_listing_links_redaction_keys_limit TO cloud_listing_links_redaction_keys_limit;

ALTER TRIGGER registry_listing_links_set_updated_at ON registry_listing_links
    RENAME TO cloud_listing_links_set_updated_at;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME COLUMN registry_listing_id TO cloud_listing_id;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME TO cloud_listing_links;

COMMIT;
