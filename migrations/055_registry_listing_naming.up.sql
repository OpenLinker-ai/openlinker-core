BEGIN;

ALTER TABLE IF EXISTS cloud_listing_links
    RENAME TO registry_listing_links;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME COLUMN cloud_listing_id TO registry_listing_id;

ALTER TRIGGER cloud_listing_links_set_updated_at ON registry_listing_links
    RENAME TO registry_listing_links_set_updated_at;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT cloud_listing_links_node_agent_unique TO registry_listing_links_node_agent_unique;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT cloud_listing_links_synced_slug_len TO registry_listing_links_synced_slug_len;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT cloud_listing_links_synced_name_len TO registry_listing_links_synced_name_len;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT cloud_listing_links_synced_description_len TO registry_listing_links_synced_description_len;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT cloud_listing_links_synced_availability_valid TO registry_listing_links_synced_availability_valid;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT cloud_listing_links_metadata_sync_error_len TO registry_listing_links_metadata_sync_error_len;

ALTER TABLE IF EXISTS registry_listing_links
    RENAME CONSTRAINT cloud_listing_links_redaction_keys_limit TO registry_listing_links_redaction_keys_limit;

ALTER INDEX IF EXISTS idx_cloud_listing_links_node
    RENAME TO idx_registry_listing_links_node;

ALTER INDEX IF EXISTS idx_cloud_listing_links_agent
    RENAME TO idx_registry_listing_links_agent;

ALTER INDEX IF EXISTS idx_cloud_listing_links_cloud_listing
    RENAME TO idx_registry_listing_links_registry_listing;

ALTER TABLE IF EXISTS proxy_runs
    DROP CONSTRAINT IF EXISTS proxy_runs_listing_idempotency_unique;

ALTER TABLE IF EXISTS proxy_runs
    RENAME COLUMN cloud_listing_id TO registry_listing_id;

ALTER TABLE IF EXISTS proxy_runs
    RENAME COLUMN cloud_listing_link_id TO registry_listing_link_id;

ALTER TABLE IF EXISTS proxy_runs
    RENAME COLUMN cloud_run_id TO registry_run_id;

ALTER TABLE IF EXISTS proxy_runs
    ADD CONSTRAINT proxy_runs_listing_idempotency_unique UNIQUE (registry_listing_id, idempotency_key);

ALTER INDEX IF EXISTS idx_proxy_runs_cloud_run
    RENAME TO idx_proxy_runs_registry_run;

ALTER TABLE IF EXISTS proxy_run_artifacts
    RENAME COLUMN cloud_run_id TO registry_run_id;

COMMIT;
