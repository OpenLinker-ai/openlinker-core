BEGIN;

ALTER TABLE cloud_listing_links
    DROP CONSTRAINT IF EXISTS cloud_listing_links_metadata_sync_error_len,
    DROP CONSTRAINT IF EXISTS cloud_listing_links_synced_availability_valid,
    DROP CONSTRAINT IF EXISTS cloud_listing_links_synced_description_len,
    DROP CONSTRAINT IF EXISTS cloud_listing_links_synced_name_len,
    DROP CONSTRAINT IF EXISTS cloud_listing_links_synced_slug_len;

ALTER TABLE cloud_listing_links
    DROP COLUMN IF EXISTS metadata_sync_error,
    DROP COLUMN IF EXISTS metadata_synced_at,
    DROP COLUMN IF EXISTS synced_availability_status,
    DROP COLUMN IF EXISTS synced_agent_tags,
    DROP COLUMN IF EXISTS synced_agent_description,
    DROP COLUMN IF EXISTS synced_agent_name,
    DROP COLUMN IF EXISTS synced_agent_slug;

COMMIT;
