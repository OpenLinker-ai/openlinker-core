BEGIN;

ALTER TABLE cloud_listing_links
    ADD COLUMN synced_agent_slug TEXT NOT NULL DEFAULT '',
    ADD COLUMN synced_agent_name TEXT NOT NULL DEFAULT '',
    ADD COLUMN synced_agent_description TEXT NOT NULL DEFAULT '',
    ADD COLUMN synced_agent_tags TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    ADD COLUMN synced_availability_status TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN metadata_synced_at TIMESTAMPTZ,
    ADD COLUMN metadata_sync_error TEXT;

UPDATE cloud_listing_links l
SET synced_agent_slug = a.slug,
    synced_agent_name = a.name,
    synced_agent_description = a.description,
    synced_agent_tags = a.tags,
    synced_availability_status = COALESCE(av.availability_status, 'unknown'),
    metadata_synced_at = NOW(),
    metadata_sync_error = NULL
FROM agents a
LEFT JOIN agent_availability_snapshots av ON av.agent_id = a.id
WHERE a.id = l.local_agent_id;

ALTER TABLE cloud_listing_links
    ADD CONSTRAINT cloud_listing_links_synced_slug_len
        CHECK (char_length(synced_agent_slug) <= 80),
    ADD CONSTRAINT cloud_listing_links_synced_name_len
        CHECK (char_length(synced_agent_name) <= 80),
    ADD CONSTRAINT cloud_listing_links_synced_description_len
        CHECK (char_length(synced_agent_description) <= 500),
    ADD CONSTRAINT cloud_listing_links_synced_availability_valid
        CHECK (synced_availability_status IN ('unknown', 'healthy', 'degraded', 'unreachable')),
    ADD CONSTRAINT cloud_listing_links_metadata_sync_error_len
        CHECK (metadata_sync_error IS NULL OR char_length(metadata_sync_error) <= 1000);

COMMIT;
