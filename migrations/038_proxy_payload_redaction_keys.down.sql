BEGIN;

ALTER TABLE proxy_runs
    DROP CONSTRAINT IF EXISTS proxy_runs_redaction_keys_limit;

ALTER TABLE cloud_listing_links
    DROP CONSTRAINT IF EXISTS cloud_listing_links_redaction_keys_limit;

ALTER TABLE proxy_runs
    DROP COLUMN IF EXISTS payload_redaction_keys;

ALTER TABLE cloud_listing_links
    DROP COLUMN IF EXISTS payload_redaction_keys;

COMMIT;
