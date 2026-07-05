BEGIN;

ALTER TABLE cloud_listing_links
    ADD COLUMN payload_redaction_keys TEXT[] NOT NULL DEFAULT ARRAY[]::text[];

ALTER TABLE proxy_runs
    ADD COLUMN payload_redaction_keys TEXT[] NOT NULL DEFAULT ARRAY[]::text[];

ALTER TABLE cloud_listing_links
    ADD CONSTRAINT cloud_listing_links_redaction_keys_limit
        CHECK (cardinality(payload_redaction_keys) <= 20);

ALTER TABLE proxy_runs
    ADD CONSTRAINT proxy_runs_redaction_keys_limit
        CHECK (cardinality(payload_redaction_keys) <= 20);

COMMIT;
