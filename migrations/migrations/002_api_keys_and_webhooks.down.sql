-- 002_api_keys_and_webhooks.down.sql

BEGIN;

DROP TRIGGER IF EXISTS webhook_deliveries_set_updated_at ON webhook_deliveries;
DROP TABLE IF EXISTS webhook_deliveries;

ALTER TABLE agents DROP CONSTRAINT IF EXISTS agents_webhook_https;
ALTER TABLE agents
    DROP COLUMN IF EXISTS webhook_secret,
    DROP COLUMN IF EXISTS webhook_url;

DROP TABLE IF EXISTS api_keys;

COMMIT;
