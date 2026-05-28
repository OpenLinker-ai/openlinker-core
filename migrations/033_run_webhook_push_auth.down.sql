BEGIN;

ALTER TABLE run_webhook_subscriptions
    DROP CONSTRAINT IF EXISTS run_webhook_subscriptions_push_metadata_object,
    DROP CONSTRAINT IF EXISTS run_webhook_subscriptions_push_auth_credentials_len,
    DROP CONSTRAINT IF EXISTS run_webhook_subscriptions_push_auth_scheme_len;

ALTER TABLE run_webhook_subscriptions
    DROP COLUMN IF EXISTS push_metadata,
    DROP COLUMN IF EXISTS push_auth_credentials,
    DROP COLUMN IF EXISTS push_auth_scheme;

COMMIT;
