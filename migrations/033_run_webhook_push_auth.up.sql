BEGIN;

ALTER TABLE run_webhook_subscriptions
    ADD COLUMN push_auth_scheme TEXT,
    ADD COLUMN push_auth_credentials TEXT,
    ADD COLUMN push_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE run_webhook_subscriptions
    ADD CONSTRAINT run_webhook_subscriptions_push_auth_scheme_len
        CHECK (push_auth_scheme IS NULL OR char_length(push_auth_scheme) BETWEEN 1 AND 80),
    ADD CONSTRAINT run_webhook_subscriptions_push_auth_credentials_len
        CHECK (push_auth_credentials IS NULL OR char_length(push_auth_credentials) <= 1000),
    ADD CONSTRAINT run_webhook_subscriptions_push_metadata_object
        CHECK (jsonb_typeof(push_metadata) = 'object');

COMMIT;
