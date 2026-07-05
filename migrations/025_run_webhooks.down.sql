BEGIN;

DROP TRIGGER IF EXISTS run_webhook_deliveries_set_updated_at ON run_webhook_deliveries;
DROP INDEX IF EXISTS idx_run_webhook_deliveries_subscription;
DROP INDEX IF EXISTS idx_run_webhook_deliveries_pending;
DROP INDEX IF EXISTS idx_run_webhook_deliveries_subscription_event;
DROP TABLE IF EXISTS run_webhook_deliveries;

DROP TRIGGER IF EXISTS run_webhook_subscriptions_set_updated_at ON run_webhook_subscriptions;
DROP INDEX IF EXISTS idx_run_webhook_subscriptions_active;
DROP INDEX IF EXISTS idx_run_webhook_subscriptions_run;
DROP TABLE IF EXISTS run_webhook_subscriptions;

COMMIT;
