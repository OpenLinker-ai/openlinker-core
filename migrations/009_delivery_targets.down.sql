BEGIN;

DROP TRIGGER IF EXISTS run_deliveries_set_updated_at ON run_deliveries;
DROP TABLE IF EXISTS run_deliveries;

DROP TRIGGER IF EXISTS delivery_targets_set_updated_at ON delivery_targets;
DROP TABLE IF EXISTS delivery_targets;

COMMIT;
