BEGIN;

DROP TRIGGER IF EXISTS hosted_service_executions_set_updated_at ON hosted_service_executions;
DROP TABLE IF EXISTS hosted_service_executions;

COMMIT;
