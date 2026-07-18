BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.runtime.migration.079', 0));

LOCK TABLE runs IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE run_attempts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_session_attachments IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_control IN SHARE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    current_digest CONSTANT TEXT := '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481';
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 079 rollback requires zero registered Core cluster members';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM runtime_cluster_control
        WHERE singleton_id = 1 AND mode = 'hard_maintenance'
    ) THEN
        RAISE EXCEPTION 'migration 079 rollback requires hard maintenance';
    END IF;
    IF EXISTS (SELECT 1 FROM runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'migration 079 rollback requires zero running Runs';
    END IF;
    IF (
        SELECT COUNT(*) FROM runtime_schema_contracts
        WHERE schema_version = 79
          AND migration_name = '079_runtime_attempt_transport_evidence'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = current_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 079 rollback requires the exact current schema contract 79';
    END IF;
    IF (
        SELECT COUNT(*) FROM runtime_schema_contracts
        WHERE schema_version = 77
          AND migration_name = '077_external_execution_cancellation'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = current_digest
          AND NOT is_current
    ) <> 1 THEN
        RAISE EXCEPTION 'migration 079 rollback requires the exact historical schema contract 77';
    END IF;
    IF EXISTS (
        SELECT 1 FROM run_attempts WHERE runtime_attachment_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'migration 079 rollback refuses recorded Runtime Attachment evidence';
    END IF;
END
$$;

UPDATE runtime_schema_contracts
SET is_current = FALSE
WHERE schema_version = 79
  AND migration_name = '079_runtime_attempt_transport_evidence'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
  AND is_current;

DELETE FROM runtime_schema_contracts
WHERE schema_version = 79
  AND migration_name = '079_runtime_attempt_transport_evidence'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
  AND NOT is_current;

UPDATE runtime_schema_contracts
SET is_current = TRUE
WHERE schema_version = 77
  AND migration_name = '077_external_execution_cancellation'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
  AND NOT is_current;

DROP TRIGGER run_attempts_runtime_attachment_evidence ON run_attempts;
DROP FUNCTION enforce_run_attempt_runtime_attachment_evidence();

ALTER TABLE run_attempts
    DROP CONSTRAINT run_attempts_runtime_attachment_identity_fk,
    DROP CONSTRAINT run_attempts_runtime_attachment_state,
    DROP COLUMN runtime_attachment_id;

ALTER TABLE runtime_session_attachments
    DROP CONSTRAINT runtime_session_attachments_attempt_identity_unique;

COMMIT;
