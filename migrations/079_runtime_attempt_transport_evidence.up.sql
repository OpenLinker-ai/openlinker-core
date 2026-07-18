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
        RAISE EXCEPTION 'migration 079 requires zero registered Core cluster members';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM runtime_cluster_control
        WHERE singleton_id = 1 AND mode = 'hard_maintenance'
    ) THEN
        RAISE EXCEPTION 'migration 079 requires hard maintenance';
    END IF;
    IF EXISTS (SELECT 1 FROM runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'migration 079 requires zero running Runs';
    END IF;
    IF (
        SELECT COUNT(*) FROM runtime_schema_contracts
        WHERE schema_version = 77
          AND migration_name = '077_external_execution_cancellation'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = current_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 079 requires the exact current schema contract 77';
    END IF;
    IF EXISTS (
        SELECT 1 FROM runtime_schema_contracts
        WHERE schema_version = 79
           OR migration_name = '079_runtime_attempt_transport_evidence'
    ) THEN
        RAISE EXCEPTION 'migration 079 found a conflicting historical schema contract 79';
    END IF;
END
$$;

ALTER TABLE runtime_session_attachments
    ADD CONSTRAINT runtime_session_attachments_attempt_identity_unique
        UNIQUE (id, runtime_session_id);

ALTER TABLE run_attempts
    ADD COLUMN runtime_attachment_id UUID,
    ADD CONSTRAINT run_attempts_runtime_attachment_state CHECK (
        runtime_attachment_id IS NULL
        OR (
            executor_type = 'runtime'
            AND accepted_at IS NOT NULL
            AND runtime_session_id IS NOT NULL
        )
    ),
    ADD CONSTRAINT run_attempts_runtime_attachment_identity_fk
        FOREIGN KEY (
            runtime_attachment_id,
            runtime_session_id
        )
        REFERENCES runtime_session_attachments (
            id,
            runtime_session_id
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

-- Existing accepted Attempts intentionally remain NULL: their transport cannot
-- be reconstructed reliably. Every new Runtime acceptance must bind the exact
-- live Attachment in the same statement, and that evidence is then immutable.
CREATE FUNCTION enforce_run_attempt_runtime_attachment_evidence()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.runtime_attachment_id IS NOT NULL
           AND NEW.runtime_attachment_id IS DISTINCT FROM OLD.runtime_attachment_id THEN
            RAISE EXCEPTION 'run attempt Runtime Attachment evidence is immutable';
        END IF;
        IF OLD.accepted_at IS NOT NULL
           AND NEW.runtime_attachment_id IS DISTINCT FROM OLD.runtime_attachment_id THEN
            RAISE EXCEPTION 'accepted run attempt Runtime Attachment evidence cannot change';
        END IF;
    END IF;

    IF NEW.runtime_attachment_id IS NOT NULL
       AND (
           NEW.executor_type <> 'runtime'
           OR NEW.accepted_at IS NULL
           OR NEW.runtime_session_id IS NULL
       ) THEN
        RAISE EXCEPTION 'run attempt Runtime Attachment evidence requires an accepted Runtime Attempt';
    END IF;

    IF NEW.executor_type = 'runtime'
       AND NEW.accepted_at IS NOT NULL
       AND NEW.runtime_attachment_id IS NULL THEN
        IF TG_OP = 'INSERT' THEN
            RAISE EXCEPTION 'new Runtime acceptance requires Runtime Attachment evidence';
        ELSIF OLD.accepted_at IS NULL THEN
            RAISE EXCEPTION 'new Runtime acceptance requires Runtime Attachment evidence';
        END IF;
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER run_attempts_runtime_attachment_evidence
    BEFORE INSERT OR UPDATE ON run_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_run_attempt_runtime_attachment_evidence();

UPDATE runtime_schema_contracts
SET is_current = FALSE
WHERE schema_version = 77
  AND migration_name = '077_external_execution_cancellation'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
  AND is_current;

INSERT INTO runtime_schema_contracts (
    schema_version, migration_name, runtime_contract_id,
    runtime_contract_digest, is_current
) VALUES (
    79,
    '079_runtime_attempt_transport_evidence',
    'openlinker.runtime.v2',
    '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481',
    TRUE
);

COMMIT;
