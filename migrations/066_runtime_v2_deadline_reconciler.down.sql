BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.runtime.v2.migration.066', 0));

LOCK TABLE runtime_session_attachments IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_sessions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_nodes IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runs IN SHARE MODE;
LOCK TABLE run_attempts IN SHARE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    old_digest CONSTANT TEXT := 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f';
    new_digest CONSTANT TEXT := '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61';
    has_old_predecessor BOOLEAN;
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 066 down requires zero registered Core cluster members';
    END IF;
    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 66
          AND migration_name = '066_runtime_v2_deadline_reconciler'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = new_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 066 down requires the exact current schema contract 66';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM runtime_schema_contracts
        WHERE schema_version = 65
          AND migration_name = '065_runtime_cancellation_lifecycle'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = old_digest
          AND NOT is_current
    ) INTO has_old_predecessor;

    IF has_old_predecessor AND (
        EXISTS (
            SELECT 1 FROM runtime_sessions
            WHERE runtime_contract_digest = new_digest
        )
        OR EXISTS (
            SELECT 1 FROM runtime_nodes
            WHERE status = 'revoked'
              AND runtime_contract_digest = new_digest
        )
    ) THEN
        RAISE EXCEPTION 'migration 066 down refuses immutable new-digest Session or revoked Node evidence';
    END IF;
END
$$;

DROP INDEX idx_run_attempts_runtime_v2_execution_due;
DROP INDEX idx_run_attempts_runtime_v2_offer_due;
DROP INDEX idx_runs_runtime_v2_run_deadline_due;
DROP INDEX idx_runs_runtime_v2_dispatch_due;

ALTER TABLE runtime_sessions
    DROP CONSTRAINT runtime_sessions_contract_current;
ALTER TABLE runtime_nodes
    DROP CONSTRAINT runtime_nodes_contract_current;

DO $$
DECLARE
    old_digest CONSTANT TEXT := 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f';
    new_digest CONSTANT TEXT := '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61';
    has_old_predecessor BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM runtime_schema_contracts
        WHERE schema_version = 65
          AND migration_name = '065_runtime_cancellation_lifecycle'
          AND runtime_contract_digest = old_digest
          AND NOT is_current
    ) INTO has_old_predecessor;

    IF has_old_predecessor THEN
        UPDATE runtime_nodes
        SET runtime_contract_digest = old_digest,
            updated_at = clock_timestamp()
        WHERE status <> 'revoked'
          AND runtime_contract_digest = new_digest;

        UPDATE runtime_schema_contracts
        SET is_current = FALSE
        WHERE schema_version = 66
          AND migration_name = '066_runtime_v2_deadline_reconciler'
          AND runtime_contract_digest = new_digest
          AND is_current;

        UPDATE runtime_schema_contracts
        SET is_current = TRUE
        WHERE schema_version = 65
          AND migration_name = '065_runtime_cancellation_lifecycle'
          AND runtime_contract_digest = old_digest
          AND NOT is_current;

        DELETE FROM runtime_schema_contracts
        WHERE schema_version = 66
          AND migration_name = '066_runtime_v2_deadline_reconciler'
          AND runtime_contract_digest = new_digest
          AND NOT is_current;
    ELSE
        UPDATE runtime_schema_contracts
        SET schema_version = 65,
            migration_name = '065_runtime_cancellation_lifecycle'
        WHERE schema_version = 66
          AND migration_name = '066_runtime_v2_deadline_reconciler'
          AND runtime_contract_digest = new_digest
          AND is_current;
    END IF;
END
$$;

-- Restore the exact pre-066 terminal Session immutability rule.
CREATE OR REPLACE FUNCTION enforce_runtime_session_identity_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime session history cannot be deleted';
    END IF;

    IF ROW(
        OLD.runtime_session_id,
        OLD.node_id,
        OLD.agent_id,
        OLD.credential_id,
        OLD.worker_id,
        OLD.session_epoch,
        OLD.device_certificate_serial,
        OLD.node_version,
        OLD.protocol_version,
        OLD.runtime_contract_id,
        OLD.runtime_contract_digest,
        OLD.features,
        OLD.connected_at,
        OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.runtime_session_id,
        NEW.node_id,
        NEW.agent_id,
        NEW.credential_id,
        NEW.worker_id,
        NEW.session_epoch,
        NEW.device_certificate_serial,
        NEW.node_version,
        NEW.protocol_version,
        NEW.runtime_contract_id,
        NEW.runtime_contract_digest,
        NEW.features,
        NEW.connected_at,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'runtime session immutable identity cannot change';
    END IF;

    IF NEW.heartbeat_at < OLD.heartbeat_at
       OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'runtime session clocks cannot move backwards';
    END IF;

    IF OLD.status IN ('revoked', 'closed')
       AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
        RAISE EXCEPTION 'terminal runtime session is immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

DO $$
DECLARE
    old_digest CONSTANT TEXT := 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f';
    new_digest CONSTANT TEXT := '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61';
    current_digest TEXT;
BEGIN
    SELECT runtime_contract_digest
    INTO STRICT current_digest
    FROM runtime_schema_contracts
    WHERE schema_version = 65
      AND migration_name = '065_runtime_cancellation_lifecycle'
      AND is_current;

    IF current_digest = old_digest THEN
        ALTER TABLE runtime_nodes
            ADD CONSTRAINT runtime_nodes_contract_current
                CHECK (
                    protocol_version = 2
                    AND runtime_contract_id = 'openlinker.runtime.v2'
                    AND runtime_contract_digest = 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f'
                );
        ALTER TABLE runtime_sessions
            ADD CONSTRAINT runtime_sessions_contract_current
                CHECK (
                    protocol_version = 2
                    AND runtime_contract_id = 'openlinker.runtime.v2'
                    AND runtime_contract_digest = 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f'
                );
    ELSIF current_digest = new_digest THEN
        ALTER TABLE runtime_nodes
            ADD CONSTRAINT runtime_nodes_contract_current
                CHECK (
                    protocol_version = 2
                    AND runtime_contract_id = 'openlinker.runtime.v2'
                    AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
                );
        ALTER TABLE runtime_sessions
            ADD CONSTRAINT runtime_sessions_contract_current
                CHECK (
                    protocol_version = 2
                    AND runtime_contract_id = 'openlinker.runtime.v2'
                    AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
                );
    ELSE
        RAISE EXCEPTION 'migration 066 down restored an unsupported predecessor digest';
    END IF;
END
$$;

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

COMMIT;
