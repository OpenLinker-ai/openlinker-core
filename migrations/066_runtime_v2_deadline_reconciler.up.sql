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
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 066 requires zero registered Core cluster members';
    END IF;

    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 65
          AND migration_name = '065_runtime_cancellation_lifecycle'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest IN (old_digest, new_digest)
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 066 requires schema contract 65 with the deployed or current runtime digest';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runtime_nodes
        WHERE runtime_contract_id <> 'openlinker.runtime.v2'
           OR runtime_contract_digest NOT IN (old_digest, new_digest)
    ) OR EXISTS (
        SELECT 1
        FROM runtime_sessions
        WHERE runtime_contract_id <> 'openlinker.runtime.v2'
           OR runtime_contract_digest NOT IN (old_digest, new_digest)
    ) THEN
        RAISE EXCEPTION 'migration 066 found an unknown runtime contract identity';
    END IF;
END
$$;

-- An existing Session identity is immutable. Old-digest Sessions therefore
-- become closed history; they are never rewritten to impersonate a new
-- handshake. Closing attachments first preserves the global attachment rule.
UPDATE runtime_session_attachments attachment
SET detached_at = clock_timestamp(),
    disconnect_reason = 'runtime contract digest cutover'
FROM runtime_sessions session
WHERE session.runtime_session_id = attachment.runtime_session_id
  AND session.runtime_contract_digest = 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f'
  AND session.status IN ('active', 'draining')
  AND attachment.detached_at IS NULL;

UPDATE runtime_sessions
SET status = 'closed',
    attached_core_instance_id = NULL,
    disconnected_at = COALESCE(disconnected_at, clock_timestamp()),
    heartbeat_at = GREATEST(heartbeat_at, clock_timestamp()),
    updated_at = clock_timestamp()
WHERE runtime_contract_digest = 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f'
  AND status IN ('active', 'draining');

-- Flush attachment/session consistency before changing table constraints;
-- PostgreSQL refuses ALTER TABLE while deferred trigger events are pending.
SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

ALTER TABLE runtime_sessions
    DROP CONSTRAINT runtime_sessions_contract_current;
ALTER TABLE runtime_nodes
    DROP CONSTRAINT runtime_nodes_contract_current;

-- Fresh databases already carry the new digest in the schema-65 row, so that
-- row advances in place (the contract pair is unique). A deployed old-digest
-- database keeps schema 65 as historical FK evidence and inserts schema 66.
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

    IF current_digest = new_digest THEN
        UPDATE runtime_schema_contracts
        SET schema_version = 66,
            migration_name = '066_runtime_v2_deadline_reconciler'
        WHERE schema_version = 65
          AND migration_name = '065_runtime_cancellation_lifecycle'
          AND runtime_contract_digest = new_digest
          AND is_current;
    ELSIF current_digest = old_digest THEN
        UPDATE runtime_schema_contracts
        SET is_current = FALSE
        WHERE schema_version = 65
          AND migration_name = '065_runtime_cancellation_lifecycle'
          AND runtime_contract_digest = old_digest
          AND is_current;

        INSERT INTO runtime_schema_contracts (
            schema_version,
            migration_name,
            runtime_contract_id,
            runtime_contract_digest,
            is_current
        ) VALUES (
            66,
            '066_runtime_v2_deadline_reconciler',
            'openlinker.runtime.v2',
            new_digest,
            TRUE
        );
    ELSE
        RAISE EXCEPTION 'migration 066 encountered an unsupported predecessor digest';
    END IF;
END
$$;

-- Revoked Nodes are immutable history. Every non-revoked Node can advertise
-- only the new digest after the old Sessions have been detached and closed.
UPDATE runtime_nodes
SET runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61',
    updated_at = clock_timestamp()
WHERE status <> 'revoked'
  AND runtime_contract_digest = 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f';

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

ALTER TABLE runtime_nodes
    ADD CONSTRAINT runtime_nodes_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (
                    status IN ('active', 'draining')
                    AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
                )
                OR (
                    status = 'revoked'
                    AND runtime_contract_digest IN (
                        'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
                        '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
                    )
                )
            )
        );

ALTER TABLE runtime_sessions
    ADD CONSTRAINT runtime_sessions_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (
                    status IN ('active', 'draining')
                    AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
                )
                OR (
                    status IN ('offline', 'revoked', 'closed')
                    AND runtime_contract_digest IN (
                        'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
                        '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
                    )
                )
            )
        );

-- A closed/revoked Session remains immutable identity history, but an Attempt
-- that acquired capacity before disconnect still owns one outstanding slot.
-- Its fenced release CAS must be able to move inflight toward zero exactly
-- once without reopening or rewriting any other terminal Session fact.
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
        IF NEW.inflight = OLD.inflight - 1
           AND (
               to_jsonb(NEW) - ARRAY['inflight', 'updated_at']::TEXT[]
           ) IS NOT DISTINCT FROM (
               to_jsonb(OLD) - ARRAY['inflight', 'updated_at']::TEXT[]
           ) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'terminal runtime session is immutable except for a fenced slot release';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- Deadline scans are bounded, but these partial indexes keep their discovery
-- cost proportional to due runtime-v2 work rather than total Run history.
CREATE INDEX idx_runs_runtime_v2_dispatch_due
    ON runs (dispatch_deadline_at, id)
    WHERE runtime_contract_id = 'openlinker.runtime.v2'
      AND status = 'running'
      AND cancel_request_id IS NULL
      AND dispatch_state IN ('pending', 'offered', 'retry_wait');

CREATE INDEX idx_runs_runtime_v2_run_deadline_due
    ON runs (run_deadline_at, id)
    WHERE runtime_contract_id = 'openlinker.runtime.v2'
      AND status = 'running'
      AND cancel_request_id IS NULL;

CREATE INDEX idx_run_attempts_runtime_v2_offer_due
    ON run_attempts (offer_expires_at, run_id, id)
    WHERE finished_at IS NULL
      AND accepted_at IS NULL;

CREATE INDEX idx_run_attempts_runtime_v2_execution_due
    ON run_attempts (lease_expires_at, attempt_deadline_at, run_id, id)
    WHERE finished_at IS NULL
      AND accepted_at IS NOT NULL;

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

COMMIT;
