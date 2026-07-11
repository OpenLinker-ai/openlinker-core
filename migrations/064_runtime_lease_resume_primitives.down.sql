BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.runtime.v2.migration.064', 0));

LOCK TABLE run_attempts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_resume_grants IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_resume_grants) THEN
        RAISE EXCEPTION 'migration 064 down refuses runtime resume grant evidence';
    END IF;
    IF EXISTS (SELECT 1 FROM run_attempts) THEN
        RAISE EXCEPTION 'migration 064 down refuses run Attempt slot evidence';
    END IF;
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 064 down requires zero registered Core cluster members';
    END IF;
    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 64
          AND migration_name = '064_runtime_lease_resume_primitives'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 064 down requires the exact current schema contract 64';
    END IF;
END
$$;

DROP TRIGGER runtime_resume_grants_identity_immutable ON runtime_resume_grants;
DROP FUNCTION enforce_runtime_resume_grant_identity();
DROP TABLE runtime_resume_grants;

DROP INDEX idx_run_attempts_unaccepted_session;

DROP TRIGGER run_attempts_slot_release_on_finish ON run_attempts;
DROP FUNCTION enforce_run_attempt_slot_release_on_finish();
DROP TRIGGER run_attempts_slot_evidence_forward_only ON run_attempts;
DROP FUNCTION enforce_run_attempt_slot_evidence();
DROP INDEX idx_run_attempts_active_runtime_session_slot;

ALTER TABLE run_attempts
    DROP CONSTRAINT run_attempts_active_runtime_session_fk,
    DROP CONSTRAINT run_attempts_slot_shape,
    DROP CONSTRAINT run_attempts_slot_time_order,
    DROP COLUMN slot_acquired_at,
    DROP COLUMN slot_released_at,
    DROP COLUMN active_runtime_session_id;

-- Restore the exact migration-063 principal function when rolling back 064.
CREATE OR REPLACE FUNCTION enforce_runtime_session_principal()
RETURNS TRIGGER AS $$
DECLARE
    node_record runtime_nodes%ROWTYPE;
    token_record agent_tokens%ROWTYPE;
    must_lock_principal BOOLEAN;
BEGIN
    must_lock_principal := TG_OP = 'INSERT';

    IF TG_OP = 'UPDATE' THEN
        must_lock_principal := (
            OLD.status NOT IN ('active', 'draining')
            AND NEW.status IN ('active', 'draining')
        ) OR ROW(
            NEW.node_id,
            NEW.agent_id,
            NEW.credential_id,
            NEW.device_certificate_serial,
            NEW.node_version,
            NEW.protocol_version,
            NEW.runtime_contract_id,
            NEW.runtime_contract_digest,
            NEW.features
        ) IS DISTINCT FROM ROW(
            OLD.node_id,
            OLD.agent_id,
            OLD.credential_id,
            OLD.device_certificate_serial,
            OLD.node_version,
            OLD.protocol_version,
            OLD.runtime_contract_id,
            OLD.runtime_contract_digest,
            OLD.features
        );
    END IF;

    IF NOT must_lock_principal THEN
        RETURN NEW;
    END IF;

    SELECT * INTO node_record
    FROM runtime_nodes
    WHERE node_id = NEW.node_id
    FOR SHARE;

    IF NOT FOUND
       OR node_record.device_certificate_serial <> NEW.device_certificate_serial THEN
        RAISE EXCEPTION 'runtime session node certificate identity mismatch';
    END IF;

    IF node_record.protocol_version <> NEW.protocol_version
       OR node_record.runtime_contract_id <> NEW.runtime_contract_id
       OR node_record.runtime_contract_digest <> NEW.runtime_contract_digest
       OR node_record.node_version <> NEW.node_version
       OR NOT (node_record.features @> NEW.features)
       OR NOT (NEW.features @> node_record.features) THEN
        RAISE EXCEPTION 'runtime session node contract identity mismatch';
    END IF;

    SELECT * INTO token_record
    FROM agent_tokens
    WHERE id = NEW.credential_id
      AND agent_id = NEW.agent_id
    FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'runtime session credential principal mismatch';
    END IF;

    IF NEW.status IN ('active', 'draining') THEN
        IF node_record.status = 'revoked'
           OR (NEW.status = 'active' AND node_record.status <> 'active') THEN
            RAISE EXCEPTION 'inactive runtime node cannot keep an active session';
        END IF;
        IF token_record.status <> 'active_runtime'
           OR token_record.revoked_at IS NOT NULL
           OR (
               token_record.expires_at IS NOT NULL
               AND token_record.expires_at <= clock_timestamp()
           ) THEN
            RAISE EXCEPTION 'inactive runtime credential cannot keep an active session';
        END IF;
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

UPDATE runtime_schema_contracts
SET schema_version = 63,
    migration_name = '063_reliable_runtime_v2'
WHERE schema_version = 64
  AND migration_name = '064_runtime_lease_resume_primitives'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
  AND is_current;

COMMIT;
