BEGIN;
SET LOCAL search_path=public;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';
-- Once authorization has fenced the old mode, rolling back to an older Core
-- would permit an authentication downgrade, even if issuance never completed.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM runtime_node_upgrade_operations WHERE kind IN ('token_to_mtls','certificate_rotation'))
 THEN RAISE EXCEPTION 'authentication operations exist; downgrade requires coordinated recovery, never automatic rollback'; END IF;
END $$;
CREATE OR REPLACE FUNCTION public.enforce_runtime_node_identity_and_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    is_guarded_activation BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime node enrollment history cannot be deleted';
    END IF;

    IF ROW(
        NEW.node_id,
        NEW.device_certificate_serial,
        NEW.device_public_key_thumbprint,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.node_id,
        OLD.device_certificate_serial,
        OLD.device_public_key_thumbprint,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'runtime node device identity is immutable';
    END IF;

    -- A wire-generation switch is legal only while the Node has no live
    -- Sessions, and the target generation must use exactly its Server-owned
    -- feature set. Steady-state generations may advertise extensions, but
    -- extensions cannot be silently carried across an adapter boundary.
    IF OLD.runtime_contract_digest IS DISTINCT FROM NEW.runtime_contract_digest THEN
        IF EXISTS (
            SELECT 1 FROM runtime_sessions
            WHERE node_id = OLD.node_id
              AND (
                  status IN ('active', 'draining')
                  OR (
                      status = 'offline'
                      AND runtime_contract_digest <> NEW.runtime_contract_digest
                  )
              )
        ) THEN
            RAISE EXCEPTION 'runtime node generation cannot change with live or resumable sessions';
        END IF;

        IF NEW.runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481' THEN
            IF NOT (
                NEW.features @> ARRAY[
                    'lease_fence', 'assignment_confirm', 'renew', 'resume',
                    'event_ack', 'result_ack', 'cancel', 'persistent_spool',
                    'session_drain'
                ]::TEXT[]
                AND ARRAY[
                    'lease_fence', 'assignment_confirm', 'renew', 'resume',
                    'event_ack', 'result_ack', 'cancel', 'persistent_spool',
                    'session_drain'
                ]::TEXT[] @> NEW.features
            ) THEN
                RAISE EXCEPTION 'runtime node target generation features must be exact';
            END IF;
        ELSIF NEW.runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9' THEN
            IF NOT (
                NEW.features @> ARRAY[
                    'lease_fence', 'assignment_confirm', 'renew', 'resume',
                    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
                ]::TEXT[]
                AND ARRAY[
                    'lease_fence', 'assignment_confirm', 'renew', 'resume',
                    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
                ]::TEXT[] @> NEW.features
            ) THEN
                RAISE EXCEPTION 'runtime node target generation features must be exact';
            END IF;
        ELSE
            RAISE EXCEPTION 'runtime node target generation is unsupported';
        END IF;
    END IF;

    is_guarded_activation := OLD.status = 'draining'
        AND NEW.status = 'active'
        AND NEW.draining_at IS NULL
        AND NEW.revoked_at IS NULL
        AND current_setting('openlinker.runtime_node_activation', TRUE)
            IS NOT DISTINCT FROM OLD.node_id::TEXT;

    IF (OLD.status = 'draining' AND NEW.status NOT IN ('draining', 'revoked') AND NOT is_guarded_activation)
       OR (OLD.status = 'revoked' AND NEW.status <> 'revoked') THEN
        RAISE EXCEPTION 'runtime node lifecycle cannot move backwards';
    END IF;

    IF OLD.draining_at IS NOT NULL
       AND NEW.draining_at IS DISTINCT FROM OLD.draining_at
       AND NOT is_guarded_activation THEN
        RAISE EXCEPTION 'runtime node draining evidence is immutable';
    END IF;

    IF OLD.status = 'revoked'
       AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
        RAISE EXCEPTION 'revoked runtime node is immutable';
    END IF;

    RETURN NEW;
END
$$;
CREATE OR REPLACE FUNCTION public.enforce_runtime_node_upgrade_admission() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    op_id uuid;
    is_new boolean;
BEGIN
    IF NEW.status NOT IN ('active','draining') THEN RETURN NEW; END IF;
    is_new := TG_OP = 'INSERT';
    IF NOT is_new THEN
        IF OLD.status IN ('active','draining') THEN RETURN NEW; END IF;
    END IF;
    SELECT current_operation_id INTO op_id FROM runtime_node_upgrade_state WHERE node_id=NEW.node_id;
    IF op_id IS NULL THEN RETURN NEW; END IF;
    IF current_setting('openlinker.runtime_node_upgrade_admission',TRUE) IS DISTINCT FROM op_id::text
       OR NEW.status <> 'draining' OR NEW.capacity <> 0 OR NOT EXISTS (
        SELECT 1 FROM runtime_node_upgrade_admissions a
        JOIN runtime_node_upgrade_operations op USING(operation_id)
        JOIN runtime_node_upgrade_state st ON st.current_operation_id=op.operation_id AND st.node_id=op.node_id AND st.revision=op.revision
        JOIN runtime_node_bindings b ON b.credential_id=a.credential_id AND b.node_id=op.node_id AND b.agent_id=a.agent_id
        JOIN runtime_sessions source ON source.runtime_session_id=a.source_session_id
        JOIN runtime_nodes n ON n.node_id=op.node_id
        WHERE op.operation_id=op_id AND op.node_id=NEW.node_id AND op.target_version=NEW.node_version
          AND op.deadline_at>clock_timestamp() AND a.agent_id=NEW.agent_id AND a.worker_id=NEW.worker_id
          AND a.credential_id=NEW.credential_id AND a.minimum_epoch<NEW.session_epoch
          AND ((is_new AND a.last_admitted_epoch<NEW.session_epoch) OR (NOT is_new AND a.last_admitted_epoch=NEW.session_epoch))
          AND source.node_id=NEW.node_id AND source.agent_id=NEW.agent_id AND source.worker_id=NEW.worker_id
          AND source.credential_id=NEW.credential_id AND source.session_epoch=a.minimum_epoch AND source.status='closed'
          AND n.status='draining' AND n.revoked_at IS NULL AND n.node_version=NEW.node_version
          AND n.device_certificate_serial=NEW.device_certificate_serial AND n.device_public_key_thumbprint=b.public_key_thumbprint
          AND NEW.protocol_version=n.protocol_version AND NEW.runtime_contract_id=n.runtime_contract_id AND NEW.runtime_contract_digest=n.runtime_contract_digest
          AND NEW.features @> n.features AND n.features @> NEW.features
          AND NEW.features @> COALESCE(op.target_features,source.features) AND COALESCE(op.target_features,source.features) @> NEW.features
    ) THEN
        RAISE EXCEPTION 'runtime session requires a current controlled admission';
    END IF;
    RETURN NEW;
END $$;
DROP TABLE runtime_cluster_authentication_policies;
DROP TRIGGER runtime_upgrade_credential_authorization ON runtime_node_upgrade_operations;
DROP TABLE runtime_node_credential_authorizations;
DROP FUNCTION enforce_runtime_credential_authorization();
DROP TRIGGER runtime_binding_identity ON runtime_node_bindings;
DROP FUNCTION enforce_runtime_binding_identity();
DROP FUNCTION runtime_credential_transition_authorized(uuid,uuid,text,text);
DROP TABLE runtime_node_credential_operations;
DROP FUNCTION enforce_runtime_credential_operation_history();
ALTER TABLE runtime_node_upgrade_operations
 DROP CONSTRAINT runtime_node_upgrade_kind,
 DROP CONSTRAINT runtime_node_upgrade_versions,
 ADD CONSTRAINT runtime_node_upgrade_operations_kind_check CHECK(kind IN ('version_change','restart_after_drain')),
 ADD CONSTRAINT runtime_node_upgrade_operations_check1 CHECK((kind='version_change' AND from_version<>target_version) OR (kind='restart_after_drain' AND from_version=target_version));
DELETE FROM runtime_schema_contracts WHERE schema_version=100;
UPDATE runtime_schema_contracts SET is_current=(schema_version=99);
COMMIT;
