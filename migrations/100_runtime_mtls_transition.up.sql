BEGIN;
SET LOCAL search_path=public;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';

-- Additive tables preserve the exact 099 catalog on a pre-authorization
-- rollback and reapplication, including PostgreSQL attribute numbering.
CREATE TABLE runtime_cluster_authentication_policies (
 instance_id uuid PRIMARY KEY REFERENCES runtime_cluster_members(instance_id) ON DELETE CASCADE,
 authentication_policy_id text NOT NULL CHECK(authentication_policy_id='' OR authentication_policy_id ~ '^[a-f0-9]{64}$')
);
ALTER TABLE runtime_node_upgrade_operations
 DROP CONSTRAINT runtime_node_upgrade_operations_kind_check,
 DROP CONSTRAINT runtime_node_upgrade_operations_check1,
 ADD CONSTRAINT runtime_node_upgrade_kind CHECK (kind IN ('version_change','restart_after_drain','token_to_mtls','certificate_rotation')),
 ADD CONSTRAINT runtime_node_upgrade_versions CHECK ((kind='version_change' AND from_version<>target_version) OR (kind<>'version_change' AND from_version=target_version));
CREATE TABLE runtime_node_credential_authorizations (
 operation_id uuid PRIMARY KEY REFERENCES runtime_node_upgrade_operations(operation_id),
 credential_id uuid NOT NULL REFERENCES agent_tokens(id),
 from_public_key_thumbprint text NOT NULL CHECK(from_public_key_thumbprint ~ '^[a-f0-9]{64}$'),
 target_public_key_thumbprint text NOT NULL CHECK(target_public_key_thumbprint ~ '^[a-f0-9]{64}$')
);
CREATE INDEX runtime_node_credential_authorizations_credential_idx ON runtime_node_credential_authorizations(credential_id);
CREATE FUNCTION enforce_runtime_credential_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE operation_kind text; previous_key text; next_key text;
BEGIN
 SELECT op.kind,a.from_public_key_thumbprint,a.target_public_key_thumbprint
 INTO operation_kind,previous_key,next_key FROM runtime_node_upgrade_operations op
 LEFT JOIN runtime_node_credential_authorizations a USING(operation_id) WHERE op.operation_id=NEW.operation_id;
 IF (operation_kind IN ('token_to_mtls','certificate_rotation') AND previous_key IS NULL)
 OR (operation_kind NOT IN ('token_to_mtls','certificate_rotation') AND previous_key IS NOT NULL)
 OR (operation_kind='certificate_rotation' AND previous_key<>next_key)
 THEN RAISE EXCEPTION 'authentication authorization must exactly match its immutable operation'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER runtime_upgrade_credential_authorization AFTER INSERT ON runtime_node_upgrade_operations
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_runtime_credential_authorization();
CREATE CONSTRAINT TRIGGER runtime_credential_authorization_kind AFTER INSERT ON runtime_node_credential_authorizations
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION enforce_runtime_credential_authorization();

CREATE TABLE runtime_node_credential_operations (
 operation_id uuid PRIMARY KEY REFERENCES runtime_node_upgrade_operations(operation_id),
 certificate_serial text NOT NULL UNIQUE REFERENCES runtime_node_certificates(certificate_serial),
 completed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION enforce_runtime_credential_operation_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'runtime credential operation history is immutable'; END $$;
CREATE TRIGGER runtime_credential_authorization_history BEFORE UPDATE OR DELETE
 ON runtime_node_credential_authorizations FOR EACH ROW EXECUTE FUNCTION enforce_runtime_credential_operation_history();
CREATE TRIGGER runtime_credential_operation_history BEFORE UPDATE OR DELETE
 ON runtime_node_credential_operations FOR EACH ROW EXECUTE FUNCTION enforce_runtime_credential_operation_history();

-- Authorization is tied to immutable receipts, an exact CAS revision, the
-- first issued certificate and a quiescent Node. A transaction setting alone
-- never grants permission to change a key.
CREATE FUNCTION runtime_credential_transition_authorized(nid uuid,cid uuid,old_key text,new_key text)
RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS (
 SELECT 1 FROM runtime_node_upgrade_operations op
 JOIN runtime_node_credential_authorizations auth USING(operation_id)
 JOIN runtime_node_upgrade_state st ON st.node_id=op.node_id AND st.current_operation_id=op.operation_id AND st.revision=op.revision
 JOIN runtime_node_credential_operations done USING(operation_id)
 JOIN runtime_node_certificates cert ON cert.certificate_serial=done.certificate_serial
 JOIN runtime_nodes n ON n.node_id=op.node_id
 JOIN agent_tokens t ON t.id=auth.credential_id
 WHERE op.node_id=nid AND auth.credential_id=cid AND op.kind='token_to_mtls'
 AND auth.from_public_key_thumbprint=old_key AND auth.target_public_key_thumbprint=new_key
 AND cert.node_id=nid AND cert.public_key_thumbprint=new_key AND cert.revoked_at IS NULL
 AND cert.not_before<=clock_timestamp() AND cert.not_after>clock_timestamp()
 AND op.deadline_at>clock_timestamp() AND op.target_version=n.node_version
 AND n.status='draining' AND n.revoked_at IS NULL AND n.inflight=0
 AND t.status='active_runtime' AND t.revoked_at IS NULL AND t.scopes @> ARRAY['agent:pull']::text[]
 AND (t.expires_at IS NULL OR t.expires_at>clock_timestamp())
 AND n.features @> op.target_features AND op.target_features @> n.features
 AND current_setting('openlinker.runtime_credential_operation',TRUE)=op.operation_id::text
 AND NOT EXISTS (SELECT 1 FROM runtime_sessions s WHERE s.node_id=nid AND (s.status IN ('active','draining') OR s.inflight<>0))
 AND NOT EXISTS (SELECT 1 FROM run_attempts a WHERE (a.node_id=nid OR a.active_runtime_session_id IN (SELECT runtime_session_id FROM runtime_sessions WHERE node_id=nid)) AND
   (a.finished_at IS NULL OR (a.slot_acquired_at IS NOT NULL AND a.slot_released_at IS NULL)))
 AND NOT EXISTS (SELECT 1 FROM runtime_session_attachments a JOIN runtime_sessions s USING(runtime_session_id) WHERE s.node_id=nid AND a.detached_at IS NULL)
 );
$$;

CREATE FUNCTION enforce_runtime_binding_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'runtime credential binding is immutable'; END IF;
 IF to_jsonb(NEW) IS NOT DISTINCT FROM to_jsonb(OLD) THEN RETURN NEW; END IF;
 IF (to_jsonb(NEW)-ARRAY['binding_mode','public_key_thumbprint'])
       IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['binding_mode','public_key_thumbprint'])
    OR OLD.binding_mode<>'token_only' OR NEW.binding_mode<>'mtls'
    OR NOT runtime_credential_transition_authorized(OLD.node_id,OLD.credential_id,OLD.public_key_thumbprint,NEW.public_key_thumbprint)
    OR NOT EXISTS (SELECT 1 FROM runtime_nodes n WHERE n.node_id=NEW.node_id AND n.device_public_key_thumbprint=NEW.public_key_thumbprint)
 THEN RAISE EXCEPTION 'runtime credential binding requires an exact authentication transition'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_binding_identity BEFORE UPDATE OR DELETE ON runtime_node_bindings
 FOR EACH ROW EXECUTE FUNCTION enforce_runtime_binding_identity();

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
    ) AND NOT (
        ROW(NEW.node_id,NEW.device_certificate_serial,NEW.created_at)
            IS NOT DISTINCT FROM ROW(OLD.node_id,OLD.device_certificate_serial,OLD.created_at)
        AND OLD.status='draining' AND NEW.status='draining'
        AND EXISTS (
          SELECT 1 FROM runtime_node_bindings b
          WHERE b.node_id=OLD.node_id AND b.binding_mode='token_only'
            AND b.public_key_thumbprint=OLD.device_public_key_thumbprint
            AND runtime_credential_transition_authorized(OLD.node_id,b.credential_id,
                  OLD.device_public_key_thumbprint,NEW.device_public_key_thumbprint)
        )
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
        LEFT JOIN runtime_node_credential_authorizations auth USING(operation_id)
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
          AND (op.kind NOT IN ('token_to_mtls','certificate_rotation') OR
            (b.binding_mode='mtls' AND b.credential_id=auth.credential_id AND b.public_key_thumbprint=auth.target_public_key_thumbprint
             AND EXISTS (SELECT 1 FROM runtime_node_credential_operations done WHERE done.operation_id=op.operation_id)))
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
UPDATE runtime_schema_contracts SET is_current=FALSE WHERE is_current;
INSERT INTO runtime_schema_contracts(schema_version,migration_name,runtime_contract_id,runtime_contract_digest,is_current)
VALUES(100,'100_runtime_mtls_transition','openlinker.runtime.v2','4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481',TRUE);
COMMIT;
