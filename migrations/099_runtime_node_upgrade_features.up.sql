BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- NULL pairs preserve pre-099 immutable operations without rewriting history.
ALTER TABLE public.runtime_node_upgrade_operations
    ADD COLUMN from_features text[],
    ADD COLUMN target_features text[],
    ADD CONSTRAINT runtime_node_upgrade_feature_pair CHECK (
      (from_features IS NULL AND target_features IS NULL) OR
      (from_features IS NOT NULL AND target_features IS NOT NULL
       AND cardinality(from_features)>0 AND cardinality(target_features)>0
       AND array_position(from_features,NULL) IS NULL AND array_position(target_features,NULL) IS NULL
       AND from_features @> ARRAY['session_drain']::text[] AND target_features @> ARRAY['session_drain']::text[]
       AND (kind='version_change' OR (from_features @> target_features AND target_features @> from_features))));

CREATE OR REPLACE FUNCTION public.enforce_runtime_node_upgrade_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- Heartbeat and slot updates do not read upgrade tables on the hot path.
    IF ROW(NEW.node_version,NEW.status,NEW.protocol_version,NEW.runtime_contract_id,NEW.runtime_contract_digest,NEW.features)
       IS NOT DISTINCT FROM ROW(OLD.node_version,OLD.status,OLD.protocol_version,OLD.runtime_contract_id,OLD.runtime_contract_digest,OLD.features) THEN
        RETURN NEW;
    END IF;
    IF NEW.node_version IS DISTINCT FROM OLD.node_version AND NOT (
        OLD.status = 'draining' AND NEW.status = 'draining' AND EXISTS (
            SELECT 1 FROM runtime_node_upgrade_state state
            JOIN runtime_node_upgrade_operations op ON op.operation_id = state.current_operation_id
            WHERE state.node_id = OLD.node_id AND op.node_id = OLD.node_id
              AND state.revision = op.revision AND op.kind = 'version_change'
              AND op.from_version = OLD.node_version AND op.target_version = NEW.node_version
              AND op.deadline_at > clock_timestamp()
              AND current_setting('openlinker.runtime_node_upgrade', TRUE) = op.operation_id::text
              AND OLD.features @> COALESCE(op.from_features,OLD.features) AND COALESCE(op.from_features,OLD.features) @> OLD.features
              AND NEW.features @> COALESCE(op.target_features,OLD.features) AND COALESCE(op.target_features,OLD.features) @> NEW.features
        )
    ) THEN
        RAISE EXCEPTION 'runtime node version requires a controlled upgrade';
    END IF;
    IF EXISTS (SELECT 1 FROM runtime_node_upgrade_state
               WHERE node_id = OLD.node_id AND current_operation_id IS NOT NULL)
       AND ROW(NEW.protocol_version, NEW.runtime_contract_id, NEW.runtime_contract_digest)
           IS DISTINCT FROM ROW(OLD.protocol_version, OLD.runtime_contract_id, OLD.runtime_contract_digest) THEN
        RAISE EXCEPTION 'runtime node upgrade cannot change the wire contract';
    END IF;
    IF NEW.features IS DISTINCT FROM OLD.features
       AND EXISTS (SELECT 1 FROM runtime_node_upgrade_state WHERE node_id=OLD.node_id AND current_operation_id IS NOT NULL)
       AND NOT (OLD.status='draining' AND NEW.status='draining' AND NEW.node_version<>OLD.node_version AND EXISTS (
         SELECT 1 FROM runtime_node_upgrade_state st JOIN runtime_node_upgrade_operations op ON op.operation_id=st.current_operation_id
         WHERE st.node_id=OLD.node_id AND op.node_id=OLD.node_id AND op.revision=st.revision AND op.kind='version_change'
           AND op.from_version=OLD.node_version AND op.target_version=NEW.node_version AND op.deadline_at>clock_timestamp()
           AND current_setting('openlinker.runtime_node_upgrade',TRUE)=op.operation_id::text
           AND OLD.features @> op.from_features AND op.from_features @> OLD.features
           AND NEW.features @> op.target_features AND op.target_features @> NEW.features)) THEN
        RAISE EXCEPTION 'runtime node features require an exact controlled upgrade';
    END IF;
    IF NEW.status <> 'draining' THEN
        UPDATE runtime_node_upgrade_state SET current_operation_id = NULL
        WHERE node_id = OLD.node_id AND current_operation_id IS NOT NULL;
    END IF;
    RETURN NEW;
END $$;

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

-- The old Core does not understand feature authorizations and must fail readiness.
UPDATE public.runtime_schema_contracts SET is_current=FALSE WHERE is_current;
INSERT INTO public.runtime_schema_contracts
    (schema_version,migration_name,runtime_contract_id,runtime_contract_digest,is_current)
VALUES (99,'099_runtime_node_upgrade_features','openlinker.runtime.v2',
        '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481',TRUE);
COMMIT;
