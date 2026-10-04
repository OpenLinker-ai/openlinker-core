BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Authorization belongs to Core. These rows contain no Runtime token or payload.
CREATE TABLE public.runtime_node_upgrade_operations (
    operation_id uuid PRIMARY KEY,
    node_id uuid NOT NULL REFERENCES public.runtime_nodes(node_id),
    actor_id uuid NOT NULL REFERENCES public.users(id),
    request_fingerprint text NOT NULL CHECK (request_fingerprint ~ '^[a-f0-9]{64}$'),
    kind text NOT NULL CHECK (kind IN ('version_change', 'restart_after_drain')),
    from_version text NOT NULL,
    target_version text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    admission_count integer NOT NULL CHECK (admission_count > 0 AND admission_count <= 10000),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deadline_at timestamptz NOT NULL,
    UNIQUE (node_id, revision),
    -- PostgreSQL names this first table CHECK runtime_node_upgrade_operations_check;
    -- the Admin API maps its failure to OPERATION_EXPIRED (covered by a lock-wait test).
    CHECK (deadline_at > created_at AND deadline_at <= created_at + INTERVAL '15 minutes'),
    CHECK ((kind = 'version_change' AND from_version <> target_version)
        OR (kind = 'restart_after_drain' AND from_version = target_version))
);
CREATE TABLE public.runtime_node_upgrade_state (
    node_id uuid PRIMARY KEY REFERENCES public.runtime_nodes(node_id),
    revision bigint NOT NULL CHECK (revision > 0),
    current_operation_id uuid REFERENCES public.runtime_node_upgrade_operations(operation_id)
);
CREATE TABLE public.runtime_node_upgrade_admissions (
    operation_id uuid NOT NULL REFERENCES public.runtime_node_upgrade_operations(operation_id),
    agent_id uuid NOT NULL REFERENCES public.agents(id),
    worker_id text NOT NULL,
    credential_id uuid NOT NULL REFERENCES public.agent_tokens(id),
    source_session_id uuid NOT NULL REFERENCES public.runtime_sessions(runtime_session_id),
    minimum_epoch bigint NOT NULL CHECK (minimum_epoch > 0),
    last_admitted_epoch bigint NOT NULL,
    PRIMARY KEY (operation_id, agent_id, worker_id),
    CHECK (last_admitted_epoch >= minimum_epoch)
);
-- Reuse runtime_sessions_identity_unique's leading Node key. Do not build a
-- blocking index over unbounded historical data inside this migration.

CREATE FUNCTION public.enforce_runtime_node_upgrade_history() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_TABLE_NAME = 'runtime_node_upgrade_operations' THEN
        RAISE EXCEPTION 'runtime node upgrade history is immutable';
    END IF;
    IF TG_TABLE_NAME = 'runtime_node_upgrade_admissions' THEN
        IF (to_jsonb(NEW) - 'last_admitted_epoch') IS DISTINCT FROM (to_jsonb(OLD) - 'last_admitted_epoch')
           OR NEW.last_admitted_epoch <= OLD.last_admitted_epoch THEN
            RAISE EXCEPTION 'runtime node upgrade admission can only advance its epoch';
        END IF;
    ELSE
        IF NEW.node_id <> OLD.node_id OR NEW.revision < OLD.revision
           OR NEW.revision > OLD.revision + 1
           OR (NEW.revision = OLD.revision AND NEW.current_operation_id IS NOT NULL
               AND NEW.current_operation_id IS DISTINCT FROM OLD.current_operation_id) THEN
            RAISE EXCEPTION 'runtime node upgrade revision cannot be reused';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER runtime_node_upgrade_operations_history BEFORE UPDATE OR DELETE
ON public.runtime_node_upgrade_operations FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_upgrade_history();
CREATE TRIGGER runtime_node_upgrade_state_history BEFORE UPDATE OR DELETE
ON public.runtime_node_upgrade_state FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_upgrade_history();
CREATE TRIGGER runtime_node_upgrade_admissions_history BEFORE UPDATE OR DELETE
ON public.runtime_node_upgrade_admissions FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_upgrade_history();

CREATE FUNCTION public.enforce_runtime_node_upgrade_guard() RETURNS trigger
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
        )
    ) THEN
        RAISE EXCEPTION 'runtime node version requires a controlled upgrade';
    END IF;
    IF EXISTS (SELECT 1 FROM runtime_node_upgrade_state
               WHERE node_id = OLD.node_id AND current_operation_id IS NOT NULL)
       AND ROW(NEW.protocol_version, NEW.runtime_contract_id, NEW.runtime_contract_digest, NEW.features)
           IS DISTINCT FROM ROW(OLD.protocol_version, OLD.runtime_contract_id, OLD.runtime_contract_digest, OLD.features) THEN
        RAISE EXCEPTION 'runtime node upgrade cannot change the wire contract';
    END IF;
    IF NEW.status <> 'draining' THEN
        UPDATE runtime_node_upgrade_state SET current_operation_id = NULL
        WHERE node_id = OLD.node_id AND current_operation_id IS NOT NULL;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER runtime_nodes_upgrade_guard BEFORE UPDATE ON public.runtime_nodes
FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_upgrade_guard();

-- A stale Core or an ordinary successor query cannot bypass an outstanding
-- operation. Only the controlled transaction sets this local authorization;
-- the DB independently verifies identity, version, deadline and epoch.
CREATE FUNCTION public.enforce_runtime_node_upgrade_admission() RETURNS trigger
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
        WHERE op.operation_id=op_id AND op.node_id=NEW.node_id AND op.target_version=NEW.node_version
          AND op.deadline_at>clock_timestamp() AND a.agent_id=NEW.agent_id AND a.worker_id=NEW.worker_id
          AND a.credential_id=NEW.credential_id AND a.minimum_epoch<NEW.session_epoch
          AND ((is_new AND a.last_admitted_epoch<NEW.session_epoch) OR (NOT is_new AND a.last_admitted_epoch=NEW.session_epoch))
          AND source.node_id=NEW.node_id AND source.agent_id=NEW.agent_id AND source.worker_id=NEW.worker_id
          AND source.credential_id=NEW.credential_id AND source.session_epoch=a.minimum_epoch AND source.status='closed'
    ) THEN
        RAISE EXCEPTION 'runtime session requires a current controlled admission';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER runtime_sessions_upgrade_admission BEFORE INSERT OR UPDATE OF status
ON public.runtime_sessions FOR EACH ROW EXECUTE FUNCTION public.enforce_runtime_node_upgrade_admission();

-- Old Core must fail readiness: it does not understand the new durable fences.
UPDATE public.runtime_schema_contracts SET is_current = FALSE WHERE is_current;
INSERT INTO public.runtime_schema_contracts
    (schema_version, migration_name, runtime_contract_id, runtime_contract_digest, is_current)
VALUES (95, '095_runtime_node_upgrade', 'openlinker.runtime.v2',
        '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481', TRUE);
COMMIT;
