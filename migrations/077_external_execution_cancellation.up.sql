BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.external-execution.migration.077', 0));

LOCK TABLE external_executions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE workflow_runs IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE workflow_run_steps IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE runtime_session_attachments IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_sessions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_nodes IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_wire_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runs IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_control IN SHARE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    old_current_digest CONSTANT TEXT := '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9';
    old_previous_digest CONSTANT TEXT := 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53';
    new_current_digest CONSTANT TEXT := '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481';
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 077 requires zero registered Core cluster members';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM runtime_cluster_control
        WHERE singleton_id = 1 AND mode = 'hard_maintenance'
    ) THEN
        RAISE EXCEPTION 'migration 077 requires hard maintenance';
    END IF;
    IF EXISTS (SELECT 1 FROM runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'migration 077 requires zero running Runs';
    END IF;
	IF EXISTS (SELECT 1 FROM runtime_sessions WHERE status = 'draining') THEN
		RAISE EXCEPTION 'migration 077 requires zero pre-077 draining Runtime Sessions';
	END IF;
    IF (
        SELECT COUNT(*) FROM runtime_schema_contracts
        WHERE schema_version = 76
          AND migration_name = '076_runtime_cancellation_terminal_reap'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = old_current_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 077 requires the exact current schema contract 76';
    END IF;
    IF EXISTS (
        SELECT 1 FROM runtime_schema_contracts
        WHERE schema_version = 77
           OR migration_name = '077_external_execution_cancellation'
    ) THEN
        RAISE EXCEPTION 'migration 077 found a conflicting historical schema contract 77';
    END IF;
    IF (SELECT COUNT(*) FROM runtime_wire_contracts
        WHERE runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = old_current_digest
          AND support_tier = 'current') <> 1
       OR (SELECT COUNT(*) FROM runtime_wire_contracts
           WHERE runtime_contract_id = 'openlinker.runtime.v2'
             AND runtime_contract_digest = old_previous_digest
             AND support_tier = 'previous') <> 1
       OR EXISTS (
           SELECT 1 FROM runtime_wire_contracts
           WHERE runtime_contract_digest = new_current_digest
       ) THEN
        RAISE EXCEPTION 'migration 077 requires the exact current/previous Runtime wire ring';
    END IF;
    IF EXISTS (
        SELECT 1 FROM runtime_nodes
        WHERE status IN ('active', 'draining')
          AND runtime_contract_digest NOT IN (old_current_digest, old_previous_digest)
    ) OR EXISTS (
        SELECT 1 FROM runtime_sessions
        WHERE status IN ('active', 'draining')
          AND runtime_contract_digest NOT IN (old_current_digest, old_previous_digest)
    ) THEN
        RAISE EXCEPTION 'migration 077 found an unknown live Runtime wire contract';
    END IF;
END
$$;

-- Publish the client-initiated drain wire generation while retaining the
-- immediately preceding attachment-fenced generation as previous.
DROP INDEX idx_runtime_wire_contracts_current;
DROP INDEX idx_runtime_wire_contracts_previous;
ALTER TABLE runtime_wire_contracts
    DROP CONSTRAINT runtime_wire_contracts_support_identity;

UPDATE runtime_wire_contracts
SET support_tier = CASE runtime_contract_digest
    WHEN '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9' THEN 'previous'
    WHEN 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53' THEN 'historical'
    ELSE support_tier
END;

INSERT INTO runtime_wire_contracts (
    runtime_contract_id, runtime_contract_digest, support_tier
) VALUES (
    'openlinker.runtime.v2',
    '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481',
    'current'
);

ALTER TABLE runtime_wire_contracts
    ADD CONSTRAINT runtime_wire_contracts_support_identity
        CHECK (
            runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (support_tier = 'current'
                 AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481')
                OR
                (support_tier = 'previous'
                 AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9')
                OR
                (support_tier = 'historical'
                 AND runtime_contract_digest NOT IN (
                    '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481',
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
                 ))
            )
        );

CREATE UNIQUE INDEX idx_runtime_wire_contracts_current
    ON runtime_wire_contracts ((1)) WHERE support_tier = 'current';
CREATE UNIQUE INDEX idx_runtime_wire_contracts_previous
    ON runtime_wire_contracts ((1)) WHERE support_tier = 'previous';

ALTER TABLE runtime_sessions
    DROP CONSTRAINT runtime_sessions_contract_current,
    DROP CONSTRAINT runtime_sessions_required_features;
ALTER TABLE runtime_nodes
    DROP CONSTRAINT runtime_nodes_contract_current,
    DROP CONSTRAINT runtime_nodes_required_features;

UPDATE runtime_schema_contracts
SET is_current = FALSE
WHERE schema_version = 76
  AND migration_name = '076_runtime_cancellation_terminal_reap'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
  AND is_current;

INSERT INTO runtime_schema_contracts (
    schema_version, migration_name, runtime_contract_id,
    runtime_contract_digest, is_current
) VALUES (
    77,
    '077_external_execution_cancellation',
    'openlinker.runtime.v2',
    '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481',
    TRUE
);

ALTER TABLE runtime_nodes
    ADD CONSTRAINT runtime_nodes_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (status IN ('active', 'draining')
                 AND runtime_contract_digest IN (
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9',
                    '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
                 ))
                OR status = 'revoked'
            )
        ),
    ADD CONSTRAINT runtime_nodes_required_features
        CHECK (
            runtime_v2_feature_set_is_valid(features)
            AND features @> ARRAY[
                'lease_fence', 'assignment_confirm', 'renew', 'resume',
                'event_ack', 'result_ack', 'cancel', 'persistent_spool'
            ]::TEXT[]
            AND (
                runtime_contract_digest <> '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
                OR features @> ARRAY['session_drain']::TEXT[]
            )
        );

ALTER TABLE runtime_sessions
    ADD CONSTRAINT runtime_sessions_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (status IN ('active', 'draining')
                 AND runtime_contract_digest IN (
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9',
                    '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
                 ))
                OR status IN ('offline', 'revoked', 'closed')
            )
        ),
    ADD CONSTRAINT runtime_sessions_required_features
        CHECK (
            runtime_v2_feature_set_is_valid(features)
            AND features @> ARRAY[
                'lease_fence', 'assignment_confirm', 'renew', 'resume',
                'event_ack', 'result_ack', 'cancel', 'persistent_spool'
            ]::TEXT[]
            AND (
                runtime_contract_digest <> '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
                OR features @> ARRAY['session_drain']::TEXT[]
            )
        );

ALTER TABLE runtime_sessions
    ADD COLUMN drain_requested_at TIMESTAMPTZ,
    ADD COLUMN drain_deadline_at TIMESTAMPTZ,
    ADD COLUMN drain_reason_code TEXT,
    ADD COLUMN resume_capacity INTEGER;

ALTER TABLE runtime_sessions
    ADD CONSTRAINT runtime_sessions_drain_evidence_consistent CHECK ((
        drain_requested_at IS NULL
        AND drain_deadline_at IS NULL
        AND drain_reason_code IS NULL
        AND resume_capacity IS NULL
        AND status <> 'draining'
    ) OR (
        drain_requested_at IS NOT NULL
        AND drain_deadline_at IS NOT NULL
        AND drain_reason_code = btrim(drain_reason_code)
        AND char_length(drain_reason_code) BETWEEN 1 AND 120
        AND resume_capacity BETWEEN 0 AND 1024
        AND capacity = 0
        AND status IN ('draining', 'offline', 'revoked', 'closed')
    ));

-- Node activation is unavailable to ordinary updates. The service sets this
-- transaction-local fence only after proving quiescence and identity validity.
CREATE OR REPLACE FUNCTION enforce_runtime_node_identity_and_lifecycle()
RETURNS TRIGGER AS $$
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
$$ LANGUAGE plpgsql;

CREATE TABLE external_execution_keys (
    caller_service_id TEXT NOT NULL,
    external_request_id UUID NOT NULL,
    actor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (caller_service_id, external_request_id),
    UNIQUE (caller_service_id, external_request_id, actor_user_id),
    CONSTRAINT external_execution_keys_caller_service_id_valid CHECK (
        caller_service_id = btrim(caller_service_id)
        AND length(caller_service_id) BETWEEN 1 AND 200
    )
);

INSERT INTO external_execution_keys (
    caller_service_id, external_request_id, actor_user_id, created_at, updated_at
)
SELECT caller_service_id, external_request_id, actor_user_id, created_at, updated_at
FROM external_executions;

ALTER TABLE external_executions
    DROP CONSTRAINT external_executions_start_state_valid,
    ADD COLUMN downstream_idempotency_key_hash BYTEA,
    ADD COLUMN downstream_creation_fingerprint BYTEA;

UPDATE external_executions
SET start_state = 'attached'
WHERE execution_id IS NOT NULL;

ALTER TABLE external_executions
    ADD CONSTRAINT external_executions_key_actor_fkey
        FOREIGN KEY (caller_service_id, external_request_id, actor_user_id)
        REFERENCES external_execution_keys (
            caller_service_id, external_request_id, actor_user_id
        ) ON DELETE RESTRICT,
    ADD CONSTRAINT external_executions_downstream_key_hash_valid CHECK (
        downstream_idempotency_key_hash IS NULL
        OR octet_length(downstream_idempotency_key_hash) = 32
    ),
    ADD CONSTRAINT external_executions_downstream_fingerprint_valid CHECK (
        downstream_creation_fingerprint IS NULL
        OR octet_length(downstream_creation_fingerprint) = 32
    ),
    ADD CONSTRAINT external_executions_downstream_identity_complete CHECK (
        (downstream_idempotency_key_hash IS NULL) =
        (downstream_creation_fingerprint IS NULL)
    ),
    ADD CONSTRAINT external_executions_start_state_valid CHECK ((
        (
            start_state = 'pending'
            AND start_token IS NULL
            AND start_lease_until IS NULL
            AND authorized_target_owner_id IS NULL
            AND rejection_code IS NULL
            AND execution_id IS NULL
        )
        OR (
            start_state = 'evaluating'
            AND start_token IS NOT NULL
            AND start_lease_until IS NOT NULL
            AND authorized_target_owner_id IS NULL
            AND rejection_code IS NULL
            AND execution_id IS NULL
        )
        OR (
            start_state = 'authorized'
            AND start_token IS NULL
            AND start_lease_until IS NULL
            AND authorized_target_owner_id IS NOT NULL
            AND rejection_code IS NULL
            AND execution_id IS NULL
        )
        OR (
            start_state = 'launching'
            AND start_token IS NOT NULL
            AND start_lease_until IS NOT NULL
            AND authorized_target_owner_id IS NOT NULL
            AND rejection_code IS NULL
            AND execution_id IS NULL
        )
        OR (
            start_state = 'attached'
            AND start_token IS NULL
            AND start_lease_until IS NULL
            AND rejection_code IS NULL
            AND execution_id IS NOT NULL
        )
        OR (
            start_state = 'rejected'
            AND start_token IS NULL
            AND start_lease_until IS NULL
            AND authorized_target_owner_id IS NULL
            AND rejection_code IN (
                'TARGET_UNAVAILABLE',
                'TARGET_CONTRACT_CHANGED',
                'DOWNSTREAM_IDENTITY_CONFLICT'
            )
            AND execution_id IS NULL
        )
        OR (
            start_state = 'canceled'
            AND start_token IS NULL
            AND start_lease_until IS NULL
            AND rejection_code IS NULL
        )
    ) IS TRUE);

CREATE TABLE external_execution_cancellations (
    id UUID PRIMARY KEY,
    caller_service_id TEXT NOT NULL,
    external_request_id UUID NOT NULL,
    actor_user_id UUID NOT NULL,
    reason_code TEXT NOT NULL CHECK (
        reason_code IN ('CALLER_REQUESTED', 'DEADLINE_EXCEEDED')
    ),
    state TEXT NOT NULL CHECK (
        state IN ('requested', 'stopping', 'stopped', 'unconfirmed', 'not_applied')
    ),
    execution_kind_snapshot TEXT CHECK (
        execution_kind_snapshot IS NULL
        OR execution_kind_snapshot IN ('run', 'workflow_run')
    ),
    execution_id_snapshot UUID,
    error_code TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    applied_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (caller_service_id, external_request_id),
    CONSTRAINT external_execution_cancellations_key_actor_fkey
        FOREIGN KEY (caller_service_id, external_request_id, actor_user_id)
        REFERENCES external_execution_keys (
            caller_service_id, external_request_id, actor_user_id
        ) ON DELETE RESTRICT,
    CONSTRAINT external_execution_cancellations_attachment_complete CHECK (
        (execution_kind_snapshot IS NULL) = (execution_id_snapshot IS NULL)
    ),
    CONSTRAINT external_execution_cancellations_timestamps_valid CHECK ((
        (state = 'requested' AND applied_at IS NULL AND finished_at IS NULL)
        OR (state = 'stopping' AND applied_at IS NOT NULL AND finished_at IS NULL)
        OR (
            state IN ('stopped', 'unconfirmed', 'not_applied')
            AND applied_at IS NOT NULL
            AND finished_at IS NOT NULL
        )
    ) IS TRUE),
    CONSTRAINT external_execution_cancellations_error_valid CHECK ((
        (state = 'unconfirmed' AND error_code = 'CANCEL_UNCONFIRMED')
        OR (state <> 'unconfirmed' AND error_code IS NULL)
    ) IS TRUE),
    CONSTRAINT external_execution_cancellations_time_order CHECK (
        (applied_at IS NULL OR applied_at >= requested_at)
        AND (finished_at IS NULL OR (applied_at IS NOT NULL AND finished_at >= applied_at))
    )
);

CREATE INDEX idx_external_execution_cancellations_reconcile
    ON external_execution_cancellations (state, requested_at, caller_service_id, external_request_id)
    WHERE state IN ('requested', 'stopping');

CREATE TABLE workflow_run_cancellations (
    workflow_run_id UUID PRIMARY KEY REFERENCES workflow_runs(id) ON DELETE RESTRICT,
    id UUID NOT NULL UNIQUE,
    actor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    reason_code TEXT NOT NULL CHECK (
        reason_code IN ('OWNER_CANCEL_REQUESTED', 'CALLER_REQUESTED', 'DEADLINE_EXCEEDED')
    ),
    state TEXT NOT NULL CHECK (
        state IN ('requested', 'stopping', 'stopped', 'unconfirmed', 'not_applied')
    ),
    error_code TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    applied_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT workflow_run_cancellations_timestamps_valid CHECK ((
        (state = 'requested' AND applied_at IS NULL AND finished_at IS NULL)
        OR (state = 'stopping' AND applied_at IS NOT NULL AND finished_at IS NULL)
        OR (
            state IN ('stopped', 'unconfirmed', 'not_applied')
            AND applied_at IS NOT NULL
            AND finished_at IS NOT NULL
        )
    ) IS TRUE),
    CONSTRAINT workflow_run_cancellations_error_valid CHECK ((
        (state = 'unconfirmed' AND error_code = 'CANCEL_UNCONFIRMED')
        OR (state <> 'unconfirmed' AND error_code IS NULL)
    ) IS TRUE),
    CONSTRAINT workflow_run_cancellations_time_order CHECK (
        (applied_at IS NULL OR applied_at >= requested_at)
        AND (finished_at IS NULL OR (applied_at IS NOT NULL AND finished_at >= applied_at))
    )
);

CREATE INDEX idx_workflow_run_cancellations_reconcile
    ON workflow_run_cancellations (state, requested_at, workflow_run_id)
    WHERE state IN ('requested', 'stopping');

CREATE TABLE workflow_step_launches (
    workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE RESTRICT,
    workflow_node_id UUID NOT NULL REFERENCES workflow_nodes(id) ON DELETE RESTRICT,
    workflow_run_step_id UUID REFERENCES workflow_run_steps(id) ON DELETE SET NULL,
    node_key TEXT NOT NULL,
    launch_token UUID NOT NULL,
    idempotency_key_hash BYTEA NOT NULL CHECK (octet_length(idempotency_key_hash) = 32),
    creation_fingerprint BYTEA NOT NULL CHECK (octet_length(creation_fingerprint) = 32),
    state TEXT NOT NULL CHECK (state IN ('claimed', 'created', 'attached', 'invalidated')),
    run_id UUID REFERENCES runs(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_run_id, workflow_node_id),
    UNIQUE (workflow_run_step_id),
    CONSTRAINT workflow_step_launches_node_key_valid CHECK (
        node_key = btrim(node_key) AND length(node_key) BETWEEN 1 AND 80
    ),
    CONSTRAINT workflow_step_launches_state_valid CHECK ((
        (state = 'claimed' AND run_id IS NULL)
        OR (state IN ('created', 'attached') AND run_id IS NOT NULL)
        OR (state = 'invalidated' AND run_id IS NULL)
    ) IS TRUE)
);

CREATE INDEX idx_workflow_step_launches_reconcile
    ON workflow_step_launches (workflow_run_id, state, workflow_node_id)
    WHERE state IN ('claimed', 'created');

COMMIT;
