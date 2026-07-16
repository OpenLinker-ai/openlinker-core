BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.external-execution.migration.077', 0));

LOCK TABLE external_execution_keys IN ACCESS EXCLUSIVE MODE;
LOCK TABLE external_executions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE external_execution_cancellations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE workflow_run_cancellations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE workflow_step_launches IN ACCESS EXCLUSIVE MODE;
LOCK TABLE workflow_runs IN SHARE ROW EXCLUSIVE MODE;
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
        RAISE EXCEPTION 'migration 077 rollback requires zero registered Core cluster members';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM runtime_cluster_control
        WHERE singleton_id = 1 AND mode = 'hard_maintenance'
    ) THEN
        RAISE EXCEPTION 'migration 077 rollback requires hard maintenance';
    END IF;
    IF EXISTS (SELECT 1 FROM runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'migration 077 rollback requires zero running Runs';
    END IF;
    IF (
        SELECT COUNT(*) FROM runtime_schema_contracts
        WHERE schema_version = 77
          AND migration_name = '077_external_execution_cancellation'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = new_current_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 077 rollback requires the exact current schema contract 77';
    END IF;
	IF (
		SELECT COUNT(*) FROM runtime_schema_contracts
		WHERE schema_version = 76
		  AND migration_name = '076_runtime_cancellation_terminal_reap'
		  AND runtime_contract_id = 'openlinker.runtime.v2'
		  AND runtime_contract_digest = old_current_digest
		  AND NOT is_current
	) <> 1 THEN
		RAISE EXCEPTION 'migration 077 rollback requires the exact historical schema contract 76';
	END IF;
	IF (SELECT COUNT(*) FROM runtime_wire_contracts
		WHERE runtime_contract_id = 'openlinker.runtime.v2'
		  AND runtime_contract_digest = new_current_digest
		  AND support_tier = 'current') <> 1
	   OR (SELECT COUNT(*) FROM runtime_wire_contracts
		   WHERE runtime_contract_id = 'openlinker.runtime.v2'
		     AND runtime_contract_digest = old_current_digest
		     AND support_tier = 'previous') <> 1
	   OR (SELECT COUNT(*) FROM runtime_wire_contracts
		   WHERE runtime_contract_id = 'openlinker.runtime.v2'
		     AND runtime_contract_digest = old_previous_digest
		     AND support_tier = 'historical') <> 1 THEN
		RAISE EXCEPTION 'migration 077 rollback requires the exact current/previous/historical Runtime wire state';
	END IF;
    IF EXISTS (
        SELECT 1 FROM runtime_nodes WHERE runtime_contract_digest = new_current_digest
    ) OR EXISTS (
        SELECT 1 FROM runtime_sessions WHERE runtime_contract_digest = new_current_digest
    ) THEN
        RAISE EXCEPTION 'migration 077 rollback refuses current drain-capable Runtime identities';
    END IF;
    IF EXISTS (
        SELECT 1 FROM runtime_sessions
        WHERE drain_requested_at IS NOT NULL
           OR drain_deadline_at IS NOT NULL
           OR drain_reason_code IS NOT NULL
           OR resume_capacity IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'migration 077 rollback refuses Runtime Session drain evidence';
    END IF;
    IF EXISTS (SELECT 1 FROM external_execution_cancellations) THEN
        RAISE EXCEPTION 'migration 077 rollback refuses external cancellation evidence';
    END IF;
    IF EXISTS (SELECT 1 FROM workflow_run_cancellations) THEN
        RAISE EXCEPTION 'migration 077 rollback refuses workflow cancellation evidence';
    END IF;
    IF EXISTS (SELECT 1 FROM workflow_step_launches) THEN
        RAISE EXCEPTION 'migration 077 rollback refuses workflow child launch evidence';
    END IF;
    IF EXISTS (
        SELECT 1 FROM external_executions
        WHERE start_state IN ('launching', 'canceled')
           OR downstream_idempotency_key_hash IS NOT NULL
           OR downstream_creation_fingerprint IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'migration 077 rollback refuses external launch or cancellation state';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM external_execution_keys k
        LEFT JOIN external_executions e
          ON e.caller_service_id = k.caller_service_id
         AND e.external_request_id = k.external_request_id
         AND e.actor_user_id = k.actor_user_id
        WHERE e.external_request_id IS NULL
    ) THEN
        RAISE EXCEPTION 'migration 077 rollback refuses key-only external execution tombstones';
    END IF;
END
$$;

ALTER TABLE runtime_sessions
    DROP CONSTRAINT runtime_sessions_drain_evidence_consistent,
    DROP CONSTRAINT runtime_sessions_contract_current,
    DROP CONSTRAINT runtime_sessions_required_features;
ALTER TABLE runtime_nodes
    DROP CONSTRAINT runtime_nodes_contract_current,
    DROP CONSTRAINT runtime_nodes_required_features;

ALTER TABLE runtime_sessions
    DROP COLUMN resume_capacity,
    DROP COLUMN drain_reason_code,
    DROP COLUMN drain_deadline_at,
    DROP COLUMN drain_requested_at;

CREATE OR REPLACE FUNCTION enforce_runtime_node_identity_and_lifecycle()
RETURNS TRIGGER AS $$
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

    IF (OLD.status = 'draining' AND NEW.status NOT IN ('draining', 'revoked'))
       OR (OLD.status = 'revoked' AND NEW.status <> 'revoked') THEN
        RAISE EXCEPTION 'runtime node lifecycle cannot move backwards';
    END IF;

    IF OLD.draining_at IS NOT NULL
       AND NEW.draining_at IS DISTINCT FROM OLD.draining_at THEN
        RAISE EXCEPTION 'runtime node draining evidence is immutable';
    END IF;

    IF OLD.status = 'revoked'
       AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
        RAISE EXCEPTION 'revoked runtime node is immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

UPDATE runtime_schema_contracts
SET is_current = FALSE
WHERE schema_version = 77
  AND migration_name = '077_external_execution_cancellation'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
  AND is_current;

DELETE FROM runtime_schema_contracts
WHERE schema_version = 77
  AND migration_name = '077_external_execution_cancellation'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
  AND NOT is_current;

UPDATE runtime_schema_contracts
SET is_current = TRUE
WHERE schema_version = 76
  AND migration_name = '076_runtime_cancellation_terminal_reap'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
  AND NOT is_current;

DROP INDEX idx_runtime_wire_contracts_current;
DROP INDEX idx_runtime_wire_contracts_previous;
ALTER TABLE runtime_wire_contracts
    DROP CONSTRAINT runtime_wire_contracts_support_identity;

DELETE FROM runtime_wire_contracts
WHERE runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
  AND support_tier = 'current';

UPDATE runtime_wire_contracts
SET support_tier = CASE runtime_contract_digest
    WHEN '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9' THEN 'current'
    WHEN 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53' THEN 'previous'
    ELSE support_tier
END;

ALTER TABLE runtime_wire_contracts
    ADD CONSTRAINT runtime_wire_contracts_support_identity
        CHECK (
            runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (support_tier = 'current'
                 AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9')
                OR
                (support_tier = 'previous'
                 AND runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53')
                OR
                (support_tier = 'historical'
                 AND runtime_contract_digest NOT IN (
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9',
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53'
                 ))
            )
        );

CREATE UNIQUE INDEX idx_runtime_wire_contracts_current
    ON runtime_wire_contracts ((1)) WHERE support_tier = 'current';
CREATE UNIQUE INDEX idx_runtime_wire_contracts_previous
    ON runtime_wire_contracts ((1)) WHERE support_tier = 'previous';

ALTER TABLE runtime_nodes
    ADD CONSTRAINT runtime_nodes_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (status IN ('active', 'draining')
                 AND runtime_contract_digest IN (
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
                 ))
                OR
                (status = 'revoked'
                 AND runtime_contract_digest IN (
                    '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61',
                    '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f',
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
                 ))
            )
        ),
    ADD CONSTRAINT runtime_nodes_required_features
        CHECK (
            runtime_v2_feature_set_is_valid(features)
            AND features @> ARRAY[
                'lease_fence', 'assignment_confirm', 'renew', 'resume',
                'event_ack', 'result_ack', 'cancel', 'persistent_spool'
            ]::TEXT[]
        );

ALTER TABLE runtime_sessions
    ADD CONSTRAINT runtime_sessions_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (status IN ('active', 'draining')
                 AND runtime_contract_digest IN (
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
                 ))
                OR
                (status IN ('offline', 'revoked', 'closed')
                 AND runtime_contract_digest IN (
                    '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61',
                    '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f',
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
                 ))
            )
        ),
    ADD CONSTRAINT runtime_sessions_required_features
        CHECK (
            runtime_v2_feature_set_is_valid(features)
            AND features @> ARRAY[
                'lease_fence', 'assignment_confirm', 'renew', 'resume',
                'event_ack', 'result_ack', 'cancel', 'persistent_spool'
            ]::TEXT[]
        );

DROP INDEX idx_workflow_step_launches_reconcile;
DROP TABLE workflow_step_launches;
DROP INDEX idx_workflow_run_cancellations_reconcile;
DROP TABLE workflow_run_cancellations;
DROP INDEX idx_external_execution_cancellations_reconcile;
DROP TABLE external_execution_cancellations;

ALTER TABLE external_executions
    DROP CONSTRAINT external_executions_start_state_valid,
    DROP CONSTRAINT external_executions_downstream_identity_complete,
    DROP CONSTRAINT external_executions_downstream_fingerprint_valid,
    DROP CONSTRAINT external_executions_downstream_key_hash_valid,
    DROP CONSTRAINT external_executions_key_actor_fkey;

UPDATE external_executions
SET start_state = 'authorized'
WHERE start_state = 'attached';

ALTER TABLE external_executions
    DROP COLUMN downstream_creation_fingerprint,
    DROP COLUMN downstream_idempotency_key_hash,
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
            AND (execution_id IS NOT NULL OR authorized_target_owner_id IS NOT NULL)
            AND rejection_code IS NULL
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
    ) IS TRUE);

DROP TABLE external_execution_keys;

COMMIT;
