BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.runtime.v2.migration.063', 0));

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_stat_activity
        WHERE datid = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND pid <> pg_backend_pid()
          AND backend_type = 'client backend'
    ) THEN
        RAISE EXCEPTION 'migration 063 down requires an exclusive database client session';
    END IF;
END
$$;

LOCK TABLE runs IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE webhook_deliveries IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_deliveries IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_callback_deliveries IN ACCESS EXCLUSIVE MODE;
LOCK TABLE agent_tokens IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_control IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_nodes IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_sessions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_session_attachments IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_attempts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_cancellations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_dead_letters IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_signal_outbox IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_effect_outbox IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_accounting_ledger IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM runs
        WHERE runtime_contract_id = 'openlinker.runtime.v2'
    ) THEN
        RAISE EXCEPTION 'migration 063 down refuses post-cutover runs';
    END IF;

    IF EXISTS (SELECT 1 FROM run_attempts)
       OR EXISTS (SELECT 1 FROM run_cancellations)
       OR EXISTS (SELECT 1 FROM run_dead_letters)
       OR EXISTS (SELECT 1 FROM runtime_signal_outbox)
       OR EXISTS (SELECT 1 FROM run_effect_outbox)
       OR EXISTS (SELECT 1 FROM runtime_nodes)
       OR EXISTS (SELECT 1 FROM runtime_sessions)
       OR EXISTS (SELECT 1 FROM runtime_session_attachments)
       OR EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 063 down refuses runtime v2 operational data';
    END IF;

    IF EXISTS (SELECT 1 FROM webhook_deliveries WHERE effect_outbox_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM run_deliveries WHERE effect_outbox_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM task_callback_deliveries WHERE effect_outbox_id IS NOT NULL) THEN
        RAISE EXCEPTION 'migration 063 down refuses downstream effect links';
    END IF;

    IF EXISTS (SELECT 1 FROM run_deliveries WHERE target_id IS NULL) THEN
        RAISE EXCEPTION 'migration 063 down cannot restore deleted delivery targets';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runtime_cluster_control
        WHERE singleton_id = 1
          AND (mode <> 'hard_maintenance' OR reopened_at IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'migration 063 down refuses a reopened runtime cluster';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM agent_tokens t
        CROSS JOIN runtime_schema_contracts c
        WHERE c.schema_version = 63
          AND (
              t.rotation_predecessor_id IS NOT NULL
              OR t.revocation_kind IS DISTINCT FROM
                    CASE WHEN t.revoked_at IS NULL THEN NULL ELSE 'manual' END
              OR t.created_at >= c.applied_at
              OR t.revoked_at >= c.applied_at
          )
    ) THEN
        RAISE EXCEPTION 'migration 063 down refuses post-cutover token changes';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM task_callback_deliveries d
        JOIN run_events e ON e.id = d.run_event_id
        WHERE e.payload @> '{"terminal":true,"migrated":true,"migration":"063_reliable_runtime_v2"}'::jsonb
    ) THEN
        RAISE EXCEPTION 'migration 063 down refuses delivered migrated events';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS runs_active_attempt_consistency ON runs;
DROP TRIGGER IF EXISTS run_attempts_active_run_consistency ON run_attempts;
DROP TRIGGER IF EXISTS runs_terminal_artifacts_consistency ON runs;
DROP TRIGGER IF EXISTS run_accounting_ledger_run_consistency ON run_accounting_ledger;
DROP TRIGGER IF EXISTS run_dead_letters_run_consistency ON run_dead_letters;
DROP TRIGGER IF EXISTS run_events_terminal_artifacts_consistency ON run_events;
DROP TRIGGER IF EXISTS run_effect_outbox_run_consistency ON run_effect_outbox;
DROP TRIGGER IF EXISTS run_accounting_ledger_immutable ON run_accounting_ledger;
DROP TRIGGER IF EXISTS run_dead_letters_immutable ON run_dead_letters;
DROP TRIGGER IF EXISTS run_effect_outbox_identity_immutable ON run_effect_outbox;
DROP TRIGGER IF EXISTS runs_cancellation_summary_consistency ON runs;
DROP TRIGGER IF EXISTS run_cancellations_run_summary_consistency ON run_cancellations;
DROP TRIGGER IF EXISTS runs_v2_contract_identity ON runs;
DROP TRIGGER IF EXISTS run_attempts_identity_immutable ON run_attempts;
DROP TRIGGER IF EXISTS run_attempts_event_sequence_consistency ON run_attempts;
DROP TRIGGER IF EXISTS run_events_attempt_sequence_consistency ON run_events;
DROP TRIGGER IF EXISTS run_events_immutable ON run_events;
DROP TRIGGER IF EXISTS run_cancellations_state_forward_only ON run_cancellations;
DROP TRIGGER IF EXISTS runtime_sessions_identity_immutable ON runtime_sessions;
DROP TRIGGER IF EXISTS runtime_sessions_principal_valid ON runtime_sessions;
DROP TRIGGER IF EXISTS runtime_sessions_attachment_consistency ON runtime_sessions;
DROP TRIGGER IF EXISTS runtime_session_attachments_session_consistency ON runtime_session_attachments;
DROP TRIGGER IF EXISTS runtime_session_attachments_history ON runtime_session_attachments;
DROP TRIGGER IF EXISTS runtime_nodes_session_contract_consistency ON runtime_nodes;
DROP TRIGGER IF EXISTS runtime_sessions_node_contract_consistency ON runtime_sessions;
DROP TRIGGER IF EXISTS runtime_nodes_revocation_guard ON runtime_nodes;
DROP TRIGGER IF EXISTS agent_tokens_runtime_revocation_guard ON agent_tokens;
DROP TRIGGER IF EXISTS runtime_nodes_identity_and_lifecycle ON runtime_nodes;
DROP TRIGGER IF EXISTS agent_tokens_identity_and_lifecycle ON agent_tokens;

DROP FUNCTION IF EXISTS enforce_run_active_attempt_consistency();
DROP FUNCTION IF EXISTS enforce_run_terminal_artifact_immutable();
DROP FUNCTION IF EXISTS enforce_run_effect_identity_immutable();
DROP FUNCTION IF EXISTS enforce_run_terminal_artifacts_consistency();
DROP FUNCTION IF EXISTS enforce_run_cancellation_summary_consistency();
DROP FUNCTION IF EXISTS enforce_run_v2_contract_identity();
DROP FUNCTION IF EXISTS enforce_run_attempt_identity_immutable();
DROP FUNCTION IF EXISTS enforce_attempt_event_sequence_consistency();
DROP FUNCTION IF EXISTS enforce_run_event_immutable();
DROP FUNCTION IF EXISTS enforce_run_cancellation_transition();
DROP FUNCTION IF EXISTS enforce_runtime_session_identity_immutable();
DROP FUNCTION IF EXISTS enforce_runtime_session_principal();
DROP FUNCTION IF EXISTS enforce_runtime_session_attachment_history();
DROP FUNCTION IF EXISTS enforce_runtime_session_attachment_consistency();
DROP FUNCTION IF EXISTS enforce_runtime_node_session_contract_consistency();
DROP FUNCTION IF EXISTS enforce_runtime_node_revocation_guard();
DROP FUNCTION IF EXISTS enforce_runtime_token_revocation_guard();
DROP FUNCTION IF EXISTS enforce_runtime_node_identity_and_lifecycle();
DROP FUNCTION IF EXISTS enforce_agent_token_identity_and_lifecycle();

DROP INDEX IF EXISTS idx_webhook_deliveries_effect_outbox;
ALTER TABLE webhook_deliveries
    DROP CONSTRAINT webhook_deliveries_effect_outbox_fk,
    DROP COLUMN effect_outbox_id;

DROP INDEX IF EXISTS idx_run_deliveries_effect_outbox;
ALTER TABLE run_deliveries
    DROP CONSTRAINT run_deliveries_effect_outbox_fk,
    DROP COLUMN effect_outbox_id,
    DROP CONSTRAINT run_deliveries_target_id_fkey,
    ALTER COLUMN target_id SET NOT NULL,
    ADD CONSTRAINT run_deliveries_target_id_fkey
        FOREIGN KEY (target_id)
        REFERENCES delivery_targets(id)
        ON DELETE CASCADE;

DROP INDEX IF EXISTS idx_task_callback_deliveries_effect_outbox;
ALTER TABLE task_callback_deliveries
    DROP CONSTRAINT task_callback_deliveries_effect_outbox_fk,
    DROP COLUMN effect_outbox_id;

ALTER TABLE runs
    DROP CONSTRAINT runs_latest_attempt_fk,
    DROP CONSTRAINT runs_active_attempt_fk,
    DROP CONSTRAINT runs_result_attempt_fk,
    DROP CONSTRAINT runs_terminal_event_fk,
    DROP CONSTRAINT runs_cancellation_fk,
    DROP CONSTRAINT runs_runtime_node_fk,
    DROP CONSTRAINT runs_runtime_session_identity_fk,
    DROP CONSTRAINT runs_finished_state_consistent,
    DROP CONSTRAINT runs_nonresult_terminal_output_consistent;

ALTER TABLE run_events
    DROP CONSTRAINT run_events_attempt_identity_fk;

DROP TABLE run_dead_letters;
DROP TABLE run_cancellations;
DROP TABLE run_effect_outbox;
DROP TABLE runtime_signal_outbox;
DROP TABLE run_accounting_ledger;
DROP TABLE run_attempts;
DROP TABLE runtime_session_attachments;
DROP TABLE runtime_sessions;
DROP TABLE runtime_nodes;
DROP TABLE runtime_cluster_members;
DROP TABLE runtime_cluster_control;

DELETE FROM run_events
WHERE event_type = 'run.status.changed'
  AND payload @> '{"terminal":true,"migrated":true,"migration":"063_reliable_runtime_v2"}'::jsonb;

DROP INDEX IF EXISTS idx_run_events_client_event_id;
DROP INDEX IF EXISTS idx_run_events_attempt_client_sequence;

ALTER TABLE run_events
    DROP CONSTRAINT run_events_client_identity_consistent,
    DROP CONSTRAINT run_events_payload_object,
    DROP CONSTRAINT run_events_run_id_id_unique,
    DROP COLUMN client_event_id,
    DROP COLUMN client_event_seq,
    DROP COLUMN payload_fingerprint,
    DROP COLUMN attempt_id,
    DROP COLUMN attempt_no,
    DROP COLUMN fencing_token;

CREATE INDEX idx_run_events_run_sequence
    ON run_events (run_id, sequence);

DROP INDEX IF EXISTS idx_runs_idempotency_key;
DROP INDEX IF EXISTS idx_runs_runtime_pending;
DROP INDEX IF EXISTS idx_runs_runtime_pending_global;
DROP INDEX IF EXISTS idx_runs_runtime_retry_due;
DROP INDEX IF EXISTS idx_runs_runtime_retry_due_global;
DROP INDEX IF EXISTS idx_runs_runtime_offer_expiry;
DROP INDEX IF EXISTS idx_runs_runtime_execution_expiry;
DROP INDEX IF EXISTS idx_runs_runtime_deadline;
DROP INDEX IF EXISTS idx_runs_replay_lineage;

ALTER TABLE runs
    DROP COLUMN runtime_contract_id,
    DROP COLUMN idempotency_key_hash,
    DROP COLUMN idempotency_fingerprint,
    DROP COLUMN connection_mode_snapshot,
    DROP COLUMN endpoint_idempotency_snapshot,
    DROP COLUMN dispatch_state,
    DROP COLUMN offer_count,
    DROP COLUMN max_offer_count,
    DROP COLUMN attempt_count,
    DROP COLUMN max_attempts,
    DROP COLUMN next_attempt_at,
    DROP COLUMN dispatch_deadline_at,
    DROP COLUMN run_deadline_at,
    DROP COLUMN latest_attempt_id,
    DROP COLUMN active_attempt_id,
    DROP COLUMN lease_id,
    DROP COLUMN fencing_token,
    DROP COLUMN executor_type,
    DROP COLUMN active_core_instance_id,
    DROP COLUMN runtime_node_id,
    DROP COLUMN runtime_worker_id,
    DROP COLUMN runtime_session_id,
    DROP COLUMN lease_token_id,
    DROP COLUMN lease_offered_at,
    DROP COLUMN lease_accepted_at,
    DROP COLUMN lease_expires_at,
    DROP COLUMN attempt_deadline_at,
    DROP COLUMN cancel_request_id,
    DROP COLUMN cancel_state,
    DROP COLUMN cancel_requested_at,
    DROP COLUMN cancel_acknowledged_at,
    DROP COLUMN cancel_reason,
    DROP COLUMN result_id,
    DROP COLUMN result_fingerprint,
    DROP COLUMN terminal_event_id,
    DROP COLUMN dead_lettered_at,
    DROP COLUMN replay_of_run_id,
    DROP CONSTRAINT runs_id_agent_unique,
    ADD COLUMN claimed_by_runtime_token_id UUID
        REFERENCES agent_tokens(id) ON DELETE SET NULL,
    ADD COLUMN claimed_at TIMESTAMPTZ;

CREATE INDEX idx_runs_runtime_pull_claim
    ON runs (agent_id, started_at ASC)
    WHERE status = 'running' AND claimed_at IS NULL;

CREATE INDEX idx_runs_runtime_claim_stale
    ON runs (agent_id, claimed_at, started_at)
    WHERE status = 'running' AND claimed_at IS NOT NULL;

CREATE INDEX idx_runs_runtime_claimed_token
    ON runs (agent_id, claimed_by_runtime_token_id, started_at)
    WHERE status = 'running' AND claimed_by_runtime_token_id IS NOT NULL;

DROP INDEX IF EXISTS idx_agent_tokens_rotation_predecessor;

ALTER TABLE agent_tokens
    DROP CONSTRAINT agent_tokens_rotation_same_agent,
    DROP CONSTRAINT agent_tokens_rotation_distinct,
    DROP CONSTRAINT agent_tokens_revocation_kind_valid,
    DROP CONSTRAINT agent_tokens_revocation_consistent,
    DROP COLUMN rotation_predecessor_id,
    DROP COLUMN revocation_kind,
    DROP CONSTRAINT agent_tokens_id_agent_unique;

DROP FUNCTION runtime_v2_feature_set_is_valid(TEXT[]);

DROP TABLE runtime_schema_contracts;

COMMIT;
