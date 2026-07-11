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
        RAISE EXCEPTION 'migration 063 requires an exclusive database client session';
    END IF;
END
$$;

LOCK TABLE runs IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE webhook_deliveries IN ACCESS EXCLUSIVE MODE;
LOCK TABLE run_deliveries IN ACCESS EXCLUSIVE MODE;
LOCK TABLE task_callback_deliveries IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'migration 063 requires zero nonterminal runs';
    END IF;

    IF EXISTS (SELECT 1 FROM webhook_deliveries WHERE status = 'pending')
       OR EXISTS (SELECT 1 FROM run_deliveries WHERE status = 'pending')
       OR EXISTS (SELECT 1 FROM task_callback_deliveries WHERE status = 'pending') THEN
        RAISE EXCEPTION 'migration 063 requires zero pending legacy deliveries';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runs
        WHERE status <> 'running'
          AND (
              finished_at IS NULL
              OR COALESCE(duration_ms, 0) < 0
              OR platform_fee_cents < 0
              OR creator_revenue_cents < 0
          )
    ) THEN
        RAISE EXCEPTION 'migration 063 found invalid historical terminal runs';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM run_events
        WHERE jsonb_typeof(payload) <> 'object'
    ) THEN
        RAISE EXCEPTION 'migration 063 requires object-shaped historical run events';
    END IF;
END
$$;

CREATE TABLE runtime_schema_contracts (
    schema_version INTEGER PRIMARY KEY,
    migration_name TEXT NOT NULL UNIQUE,
    runtime_contract_id TEXT NOT NULL,
    runtime_contract_digest TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT runtime_schema_contracts_contract_unique
        UNIQUE (schema_version, runtime_contract_id, runtime_contract_digest),
    CONSTRAINT runtime_schema_contracts_runtime_pair_unique
        UNIQUE (runtime_contract_id, runtime_contract_digest),
    CONSTRAINT runtime_schema_contracts_version_positive
        CHECK (schema_version > 0),
    CONSTRAINT runtime_schema_contracts_contract_id_len
        CHECK (char_length(runtime_contract_id) BETWEEN 1 AND 200),
    CONSTRAINT runtime_schema_contracts_digest_format
        CHECK (runtime_contract_digest ~ '^[a-f0-9]{64}$')
);

CREATE UNIQUE INDEX idx_runtime_schema_contracts_current
    ON runtime_schema_contracts (is_current)
    WHERE is_current;

CREATE TABLE runtime_cluster_control (
    singleton_id SMALLINT PRIMARY KEY DEFAULT 1,
    mode TEXT NOT NULL DEFAULT 'hard_maintenance',
    expected_replicas INTEGER NOT NULL DEFAULT 1,
    cutover_id UUID NOT NULL DEFAULT gen_random_uuid(),
    drain_started_at TIMESTAMPTZ,
    drain_deadline_at TIMESTAMPTZ,
    hard_maintenance_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    reopened_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1,
    updated_by_type TEXT NOT NULL DEFAULT 'migration',
    updated_by_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT runtime_cluster_control_singleton
        CHECK (singleton_id = 1),
    CONSTRAINT runtime_cluster_control_mode_valid
        CHECK (mode IN ('normal', 'draining', 'hard_maintenance')),
    CONSTRAINT runtime_cluster_control_expected_replicas
        CHECK (expected_replicas BETWEEN 1 AND 1024),
    CONSTRAINT runtime_cluster_control_version_positive
        CHECK (version > 0),
    CONSTRAINT runtime_cluster_control_drain_order
        CHECK (
            drain_deadline_at IS NULL
            OR drain_started_at IS NULL
            OR drain_deadline_at >= drain_started_at
        )
);

INSERT INTO runtime_cluster_control (singleton_id)
VALUES (1);

CREATE TABLE runtime_cluster_members (
    instance_id UUID PRIMARY KEY,
    release_version TEXT NOT NULL,
    release_commit TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    schema_checksum TEXT NOT NULL,
    runtime_contract_id TEXT NOT NULL,
    runtime_contract_digest TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    draining BOOLEAN NOT NULL DEFAULT FALSE,
    ready BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT runtime_cluster_members_schema_contract_fk
        FOREIGN KEY (
            schema_version,
            runtime_contract_id,
            runtime_contract_digest
        )
        REFERENCES runtime_schema_contracts (
            schema_version,
            runtime_contract_id,
            runtime_contract_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT runtime_cluster_members_schema_version
        CHECK (schema_version > 0),
    CONSTRAINT runtime_cluster_members_release_len
        CHECK (
            char_length(release_version) BETWEEN 1 AND 100
            AND char_length(release_commit) BETWEEN 1 AND 100
        ),
    CONSTRAINT runtime_cluster_members_checksum_format
        CHECK (
            schema_checksum ~ '^[a-f0-9]{64}$'
            AND runtime_contract_digest ~ '^[a-f0-9]{64}$'
        )
);

CREATE INDEX idx_runtime_cluster_members_heartbeat
    ON runtime_cluster_members (heartbeat_at, instance_id);

CREATE OR REPLACE FUNCTION runtime_v2_feature_set_is_valid(feature_set TEXT[])
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT cardinality(feature_set) >= 8
       AND COUNT(*) = COUNT(DISTINCT feature)
       AND BOOL_AND(char_length(feature) BETWEEN 1 AND 100)
    FROM unnest(feature_set) AS feature
$$;

ALTER TABLE agent_tokens
    ADD CONSTRAINT agent_tokens_id_agent_unique
        UNIQUE (id, agent_id),
    ADD COLUMN rotation_predecessor_id UUID,
    ADD COLUMN revocation_kind TEXT;

UPDATE agent_tokens
SET revocation_kind = 'manual'
WHERE revoked_at IS NOT NULL;

ALTER TABLE agent_tokens
    ADD CONSTRAINT agent_tokens_rotation_distinct
        CHECK (rotation_predecessor_id IS NULL OR rotation_predecessor_id <> id),
    ADD CONSTRAINT agent_tokens_revocation_kind_valid
        CHECK (
            revocation_kind IS NULL
            OR revocation_kind IN ('planned_rotation', 'security', 'manual', 'expired')
        ),
    ADD CONSTRAINT agent_tokens_revocation_consistent
        CHECK (
            (
                status <> 'revoked'
                AND revoked_at IS NULL
                AND revocation_kind IS NULL
            )
            OR (
                status = 'revoked'
                AND revoked_at IS NOT NULL
                AND revocation_kind IS NOT NULL
            )
        ),
    ADD CONSTRAINT agent_tokens_rotation_same_agent
        FOREIGN KEY (rotation_predecessor_id, agent_id)
        REFERENCES agent_tokens (id, agent_id)
        DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX idx_agent_tokens_rotation_predecessor
    ON agent_tokens (rotation_predecessor_id)
    WHERE rotation_predecessor_id IS NOT NULL;

DROP INDEX IF EXISTS idx_runs_runtime_pull_claim;
DROP INDEX IF EXISTS idx_runs_runtime_claim_stale;
DROP INDEX IF EXISTS idx_runs_runtime_claimed_token;

ALTER TABLE runs
    ADD CONSTRAINT runs_id_agent_unique UNIQUE (id, agent_id),
    ADD COLUMN runtime_contract_id TEXT,
    ADD COLUMN idempotency_key_hash BYTEA,
    ADD COLUMN idempotency_fingerprint BYTEA,
    ADD COLUMN connection_mode_snapshot TEXT,
    ADD COLUMN endpoint_idempotency_snapshot BOOLEAN,
    ADD COLUMN dispatch_state TEXT,
    ADD COLUMN offer_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_offer_count INTEGER NOT NULL DEFAULT 20,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN next_attempt_at TIMESTAMPTZ,
    ADD COLUMN dispatch_deadline_at TIMESTAMPTZ,
    ADD COLUMN run_deadline_at TIMESTAMPTZ,
    ADD COLUMN latest_attempt_id UUID,
    ADD COLUMN active_attempt_id UUID,
    ADD COLUMN lease_id UUID,
    ADD COLUMN fencing_token BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN executor_type TEXT,
    ADD COLUMN active_core_instance_id UUID,
    ADD COLUMN runtime_node_id UUID,
    ADD COLUMN runtime_worker_id TEXT,
    ADD COLUMN runtime_session_id UUID,
    ADD COLUMN lease_token_id UUID,
    ADD COLUMN lease_offered_at TIMESTAMPTZ,
    ADD COLUMN lease_accepted_at TIMESTAMPTZ,
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN attempt_deadline_at TIMESTAMPTZ,
    ADD COLUMN cancel_request_id UUID,
    ADD COLUMN cancel_state TEXT,
    ADD COLUMN cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN cancel_acknowledged_at TIMESTAMPTZ,
    ADD COLUMN cancel_reason TEXT,
    ADD COLUMN result_id UUID,
    ADD COLUMN result_fingerprint BYTEA,
    ADD COLUMN terminal_event_id UUID,
    ADD COLUMN dead_lettered_at TIMESTAMPTZ,
    ADD COLUMN replay_of_run_id UUID;

UPDATE runs
SET runtime_contract_id = 'legacy.pre-v2',
    connection_mode_snapshot = NULL,
    dispatch_state = 'terminal';

ALTER TABLE runs
    ALTER COLUMN runtime_contract_id SET DEFAULT 'openlinker.runtime.v2',
    ALTER COLUMN runtime_contract_id SET NOT NULL,
    ALTER COLUMN dispatch_state SET DEFAULT 'pending',
    ALTER COLUMN dispatch_state SET NOT NULL;

ALTER TABLE run_events
    ADD COLUMN client_event_id UUID,
    ADD COLUMN client_event_seq BIGINT,
    ADD COLUMN payload_fingerprint BYTEA,
    ADD COLUMN attempt_id UUID,
    ADD COLUMN attempt_no INTEGER,
    ADD COLUMN fencing_token BIGINT,
    ADD CONSTRAINT run_events_run_id_id_unique UNIQUE (run_id, id);

DROP INDEX IF EXISTS idx_run_events_run_sequence;

CREATE TABLE runtime_nodes (
    node_id UUID PRIMARY KEY,
    display_name TEXT NOT NULL,
    device_certificate_serial TEXT NOT NULL UNIQUE,
    device_public_key_thumbprint TEXT NOT NULL UNIQUE,
    node_version TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    runtime_contract_id TEXT NOT NULL,
    runtime_contract_digest TEXT NOT NULL,
    features TEXT[] NOT NULL,
    capacity INTEGER NOT NULL DEFAULT 1,
    inflight INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    last_seen_at TIMESTAMPTZ,
    draining_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revoke_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT runtime_nodes_contract_fk
        FOREIGN KEY (runtime_contract_id, runtime_contract_digest)
        REFERENCES runtime_schema_contracts (
            runtime_contract_id,
            runtime_contract_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT runtime_nodes_display_name_len
        CHECK (char_length(display_name) BETWEEN 1 AND 200),
    CONSTRAINT runtime_nodes_version_len
        CHECK (char_length(node_version) BETWEEN 1 AND 100),
    CONSTRAINT runtime_nodes_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND runtime_contract_digest = 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f'
        ),
    CONSTRAINT runtime_nodes_contract_digest
        CHECK (runtime_contract_digest ~ '^[a-f0-9]{64}$'),
    CONSTRAINT runtime_nodes_required_features
        CHECK (
            runtime_v2_feature_set_is_valid(features)
            AND features @> ARRAY[
                'lease_fence',
                'assignment_confirm',
                'renew',
                'resume',
                'event_ack',
                'result_ack',
                'cancel',
                'persistent_spool'
            ]::TEXT[]
        ),
    CONSTRAINT runtime_nodes_capacity
        CHECK (
            capacity BETWEEN 0 AND 1024
            AND inflight BETWEEN 0 AND 1024
            AND (inflight <= capacity OR status = 'draining')
        ),
    CONSTRAINT runtime_nodes_status_valid
        CHECK (status IN ('active', 'draining', 'revoked')),
    CONSTRAINT runtime_nodes_revoke_consistent
        CHECK (
            (status = 'revoked' AND revoked_at IS NOT NULL)
            OR (status <> 'revoked' AND revoked_at IS NULL)
        ),
    CONSTRAINT runtime_nodes_lifecycle_consistent
        CHECK (
            (status = 'active' AND draining_at IS NULL)
            OR (status = 'draining' AND draining_at IS NOT NULL)
            OR status = 'revoked'
        )
);

CREATE INDEX idx_runtime_nodes_last_seen
    ON runtime_nodes (last_seen_at, node_id)
    WHERE status <> 'revoked';

CREATE TABLE runtime_sessions (
    runtime_session_id UUID PRIMARY KEY,
    node_id UUID NOT NULL REFERENCES runtime_nodes(node_id) ON DELETE RESTRICT,
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    credential_id UUID NOT NULL,
    worker_id TEXT NOT NULL,
    session_epoch BIGINT NOT NULL,
    device_certificate_serial TEXT NOT NULL,
    node_version TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    runtime_contract_id TEXT NOT NULL,
    runtime_contract_digest TEXT NOT NULL,
    features TEXT[] NOT NULL,
    capacity INTEGER NOT NULL,
    inflight INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    attached_core_instance_id UUID,
    connected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    disconnected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT runtime_sessions_identity_unique
        UNIQUE (node_id, agent_id, worker_id, session_epoch),
    CONSTRAINT runtime_sessions_attempt_identity_unique
        UNIQUE (runtime_session_id, node_id, agent_id, credential_id, worker_id),
    CONSTRAINT runtime_sessions_credential_agent_fk
        FOREIGN KEY (credential_id, agent_id)
        REFERENCES agent_tokens (id, agent_id)
        ON DELETE RESTRICT,
    CONSTRAINT runtime_sessions_contract_fk
        FOREIGN KEY (runtime_contract_id, runtime_contract_digest)
        REFERENCES runtime_schema_contracts (
            runtime_contract_id,
            runtime_contract_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT runtime_sessions_worker_len
        CHECK (char_length(worker_id) BETWEEN 1 AND 200),
    CONSTRAINT runtime_sessions_epoch_positive
        CHECK (session_epoch > 0),
    CONSTRAINT runtime_sessions_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND runtime_contract_digest = 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f'
        ),
    CONSTRAINT runtime_sessions_contract_digest
        CHECK (runtime_contract_digest ~ '^[a-f0-9]{64}$'),
    CONSTRAINT runtime_sessions_required_features
        CHECK (
            runtime_v2_feature_set_is_valid(features)
            AND features @> ARRAY[
                'lease_fence',
                'assignment_confirm',
                'renew',
                'resume',
                'event_ack',
                'result_ack',
                'cancel',
                'persistent_spool'
            ]::TEXT[]
        ),
    CONSTRAINT runtime_sessions_capacity
        CHECK (
            capacity BETWEEN 0 AND 1024
            AND inflight BETWEEN 0 AND 1024
            AND (inflight <= capacity OR status = 'draining')
        ),
    CONSTRAINT runtime_sessions_status_valid
        CHECK (status IN ('active', 'draining', 'offline', 'revoked', 'closed')),
    CONSTRAINT runtime_sessions_disconnect_consistent
        CHECK (
            (
                status IN ('active', 'draining')
                AND disconnected_at IS NULL
                AND attached_core_instance_id IS NOT NULL
            )
            OR (
                status IN ('offline', 'revoked', 'closed')
                AND disconnected_at IS NOT NULL
                AND attached_core_instance_id IS NULL
            )
        )
);

CREATE UNIQUE INDEX idx_runtime_sessions_active_worker
    ON runtime_sessions (node_id, agent_id, worker_id)
    WHERE status IN ('active', 'draining');

CREATE INDEX idx_runtime_sessions_heartbeat
    ON runtime_sessions (heartbeat_at, runtime_session_id)
    WHERE status IN ('active', 'draining');

CREATE INDEX idx_runtime_sessions_agent_status
    ON runtime_sessions (agent_id, status, heartbeat_at DESC);

CREATE TABLE runtime_session_attachments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    runtime_session_id UUID NOT NULL
        REFERENCES runtime_sessions(runtime_session_id) ON DELETE RESTRICT,
    core_instance_id UUID NOT NULL,
    attachment_kind TEXT NOT NULL,
    attached_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    detached_at TIMESTAMPTZ,
    disconnect_reason TEXT,
    CONSTRAINT runtime_session_attachments_kind_valid
        CHECK (attachment_kind IN ('connected', 'resumed')),
    CONSTRAINT runtime_session_attachments_time_order
        CHECK (detached_at IS NULL OR detached_at >= attached_at),
    CONSTRAINT runtime_session_attachments_detach_evidence
        CHECK (
            (detached_at IS NULL AND disconnect_reason IS NULL)
            OR (
                detached_at IS NOT NULL
                AND disconnect_reason IS NOT NULL
                AND char_length(disconnect_reason) BETWEEN 1 AND 200
            )
        )
);

CREATE UNIQUE INDEX idx_runtime_session_attachments_active
    ON runtime_session_attachments (runtime_session_id)
    WHERE detached_at IS NULL;

CREATE INDEX idx_runtime_session_attachments_instance
    ON runtime_session_attachments (core_instance_id, attached_at DESC);

CREATE TABLE run_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE NO ACTION,
    agent_id UUID NOT NULL,
    offer_no INTEGER NOT NULL,
    attempt_no INTEGER,
    executor_type TEXT NOT NULL,
    lease_id UUID NOT NULL UNIQUE,
    fencing_token BIGINT NOT NULL,
    runtime_token_id UUID,
    runtime_worker_id TEXT,
    runtime_session_id UUID,
    node_id UUID,
    offered_by_core_instance_id UUID NOT NULL,
    attached_core_instance_id UUID NOT NULL,
    offered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    offer_expires_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ,
    last_renewed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    attempt_deadline_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    outcome TEXT,
    result_id UUID,
    result_fingerprint BYTEA,
    result_classification TEXT,
    result_acknowledged_at TIMESTAMPTZ,
    last_client_event_seq BIGINT NOT NULL DEFAULT 0,
    final_client_event_seq BIGINT,
    error_code TEXT,
    error_detail_redacted TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT run_attempts_run_id_id_unique UNIQUE (run_id, id),
    CONSTRAINT run_attempts_run_offer_unique UNIQUE (run_id, offer_no),
    CONSTRAINT run_attempts_run_attempt_unique UNIQUE (run_id, attempt_no),
    CONSTRAINT run_attempts_run_fence_unique UNIQUE (run_id, fencing_token),
    CONSTRAINT run_attempts_run_attempt_fence_unique
        UNIQUE (run_id, attempt_no, fencing_token),
    CONSTRAINT run_attempts_event_identity_unique
        UNIQUE (run_id, id, attempt_no, fencing_token),
    CONSTRAINT run_attempts_run_result_unique UNIQUE (run_id, result_id),
    CONSTRAINT run_attempts_run_agent_fk
        FOREIGN KEY (run_id, agent_id)
        REFERENCES runs (id, agent_id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT run_attempts_session_identity_fk
        FOREIGN KEY (
            runtime_session_id,
            node_id,
            agent_id,
            runtime_token_id,
            runtime_worker_id
        )
        REFERENCES runtime_sessions (
            runtime_session_id,
            node_id,
            agent_id,
            credential_id,
            worker_id
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT run_attempts_offer_positive CHECK (offer_no > 0),
    CONSTRAINT run_attempts_attempt_positive
        CHECK (attempt_no IS NULL OR attempt_no > 0),
    CONSTRAINT run_attempts_fence_positive CHECK (fencing_token > 0),
    CONSTRAINT run_attempts_executor_valid
        CHECK (executor_type IN ('agent_node', 'core_http', 'core_mcp')),
    CONSTRAINT run_attempts_executor_identity
        CHECK (
            (
                executor_type = 'agent_node'
                AND runtime_token_id IS NOT NULL
                AND runtime_worker_id IS NOT NULL
                AND runtime_session_id IS NOT NULL
                AND node_id IS NOT NULL
            )
            OR
            (
                executor_type IN ('core_http', 'core_mcp')
                AND runtime_token_id IS NULL
                AND runtime_worker_id IS NULL
                AND runtime_session_id IS NULL
                AND node_id IS NULL
            )
        ),
    CONSTRAINT run_attempts_acceptance_consistent
        CHECK (
            (accepted_at IS NULL AND attempt_no IS NULL)
            OR (accepted_at IS NOT NULL AND attempt_no IS NOT NULL)
        ),
    CONSTRAINT run_attempts_result_consistent
        CHECK (
            (
                result_id IS NULL
                AND result_fingerprint IS NULL
                AND result_classification IS NULL
                AND final_client_event_seq IS NULL
            )
            OR (
                result_id IS NOT NULL
                AND result_fingerprint IS NOT NULL
                AND result_classification IS NOT NULL
                AND final_client_event_seq IS NOT NULL
                AND octet_length(result_fingerprint) = 32
                AND final_client_event_seq = last_client_event_seq
            )
        ),
    CONSTRAINT run_attempts_result_ack_consistent
        CHECK (result_acknowledged_at IS NULL OR result_id IS NOT NULL),
    CONSTRAINT run_attempts_outcome_valid
        CHECK (
            outcome IS NULL
            OR outcome IN (
                'offer_rejected',
                'offer_expired',
                'success',
                'retryable_failure',
                'non_retryable_failure',
                'lease_expired',
                'canceled',
                'timeout',
                'result_unknown'
            )
        ),
    CONSTRAINT run_attempts_outcome_attempt_consistent
        CHECK (
            outcome IS NULL
            OR (
                outcome IN ('offer_rejected', 'offer_expired')
                AND attempt_no IS NULL
            )
            OR outcome = 'canceled'
            OR (
                outcome NOT IN ('offer_rejected', 'offer_expired', 'canceled')
                AND attempt_no IS NOT NULL
            )
        ),
    CONSTRAINT run_attempts_result_classification_valid
        CHECK (
            result_classification IS NULL
            OR result_classification IN (
                'success',
                'retryable_failure',
                'non_retryable_failure'
            )
        ),
    CONSTRAINT run_attempts_outcome_result_consistent
        CHECK (
            CASE
                WHEN outcome = 'success' THEN
                    result_classification IS NOT DISTINCT FROM 'success'
                    AND result_id IS NOT NULL
                WHEN outcome = 'retryable_failure' THEN
                    result_classification IS NOT DISTINCT FROM 'retryable_failure'
                    AND result_id IS NOT NULL
                WHEN outcome = 'non_retryable_failure' THEN
                    result_classification IS NOT DISTINCT FROM 'non_retryable_failure'
                    AND result_id IS NOT NULL
                WHEN outcome IN (
                    'offer_rejected',
                    'offer_expired',
                    'lease_expired',
                    'canceled',
                    'timeout',
                    'result_unknown'
                ) THEN result_id IS NULL
                ELSE result_id IS NULL
            END
        ),
    CONSTRAINT run_attempts_finished_consistent
        CHECK (
            (finished_at IS NULL AND outcome IS NULL)
            OR (finished_at IS NOT NULL AND outcome IS NOT NULL)
        ),
    CONSTRAINT run_attempts_event_sequences
        CHECK (
            last_client_event_seq >= 0
            AND (final_client_event_seq IS NULL OR final_client_event_seq >= 0)
        ),
    CONSTRAINT run_attempts_time_order
        CHECK (
            offer_expires_at >= offered_at
            AND lease_expires_at >= offered_at
            AND attempt_deadline_at >= offered_at
            AND lease_expires_at <= attempt_deadline_at
            AND (accepted_at IS NULL OR accepted_at >= offered_at)
            AND (accepted_at IS NULL OR accepted_at <= lease_expires_at)
            AND (
                last_renewed_at IS NULL
                OR (
                    accepted_at IS NOT NULL
                    AND last_renewed_at >= accepted_at
                )
            )
            AND (finished_at IS NULL OR finished_at >= offered_at)
        )
);

CREATE INDEX idx_run_attempts_active_session
    ON run_attempts (runtime_session_id, run_id, attempt_no)
    WHERE finished_at IS NULL AND runtime_session_id IS NOT NULL;

CREATE INDEX idx_run_attempts_active_node
    ON run_attempts (node_id, run_id, attempt_no)
    WHERE finished_at IS NULL AND node_id IS NOT NULL;

CREATE INDEX idx_run_attempts_lease_expiry
    ON run_attempts (lease_expires_at, run_id)
    WHERE finished_at IS NULL;

CREATE UNIQUE INDEX idx_run_attempts_unfinished_run
    ON run_attempts (run_id)
    WHERE finished_at IS NULL;

CREATE TABLE run_cancellations (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    target_attempt_id UUID,
    state TEXT NOT NULL DEFAULT 'requested',
    requested_by_type TEXT NOT NULL,
    requested_by_id UUID NOT NULL,
    reason TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    delivered_at TIMESTAMPTZ,
    stopping_at TIMESTAMPTZ,
    stopped_at TIMESTAMPTZ,
    acknowledged_at TIMESTAMPTZ,
    error_code TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT run_cancellations_run_unique UNIQUE (run_id),
    CONSTRAINT run_cancellations_run_id_id_unique UNIQUE (run_id, id),
    CONSTRAINT run_cancellations_run_fk
        FOREIGN KEY (run_id) REFERENCES runs(id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT run_cancellations_target_attempt_fk
        FOREIGN KEY (run_id, target_attempt_id)
        REFERENCES run_attempts (run_id, id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT run_cancellations_state_valid
        CHECK (
            state IN (
                'requested',
                'delivered',
                'stopping',
                'stopped',
                'unsupported',
                'failed',
                'unconfirmed'
            )
        ),
    CONSTRAINT run_cancellations_requester_valid
        CHECK (
            requested_by_type IN (
                'user',
                'service_account',
                'agent',
                'instance_admin',
                'system'
            )
        ),
    CONSTRAINT run_cancellations_reason_len
        CHECK (reason IS NULL OR char_length(reason) <= 500),
    CONSTRAINT run_cancellations_time_order
        CHECK (
            (delivered_at IS NULL OR delivered_at >= requested_at)
            AND (stopping_at IS NULL OR stopping_at >= COALESCE(delivered_at, requested_at))
            AND (stopped_at IS NULL OR stopped_at >= COALESCE(stopping_at, delivered_at, requested_at))
            AND (acknowledged_at IS NULL OR acknowledged_at >= requested_at)
            AND updated_at >= requested_at
        ),
    CONSTRAINT run_cancellations_state_evidence
        CHECK (
            CASE state
                WHEN 'requested' THEN
                    delivered_at IS NULL
                    AND stopping_at IS NULL
                    AND stopped_at IS NULL
                    AND acknowledged_at IS NULL
                    AND error_code IS NULL
                WHEN 'delivered' THEN
                    target_attempt_id IS NOT NULL
                    AND delivered_at IS NOT NULL
                    AND stopping_at IS NULL
                    AND stopped_at IS NULL
                    AND acknowledged_at IS NULL
                    AND error_code IS NULL
                WHEN 'stopping' THEN
                    target_attempt_id IS NOT NULL
                    AND delivered_at IS NOT NULL
                    AND stopping_at IS NOT NULL
                    AND stopped_at IS NULL
                    AND acknowledged_at IS NOT NULL
                    AND error_code IS NULL
                WHEN 'stopped' THEN
                    stopped_at IS NOT NULL
                    AND error_code IS NULL
                    AND (
                        (
                            target_attempt_id IS NULL
                            AND delivered_at IS NULL
                            AND stopping_at IS NULL
                            AND acknowledged_at IS NULL
                        )
                        OR (
                            target_attempt_id IS NOT NULL
                            AND delivered_at IS NOT NULL
                            AND acknowledged_at IS NOT NULL
                        )
                    )
                WHEN 'unsupported' THEN
                    target_attempt_id IS NOT NULL
                    AND delivered_at IS NOT NULL
                    AND stopping_at IS NULL
                    AND stopped_at IS NULL
                    AND acknowledged_at IS NOT NULL
                    AND error_code IS NOT NULL
                WHEN 'failed' THEN
                    target_attempt_id IS NOT NULL
                    AND stopped_at IS NULL
                    AND error_code IS NOT NULL
                WHEN 'unconfirmed' THEN
                    target_attempt_id IS NOT NULL
                    AND stopped_at IS NULL
                    AND error_code IS NOT NULL
                ELSE FALSE
            END
        )
);

CREATE INDEX idx_run_cancellations_unsettled
    ON run_cancellations (updated_at, id)
    WHERE state IN ('requested', 'delivered', 'stopping', 'unconfirmed');

CREATE TABLE run_dead_letters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL UNIQUE,
    final_attempt_no INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    reason_redacted TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT run_dead_letters_final_attempt_positive
        CHECK (final_attempt_no > 0),
    CONSTRAINT run_dead_letters_run_attempt_fk
        FOREIGN KEY (run_id, final_attempt_no)
        REFERENCES run_attempts (run_id, attempt_no)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE runtime_signal_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    run_id UUID,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    status TEXT NOT NULL DEFAULT 'pending',
    lease_owner UUID,
    lease_expires_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    CONSTRAINT runtime_signal_outbox_run_agent_fk
        FOREIGN KEY (run_id, agent_id)
        REFERENCES runs(id, agent_id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT runtime_signal_outbox_event_type_format
        CHECK (event_type ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$'),
    CONSTRAINT runtime_signal_outbox_payload_object
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT runtime_signal_outbox_status_valid
        CHECK (status IN ('pending', 'processing', 'published')),
    CONSTRAINT runtime_signal_outbox_lease_consistent
        CHECK (
            (status = 'processing' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR (status <> 'processing' AND lease_owner IS NULL AND lease_expires_at IS NULL)
        ),
    CONSTRAINT runtime_signal_outbox_published_consistent
        CHECK (
            (status = 'published' AND published_at IS NOT NULL)
            OR (status <> 'published' AND published_at IS NULL)
        ),
    CONSTRAINT runtime_signal_outbox_attempts_nonnegative
        CHECK (attempt_count >= 0)
);

CREATE INDEX idx_runtime_signal_outbox_pending
    ON runtime_signal_outbox (available_at, created_at, id)
    WHERE status = 'pending';

CREATE INDEX idx_runtime_signal_outbox_processing_expiry
    ON runtime_signal_outbox (lease_expires_at, id)
    WHERE status = 'processing';

CREATE TABLE run_effect_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    terminal_event_id UUID NOT NULL,
    effect_type TEXT NOT NULL,
    target_key TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_owner UUID,
    lease_expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 12,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT run_effect_outbox_business_unique
        UNIQUE (run_id, effect_type, target_key),
    CONSTRAINT run_effect_outbox_terminal_event_fk
        FOREIGN KEY (run_id, terminal_event_id)
        REFERENCES run_events (run_id, id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT run_effect_outbox_type_format
        CHECK (effect_type ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$'),
    CONSTRAINT run_effect_outbox_target_len
        CHECK (char_length(target_key) BETWEEN 1 AND 500),
    CONSTRAINT run_effect_outbox_metadata_object
        CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT run_effect_outbox_status_valid
        CHECK (status IN ('pending', 'processing', 'succeeded', 'dead_letter')),
    CONSTRAINT run_effect_outbox_lease_consistent
        CHECK (
            (status = 'processing' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR (status <> 'processing' AND lease_owner IS NULL AND lease_expires_at IS NULL)
        ),
    CONSTRAINT run_effect_outbox_completed_consistent
        CHECK (
            (status = 'succeeded' AND completed_at IS NOT NULL)
            OR (status <> 'succeeded' AND completed_at IS NULL)
        ),
    CONSTRAINT run_effect_outbox_attempts
        CHECK (
            attempt_count >= 0
            AND max_attempts BETWEEN 1 AND 100
            AND attempt_count <= max_attempts
        )
);

CREATE INDEX idx_run_effect_outbox_pending
    ON run_effect_outbox (available_at, created_at, id)
    WHERE status = 'pending';

CREATE INDEX idx_run_effect_outbox_processing_expiry
    ON run_effect_outbox (lease_expires_at, id)
    WHERE status = 'processing';

CREATE TABLE run_accounting_ledger (
    run_id UUID PRIMARY KEY,
    terminal_event_id UUID NOT NULL UNIQUE,
    agent_id UUID NOT NULL,
    success_delta INTEGER NOT NULL,
    revenue_delta_cents BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT run_accounting_ledger_terminal_event_fk
        FOREIGN KEY (run_id, terminal_event_id)
        REFERENCES run_events (run_id, id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT run_accounting_ledger_run_agent_fk
        FOREIGN KEY (run_id, agent_id)
        REFERENCES runs (id, agent_id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT run_accounting_ledger_success_delta
        CHECK (success_delta IN (0, 1)),
    CONSTRAINT run_accounting_ledger_revenue_nonnegative
        CHECK (revenue_delta_cents >= 0)
);

ALTER TABLE webhook_deliveries
    ADD COLUMN effect_outbox_id UUID,
    ADD CONSTRAINT webhook_deliveries_effect_outbox_fk
        FOREIGN KEY (effect_outbox_id)
        REFERENCES run_effect_outbox(id)
        ON DELETE RESTRICT;

CREATE UNIQUE INDEX idx_webhook_deliveries_effect_outbox
    ON webhook_deliveries (effect_outbox_id)
    WHERE effect_outbox_id IS NOT NULL;

ALTER TABLE run_deliveries
    DROP CONSTRAINT run_deliveries_target_id_fkey,
    ALTER COLUMN target_id DROP NOT NULL,
    ADD CONSTRAINT run_deliveries_target_id_fkey
        FOREIGN KEY (target_id)
        REFERENCES delivery_targets(id)
        ON DELETE SET NULL,
    ADD COLUMN effect_outbox_id UUID,
    ADD CONSTRAINT run_deliveries_effect_outbox_fk
        FOREIGN KEY (effect_outbox_id)
        REFERENCES run_effect_outbox(id)
        ON DELETE RESTRICT;

CREATE UNIQUE INDEX idx_run_deliveries_effect_outbox
    ON run_deliveries (effect_outbox_id)
    WHERE effect_outbox_id IS NOT NULL;

ALTER TABLE task_callback_deliveries
    ADD COLUMN effect_outbox_id UUID,
    ADD CONSTRAINT task_callback_deliveries_effect_outbox_fk
        FOREIGN KEY (effect_outbox_id)
        REFERENCES run_effect_outbox(id)
        ON DELETE RESTRICT;

CREATE UNIQUE INDEX idx_task_callback_deliveries_effect_outbox
    ON task_callback_deliveries (effect_outbox_id)
    WHERE effect_outbox_id IS NOT NULL;

WITH matched AS (
    SELECT
        r.id AS run_id,
        (
            SELECT e.id
            FROM run_events e
            WHERE e.run_id = r.id
              AND (
                  (
                      r.status = 'success'
                      AND e.event_type = 'run.completed'
                      AND (e.payload->>'status' IS NULL OR e.payload->>'status' = 'success')
                  )
                  OR
                  (
                      r.status = 'failed'
                      AND e.event_type = 'run.failed'
                      AND (e.payload->>'status' IS NULL OR e.payload->>'status' = 'failed')
                  )
                  OR
                  (
                      r.status = 'timeout'
                      AND e.event_type = 'run.failed'
                      AND (
                          e.payload->>'status' IS NULL
                          OR e.payload->>'status' IN ('timeout', 'failed')
                      )
                  )
                  OR
                  (
                      r.status = 'canceled'
                      AND e.event_type = 'run.canceled'
                      AND (e.payload->>'status' IS NULL OR e.payload->>'status' = 'canceled')
                  )
              )
            ORDER BY e.sequence DESC, e.id DESC
            LIMIT 1
        ) AS terminal_event_id
    FROM runs r
)
UPDATE runs r
SET terminal_event_id = matched.terminal_event_id
FROM matched
WHERE r.id = matched.run_id;

WITH missing AS (
    SELECT
        r.id AS run_id,
        r.status,
        r.finished_at,
        d.parent_run_id,
        COALESCE(MAX(e.sequence), 0) + 1 AS next_sequence
    FROM runs r
    LEFT JOIN run_events e ON e.run_id = r.id
    LEFT JOIN run_delegations d ON d.child_run_id = r.id
    WHERE r.terminal_event_id IS NULL
    GROUP BY r.id, r.status, r.finished_at, d.parent_run_id
)
INSERT INTO run_events (
    run_id,
    parent_run_id,
    sequence,
    event_type,
    payload,
    created_at
)
SELECT
    missing.run_id,
    missing.parent_run_id,
    missing.next_sequence,
    'run.status.changed',
    jsonb_build_object(
        'status', missing.status,
        'terminal', TRUE,
        'migrated', TRUE,
        'migration', '063_reliable_runtime_v2'
    ),
    missing.finished_at
FROM missing;

UPDATE runs r
SET terminal_event_id = e.id
FROM run_events e
WHERE r.terminal_event_id IS NULL
  AND e.run_id = r.id
  AND e.event_type = 'run.status.changed'
  AND e.payload @> '{"terminal":true,"migrated":true,"migration":"063_reliable_runtime_v2"}'::jsonb;

INSERT INTO run_accounting_ledger (
    run_id,
    terminal_event_id,
    agent_id,
    success_delta,
    revenue_delta_cents,
    created_at
)
SELECT
    r.id,
    r.terminal_event_id,
    r.agent_id,
    CASE WHEN r.status = 'success' THEN 1 ELSE 0 END,
    CASE WHEN r.status = 'success' THEN r.creator_revenue_cents::bigint ELSE 0 END,
    COALESCE(r.finished_at, clock_timestamp())
FROM runs r;

ALTER TABLE run_events
    ADD CONSTRAINT run_events_client_identity_consistent
        CHECK (
            (
                client_event_id IS NULL
                AND client_event_seq IS NULL
                AND payload_fingerprint IS NULL
                AND attempt_id IS NULL
                AND attempt_no IS NULL
                AND fencing_token IS NULL
            )
            OR
            (
                client_event_id IS NOT NULL
                AND client_event_seq IS NOT NULL
                AND payload_fingerprint IS NOT NULL
                AND attempt_id IS NOT NULL
                AND attempt_no IS NOT NULL
                AND fencing_token IS NOT NULL
                AND client_event_seq > 0
                AND attempt_no > 0
                AND fencing_token > 0
                AND octet_length(payload_fingerprint) = 32
            )
        ),
    ADD CONSTRAINT run_events_payload_object
        CHECK (jsonb_typeof(payload) = 'object'),
    ADD CONSTRAINT run_events_attempt_identity_fk
        FOREIGN KEY (run_id, attempt_id, attempt_no, fencing_token)
        REFERENCES run_attempts (run_id, id, attempt_no, fencing_token)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX idx_run_events_client_event_id
    ON run_events (run_id, client_event_id)
    WHERE client_event_id IS NOT NULL;

CREATE UNIQUE INDEX idx_run_events_attempt_client_sequence
    ON run_events (run_id, attempt_no, client_event_seq)
    WHERE client_event_seq IS NOT NULL;

ALTER TABLE runs
    ADD CONSTRAINT runs_runtime_contract_id_len
        CHECK (char_length(runtime_contract_id) BETWEEN 1 AND 200),
    ADD CONSTRAINT runs_runtime_contract_id_valid
        CHECK (runtime_contract_id IN ('legacy.pre-v2', 'openlinker.runtime.v2')),
    ADD CONSTRAINT runs_idempotency_consistent
        CHECK (
            (
                runtime_contract_id = 'legacy.pre-v2'
                AND
                idempotency_key_hash IS NULL
                AND idempotency_fingerprint IS NULL
            )
            OR
            (
                runtime_contract_id = 'openlinker.runtime.v2'
                AND
                idempotency_key_hash IS NOT NULL
                AND idempotency_fingerprint IS NOT NULL
                AND octet_length(idempotency_key_hash) = 32
                AND octet_length(idempotency_fingerprint) = 32
            )
        ),
    ADD CONSTRAINT runs_connection_snapshot_consistent
        CHECK (
            (
                runtime_contract_id = 'legacy.pre-v2'
                AND connection_mode_snapshot IS NULL
                AND endpoint_idempotency_snapshot IS NULL
            )
            OR
            (
                runtime_contract_id = 'openlinker.runtime.v2'
                AND connection_mode_snapshot IN (
                    'direct_http',
                    'mcp_server',
                    'runtime_pull',
                    'runtime_ws'
                )
                AND (
                    connection_mode_snapshot NOT IN ('direct_http', 'mcp_server')
                    OR endpoint_idempotency_snapshot IS NOT NULL
                )
            )
        ),
    ADD CONSTRAINT runs_connection_mode_snapshot_valid
        CHECK (
            connection_mode_snapshot IS NULL
            OR connection_mode_snapshot IN (
                'direct_http',
                'mcp_server',
                'runtime_pull',
                'runtime_ws'
            )
        ),
    ADD CONSTRAINT runs_dispatch_state_valid
        CHECK (
            dispatch_state IN (
                'pending',
                'offered',
                'executing',
                'retry_wait',
                'terminal',
                'dead_letter'
            )
        ),
    ADD CONSTRAINT runs_status_dispatch_consistent
        CHECK (
            (
                status = 'running'
                AND dispatch_state IN ('pending', 'offered', 'executing', 'retry_wait')
            )
            OR
            (
                status IN ('success', 'failed', 'timeout', 'canceled')
                AND dispatch_state = 'terminal'
            )
            OR
            (
                status = 'failed'
                AND dispatch_state = 'dead_letter'
                AND error_code = 'RUNTIME_RETRY_EXHAUSTED'
            )
        ),
    ADD CONSTRAINT runs_finished_state_consistent
        CHECK (
            (status = 'running' AND finished_at IS NULL)
            OR (status <> 'running' AND finished_at IS NOT NULL)
        ),
    ADD CONSTRAINT runs_counter_ranges
        CHECK (
            offer_count >= 0
            AND max_offer_count BETWEEN 1 AND 100
            AND offer_count <= max_offer_count
            AND attempt_count >= 0
            AND max_attempts BETWEEN 1 AND 20
            AND attempt_count <= max_attempts
            AND fencing_token >= 0
        ),
    ADD CONSTRAINT runs_retry_state_consistent
        CHECK (
            (dispatch_state = 'retry_wait' AND next_attempt_at IS NOT NULL)
            OR (dispatch_state <> 'retry_wait' AND next_attempt_at IS NULL)
        ),
    ADD CONSTRAINT runs_active_attempt_state
        CHECK (
            (
                dispatch_state IN ('offered', 'executing')
                AND active_attempt_id IS NOT NULL
                AND latest_attempt_id = active_attempt_id
                AND lease_id IS NOT NULL
                AND fencing_token > 0
                AND executor_type IS NOT NULL
                AND active_core_instance_id IS NOT NULL
                AND lease_offered_at IS NOT NULL
                AND lease_expires_at IS NOT NULL
                AND attempt_deadline_at IS NOT NULL
            )
            OR
            (
                dispatch_state NOT IN ('offered', 'executing')
                AND active_attempt_id IS NULL
                AND lease_id IS NULL
                AND executor_type IS NULL
                AND active_core_instance_id IS NULL
                AND runtime_node_id IS NULL
                AND runtime_worker_id IS NULL
                AND runtime_session_id IS NULL
                AND lease_token_id IS NULL
                AND lease_offered_at IS NULL
                AND lease_accepted_at IS NULL
                AND lease_expires_at IS NULL
                AND attempt_deadline_at IS NULL
            )
        ),
    ADD CONSTRAINT runs_offer_acceptance_state
        CHECK (
            (dispatch_state = 'offered' AND lease_accepted_at IS NULL)
            OR (dispatch_state = 'executing' AND lease_accepted_at IS NOT NULL)
            OR dispatch_state NOT IN ('offered', 'executing')
        ),
    ADD CONSTRAINT runs_executor_identity
        CHECK (
            dispatch_state NOT IN ('offered', 'executing')
            OR (
                executor_type = 'agent_node'
                AND runtime_node_id IS NOT NULL
                AND runtime_worker_id IS NOT NULL
                AND runtime_session_id IS NOT NULL
                AND lease_token_id IS NOT NULL
            )
            OR (
                executor_type IN ('core_http', 'core_mcp')
                AND runtime_node_id IS NULL
                AND runtime_worker_id IS NULL
                AND runtime_session_id IS NULL
                AND lease_token_id IS NULL
            )
        ),
    ADD CONSTRAINT runs_terminal_event_consistent
        CHECK (
            (
                dispatch_state IN ('terminal', 'dead_letter')
                AND terminal_event_id IS NOT NULL
            )
            OR (
                dispatch_state NOT IN ('terminal', 'dead_letter')
                AND terminal_event_id IS NULL
            )
        ),
    ADD CONSTRAINT runs_terminal_attempt_evidence
        CHECK (
            status NOT IN ('success', 'failed')
            OR latest_attempt_id IS NOT NULL
            OR runtime_contract_id = 'legacy.pre-v2'
        ),
    ADD CONSTRAINT runs_dead_letter_consistent
        CHECK (
            (
                dispatch_state = 'dead_letter'
                AND dead_lettered_at IS NOT NULL
                AND error_code = 'RUNTIME_RETRY_EXHAUSTED'
            )
            OR (
                dispatch_state <> 'dead_letter'
                AND dead_lettered_at IS NULL
                AND (
                    runtime_contract_id = 'legacy.pre-v2'
                    OR error_code IS DISTINCT FROM 'RUNTIME_RETRY_EXHAUSTED'
                )
            )
        ),
    ADD CONSTRAINT runs_result_consistent
        CHECK (
            (result_id IS NULL AND result_fingerprint IS NULL)
            OR (
                status IN ('success', 'failed')
                AND
                result_id IS NOT NULL
                AND result_fingerprint IS NOT NULL
                AND octet_length(result_fingerprint) = 32
            )
        ),
    ADD CONSTRAINT runs_nonresult_terminal_output_consistent
        CHECK (
            status NOT IN ('timeout', 'canceled')
            OR output IS NULL
        ),
    ADD CONSTRAINT runs_cancel_state_valid
        CHECK (
            cancel_state IS NULL
            OR cancel_state IN (
                'requested',
                'delivered',
                'stopping',
                'stopped',
                'unsupported',
                'failed',
                'unconfirmed'
            )
        ),
    ADD CONSTRAINT runs_cancel_summary_consistent
        CHECK (
            (
                cancel_request_id IS NULL
                AND cancel_state IS NULL
                AND cancel_requested_at IS NULL
                AND cancel_acknowledged_at IS NULL
                AND cancel_reason IS NULL
            )
            OR
            (
                cancel_request_id IS NOT NULL
                AND cancel_state IS NOT NULL
                AND cancel_requested_at IS NOT NULL
            )
        ),
    ADD CONSTRAINT runs_deadline_order
        CHECK (
            (
                runtime_contract_id = 'legacy.pre-v2'
                AND dispatch_deadline_at IS NULL
                AND run_deadline_at IS NULL
            )
            OR (
                runtime_contract_id = 'openlinker.runtime.v2'
                AND dispatch_deadline_at IS NOT NULL
                AND run_deadline_at IS NOT NULL
                AND dispatch_deadline_at <= run_deadline_at
                AND (
                    lease_expires_at IS NULL
                    OR lease_offered_at IS NULL
                    OR lease_expires_at >= lease_offered_at
                )
                AND (
                    attempt_deadline_at IS NULL
                    OR lease_offered_at IS NULL
                    OR attempt_deadline_at >= lease_offered_at
                )
                AND (
                    attempt_deadline_at IS NULL
                    OR attempt_deadline_at <= run_deadline_at
                )
            )
        ),
    ADD CONSTRAINT runs_replay_distinct
        CHECK (replay_of_run_id IS NULL OR replay_of_run_id <> id),
    ADD CONSTRAINT runs_replay_fk
        FOREIGN KEY (replay_of_run_id)
        REFERENCES runs(id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT runs_runtime_node_fk
        FOREIGN KEY (runtime_node_id)
        REFERENCES runtime_nodes(node_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT runs_lease_token_agent_fk
        FOREIGN KEY (lease_token_id, agent_id)
        REFERENCES agent_tokens(id, agent_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT runs_runtime_session_identity_fk
        FOREIGN KEY (
            runtime_session_id,
            runtime_node_id,
            agent_id,
            lease_token_id,
            runtime_worker_id
        )
        REFERENCES runtime_sessions (
            runtime_session_id,
            node_id,
            agent_id,
            credential_id,
            worker_id
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT runs_latest_attempt_fk
        FOREIGN KEY (id, latest_attempt_id)
        REFERENCES run_attempts(run_id, id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT runs_active_attempt_fk
        FOREIGN KEY (id, active_attempt_id)
        REFERENCES run_attempts(run_id, id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT runs_result_attempt_fk
        FOREIGN KEY (id, result_id)
        REFERENCES run_attempts(run_id, result_id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT runs_terminal_event_fk
        FOREIGN KEY (id, terminal_event_id)
        REFERENCES run_events(run_id, id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT runs_cancellation_fk
        FOREIGN KEY (id, cancel_request_id)
        REFERENCES run_cancellations(run_id, id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX idx_runs_idempotency_key
    ON runs (user_id, idempotency_key_hash)
    WHERE idempotency_key_hash IS NOT NULL;

CREATE INDEX idx_runs_runtime_pending
    ON runs (agent_id, started_at, id)
    WHERE status = 'running' AND dispatch_state = 'pending';

CREATE INDEX idx_runs_runtime_pending_global
    ON runs (started_at, id)
    WHERE status = 'running' AND dispatch_state = 'pending';

CREATE INDEX idx_runs_runtime_retry_due
    ON runs (agent_id, next_attempt_at, started_at, id)
    WHERE status = 'running' AND dispatch_state = 'retry_wait';

CREATE INDEX idx_runs_runtime_retry_due_global
    ON runs (next_attempt_at, started_at, id)
    WHERE status = 'running' AND dispatch_state = 'retry_wait';

CREATE INDEX idx_runs_runtime_offer_expiry
    ON runs (lease_expires_at, id)
    WHERE status = 'running' AND dispatch_state = 'offered';

CREATE INDEX idx_runs_runtime_execution_expiry
    ON runs (lease_expires_at, id)
    WHERE status = 'running' AND dispatch_state = 'executing';

CREATE INDEX idx_runs_runtime_deadline
    ON runs (run_deadline_at, id)
    WHERE status = 'running' AND run_deadline_at IS NOT NULL;

CREATE INDEX idx_runs_replay_lineage
    ON runs (replay_of_run_id, started_at, id)
    WHERE replay_of_run_id IS NOT NULL;

CREATE OR REPLACE FUNCTION enforce_run_v2_contract_identity()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.runtime_contract_id = 'openlinker.runtime.v2' THEN
            RAISE EXCEPTION 'runtime v2 runs cannot be deleted';
        END IF;
        RETURN OLD;
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.runtime_contract_id <> 'openlinker.runtime.v2'
           OR NEW.idempotency_key_hash IS NULL
           OR octet_length(NEW.idempotency_key_hash) <> 32
           OR NEW.idempotency_fingerprint IS NULL
           OR octet_length(NEW.idempotency_fingerprint) <> 32
           OR NEW.connection_mode_snapshot IS NULL
           OR NEW.dispatch_deadline_at IS NULL
           OR NEW.run_deadline_at IS NULL
           OR NEW.dispatch_deadline_at <= clock_timestamp()
           OR NEW.run_deadline_at <= NEW.dispatch_deadline_at
           OR (
               NEW.connection_mode_snapshot IN ('direct_http', 'mcp_server')
               AND NEW.endpoint_idempotency_snapshot IS NULL
           )
           OR NEW.status <> 'running'
           OR NEW.dispatch_state <> 'pending'
           OR NEW.output IS NOT NULL
           OR NEW.error_code IS NOT NULL
           OR NEW.error_message IS NOT NULL
           OR NEW.duration_ms IS NOT NULL
           OR NEW.finished_at IS NOT NULL
           OR NEW.offer_count <> 0
           OR NEW.attempt_count <> 0
           OR NEW.fencing_token <> 0
           OR NEW.latest_attempt_id IS NOT NULL
           OR NEW.active_attempt_id IS NOT NULL
           OR NEW.lease_id IS NOT NULL
           OR NEW.executor_type IS NOT NULL
           OR NEW.cancel_request_id IS NOT NULL
           OR NEW.result_id IS NOT NULL
           OR NEW.terminal_event_id IS NOT NULL
           OR NEW.dead_lettered_at IS NOT NULL THEN
            RAISE EXCEPTION 'runtime v2 run insert requires complete v2 creation identity';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.runtime_contract_id IS DISTINCT FROM OLD.runtime_contract_id THEN
        RAISE EXCEPTION 'run runtime contract is immutable';
    END IF;

    IF ROW(
        NEW.id,
        NEW.user_id,
        NEW.agent_id,
        NEW.input,
        NEW.cost_cents,
        NEW.platform_fee_cents,
        NEW.creator_revenue_cents,
        NEW.source,
        NEW.started_at,
        NEW.idempotency_key_hash,
        NEW.idempotency_fingerprint,
        NEW.connection_mode_snapshot,
        NEW.endpoint_idempotency_snapshot,
        NEW.max_offer_count,
        NEW.max_attempts,
        NEW.dispatch_deadline_at,
        NEW.run_deadline_at,
        NEW.replay_of_run_id
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.user_id,
        OLD.agent_id,
        OLD.input,
        OLD.cost_cents,
        OLD.platform_fee_cents,
        OLD.creator_revenue_cents,
        OLD.source,
        OLD.started_at,
        OLD.idempotency_key_hash,
        OLD.idempotency_fingerprint,
        OLD.connection_mode_snapshot,
        OLD.endpoint_idempotency_snapshot,
        OLD.max_offer_count,
        OLD.max_attempts,
        OLD.dispatch_deadline_at,
        OLD.run_deadline_at,
        OLD.replay_of_run_id
    ) THEN
        RAISE EXCEPTION 'run creation identity is immutable';
    END IF;

    IF OLD.cancel_request_id IS NOT NULL
       AND ROW(
           NEW.cancel_request_id,
           NEW.cancel_requested_at,
           NEW.cancel_reason
       ) IS DISTINCT FROM ROW(
           OLD.cancel_request_id,
           OLD.cancel_requested_at,
           OLD.cancel_reason
       ) THEN
        RAISE EXCEPTION 'run cancellation request identity is immutable';
    END IF;

    IF OLD.dispatch_state IN ('terminal', 'dead_letter')
       AND OLD.cancel_request_id IS NULL
       AND ROW(
           NEW.cancel_request_id,
           NEW.cancel_state,
           NEW.cancel_requested_at,
           NEW.cancel_acknowledged_at,
           NEW.cancel_reason
       ) IS DISTINCT FROM ROW(
           OLD.cancel_request_id,
           OLD.cancel_state,
           OLD.cancel_requested_at,
           OLD.cancel_acknowledged_at,
           OLD.cancel_reason
       ) THEN
        RAISE EXCEPTION 'terminal run cannot acquire a cancellation request';
    END IF;

    IF OLD.dispatch_state IN ('terminal', 'dead_letter')
       AND (
           to_jsonb(NEW) - ARRAY[
               'cancel_request_id',
               'cancel_state',
               'cancel_requested_at',
               'cancel_acknowledged_at',
               'cancel_reason'
           ]::TEXT[]
       ) IS DISTINCT FROM (
           to_jsonb(OLD) - ARRAY[
               'cancel_request_id',
               'cancel_state',
               'cancel_requested_at',
               'cancel_acknowledged_at',
               'cancel_reason'
           ]::TEXT[]
       ) THEN
        RAISE EXCEPTION 'terminal run facts are immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER runs_v2_contract_identity
    BEFORE INSERT OR UPDATE OR DELETE ON runs
    FOR EACH ROW EXECUTE FUNCTION enforce_run_v2_contract_identity();

CREATE OR REPLACE FUNCTION enforce_run_attempt_identity_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'run attempts are immutable history and cannot be deleted';
    END IF;

    IF ROW(
        OLD.id,
        OLD.run_id,
        OLD.agent_id,
        OLD.offer_no,
        OLD.executor_type,
        OLD.lease_id,
        OLD.fencing_token,
        OLD.runtime_token_id,
        OLD.runtime_worker_id,
        OLD.runtime_session_id,
        OLD.node_id,
        OLD.offered_by_core_instance_id,
        OLD.offered_at,
        OLD.offer_expires_at,
        OLD.attempt_deadline_at,
        OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.id,
        NEW.run_id,
        NEW.agent_id,
        NEW.offer_no,
        NEW.executor_type,
        NEW.lease_id,
        NEW.fencing_token,
        NEW.runtime_token_id,
        NEW.runtime_worker_id,
        NEW.runtime_session_id,
        NEW.node_id,
        NEW.offered_by_core_instance_id,
        NEW.offered_at,
        NEW.offer_expires_at,
        NEW.attempt_deadline_at,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'run attempt immutable identity cannot change';
    END IF;

    IF OLD.attempt_no IS NOT NULL
       AND NEW.attempt_no IS DISTINCT FROM OLD.attempt_no THEN
        RAISE EXCEPTION 'run attempt number cannot change after acceptance';
    END IF;

    IF OLD.accepted_at IS NOT NULL
       AND NEW.accepted_at IS DISTINCT FROM OLD.accepted_at THEN
        RAISE EXCEPTION 'run attempt acceptance evidence is immutable';
    END IF;

    IF NEW.last_client_event_seq < OLD.last_client_event_seq THEN
        RAISE EXCEPTION 'run attempt event sequence cannot move backwards';
    END IF;

    IF OLD.final_client_event_seq IS NOT NULL
       AND NEW.final_client_event_seq IS DISTINCT FROM OLD.final_client_event_seq THEN
        RAISE EXCEPTION 'run attempt final event sequence is immutable';
    END IF;

    IF NEW.lease_expires_at < OLD.lease_expires_at THEN
        RAISE EXCEPTION 'run attempt lease expiry cannot move backwards';
    END IF;

    IF OLD.last_renewed_at IS NOT NULL
       AND (
           NEW.last_renewed_at IS NULL
           OR NEW.last_renewed_at < OLD.last_renewed_at
       ) THEN
        RAISE EXCEPTION 'run attempt renewal evidence cannot move backwards';
    END IF;

    IF OLD.result_id IS NOT NULL
       AND ROW(
           NEW.result_id,
           NEW.result_fingerprint,
           NEW.result_classification
       ) IS DISTINCT FROM ROW(
           OLD.result_id,
           OLD.result_fingerprint,
           OLD.result_classification
       ) THEN
        RAISE EXCEPTION 'run attempt result identity is immutable';
    END IF;

    IF OLD.finished_at IS NOT NULL
       AND ROW(
           NEW.finished_at,
           NEW.outcome,
           NEW.error_code,
           NEW.error_detail_redacted,
           NEW.attached_core_instance_id,
           NEW.lease_expires_at,
           NEW.last_renewed_at,
           NEW.last_client_event_seq,
           NEW.final_client_event_seq,
           NEW.result_id,
           NEW.result_fingerprint,
           NEW.result_classification
       ) IS DISTINCT FROM ROW(
           OLD.finished_at,
           OLD.outcome,
           OLD.error_code,
           OLD.error_detail_redacted,
           OLD.attached_core_instance_id,
           OLD.lease_expires_at,
           OLD.last_renewed_at,
           OLD.last_client_event_seq,
           OLD.final_client_event_seq,
           OLD.result_id,
           OLD.result_fingerprint,
           OLD.result_classification
       ) THEN
        RAISE EXCEPTION 'run attempt terminal evidence is immutable';
    END IF;

    IF OLD.result_acknowledged_at IS NOT NULL
       AND NEW.result_acknowledged_at IS DISTINCT FROM OLD.result_acknowledged_at THEN
        RAISE EXCEPTION 'run attempt result acknowledgement is immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER run_attempts_identity_immutable
    BEFORE UPDATE OR DELETE ON run_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_run_attempt_identity_immutable();

CREATE OR REPLACE FUNCTION enforce_run_event_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'run events are append-only';
    END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER run_events_immutable
    BEFORE UPDATE OR DELETE ON run_events
    FOR EACH ROW EXECUTE FUNCTION enforce_run_event_immutable();

CREATE OR REPLACE FUNCTION enforce_attempt_event_sequence_consistency()
RETURNS TRIGGER AS $$
DECLARE
    target_run_id UUID;
    target_attempt_id UUID;
    stored_last_sequence BIGINT;
    observed_last_sequence BIGINT;
    observed_event_count BIGINT;
    stored_result_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'run_attempts' THEN
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
            target_attempt_id := OLD.id;
        ELSE
            target_run_id := NEW.run_id;
            target_attempt_id := NEW.id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
            target_attempt_id := OLD.attempt_id;
        ELSE
            target_run_id := NEW.run_id;
            target_attempt_id := NEW.attempt_id;
        END IF;
    END IF;

    IF target_attempt_id IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT last_client_event_seq, result_id
    INTO stored_last_sequence, stored_result_id
    FROM run_attempts
    WHERE run_id = target_run_id
      AND id = target_attempt_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT COALESCE(MAX(client_event_seq), 0), COUNT(client_event_seq)
    INTO observed_last_sequence, observed_event_count
    FROM run_events
    WHERE run_id = target_run_id
      AND attempt_id = target_attempt_id
      AND client_event_seq IS NOT NULL;

    IF stored_last_sequence <> observed_last_sequence THEN
        RAISE EXCEPTION 'run attempt event sequence summary does not match stored events';
    END IF;

    IF stored_result_id IS NOT NULL
       AND observed_event_count <> stored_last_sequence THEN
        RAISE EXCEPTION 'run attempt Result cannot finalize with missing client events';
    END IF;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER run_attempts_event_sequence_consistency
    AFTER INSERT OR UPDATE OR DELETE ON run_attempts
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_attempt_event_sequence_consistency();

CREATE CONSTRAINT TRIGGER run_events_attempt_sequence_consistency
    AFTER INSERT OR DELETE ON run_events
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_attempt_event_sequence_consistency();

CREATE OR REPLACE FUNCTION enforce_run_cancellation_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'run cancellation evidence cannot be deleted';
    END IF;

    IF ROW(
        NEW.id,
        NEW.run_id,
        NEW.target_attempt_id,
        NEW.requested_by_type,
        NEW.requested_by_id,
        NEW.reason,
        NEW.requested_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.run_id,
        OLD.target_attempt_id,
        OLD.requested_by_type,
        OLD.requested_by_id,
        OLD.reason,
        OLD.requested_at
    ) THEN
        RAISE EXCEPTION 'cancellation request identity is immutable';
    END IF;

    IF OLD.state IN ('stopped', 'unsupported', 'failed')
       AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'terminal cancellation state cannot change';
    END IF;

    IF NEW.state <> OLD.state
       AND NOT (
           (OLD.state = 'requested' AND NEW.state IN (
               'delivered', 'stopping', 'stopped', 'unsupported', 'failed', 'unconfirmed'
           ))
           OR (OLD.state = 'delivered' AND NEW.state IN (
               'stopping', 'stopped', 'unsupported', 'failed', 'unconfirmed'
           ))
           OR (OLD.state = 'stopping' AND NEW.state IN (
               'stopped', 'failed', 'unconfirmed'
           ))
           OR (OLD.state = 'unconfirmed' AND NEW.state IN (
               'stopped', 'unsupported', 'failed'
           ))
       ) THEN
        RAISE EXCEPTION 'invalid cancellation state transition';
    END IF;

    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'cancellation updated_at cannot move backwards';
    END IF;

    IF (OLD.delivered_at IS NOT NULL
        AND NEW.delivered_at IS DISTINCT FROM OLD.delivered_at)
       OR (OLD.stopping_at IS NOT NULL
           AND NEW.stopping_at IS DISTINCT FROM OLD.stopping_at)
       OR (OLD.stopped_at IS NOT NULL
           AND NEW.stopped_at IS DISTINCT FROM OLD.stopped_at)
       OR (OLD.acknowledged_at IS NOT NULL
           AND NEW.acknowledged_at IS DISTINCT FROM OLD.acknowledged_at) THEN
        RAISE EXCEPTION 'cancellation evidence timestamps are immutable once recorded';
    END IF;

    IF OLD.state IN ('stopped', 'unsupported', 'failed')
       AND (
           to_jsonb(NEW) - 'updated_at'
       ) IS DISTINCT FROM (
           to_jsonb(OLD) - 'updated_at'
       ) THEN
        RAISE EXCEPTION 'terminal cancellation evidence is immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER run_cancellations_state_forward_only
    BEFORE UPDATE OR DELETE ON run_cancellations
    FOR EACH ROW EXECUTE FUNCTION enforce_run_cancellation_transition();

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

CREATE TRIGGER runtime_sessions_identity_immutable
    BEFORE UPDATE OR DELETE ON runtime_sessions
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_session_identity_immutable();

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

CREATE TRIGGER runtime_sessions_principal_valid
    BEFORE INSERT OR UPDATE ON runtime_sessions
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_session_principal();

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

CREATE TRIGGER runtime_nodes_identity_and_lifecycle
    BEFORE UPDATE OR DELETE ON runtime_nodes
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_node_identity_and_lifecycle();

CREATE OR REPLACE FUNCTION enforce_agent_token_identity_and_lifecycle()
RETURNS TRIGGER AS $$
DECLARE
    is_redemption BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'agent token credential history cannot be deleted';
    END IF;

    is_redemption := OLD.status = 'pending_registration'
        AND OLD.agent_id IS NULL
        AND OLD.redeemed_at IS NULL
        AND NEW.status = 'active_runtime'
        AND NEW.agent_id IS NOT NULL
        AND NEW.redeemed_at IS NOT NULL
        AND NEW.revoked_at IS NULL
        AND NEW.revocation_kind IS NULL;

    IF ROW(
        NEW.id,
        NEW.creator_user_id,
        NEW.prefix,
        NEW.rotation_predecessor_id,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.creator_user_id,
        OLD.prefix,
        OLD.rotation_predecessor_id,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'agent token creation identity is immutable';
    END IF;

    IF ROW(
        NEW.agent_id,
        NEW.token_hash,
        NEW.scopes,
        NEW.redeemed_at
    ) IS DISTINCT FROM ROW(
        OLD.agent_id,
        OLD.token_hash,
        OLD.scopes,
        OLD.redeemed_at
    ) AND NOT is_redemption THEN
        RAISE EXCEPTION 'redeemed agent token credential identity is immutable';
    END IF;

    IF (OLD.status = 'active_runtime'
        AND NEW.status NOT IN ('active_runtime', 'revoked'))
       OR (OLD.status = 'revoked' AND NEW.status <> 'revoked') THEN
        RAISE EXCEPTION 'agent token lifecycle cannot move backwards';
    END IF;

    IF OLD.status = 'revoked'
       AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
        RAISE EXCEPTION 'revoked agent token is immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER agent_tokens_identity_and_lifecycle
    BEFORE UPDATE OR DELETE ON agent_tokens
    FOR EACH ROW EXECUTE FUNCTION enforce_agent_token_identity_and_lifecycle();

CREATE OR REPLACE FUNCTION enforce_runtime_node_revocation_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'revoked'
       AND (
           OLD.status IS DISTINCT FROM NEW.status
           OR OLD.revoked_at IS DISTINCT FROM NEW.revoked_at
       )
       AND EXISTS (
           SELECT 1
           FROM runtime_sessions
           WHERE node_id = NEW.node_id
             AND status IN ('active', 'draining')
       ) THEN
        RAISE EXCEPTION 'runtime node sessions must close before node revocation';
    END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER runtime_nodes_revocation_guard
    BEFORE UPDATE OF status, revoked_at ON runtime_nodes
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_node_revocation_guard();

CREATE OR REPLACE FUNCTION enforce_runtime_token_revocation_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF (
        NEW.revoked_at IS NOT NULL
        OR NEW.status <> 'active_runtime'
        OR (
            NEW.expires_at IS NOT NULL
            AND NEW.expires_at <= clock_timestamp()
        )
    )
    AND (
        OLD.revoked_at IS DISTINCT FROM NEW.revoked_at
        OR OLD.status IS DISTINCT FROM NEW.status
        OR OLD.expires_at IS DISTINCT FROM NEW.expires_at
    )
    AND EXISTS (
        SELECT 1
        FROM runtime_sessions
        WHERE credential_id = NEW.id
          AND status IN ('active', 'draining')
    ) THEN
        RAISE EXCEPTION 'runtime credential sessions must close before token revocation';
    END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER agent_tokens_runtime_revocation_guard
    BEFORE UPDATE OF status, revoked_at, expires_at ON agent_tokens
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_token_revocation_guard();

CREATE OR REPLACE FUNCTION enforce_runtime_session_attachment_history()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime session attachment history cannot be deleted';
    END IF;

    IF ROW(
        NEW.id,
        NEW.runtime_session_id,
        NEW.core_instance_id,
        NEW.attachment_kind,
        NEW.attached_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.runtime_session_id,
        OLD.core_instance_id,
        OLD.attachment_kind,
        OLD.attached_at
    ) THEN
        RAISE EXCEPTION 'runtime session attachment identity is immutable';
    END IF;

    IF OLD.detached_at IS NOT NULL
       AND ROW(
           NEW.detached_at,
           NEW.disconnect_reason
       ) IS DISTINCT FROM ROW(
           OLD.detached_at,
           OLD.disconnect_reason
       ) THEN
        RAISE EXCEPTION 'detached runtime session attachment is immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER runtime_session_attachments_history
    BEFORE UPDATE OR DELETE ON runtime_session_attachments
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_session_attachment_history();

CREATE OR REPLACE FUNCTION enforce_runtime_session_attachment_consistency()
RETURNS TRIGGER AS $$
DECLARE
    target_session_id UUID;
    session_record runtime_sessions%ROWTYPE;
    active_attachment_count INTEGER;
    active_attachment_core_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'runtime_sessions' THEN
        IF TG_OP = 'DELETE' THEN
            target_session_id := OLD.runtime_session_id;
        ELSE
            target_session_id := NEW.runtime_session_id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_session_id := OLD.runtime_session_id;
        ELSE
            target_session_id := NEW.runtime_session_id;
        END IF;
    END IF;

    SELECT * INTO session_record
    FROM runtime_sessions
    WHERE runtime_session_id = target_session_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT COUNT(*), MIN(core_instance_id::TEXT)::UUID
    INTO active_attachment_count, active_attachment_core_id
    FROM runtime_session_attachments
    WHERE runtime_session_id = target_session_id
      AND detached_at IS NULL;

    IF session_record.status IN ('active', 'draining') THEN
        IF active_attachment_count <> 1
           OR active_attachment_core_id IS DISTINCT FROM session_record.attached_core_instance_id THEN
            RAISE EXCEPTION 'active runtime session attachment does not match its Core instance';
        END IF;
    ELSIF active_attachment_count <> 0 THEN
        RAISE EXCEPTION 'inactive runtime session cannot keep an active attachment';
    END IF;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER runtime_sessions_attachment_consistency
    AFTER INSERT OR UPDATE OR DELETE ON runtime_sessions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_session_attachment_consistency();

CREATE CONSTRAINT TRIGGER runtime_session_attachments_session_consistency
    AFTER INSERT OR UPDATE OR DELETE ON runtime_session_attachments
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_session_attachment_consistency();

CREATE OR REPLACE FUNCTION enforce_runtime_node_session_contract_consistency()
RETURNS TRIGGER AS $$
DECLARE
    target_node_id UUID;
    node_record runtime_nodes%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME = 'runtime_nodes' THEN
        IF TG_OP = 'DELETE' THEN
            target_node_id := OLD.node_id;
        ELSE
            target_node_id := NEW.node_id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_node_id := OLD.node_id;
        ELSE
            target_node_id := NEW.node_id;
        END IF;
    END IF;

    SELECT * INTO node_record
    FROM runtime_nodes
    WHERE node_id = target_node_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runtime_sessions s
        WHERE s.node_id = node_record.node_id
          AND s.status IN ('active', 'draining')
          AND (
              s.device_certificate_serial <> node_record.device_certificate_serial
              OR s.node_version <> node_record.node_version
              OR s.protocol_version <> node_record.protocol_version
              OR s.runtime_contract_id <> node_record.runtime_contract_id
              OR s.runtime_contract_digest <> node_record.runtime_contract_digest
              OR NOT (s.features @> node_record.features)
              OR NOT (node_record.features @> s.features)
          )
    ) THEN
        RAISE EXCEPTION 'active runtime sessions do not match their Node contract';
    END IF;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER runtime_nodes_session_contract_consistency
    AFTER INSERT OR UPDATE OR DELETE ON runtime_nodes
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_node_session_contract_consistency();

CREATE CONSTRAINT TRIGGER runtime_sessions_node_contract_consistency
    AFTER INSERT OR UPDATE OR DELETE ON runtime_sessions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_node_session_contract_consistency();

CREATE OR REPLACE FUNCTION enforce_run_active_attempt_consistency()
RETURNS TRIGGER AS $$
DECLARE
    target_run_id UUID;
    current_run runs%ROWTYPE;
    active_attempt run_attempts%ROWTYPE;
    latest_attempt run_attempts%ROWTYPE;
    attempt_rows INTEGER;
    max_offer_no INTEGER;
    accepted_attempt_rows INTEGER;
    max_attempt_no INTEGER;
    max_fencing_token BIGINT;
    unfinished_attempt_rows INTEGER;
    unfinished_attempt_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'runs' THEN
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.id;
        ELSE
            target_run_id := NEW.id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
        ELSE
            target_run_id := NEW.run_id;
        END IF;
    END IF;

    SELECT * INTO current_run
    FROM runs
    WHERE id = target_run_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT
        COUNT(*)::INTEGER,
        COALESCE(MAX(offer_no), 0)::INTEGER,
        COUNT(attempt_no)::INTEGER,
        COALESCE(MAX(attempt_no), 0)::INTEGER,
        COALESCE(MAX(fencing_token), 0),
        COUNT(*) FILTER (WHERE finished_at IS NULL)::INTEGER,
        (MIN(id::TEXT) FILTER (WHERE finished_at IS NULL))::UUID
    INTO
        attempt_rows,
        max_offer_no,
        accepted_attempt_rows,
        max_attempt_no,
        max_fencing_token,
        unfinished_attempt_rows,
        unfinished_attempt_id
    FROM run_attempts
    WHERE run_id = current_run.id;

    IF current_run.offer_count <> attempt_rows
       OR current_run.offer_count <> max_offer_no
       OR current_run.attempt_count <> accepted_attempt_rows
       OR current_run.attempt_count <> max_attempt_no
       OR current_run.fencing_token <> max_fencing_token THEN
        RAISE EXCEPTION 'run offer, attempt, or fence counters do not match attempt history';
    END IF;

    IF attempt_rows = 0 THEN
        IF current_run.latest_attempt_id IS NOT NULL THEN
            RAISE EXCEPTION 'run without attempts cannot keep a latest attempt';
        END IF;
    ELSE
        SELECT * INTO latest_attempt
        FROM run_attempts
        WHERE run_id = current_run.id
          AND offer_no = max_offer_no;

        IF NOT FOUND
           OR current_run.latest_attempt_id IS DISTINCT FROM latest_attempt.id THEN
            RAISE EXCEPTION 'run latest attempt does not match latest offer';
        END IF;

        IF current_run.dispatch_state = 'pending'
           AND (
               latest_attempt.finished_at IS NULL
               OR latest_attempt.outcome NOT IN ('offer_rejected', 'offer_expired')
           ) THEN
            RAISE EXCEPTION 'pending Run latest attempt is not a finished offer';
        END IF;

        IF current_run.dispatch_state = 'retry_wait'
           AND (
               latest_attempt.finished_at IS NULL
               OR latest_attempt.outcome NOT IN (
                   'retryable_failure',
                   'lease_expired',
                   'result_unknown'
               )
           ) THEN
            RAISE EXCEPTION 'retry-wait Run latest attempt is not retryable';
        END IF;
    END IF;

    IF current_run.active_attempt_id IS NULL THEN
        IF unfinished_attempt_rows <> 0 THEN
            RAISE EXCEPTION 'unfinished attempt must be the Run active attempt';
        END IF;
        RETURN NULL;
    END IF;

    IF unfinished_attempt_rows <> 1
       OR unfinished_attempt_id IS DISTINCT FROM current_run.active_attempt_id THEN
        RAISE EXCEPTION 'Run active attempt must be its only unfinished attempt';
    END IF;

    SELECT * INTO active_attempt
    FROM run_attempts
    WHERE run_id = current_run.id
      AND id = current_run.active_attempt_id;

    IF NOT FOUND
       OR active_attempt.finished_at IS NOT NULL
       OR active_attempt.outcome IS NOT NULL THEN
        RAISE EXCEPTION 'active run attempt does not exist';
    END IF;

    IF current_run.latest_attempt_id IS DISTINCT FROM active_attempt.id
       OR current_run.agent_id IS DISTINCT FROM active_attempt.agent_id
       OR current_run.lease_id IS DISTINCT FROM active_attempt.lease_id
       OR current_run.fencing_token IS DISTINCT FROM active_attempt.fencing_token
       OR current_run.executor_type IS DISTINCT FROM active_attempt.executor_type
       OR current_run.active_core_instance_id IS DISTINCT FROM active_attempt.attached_core_instance_id
       OR current_run.runtime_node_id IS DISTINCT FROM active_attempt.node_id
       OR current_run.runtime_worker_id IS DISTINCT FROM active_attempt.runtime_worker_id
       OR current_run.runtime_session_id IS DISTINCT FROM active_attempt.runtime_session_id
       OR current_run.lease_token_id IS DISTINCT FROM active_attempt.runtime_token_id
       OR current_run.offer_count IS DISTINCT FROM active_attempt.offer_no
       OR current_run.lease_offered_at IS DISTINCT FROM active_attempt.offered_at
       OR current_run.lease_accepted_at IS DISTINCT FROM active_attempt.accepted_at
       OR current_run.lease_expires_at IS DISTINCT FROM active_attempt.lease_expires_at
       OR current_run.attempt_deadline_at IS DISTINCT FROM active_attempt.attempt_deadline_at THEN
        RAISE EXCEPTION 'run active lease summary does not match active attempt';
    END IF;

    IF current_run.dispatch_state = 'offered'
       AND (
           active_attempt.accepted_at IS NOT NULL
           OR active_attempt.attempt_no IS NOT NULL
       ) THEN
        RAISE EXCEPTION 'offered run points to accepted attempt';
    END IF;

    IF current_run.dispatch_state = 'executing'
       AND (
           active_attempt.accepted_at IS NULL
           OR active_attempt.attempt_no IS DISTINCT FROM current_run.attempt_count
       ) THEN
        RAISE EXCEPTION 'executing run points to unaccepted attempt';
    END IF;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER runs_active_attempt_consistency
    AFTER INSERT OR UPDATE OR DELETE ON runs
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_active_attempt_consistency();

CREATE CONSTRAINT TRIGGER run_attempts_active_run_consistency
    AFTER INSERT OR UPDATE OR DELETE ON run_attempts
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_active_attempt_consistency();

CREATE OR REPLACE FUNCTION enforce_run_terminal_artifact_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' OR NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal Run artifact is immutable';
    END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER run_accounting_ledger_immutable
    BEFORE UPDATE OR DELETE ON run_accounting_ledger
    FOR EACH ROW EXECUTE FUNCTION enforce_run_terminal_artifact_immutable();

CREATE TRIGGER run_dead_letters_immutable
    BEFORE UPDATE OR DELETE ON run_dead_letters
    FOR EACH ROW EXECUTE FUNCTION enforce_run_terminal_artifact_immutable();

CREATE OR REPLACE FUNCTION enforce_run_effect_identity_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Run effect outbox history cannot be deleted';
    END IF;

    IF ROW(
        NEW.id,
        NEW.run_id,
        NEW.terminal_event_id,
        NEW.effect_type,
        NEW.target_key,
        NEW.metadata,
        NEW.max_attempts,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.run_id,
        OLD.terminal_event_id,
        OLD.effect_type,
        OLD.target_key,
        OLD.metadata,
        OLD.max_attempts,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'Run effect delivery identity is immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER run_effect_outbox_identity_immutable
    BEFORE UPDATE OR DELETE ON run_effect_outbox
    FOR EACH ROW EXECUTE FUNCTION enforce_run_effect_identity_immutable();

CREATE OR REPLACE FUNCTION enforce_run_terminal_artifacts_consistency()
RETURNS TRIGGER AS $$
DECLARE
    target_run_id UUID;
    current_run runs%ROWTYPE;
    ledger_record run_accounting_ledger%ROWTYPE;
    dead_letter_record run_dead_letters%ROWTYPE;
    latest_attempt run_attempts%ROWTYPE;
    terminal_event run_events%ROWTYPE;
    terminal_event_count INTEGER;
    cancellation_target_attempt_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'runs' THEN
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.id;
        ELSE
            target_run_id := NEW.id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
        ELSE
            target_run_id := NEW.run_id;
        END IF;
    END IF;

    SELECT * INTO current_run
    FROM runs
    WHERE id = target_run_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT * INTO ledger_record
    FROM run_accounting_ledger
    WHERE run_id = current_run.id;

    IF current_run.dispatch_state IN ('terminal', 'dead_letter') THEN
        IF NOT FOUND
           OR ledger_record.terminal_event_id IS DISTINCT FROM current_run.terminal_event_id
           OR ledger_record.agent_id IS DISTINCT FROM current_run.agent_id
           OR ledger_record.success_delta IS DISTINCT FROM (
                CASE WHEN current_run.status = 'success' THEN 1 ELSE 0 END
           )
           OR ledger_record.revenue_delta_cents IS DISTINCT FROM (
                CASE
                    WHEN current_run.status = 'success'
                        THEN current_run.creator_revenue_cents::BIGINT
                    ELSE 0::BIGINT
                END
           ) THEN
            RAISE EXCEPTION 'terminal Run accounting ledger is missing or inconsistent';
        END IF;
    ELSIF FOUND THEN
        RAISE EXCEPTION 'nonterminal Run cannot have an accounting ledger';
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       THEN
        SELECT COUNT(*)::INTEGER
        INTO terminal_event_count
        FROM run_events
        WHERE run_id = current_run.id
          AND (
              event_type IN ('run.completed', 'run.failed', 'run.canceled')
              OR payload->>'terminal' = 'true'
          );

        IF current_run.dispatch_state IN ('terminal', 'dead_letter')
           AND terminal_event_count <> 1 THEN
            RAISE EXCEPTION 'terminal Run must have exactly one terminal event';
        ELSIF current_run.dispatch_state NOT IN ('terminal', 'dead_letter')
              AND terminal_event_count <> 0 THEN
            RAISE EXCEPTION 'nonterminal Run cannot have a terminal event';
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       AND current_run.dispatch_state IN ('terminal', 'dead_letter') THEN
        SELECT * INTO terminal_event
        FROM run_events
        WHERE run_id = current_run.id
          AND id = current_run.terminal_event_id;

        IF NOT FOUND
           OR terminal_event.payload->>'terminal' IS DISTINCT FROM 'true'
           OR terminal_event.payload->>'status' IS DISTINCT FROM current_run.status
           OR terminal_event.event_type IS DISTINCT FROM (
                CASE current_run.status
                    WHEN 'success' THEN 'run.completed'
                    WHEN 'canceled' THEN 'run.canceled'
                    ELSE 'run.failed'
                END
           ) THEN
            RAISE EXCEPTION 'terminal Run event semantics are inconsistent';
        END IF;
    END IF;

    SELECT * INTO dead_letter_record
    FROM run_dead_letters
    WHERE run_id = current_run.id;

    IF current_run.dispatch_state = 'dead_letter' THEN
        IF NOT FOUND
           OR dead_letter_record.final_attempt_no IS DISTINCT FROM current_run.attempt_count THEN
            RAISE EXCEPTION 'dead-letter Run is missing matching DLQ evidence';
        END IF;

        SELECT * INTO latest_attempt
        FROM run_attempts
        WHERE run_id = current_run.id
          AND id = current_run.latest_attempt_id;

        IF NOT FOUND
           OR latest_attempt.attempt_no IS DISTINCT FROM dead_letter_record.final_attempt_no
           OR latest_attempt.finished_at IS NULL
           OR latest_attempt.outcome NOT IN (
               'retryable_failure',
               'lease_expired',
               'result_unknown'
           ) THEN
            RAISE EXCEPTION 'dead-letter Run final attempt evidence is inconsistent';
        END IF;
    ELSIF FOUND THEN
        RAISE EXCEPTION 'non-DLQ Run cannot have dead-letter evidence';
    END IF;

    IF current_run.result_id IS NOT NULL THEN
        SELECT * INTO latest_attempt
        FROM run_attempts
        WHERE run_id = current_run.id
          AND id = current_run.latest_attempt_id;

        IF NOT FOUND
           OR latest_attempt.result_id IS DISTINCT FROM current_run.result_id
           OR latest_attempt.result_fingerprint IS DISTINCT FROM current_run.result_fingerprint THEN
            RAISE EXCEPTION 'Run result summary does not match its final attempt';
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       AND current_run.status IN ('success', 'failed')
       AND current_run.dispatch_state = 'terminal' THEN
        SELECT * INTO latest_attempt
        FROM run_attempts
        WHERE run_id = current_run.id
          AND id = current_run.latest_attempt_id;

        IF NOT FOUND
           OR latest_attempt.finished_at IS NULL
           OR latest_attempt.outcome IS DISTINCT FROM (
               CASE current_run.status
                   WHEN 'success' THEN 'success'
                   ELSE 'non_retryable_failure'
               END
           )
           OR latest_attempt.result_classification IS DISTINCT FROM (
               CASE current_run.status
                   WHEN 'success' THEN 'success'
                   ELSE 'non_retryable_failure'
               END
           )
           OR latest_attempt.result_id IS DISTINCT FROM current_run.result_id
           OR latest_attempt.result_fingerprint IS DISTINCT FROM current_run.result_fingerprint THEN
            RAISE EXCEPTION 'terminal Run status does not match its final attempt Result';
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       AND current_run.status IN ('timeout', 'canceled') THEN
        IF current_run.status = 'canceled' THEN
            SELECT target_attempt_id
            INTO cancellation_target_attempt_id
            FROM run_cancellations
            WHERE run_id = current_run.id;
        END IF;

        IF current_run.result_id IS NOT NULL
           OR current_run.result_fingerprint IS NOT NULL
           OR current_run.output IS NOT NULL THEN
            RAISE EXCEPTION 'timeout or canceled Run cannot publish a Result';
        END IF;

        IF current_run.latest_attempt_id IS NOT NULL THEN
            SELECT * INTO latest_attempt
            FROM run_attempts
            WHERE run_id = current_run.id
              AND id = current_run.latest_attempt_id;

            IF NOT FOUND
               OR latest_attempt.finished_at IS NULL
               OR (
                   current_run.status = 'timeout'
                   AND latest_attempt.outcome NOT IN (
                       'offer_rejected',
                       'offer_expired',
                       'retryable_failure',
                       'lease_expired',
                       'timeout',
                       'result_unknown'
                   )
               )
               OR (
                   current_run.status = 'canceled'
                   AND cancellation_target_attempt_id IS NULL
                   AND latest_attempt.outcome NOT IN (
                       'offer_rejected',
                       'offer_expired',
                       'retryable_failure',
                       'lease_expired',
                       'result_unknown'
                   )
               ) THEN
                RAISE EXCEPTION 'timeout or canceled Run contradicts its latest attempt';
            END IF;
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       AND current_run.status = 'canceled' THEN
        IF cancellation_target_attempt_id IS NOT NULL
           AND (
               current_run.latest_attempt_id IS DISTINCT FROM cancellation_target_attempt_id
               OR latest_attempt.id IS DISTINCT FROM cancellation_target_attempt_id
               OR latest_attempt.outcome IS DISTINCT FROM 'canceled'
           ) THEN
            RAISE EXCEPTION 'canceled Run target attempt was not ended as canceled';
        END IF;
    END IF;

    IF current_run.replay_of_run_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM runs original
           JOIN run_dead_letters dlq ON dlq.run_id = original.id
           WHERE original.id = current_run.replay_of_run_id
             AND original.dispatch_state = 'dead_letter'
       ) THEN
        RAISE EXCEPTION 'Run replay must reference a real dead-letter Run';
    END IF;

    IF current_run.dispatch_state <> 'dead_letter'
       AND EXISTS (
           SELECT 1
           FROM runs replay
           WHERE replay.replay_of_run_id = current_run.id
       ) THEN
        RAISE EXCEPTION 'a replay source must remain a dead-letter Run';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM run_effect_outbox effect
        WHERE effect.run_id = current_run.id
          AND (
              current_run.dispatch_state NOT IN ('terminal', 'dead_letter')
              OR effect.terminal_event_id IS DISTINCT FROM current_run.terminal_event_id
          )
    ) THEN
        RAISE EXCEPTION 'Run effect outbox does not match the Run terminal event';
    END IF;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER runs_terminal_artifacts_consistency
    AFTER INSERT OR UPDATE OR DELETE ON runs
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_terminal_artifacts_consistency();

CREATE CONSTRAINT TRIGGER run_accounting_ledger_run_consistency
    AFTER INSERT OR UPDATE OR DELETE ON run_accounting_ledger
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_terminal_artifacts_consistency();

CREATE CONSTRAINT TRIGGER run_dead_letters_run_consistency
    AFTER INSERT OR UPDATE OR DELETE ON run_dead_letters
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_terminal_artifacts_consistency();

CREATE CONSTRAINT TRIGGER run_events_terminal_artifacts_consistency
    AFTER INSERT OR UPDATE OR DELETE ON run_events
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_terminal_artifacts_consistency();

CREATE CONSTRAINT TRIGGER run_effect_outbox_run_consistency
    AFTER INSERT OR UPDATE OR DELETE ON run_effect_outbox
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_terminal_artifacts_consistency();

CREATE OR REPLACE FUNCTION enforce_run_cancellation_summary_consistency()
RETURNS TRIGGER AS $$
DECLARE
    target_run_id UUID;
    current_run runs%ROWTYPE;
    cancellation_record run_cancellations%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME = 'runs' THEN
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.id;
        ELSE
            target_run_id := NEW.id;
        END IF;
    ELSE
        IF TG_OP = 'DELETE' THEN
            target_run_id := OLD.run_id;
        ELSE
            target_run_id := NEW.run_id;
        END IF;
    END IF;

    SELECT * INTO current_run
    FROM runs
    WHERE id = target_run_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT * INTO cancellation_record
    FROM run_cancellations
    WHERE run_id = current_run.id;

    IF current_run.cancel_request_id IS NULL THEN
        IF FOUND THEN
            RAISE EXCEPTION 'Run cancellation row is not reflected in its summary';
        END IF;
    ELSE
        IF NOT FOUND
           OR cancellation_record.id IS DISTINCT FROM current_run.cancel_request_id
           OR cancellation_record.state IS DISTINCT FROM current_run.cancel_state
           OR cancellation_record.requested_at IS DISTINCT FROM current_run.cancel_requested_at
           OR cancellation_record.acknowledged_at IS DISTINCT FROM current_run.cancel_acknowledged_at
           OR cancellation_record.reason IS DISTINCT FROM current_run.cancel_reason THEN
            RAISE EXCEPTION 'Run cancellation summary does not match cancellation evidence';
        END IF;
    END IF;

    IF current_run.runtime_contract_id = 'openlinker.runtime.v2' THEN
        IF current_run.status = 'canceled' THEN
            IF current_run.dispatch_state <> 'terminal'
               OR current_run.cancel_request_id IS NULL THEN
                RAISE EXCEPTION 'canceled Run requires cancellation request evidence';
            END IF;
        ELSIF current_run.cancel_request_id IS NOT NULL THEN
            RAISE EXCEPTION 'runtime v2 cancellation must atomically finalize the Run as canceled';
        END IF;
    END IF;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER runs_cancellation_summary_consistency
    AFTER INSERT OR UPDATE OR DELETE ON runs
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_cancellation_summary_consistency();

CREATE CONSTRAINT TRIGGER run_cancellations_run_summary_consistency
    AFTER INSERT OR UPDATE OR DELETE ON run_cancellations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_cancellation_summary_consistency();

UPDATE runs
SET status = status;

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

ALTER TABLE runs
    DROP COLUMN claimed_by_runtime_token_id,
    DROP COLUMN claimed_at;

INSERT INTO runtime_schema_contracts (
    schema_version,
    migration_name,
    runtime_contract_id,
    runtime_contract_digest,
    is_current
) VALUES (
    63,
    '063_reliable_runtime_v2',
    'openlinker.runtime.v2',
    'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
    TRUE
);

COMMIT;
