BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.runtime.v2.migration.064', 0));

LOCK TABLE run_attempts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_sessions IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 064 requires zero registered Core cluster members';
    END IF;

    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 63
          AND migration_name = '063_reliable_runtime_v2'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 064 requires the exact current schema contract 63';
    END IF;
END
$$;

UPDATE runtime_schema_contracts
SET schema_version = 64,
    migration_name = '064_runtime_lease_resume_primitives'
WHERE schema_version = 63
  AND migration_name = '063_reliable_runtime_v2'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
  AND is_current;

-- Session construction/reactivation is the database security boundary, not
-- merely an API convention. A call-only Agent credential must never become a
-- runtime principal even if one handler forgets to repeat the scope check.
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
           OR NOT ('agent:pull' = ANY(token_record.scopes))
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

ALTER TABLE run_attempts
    ADD COLUMN slot_acquired_at TIMESTAMPTZ,
    ADD COLUMN slot_released_at TIMESTAMPTZ,
    ADD COLUMN active_runtime_session_id UUID;

-- Preserve finished pre-064 history as already released. A live Agent Node
-- Attempt keeps its immutable source Session as the current slot owner.
UPDATE run_attempts
SET slot_acquired_at = offered_at,
    slot_released_at = CASE
        WHEN finished_at IS NULL THEN NULL
        ELSE finished_at
    END,
    active_runtime_session_id = CASE
        WHEN finished_at IS NULL THEN runtime_session_id
        ELSE NULL
    END
WHERE executor_type = 'agent_node';

ALTER TABLE run_attempts
    ADD CONSTRAINT run_attempts_active_runtime_session_fk
        FOREIGN KEY (active_runtime_session_id)
        REFERENCES runtime_sessions(runtime_session_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT run_attempts_slot_shape
        CHECK (
            (
                executor_type = 'agent_node'
                AND slot_acquired_at IS NOT NULL
                AND (
                    (
                        slot_released_at IS NULL
                        AND active_runtime_session_id IS NOT NULL
                    )
                    OR (
                        slot_released_at IS NOT NULL
                        AND active_runtime_session_id IS NULL
                    )
                )
            )
            OR (
                executor_type IN ('core_http', 'core_mcp')
                AND slot_acquired_at IS NULL
                AND slot_released_at IS NULL
                AND active_runtime_session_id IS NULL
            )
        ),
    ADD CONSTRAINT run_attempts_slot_time_order
        CHECK (
            slot_released_at IS NULL
            OR slot_released_at >= slot_acquired_at
        );

CREATE INDEX idx_run_attempts_active_runtime_session_slot
    ON run_attempts (active_runtime_session_id, run_id, id)
    WHERE slot_released_at IS NULL
      AND active_runtime_session_id IS NOT NULL;

CREATE OR REPLACE FUNCTION enforce_run_attempt_slot_evidence()
RETURNS TRIGGER AS $$
DECLARE
    owner_session runtime_sessions%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.slot_acquired_at IS DISTINCT FROM OLD.slot_acquired_at THEN
            RAISE EXCEPTION 'run attempt slot acquisition evidence is immutable';
        END IF;

        IF OLD.slot_released_at IS NOT NULL
           AND ROW(
               NEW.slot_released_at,
               NEW.active_runtime_session_id
           ) IS DISTINCT FROM ROW(
               OLD.slot_released_at,
               OLD.active_runtime_session_id
           ) THEN
            RAISE EXCEPTION 'run attempt slot release evidence is immutable';
        END IF;

        IF OLD.active_runtime_session_id IS NULL
           AND NEW.active_runtime_session_id IS NOT NULL THEN
            RAISE EXCEPTION 'released run attempt slot cannot be reacquired';
        END IF;

        IF OLD.active_runtime_session_id IS NOT NULL
           AND NEW.active_runtime_session_id IS NULL
           AND NEW.slot_released_at IS NULL THEN
            RAISE EXCEPTION 'run attempt slot owner cannot clear without release evidence';
        END IF;
    END IF;

    IF NEW.executor_type = 'agent_node'
       AND NEW.active_runtime_session_id IS NOT NULL THEN
        SELECT * INTO owner_session
        FROM runtime_sessions
        WHERE runtime_session_id = NEW.active_runtime_session_id;

        IF NOT FOUND
           OR owner_session.node_id IS DISTINCT FROM NEW.node_id
           OR owner_session.agent_id IS DISTINCT FROM NEW.agent_id
           OR owner_session.worker_id IS DISTINCT FROM NEW.runtime_worker_id THEN
            RAISE EXCEPTION 'run attempt active slot owner identity mismatch';
        END IF;
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER run_attempts_slot_evidence_forward_only
    BEFORE INSERT OR UPDATE ON run_attempts
    FOR EACH ROW EXECUTE FUNCTION enforce_run_attempt_slot_evidence();

CREATE OR REPLACE FUNCTION enforce_run_attempt_slot_release_on_finish()
RETURNS TRIGGER AS $$
DECLARE
    current_attempt run_attempts%ROWTYPE;
BEGIN
    SELECT * INTO current_attempt
    FROM run_attempts
    WHERE id = NEW.id;

    IF NOT FOUND OR current_attempt.executor_type <> 'agent_node' THEN
        RETURN NULL;
    END IF;

    IF (current_attempt.finished_at IS NULL)
       IS DISTINCT FROM (current_attempt.slot_released_at IS NULL) THEN
        RAISE EXCEPTION 'agent Node Attempt finish and slot release must commit together';
    END IF;

    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER run_attempts_slot_release_on_finish
    AFTER INSERT OR UPDATE ON run_attempts
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_run_attempt_slot_release_on_finish();

DO $$
BEGIN
    IF EXISTS (
        SELECT runtime_session_id
        FROM run_attempts
        WHERE runtime_session_id IS NOT NULL
          AND accepted_at IS NULL
          AND finished_at IS NULL
        GROUP BY runtime_session_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'migration 064 found multiple unaccepted offers for one runtime session';
    END IF;
END
$$;

-- A Session may execute up to its declared capacity, but it may only have one
-- outstanding assignment decision. This makes a repeated claim return the
-- original offer instead of racing a second Run into the same Session.
CREATE UNIQUE INDEX idx_run_attempts_unaccepted_session
    ON run_attempts (runtime_session_id)
    WHERE runtime_session_id IS NOT NULL
      AND accepted_at IS NULL
      AND finished_at IS NULL;

CREATE TABLE runtime_resume_grants (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    lease_id UUID NOT NULL,
    fencing_token BIGINT NOT NULL,
    agent_id UUID NOT NULL,
    node_id UUID NOT NULL,
    worker_id TEXT NOT NULL,
    source_session_id UUID NOT NULL,
    source_credential_id UUID NOT NULL,
    target_session_id UUID NOT NULL,
    target_credential_id UUID NOT NULL,
    permission TEXT NOT NULL,
    granted_by_core_instance_id UUID NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    first_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revoked_by_type TEXT,
    revoked_by_id UUID,
    revoke_reason TEXT,
    CONSTRAINT runtime_resume_grants_attempt_fk
        FOREIGN KEY (run_id, attempt_id)
        REFERENCES run_attempts (run_id, id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT runtime_resume_grants_source_session_fk
        FOREIGN KEY (
            source_session_id,
            node_id,
            agent_id,
            source_credential_id,
            worker_id
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
    CONSTRAINT runtime_resume_grants_target_session_fk
        FOREIGN KEY (
            target_session_id,
            node_id,
            agent_id,
            target_credential_id,
            worker_id
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
    CONSTRAINT runtime_resume_grants_distinct_sessions
        CHECK (source_session_id <> target_session_id),
    CONSTRAINT runtime_resume_grants_fence_positive
        CHECK (fencing_token > 0),
    CONSTRAINT runtime_resume_grants_worker_len
        CHECK (char_length(worker_id) BETWEEN 1 AND 200),
    CONSTRAINT runtime_resume_grants_permission_valid
        CHECK (permission IN ('upload_spool_only', 'continue_execution')),
    CONSTRAINT runtime_resume_grants_time_order
        CHECK (
            expires_at > granted_at
            AND (first_used_at IS NULL OR first_used_at >= granted_at)
            AND (revoked_at IS NULL OR revoked_at >= granted_at)
            AND (
                first_used_at IS NULL
                OR revoked_at IS NULL
                OR first_used_at <= revoked_at
            )
        ),
    CONSTRAINT runtime_resume_grants_revoke_evidence
        CHECK (
            (
                revoked_at IS NULL
                AND revoked_by_type IS NULL
                AND revoked_by_id IS NULL
                AND revoke_reason IS NULL
            )
            OR (
                revoked_at IS NOT NULL
                AND revoked_by_type IS NOT NULL
                AND revoked_by_type IN ('runtime_session', 'core_instance', 'system', 'operator')
                AND revoke_reason IS NOT NULL
                AND char_length(revoke_reason) BETWEEN 1 AND 500
                AND (
                    revoked_by_type = 'system'
                    OR revoked_by_id IS NOT NULL
                )
            )
        )
);

-- An expired grant remains evidence. The runtime must explicitly revoke it
-- before issuing a successor, so concurrent takeovers cannot both be active.
CREATE UNIQUE INDEX idx_runtime_resume_grants_unrevoked_attempt
    ON runtime_resume_grants (attempt_id)
    WHERE revoked_at IS NULL;

CREATE INDEX idx_runtime_resume_grants_target_active
    ON runtime_resume_grants (target_session_id, expires_at, attempt_id)
    WHERE revoked_at IS NULL;

CREATE INDEX idx_runtime_resume_grants_source_history
    ON runtime_resume_grants (source_session_id, granted_at DESC, id DESC);

CREATE OR REPLACE FUNCTION enforce_runtime_resume_grant_identity()
RETURNS TRIGGER AS $$
DECLARE
    source_attempt run_attempts%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'runtime resume grant evidence cannot be deleted';
    END IF;

    IF TG_OP = 'INSERT' THEN
        SELECT * INTO source_attempt
        FROM run_attempts
        WHERE run_id = NEW.run_id
          AND id = NEW.attempt_id;

        IF NOT FOUND
           OR source_attempt.lease_id IS DISTINCT FROM NEW.lease_id
           OR source_attempt.fencing_token IS DISTINCT FROM NEW.fencing_token
           OR source_attempt.agent_id IS DISTINCT FROM NEW.agent_id
           OR source_attempt.node_id IS DISTINCT FROM NEW.node_id
           OR source_attempt.runtime_worker_id IS DISTINCT FROM NEW.worker_id
           OR source_attempt.runtime_session_id IS DISTINCT FROM NEW.source_session_id
           OR source_attempt.runtime_token_id IS DISTINCT FROM NEW.source_credential_id THEN
            RAISE EXCEPTION 'runtime resume grant does not match immutable Attempt identity';
        END IF;

        IF NEW.permission = 'continue_execution'
           AND (
               source_attempt.accepted_at IS NULL
               OR source_attempt.finished_at IS NOT NULL
           ) THEN
            RAISE EXCEPTION 'continue_execution requires an unfinished accepted Attempt';
        END IF;

        RETURN NEW;
    END IF;

    IF ROW(
        NEW.id,
        NEW.run_id,
        NEW.attempt_id,
        NEW.lease_id,
        NEW.fencing_token,
        NEW.agent_id,
        NEW.node_id,
        NEW.worker_id,
        NEW.source_session_id,
        NEW.source_credential_id,
        NEW.target_session_id,
        NEW.target_credential_id,
        NEW.permission,
        NEW.granted_by_core_instance_id,
        NEW.granted_at,
        NEW.expires_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.run_id,
        OLD.attempt_id,
        OLD.lease_id,
        OLD.fencing_token,
        OLD.agent_id,
        OLD.node_id,
        OLD.worker_id,
        OLD.source_session_id,
        OLD.source_credential_id,
        OLD.target_session_id,
        OLD.target_credential_id,
        OLD.permission,
        OLD.granted_by_core_instance_id,
        OLD.granted_at,
        OLD.expires_at
    ) THEN
        RAISE EXCEPTION 'runtime resume grant immutable identity cannot change';
    END IF;

    IF OLD.first_used_at IS NOT NULL
       AND NEW.first_used_at IS DISTINCT FROM OLD.first_used_at THEN
        RAISE EXCEPTION 'runtime resume grant first-use evidence is immutable';
    END IF;

    IF OLD.revoked_at IS NOT NULL
       AND ROW(
           NEW.revoked_at,
           NEW.revoked_by_type,
           NEW.revoked_by_id,
           NEW.revoke_reason
       ) IS DISTINCT FROM ROW(
           OLD.revoked_at,
           OLD.revoked_by_type,
           OLD.revoked_by_id,
           OLD.revoke_reason
       ) THEN
        RAISE EXCEPTION 'runtime resume grant revocation evidence is immutable';
    END IF;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER runtime_resume_grants_identity_immutable
    BEFORE INSERT OR UPDATE OR DELETE ON runtime_resume_grants
    FOR EACH ROW EXECUTE FUNCTION enforce_runtime_resume_grant_identity();

COMMIT;
