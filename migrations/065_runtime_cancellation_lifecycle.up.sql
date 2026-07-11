BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.runtime.v2.migration.065', 0));

LOCK TABLE runs IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE run_attempts IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE run_cancellations IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 065 requires zero registered Core cluster members';
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
        RAISE EXCEPTION 'migration 065 requires the exact current schema contract 64';
    END IF;
END
$$;

UPDATE runtime_schema_contracts
SET schema_version = 65,
    migration_name = '065_runtime_cancellation_lifecycle'
WHERE schema_version = 64
  AND migration_name = '064_runtime_lease_resume_primitives'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
  AND is_current;

-- A canceled Run clears its active lease summary immediately. Its immutable
-- target Attempt may nevertheless remain unfinished while stop evidence is
-- requested/delivered/stopping/unsupported/failed. A confirmed stop or the
-- deadline reaper finishes the Attempt before committing stopped/unconfirmed.
-- The Cancellation row is the sole
-- durable authorization for that exception.
DO $migration$
DECLARE
    definition TEXT;
    old_declaration TEXT := $old$
    unfinished_attempt_rows INTEGER;
    unfinished_attempt_id UUID;
BEGIN$old$;
    new_declaration TEXT := $new$
    unfinished_attempt_rows INTEGER;
    unfinished_attempt_id UUID;
    cancellation_target_attempt_id UUID;
    cancellation_state TEXT;
BEGIN$new$;
    old_body TEXT := $old$
    IF current_run.active_attempt_id IS NULL THEN
        IF unfinished_attempt_rows <> 0 THEN
            RAISE EXCEPTION 'unfinished attempt must be the Run active attempt';
        END IF;
        RETURN NULL;
    END IF;$old$;
    new_body TEXT := $new$
    IF current_run.active_attempt_id IS NULL THEN
        IF unfinished_attempt_rows <> 0 THEN
            SELECT target_attempt_id, state
            INTO cancellation_target_attempt_id, cancellation_state
            FROM run_cancellations
            WHERE run_id = current_run.id;

            IF current_run.status IS DISTINCT FROM 'canceled'
               OR current_run.dispatch_state IS DISTINCT FROM 'terminal'
               OR unfinished_attempt_rows <> 1
               OR cancellation_target_attempt_id IS DISTINCT FROM unfinished_attempt_id
               OR cancellation_state NOT IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')
               OR latest_attempt.id IS DISTINCT FROM unfinished_attempt_id
               OR latest_attempt.executor_type IS DISTINCT FROM 'agent_node'
               OR latest_attempt.finished_at IS NOT NULL
               OR latest_attempt.outcome IS NOT NULL THEN
                RAISE EXCEPTION 'unfinished attempt must be the Run active attempt or unsettled cancellation target';
            END IF;
        END IF;
        RETURN NULL;
    END IF;$new$;
BEGIN
    definition := pg_get_functiondef('enforce_run_active_attempt_consistency()'::regprocedure);
    IF POSITION(old_declaration IN definition) = 0 OR POSITION(old_body IN definition) = 0 THEN
        RAISE EXCEPTION 'migration 065 active Attempt invariant source mismatch';
    END IF;
    definition := replace(definition, old_declaration, new_declaration);
    definition := replace(definition, old_body, new_body);
    EXECUTE definition;
END
$migration$;

-- Terminal cancellation is intentionally two-phase: the public Run terminal
-- fact is immediate, while Attempt/capacity terminal evidence follows either
-- a confirmed stopped ACK or the database-deadline reaper. Negative ACKs
-- remain capacity-owning until one of those two safe release paths wins.
DO $migration$
DECLARE
    definition TEXT;
    old_declaration TEXT := $old$
    cancellation_target_attempt_id UUID;
BEGIN$old$;
    new_declaration TEXT := $new$
    cancellation_target_attempt_id UUID;
    cancellation_state TEXT;
BEGIN$new$;
    old_body TEXT := $old$
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
    END IF;$old$;
    new_body TEXT := $new$
    IF current_run.runtime_contract_id = 'openlinker.runtime.v2'
       AND current_run.status IN ('timeout', 'canceled') THEN
        IF current_run.status = 'canceled' THEN
            SELECT target_attempt_id, state
            INTO cancellation_target_attempt_id, cancellation_state
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
               OR (
                   current_run.status = 'timeout'
                   AND (
                       latest_attempt.finished_at IS NULL
                       OR latest_attempt.outcome NOT IN (
                           'offer_rejected',
                           'offer_expired',
                           'retryable_failure',
                           'lease_expired',
                           'timeout',
                           'result_unknown'
                       )
                   )
               )
               OR (
                   current_run.status = 'canceled'
                   AND cancellation_target_attempt_id IS NULL
                   AND (
                       latest_attempt.finished_at IS NULL
                       OR latest_attempt.outcome NOT IN (
                           'offer_rejected',
                           'offer_expired',
                           'retryable_failure',
                           'lease_expired',
                           'result_unknown'
                       )
                   )
               )
               OR (
                   current_run.status = 'canceled'
                   AND cancellation_target_attempt_id IS NOT NULL
                   AND (
                       current_run.latest_attempt_id IS DISTINCT FROM cancellation_target_attempt_id
                       OR latest_attempt.id IS DISTINCT FROM cancellation_target_attempt_id
                       OR (
                           cancellation_state IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')
                           AND (
                               latest_attempt.executor_type IS DISTINCT FROM 'agent_node'
                               OR latest_attempt.finished_at IS NOT NULL
                               OR latest_attempt.outcome IS NOT NULL
                           )
                       )
                       OR (
                           cancellation_state IN ('stopped', 'unconfirmed')
                           AND (
                               latest_attempt.finished_at IS NULL
                               OR latest_attempt.outcome IS DISTINCT FROM 'canceled'
                           )
                       )
                       OR cancellation_state NOT IN (
                           'requested', 'delivered', 'stopping', 'unsupported', 'failed',
                           'stopped', 'unconfirmed'
                       )
                   )
               ) THEN
                RAISE EXCEPTION 'timeout or canceled Run contradicts its latest Attempt or cancellation lifecycle';
            END IF;
        END IF;
    END IF;$new$;
BEGIN
    definition := pg_get_functiondef('enforce_run_terminal_artifacts_consistency()'::regprocedure);
    IF POSITION(old_declaration IN definition) = 0 OR POSITION(old_body IN definition) = 0 THEN
        RAISE EXCEPTION 'migration 065 terminal artifact invariant source mismatch';
    END IF;
    definition := replace(definition, old_declaration, new_declaration);
    definition := replace(definition, old_body, new_body);
    EXECUTE definition;
END
$migration$;

COMMIT;
