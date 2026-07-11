DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM runtime_schema_contracts
        WHERE schema_version = 65
          AND migration_name = '065_runtime_cancellation_lifecycle'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
          AND is_current
    ) OR EXISTS (
        SELECT 1
        FROM runtime_schema_contracts
        WHERE schema_version IN (63, 64)
           OR migration_name IN (
               '063_reliable_runtime_v2',
               '064_runtime_lease_resume_primitives'
           )
    ) OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'runtime schema contract 65 is missing or mismatched';
    END IF;

    IF pg_get_functiondef('enforce_run_active_attempt_consistency()'::regprocedure)
           NOT LIKE '%unsettled cancellation target%'
       OR pg_get_functiondef('enforce_run_active_attempt_consistency()'::regprocedure)
           NOT LIKE '%cancellation_state NOT IN (%'
       OR pg_get_functiondef('enforce_run_terminal_artifacts_consistency()'::regprocedure)
           NOT LIKE '%latest Attempt or cancellation lifecycle%'
       OR pg_get_functiondef('enforce_run_terminal_artifacts_consistency()'::regprocedure)
           NOT LIKE '%requested%delivered%stopping%unsupported%failed%' THEN
        RAISE EXCEPTION 'runtime cancellation lifecycle invariants are missing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runs r
        JOIN run_cancellations c ON c.run_id = r.id
        LEFT JOIN run_attempts a
          ON a.run_id = r.id
         AND a.id = c.target_attempt_id
        WHERE r.runtime_contract_id = 'openlinker.runtime.v2'
          AND r.status = 'canceled'
          AND c.target_attempt_id IS NOT NULL
          AND (
              r.latest_attempt_id IS DISTINCT FROM c.target_attempt_id
              OR a.id IS NULL
              OR (
                  c.state IN ('requested', 'delivered', 'stopping', 'unsupported', 'failed')
                  AND (
                      a.executor_type IS DISTINCT FROM 'agent_node'
                      OR a.finished_at IS NOT NULL
                      OR a.outcome IS NOT NULL
                      OR a.slot_released_at IS NOT NULL
                      OR a.active_runtime_session_id IS NULL
                  )
              )
              OR (
                  c.state IN ('stopped', 'unconfirmed')
                  AND (
                      a.finished_at IS NULL
                      OR a.outcome IS DISTINCT FROM 'canceled'
                      OR (
                          a.executor_type = 'agent_node'
                          AND (
                              a.slot_released_at IS NULL
                              OR a.active_runtime_session_id IS NOT NULL
                          )
                      )
                  )
              )
          )
    ) THEN
        RAISE EXCEPTION 'stored runtime cancellation lifecycle evidence is inconsistent';
    END IF;
END
$$;
