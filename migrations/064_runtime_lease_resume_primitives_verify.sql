DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM runtime_schema_contracts
        WHERE schema_version = 64
          AND migration_name = '064_runtime_lease_resume_primitives'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61'
          AND is_current
    ) OR EXISTS (
        SELECT 1
        FROM runtime_schema_contracts
        WHERE schema_version = 63
           OR migration_name = '063_reliable_runtime_v2'
    ) OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'runtime schema contract 64 is missing or mismatched';
    END IF;

    IF to_regclass('runtime_resume_grants') IS NULL THEN
        RAISE EXCEPTION 'runtime resume grant table is missing';
    END IF;

    IF EXISTS (
        SELECT required.column_name
        FROM (VALUES
            ('slot_acquired_at', 'timestamp with time zone', 'YES'),
            ('slot_released_at', 'timestamp with time zone', 'YES'),
            ('active_runtime_session_id', 'uuid', 'YES')
        ) AS required(column_name, data_type, is_nullable)
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns c
            WHERE c.table_schema = current_schema()
              AND c.table_name = 'run_attempts'
              AND c.column_name = required.column_name
              AND c.data_type = required.data_type
              AND c.is_nullable = required.is_nullable
        )
    ) THEN
        RAISE EXCEPTION 'run Attempt slot evidence columns are missing or mismatched';
    END IF;

    IF EXISTS (
        SELECT required.constraint_name
        FROM (VALUES
            ('run_attempts_active_runtime_session_fk', 'f'),
            ('run_attempts_slot_shape', 'c'),
            ('run_attempts_slot_time_order', 'c')
        ) AS required(constraint_name, constraint_type)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_constraint c
            WHERE c.conrelid = 'run_attempts'::regclass
              AND c.conname = required.constraint_name
              AND c.contype::text = required.constraint_type
              AND c.convalidated
        )
    ) THEN
        RAISE EXCEPTION 'run Attempt slot evidence constraints are missing or unvalidated';
    END IF;

    IF to_regclass('idx_run_attempts_active_runtime_session_slot') IS NULL THEN
        RAISE EXCEPTION 'active runtime Session slot index is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger t
        WHERE t.tgrelid = 'run_attempts'::regclass
          AND t.tgname = 'run_attempts_slot_evidence_forward_only'
          AND NOT t.tgisinternal
          AND t.tgenabled = 'O'
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_trigger t
        WHERE t.tgrelid = 'run_attempts'::regclass
          AND t.tgname = 'run_attempts_slot_release_on_finish'
          AND NOT t.tgisinternal
          AND t.tgenabled = 'O'
          AND (t.tgdeferrable AND t.tginitdeferred)
    ) THEN
        RAISE EXCEPTION 'run Attempt slot evidence triggers are missing or mismatched';
    END IF;

    IF EXISTS (
        SELECT required.column_name
        FROM (VALUES
            ('id', 'uuid', 'NO'),
            ('run_id', 'uuid', 'NO'),
            ('attempt_id', 'uuid', 'NO'),
            ('lease_id', 'uuid', 'NO'),
            ('fencing_token', 'bigint', 'NO'),
            ('agent_id', 'uuid', 'NO'),
            ('node_id', 'uuid', 'NO'),
            ('worker_id', 'text', 'NO'),
            ('source_session_id', 'uuid', 'NO'),
            ('source_credential_id', 'uuid', 'NO'),
            ('target_session_id', 'uuid', 'NO'),
            ('target_credential_id', 'uuid', 'NO'),
            ('permission', 'text', 'NO'),
            ('granted_by_core_instance_id', 'uuid', 'NO'),
            ('granted_at', 'timestamp with time zone', 'NO'),
            ('expires_at', 'timestamp with time zone', 'NO'),
            ('first_used_at', 'timestamp with time zone', 'YES'),
            ('revoked_at', 'timestamp with time zone', 'YES'),
            ('revoked_by_type', 'text', 'YES'),
            ('revoked_by_id', 'uuid', 'YES'),
            ('revoke_reason', 'text', 'YES')
        ) AS required(column_name, data_type, is_nullable)
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns c
            WHERE c.table_schema = current_schema()
              AND c.table_name = 'runtime_resume_grants'
              AND c.column_name = required.column_name
              AND c.data_type = required.data_type
              AND c.is_nullable = required.is_nullable
        )
    ) THEN
        RAISE EXCEPTION 'runtime resume grant columns are missing or mismatched';
    END IF;

    IF EXISTS (
        SELECT required.constraint_name
        FROM (VALUES
            ('runtime_resume_grants_attempt_fk', 'f'),
            ('runtime_resume_grants_source_session_fk', 'f'),
            ('runtime_resume_grants_target_session_fk', 'f'),
            ('runtime_resume_grants_distinct_sessions', 'c'),
            ('runtime_resume_grants_fence_positive', 'c'),
            ('runtime_resume_grants_worker_len', 'c'),
            ('runtime_resume_grants_permission_valid', 'c'),
            ('runtime_resume_grants_time_order', 'c'),
            ('runtime_resume_grants_revoke_evidence', 'c')
        ) AS required(constraint_name, constraint_type)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_constraint c
            WHERE c.conrelid = 'runtime_resume_grants'::regclass
              AND c.conname = required.constraint_name
              AND c.contype::text = required.constraint_type
              AND c.convalidated
        )
    ) THEN
        RAISE EXCEPTION 'runtime resume grant constraints are missing or unvalidated';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'run_attempts'
          AND indexname = 'idx_run_attempts_unaccepted_session'
          AND indexdef LIKE 'CREATE UNIQUE INDEX%'
          AND indexdef LIKE '%(runtime_session_id)%'
          AND indexdef LIKE '%accepted_at IS NULL%'
          AND indexdef LIKE '%finished_at IS NULL%'
    ) THEN
        RAISE EXCEPTION 'single unaccepted Session offer index is missing or mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'runtime_resume_grants'
          AND indexname = 'idx_runtime_resume_grants_unrevoked_attempt'
          AND indexdef LIKE 'CREATE UNIQUE INDEX%'
          AND indexdef LIKE '%revoked_at IS NULL%'
    ) OR to_regclass('idx_runtime_resume_grants_target_active') IS NULL
       OR to_regclass('idx_runtime_resume_grants_source_history') IS NULL THEN
        RAISE EXCEPTION 'runtime resume grant indexes are missing or mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger t
        WHERE t.tgrelid = 'runtime_resume_grants'::regclass
          AND t.tgname = 'runtime_resume_grants_identity_immutable'
          AND NOT t.tgisinternal
          AND t.tgenabled = 'O'
    ) THEN
        RAISE EXCEPTION 'runtime resume grant identity trigger is missing';
    END IF;

    IF pg_get_functiondef('enforce_runtime_session_principal()'::regprocedure)
           NOT LIKE '%agent:pull%'
       OR pg_get_functiondef('enforce_runtime_session_principal()'::regprocedure)
           NOT LIKE '%token_record.scopes%' THEN
        RAISE EXCEPTION 'runtime Session principal does not enforce agent:pull scope';
    END IF;

    IF EXISTS (
        SELECT runtime_session_id
        FROM run_attempts
        WHERE runtime_session_id IS NOT NULL
          AND accepted_at IS NULL
          AND finished_at IS NULL
        GROUP BY runtime_session_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'one runtime session has multiple unaccepted offers';
    END IF;
END
$$;
