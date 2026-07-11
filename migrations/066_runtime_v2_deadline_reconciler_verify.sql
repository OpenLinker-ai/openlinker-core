DO $$
DECLARE
    old_digest CONSTANT TEXT := 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f';
    new_digest CONSTANT TEXT := '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61';
    node_constraint TEXT;
    session_constraint TEXT;
BEGIN
    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 66
          AND migration_name = '066_runtime_v2_deadline_reconciler'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = new_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'runtime schema contract 66 is missing or mismatched';
    END IF;

    IF EXISTS (
        SELECT 1 FROM runtime_nodes
        WHERE status IN ('active', 'draining')
          AND runtime_contract_digest <> new_digest
    ) OR EXISTS (
        SELECT 1 FROM runtime_sessions
        WHERE status IN ('active', 'draining')
          AND runtime_contract_digest <> new_digest
    ) THEN
        RAISE EXCEPTION 'active runtime principal retained an obsolete digest';
    END IF;

    IF EXISTS (
        SELECT 1 FROM runtime_nodes
        WHERE runtime_contract_digest NOT IN (old_digest, new_digest)
    ) OR EXISTS (
        SELECT 1 FROM runtime_sessions
        WHERE runtime_contract_digest NOT IN (old_digest, new_digest)
    ) THEN
        RAISE EXCEPTION 'runtime principal carries an unknown digest';
    END IF;

    SELECT pg_get_constraintdef(oid)
    INTO STRICT node_constraint
    FROM pg_constraint
    WHERE conrelid = 'runtime_nodes'::regclass
      AND conname = 'runtime_nodes_contract_current'
      AND contype = 'c'
      AND convalidated;

    SELECT pg_get_constraintdef(oid)
    INTO STRICT session_constraint
    FROM pg_constraint
    WHERE conrelid = 'runtime_sessions'::regclass
      AND conname = 'runtime_sessions_contract_current'
      AND contype = 'c'
      AND convalidated;

    IF node_constraint NOT LIKE '%active%draining%revoked%'
       OR node_constraint NOT LIKE '%' || old_digest || '%'
       OR node_constraint NOT LIKE '%' || new_digest || '%'
       OR session_constraint NOT LIKE '%active%draining%offline%revoked%closed%'
       OR session_constraint NOT LIKE '%' || old_digest || '%'
       OR session_constraint NOT LIKE '%' || new_digest || '%' THEN
        RAISE EXCEPTION 'runtime current-contract checks do not preserve historical identities safely';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runtime_sessions session
        JOIN runtime_session_attachments attachment
          ON attachment.runtime_session_id = session.runtime_session_id
        WHERE session.status IN ('offline', 'revoked', 'closed')
          AND attachment.detached_at IS NULL
    ) THEN
        RAISE EXCEPTION 'inactive runtime Session retained an active attachment';
    END IF;

    IF pg_get_functiondef('enforce_runtime_session_identity_immutable()'::regprocedure)
           NOT LIKE '%fenced slot release%'
       OR pg_get_functiondef('enforce_runtime_session_identity_immutable()'::regprocedure)
           NOT LIKE '%NEW.inflight = OLD.inflight - 1%'
       OR pg_get_functiondef('enforce_runtime_session_identity_immutable()'::regprocedure)
           NOT LIKE '%ARRAY[''inflight'', ''updated_at'']%' THEN
        RAISE EXCEPTION 'terminal runtime Session fenced capacity release rule is missing';
    END IF;

    IF to_regclass('idx_runs_runtime_v2_dispatch_due') IS NULL
       OR to_regclass('idx_runs_runtime_v2_run_deadline_due') IS NULL
       OR to_regclass('idx_run_attempts_runtime_v2_offer_due') IS NULL
       OR to_regclass('idx_run_attempts_runtime_v2_execution_due') IS NULL THEN
        RAISE EXCEPTION 'runtime v2 deadline reconcile indexes are missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname = 'idx_runs_runtime_v2_dispatch_due'
          AND indexdef LIKE '%cancel_request_id IS NULL%'
          AND indexdef LIKE '%pending%offered%retry_wait%'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname = 'idx_run_attempts_runtime_v2_offer_due'
          AND indexdef LIKE '%offer_expires_at%'
          AND indexdef LIKE '%accepted_at IS NULL%'
          AND indexdef LIKE '%finished_at IS NULL%'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname = 'idx_run_attempts_runtime_v2_execution_due'
          AND indexdef LIKE '%lease_expires_at%attempt_deadline_at%'
          AND indexdef LIKE '%accepted_at IS NOT NULL%'
          AND indexdef LIKE '%finished_at IS NULL%'
    ) THEN
        RAISE EXCEPTION 'runtime v2 deadline reconcile index definitions are mismatched';
    END IF;
END
$$;
