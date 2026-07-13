DO $$
DECLARE
    oldest_digest CONSTANT TEXT := '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61';
    entry_digest CONSTANT TEXT := '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f';
    old_digest CONSTANT TEXT := 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53';
    new_digest CONSTANT TEXT := '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9';
    node_constraint TEXT;
    session_constraint TEXT;
BEGIN
    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 71
          AND migration_name = '071_runtime_attachment_generation'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = new_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'runtime schema contract 71 is missing or mismatched';
    END IF;

    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 70
          AND migration_name = '070_sdk_first_runtime_boundary'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = old_digest
          AND NOT is_current
    ) <> 1 THEN
        RAISE EXCEPTION 'runtime schema contract 70 history is missing or mismatched';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runtime_nodes
        WHERE status IN ('active', 'draining')
          AND runtime_contract_digest <> new_digest
    ) OR EXISTS (
        SELECT 1
        FROM runtime_sessions
        WHERE status IN ('active', 'draining')
          AND runtime_contract_digest <> new_digest
    ) THEN
        RAISE EXCEPTION 'active Runtime principal retained the pre-attachment-generation contract';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runtime_nodes
        WHERE runtime_contract_id <> 'openlinker.runtime.v2'
           OR runtime_contract_digest NOT IN (oldest_digest, entry_digest, old_digest, new_digest)
    ) OR EXISTS (
        SELECT 1
        FROM runtime_sessions
        WHERE runtime_contract_id <> 'openlinker.runtime.v2'
           OR runtime_contract_digest NOT IN (oldest_digest, entry_digest, old_digest, new_digest)
    ) THEN
        RAISE EXCEPTION 'Runtime principal carries an unknown contract identity';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runtime_sessions session
        JOIN runtime_session_attachments attachment
          ON attachment.runtime_session_id = session.runtime_session_id
        WHERE session.status IN ('offline', 'revoked', 'closed')
          AND attachment.detached_at IS NULL
    ) THEN
        RAISE EXCEPTION 'inactive Runtime Session retained an active attachment';
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

    IF node_constraint NOT LIKE '%' || new_digest || '%'
       OR node_constraint NOT LIKE '%' || old_digest || '%'
       OR node_constraint NOT LIKE '%' || entry_digest || '%'
       OR node_constraint NOT LIKE '%' || oldest_digest || '%'
       OR session_constraint NOT LIKE '%' || new_digest || '%'
       OR session_constraint NOT LIKE '%' || old_digest || '%'
       OR session_constraint NOT LIKE '%' || entry_digest || '%'
       OR session_constraint NOT LIKE '%' || oldest_digest || '%' THEN
        RAISE EXCEPTION 'Runtime current-contract checks do not preserve attachment-generation history';
    END IF;
END
$$;
