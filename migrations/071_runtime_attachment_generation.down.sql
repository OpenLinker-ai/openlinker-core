BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.runtime.migration.071', 0));

LOCK TABLE runtime_session_attachments IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_sessions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_nodes IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runs IN SHARE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_control IN SHARE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    oldest_digest CONSTANT TEXT := '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61';
    entry_digest CONSTANT TEXT := '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f';
    old_digest CONSTANT TEXT := 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53';
    new_digest CONSTANT TEXT := '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9';
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 071 rollback requires zero registered Core cluster members';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM runtime_cluster_control
        WHERE singleton_id = 1
          AND mode = 'hard_maintenance'
    ) THEN
        RAISE EXCEPTION 'migration 071 rollback requires hard maintenance';
    END IF;
    IF EXISTS (SELECT 1 FROM runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'migration 071 rollback requires zero running Runs';
    END IF;
    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 71
          AND migration_name = '071_runtime_attachment_generation'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = new_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 071 rollback requires the exact current schema contract 71';
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
        RAISE EXCEPTION 'migration 071 rollback requires the exact historical schema contract 70';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM runtime_nodes
        WHERE runtime_contract_id <> 'openlinker.runtime.v2'
           OR runtime_contract_digest NOT IN (
                oldest_digest,
                entry_digest,
                old_digest,
                new_digest
           )
    ) OR EXISTS (
        SELECT 1
        FROM runtime_sessions
        WHERE runtime_contract_id <> 'openlinker.runtime.v2'
           OR runtime_contract_digest NOT IN (
                oldest_digest,
                entry_digest,
                old_digest,
                new_digest
           )
    ) THEN
        RAISE EXCEPTION 'migration 071 rollback found an unknown Runtime contract identity';
    END IF;
END
$$;

UPDATE runtime_session_attachments attachment
SET detached_at = clock_timestamp(),
    disconnect_reason = 'runtime attachment generation rollback'
FROM runtime_sessions session
WHERE session.runtime_session_id = attachment.runtime_session_id
  AND session.runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
  AND session.status IN ('active', 'draining')
  AND attachment.detached_at IS NULL;

UPDATE runtime_sessions
SET status = 'closed',
    attached_core_instance_id = NULL,
    disconnected_at = COALESCE(disconnected_at, clock_timestamp()),
    heartbeat_at = GREATEST(heartbeat_at, clock_timestamp()),
    updated_at = clock_timestamp()
WHERE runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
  AND status IN ('active', 'draining');

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

ALTER TABLE runtime_sessions DROP CONSTRAINT runtime_sessions_contract_current;
ALTER TABLE runtime_nodes DROP CONSTRAINT runtime_nodes_contract_current;

UPDATE runtime_schema_contracts
SET is_current = FALSE
WHERE schema_version = 71
  AND migration_name = '071_runtime_attachment_generation'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
  AND is_current;

UPDATE runtime_schema_contracts
SET is_current = TRUE
WHERE schema_version = 70
  AND migration_name = '070_sdk_first_runtime_boundary'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53'
  AND NOT is_current;

UPDATE runtime_nodes
SET runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
    updated_at = clock_timestamp()
WHERE status <> 'revoked'
  AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9';

SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

ALTER TABLE runtime_nodes
    ADD CONSTRAINT runtime_nodes_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (status IN ('active', 'draining')
                 AND runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53')
                OR
                (status = 'revoked'
                 AND runtime_contract_digest IN (
                    '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61',
                    '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f',
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
                 ))
            )
        );

ALTER TABLE runtime_sessions
    ADD CONSTRAINT runtime_sessions_contract_current
        CHECK (
            protocol_version = 2
            AND runtime_contract_id = 'openlinker.runtime.v2'
            AND (
                (status IN ('active', 'draining')
                 AND runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53')
                OR
                (status IN ('offline', 'revoked', 'closed')
                 AND runtime_contract_digest IN (
                    '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61',
                    '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f',
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
                    '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
                 ))
            )
        );

COMMIT;
