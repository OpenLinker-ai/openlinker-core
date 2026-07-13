BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

SELECT pg_advisory_xact_lock(hashtextextended('openlinker.runtime.v2.migration.069', 0));

LOCK TABLE runtime_session_attachments IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_sessions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_nodes IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runs IN SHARE MODE;
LOCK TABLE runtime_schema_contracts IN ACCESS EXCLUSIVE MODE;
LOCK TABLE runtime_cluster_control IN SHARE MODE;
LOCK TABLE runtime_cluster_members IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    old_digest CONSTANT TEXT := '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f';
    new_digest CONSTANT TEXT := 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53';
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration 069 requires zero registered Core cluster members';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM runtime_cluster_control
        WHERE singleton_id = 1
          AND mode = 'hard_maintenance'
    ) THEN
        RAISE EXCEPTION 'migration 069 requires hard maintenance';
    END IF;
    IF EXISTS (SELECT 1 FROM runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'migration 069 requires zero running Runs';
    END IF;
    IF (
        SELECT COUNT(*)
        FROM runtime_schema_contracts
        WHERE schema_version = 67
          AND migration_name = '067_runtime_v2_core_execution'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = old_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 069 requires the exact current schema contract 67';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM runtime_schema_contracts
        WHERE (
                schema_version = 69
                OR migration_name = '069_runtime_entry_discovery'
                OR runtime_contract_digest = new_digest
              )
          AND NOT (
                schema_version = 69
                AND migration_name = '069_runtime_entry_discovery'
                AND runtime_contract_id = 'openlinker.runtime.v2'
                AND runtime_contract_digest = new_digest
                AND NOT is_current
              )
    ) THEN
        RAISE EXCEPTION 'migration 069 found a conflicting historical schema contract 69';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM runtime_nodes
        WHERE runtime_contract_id <> 'openlinker.runtime.v2'
           OR runtime_contract_digest NOT IN (
                '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61',
                old_digest,
                new_digest
           )
    ) OR EXISTS (
        SELECT 1
        FROM runtime_sessions
        WHERE runtime_contract_id <> 'openlinker.runtime.v2'
           OR runtime_contract_digest NOT IN (
                '60bef5cec7eeab563937187f48a458059995aebee161765032cddc17d0cdfa61',
                old_digest,
                new_digest
           )
    ) THEN
        RAISE EXCEPTION 'migration 069 found an unknown Runtime contract identity';
    END IF;
END
$$;

-- Session identity is immutable. The URL path is part of the pinned contract,
-- so every Session negotiated with the old path becomes closed history and a
-- restarted Agent Node must negotiate the canonical discovered entrypoint.
UPDATE runtime_session_attachments attachment
SET detached_at = clock_timestamp(),
    disconnect_reason = 'runtime entry contract digest cutover'
FROM runtime_sessions session
WHERE session.runtime_session_id = attachment.runtime_session_id
  AND session.runtime_contract_digest = '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f'
  AND session.status IN ('active', 'draining')
  AND attachment.detached_at IS NULL;

UPDATE runtime_sessions
SET status = 'closed',
    attached_core_instance_id = NULL,
    disconnected_at = COALESCE(disconnected_at, clock_timestamp()),
    heartbeat_at = GREATEST(heartbeat_at, clock_timestamp()),
    updated_at = clock_timestamp()
WHERE runtime_contract_digest = '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f'
  AND status IN ('active', 'draining');

-- Flush only the rows changed above before replacing the current-contract
-- checks. Historical Runs and Events are never touched by this migration.
SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

ALTER TABLE runtime_sessions DROP CONSTRAINT runtime_sessions_contract_current;
ALTER TABLE runtime_nodes DROP CONSTRAINT runtime_nodes_contract_current;

UPDATE runtime_schema_contracts
SET is_current = FALSE
WHERE schema_version = 67
  AND migration_name = '067_runtime_v2_core_execution'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f'
  AND is_current;

INSERT INTO runtime_schema_contracts (
    schema_version,
    migration_name,
    runtime_contract_id,
    runtime_contract_digest,
    is_current
) VALUES (
    69,
    '069_runtime_entry_discovery',
    'openlinker.runtime.v2',
    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
    TRUE
)
ON CONFLICT (schema_version) DO NOTHING;

UPDATE runtime_schema_contracts
SET is_current = TRUE
WHERE schema_version = 69
  AND migration_name = '069_runtime_entry_discovery'
  AND runtime_contract_id = 'openlinker.runtime.v2'
  AND runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53'
  AND NOT is_current;

UPDATE runtime_nodes
SET runtime_contract_digest = 'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53',
    updated_at = clock_timestamp()
WHERE status <> 'revoked'
  AND runtime_contract_digest = '857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f';

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
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53'
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
                    'fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53'
                 ))
            )
        );

COMMIT;
