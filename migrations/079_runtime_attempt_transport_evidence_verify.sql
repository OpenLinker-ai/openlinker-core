DO $$
DECLARE
    current_digest CONSTANT TEXT := '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481';
BEGIN
    IF (
        SELECT COUNT(*) FROM runtime_schema_contracts
        WHERE schema_version = 79
          AND migration_name = '079_runtime_attempt_transport_evidence'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = current_digest
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'runtime schema contract 79 is missing or mismatched';
    END IF;

    IF (
        SELECT COUNT(*) FROM runtime_schema_contracts
        WHERE schema_version = 77
          AND migration_name = '077_external_execution_cancellation'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = current_digest
          AND NOT is_current
    ) <> 1 THEN
        RAISE EXCEPTION 'runtime schema contract 77 history is missing or mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'run_attempts'
          AND column_name = 'runtime_attachment_id'
          AND udt_name = 'uuid'
          AND is_nullable = 'YES'
    ) THEN
        RAISE EXCEPTION 'Run Attempt Runtime Attachment evidence column is missing';
    END IF;

    IF (
        SELECT COUNT(*) FROM pg_constraint
        WHERE conrelid IN (
            'run_attempts'::regclass,
            'runtime_session_attachments'::regclass
        )
          AND conname IN (
              'run_attempts_runtime_attachment_state',
              'run_attempts_runtime_attachment_identity_fk',
              'runtime_session_attachments_attempt_identity_unique'
          )
          AND convalidated
    ) <> 3 THEN
        RAISE EXCEPTION 'Run Attempt Runtime Attachment constraints are missing or unvalidated';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'run_attempts'::regclass
          AND tgname = 'run_attempts_runtime_attachment_evidence'
          AND tgenabled = 'O'
          AND NOT tgisinternal
    ) OR to_regprocedure('enforce_run_attempt_runtime_attachment_evidence()') IS NULL THEN
        RAISE EXCEPTION 'Run Attempt Runtime Attachment evidence trigger is missing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM run_attempts attempt
        LEFT JOIN runtime_session_attachments attachment
          ON attachment.id = attempt.runtime_attachment_id
         AND attachment.runtime_session_id = attempt.runtime_session_id
        WHERE attempt.runtime_attachment_id IS NOT NULL
          AND (
              attempt.executor_type <> 'runtime'
              OR attempt.accepted_at IS NULL
              OR attachment.id IS NULL
              OR attachment.transport NOT IN ('websocket', 'long_poll')
              OR attachment.transport_reason IS NULL
          )
    ) THEN
        RAISE EXCEPTION 'Run Attempt Runtime Attachment evidence is inconsistent';
    END IF;
END
$$;
