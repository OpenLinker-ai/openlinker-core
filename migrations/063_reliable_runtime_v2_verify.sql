DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM runtime_schema_contracts
        WHERE schema_version = 63
          AND migration_name = '063_reliable_runtime_v2'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = 'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f'
          AND is_current
    ) THEN
        RAISE EXCEPTION 'runtime schema contract 63 is missing or mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM runtime_cluster_control
        WHERE singleton_id = 1
          AND mode = 'hard_maintenance'
    ) THEN
        RAISE EXCEPTION 'runtime cluster control is not in hard maintenance';
    END IF;

    IF EXISTS (SELECT 1 FROM runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'migration verification found nonterminal runs';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runs
        WHERE runtime_contract_id <> 'legacy.pre-v2'
           OR dispatch_state <> 'terminal'
           OR idempotency_key_hash IS NOT NULL
           OR idempotency_fingerprint IS NOT NULL
           OR request_metadata <> '{}'::jsonb
           OR connection_mode_snapshot IS NOT NULL
           OR endpoint_idempotency_snapshot IS NOT NULL
           OR dispatch_deadline_at IS NOT NULL
           OR run_deadline_at IS NOT NULL
           OR terminal_event_id IS NULL
           OR latest_attempt_id IS NOT NULL
           OR active_attempt_id IS NOT NULL
           OR lease_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'historical run backfill is inconsistent';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runs r
        JOIN run_events e
          ON e.run_id = r.id
         AND e.id = r.terminal_event_id
        WHERE NOT (
            (
                e.event_type = 'run.status.changed'
                AND e.payload @> '{"terminal":true,"migrated":true,"migration":"063_reliable_runtime_v2"}'::jsonb
                AND e.payload->>'status' = r.status
            )
            OR (
                r.status = 'success'
                AND e.event_type = 'run.completed'
                AND (e.payload->>'status' IS NULL OR e.payload->>'status' = 'success')
            )
            OR (
                r.status = 'failed'
                AND e.event_type = 'run.failed'
                AND (e.payload->>'status' IS NULL OR e.payload->>'status' = 'failed')
            )
            OR (
                r.status = 'timeout'
                AND e.event_type = 'run.failed'
                AND (
                    e.payload->>'status' IS NULL
                    OR e.payload->>'status' IN ('timeout', 'failed')
                )
            )
            OR (
                r.status = 'canceled'
                AND e.event_type = 'run.canceled'
                AND (e.payload->>'status' IS NULL OR e.payload->>'status' = 'canceled')
            )
        )
    ) THEN
        RAISE EXCEPTION 'historical terminal event semantics are inconsistent';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM runs r
        LEFT JOIN run_events e
          ON e.run_id = r.id
         AND e.id = r.terminal_event_id
        WHERE e.id IS NULL
    ) THEN
        RAISE EXCEPTION 'terminal event does not belong to its run';
    END IF;

    IF (
        SELECT COUNT(*) FROM run_accounting_ledger
    ) <> (
        SELECT COUNT(*) FROM runs
    ) THEN
        RAISE EXCEPTION 'accounting ledger row count does not match terminal runs';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM run_accounting_ledger l
        JOIN runs r ON r.id = l.run_id
        WHERE l.agent_id <> r.agent_id
           OR l.terminal_event_id <> r.terminal_event_id
           OR l.success_delta <> CASE WHEN r.status = 'success' THEN 1 ELSE 0 END
           OR l.revenue_delta_cents <> CASE
                WHEN r.status = 'success' THEN r.creator_revenue_cents::bigint
                ELSE 0
              END
    ) THEN
        RAISE EXCEPTION 'accounting ledger does not match historical runs';
    END IF;

    IF EXISTS (SELECT 1 FROM run_attempts)
       OR EXISTS (SELECT 1 FROM run_event_retention_watermarks)
       OR EXISTS (SELECT 1 FROM run_cancellations)
       OR EXISTS (SELECT 1 FROM run_dead_letters)
       OR EXISTS (SELECT 1 FROM runtime_signal_outbox)
       OR EXISTS (SELECT 1 FROM run_effect_outbox)
       OR EXISTS (SELECT 1 FROM run_effect_replays)
       OR EXISTS (SELECT 1 FROM runtime_nodes)
       OR EXISTS (SELECT 1 FROM runtime_sessions)
       OR EXISTS (SELECT 1 FROM runtime_session_attachments)
       OR EXISTS (SELECT 1 FROM runtime_cluster_members) THEN
        RAISE EXCEPTION 'migration fabricated runtime v2 operational history';
    END IF;

    IF EXISTS (SELECT 1 FROM webhook_deliveries WHERE effect_outbox_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM run_deliveries WHERE effect_outbox_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM task_callback_deliveries WHERE effect_outbox_id IS NOT NULL) THEN
        RAISE EXCEPTION 'migration fabricated delivery effect links';
    END IF;

    IF EXISTS (
        SELECT run_id
        FROM run_events
        WHERE payload @> '{"terminal":true,"migrated":true,"migration":"063_reliable_runtime_v2"}'::jsonb
        GROUP BY run_id
        HAVING COUNT(*) <> 1
    ) THEN
        RAISE EXCEPTION 'migration created duplicate terminal marker events';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'runs'
          AND column_name IN ('claimed_by_runtime_token_id', 'claimed_at')
    ) THEN
        RAISE EXCEPTION 'legacy claim columns still exist';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'runs'
          AND column_name = 'request_metadata'
          AND data_type = 'jsonb'
          AND is_nullable = 'NO'
          AND column_default = '''{}''::jsonb'
    ) THEN
        RAISE EXCEPTION 'Run request metadata column is missing or mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        WHERE c.conrelid = 'runs'::regclass
          AND c.conname = 'runs_request_metadata_object'
          AND c.contype = 'c'
          AND c.convalidated
          AND pg_get_constraintdef(c.oid)
              = 'CHECK ((jsonb_typeof(request_metadata) = ''object''::text))'
    ) THEN
        RAISE EXCEPTION 'Run request metadata object constraint is missing or mismatched';
    END IF;

    IF to_regclass('run_event_retention_watermarks') IS NULL THEN
        RAISE EXCEPTION 'Run event retention watermark table is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'run_event_retention_watermarks'
          AND column_name = 'retained_through_sequence'
          AND data_type = 'integer'
          AND is_nullable = 'NO'
          AND column_default = '0'
    ) OR NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'run_event_retention_watermarks'
          AND column_name = 'updated_at'
          AND data_type = 'timestamp with time zone'
          AND is_nullable = 'NO'
          AND column_default = 'clock_timestamp()'
    ) THEN
        RAISE EXCEPTION 'Run event retention watermark columns are mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        WHERE c.conrelid = 'run_event_retention_watermarks'::regclass
          AND c.conname = 'run_event_retention_watermarks_sequence_nonnegative'
          AND c.contype = 'c'
          AND c.convalidated
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        WHERE c.conrelid = 'run_event_retention_watermarks'::regclass
          AND c.conname = 'run_event_retention_watermarks_run_fk'
          AND c.contype = 'f'
          AND c.confrelid = 'runs'::regclass
          AND c.confdeltype = 'a'
          AND c.convalidated
    ) THEN
        RAISE EXCEPTION 'Run event retention watermark constraints are mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'run_effect_outbox'
          AND column_name = 'dead_lettered_at'
          AND data_type = 'timestamp with time zone'
          AND is_nullable = 'YES'
    ) THEN
        RAISE EXCEPTION 'Run effect dead-letter timestamp column is missing or mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        WHERE c.conrelid = 'run_effect_outbox'::regclass
          AND c.conname = 'run_effect_outbox_dead_letter_consistent'
          AND c.contype = 'c'
          AND c.convalidated
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        WHERE c.conrelid = 'run_effect_outbox'::regclass
          AND c.conname = 'run_effect_outbox_last_error_len'
          AND c.contype = 'c'
          AND c.convalidated
    ) THEN
        RAISE EXCEPTION 'Run effect dead-letter constraints are missing or mismatched';
    END IF;

    IF to_regclass('run_effect_replays') IS NULL THEN
        RAISE EXCEPTION 'Run effect replay audit table is missing';
    END IF;

    IF EXISTS (
        SELECT required.column_name
        FROM (VALUES
            ('id', 'uuid', 'NO'),
            ('effect_outbox_id', 'uuid', 'NO'),
            ('actor_type', 'text', 'NO'),
            ('actor_id', 'uuid', 'YES'),
            ('reason', 'text', 'NO'),
            ('replayed_at', 'timestamp with time zone', 'NO')
        ) AS required(column_name, data_type, is_nullable)
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns c
            WHERE c.table_schema = current_schema()
              AND c.table_name = 'run_effect_replays'
              AND c.column_name = required.column_name
              AND c.data_type = required.data_type
              AND c.is_nullable = required.is_nullable
        )
    ) THEN
        RAISE EXCEPTION 'Run effect replay audit columns are mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        WHERE c.conrelid = 'run_effect_replays'::regclass
          AND c.conname = 'run_effect_replays_effect_fk'
          AND c.contype = 'f'
          AND c.confrelid = 'run_effect_outbox'::regclass
          AND c.confdeltype = 'a'
          AND c.convalidated
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        WHERE c.conrelid = 'run_effect_replays'::regclass
          AND c.conname = 'run_effect_replays_actor_type_valid'
          AND c.contype = 'c'
          AND c.convalidated
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        WHERE c.conrelid = 'run_effect_replays'::regclass
          AND c.conname = 'run_effect_replays_reason_len'
          AND c.contype = 'c'
          AND c.convalidated
    ) THEN
        RAISE EXCEPTION 'Run effect replay audit constraints are missing or mismatched';
    END IF;

    IF to_regclass('idx_runs_runtime_pull_claim') IS NOT NULL
       OR to_regclass('idx_runs_runtime_claim_stale') IS NOT NULL
       OR to_regclass('idx_runs_runtime_claimed_token') IS NOT NULL THEN
        RAISE EXCEPTION 'legacy claim indexes still exist';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = current_schema()
          AND t.relname IN (
              'runs',
              'run_events',
              'run_event_retention_watermarks',
              'run_attempts',
              'run_cancellations',
              'run_dead_letters',
              'runtime_signal_outbox',
              'run_effect_outbox',
              'run_effect_replays',
              'run_accounting_ledger',
              'runtime_schema_contracts',
              'runtime_cluster_control',
              'runtime_cluster_members',
              'runtime_nodes',
              'runtime_sessions',
              'runtime_session_attachments',
              'agent_tokens'
          )
          AND NOT c.convalidated
    ) THEN
        RAISE EXCEPTION 'runtime v2 contains unvalidated constraints';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'run_deliveries'
          AND column_name = 'target_id'
          AND is_nullable = 'YES'
    ) THEN
        RAISE EXCEPTION 'run delivery target history fix is missing';
    END IF;

    IF EXISTS (
        SELECT required.index_name
        FROM unnest(ARRAY[
            'idx_runs_idempotency_key',
            'idx_runs_runtime_pending',
            'idx_runs_runtime_pending_global',
            'idx_runs_runtime_retry_due',
            'idx_runs_runtime_retry_due_global',
            'idx_runs_runtime_offer_expiry',
            'idx_runs_runtime_execution_expiry',
            'idx_runs_runtime_deadline',
            'idx_run_attempts_unfinished_run',
            'idx_run_attempts_lease_expiry',
            'idx_run_events_client_event_id',
            'idx_run_events_attempt_client_sequence',
            'idx_run_cancellations_unsettled',
            'idx_runtime_signal_outbox_pending',
            'idx_runtime_signal_outbox_processing_expiry',
            'idx_run_effect_outbox_pending',
            'idx_run_effect_outbox_processing_expiry',
            'idx_run_effect_replays_effect',
            'idx_runtime_sessions_active_worker'
        ]::TEXT[]) AS required(index_name)
        WHERE to_regclass(required.index_name) IS NULL
    ) THEN
        RAISE EXCEPTION 'runtime v2 expected hot-path index is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indexrelid = to_regclass('idx_run_attempts_unfinished_run')
          AND i.indisunique
          AND pg_get_expr(i.indpred, i.indrelid) = '(finished_at IS NULL)'
    ) THEN
        RAISE EXCEPTION 'unfinished Attempt uniqueness predicate is mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indexrelid = to_regclass('idx_runs_runtime_pending_global')
          AND pg_get_expr(i.indpred, i.indrelid)
              = '((status = ''running''::text) AND (dispatch_state = ''pending''::text))'
    ) THEN
        RAISE EXCEPTION 'global pending Run index predicate is mismatched';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indexrelid = to_regclass('idx_runs_runtime_retry_due_global')
          AND pg_get_expr(i.indpred, i.indrelid)
              = '((status = ''running''::text) AND (dispatch_state = ''retry_wait''::text))'
    ) THEN
        RAISE EXCEPTION 'global retry Run index predicate is mismatched';
    END IF;

    IF EXISTS (
        SELECT required.trigger_name
        FROM (VALUES
            ('runs_v2_contract_identity', 'runs', 'enforce_run_v2_contract_identity'),
            ('run_attempts_identity_immutable', 'run_attempts', 'enforce_run_attempt_identity_immutable'),
            ('run_attempts_event_sequence_consistency', 'run_attempts', 'enforce_attempt_event_sequence_consistency'),
            ('run_events_immutable', 'run_events', 'enforce_run_event_immutable'),
            ('run_events_attempt_sequence_consistency', 'run_events', 'enforce_attempt_event_sequence_consistency'),
            ('run_event_retention_watermarks_forward_only', 'run_event_retention_watermarks', 'enforce_run_event_retention_watermark'),
            ('runs_active_attempt_consistency', 'runs', 'enforce_run_active_attempt_consistency'),
            ('run_attempts_active_run_consistency', 'run_attempts', 'enforce_run_active_attempt_consistency'),
            ('runs_terminal_artifacts_consistency', 'runs', 'enforce_run_terminal_artifacts_consistency'),
            ('run_accounting_ledger_run_consistency', 'run_accounting_ledger', 'enforce_run_terminal_artifacts_consistency'),
            ('run_dead_letters_run_consistency', 'run_dead_letters', 'enforce_run_terminal_artifacts_consistency'),
            ('run_events_terminal_artifacts_consistency', 'run_events', 'enforce_run_terminal_artifacts_consistency'),
            ('run_effect_outbox_run_consistency', 'run_effect_outbox', 'enforce_run_terminal_artifacts_consistency'),
            ('run_accounting_ledger_immutable', 'run_accounting_ledger', 'enforce_run_terminal_artifact_immutable'),
            ('run_dead_letters_immutable', 'run_dead_letters', 'enforce_run_terminal_artifact_immutable'),
            ('run_effect_outbox_identity_immutable', 'run_effect_outbox', 'enforce_run_effect_identity_immutable'),
            ('run_effect_replays_immutable', 'run_effect_replays', 'enforce_run_effect_replay_immutable'),
            ('runs_cancellation_summary_consistency', 'runs', 'enforce_run_cancellation_summary_consistency'),
            ('run_cancellations_run_summary_consistency', 'run_cancellations', 'enforce_run_cancellation_summary_consistency'),
            ('run_cancellations_state_forward_only', 'run_cancellations', 'enforce_run_cancellation_transition'),
            ('runtime_sessions_identity_immutable', 'runtime_sessions', 'enforce_runtime_session_identity_immutable'),
            ('runtime_sessions_principal_valid', 'runtime_sessions', 'enforce_runtime_session_principal'),
            ('runtime_sessions_attachment_consistency', 'runtime_sessions', 'enforce_runtime_session_attachment_consistency'),
            ('runtime_session_attachments_session_consistency', 'runtime_session_attachments', 'enforce_runtime_session_attachment_consistency'),
            ('runtime_session_attachments_history', 'runtime_session_attachments', 'enforce_runtime_session_attachment_history'),
            ('runtime_nodes_session_contract_consistency', 'runtime_nodes', 'enforce_runtime_node_session_contract_consistency'),
            ('runtime_sessions_node_contract_consistency', 'runtime_sessions', 'enforce_runtime_node_session_contract_consistency'),
            ('runtime_nodes_identity_and_lifecycle', 'runtime_nodes', 'enforce_runtime_node_identity_and_lifecycle'),
            ('agent_tokens_identity_and_lifecycle', 'agent_tokens', 'enforce_agent_token_identity_and_lifecycle'),
            ('runtime_nodes_revocation_guard', 'runtime_nodes', 'enforce_runtime_node_revocation_guard'),
            ('agent_tokens_runtime_revocation_guard', 'agent_tokens', 'enforce_runtime_token_revocation_guard')
        ) AS required(trigger_name, table_name, function_name)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_trigger t
            WHERE t.tgname = required.trigger_name
              AND t.tgrelid = to_regclass(required.table_name)
              AND t.tgfoid = to_regprocedure(required.function_name || '()')
              AND t.tgenabled = 'O'
              AND t.tgtype = CASE
                  WHEN required.trigger_name = 'runs_v2_contract_identity'
                      THEN 31
                  WHEN required.trigger_name = 'run_event_retention_watermarks_forward_only'
                      THEN 31
                  WHEN required.trigger_name IN (
                      'run_attempts_identity_immutable',
                      'run_events_immutable',
                      'run_accounting_ledger_immutable',
                      'run_dead_letters_immutable',
                      'run_effect_outbox_identity_immutable',
                      'run_effect_replays_immutable',
                      'run_cancellations_state_forward_only',
                      'runtime_sessions_identity_immutable',
                      'runtime_session_attachments_history',
                      'runtime_nodes_identity_and_lifecycle',
                      'agent_tokens_identity_and_lifecycle'
                  ) THEN 27
                  WHEN required.trigger_name = 'runtime_sessions_principal_valid'
                      THEN 23
                  WHEN required.trigger_name IN (
                      'runtime_nodes_revocation_guard',
                      'agent_tokens_runtime_revocation_guard'
                  ) THEN 19
                  WHEN required.trigger_name = 'run_events_attempt_sequence_consistency'
                      THEN 13
                  ELSE 29
              END
              AND t.tgdeferrable = (
                  required.trigger_name IN (
                      'run_attempts_event_sequence_consistency',
                      'run_events_attempt_sequence_consistency',
                      'runs_active_attempt_consistency',
                      'run_attempts_active_run_consistency',
                      'runs_terminal_artifacts_consistency',
                      'run_accounting_ledger_run_consistency',
                      'run_dead_letters_run_consistency',
                      'run_events_terminal_artifacts_consistency',
                      'run_effect_outbox_run_consistency',
                      'runs_cancellation_summary_consistency',
                      'run_cancellations_run_summary_consistency',
                      'runtime_sessions_attachment_consistency',
                      'runtime_session_attachments_session_consistency',
                      'runtime_nodes_session_contract_consistency',
                      'runtime_sessions_node_contract_consistency'
                  )
              )
              AND t.tginitdeferred = t.tgdeferrable
              AND NOT t.tgisinternal
        )
    ) THEN
        RAISE EXCEPTION 'runtime v2 invariant trigger is missing';
    END IF;
END
$$;

SELECT
    (SELECT COUNT(*) FROM runs) AS historical_runs,
    (SELECT COUNT(*) FROM run_accounting_ledger) AS ledger_rows,
    (
        SELECT COUNT(*)
        FROM run_events
        WHERE payload @> '{"terminal":true,"migrated":true,"migration":"063_reliable_runtime_v2"}'::jsonb
    ) AS migrated_terminal_events;

SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = current_schema()
  AND indexname IN (
      'idx_runs_idempotency_key',
      'idx_runs_runtime_pending',
      'idx_runs_runtime_pending_global',
      'idx_runs_runtime_retry_due',
      'idx_runs_runtime_retry_due_global',
      'idx_runs_runtime_offer_expiry',
      'idx_runs_runtime_execution_expiry',
      'idx_run_events_client_event_id',
      'idx_run_events_attempt_client_sequence',
      'idx_run_attempts_unfinished_run',
      'idx_runtime_signal_outbox_pending',
      'idx_run_effect_outbox_pending',
      'idx_run_effect_replays_effect',
      'idx_runtime_sessions_active_worker'
  )
ORDER BY indexname;
