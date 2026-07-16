DO $$
BEGIN
    IF to_regclass('public.external_execution_keys') IS NULL
       OR to_regclass('public.external_execution_cancellations') IS NULL
       OR to_regclass('public.workflow_run_cancellations') IS NULL
       OR to_regclass('public.workflow_step_launches') IS NULL THEN
        RAISE EXCEPTION 'migration 077 cancellation schema is incomplete';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM external_executions e
        LEFT JOIN external_execution_keys k
          ON k.caller_service_id = e.caller_service_id
         AND k.external_request_id = e.external_request_id
         AND k.actor_user_id = e.actor_user_id
        WHERE k.external_request_id IS NULL
    ) THEN
        RAISE EXCEPTION 'migration 077 found an external execution without a key';
    END IF;
    IF (
        SELECT COUNT(*) FROM runtime_schema_contracts
        WHERE schema_version = 77
          AND migration_name = '077_external_execution_cancellation'
          AND runtime_contract_id = 'openlinker.runtime.v2'
          AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
          AND is_current
    ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
        RAISE EXCEPTION 'migration 077 Runtime schema contract is not current';
    END IF;
    IF (SELECT COUNT(*) FROM runtime_wire_contracts
        WHERE runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
          AND support_tier = 'current') <> 1
       OR (SELECT COUNT(*) FROM runtime_wire_contracts
           WHERE runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
             AND support_tier = 'previous') <> 1 THEN
        RAISE EXCEPTION 'migration 077 Runtime wire ring is invalid';
    END IF;
    IF (
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'runtime_sessions'
          AND (
              (column_name = 'drain_requested_at' AND data_type = 'timestamp with time zone')
              OR (column_name = 'drain_deadline_at' AND data_type = 'timestamp with time zone')
              OR (column_name = 'drain_reason_code' AND data_type = 'text')
              OR (column_name = 'resume_capacity' AND data_type = 'integer')
          )
    ) <> 4 THEN
        RAISE EXCEPTION 'migration 077 Runtime Session drain evidence is incomplete';
    END IF;
	IF (
		SELECT COUNT(*) FROM pg_constraint
		WHERE conname = ANY (ARRAY[
			'runtime_wire_contracts_support_identity',
			'runtime_nodes_contract_current',
			'runtime_nodes_required_features',
			'runtime_sessions_contract_current',
			'runtime_sessions_required_features',
			'runtime_sessions_drain_evidence_consistent',
			'external_executions_key_actor_fkey',
			'external_executions_downstream_identity_complete',
			'external_executions_start_state_valid',
			'external_execution_cancellations_attachment_complete',
			'external_execution_cancellations_timestamps_valid',
			'workflow_run_cancellations_timestamps_valid',
			'workflow_step_launches_state_valid'
		]::TEXT[])
	) <> 13 THEN
		RAISE EXCEPTION 'migration 077 critical constraints are incomplete';
	END IF;
	IF (
		SELECT COUNT(*) FROM pg_class
		WHERE relkind = 'i'
		  AND relname = ANY (ARRAY[
			'idx_runtime_wire_contracts_current',
			'idx_runtime_wire_contracts_previous',
			'idx_external_execution_cancellations_reconcile',
			'idx_workflow_run_cancellations_reconcile',
			'idx_workflow_step_launches_reconcile'
		]::TEXT[])
	) <> 5 THEN
		RAISE EXCEPTION 'migration 077 critical indexes are incomplete';
	END IF;
	IF (
		SELECT COUNT(*)
		FROM pg_trigger trigger
		JOIN pg_class relation ON relation.oid = trigger.tgrelid
		WHERE trigger.tgname = 'runtime_nodes_identity_and_lifecycle'
		  AND relation.relname = 'runtime_nodes'
		  AND NOT trigger.tgisinternal
	) <> 1
	   OR POSITION(
		'openlinker.runtime_node_activation'
		IN pg_get_functiondef('enforce_runtime_node_identity_and_lifecycle()'::regprocedure)
	   ) = 0
	   OR POSITION(
		'runtime node generation cannot change with live or resumable sessions'
		IN pg_get_functiondef('enforce_runtime_node_identity_and_lifecycle()'::regprocedure)
	   ) = 0 THEN
		RAISE EXCEPTION 'migration 077 Runtime Node lifecycle trigger is incomplete';
	END IF;
END
$$;
