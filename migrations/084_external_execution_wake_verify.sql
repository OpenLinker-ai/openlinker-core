DO $$
DECLARE
    required RECORD;
BEGIN
    IF to_regprocedure('emit_external_execution_wake_notification()') IS NULL THEN
        RAISE EXCEPTION 'external execution wake function is missing';
    END IF;
    FOR required IN
        SELECT * FROM (VALUES
            ('external_execution_cancellations'::regclass, 'external_execution_cancellations_wake_insert'),
            ('external_execution_cancellations'::regclass, 'external_execution_cancellations_wake_update'),
            ('external_executions'::regclass, 'external_executions_cancellation_wake_update'),
            ('run_cancellations'::regclass, 'run_cancellations_external_execution_wake_insert'),
            ('run_cancellations'::regclass, 'run_cancellations_external_execution_wake_update'),
            ('workflow_run_cancellations'::regclass, 'workflow_run_cancellations_external_execution_wake_insert'),
            ('workflow_run_cancellations'::regclass, 'workflow_run_cancellations_external_execution_wake_update')
        ) AS expected(table_oid, trigger_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = required.table_oid
              AND tgname = required.trigger_name
              AND NOT tgisinternal
        ) THEN
            RAISE EXCEPTION 'external execution wake trigger % is missing', required.trigger_name;
        END IF;
    END LOOP;
    IF pg_get_functiondef('emit_external_execution_wake_notification()'::regprocedure)
           NOT LIKE '%external_execution.changed%'
       OR pg_get_functiondef('emit_external_execution_wake_notification()'::regprocedure)
           NOT LIKE '%openlinker_external_v1%'
       OR pg_get_functiondef('emit_external_execution_wake_notification()'::regprocedure)
           LIKE '%input%'
       OR pg_get_functiondef('emit_external_execution_wake_notification()'::regprocedure)
           LIKE '%output%'
       OR pg_get_functiondef('emit_external_execution_wake_notification()'::regprocedure)
           LIKE '%token%' THEN
        RAISE EXCEPTION 'external execution wake payload boundary is invalid';
    END IF;
END
$$;
