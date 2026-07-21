DO $$
DECLARE
    trigger_count INTEGER;
    function_definition TEXT;
BEGIN
    IF to_regprocedure('emit_event_wake_notification()') IS NULL THEN
        RAISE EXCEPTION 'migration 082 event wake function is missing';
    END IF;

    SELECT COUNT(*)
    INTO trigger_count
    FROM pg_trigger
    WHERE NOT tgisinternal
      AND tgenabled = 'O'
      AND (tgrelid, tgname) IN (
          ('run_events'::regclass, 'run_events_event_wake_insert'),
          ('runs'::regclass, 'runs_event_wake_insert'),
          ('runs'::regclass, 'runs_event_wake_state_update'),
          ('runtime_signal_outbox'::regclass, 'runtime_signal_outbox_event_wake_insert'),
          ('runtime_signal_outbox'::regclass, 'runtime_signal_outbox_event_wake_due_update'),
          ('run_effect_outbox'::regclass, 'run_effect_outbox_event_wake_insert'),
          ('run_effect_outbox'::regclass, 'run_effect_outbox_event_wake_due_update')
      );
    IF trigger_count <> 7 THEN
        RAISE EXCEPTION 'migration 082 event wake trigger set is incomplete';
    END IF;

    SELECT pg_get_functiondef('emit_event_wake_notification()'::regprocedure)
    INTO function_definition;
    IF function_definition NOT LIKE '%openlinker_run_v1%'
       OR function_definition NOT LIKE '%openlinker_work_v1%'
       OR function_definition NOT LIKE '%work.runtime_signal.available%'
       OR function_definition NOT LIKE '%work.run_effect.available%'
       OR function_definition NOT LIKE '%octet_length(payload) > 1024%'
       OR function_definition LIKE '%to_jsonb(NEW)%' THEN
        RAISE EXCEPTION 'migration 082 event wake payload contract is invalid';
    END IF;
END
$$;
