DO $$
BEGIN
    IF to_regclass('idx_run_events_metric_cursor') IS NULL THEN
        RAISE EXCEPTION 'RunEvent metric cursor index is missing';
    END IF;
END
$$;
