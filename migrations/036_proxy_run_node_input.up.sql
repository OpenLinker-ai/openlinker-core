BEGIN;

ALTER TABLE proxy_runs
    ADD COLUMN node_input JSONB;

UPDATE proxy_runs
SET node_input = input
WHERE status = 'pending';

UPDATE proxy_runs
SET input = '{}'::jsonb,
    input_summary = NULL
WHERE payload_policy = 'metadata_only';

UPDATE proxy_runs
SET input = '{}'::jsonb
WHERE payload_policy = 'store_run_summary';

UPDATE proxy_runs
SET output = '{}'::jsonb,
    output_summary = CASE
        WHEN status IN ('failed', 'timeout') THEN output_summary
        ELSE NULL
    END
WHERE payload_policy = 'metadata_only';

UPDATE proxy_runs
SET output = '{}'::jsonb
WHERE payload_policy = 'store_run_summary';

COMMIT;
