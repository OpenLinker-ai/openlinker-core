-- 086_current_schema_init_verify.sql
-- Run after the Core initializer and before Cloud initialization.

DO $$
DECLARE
    public_tables BIGINT;
    public_constraints BIGINT;
    public_indexes BIGINT;
    public_triggers BIGINT;
    public_functions BIGINT;
BEGIN
    SELECT count(*) INTO public_tables
    FROM pg_catalog.pg_tables
    WHERE schemaname = 'public'
      AND tablename NOT IN ('schema_migrations', 'schema_migrations_cloud');
    IF public_tables <> 69 THEN
        RAISE EXCEPTION 'Core initializer table count is %, expected 69', public_tables;
    END IF;

    SELECT count(*) INTO public_constraints
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public'
      AND r.relname NOT IN ('schema_migrations', 'schema_migrations_cloud');
    IF public_constraints <> 587 THEN
        RAISE EXCEPTION 'Core initializer constraint count is %, expected 587', public_constraints;
    END IF;

    SELECT count(*) INTO public_indexes
    FROM pg_catalog.pg_indexes
    WHERE schemaname = 'public'
      AND tablename NOT IN ('schema_migrations', 'schema_migrations_cloud');
    IF public_indexes <> 259 THEN
        RAISE EXCEPTION 'Core initializer index count is %, expected 259', public_indexes;
    END IF;

    SELECT count(*) INTO public_triggers
    FROM pg_catalog.pg_trigger t
    JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname NOT IN ('schema_migrations', 'schema_migrations_cloud')
      AND NOT t.tgisinternal;
    IF public_triggers <> 70 THEN
        RAISE EXCEPTION 'Core initializer trigger count is %, expected 70', public_triggers;
    END IF;

    SELECT count(*) INTO public_functions
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public';
    IF public_functions <> 65 THEN
        RAISE EXCEPTION 'Core initializer function count is %, expected 65', public_functions;
    END IF;

    IF (SELECT count(*) FROM skills WHERE id IN (
        'content/translation', 'content/summarization', 'content/copywriting',
        'content/proofreading', 'content/structured-data', 'dev/code-review',
        'dev/code-generation', 'dev/code-explanation', 'dev/test-generation',
        'dev/devops-ci', 'data/sql-query', 'data/data-cleaning', 'data/analysis',
        'data/visualization', 'data/forecasting', 'media/image-generate',
        'media/image-edit', 'media/audio-transcribe', 'media/audio-generate',
        'media/video-process', 'ops/document-generate', 'ops/email-process',
        'ops/scheduling', 'ops/web-scraping', 'ops/notification', 'ai/rag',
        'ai/agent-orchestration', 'ai/finetune', 'ai/prompt-engineering',
        'ai/safety-eval'
    )) <> 30 THEN
        RAISE EXCEPTION 'Core initializer built-in skills are incomplete';
    END IF;

    IF (SELECT count(*) FROM skill_test_cases WHERE skill_id IN (
        'content/translation', 'content/summarization', 'dev/code-review',
        'data/sql-query', 'ops/email-process'
    )) < 15 THEN
        RAISE EXCEPTION 'Core initializer benchmark cases are incomplete';
    END IF;

    IF (SELECT count(*) FROM core_instance_identity WHERE singleton) <> 1
       OR (SELECT count(*) FROM runtime_cluster_control WHERE singleton_id = 1) <> 1 THEN
        RAISE EXCEPTION 'Core singleton initialization is incomplete';
    END IF;

    IF (SELECT count(*) FROM runtime_schema_contracts) <> 10
       OR (SELECT count(*) FROM runtime_schema_contracts WHERE is_current) <> 1
       OR NOT EXISTS (
            SELECT 1 FROM runtime_schema_contracts
            WHERE schema_version = 80
              AND migration_name = '080_runtime_attempt_transport_evidence'
              AND runtime_contract_id = 'openlinker.runtime.v2'
              AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
              AND is_current
       ) THEN
        RAISE EXCEPTION 'Core Runtime schema contract initialization is inconsistent';
    END IF;

    IF (SELECT count(*) FROM runtime_wire_contracts) <> 5
       OR NOT EXISTS (
            SELECT 1 FROM runtime_wire_contracts
            WHERE runtime_contract_id = 'openlinker.runtime.v2'
              AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481'
              AND support_tier = 'current'
       )
       OR NOT EXISTS (
            SELECT 1 FROM runtime_wire_contracts
            WHERE runtime_contract_id = 'openlinker.runtime.v2'
              AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
              AND support_tier = 'previous'
       ) THEN
        RAISE EXCEPTION 'Core Runtime wire contract initialization is inconsistent';
    END IF;

    IF to_regclass('public.idx_run_events_metric_cursor') IS NULL
       OR to_regclass('public.idx_runtime_node_certificates_retention') IS NULL
       OR to_regclass('public.idx_runtime_sessions_credential_lifecycle') IS NULL THEN
        RAISE EXCEPTION 'Core initializer is missing a required current index';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'runtime_node_bindings'
          AND column_name = 'binding_mode'
          AND is_nullable = 'NO'
          AND column_default = '''mtls''::text'
    ) THEN
        RAISE EXCEPTION 'Core initializer is missing Runtime token-only binding state';
    END IF;
END
$$;
