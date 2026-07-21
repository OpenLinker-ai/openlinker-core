DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'runtime_node_bindings'
          AND column_name = 'binding_mode'
          AND is_nullable = 'NO'
          AND column_default = '''mtls''::text'
    ) THEN
        RAISE EXCEPTION 'runtime_node_bindings.binding_mode is missing or unsafe';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'runtime_node_bindings'::regclass
          AND conname = 'runtime_node_bindings_mode_valid'
    ) THEN
        RAISE EXCEPTION 'runtime_node_bindings binding_mode check is missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'runtime_node_certificates'::regclass
          AND conname = 'runtime_node_certificates_replay_material_shape'
    ) THEN
        RAISE EXCEPTION 'Runtime certificate replay material constraint is missing';
    END IF;

    IF to_regclass('public.idx_runtime_node_certificates_retention') IS NULL THEN
        RAISE EXCEPTION 'Runtime certificate retention index is missing';
    END IF;
END
$$;
