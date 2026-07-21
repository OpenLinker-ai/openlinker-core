DO $$
BEGIN
    IF to_regclass('public.runtime_pki_authorities') IS NULL
       OR to_regclass('public.runtime_node_bindings') IS NULL
       OR to_regclass('public.runtime_node_certificates') IS NULL THEN
        RAISE EXCEPTION 'automatic Runtime mTLS credential schema is incomplete';
    END IF;
END
$$;
