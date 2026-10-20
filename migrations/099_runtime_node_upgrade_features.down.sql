-- Feature authorization history and admission fences must survive Node rollback.
DO $$ BEGIN
    RAISE EXCEPTION '099 runtime node upgrade feature fences cannot be rolled back';
END $$;
