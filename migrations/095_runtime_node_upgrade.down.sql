-- Removing durable version/epoch fences is not a supported rollback. Restore
-- service with a compatible Core; reverse Node versions via a new operation.
DO $$ BEGIN
    RAISE EXCEPTION '095 runtime node upgrade fences cannot be rolled back';
END $$;
