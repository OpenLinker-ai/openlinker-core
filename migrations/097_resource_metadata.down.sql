DO $$ BEGIN
 RAISE EXCEPTION '097_resource_metadata is forward-only: frozen publication declarations must be preserved';
END $$;
