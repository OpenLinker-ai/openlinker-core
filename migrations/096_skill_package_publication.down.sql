-- Dropping publication state would silently unpublish packages and erase import
-- provenance. Re-adding the columns would also change catalog column ordinals
-- covered by the schema fingerprint. Restore with a compatible Core instead.
DO $$ BEGIN
    RAISE EXCEPTION '096 skill package publication cannot be rolled back';
END $$;
