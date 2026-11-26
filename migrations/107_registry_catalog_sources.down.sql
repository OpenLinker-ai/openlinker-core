BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

LOCK TABLE public.skill_package_versions, public.agent_catalog_sources IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.skill_package_versions WHERE source_kind IS NOT NULL) THEN
    RAISE EXCEPTION 'skill package versions carry source records; refusing to drop them';
  END IF;
  IF EXISTS (SELECT 1 FROM public.agent_catalog_sources) THEN
    RAISE EXCEPTION 'agents carry catalog source records; refusing to drop them';
  END IF;
END $$;

DROP TABLE public.agent_catalog_sources;
DROP INDEX public.skill_package_versions_registry_source_idx;
ALTER TABLE public.skill_package_versions
  DROP CONSTRAINT skill_source_publishable,
  DROP CONSTRAINT skill_source_complete,
  DROP COLUMN source_redistributable,
  DROP COLUMN source_checked_at,
  DROP COLUMN source_license,
  DROP COLUMN source_registry_mode,
  DROP COLUMN source_registry_digest,
  DROP COLUMN source_registry_version,
  DROP COLUMN source_registry_entry,
  DROP COLUMN source_registry_url,
  DROP COLUMN source_tree_sha,
  DROP COLUMN source_path,
  DROP COLUMN source_commit,
  DROP COLUMN source_repository,
  DROP COLUMN source_kind;

COMMIT;
