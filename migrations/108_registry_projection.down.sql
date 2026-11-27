BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

LOCK TABLE public.registry_projection_settings, public.registry_projection_dirty, public.registry_projection_links,
    public.skill_package_registry_links, public.registry_projection_version_errors, public.registry_projection_exclusions,
    public.registry_projection_revisions IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.registry_projection_settings WHERE mode <> 'local') THEN
    RAISE EXCEPTION 'registry projection is not local; roll it back before downgrading';
  END IF;
  IF EXISTS (SELECT 1 FROM public.registry_projection_links) OR EXISTS (SELECT 1 FROM public.skill_package_registry_links)
     OR EXISTS (SELECT 1 FROM public.registry_projection_dirty) OR EXISTS (SELECT 1 FROM public.registry_projection_exclusions) THEN
    RAISE EXCEPTION 'registry projection records exist; refusing to drop them';
  END IF;
END $$;

DROP TRIGGER registry_projection_mcp_metadata_trg ON public.mcp_service_metadata;
DROP TRIGGER registry_projection_agents_trg ON public.agents;
DROP TRIGGER registry_projection_users_trg ON public.users;
DROP TRIGGER registry_projection_skill_versions_trg ON public.skill_package_versions;
DROP TRIGGER registry_projection_skill_packages_trg ON public.skill_packages;
DROP FUNCTION public.registry_projection_mcp_metadata();
DROP FUNCTION public.registry_projection_agents();
DROP FUNCTION public.registry_projection_users();
DROP FUNCTION public.registry_projection_skill_versions();
DROP FUNCTION public.registry_projection_skill_packages();
DROP FUNCTION public.registry_projection_mark(text, uuid, smallint);
DROP TABLE public.registry_projection_exclusions;
DROP TABLE public.registry_projection_version_errors;
DROP TABLE public.skill_package_registry_links;
DROP TABLE public.registry_projection_links;
DROP TABLE public.registry_projection_dirty;
DROP TABLE public.registry_projection_revisions;
DROP TABLE public.registry_projection_settings;

COMMIT;
