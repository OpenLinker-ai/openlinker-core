BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Packages stay private by default. Publication is per immutable version, so a
-- later private draft never changes what public readers can see or import.
ALTER TABLE public.skill_packages
    ADD COLUMN visibility text DEFAULT 'private'::text NOT NULL,
    ADD COLUMN source_package_id uuid,
    ADD CONSTRAINT skill_packages_visibility_valid CHECK (visibility IN ('private', 'unlisted', 'public'));

-- Import provenance is informational and intentionally has no foreign key: an
-- imported private copy must survive withdrawal or deletion of its source.
ALTER TABLE public.skill_package_versions
    ADD COLUMN published_at timestamptz,
    ADD COLUMN source_version_id uuid;

CREATE INDEX skill_packages_public_listing ON public.skill_packages(updated_at DESC, id)
    WHERE visibility = 'public';
CREATE INDEX skill_package_versions_published ON public.skill_package_versions(package_id, created_at DESC)
    WHERE published_at IS NOT NULL;
CREATE INDEX skill_packages_owner_source ON public.skill_packages(owner_user_id, source_package_id)
    WHERE source_package_id IS NOT NULL;
COMMIT;
