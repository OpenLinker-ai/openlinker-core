BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.skill_package_versions ADD COLUMN publication_metadata jsonb;
-- Freeze unknown declarations for currently published legacy versions. Legacy
-- withdrawn versions have no publication-history marker and freeze on republish.
UPDATE public.skill_package_versions SET publication_metadata='{}'::jsonb WHERE published_at IS NOT NULL;
ALTER TABLE public.skill_package_versions
 ADD CONSTRAINT skill_package_publication_metadata_object CHECK (publication_metadata IS NULL OR (jsonb_typeof(publication_metadata)='object' AND octet_length(publication_metadata::text)<=65536)),
 ADD CONSTRAINT skill_package_published_metadata_required CHECK (published_at IS NULL OR publication_metadata IS NOT NULL);

CREATE TABLE public.mcp_service_metadata (
 agent_id uuid PRIMARY KEY REFERENCES public.agents(id) ON DELETE CASCADE,
 metadata jsonb NOT NULL CHECK (jsonb_typeof(metadata)='object' AND octet_length(metadata::text)<=65536),
 revision bigint NOT NULL CHECK (revision>=1),
 updated_at timestamptz NOT NULL DEFAULT now()
);
COMMIT;
