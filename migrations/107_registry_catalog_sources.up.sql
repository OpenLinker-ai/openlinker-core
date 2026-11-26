BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.skill_package_versions
  ADD COLUMN source_kind text CHECK (source_kind IN ('github','registry')),
  ADD COLUMN source_repository text,
  ADD COLUMN source_commit text,
  ADD COLUMN source_path text,
  ADD COLUMN source_tree_sha text,
  ADD COLUMN source_registry_url text,
  ADD COLUMN source_registry_entry text,
  ADD COLUMN source_registry_version text,
  ADD COLUMN source_registry_digest text,
  ADD COLUMN source_registry_mode text CHECK (source_registry_mode IN ('indexed','hosted')),
  ADD COLUMN source_license text,
  ADD COLUMN source_checked_at timestamptz,
  ADD COLUMN source_redistributable boolean,
  ADD CONSTRAINT skill_source_complete CHECK (
    source_kind IS NULL
    OR (source_kind = 'github' AND source_repository IS NOT NULL
        AND source_commit IS NOT NULL AND source_commit ~ '^[0-9a-f]{40}$'
        AND source_path IS NOT NULL AND source_tree_sha IS NOT NULL
        AND source_redistributable IS NOT NULL)
    OR (source_kind = 'registry' AND source_registry_url IS NOT NULL AND source_registry_entry IS NOT NULL
        AND source_registry_version IS NOT NULL
        AND source_registry_digest IS NOT NULL AND source_registry_digest ~ '^sha256:[0-9a-f]{64}$'
        AND source_registry_mode IS NOT NULL AND source_redistributable IS NOT NULL)),
  ADD CONSTRAINT skill_source_publishable CHECK (
    published_at IS NULL OR source_kind IS NULL OR source_redistributable IS TRUE);

CREATE INDEX skill_package_versions_registry_source_idx
  ON public.skill_package_versions (source_registry_url, source_registry_entry, source_registry_version)
  WHERE source_kind = 'registry';

CREATE TABLE public.agent_catalog_sources (
  agent_id uuid PRIMARY KEY REFERENCES public.agents(id) ON DELETE CASCADE,
  registry_url text NOT NULL CHECK (registry_url ~ '^https?://'),
  server_name text NOT NULL CHECK (length(server_name) BETWEEN 3 AND 200),
  server_version text NOT NULL CHECK (length(server_version) BETWEEN 1 AND 255),
  recorded_at timestamptz NOT NULL DEFAULT now()
);

COMMIT;
