BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.skill_packages (
    id uuid PRIMARY KEY,
    owner_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    description text NOT NULL CHECK (length(description) BETWEEN 1 AND 2000),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX skill_packages_owner ON public.skill_packages(owner_user_id, updated_at DESC);

CREATE TABLE public.skill_package_versions (
    id uuid PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES public.skill_packages(id) ON DELETE CASCADE,
    version text NOT NULL CHECK (length(version) BETWEEN 1 AND 64),
    digest text NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
    payload text NOT NULL CHECK (octet_length(payload) <= 65536),
    capability_ids text[] NOT NULL DEFAULT '{}',
    providers text[] NOT NULL CHECK (cardinality(providers) BETWEEN 1 AND 2 AND providers <@ ARRAY['codex','claude']::text[]),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (package_id, version),
    UNIQUE (package_id, id)
);

CREATE TABLE public.agent_skill_package_bindings (
    agent_id uuid NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    package_id uuid NOT NULL REFERENCES public.skill_packages(id) ON DELETE CASCADE,
    version_id uuid NOT NULL,
    binding_id uuid NOT NULL UNIQUE,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','loaded','failed')),
    error_code text NOT NULL DEFAULT '',
    last_run_id uuid REFERENCES public.runs(id) ON DELETE SET NULL,
    loaded_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (agent_id, package_id),
    FOREIGN KEY (package_id, version_id) REFERENCES public.skill_package_versions(package_id, id) ON DELETE RESTRICT
);

CREATE TABLE public.run_skill_package_snapshots (
    run_id uuid NOT NULL REFERENCES public.runs(id) ON DELETE CASCADE,
    package_id uuid NOT NULL,
    version_id uuid NOT NULL,
    binding_id uuid NOT NULL,
    digest text NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, package_id),
    FOREIGN KEY (package_id, version_id) REFERENCES public.skill_package_versions(package_id, id) ON DELETE RESTRICT
);
CREATE INDEX run_skill_package_snapshots_version ON public.run_skill_package_snapshots(package_id, version_id);
COMMIT;
