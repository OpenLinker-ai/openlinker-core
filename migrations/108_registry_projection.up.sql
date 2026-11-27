BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE public.registry_projection_settings (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    mode text NOT NULL DEFAULT 'local' CHECK (mode IN ('local', 'registry', 'draining')),
    core_instance uuid NOT NULL DEFAULT gen_random_uuid(),
    registry_url text,
    consumer text,
    reconciled_at timestamptz,
    changed_at timestamptz NOT NULL DEFAULT now(),
    CHECK (mode = 'local' OR (registry_url IS NOT NULL AND consumer IS NOT NULL))
);
INSERT INTO public.registry_projection_settings (singleton) VALUES (true);

CREATE TABLE public.registry_projection_revisions (
    kind text NOT NULL CHECK (kind IN ('skill', 'mcp')),
    subject_id uuid NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    PRIMARY KEY (kind, subject_id)
);

CREATE TABLE public.registry_projection_dirty (
    kind text NOT NULL CHECK (kind IN ('skill', 'mcp')),
    subject_id uuid NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    priority smallint NOT NULL DEFAULT 1 CHECK (priority IN (0, 1)),
    marked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_token uuid,
    claimed_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_error text NOT NULL DEFAULT '' CHECK (length(last_error) <= 500),
    suspended boolean NOT NULL DEFAULT false,
    PRIMARY KEY (kind, subject_id),
    CHECK ((claim_token IS NULL) = (claimed_at IS NULL))
);
CREATE INDEX registry_projection_dirty_due ON public.registry_projection_dirty (priority, marked_at);

CREATE TABLE public.registry_projection_links (
    kind text NOT NULL CHECK (kind IN ('skill', 'mcp')),
    subject_id uuid NOT NULL,
    registry_url text NOT NULL,
    registry_entry text NOT NULL,
    registry_namespace text NOT NULL DEFAULT '',
    owner_id uuid NOT NULL,
    attempted boolean NOT NULL DEFAULT false,
    applied_revision bigint NOT NULL DEFAULT 0,
    applied_phase smallint NOT NULL DEFAULT 0,
    applied_hidden boolean NOT NULL DEFAULT true,
    mcp_version text NOT NULL DEFAULT '',
    mcp_digest text NOT NULL DEFAULT '',
    linked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, subject_id)
);

CREATE TABLE public.skill_package_registry_links (
    package_id uuid NOT NULL,
    version_id uuid NOT NULL,
    registry_url text NOT NULL,
    registry_entry text NOT NULL,
    registry_version text NOT NULL,
    registry_digest text NOT NULL CHECK (registry_digest ~ '^sha256:[0-9a-f]{64}$'),
    linked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (package_id, version_id),
    FOREIGN KEY (package_id, version_id) REFERENCES public.skill_package_versions (package_id, id) ON DELETE RESTRICT,
    UNIQUE (registry_url, registry_entry, registry_version)
);

CREATE TABLE public.registry_projection_version_errors (
    package_id uuid NOT NULL,
    version_id uuid NOT NULL,
    code text NOT NULL CHECK (length(code) BETWEEN 1 AND 100),
    message text NOT NULL DEFAULT '' CHECK (length(message) <= 500),
    at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (package_id, version_id)
);

CREATE TABLE public.registry_projection_exclusions (
    kind text NOT NULL CHECK (kind IN ('skill', 'mcp')),
    subject_id uuid NOT NULL,
    version_id uuid,
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (kind, subject_id, version_id),
    CHECK (kind = 'skill' OR version_id IS NULL)
);

CREATE FUNCTION public.registry_projection_mark(p_kind text, p_subject uuid, p_priority smallint) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    current_mode text;
    next_revision bigint;
BEGIN
    SELECT mode INTO current_mode FROM public.registry_projection_settings WHERE singleton FOR SHARE;
    IF current_mode IS NULL OR current_mode = 'local' THEN
        RETURN;
    END IF;
    INSERT INTO public.registry_projection_revisions AS r (kind, subject_id, revision) VALUES (p_kind, p_subject, 1)
    ON CONFLICT (kind, subject_id) DO UPDATE SET revision = r.revision + 1
    RETURNING revision INTO next_revision;
    INSERT INTO public.registry_projection_dirty AS d (kind, subject_id, revision, priority, marked_at)
    VALUES (p_kind, p_subject, next_revision, p_priority, clock_timestamp())
    ON CONFLICT (kind, subject_id) DO UPDATE SET revision = EXCLUDED.revision,
        priority = LEAST(d.priority, EXCLUDED.priority), marked_at = EXCLUDED.marked_at, next_attempt_at = NULL, suspended = false;
END;
$$;

CREATE FUNCTION public.registry_projection_lock_owner(p_owner uuid) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    current_mode text;
BEGIN
    SELECT mode INTO current_mode FROM public.registry_projection_settings WHERE singleton FOR SHARE;
    IF current_mode IS NULL OR current_mode = 'local' THEN
        RETURN;
    END IF;
    PERFORM 1 FROM public.users WHERE id = p_owner FOR SHARE;
END;
$$;

CREATE FUNCTION public.registry_projection_skill_packages() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND (NEW.visibility, NEW.owner_user_id) IS NOT DISTINCT FROM (OLD.visibility, OLD.owner_user_id) THEN
        RETURN NULL;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        PERFORM public.registry_projection_lock_owner(NEW.owner_user_id);
    END IF;
    IF TG_OP = 'DELETE' THEN
        PERFORM public.registry_projection_mark('skill', OLD.id, 0::smallint);
    ELSIF TG_OP = 'INSERT' THEN
        PERFORM public.registry_projection_mark('skill', NEW.id, 1::smallint);
    ELSE
        PERFORM public.registry_projection_mark('skill', NEW.id,
            CASE WHEN NEW.visibility = 'private' OR NEW.owner_user_id <> OLD.owner_user_id THEN 0 ELSE 1 END::smallint);
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER registry_projection_skill_packages_trg AFTER INSERT OR UPDATE OR DELETE ON public.skill_packages
    FOR EACH ROW EXECUTE FUNCTION public.registry_projection_skill_packages();

CREATE FUNCTION public.registry_projection_skill_versions() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.published_at IS NULL THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF OLD.published_at IS NOT NULL THEN
            PERFORM public.registry_projection_mark('skill', OLD.package_id, 0::smallint);
        END IF;
    ELSE
        PERFORM public.registry_projection_mark('skill', NEW.package_id,
            CASE WHEN NEW.published_at IS NULL THEN 0 ELSE 1 END::smallint);
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER registry_projection_skill_versions_trg AFTER INSERT OR UPDATE OR DELETE ON public.skill_package_versions
    FOR EACH ROW EXECUTE FUNCTION public.registry_projection_skill_versions();

CREATE FUNCTION public.registry_projection_users() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target uuid;
    restrictive smallint;
BEGIN
    IF TG_OP = 'UPDATE' AND (NEW.disabled_at, NEW.deleted_at) IS NOT DISTINCT FROM (OLD.disabled_at, OLD.deleted_at) THEN
        RETURN NULL;
    END IF;
    PERFORM 1 FROM public.registry_projection_settings WHERE singleton FOR SHARE;
    IF TG_OP = 'DELETE' THEN
        target := OLD.id;
        restrictive := 0;
    ELSE
        target := NEW.id;
        restrictive := CASE WHEN NEW.disabled_at IS NOT NULL OR NEW.deleted_at IS NOT NULL THEN 0 ELSE 1 END;
    END IF;
    PERFORM public.registry_projection_mark('skill', p.id, restrictive) FROM public.skill_packages p WHERE p.owner_user_id = target;
    PERFORM public.registry_projection_mark('mcp', a.id, restrictive) FROM public.agents a WHERE a.creator_id = target AND a.connection_mode = 'mcp_server';
    RETURN NULL;
END;
$$;
CREATE TRIGGER registry_projection_users_trg AFTER UPDATE OR DELETE ON public.users
    FOR EACH ROW EXECUTE FUNCTION public.registry_projection_users();

CREATE FUNCTION public.registry_projection_agents() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.connection_mode = 'mcp_server' THEN
            PERFORM public.registry_projection_lock_owner(NEW.creator_id);
            PERFORM public.registry_projection_mark('mcp', NEW.id, 1::smallint);
        END IF;
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF OLD.connection_mode = 'mcp_server' THEN
            PERFORM public.registry_projection_mark('mcp', OLD.id, 0::smallint);
        END IF;
        RETURN NULL;
    END IF;
    IF NEW.connection_mode <> 'mcp_server' AND OLD.connection_mode <> 'mcp_server' THEN
        RETURN NULL;
    END IF;
    IF (NEW.visibility, NEW.lifecycle_status, NEW.connection_mode, NEW.tags, NEW.name, NEW.description, NEW.slug, NEW.mcp_tool_name, NEW.creator_id)
       IS NOT DISTINCT FROM
       (OLD.visibility, OLD.lifecycle_status, OLD.connection_mode, OLD.tags, OLD.name, OLD.description, OLD.slug, OLD.mcp_tool_name, OLD.creator_id) THEN
        RETURN NULL;
    END IF;
    PERFORM public.registry_projection_lock_owner(NEW.creator_id);
    PERFORM public.registry_projection_mark('mcp', NEW.id,
        CASE WHEN NEW.visibility <> 'public' OR NEW.lifecycle_status <> 'active' OR NEW.connection_mode <> 'mcp_server'
                  OR NEW.tags IS DISTINCT FROM OLD.tags OR NEW.creator_id <> OLD.creator_id THEN 0 ELSE 1 END::smallint);
    RETURN NULL;
END;
$$;
CREATE TRIGGER registry_projection_agents_trg AFTER INSERT OR UPDATE OR DELETE ON public.agents
    FOR EACH ROW EXECUTE FUNCTION public.registry_projection_agents();

CREATE FUNCTION public.registry_projection_mcp_metadata() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND (NEW.metadata, NEW.revision) IS NOT DISTINCT FROM (OLD.metadata, OLD.revision) THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN
        PERFORM public.registry_projection_mark('mcp', OLD.agent_id, 1::smallint);
    ELSE
        PERFORM public.registry_projection_mark('mcp', NEW.agent_id, 1::smallint);
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER registry_projection_mcp_metadata_trg AFTER INSERT OR UPDATE OR DELETE ON public.mcp_service_metadata
    FOR EACH ROW EXECUTE FUNCTION public.registry_projection_mcp_metadata();

COMMIT;
