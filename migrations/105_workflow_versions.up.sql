BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Phase B of the workflow pipeline completion: immutable workflow versions.
-- Every workflow gets version 1 from its current definition, keeping the
-- node UUIDs, configs and edges byte for byte (so the execution contract
-- hash of an unedited workflow does not change). Steps, launches and logical
-- nodes switch their node foreign keys from the editable workflow_nodes
-- table to the immutable version nodes; workflow_nodes stays as the
-- projection of each workflow's current version. Runs pin the version they
-- execute. Like 104, the release drains workflow runs first.
LOCK TABLE public.workflows, public.workflow_nodes, public.workflow_runs,
    public.workflow_run_steps, public.workflow_step_launches, public.workflow_run_nodes
    IN SHARE ROW EXCLUSIVE MODE;

DO $$ BEGIN
 IF EXISTS (
  SELECT 1 FROM public.workflow_runs WHERE status IN ('pending', 'running', 'paused')
 ) THEN
  RAISE EXCEPTION '105_workflow_versions refused: drain workflow runs first (pending, running or paused runs exist)';
 END IF;
END $$;

-- semantics v1 interprets definitions exactly as before (extra keys such as
-- node policies are ignored); only v2 definitions interpret them, and only
-- v2 may carry an output mapping.
CREATE TABLE public.workflow_versions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    workflow_id uuid NOT NULL,
    version integer NOT NULL,
    semantics text DEFAULT 'v1'::text NOT NULL,
    name text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    edges jsonb DEFAULT '[]'::jsonb NOT NULL,
    output_mapping jsonb,
    definition_digest text NOT NULL,
    created_by uuid,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT workflow_versions_pkey PRIMARY KEY (id),
    CONSTRAINT workflow_versions_workflow_version_key UNIQUE (workflow_id, version),
    CONSTRAINT workflow_versions_workflow_id_id_key UNIQUE (workflow_id, id),
    CONSTRAINT workflow_versions_version_positive CHECK (version > 0),
    CONSTRAINT workflow_versions_semantics_valid CHECK (semantics = ANY (ARRAY['v1'::text, 'v2'::text])),
    CONSTRAINT workflow_versions_output_mapping_v2 CHECK (semantics = 'v2'::text OR output_mapping IS NULL),
    CONSTRAINT workflow_versions_edges_array CHECK (jsonb_typeof(edges) = 'array'::text),
    CONSTRAINT workflow_versions_name_len CHECK (char_length(name) >= 1 AND char_length(name) <= 120),
    CONSTRAINT workflow_versions_digest_len CHECK (char_length(definition_digest) = 64),
    CONSTRAINT workflow_versions_workflow_id_fkey FOREIGN KEY (workflow_id) REFERENCES public.workflows(id) ON DELETE CASCADE
);

CREATE TABLE public.workflow_version_nodes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    workflow_version_id uuid NOT NULL,
    node_key text NOT NULL,
    node_type text DEFAULT 'agent'::text NOT NULL,
    agent_id uuid NOT NULL,
    title text NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    "position" integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT workflow_version_nodes_pkey PRIMARY KEY (id),
    CONSTRAINT workflow_version_nodes_version_key UNIQUE (workflow_version_id, node_key),
    CONSTRAINT workflow_version_nodes_key_len CHECK (char_length(node_key) >= 1 AND char_length(node_key) <= 80),
    CONSTRAINT workflow_version_nodes_position_nonnegative CHECK ("position" >= 0),
    CONSTRAINT workflow_version_nodes_title_len CHECK (char_length(title) >= 1 AND char_length(title) <= 160),
    CONSTRAINT workflow_version_nodes_type_valid CHECK (node_type = 'agent'::text),
    CONSTRAINT workflow_version_nodes_version_fkey FOREIGN KEY (workflow_version_id) REFERENCES public.workflow_versions(id) ON DELETE CASCADE,
    CONSTRAINT workflow_version_nodes_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES public.agents(id) ON DELETE RESTRICT
);

-- 1. Version 1 of every workflow, node rows keep their ids and timestamps.
INSERT INTO public.workflow_versions (workflow_id, version, semantics, name, description, edges, definition_digest, created_by, created_at)
SELECT w.id, 1, 'v1', w.name, w.description, w.edges, repeat('0', 64), w.user_id, w.created_at
FROM public.workflows w;

INSERT INTO public.workflow_version_nodes (id, workflow_version_id, node_key, node_type, agent_id, title, config, "position", created_at)
SELECT n.id, v.id, n.node_key, n.node_type, n.agent_id, n.title, n.config, n."position", n.created_at
FROM public.workflow_nodes n
JOIN public.workflow_versions v ON v.workflow_id = n.workflow_id AND v.version = 1;

-- The definition digest identifies a version's definition (semantics,
-- edges, output mapping and nodes ordered by key). Core computes it with the
-- same statement when it creates a version.
UPDATE public.workflow_versions v
SET definition_digest = encode(sha256(convert_to(jsonb_build_object(
        'semantics', v.semantics,
        'edges', v.edges,
        'output_mapping', v.output_mapping,
        'nodes', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'node_key', vn.node_key, 'node_type', vn.node_type, 'agent_id', vn.agent_id,
                'title', vn.title, 'config', vn.config, 'position', vn."position"
            ) ORDER BY vn.node_key)
            FROM public.workflow_version_nodes vn WHERE vn.workflow_version_id = v.id
        ), '[]'::jsonb)
    )::text, 'UTF8')), 'hex');

-- 2. Every projection node must now exist, unchanged, as a version node.
DO $$ BEGIN
 IF EXISTS (
  SELECT 1 FROM public.workflow_nodes n
  LEFT JOIN public.workflow_version_nodes vn ON vn.id = n.id
  WHERE vn.id IS NULL
     OR vn.node_key <> n.node_key OR vn.agent_id <> n.agent_id OR vn.title <> n.title
     OR vn.config <> n.config OR vn."position" <> n."position" OR vn.node_type <> n.node_type
 ) OR (SELECT count(*) FROM public.workflow_nodes) <> (SELECT count(*) FROM public.workflow_version_nodes) THEN
  RAISE EXCEPTION '105_workflow_versions refused: version 1 does not match the current workflow nodes';
 END IF;
END $$;

-- 3. Execution history now references immutable version nodes. Each switch
-- adds the new constraint NOT VALID, validates it, then drops the old one,
-- all in this transaction: there is no window without a constraint.
ALTER TABLE public.workflow_run_steps
    ADD CONSTRAINT workflow_run_steps_workflow_version_node_id_fkey FOREIGN KEY (workflow_node_id)
        REFERENCES public.workflow_version_nodes(id) NOT VALID;
ALTER TABLE public.workflow_run_steps VALIDATE CONSTRAINT workflow_run_steps_workflow_version_node_id_fkey;
ALTER TABLE public.workflow_run_steps DROP CONSTRAINT workflow_run_steps_workflow_node_id_fkey;

ALTER TABLE public.workflow_step_launches
    ADD CONSTRAINT workflow_step_launches_workflow_version_node_id_fkey FOREIGN KEY (workflow_node_id)
        REFERENCES public.workflow_version_nodes(id) NOT VALID;
ALTER TABLE public.workflow_step_launches VALIDATE CONSTRAINT workflow_step_launches_workflow_version_node_id_fkey;
ALTER TABLE public.workflow_step_launches DROP CONSTRAINT workflow_step_launches_workflow_node_id_fkey;

ALTER TABLE public.workflow_run_nodes
    ADD CONSTRAINT workflow_run_nodes_workflow_version_node_id_fkey FOREIGN KEY (workflow_node_id)
        REFERENCES public.workflow_version_nodes(id) NOT VALID;
ALTER TABLE public.workflow_run_nodes VALIDATE CONSTRAINT workflow_run_nodes_workflow_version_node_id_fkey;
ALTER TABLE public.workflow_run_nodes DROP CONSTRAINT workflow_run_nodes_workflow_node_id_fkey;

-- 4. Current version of each workflow and the version each run executes.
ALTER TABLE public.workflows ADD COLUMN current_version_id uuid;
UPDATE public.workflows w SET current_version_id = v.id
FROM public.workflow_versions v WHERE v.workflow_id = w.id AND v.version = 1;
ALTER TABLE public.workflows
    ADD CONSTRAINT workflows_current_version_fkey FOREIGN KEY (id, current_version_id)
        REFERENCES public.workflow_versions(workflow_id, id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.workflow_runs ADD COLUMN workflow_version_id uuid;
UPDATE public.workflow_runs r SET workflow_version_id = v.id
FROM public.workflow_versions v WHERE v.workflow_id = r.workflow_id AND v.version = 1;
ALTER TABLE public.workflow_runs ALTER COLUMN workflow_version_id SET NOT NULL;
ALTER TABLE public.workflow_runs
    ADD CONSTRAINT workflow_runs_workflow_version_fkey FOREIGN KEY (workflow_id, workflow_version_id)
        REFERENCES public.workflow_versions(workflow_id, id);

CREATE INDEX idx_workflow_runs_version ON public.workflow_runs USING btree (workflow_version_id);

COMMIT;
