BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
CREATE TABLE public.agent_mcp_bindings (
    agent_id uuid NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    target_agent_id uuid NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    binding_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    owner_user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    digest text NOT NULL CHECK (digest ~ '^[a-f0-9]{64}$'),
    tool jsonb NOT NULL CHECK (jsonb_typeof(tool) = 'object'),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','ready','used','failed')),
    last_run_id uuid REFERENCES public.runs(id) ON DELETE SET NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (agent_id,target_agent_id),
    CHECK (agent_id <> target_agent_id)
);
CREATE INDEX agent_mcp_bindings_target_idx ON public.agent_mcp_bindings(target_agent_id);
CREATE INDEX agent_mcp_bindings_owner_idx ON public.agent_mcp_bindings(owner_user_id);
CREATE INDEX agent_mcp_bindings_run_idx ON public.agent_mcp_bindings(last_run_id);
CREATE TABLE public.run_mcp_binding_snapshots (
    run_id uuid NOT NULL REFERENCES public.runs(id) ON DELETE CASCADE,
    binding_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    target_agent_id uuid NOT NULL,
    owner_user_id uuid NOT NULL,
    digest text NOT NULL,
    tool jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id,binding_id)
);
CREATE INDEX run_mcp_binding_snapshots_binding_idx ON public.run_mcp_binding_snapshots(binding_id);
CREATE TABLE public.run_mcp_tool_calls (
    run_id uuid PRIMARY KEY REFERENCES public.runs(id) ON DELETE CASCADE,
    parent_run_id uuid NOT NULL,
    parent_attempt_id uuid NOT NULL,
    binding_id uuid NOT NULL,
    FOREIGN KEY(parent_run_id,binding_id) REFERENCES public.run_mcp_binding_snapshots(run_id,binding_id) ON DELETE CASCADE
);
CREATE INDEX run_mcp_tool_calls_parent_idx ON public.run_mcp_tool_calls(parent_run_id,binding_id);

COMMIT;
