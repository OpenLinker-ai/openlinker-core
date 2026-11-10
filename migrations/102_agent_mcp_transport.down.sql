BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Dropping either column would silently change how an Agent is called: a
-- streamable_http or legacy_sse Agent would fall back to one JSON POST, and a
-- custom header name would fall back to Bearer / X-OpenLinker-Token routing.
-- Refuse instead of losing that configuration.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM public.agents WHERE mcp_transport <> 'legacy_json') THEN
  RAISE EXCEPTION '102_agent_mcp_transport rollback refused: Agents use a non-default mcp_transport';
 END IF;
 IF EXISTS (SELECT 1 FROM public.agents WHERE endpoint_auth_header_name IS NOT NULL) THEN
  RAISE EXCEPTION '102_agent_mcp_transport rollback refused: Agents use a custom endpoint_auth_header_name';
 END IF;
END $$;

ALTER TABLE public.agents
    DROP CONSTRAINT agents_endpoint_auth_header_name_token,
    DROP CONSTRAINT agents_mcp_transport_valid,
    DROP COLUMN endpoint_auth_header_name,
    DROP COLUMN mcp_transport;

COMMIT;
