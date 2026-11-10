BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Existing Agents keep the single stateless JSON-RPC POST (legacy_json) and
-- the current credential routing (no header name). The new transports and a
-- custom credential header name are explicit owner choices.
ALTER TABLE public.agents
    ADD COLUMN mcp_transport text DEFAULT 'legacy_json'::text NOT NULL,
    ADD COLUMN endpoint_auth_header_name text;

ALTER TABLE public.agents
    ADD CONSTRAINT agents_mcp_transport_valid
    CHECK (mcp_transport = ANY (ARRAY['legacy_json'::text, 'streamable_http'::text, 'legacy_sse'::text])) NOT VALID;

ALTER TABLE public.agents
    VALIDATE CONSTRAINT agents_mcp_transport_valid;

-- RFC 9110 token syntax. Reserved names are rejected by Core before storage.
ALTER TABLE public.agents
    ADD CONSTRAINT agents_endpoint_auth_header_name_token
    CHECK (endpoint_auth_header_name IS NULL OR endpoint_auth_header_name ~ '^[!#$%&''*+.^_`|~0-9A-Za-z-]{1,128}$') NOT VALID;

ALTER TABLE public.agents
    VALIDATE CONSTRAINT agents_endpoint_auth_header_name_token;

COMMIT;
