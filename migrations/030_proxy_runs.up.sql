BEGIN;

DROP INDEX IF EXISTS idx_cloud_listing_links_cloud_listing;

ALTER TABLE cloud_listing_links
    ADD CONSTRAINT cloud_listing_links_cloud_listing_unique UNIQUE (cloud_listing_id);

CREATE TABLE proxy_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cloud_run_id UUID NOT NULL DEFAULT gen_random_uuid(),
    cloud_listing_link_id UUID NOT NULL REFERENCES cloud_listing_links(id) ON DELETE CASCADE,
    cloud_listing_id UUID NOT NULL REFERENCES cloud_listing_links(cloud_listing_id) ON DELETE CASCADE,
    registry_node_id UUID NOT NULL REFERENCES registry_nodes(id) ON DELETE CASCADE,
    local_agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    requesting_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'claimed', 'success', 'failed', 'timeout')),
    payload_policy TEXT NOT NULL DEFAULT 'metadata_only'
        CHECK (payload_policy IN ('metadata_only', 'store_run_summary', 'store_full_payload')),
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_summary TEXT,
    output JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_summary TEXT,
    error_code TEXT,
    error_message TEXT,
    claimed_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT proxy_runs_idempotency_unique UNIQUE (registry_node_id, idempotency_key),
    CONSTRAINT proxy_runs_idempotency_key_len CHECK (char_length(idempotency_key) BETWEEN 8 AND 160),
    CONSTRAINT proxy_runs_input_summary_len CHECK (input_summary IS NULL OR char_length(input_summary) <= 500),
    CONSTRAINT proxy_runs_output_summary_len CHECK (output_summary IS NULL OR char_length(output_summary) <= 1000),
    CONSTRAINT proxy_runs_error_code_len CHECK (error_code IS NULL OR char_length(error_code) <= 80),
    CONSTRAINT proxy_runs_error_message_len CHECK (error_message IS NULL OR char_length(error_message) <= 1000),
    CONSTRAINT proxy_runs_claimed_at_status CHECK (
        (status = 'pending' AND claimed_at IS NULL)
        OR (status <> 'pending' AND claimed_at IS NOT NULL)
    ),
    CONSTRAINT proxy_runs_finished_at_status CHECK (
        (status IN ('success', 'failed', 'timeout') AND finished_at IS NOT NULL)
        OR (status IN ('pending', 'claimed') AND finished_at IS NULL)
    )
);

CREATE UNIQUE INDEX idx_proxy_runs_cloud_run
    ON proxy_runs (cloud_run_id);

CREATE INDEX idx_proxy_runs_node_pending
    ON proxy_runs (registry_node_id, status, created_at);

CREATE INDEX idx_proxy_runs_requester
    ON proxy_runs (requesting_user_id, created_at DESC);

CREATE INDEX idx_proxy_runs_listing
    ON proxy_runs (cloud_listing_id, created_at DESC);

CREATE TRIGGER proxy_runs_set_updated_at BEFORE UPDATE ON proxy_runs
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

COMMIT;
