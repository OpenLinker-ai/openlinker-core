-- This rollback intentionally fails while local HTTP demo agents still exist.
ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_endpoint_https,
    ADD CONSTRAINT agents_endpoint_https CHECK (endpoint_url LIKE 'https://%');
