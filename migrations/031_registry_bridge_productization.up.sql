BEGIN;

ALTER TABLE registry_nodes
    ALTER COLUMN scopes SET DEFAULT ARRAY['heartbeat', 'listing:sync', 'proxy:pull', 'proxy:result']::text[];

COMMIT;
