-- 001_init.down.sql

BEGIN;

DROP TRIGGER IF EXISTS users_set_updated_at ON users;
DROP TRIGGER IF EXISTS agents_set_updated_at ON agents;
DROP FUNCTION IF EXISTS trigger_set_updated_at();

DROP TABLE IF EXISTS runs;
DROP TABLE IF EXISTS agents;
DROP TABLE IF EXISTS users;

COMMIT;
