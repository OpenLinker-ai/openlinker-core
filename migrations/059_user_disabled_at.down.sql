-- 059_user_disabled_at.down.sql

BEGIN;

DROP INDEX IF EXISTS idx_users_disabled;

ALTER TABLE users
    DROP COLUMN IF EXISTS disabled_at;

COMMIT;
