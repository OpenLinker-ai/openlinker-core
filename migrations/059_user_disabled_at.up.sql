-- 059_user_disabled_at.up.sql
-- Platform admins can disable user accounts without deleting historical data.

BEGIN;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_users_disabled
    ON users (disabled_at)
    WHERE disabled_at IS NOT NULL AND deleted_at IS NULL;

COMMIT;
