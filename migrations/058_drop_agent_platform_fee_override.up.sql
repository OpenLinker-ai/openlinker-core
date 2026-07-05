BEGIN;

ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_platform_fee_rate_override_valid,
    DROP COLUMN IF EXISTS platform_fee_rate_override;

COMMIT;
