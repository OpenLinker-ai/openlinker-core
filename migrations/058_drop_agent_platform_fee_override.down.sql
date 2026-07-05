BEGIN;

ALTER TABLE agents
    ADD COLUMN IF NOT EXISTS platform_fee_rate_override DOUBLE PRECISION NULL,
    ADD CONSTRAINT agents_platform_fee_rate_override_valid
        CHECK (platform_fee_rate_override IS NULL OR (platform_fee_rate_override >= 0 AND platform_fee_rate_override <= 1));

COMMIT;
