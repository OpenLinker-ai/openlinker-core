BEGIN;

ALTER TABLE agents
    ADD COLUMN platform_fee_rate_override DOUBLE PRECISION NULL,
    ADD CONSTRAINT agents_platform_fee_rate_override_valid
        CHECK (platform_fee_rate_override IS NULL OR (platform_fee_rate_override >= 0 AND platform_fee_rate_override <= 1));

COMMENT ON COLUMN agents.platform_fee_rate_override IS
    'Optional per-Agent platform fee rate. NULL uses core PLATFORM_FEE_RATE.';

COMMIT;
