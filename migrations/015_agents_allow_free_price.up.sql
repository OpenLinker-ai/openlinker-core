-- Phase 1 only records future pricing; free Agents must be valid.
ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_price_positive,
    ADD CONSTRAINT agents_price_nonnegative
        CHECK (price_per_call_cents >= 0 AND price_per_call_cents <= 1000000);
