UPDATE agents SET price_per_call_cents = 1 WHERE price_per_call_cents = 0;

ALTER TABLE agents
    DROP CONSTRAINT IF EXISTS agents_price_nonnegative,
    ADD CONSTRAINT agents_price_positive
        CHECK (price_per_call_cents > 0 AND price_per_call_cents <= 1000000);
