BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Only hashed exchange secrets are durable. Browser sessions never receive a
-- User Token; issuance and single-use consumption happen in one transaction.
CREATE TABLE public.cli_login_requests (
    device_hash text PRIMARY KEY,
    user_code_hash text NOT NULL UNIQUE,
    challenge text NOT NULL,
    redirect_uri text NOT NULL DEFAULT '',
    state text NOT NULL DEFAULT '',
    scopes text[] NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'denied', 'consumed')),
    user_id uuid REFERENCES public.users(id) ON DELETE CASCADE,
    user_version bigint,
    code_hash text UNIQUE,
    expires_at timestamptz NOT NULL,
    last_poll_at timestamptz,
    poll_interval integer NOT NULL DEFAULT 5,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX cli_login_requests_expiry ON public.cli_login_requests(expires_at);

-- Bounded fixed-window counters shared by every Core replica.
CREATE TABLE public.cli_login_rate_limits (
    key text PRIMARY KEY,
    hits integer NOT NULL,
    expires_at timestamptz NOT NULL
);
COMMIT;
