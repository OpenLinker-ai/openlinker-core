BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DROP TABLE public.cli_login_rate_limits;
DROP TABLE public.cli_login_requests;
COMMIT;
