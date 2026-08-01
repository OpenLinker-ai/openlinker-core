BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.users
    ADD COLUMN token_version bigint DEFAULT 0 NOT NULL;

ALTER TABLE public.users
    ADD CONSTRAINT users_token_version_nonnegative
    CHECK (token_version >= 0) NOT VALID;

ALTER TABLE public.users
    VALIDATE CONSTRAINT users_token_version_nonnegative;

COMMIT;
