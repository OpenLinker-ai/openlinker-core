BEGIN;

CREATE TABLE oauth_login_codes (
    code_hash TEXT PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    jwt TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT oauth_login_codes_hash_len CHECK (char_length(code_hash) = 64),
    CONSTRAINT oauth_login_codes_jwt_nonempty CHECK (char_length(jwt) > 0)
);

CREATE INDEX idx_oauth_login_codes_expires_at
    ON oauth_login_codes (expires_at);

COMMIT;
