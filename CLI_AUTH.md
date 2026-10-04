# CLI browser authorization

Core owns `/api/v1/cli-auth/*`, including on Hosted deployments. Hosted sign-in
continues through Cloud; both Web products use the existing browser JWT to
approve a Core-owned authorization. Core has no Cloud, CLI, Node or Plugin
dependency. This is a JSON platform API using PKCE and device authorization
mechanisms, not a general-purpose OAuth authorization server.

Requires migration **094** and `FRONTEND_URL` pointing to the matching Core Web
or Hosted Web containing `/cli/authorize`. Use HTTPS, except local loopback
development. Roll out the Core API and matching Web page before upgrading CLI.
Existing User Tokens and environment-based clients retain their behavior.

| Endpoint | Authentication | Behavior |
| --- | --- | --- |
| `POST /start` | None | Create a ten-minute request with `code_challenge`, `code_challenge_method: S256`, `scopes`, and optional loopback `redirect_uri` plus `state`. |
| `POST /request` | Browser JWT only | Read consent details for `user_code`; never return exchange secrets. |
| `POST /decision` | Browser JWT only | Accept `user_code` and explicit `approve: true/false`; return a code-only browser redirect or device outcome. |
| `POST /token` | One-time grant and PKCE verifier | Exchange `authorization_code` with matching `redirect_uri`, or `urn:ietf:params:oauth:grant-type:device_code`. |
| `GET /session` | User Token | Verify the exact caller token and return user ID, issuer, grants and expiry without plaintext, email or display name. |
| `DELETE /session` | User Token | Revoke only the presented token. No general token-management authority is granted. |

All bodies use JSON and standard Core error envelopes. Start returns
`device_code`, `user_code`, `verification_uri`, `verification_uri_complete`,
`expires_in` and `interval`. Device clients wait the interval before polling;
`AUTHORIZATION_PENDING` continues, `SLOW_DOWN` adds five seconds, and denial or
expiry terminates. Desktop requests cannot be redeemed through device polling.

Desktop callback URLs must use an explicit loopback IP (`127.0.0.1` or `::1`),
an ephemeral port and exact `/callback` path, without userinfo/query/fragment.
PKCE S256 is mandatory for both modes. The browser returns only a one-time code
and the CLI-generated state; the CLI verifies the state and exchanges the code
directly with Core. Browser JWTs and Runtime credentials never reach the CLI.

Allowed permissions are `agents:read`, `agents:run`, `runs:read`, `runs:cancel`
and `tasks:create`. The requested subset is shown before approval. Grants cover
the caller's accessible resources; existing ownership/visibility checks still
apply. Tokens last 30 days, share the existing ten-active-token quota, can be
revoked in User Token settings, and require a new login after expiry. There is
no refresh token or silent permission expansion.
Consent preflights the token quota; exchange rechecks it atomically. A
`TOKEN_QUOTA_EXCEEDED` response asks the user to revoke an old token in website
settings and retry. Approval is not a reservation of quota.

Redemption serializes on the grant row. Consuming the grant and issuing the
normal User Token use one transaction, including the existing per-user quota
lock. Concurrent exchanges mint once; issuance failures leave the grant
unconsumed. Disabled/deleted users and changed browser-session generations are
rechecked at exchange. Device/code secrets are hashed in PostgreSQL; no token
plaintext is stored in the grant table. All responses are `no-store`.

Replica-shared ten-minute rate limits apply to start (10/source IP), exchange
(1800/source IP), and consent inspection/decision (60/account). By default the
login endpoints use the TCP peer and ignore all forwarded headers. Behind a
proxy, explicitly set `CLI_AUTH_TRUSTED_PROXY_CIDRS` to comma-separated CIDRs
covering only the actual proxy peers. The X-Forwarded-For chain is walked from
the direct peer toward the client, stopping at the first untrusted IP; private
and loopback networks are not implicitly trusted. Invalid CIDRs fail startup.
With an empty setting, clients behind one proxy share a rate-limit bucket.
This setting changes only CLI authorization, not other HTTP endpoints.
Pending requests expire after ten minutes, with bounded
cleanup on subsequent starts. CLI does not retry an ambiguous successful
exchange: a lost response may leave a token to revoke in website settings.

Validation: `TEST_DATABASE_URL=<disposable-current-schema> go test -race
./pkg/usertoken -run 'TestCLILogin|TestServiceCreateVerify'`. The database is
mandatory for these integration cases; a skipped run is not acceptance.
