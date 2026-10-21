# Owner-selected MCP tools

Core owns the private binding, immutable Run snapshot and live per-call grant.
Endpoint credentials are never included in the tool snapshot. Selection preview,
save, Run admission and delegated execution all use the same keyed configuration
digest: HMAC-SHA256, domain `openlinker/mcp-binding/config/v1`, target Agent ID and
the execution-relevant configuration. This prevents exposed digests from becoming
offline verifiers for guessed credentials or private endpoint URLs.

The key is the existing stable Runtime master secret (`RUNTIME_PKI_MASTER_SECRET`,
with `JWT_SECRET` as the existing fallback). All Core instances must use the same
effective secret. No new secret or consumer-side key is introduced. An absent
secret fails closed. Changing this effective secret invalidates bindings as well
as the other Runtime material derived from it; owners must review and save them
again. Pre-hardening bindings containing plain SHA-256 digests also require review
and save after deployment; old snapshots are never automatically reauthorized.

Changing the tool endpoint, credential, schema or call policy invalidates its
grant. The owner settings list reports `needs_review`; the owner may review/save
or remove the binding. Run admission fails closed while any selected grant is
invalid, instead of silently omitting a tool the Agent depends on. Callers receive
a generic unavailable error so the response does not disclose private bindings.
Already executing operations may complete; new calls from stale snapshots fail.

The owner view and Run admission share the same Host compatibility lookup: current
non-closed/non-revoked Sessions, or the last closed Session if none remain. Unknown
and last-known compatible offline Hosts retain offline queuing. A known incompatible
Host blocks admission with the same generic unavailable response. `host_connected`
is separate from compatibility, so the settings page can explain an offline Host
that also requires an upgrade.

`ready` and `used` receipts are evidence reported by the authorized Runtime Worker;
they are not an independent attestation of model intent. `failed` means the tools
did not become ready for that Run. A `provider_ended_before_discovery` stage preserves
the Provider's own authentication/network/startup error rather than relabeling it.
Callers may see granted tool names, descriptions, results and child-Run endpoint
host/port metadata. Associations remain owner-only; that does not make tool outputs
secret from callers. Request keys deduplicate repeated calls within the existing
Attempt/child-Run contract; side-effecting services still need business-level
idempotency across new Attempts and new model-generated keys.
