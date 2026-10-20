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
