# Core Current-Schema Initialization

`086_current_schema_init.up.sql` initializes the foundational Core schema and
canonical seed data for a fresh database. The migration runner then applies
`087_browser_agent_execution_profile.up.sql`,
`088_browser_human_control.up.sql`, `089_user_jwt_token_version.up.sql`,
`090_task_callback_owner_index.up.sql`,
`091_browser_interaction_policy.up.sql`,
`092_browser_observation_audit.up.sql`, and
`093_skill_packages.up.sql`, `094_cli_login.up.sql` and `095_runtime_node_upgrade.up.sql`. Supported exact clean predecessors are `094`, `093`, `092`,
`091`, `088` and `086`; the current version is `095`.
`086_current_schema_init_verify.sql` is the PostgreSQL 16 current-catalog and
seed fingerprint used after the complete migration chain. Fresh and predecessor
paths therefore execute the same `095` DDL and converge on one version `095`
catalog fingerprint without an idempotent duplicate schema definition.

Migration `093` adds private skill packages, immutable versions, Agent bindings
and per-Run references to pinned versions. Payloads are joined only for authenticated
assignment delivery; foreign keys prevent deleting referenced versions, with a
`(package_id, version_id)` snapshot index for those reference checks. This
migration was included in the v0.2.0 baseline.

Migration `091` also adds an Owner-only
`runtime_agent_browser_policy_intents` staging row for a private Runtime Agent
that has not yet been classified by its first Browser Session. The row is not
dispatch authority: the first matching Browser Session consumes it while
creating `runtime_agent_execution_profiles` under the same per-Agent advisory
lock and transaction. Staging marks the Agent Browser-declared, and catalog
constraints keep both declared and classified Browser Agents in Runtime mode;
standard Sessions, publication and connection-mode changes therefore cannot
race past the initial policy decision.

The migration command accepts only a truly empty database, an exact clean
supported predecessor (`094`, `093`, `092`, `091`, `088` or `086`), or the exact clean version `095`
current schema.
Exactness is enforced with catalog object counts and a SHA-256 fingerprint over
table, column/default, constraint, index, trigger, and function definitions.
Legacy, dirty, partial, or malformed databases are rejected before the
migration driver is created. `api migrate check` reports `fresh`,
`upgradeable`, or `current` without mutation. The CLI authorization migration includes an explicit down script for disposable test
databases; the production migration command remains forward-only. Rolling it back
removes pending CLI authorizations and rate counters, but does not revoke already
issued User Tokens. No previous migration is rewritten.

Migration `090` uses `CREATE INDEX CONCURRENTLY`. If PostgreSQL interrupts that
build, it can retain `public.idx_task_callback_subscriptions_owner` with
`pg_index.indisvalid = false`. Do not retry `090` while that relation exists:
the replay will fail with `relation already exists`. On a reviewed maintenance
connection, first confirm that the index is invalid, then run:

```sql
DROP INDEX CONCURRENTLY IF EXISTS public.idx_task_callback_subscriptions_owner;
```

After the invalid relation is gone, use the same pinned `golang-migrate`
version as the deployment to force the dirty migration state back to version
`089`, rerun exactly one migration, and finish with `api migrate check`. Never
force the version or mark the rollout complete while the invalid index still
exists. Both the production migration inspector and the current-schema SQL
verifier require the version-090 index to have `indisvalid = true`.

Version `090` postflight also fails while any Agent with historical
`browser_execution_profile.v1` Runtime Sessions is missing a reviewed durable
profile row. Follow `docs/58-browser-agent-execution-profile-runbook.md` before
enabling the new Core read path.

Migration `094` adds expiring CLI authorization grants and shared rate counters.
It stores hashes of device codes, user codes and browser authorization codes;
User Token plaintext is returned only once at redemption. Exact `094` and `093`
predecessors retain their prior fingerprints.

Migration `095` adds immutable Node upgrade operations, revision state and
successor admissions with database enforcement. It changes Runtime schema
readiness identity and is forward-only. Use coordinated Core maintenance; see
[controlled Node upgrades](../docs/runtime-node-upgrade.md) before upgrading.

After the full chain through `095`, the PostgreSQL 16 current shape is 84 tables,
691 constraints, 290 indexes and 75 triggers, with digest
`f9f50c587f496bcc49be97b8cc76a3efa637563c7b2ca08705cb9e6fff5195ae`.
