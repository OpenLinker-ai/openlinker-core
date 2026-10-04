# Core Current-Schema Initialization

`086_current_schema_init.up.sql` initializes the foundational Core schema and
canonical seed data for a fresh database. The migration runner then applies
`087_browser_agent_execution_profile.up.sql`,
`088_browser_human_control.up.sql`, `089_user_jwt_token_version.up.sql`,
`090_task_callback_owner_index.up.sql`,
`091_browser_interaction_policy.up.sql`,
`092_browser_observation_audit.up.sql`, and
`093_skill_packages.up.sql` and `094_cli_login.up.sql`. Supported exact clean predecessors are `093`, `092`,
`091`, `088` and `086`; the current version is `094`.
`086_current_schema_init_verify.sql` is the PostgreSQL 16 current-catalog and
seed fingerprint used after the complete migration chain. Fresh and predecessor
paths therefore execute the same `094` DDL and converge on one version `094`
catalog fingerprint without an idempotent duplicate schema definition.

Migration `093` adds private skill packages, immutable versions, Agent bindings
and per-Run references to pinned versions. Payloads are joined only for authenticated
assignment delivery; foreign keys prevent deleting referenced versions, with a
`(package_id, version_id)` snapshot index for those reference checks. This
source migration has not been released or deployed.

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
supported predecessor (`093`, `092`, `091`, `088` or `086`), or the exact clean version `094`
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
User Token plaintext is returned only once at redemption. The PostgreSQL 16
current shape is 81 tables, 670 constraints, 286 indexes and 70 triggers, with
digest `8f0c9af06f21b01ce80e36b817faa83dea4cd416d37037f5d366714e65332438`.
The exact `093` predecessor retains its prior fingerprint.
