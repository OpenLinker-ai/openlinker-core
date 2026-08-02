# Core Current-Schema Initialization

`086_current_schema_init.up.sql` initializes the foundational Core schema and
canonical seed data for a fresh database. The migration runner then applies
`087_browser_agent_execution_profile.up.sql`,
`088_browser_human_control.up.sql`, `089_user_jwt_token_version.up.sql`, and
`090_task_callback_owner_index.up.sql`. The only supported installed-schema
upgrade is from the exact clean version `088` predecessor to version `090`.
`086_current_schema_init_verify.sql` is the PostgreSQL 16 current-catalog and
seed fingerprint used after the complete migration chain. Fresh and predecessor
paths therefore execute the same `089`/`090` DDL and converge on one version `090`
catalog fingerprint without an idempotent duplicate schema definition.

The migration command accepts only a truly empty database, the exact clean
version `088` predecessor, or the exact clean version `090` current schema.
Exactness is enforced with catalog object counts and a SHA-256 fingerprint over
table, column/default, constraint, index, trigger, and function definitions.
Legacy, dirty, partial, or malformed databases are rejected before the
migration driver is created. `api migrate check` reports `fresh`,
`upgradeable`, or `current` without mutation. There is no down migration;
recreate disposable databases instead.

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
