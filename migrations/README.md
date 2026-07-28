# Core Current-Schema Initialization

`086_current_schema_init.up.sql` initializes the foundational Core schema and
canonical seed data for a fresh database. The migration runner then applies
`087_browser_agent_execution_profile.up.sql` and
`088_browser_human_control.up.sql`. The only supported installed-schema
upgrade is from the exact clean version `087` predecessor to version `088`.
`086_current_schema_init_verify.sql` is the PostgreSQL 16 current-catalog and
seed fingerprint used after the complete migration chain. Fresh and predecessor
paths therefore execute the same `088` DDL and converge on one version `088`
catalog fingerprint without an idempotent duplicate schema definition.

The migration command accepts only a truly empty database, the exact clean
version `087` predecessor, or the exact clean version `088` current schema.
Exactness is enforced with catalog object counts and a SHA-256 fingerprint over
table, column/default, constraint, index, trigger, and function definitions.
Legacy, dirty, partial, or malformed databases are rejected before the
migration driver is created. `api migrate check` reports `fresh`,
`upgradeable`, or `current` without mutation. There is no down migration;
recreate disposable databases instead.

Version `087` also fails postflight while any Agent with historical
`browser_execution_profile.v1` Runtime Sessions is missing a reviewed durable
profile row. Follow `docs/58-browser-agent-execution-profile-runbook.md` before
enabling the new Core read path.
