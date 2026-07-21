# Core Current-Schema Initialization

`086_current_schema_init.up.sql` initializes the complete current Core schema
and canonical seed data. `086_current_schema_init_verify.sql` is the
PostgreSQL 16 catalog and seed fingerprint used by integration checks.

The migration command accepts only a truly empty database or an exact, clean
version `086` schema. Exactness is enforced with catalog object counts and a
SHA-256 fingerprint over table, column/default, constraint, index, trigger,
and function definitions. Legacy, dirty, partial, or malformed databases are
rejected before the migration driver is created. `api migrate check` performs
the same validation without mutation. There is no down migration; recreate
disposable databases instead.

The next Core schema change starts at version `087` and may introduce an
ordinary forward migration when upgrade support is required.
