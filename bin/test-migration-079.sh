#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS_DIR="$ROOT_DIR/migrations"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
CONTAINER_NAME="openlinker-migration-079-${PPID}-$$"
DATABASE_NAME="openlinker"

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

fail() {
  echo "migration 079 test failed: $*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
docker info >/dev/null 2>&1 || fail "docker daemon is not available"

docker run --detach --name "$CONTAINER_NAME" \
  --env POSTGRES_HOST_AUTH_METHOD=trust \
  --env POSTGRES_DB="$DATABASE_NAME" \
  --volume "$MIGRATIONS_DIR:/migrations:ro" \
  "$POSTGRES_IMAGE" >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$CONTAINER_NAME" pg_isready -U postgres -d "$DATABASE_NAME" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$CONTAINER_NAME" pg_isready -U postgres -d "$DATABASE_NAME" >/dev/null 2>&1 \
  || fail "postgres did not become ready"

psql_stdin() {
  docker exec -i --env PGOPTIONS="-c client_min_messages=warning" "$CONTAINER_NAME" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d "$DATABASE_NAME" "$@"
}

run_migration() {
  psql_stdin --quiet <"$1"
}

expect_up_failure() {
  local expected="$1"
  local output status
  set +e
  output="$(run_migration "$MIGRATIONS_DIR/079_runtime_attempt_transport_evidence.up.sql" 2>&1)"
  status=$?
  set -e
  if ((status == 0)); then
    fail "079 upgrade unexpectedly succeeded; expected: $expected"
  fi
  [[ "$output" == *"$expected"* ]] \
    || fail "079 upgrade failed for the wrong reason; expected '$expected', got: $output"
}

echo "[079] apply predecessor migrations through 078"
for migration_path in "$MIGRATIONS_DIR"/[0-9][0-9][0-9]_*.up.sql; do
  migration_name="$(basename "$migration_path")"
  version="${migration_name%%_*}"
  if ((10#$version <= 78)); then
    run_migration "$migration_path" >/dev/null
  fi
done

echo "[079] fail closed outside hard maintenance"
psql_stdin --quiet <<'SQL'
UPDATE runtime_cluster_control
SET mode = 'normal'
WHERE singleton_id = 1;
SQL
expect_up_failure "requires hard maintenance"

psql_stdin --quiet <<'SQL'
UPDATE runtime_cluster_control
SET mode = 'hard_maintenance'
WHERE singleton_id = 1;
SQL

echo "[079] upgrade, verify, clean rollback, and re-upgrade"
run_migration "$MIGRATIONS_DIR/079_runtime_attempt_transport_evidence.up.sql" >/dev/null
run_migration "$MIGRATIONS_DIR/079_runtime_attempt_transport_evidence_verify.sql" >/dev/null
run_migration "$MIGRATIONS_DIR/079_runtime_attempt_transport_evidence.down.sql" >/dev/null
run_migration "$MIGRATIONS_DIR/079_runtime_attempt_transport_evidence.up.sql" >/dev/null
run_migration "$MIGRATIONS_DIR/079_runtime_attempt_transport_evidence_verify.sql" >/dev/null

echo "migration 079 test passed"
