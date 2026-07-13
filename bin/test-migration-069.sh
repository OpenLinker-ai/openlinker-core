#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS_DIR="$ROOT_DIR/migrations"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
CONTAINER_NAME="openlinker-migration-069-${PPID}-$$"
DATABASE_NAME="openlinker"
OLD_DIGEST="857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f"
NEW_DIGEST="fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53"
UNKNOWN_DIGEST="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

fail() {
  echo "migration 069 test failed: $*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
docker info >/dev/null 2>&1 || fail "docker daemon is not available"

for fragment in \
  "UPDATE runtime_session_attachments attachment" \
  "UPDATE runtime_sessions" \
  "UPDATE runtime_nodes" \
  "069_runtime_entry_discovery"; do
  grep -Fq "$fragment" "$MIGRATIONS_DIR/069_runtime_entry_discovery.up.sql" \
    || fail "set-based cutover fragment is missing: $fragment"
done
if grep -Fq "SET status = status" "$MIGRATIONS_DIR/069_runtime_entry_discovery.up.sql"; then
  fail "069 must not enqueue historical rows with a no-op update"
fi

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

psql_command() {
  docker exec --env PGOPTIONS="-c client_min_messages=warning" "$CONTAINER_NAME" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d "$DATABASE_NAME" "$@"
}

reset_database() {
  docker exec "$CONTAINER_NAME" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres --quiet \
      -c "DROP DATABASE IF EXISTS $DATABASE_NAME WITH (FORCE)" \
      -c "CREATE DATABASE $DATABASE_NAME"
}

run_migration() {
  psql_stdin --quiet <"$1"
}

apply_through_068() {
  local migration_path migration_name version
  for migration_path in "$MIGRATIONS_DIR"/[0-9][0-9][0-9]_*.up.sql; do
    migration_name="$(basename "$migration_path")"
    version="${migration_name%%_*}"
    if ((10#$version <= 68)); then
      run_migration "$migration_path" >/dev/null
    fi
  done
}

apply_069() {
  run_migration "$MIGRATIONS_DIR/069_runtime_entry_discovery.up.sql" >/dev/null
}

revert_069() {
  run_migration "$MIGRATIONS_DIR/069_runtime_entry_discovery.down.sql" >/dev/null
}

verify_069() {
  run_migration "$MIGRATIONS_DIR/069_runtime_entry_discovery_verify.sql" >/dev/null
}

expect_apply_failure() {
  local expected="$1"
  local output status
  set +e
  output="$(apply_069 2>&1)"
  status=$?
  set -e
  if ((status == 0)); then
    fail "069 unexpectedly succeeded; expected: $expected"
  fi
  [[ "$output" == *"$expected"* ]] \
    || fail "069 failed for the wrong reason; expected '$expected', got: $output"
}

insert_principals() {
  psql_stdin --quiet <<SQL
INSERT INTO users (id, email, password_hash, display_name, is_creator)
VALUES (
  '69000000-0000-4000-8000-000000000001',
  'migration-069@example.test', 'test-hash', 'Migration 069', TRUE
);

INSERT INTO agents (
  id, creator_id, slug, name, description, endpoint_url,
  price_per_call_cents, connection_mode
) VALUES (
  '69000000-0000-4000-8000-000000000002',
  '69000000-0000-4000-8000-000000000001',
  'migration-069-agent', 'Migration 069 Agent', 'Runtime entry fixture',
  'openlinker-agent-node://migration-069-agent', 0, 'agent_node'
);

INSERT INTO agent_tokens (
  id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
  status, redeemed_at
) VALUES (
  '69000000-0000-4000-8000-000000000003',
  '69000000-0000-4000-8000-000000000002',
  '69000000-0000-4000-8000-000000000001',
  'Migration 069 Token', 'ol_agent_06900000', 'migration-069-token-hash',
  ARRAY['agent:call', 'agent:pull'], 'active_runtime', clock_timestamp()
);

INSERT INTO runtime_nodes (
  node_id, display_name, device_certificate_serial,
  device_public_key_thumbprint, node_version, protocol_version,
  runtime_contract_id, runtime_contract_digest, features, capacity,
  last_seen_at
) VALUES (
  '69000000-0000-4000-8000-000000000004',
  'Migration 069 Node', 'serial-migration-069', 'thumbprint-migration-069',
  '0.2.0-test', 2, 'openlinker.runtime.v2', '$OLD_DIGEST',
  ARRAY[
    'lease_fence', 'assignment_confirm', 'renew', 'resume',
    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
  ],
  4, clock_timestamp()
);
SQL
}

insert_session() {
  local suffix="$1"
  local digest="$2"
  local session_id="69000000-0000-4000-8000-0000000000${suffix}"
  local attachment_id="69000000-0000-4000-8000-0000000001${suffix}"
  psql_stdin --quiet <<SQL
BEGIN;
INSERT INTO runtime_sessions (
  runtime_session_id, node_id, agent_id, credential_id, worker_id,
  session_epoch, device_certificate_serial, node_version,
  protocol_version, runtime_contract_id, runtime_contract_digest,
  features, capacity, attached_core_instance_id
) VALUES (
  '$session_id',
  '69000000-0000-4000-8000-000000000004',
  '69000000-0000-4000-8000-000000000002',
  '69000000-0000-4000-8000-000000000003',
  'worker-$suffix', $((10#$suffix)), 'serial-migration-069', '0.2.0-test', 2,
  'openlinker.runtime.v2', '$digest',
  ARRAY[
    'lease_fence', 'assignment_confirm', 'renew', 'resume',
    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
  ],
  1, '69000000-0000-4000-8000-000000000099'
);
INSERT INTO runtime_session_attachments (
  id, runtime_session_id, core_instance_id, attachment_kind
) VALUES (
  '$attachment_id', '$session_id',
  '69000000-0000-4000-8000-000000000099', 'connected'
);
COMMIT;
SQL
}

assert_cutover() {
  local closed_session="$1"
  local want_schema="$2"
  local want_digest="$3"
  psql_stdin --quiet <<SQL
DO \$\$
BEGIN
  IF (
    SELECT COUNT(*) FROM runtime_schema_contracts
    WHERE schema_version = $want_schema
      AND runtime_contract_digest = '$want_digest'
      AND is_current
  ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
    RAISE EXCEPTION 'unexpected current Runtime schema identity';
  END IF;
  IF (
    SELECT runtime_contract_digest FROM runtime_nodes
    WHERE node_id = '69000000-0000-4000-8000-000000000004'
  ) <> '$want_digest' THEN
    RAISE EXCEPTION 'Runtime Node digest was not cut over';
  END IF;
  IF (
    SELECT status FROM runtime_sessions
    WHERE runtime_session_id = '$closed_session'
  ) <> 'closed' THEN
    RAISE EXCEPTION 'old Runtime Session was not closed';
  END IF;
  IF EXISTS (
    SELECT 1 FROM runtime_session_attachments
    WHERE runtime_session_id = '$closed_session'
      AND detached_at IS NULL
  ) THEN
    RAISE EXCEPTION 'old Runtime Session attachment was not detached';
  END IF;
END
\$\$;
SQL
}

echo "[069] apply predecessor migrations"
apply_through_068

echo "[069] fail closed outside hard maintenance"
psql_command --quiet -c "UPDATE runtime_cluster_control SET mode = 'normal' WHERE singleton_id = 1" >/dev/null
expect_apply_failure "migration 069 requires hard maintenance"
psql_command --quiet -c "UPDATE runtime_cluster_control SET mode = 'hard_maintenance' WHERE singleton_id = 1" >/dev/null

echo "[069] fail closed while a Core member is registered"
psql_command --quiet -c "
  INSERT INTO runtime_cluster_members (
    instance_id, release_version, release_commit, schema_version,
    schema_checksum, runtime_contract_id, runtime_contract_digest
  ) VALUES (
    '69000000-0000-4000-8000-000000000098', 'test', 'test', 67,
    repeat('a', 64), 'openlinker.runtime.v2', '$OLD_DIGEST'
  )" >/dev/null
expect_apply_failure "migration 069 requires zero registered Core cluster members"
psql_command --quiet -c "DELETE FROM runtime_cluster_members" >/dev/null

echo "[069] fail closed when a migration lock cannot be acquired"
docker exec \
  --env PGOPTIONS="-c client_min_messages=warning" \
  --env PGAPPNAME="migration-069-lock-holder" \
  "$CONTAINER_NAME" \
  psql -X -v ON_ERROR_STOP=1 -U postgres -d "$DATABASE_NAME" \
  -c "BEGIN; LOCK TABLE runtime_session_attachments IN ACCESS SHARE MODE; SELECT pg_sleep(30); COMMIT;" \
  >/dev/null 2>&1 &
lock_holder_pid=$!
for _ in $(seq 1 100); do
  if [[ "$(psql_command --tuples-only --no-align -c "
    SELECT COUNT(*)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND application_name = 'migration-069-lock-holder'
      AND state = 'active'
  ")" == "1" ]]; then
    break
  fi
  sleep 0.1
done
[[ "$(psql_command --tuples-only --no-align -c "
  SELECT COUNT(*)
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND application_name = 'migration-069-lock-holder'
    AND state = 'active'
")" == "1" ]] || fail "lock holder did not become active"
expect_apply_failure "canceling statement due to lock timeout"
psql_command --quiet -c "
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND application_name = 'migration-069-lock-holder'
" >/dev/null
wait "$lock_holder_pid" 2>/dev/null || true

insert_principals

echo "[069] fail closed while a Run is still running"
psql_command --quiet -c "
  INSERT INTO runs (
    id, user_id, agent_id, input, status, cost_cents,
    platform_fee_cents, creator_revenue_cents, source,
    idempotency_key_hash, idempotency_fingerprint,
    connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
  ) VALUES (
    '69000000-0000-4000-8000-000000000040',
    '69000000-0000-4000-8000-000000000001',
    '69000000-0000-4000-8000-000000000002',
    '{\"case\":\"migration-069-running\"}', 'running', 0, 0, 0, 'api',
    decode(repeat('11', 32), 'hex'), decode(repeat('22', 32), 'hex'),
    'agent_node', clock_timestamp() + interval '2 minutes',
    clock_timestamp() + interval '10 minutes'
  )" >/dev/null
expect_apply_failure "migration 069 requires zero running Runs"
reset_database
apply_through_068
insert_principals

echo "[069] fail closed on a conflicting schema 69 identity"
psql_command --quiet -c "
  INSERT INTO runtime_schema_contracts (
    schema_version, migration_name, runtime_contract_id,
    runtime_contract_digest, is_current
  ) VALUES (
    69, '069_conflicting_fixture', 'openlinker.runtime.v2',
    '$UNKNOWN_DIGEST', FALSE
  )" >/dev/null
expect_apply_failure "migration 069 found a conflicting historical schema contract 69"
psql_command --quiet -c "DELETE FROM runtime_schema_contracts WHERE schema_version = 69" >/dev/null

echo "[069] fail closed on an unknown Runtime principal digest"
psql_stdin --quiet <<SQL
ALTER TABLE runtime_nodes DROP CONSTRAINT runtime_nodes_contract_current;
INSERT INTO runtime_schema_contracts (
  schema_version, migration_name, runtime_contract_id,
  runtime_contract_digest, is_current
) VALUES (
  969, '969_unknown_digest_fixture', 'openlinker.runtime.v2',
  '$UNKNOWN_DIGEST', FALSE
);
INSERT INTO runtime_nodes (
  node_id, display_name, device_certificate_serial,
  device_public_key_thumbprint, node_version, protocol_version,
  runtime_contract_id, runtime_contract_digest, features, capacity,
  status, revoked_at
) VALUES (
  '69000000-0000-4000-8000-000000000041',
  'Unknown Digest Fixture', 'serial-migration-069-unknown',
  'thumbprint-migration-069-unknown', '0.2.0-test', 2,
  'openlinker.runtime.v2', '$UNKNOWN_DIGEST',
  ARRAY[
    'lease_fence', 'assignment_confirm', 'renew', 'resume',
    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
  ],
  0, 'revoked', clock_timestamp()
);
SQL
expect_apply_failure "migration 069 found an unknown Runtime contract identity"
reset_database
apply_through_068
insert_principals

echo "[069] close old Session and activate canonical entry contract"
insert_session "10" "$OLD_DIGEST"
apply_069
verify_069
assert_cutover "69000000-0000-4000-8000-000000000010" 69 "$NEW_DIGEST"

echo "[069] rollback closes new Session and restores schema 67"
insert_session "20" "$NEW_DIGEST"
revert_069
assert_cutover "69000000-0000-4000-8000-000000000020" 67 "$OLD_DIGEST"

echo "[069] re-up is deterministic and closes post-rollback Session"
insert_session "30" "$OLD_DIGEST"
apply_069
verify_069
assert_cutover "69000000-0000-4000-8000-000000000030" 69 "$NEW_DIGEST"

echo "migration 069 test passed"
