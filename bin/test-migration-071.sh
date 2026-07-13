#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS_DIR="$ROOT_DIR/migrations"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
CONTAINER_NAME="openlinker-migration-071-${PPID}-$$"
DATABASE_NAME="openlinker"
OLD_DIGEST="fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53"
NEW_DIGEST="3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9"
UNKNOWN_DIGEST="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

fail() {
  echo "migration 071 test failed: $*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
docker info >/dev/null 2>&1 || fail "docker daemon is not available"

for fragment in \
  "LOCK TABLE runtime_session_attachments IN ACCESS EXCLUSIVE MODE" \
  "UPDATE runtime_session_attachments attachment" \
  "UPDATE runtime_sessions" \
  "UPDATE runtime_nodes" \
  "071_runtime_attachment_generation"; do
  grep -Fq "$fragment" "$MIGRATIONS_DIR/071_runtime_attachment_generation.up.sql" \
    || fail "attachment-generation cutover fragment is missing: $fragment"
done

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

apply_through_070() {
  local migration_path migration_name version
  for migration_path in "$MIGRATIONS_DIR"/[0-9][0-9][0-9]_*.up.sql; do
    migration_name="$(basename "$migration_path")"
    version="${migration_name%%_*}"
    if ((10#$version <= 70)); then
      run_migration "$migration_path" >/dev/null
    fi
  done
}

apply_071() {
  run_migration "$MIGRATIONS_DIR/071_runtime_attachment_generation.up.sql" >/dev/null
}

revert_071() {
  run_migration "$MIGRATIONS_DIR/071_runtime_attachment_generation.down.sql" >/dev/null
}

verify_071() {
  run_migration "$MIGRATIONS_DIR/071_runtime_attachment_generation_verify.sql" >/dev/null
}

verify_070() {
  run_migration "$MIGRATIONS_DIR/070_sdk_first_runtime_boundary_verify.sql" >/dev/null
}

expect_apply_failure() {
  local expected="$1"
  local output status
  set +e
  output="$(apply_071 2>&1)"
  status=$?
  set -e
  if ((status == 0)); then
    fail "071 unexpectedly succeeded; expected: $expected"
  fi
  [[ "$output" == *"$expected"* ]] \
    || fail "071 failed for the wrong reason; expected '$expected', got: $output"
}

insert_principals() {
  psql_stdin --quiet <<SQL
INSERT INTO users (id, email, password_hash, display_name, is_creator)
VALUES (
  '71000000-0000-4000-8000-000000000001',
  'migration-071@example.test', 'test-hash', 'Migration 071', TRUE
);

INSERT INTO agents (
  id, creator_id, slug, name, description, endpoint_url,
  price_per_call_cents, connection_mode
) VALUES (
  '71000000-0000-4000-8000-000000000002',
  '71000000-0000-4000-8000-000000000001',
  'migration-071-agent', 'Migration 071 Agent', 'Attachment generation fixture',
  'openlinker-runtime://migration-071-agent', 0, 'runtime'
);

INSERT INTO agent_tokens (
  id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
  status, redeemed_at
) VALUES (
  '71000000-0000-4000-8000-000000000003',
  '71000000-0000-4000-8000-000000000002',
  '71000000-0000-4000-8000-000000000001',
  'Migration 071 Token', 'ol_agent_07100000', 'migration-071-token-hash',
  ARRAY['agent:call', 'agent:pull'], 'active_runtime', clock_timestamp()
);

INSERT INTO runtime_nodes (
  node_id, display_name, device_certificate_serial,
  device_public_key_thumbprint, node_version, protocol_version,
  runtime_contract_id, runtime_contract_digest, features, capacity,
  last_seen_at
) VALUES (
  '71000000-0000-4000-8000-000000000004',
  'Migration 071 Node', 'serial-migration-071', 'thumbprint-migration-071',
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
  local session_id="71000000-0000-4000-8000-0000000000${suffix}"
  local attachment_id="71000000-0000-4000-8000-0000000001${suffix}"
  psql_stdin --quiet <<SQL
BEGIN;
INSERT INTO runtime_sessions (
  runtime_session_id, node_id, agent_id, credential_id, worker_id,
  session_epoch, device_certificate_serial, node_version,
  protocol_version, runtime_contract_id, runtime_contract_digest,
  features, capacity, attached_core_instance_id
) VALUES (
  '$session_id',
  '71000000-0000-4000-8000-000000000004',
  '71000000-0000-4000-8000-000000000002',
  '71000000-0000-4000-8000-000000000003',
  'worker-$suffix', $((10#$suffix)), 'serial-migration-071', '0.2.0-test', 2,
  'openlinker.runtime.v2', '$digest',
  ARRAY[
    'lease_fence', 'assignment_confirm', 'renew', 'resume',
    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
  ],
  1, '71000000-0000-4000-8000-000000000099'
);
INSERT INTO runtime_session_attachments (
  id, runtime_session_id, core_instance_id, attachment_kind
) VALUES (
  '$attachment_id', '$session_id',
  '71000000-0000-4000-8000-000000000099', 'connected'
);
COMMIT;
SQL
}

assert_cutover() {
  local closed_session="$1"
  local current_schema="$2"
  local current_migration="$3"
  local current_digest="$4"
  local historical_schema="$5"
  local historical_migration="$6"
  local historical_digest="$7"
  psql_stdin --quiet <<SQL
DO \$\$
BEGIN
  IF (
    SELECT COUNT(*) FROM runtime_schema_contracts
    WHERE schema_version = $current_schema
      AND migration_name = '$current_migration'
      AND runtime_contract_digest = '$current_digest'
      AND is_current
  ) <> 1 OR (SELECT COUNT(*) FROM runtime_schema_contracts WHERE is_current) <> 1 THEN
    RAISE EXCEPTION 'unexpected current Runtime schema identity';
  END IF;
  IF (
    SELECT COUNT(*) FROM runtime_schema_contracts
    WHERE schema_version = $historical_schema
      AND migration_name = '$historical_migration'
      AND runtime_contract_digest = '$historical_digest'
      AND NOT is_current
  ) <> 1 THEN
    RAISE EXCEPTION 'expected Runtime schema history is missing';
  END IF;
  IF (
    SELECT runtime_contract_digest FROM runtime_nodes
    WHERE node_id = '71000000-0000-4000-8000-000000000004'
  ) <> '$current_digest' THEN
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

echo "[071] apply predecessor migrations"
apply_through_070

echo "[071] fail closed outside hard maintenance"
psql_command --quiet -c "UPDATE runtime_cluster_control SET mode = 'normal' WHERE singleton_id = 1" >/dev/null
expect_apply_failure "migration 071 requires hard maintenance"
psql_command --quiet -c "UPDATE runtime_cluster_control SET mode = 'hard_maintenance' WHERE singleton_id = 1" >/dev/null

echo "[071] fail closed while a Core member is registered"
psql_command --quiet -c "
  INSERT INTO runtime_cluster_members (
    instance_id, release_version, release_commit, schema_version,
    schema_checksum, runtime_contract_id, runtime_contract_digest
  ) VALUES (
    '71000000-0000-4000-8000-000000000098', 'test', 'test', 70,
    repeat('a', 64), 'openlinker.runtime.v2', '$OLD_DIGEST'
  )" >/dev/null
expect_apply_failure "migration 071 requires zero registered Core cluster members"
psql_command --quiet -c "DELETE FROM runtime_cluster_members" >/dev/null

insert_principals

echo "[071] fail closed while a Run is still running"
psql_command --quiet -c "
  INSERT INTO runs (
    id, user_id, agent_id, input, status, cost_cents,
    platform_fee_cents, creator_revenue_cents, source,
    idempotency_key_hash, idempotency_fingerprint,
    connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
  ) VALUES (
    '71000000-0000-4000-8000-000000000040',
    '71000000-0000-4000-8000-000000000001',
    '71000000-0000-4000-8000-000000000002',
    '{\"case\":\"migration-071-running\"}', 'running', 0, 0, 0, 'api',
    decode(repeat('11', 32), 'hex'), decode(repeat('22', 32), 'hex'),
    'runtime', clock_timestamp() + interval '2 minutes',
    clock_timestamp() + interval '10 minutes'
  )" >/dev/null
expect_apply_failure "migration 071 requires zero running Runs"

reset_database
apply_through_070
insert_principals

echo "[071] fail closed on a conflicting schema 71 identity"
psql_command --quiet -c "
  INSERT INTO runtime_schema_contracts (
    schema_version, migration_name, runtime_contract_id,
    runtime_contract_digest, is_current
  ) VALUES (
    71, '071_conflicting_fixture', 'openlinker.runtime.v2',
    '$UNKNOWN_DIGEST', FALSE
  )" >/dev/null
expect_apply_failure "migration 071 found a conflicting historical schema contract 71"

reset_database
apply_through_070
insert_principals

echo "[071] close old Session and activate attachment-generation contract"
insert_session "10" "$OLD_DIGEST"
apply_071
verify_071
assert_cutover \
  "71000000-0000-4000-8000-000000000010" \
  71 "071_runtime_attachment_generation" "$NEW_DIGEST" \
  70 "070_sdk_first_runtime_boundary" "$OLD_DIGEST"

echo "[071] rollback closes new Session and restores schema 70"
insert_session "20" "$NEW_DIGEST"
revert_071
verify_070
assert_cutover \
  "71000000-0000-4000-8000-000000000020" \
  70 "070_sdk_first_runtime_boundary" "$OLD_DIGEST" \
  71 "071_runtime_attachment_generation" "$NEW_DIGEST"

echo "[071] re-up is deterministic and closes post-rollback Session"
insert_session "30" "$OLD_DIGEST"
apply_071
verify_071
assert_cutover \
  "71000000-0000-4000-8000-000000000030" \
  71 "071_runtime_attachment_generation" "$NEW_DIGEST" \
  70 "070_sdk_first_runtime_boundary" "$OLD_DIGEST"

echo "migration 071 test passed"
