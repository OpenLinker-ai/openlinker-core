#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS_DIR="$ROOT_DIR/migrations"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
CONTAINER_NAME="openlinker-migration-077-${PPID}-$$"
DATABASE_NAME="openlinker"

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

fail() {
  echo "migration 077 test failed: $*" >&2
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

expect_down_failure() {
  local expected="$1"
  local output status
  set +e
  output="$(run_migration "$MIGRATIONS_DIR/077_external_execution_cancellation.down.sql" 2>&1)"
  status=$?
  set -e
  if ((status == 0)); then
    fail "077 rollback unexpectedly succeeded; expected: $expected"
  fi
  [[ "$output" == *"$expected"* ]] \
    || fail "077 rollback failed for the wrong reason; expected '$expected', got: $output"
}

expect_up_failure() {
  local expected="$1"
  local output status
  set +e
  output="$(run_migration "$MIGRATIONS_DIR/077_external_execution_cancellation.up.sql" 2>&1)"
  status=$?
  set -e
  if ((status == 0)); then
    fail "077 upgrade unexpectedly succeeded; expected: $expected"
  fi
  [[ "$output" == *"$expected"* ]] \
    || fail "077 upgrade failed for the wrong reason; expected '$expected', got: $output"
}

echo "[077] apply predecessor migrations through 072"
for migration_path in "$MIGRATIONS_DIR"/[0-9][0-9][0-9]_*.up.sql; do
  migration_name="$(basename "$migration_path")"
  version="${migration_name%%_*}"
  if ((10#$version <= 72)); then
    run_migration "$migration_path" >/dev/null
  fi
done
psql_stdin --quiet <<'SQL'
UPDATE runtime_cluster_control SET mode = 'hard_maintenance' WHERE singleton_id = 1;
SQL

echo "[077] apply 073 through 076 and seed a pre-077 execution"
for version in 073 074 075 076; do
  migration_path="$(find "$MIGRATIONS_DIR" -maxdepth 1 -name "${version}_*.up.sql" -print -quit)"
  run_migration "$migration_path" >/dev/null
done
psql_stdin --quiet <<'SQL'
INSERT INTO users (id, email, password_hash, display_name)
VALUES (
  '77000000-0000-4000-8000-000000000001',
  'migration-077@example.test', 'test-hash', 'Migration 077 Actor'
);
INSERT INTO external_executions (
  caller_service_id, external_request_id, actor_user_id, target_type, target_id,
  input_fingerprint, trace_id, expected_contract_hash, input_schema_fingerprint
) VALUES (
  'migration-077', '77000000-0000-4000-8000-000000000010',
  '77000000-0000-4000-8000-000000000001', 'agent',
  '77000000-0000-4000-8000-000000000020', decode(repeat('11', 32), 'hex'),
  'migration-077-seed',
  'hct:v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  decode(repeat('22', 32), 'hex')
);

UPDATE users
SET is_creator = TRUE
WHERE id = '77000000-0000-4000-8000-000000000001';
INSERT INTO agents (
  id, creator_id, slug, name, description, endpoint_url,
  price_per_call_cents, connection_mode
) VALUES (
  '77000000-0000-4000-8000-000000000002',
  '77000000-0000-4000-8000-000000000001',
  'migration-077-agent', 'Migration 077 Agent', 'Drain rollback fixture',
  'openlinker-runtime://migration-077-agent', 0, 'runtime'
);
INSERT INTO agent_tokens (
  id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
  status, redeemed_at
) VALUES (
  '77000000-0000-4000-8000-000000000003',
  '77000000-0000-4000-8000-000000000002',
  '77000000-0000-4000-8000-000000000001',
  'Migration 077 Token', 'ol_agent_07700000', 'migration-077-token-hash',
  ARRAY['agent:call', 'agent:pull'], 'active_runtime', clock_timestamp()
);
INSERT INTO runtime_nodes (
  node_id, display_name, device_certificate_serial,
  device_public_key_thumbprint, node_version, protocol_version,
  runtime_contract_id, runtime_contract_digest, features,
  capacity, inflight, status, last_seen_at
) VALUES (
  '77000000-0000-4000-8000-000000000004', 'Migration 077 Node',
  '77000000000040008000000000000004', repeat('7', 64), '0.77.0-test', 2,
  'openlinker.runtime.v2',
  '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9',
  ARRAY[
    'lease_fence', 'assignment_confirm', 'renew', 'resume',
    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
  ], 4, 1, 'active', clock_timestamp()
);
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO runtime_sessions (
  runtime_session_id, node_id, agent_id, credential_id, worker_id,
  session_epoch, device_certificate_serial, node_version,
  protocol_version, runtime_contract_id, runtime_contract_digest,
  features, capacity, inflight, status, attached_core_instance_id
) VALUES (
  '77000000-0000-4000-8000-000000000005',
  '77000000-0000-4000-8000-000000000004',
  '77000000-0000-4000-8000-000000000002',
  '77000000-0000-4000-8000-000000000003',
  'migration-077-worker', 1, '77000000000040008000000000000004',
  '0.77.0-test', 2, 'openlinker.runtime.v2',
  '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9',
  ARRAY[
    'lease_fence', 'assignment_confirm', 'renew', 'resume',
    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
  ], 1, 1, 'draining', '77000000-0000-4000-8000-000000000099'
);
INSERT INTO runtime_session_attachments (
  id, runtime_session_id, core_instance_id, attachment_kind,
  transport, transport_reason
) VALUES (
  '77000000-0000-4000-8000-000000000006',
  '77000000-0000-4000-8000-000000000005',
  '77000000-0000-4000-8000-000000000099',
  'connected', 'websocket', 'explicit'
);
COMMIT;
SQL

echo "[077] fail closed for an ambiguous pre-077 draining Session"
expect_up_failure "requires zero pre-077 draining Runtime Sessions"
psql_stdin --quiet <<'SQL'
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
UPDATE runtime_session_attachments
SET detached_at = clock_timestamp(), disconnect_reason = 'migration 077 fixture closed'
WHERE runtime_session_id = '77000000-0000-4000-8000-000000000005';
UPDATE runtime_sessions
SET status = 'closed', attached_core_instance_id = NULL,
    disconnected_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE runtime_session_id = '77000000-0000-4000-8000-000000000005';
UPDATE runtime_nodes
SET inflight = 0, updated_at = clock_timestamp()
WHERE node_id = '77000000-0000-4000-8000-000000000004';
COMMIT;
SQL

echo "[077] upgrade, verify backfill, clean rollback, and re-upgrade"
run_migration "$MIGRATIONS_DIR/077_external_execution_cancellation.up.sql" >/dev/null
run_migration "$MIGRATIONS_DIR/077_external_execution_cancellation_verify.sql" >/dev/null

echo "[077] rollback fails closed when predecessor schema or wire state is missing"
psql_stdin --quiet <<'SQL'
UPDATE runtime_schema_contracts
SET migration_name = '076_runtime_cancellation_terminal_reap_broken'
WHERE schema_version = 76;
SQL
expect_down_failure "requires the exact historical schema contract 76"
psql_stdin --quiet <<'SQL'
UPDATE runtime_schema_contracts
SET migration_name = '076_runtime_cancellation_terminal_reap'
WHERE schema_version = 76;
ALTER TABLE runtime_wire_contracts
DROP CONSTRAINT runtime_wire_contracts_support_identity;
UPDATE runtime_wire_contracts
SET support_tier = 'historical'
WHERE runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
  AND support_tier = 'previous';
SQL
expect_down_failure "requires the exact current/previous/historical Runtime wire state"
psql_stdin --quiet <<'SQL'
UPDATE runtime_wire_contracts
SET support_tier = 'previous'
WHERE runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
  AND support_tier = 'historical';
ALTER TABLE runtime_wire_contracts
ADD CONSTRAINT runtime_wire_contracts_support_identity
CHECK (
  runtime_contract_id = 'openlinker.runtime.v2'
  AND (
    (support_tier = 'current'
     AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481')
    OR
    (support_tier = 'previous'
     AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9')
    OR
    (support_tier = 'historical'
     AND runtime_contract_digest NOT IN (
       '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481',
       '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9'
     ))
  )
);
SQL
psql_stdin --quiet <<'SQL'
DO $$
BEGIN
  IF (
    SELECT count(*) FROM external_execution_keys
    WHERE caller_service_id = 'migration-077'
      AND external_request_id = '77000000-0000-4000-8000-000000000010'
      AND actor_user_id = '77000000-0000-4000-8000-000000000001'
  ) <> 1 THEN
    RAISE EXCEPTION '077 did not backfill exact actor key';
  END IF;
END
$$;
SQL
run_migration "$MIGRATIONS_DIR/077_external_execution_cancellation.down.sql" >/dev/null
run_migration "$MIGRATIONS_DIR/077_external_execution_cancellation.up.sql" >/dev/null
run_migration "$MIGRATIONS_DIR/077_external_execution_cancellation_verify.sql" >/dev/null

echo "[077] fail closed after cancellation evidence exists"
psql_stdin --quiet <<'SQL'
INSERT INTO external_execution_cancellations (
  id, caller_service_id, external_request_id, actor_user_id,
  reason_code, state, applied_at, finished_at
) VALUES (
  '77000000-0000-4000-8000-000000000030', 'migration-077',
  '77000000-0000-4000-8000-000000000010',
  '77000000-0000-4000-8000-000000000001',
  'CALLER_REQUESTED', 'stopped', clock_timestamp(), clock_timestamp()
);
SQL
expect_down_failure "rollback refuses external cancellation evidence"
run_migration "$MIGRATIONS_DIR/077_external_execution_cancellation_verify.sql" >/dev/null

echo "migration 077 test passed"
