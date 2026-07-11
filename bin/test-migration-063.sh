#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS_DIR="$ROOT_DIR/migrations"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
CONTAINER_NAME="openlinker-migration-063-${PPID}-$$"
DATABASE_NAME="openlinker"
LOCK_ORDER_OUTPUT_FILE=""

cleanup() {
  if [[ -n "$LOCK_ORDER_OUTPUT_FILE" ]]; then
    rm -f "$LOCK_ORDER_OUTPUT_FILE"
  fi
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

fail() {
  echo "migration 063 test failed: $*" >&2
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

psql_command() {
  docker exec --env PGOPTIONS="-c client_min_messages=warning" "$CONTAINER_NAME" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d "$DATABASE_NAME" "$@"
}

run_migration() {
  local migration_path="$1"
  psql_stdin --quiet <"$migration_path"
}

reset_database() {
  docker exec "$CONTAINER_NAME" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres --quiet \
      -c "DROP DATABASE IF EXISTS $DATABASE_NAME WITH (FORCE)" \
      -c "CREATE DATABASE $DATABASE_NAME"
}

apply_through_062() {
  local migration_path migration_name version
  for migration_path in "$MIGRATIONS_DIR"/[0-9][0-9][0-9]_*.up.sql; do
    migration_name="$(basename "$migration_path")"
    version="${migration_name%%_*}"
    if ((10#$version <= 62)); then
      run_migration "$migration_path"
    fi
  done
}

apply_063() {
  run_migration "$MIGRATIONS_DIR/063_reliable_runtime_v2.up.sql" >/dev/null
}

verify_063() {
  run_migration "$MIGRATIONS_DIR/063_reliable_runtime_v2_verify.sql" >/dev/null
}

revert_063() {
  run_migration "$MIGRATIONS_DIR/063_reliable_runtime_v2.down.sql" >/dev/null
}

expect_migration_failure() {
  local expected="$1"
  local output status
  set +e
  output="$(apply_063 2>&1)"
  status=$?
  set -e
  if ((status == 0)); then
    fail "063 unexpectedly succeeded; expected: $expected"
  fi
  [[ "$output" == *"$expected"* ]] \
    || fail "063 failed for the wrong reason; expected '$expected', got: $output"
}

expect_sql_failure() {
  local expected="$1"
  local statement="$2"
  local output status
  set +e
  output="$(psql_command --quiet -c "$statement" 2>&1)"
  status=$?
  set -e
  if ((status == 0)); then
    fail "SQL unexpectedly succeeded; expected: $expected"
  fi
  [[ "$output" == *"$expected"* ]] \
    || fail "SQL failed for the wrong reason; expected '$expected', got: $output"
}

assert_plan_uses() {
  local index_name="$1"
  local query="$2"
  local plan
  plan="$(psql_command --tuples-only --no-align \
    -c "SET enable_seqscan = off; EXPLAIN (COSTS OFF) $query")"
  [[ "$plan" == *"$index_name"* ]] \
    || fail "query plan did not use $index_name: $plan"
}

insert_historical_fixture() {
  psql_stdin --quiet <<'SQL'
INSERT INTO users (id, email, password_hash, display_name, is_creator)
VALUES (
  '00000000-0000-4000-8000-000000000001',
  'migration-063@example.test',
  'test-hash',
  'Migration 063',
  TRUE
);

INSERT INTO agents (
  id, creator_id, slug, name, description, endpoint_url,
  price_per_call_cents, connection_mode
) VALUES (
  '00000000-0000-4000-8000-000000000002',
  '00000000-0000-4000-8000-000000000001',
  'migration-063-agent',
  'Migration 063 Agent',
  'Migration regression fixture',
  'https://example.test/agent',
  100,
  'direct_http'
);

INSERT INTO runs (
  id, user_id, agent_id, input, output, status, error_code, error_message,
  cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
  started_at, finished_at, source
) VALUES
  (
    '00000000-0000-4000-8000-000000000101',
    '00000000-0000-4000-8000-000000000001',
    '00000000-0000-4000-8000-000000000002',
    '{"case":"success"}', '{"ok":true}', 'success', NULL, NULL,
    100, 20, 80, 1200,
    '2026-07-01T00:00:00Z', '2026-07-01T00:00:01.2Z', 'web'
  ),
  (
    '00000000-0000-4000-8000-000000000102',
    '00000000-0000-4000-8000-000000000001',
    '00000000-0000-4000-8000-000000000002',
    '{"case":"failed"}', NULL, 'failed', 'FIXTURE_FAILED', 'fixture failed',
    100, 20, 80, 1300,
    '2026-07-01T00:01:00Z', '2026-07-01T00:01:01.3Z', 'api'
  ),
  (
    '00000000-0000-4000-8000-000000000103',
    '00000000-0000-4000-8000-000000000001',
    '00000000-0000-4000-8000-000000000002',
    '{"case":"timeout"}', NULL, 'timeout', 'FIXTURE_TIMEOUT', 'fixture timeout',
    100, 20, 80, 1400,
    '2026-07-01T00:02:00Z', '2026-07-01T00:02:01.4Z', 'mcp'
  ),
  (
    '00000000-0000-4000-8000-000000000104',
    '00000000-0000-4000-8000-000000000001',
    '00000000-0000-4000-8000-000000000002',
    '{"case":"canceled"}', NULL, 'canceled', 'CANCELED', 'fixture canceled',
    100, 20, 80, 1500,
    '2026-07-01T00:03:00Z', '2026-07-01T00:03:01.5Z', 'web'
  );

INSERT INTO run_events (id, run_id, sequence, event_type, payload, created_at)
VALUES
  (
    '00000000-0000-4000-8000-000000000201',
    '00000000-0000-4000-8000-000000000101',
    1, 'run.completed', '{"status":"success","fixture":true}',
    '2026-07-01T00:00:01.2Z'
  ),
  (
    '00000000-0000-4000-8000-000000000202',
    '00000000-0000-4000-8000-000000000102',
    1, 'run.completed', '{"status":"success","fixture":"conflict"}',
    '2026-07-01T00:01:01.3Z'
  ),
  (
    '00000000-0000-4000-8000-000000000203',
    '00000000-0000-4000-8000-000000000103',
    1, 'run.failed', '{"status":"timeout","fixture":true}',
    '2026-07-01T00:02:01.4Z'
  );

CREATE TABLE migration_063_test_runs_before AS
SELECT
  id, user_id, agent_id, input, output, status, error_code, error_message,
  cost_cents, platform_fee_cents, creator_revenue_cents, duration_ms,
  started_at, finished_at, source
FROM runs;

CREATE TABLE migration_063_test_events_before AS
SELECT id, run_id, parent_run_id, sequence, event_type, payload, created_at
FROM run_events;
SQL
}

assert_historical_upgrade() {
  psql_stdin --quiet <<'SQL'
DO $$
BEGIN
  IF (SELECT COUNT(*) FROM runs) <> 4 THEN
    RAISE EXCEPTION 'historical run count changed';
  END IF;

  IF EXISTS (SELECT 1 FROM runs WHERE request_metadata <> '{}'::jsonb) THEN
    RAISE EXCEPTION 'historical Run request metadata backfill is inconsistent';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM runs r
    JOIN migration_063_test_runs_before b USING (id)
    WHERE ROW(
      r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code,
      r.error_message, r.cost_cents, r.platform_fee_cents,
      r.creator_revenue_cents, r.duration_ms, r.started_at, r.finished_at, r.source
    ) IS DISTINCT FROM ROW(
      b.user_id, b.agent_id, b.input, b.output, b.status, b.error_code,
      b.error_message, b.cost_cents, b.platform_fee_cents,
      b.creator_revenue_cents, b.duration_ms, b.started_at, b.finished_at, b.source
    )
  ) THEN
    RAISE EXCEPTION 'historical run fields changed';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM run_events e
    JOIN migration_063_test_events_before b USING (id)
    WHERE ROW(e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload, e.created_at)
      IS DISTINCT FROM
      ROW(b.run_id, b.parent_run_id, b.sequence, b.event_type, b.payload, b.created_at)
  ) THEN
    RAISE EXCEPTION 'historical run events changed';
  END IF;

  IF (
    SELECT COUNT(*)
    FROM run_events
    WHERE payload @> '{"terminal":true,"migrated":true,"migration":"063_reliable_runtime_v2"}'::jsonb
  ) <> 2 THEN
    RAISE EXCEPTION 'unexpected migrated terminal event count';
  END IF;

  IF (SELECT COUNT(*) FROM run_accounting_ledger) <> 4 THEN
    RAISE EXCEPTION 'unexpected accounting ledger count';
  END IF;

  IF (
    SELECT terminal_event_id
    FROM runs
    WHERE id = '00000000-0000-4000-8000-000000000101'
  ) <> '00000000-0000-4000-8000-000000000201'::uuid THEN
    RAISE EXCEPTION 'compatible success event was not reused';
  END IF;

  IF (
    SELECT terminal_event_id
    FROM runs
    WHERE id = '00000000-0000-4000-8000-000000000103'
  ) <> '00000000-0000-4000-8000-000000000203'::uuid THEN
    RAISE EXCEPTION 'compatible timeout event was not reused';
  END IF;
END
$$;
SQL
}

assert_historical_down() {
  psql_stdin --quiet <<'SQL'
DO $$
BEGIN
  IF (SELECT COUNT(*) FROM runs) <> 4 OR (SELECT COUNT(*) FROM run_events) <> 3 THEN
    RAISE EXCEPTION 'down migration did not restore historical row counts';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM runs r
    JOIN migration_063_test_runs_before b USING (id)
    WHERE ROW(
      r.user_id, r.agent_id, r.input, r.output, r.status, r.error_code,
      r.error_message, r.cost_cents, r.platform_fee_cents,
      r.creator_revenue_cents, r.duration_ms, r.started_at, r.finished_at, r.source
    ) IS DISTINCT FROM ROW(
      b.user_id, b.agent_id, b.input, b.output, b.status, b.error_code,
      b.error_message, b.cost_cents, b.platform_fee_cents,
      b.creator_revenue_cents, b.duration_ms, b.started_at, b.finished_at, b.source
    )
  ) THEN
    RAISE EXCEPTION 'down migration changed historical run fields';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM run_events e
    JOIN migration_063_test_events_before b USING (id)
    WHERE ROW(e.run_id, e.parent_run_id, e.sequence, e.event_type, e.payload, e.created_at)
      IS DISTINCT FROM
      ROW(b.run_id, b.parent_run_id, b.sequence, b.event_type, b.payload, b.created_at)
  ) THEN
    RAISE EXCEPTION 'down migration changed historical run events';
  END IF;
END
$$;
SQL
}

run_fresh_database_scenario() {
  echo "[063] fresh database and reversible pre-reopen cutover"
  reset_database
  apply_through_062
  apply_063
  verify_063
  revert_063
  apply_063
  verify_063
}

run_historical_upgrade_scenario() {
  echo "[063] historical terminal backfill and exact rollback"
  reset_database
  apply_through_062
  insert_historical_fixture
  apply_063
  verify_063
  assert_historical_upgrade
  revert_063
  assert_historical_down
  apply_063
  verify_063
  assert_historical_upgrade
}

run_preflight_scenarios() {
  echo "[063] preflight rejects a surviving legacy database session"
  reset_database
  apply_through_062
  docker exec --detach \
    --env PGAPPNAME="openlinker-core-v0.1.42" \
    "$CONTAINER_NAME" \
    psql -X -U postgres -d "$DATABASE_NAME" \
      -c "SELECT pg_sleep(60)" >/dev/null
  for _ in $(seq 1 50); do
    if [[ "$(psql_command --tuples-only --no-align -c "
      SELECT COUNT(*)
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND application_name = 'openlinker-core-v0.1.42'
    ")" == "1" ]]; then
      break
    fi
    sleep 0.1
  done
  expect_migration_failure "migration 063 requires an exclusive database client session"
  psql_command --quiet -c "
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND application_name = 'openlinker-core-v0.1.42'
  " >/dev/null

  echo "[063] preflight rejects nonterminal runs"
  reset_database
  apply_through_062
  insert_historical_fixture
  psql_command --quiet -c "
    INSERT INTO runs (
      id, user_id, agent_id, input, status, cost_cents,
      platform_fee_cents, creator_revenue_cents, source
    ) VALUES (
      '00000000-0000-4000-8000-000000000105',
      '00000000-0000-4000-8000-000000000001',
      '00000000-0000-4000-8000-000000000002',
      '{\"case\":\"running\"}', 'running', 100, 20, 80, 'web'
    )"
  expect_migration_failure "migration 063 requires zero nonterminal runs"

  echo "[063] preflight rejects pending legacy deliveries"
  reset_database
  apply_through_062
  insert_historical_fixture
  psql_command --quiet -c "
    INSERT INTO webhook_deliveries (
      id, agent_id, run_id, url, payload, status, next_retry_at
    ) VALUES (
      '00000000-0000-4000-8000-000000000301',
      '00000000-0000-4000-8000-000000000002',
      '00000000-0000-4000-8000-000000000101',
      'https://example.test/webhook', '{}', 'pending', clock_timestamp()
    )"
  expect_migration_failure "migration 063 requires zero pending legacy deliveries"
}

run_runtime_v2_invariant_scenario() {
  echo "[063] runtime v2 identity, terminal evidence, guards and hot indexes"
  reset_database
  apply_through_062
  apply_063
  verify_063

  psql_stdin --quiet <<'SQL'
INSERT INTO users (id, email, password_hash, display_name, is_creator)
VALUES (
  '10000000-0000-4000-8000-000000000001',
  'runtime-v2@example.test', 'test-hash', 'Runtime V2', TRUE
);

INSERT INTO agents (
  id, creator_id, slug, name, description, endpoint_url,
  price_per_call_cents, connection_mode
) VALUES
  (
    '10000000-0000-4000-8000-000000000002',
    '10000000-0000-4000-8000-000000000001',
    'runtime-v2-agent', 'Runtime V2 Agent', 'Runtime V2 fixture',
    'openlinker-runtime-ws://runtime-v2-agent', 100, 'runtime_ws'
  ),
  (
    '10000000-0000-4000-8000-000000000003',
    '10000000-0000-4000-8000-000000000001',
    'runtime-v2-other', 'Runtime V2 Other', 'Wrong principal fixture',
    'openlinker-runtime-ws://runtime-v2-other', 100, 'runtime_ws'
  );

INSERT INTO agent_tokens (
  id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
  status, redeemed_at
) VALUES (
  '10000000-0000-4000-8000-000000000004',
  '10000000-0000-4000-8000-000000000002',
  '10000000-0000-4000-8000-000000000001',
  'Runtime V2 Token', 'ol_agent_aabbccdd', 'test-token-hash',
  ARRAY['agent:call', 'agent:pull'], 'active_runtime', clock_timestamp()
);

INSERT INTO runtime_nodes (
  node_id, display_name, device_certificate_serial,
  device_public_key_thumbprint, node_version, protocol_version,
  runtime_contract_id, runtime_contract_digest, features, capacity,
  last_seen_at
) VALUES (
  '10000000-0000-4000-8000-000000000005',
  'Runtime V2 Node', 'serial-runtime-v2', 'thumbprint-runtime-v2',
  '0.2.0-test', 2, 'openlinker.runtime.v2',
  'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
  ARRAY[
    'lease_fence', 'assignment_confirm', 'renew', 'resume',
    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
  ],
  4, clock_timestamp()
);

BEGIN;
INSERT INTO runtime_sessions (
  runtime_session_id, node_id, agent_id, credential_id, worker_id,
  session_epoch, device_certificate_serial, node_version,
  protocol_version, runtime_contract_id, runtime_contract_digest,
  features, capacity, attached_core_instance_id
) VALUES (
  '10000000-0000-4000-8000-000000000006',
  '10000000-0000-4000-8000-000000000005',
  '10000000-0000-4000-8000-000000000002',
  '10000000-0000-4000-8000-000000000004',
  'worker-runtime-v2', 1, 'serial-runtime-v2', '0.2.0-test', 2,
  'openlinker.runtime.v2',
  'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
  ARRAY[
    'lease_fence', 'assignment_confirm', 'renew', 'resume',
    'event_ack', 'result_ack', 'cancel', 'persistent_spool'
  ],
  2, '10000000-0000-4000-8000-000000000007'
);

INSERT INTO runtime_session_attachments (
  id, runtime_session_id, core_instance_id, attachment_kind
) VALUES (
  '10000000-0000-4000-8000-000000000008',
  '10000000-0000-4000-8000-000000000006',
  '10000000-0000-4000-8000-000000000007',
  'connected'
);
COMMIT;

INSERT INTO runs (
  id, user_id, agent_id, input, status, cost_cents,
  platform_fee_cents, creator_revenue_cents, source,
  idempotency_key_hash, idempotency_fingerprint,
  connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
) VALUES (
  '10000000-0000-4000-8000-000000000010',
  '10000000-0000-4000-8000-000000000001',
  '10000000-0000-4000-8000-000000000002',
  '{"prompt":"runtime-v2"}', 'running', 100, 20, 80, 'api',
  decode(repeat('11', 32), 'hex'), decode(repeat('22', 32), 'hex'),
  'runtime_ws', clock_timestamp() + interval '2 minutes',
  clock_timestamp() + interval '10 minutes'
);

INSERT INTO run_events (
  id, run_id, sequence, event_type, payload
) VALUES (
  '10000000-0000-4000-8000-000000000011',
  '10000000-0000-4000-8000-000000000010',
  1, 'run.created', '{"source":"migration-test"}'
);

INSERT INTO runtime_signal_outbox (id, event_type, agent_id, run_id, payload)
VALUES (
  '10000000-0000-4000-8000-000000000012',
  'run.available',
  '10000000-0000-4000-8000-000000000002',
  '10000000-0000-4000-8000-000000000010',
  '{"run_id":"10000000-0000-4000-8000-000000000010"}'
);

BEGIN;
INSERT INTO run_attempts (
  id, run_id, agent_id, offer_no, executor_type, lease_id, fencing_token,
  runtime_token_id, runtime_worker_id, runtime_session_id, node_id,
  offered_by_core_instance_id, attached_core_instance_id,
  offered_at, offer_expires_at, lease_expires_at, attempt_deadline_at
) VALUES (
  '10000000-0000-4000-8000-000000000020',
  '10000000-0000-4000-8000-000000000010',
  '10000000-0000-4000-8000-000000000002',
  1, 'agent_node', '10000000-0000-4000-8000-000000000021', 1,
  '10000000-0000-4000-8000-000000000004',
  'worker-runtime-v2',
  '10000000-0000-4000-8000-000000000006',
  '10000000-0000-4000-8000-000000000005',
  '10000000-0000-4000-8000-000000000007',
  '10000000-0000-4000-8000-000000000007',
  clock_timestamp(), clock_timestamp() + interval '30 seconds',
  clock_timestamp() + interval '60 seconds',
  clock_timestamp() + interval '5 minutes'
);

UPDATE runs
SET dispatch_state = 'offered',
    offer_count = 1,
    latest_attempt_id = '10000000-0000-4000-8000-000000000020',
    active_attempt_id = '10000000-0000-4000-8000-000000000020',
    lease_id = '10000000-0000-4000-8000-000000000021',
    fencing_token = 1,
    executor_type = 'agent_node',
    active_core_instance_id = '10000000-0000-4000-8000-000000000007',
    runtime_node_id = '10000000-0000-4000-8000-000000000005',
    runtime_worker_id = 'worker-runtime-v2',
    runtime_session_id = '10000000-0000-4000-8000-000000000006',
    lease_token_id = '10000000-0000-4000-8000-000000000004',
    lease_offered_at = (
      SELECT offered_at FROM run_attempts
      WHERE id = '10000000-0000-4000-8000-000000000020'
    ),
    lease_expires_at = (
      SELECT lease_expires_at FROM run_attempts
      WHERE id = '10000000-0000-4000-8000-000000000020'
    ),
    attempt_deadline_at = (
      SELECT attempt_deadline_at FROM run_attempts
      WHERE id = '10000000-0000-4000-8000-000000000020'
    )
WHERE id = '10000000-0000-4000-8000-000000000010';
COMMIT;

BEGIN;
UPDATE run_attempts
SET attempt_no = 1,
    accepted_at = clock_timestamp()
WHERE id = '10000000-0000-4000-8000-000000000020';

UPDATE runs
SET dispatch_state = 'executing',
    attempt_count = 1,
    lease_accepted_at = (
      SELECT accepted_at FROM run_attempts
      WHERE id = '10000000-0000-4000-8000-000000000020'
    )
WHERE id = '10000000-0000-4000-8000-000000000010';
COMMIT;

BEGIN;
INSERT INTO run_events (
  id, run_id, sequence, event_type, payload,
  client_event_id, client_event_seq, payload_fingerprint,
  attempt_id, attempt_no, fencing_token
) VALUES (
  '10000000-0000-4000-8000-000000000030',
  '10000000-0000-4000-8000-000000000010',
  2, 'run.progress', '{"progress":50}',
  '10000000-0000-4000-8000-000000000031', 1,
  decode(repeat('33', 32), 'hex'),
  '10000000-0000-4000-8000-000000000020', 1, 1
);
UPDATE run_attempts
SET last_client_event_seq = 1
WHERE id = '10000000-0000-4000-8000-000000000020';
COMMIT;

BEGIN;
UPDATE run_attempts
SET finished_at = clock_timestamp(),
    outcome = 'success',
    result_id = '10000000-0000-4000-8000-000000000040',
    result_fingerprint = decode(repeat('44', 32), 'hex'),
    result_classification = 'success',
    final_client_event_seq = 1
WHERE id = '10000000-0000-4000-8000-000000000020';

INSERT INTO run_events (id, run_id, sequence, event_type, payload)
VALUES (
  '10000000-0000-4000-8000-000000000041',
  '10000000-0000-4000-8000-000000000010',
  3, 'run.completed', '{"status":"success","terminal":true}'
);

INSERT INTO run_accounting_ledger (
  run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
) VALUES (
  '10000000-0000-4000-8000-000000000010',
  '10000000-0000-4000-8000-000000000041',
  '10000000-0000-4000-8000-000000000002', 1, 80
);

INSERT INTO run_effect_outbox (
  id, run_id, terminal_event_id, effect_type, target_key, metadata
) VALUES (
  '10000000-0000-4000-8000-000000000042',
  '10000000-0000-4000-8000-000000000010',
  '10000000-0000-4000-8000-000000000041',
  'webhook.agent', 'agent-default', '{"fixture":true}'
);

UPDATE runs
SET output = '{"ok":true}',
    status = 'success',
    dispatch_state = 'terminal',
    duration_ms = 1000,
    finished_at = clock_timestamp(),
    active_attempt_id = NULL,
    lease_id = NULL,
    executor_type = NULL,
    active_core_instance_id = NULL,
    runtime_node_id = NULL,
    runtime_worker_id = NULL,
    runtime_session_id = NULL,
    lease_token_id = NULL,
    lease_offered_at = NULL,
    lease_accepted_at = NULL,
    lease_expires_at = NULL,
    attempt_deadline_at = NULL,
    result_id = '10000000-0000-4000-8000-000000000040',
    result_fingerprint = decode(repeat('44', 32), 'hex'),
    terminal_event_id = '10000000-0000-4000-8000-000000000041'
WHERE id = '10000000-0000-4000-8000-000000000010';
COMMIT;

INSERT INTO run_event_retention_watermarks (
  run_id, retained_through_sequence, updated_at
) VALUES (
  '10000000-0000-4000-8000-000000000010', 1, '2000-01-01T00:00:00Z'
);

UPDATE run_event_retention_watermarks
SET retained_through_sequence = 2,
    updated_at = '2000-01-01T00:00:00Z'
WHERE run_id = '10000000-0000-4000-8000-000000000010';

DO $$
BEGIN
  IF (
    SELECT retained_through_sequence <> 2
           OR updated_at <= '2020-01-01T00:00:00Z'::timestamptz
    FROM run_event_retention_watermarks
    WHERE run_id = '10000000-0000-4000-8000-000000000010'
  ) THEN
    RAISE EXCEPTION 'retention watermark did not advance with DB-clock evidence';
  END IF;

  IF (
    SELECT COUNT(*)
    FROM run_events e
    LEFT JOIN run_event_retention_watermarks w ON w.run_id = e.run_id
    WHERE e.run_id = '10000000-0000-4000-8000-000000000010'
      AND e.sequence > COALESCE(w.retained_through_sequence, 0)
  ) <> 1 THEN
    RAISE EXCEPTION 'logical retention watermark did not hide retained events';
  END IF;
END
$$;

INSERT INTO webhook_deliveries (
  id, agent_id, run_id, url, payload, status, next_retry_at, effect_outbox_id
) VALUES (
  '10000000-0000-4000-8000-000000000043',
  '10000000-0000-4000-8000-000000000002',
  '10000000-0000-4000-8000-000000000010',
  'https://example.test/runtime-v2-webhook', '{}', 'pending',
  clock_timestamp(), '10000000-0000-4000-8000-000000000042'
);

INSERT INTO runs (
  id, user_id, agent_id, input, status, cost_cents,
  platform_fee_cents, creator_revenue_cents, source,
  idempotency_key_hash, idempotency_fingerprint,
  connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
) VALUES (
  '10000000-0000-4000-8000-000000000050',
  '10000000-0000-4000-8000-000000000001',
  '10000000-0000-4000-8000-000000000002',
  '{"prompt":"cancel"}', 'running', 100, 20, 80, 'web',
  decode(repeat('55', 32), 'hex'), decode(repeat('66', 32), 'hex'),
  'runtime_ws', clock_timestamp() + interval '2 minutes',
  clock_timestamp() + interval '10 minutes'
);

BEGIN;
INSERT INTO run_cancellations (
  id, run_id, state, requested_by_type, requested_by_id, reason
) VALUES (
  '10000000-0000-4000-8000-000000000051',
  '10000000-0000-4000-8000-000000000050',
  'requested', 'user', '10000000-0000-4000-8000-000000000001',
  'migration invariant fixture'
);
INSERT INTO run_events (id, run_id, sequence, event_type, payload)
VALUES (
  '10000000-0000-4000-8000-000000000054',
  '10000000-0000-4000-8000-000000000050',
  1, 'run.canceled', '{"status":"canceled","terminal":true}'
);
INSERT INTO run_accounting_ledger (
  run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
) VALUES (
  '10000000-0000-4000-8000-000000000050',
  '10000000-0000-4000-8000-000000000054',
  '10000000-0000-4000-8000-000000000002', 0, 0
);
UPDATE runs
SET status = 'canceled',
    dispatch_state = 'terminal',
    error_code = 'RUN_CANCEL_REQUESTED',
    duration_ms = 1,
    finished_at = clock_timestamp(),
    terminal_event_id = '10000000-0000-4000-8000-000000000054',
    cancel_request_id = '10000000-0000-4000-8000-000000000051',
    cancel_state = 'requested',
    cancel_requested_at = (
      SELECT requested_at FROM run_cancellations
      WHERE id = '10000000-0000-4000-8000-000000000051'
    ),
    cancel_reason = (
      SELECT reason FROM run_cancellations
      WHERE id = '10000000-0000-4000-8000-000000000051'
    )
WHERE id = '10000000-0000-4000-8000-000000000050';
COMMIT;

BEGIN;
UPDATE run_cancellations
SET state = 'stopped', stopped_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE id = '10000000-0000-4000-8000-000000000051';
UPDATE runs
SET cancel_state = 'stopped'
WHERE id = '10000000-0000-4000-8000-000000000050';
COMMIT;

DO $$
BEGIN
  IF (SELECT COUNT(*) FROM run_attempts) <> 1
     OR (SELECT COUNT(*) FROM run_accounting_ledger) <> 2
     OR (SELECT COUNT(*) FROM run_effect_outbox) <> 1
     OR (SELECT COUNT(*) FROM webhook_deliveries WHERE effect_outbox_id IS NOT NULL) <> 1 THEN
    RAISE EXCEPTION 'runtime v2 terminal evidence is incomplete';
  END IF;
END
$$;
SQL

  psql_stdin --quiet <<'SQL'
BEGIN;
INSERT INTO runs (
  id, user_id, agent_id, input, status, cost_cents,
  platform_fee_cents, creator_revenue_cents, source,
  idempotency_key_hash, idempotency_fingerprint,
  connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
) VALUES (
  '10000000-0000-4000-8000-0000000000e0',
  '10000000-0000-4000-8000-000000000001',
  '10000000-0000-4000-8000-000000000002',
  '{"case":"deadline-wins-after-result"}', 'running', 100, 20, 80, 'api',
  decode(repeat('e0', 32), 'hex'), decode(repeat('e1', 32), 'hex'),
  'runtime_ws', transaction_timestamp() + interval '2 minutes',
  transaction_timestamp() + interval '10 minutes'
);

INSERT INTO run_attempts (
  id, run_id, agent_id, offer_no, attempt_no, executor_type,
  lease_id, fencing_token, offered_by_core_instance_id,
  attached_core_instance_id, offered_at, offer_expires_at, accepted_at,
  lease_expires_at, attempt_deadline_at, finished_at, outcome,
  result_id, result_fingerprint, result_classification,
  result_acknowledged_at, last_client_event_seq, final_client_event_seq
) VALUES (
  '10000000-0000-4000-8000-0000000000e1',
  '10000000-0000-4000-8000-0000000000e0',
  '10000000-0000-4000-8000-000000000002',
  1, 1, 'core_http',
  '10000000-0000-4000-8000-0000000000e2', 1,
  '10000000-0000-4000-8000-000000000007',
  '10000000-0000-4000-8000-000000000007',
  transaction_timestamp(), transaction_timestamp() + interval '30 seconds',
  transaction_timestamp(), transaction_timestamp() + interval '60 seconds',
  transaction_timestamp() + interval '5 minutes', transaction_timestamp(),
  'timeout', '10000000-0000-4000-8000-0000000000e5',
  decode(repeat('e5', 32), 'hex'), 'success', transaction_timestamp(), 1, 3
);

INSERT INTO run_events (
  id, run_id, sequence, event_type, payload,
  client_event_id, client_event_seq, payload_fingerprint,
  attempt_id, attempt_no, fencing_token
) VALUES (
  '10000000-0000-4000-8000-0000000000e3',
  '10000000-0000-4000-8000-0000000000e0',
  1, 'run.progress', '{"progress":90}',
  '10000000-0000-4000-8000-0000000000e6', 1,
  decode(repeat('e6', 32), 'hex'),
  '10000000-0000-4000-8000-0000000000e1', 1, 1
);

INSERT INTO run_events (id, run_id, sequence, event_type, payload)
VALUES (
  '10000000-0000-4000-8000-0000000000e4',
  '10000000-0000-4000-8000-0000000000e0',
  2, 'run.failed', '{"status":"timeout","terminal":true}'
);

INSERT INTO run_accounting_ledger (
  run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
) VALUES (
  '10000000-0000-4000-8000-0000000000e0',
  '10000000-0000-4000-8000-0000000000e4',
  '10000000-0000-4000-8000-000000000002', 0, 0
);

UPDATE runs
SET status = 'timeout', dispatch_state = 'terminal',
    error_code = 'RUN_DEADLINE_EXCEEDED', duration_ms = 9000,
    finished_at = transaction_timestamp(), offer_count = 1,
    attempt_count = 1, fencing_token = 1,
    latest_attempt_id = '10000000-0000-4000-8000-0000000000e1',
    terminal_event_id = '10000000-0000-4000-8000-0000000000e4'
WHERE id = '10000000-0000-4000-8000-0000000000e0';

SET CONSTRAINTS ALL IMMEDIATE;
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM runs
    WHERE id = '10000000-0000-4000-8000-0000000000e0'
      AND (result_id IS NOT NULL OR result_fingerprint IS NOT NULL OR output IS NOT NULL)
  ) OR NOT EXISTS (
    SELECT 1
    FROM run_attempts
    WHERE id = '10000000-0000-4000-8000-0000000000e1'
      AND outcome = 'timeout'
      AND result_id = '10000000-0000-4000-8000-0000000000e5'
      AND result_classification = 'success'
      AND last_client_event_seq = 1
      AND final_client_event_seq = 3
  ) THEN
    RAISE EXCEPTION 'deadline-wins Result evidence was not private to Attempt';
  END IF;
END
$$;
ROLLBACK;
SQL

  LOCK_ORDER_OUTPUT_FILE="$(mktemp)"
  docker exec \
    --env PGAPPNAME="runtime-heartbeat-lock-order" \
    "$CONTAINER_NAME" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d "$DATABASE_NAME" -c "
      SET deadlock_timeout = '100ms';
      SET lock_timeout = '4s';
      BEGIN;
      SELECT node_id
      FROM runtime_nodes
      WHERE node_id = '10000000-0000-4000-8000-000000000005'
      FOR UPDATE;
      SELECT pg_sleep(1);
      SELECT runtime_session_id
      FROM runtime_sessions
      WHERE runtime_session_id = '10000000-0000-4000-8000-000000000006'
      FOR UPDATE;
      COMMIT;
    " >"$LOCK_ORDER_OUTPUT_FILE" 2>&1 &
  local lock_order_pid=$!

  for _ in $(seq 1 50); do
    if [[ "$(psql_command --tuples-only --no-align -c "
      SELECT COUNT(*)
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND application_name = 'runtime-heartbeat-lock-order'
        AND wait_event_type = 'Timeout'
    ")" == "1" ]]; then
      break
    fi
    sleep 0.05
  done

  psql_command --quiet -c "
    SET deadlock_timeout = '100ms';
    SET lock_timeout = '4s';
    BEGIN;
    UPDATE runtime_sessions
    SET heartbeat_at = clock_timestamp(), updated_at = clock_timestamp()
    WHERE runtime_session_id = '10000000-0000-4000-8000-000000000006';
    SELECT pg_sleep(1.5);
    COMMIT;
  " >/dev/null

  if ! wait "$lock_order_pid"; then
    local lock_order_output
    lock_order_output="$(<"$LOCK_ORDER_OUTPUT_FILE")"
    fail "Session/Node lock order regression failed: $lock_order_output"
  fi
  rm -f "$LOCK_ORDER_OUTPUT_FILE"
  LOCK_ORDER_OUTPUT_FILE=""

  psql_command --quiet -c "
    INSERT INTO agent_tokens (
      id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
      status, redeemed_at
    ) VALUES (
      '10000000-0000-4000-8000-000000000090',
      '10000000-0000-4000-8000-000000000002',
      '10000000-0000-4000-8000-000000000001',
      'Runtime V2 Race Token', 'ol_agent_eeff0011', 'race-token-hash',
      ARRAY['agent:call', 'agent:pull'], 'active_runtime', clock_timestamp()
    );
    INSERT INTO runtime_nodes (
      node_id, display_name, device_certificate_serial,
      device_public_key_thumbprint, node_version, protocol_version,
      runtime_contract_id, runtime_contract_digest, features, capacity,
      last_seen_at
    ) VALUES (
      '10000000-0000-4000-8000-000000000091',
      'Runtime V2 Race Node', 'serial-runtime-race', 'thumbprint-runtime-race',
      '0.2.0-test', 2, 'openlinker.runtime.v2',
      'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
      ARRAY[
        'lease_fence', 'assignment_confirm', 'renew', 'resume',
        'event_ack', 'result_ack', 'cancel', 'persistent_spool'
      ], 1, clock_timestamp()
    )
  "

  docker exec --detach \
    --env PGAPPNAME="runtime-session-revoke-race" \
    "$CONTAINER_NAME" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d "$DATABASE_NAME" -c "
      BEGIN;
      INSERT INTO runtime_sessions (
        runtime_session_id, node_id, agent_id, credential_id, worker_id,
        session_epoch, device_certificate_serial, node_version,
        protocol_version, runtime_contract_id, runtime_contract_digest,
        features, capacity, attached_core_instance_id
      ) VALUES (
        '10000000-0000-4000-8000-000000000092',
        '10000000-0000-4000-8000-000000000091',
        '10000000-0000-4000-8000-000000000002',
        '10000000-0000-4000-8000-000000000090',
        'worker-runtime-race', 1, 'serial-runtime-race', '0.2.0-test', 2,
        'openlinker.runtime.v2',
        'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
        ARRAY[
          'lease_fence', 'assignment_confirm', 'renew', 'resume',
          'event_ack', 'result_ack', 'cancel', 'persistent_spool'
        ], 1, '10000000-0000-4000-8000-000000000093'
      );
      INSERT INTO runtime_session_attachments (
        id, runtime_session_id, core_instance_id, attachment_kind
      ) VALUES (
        '10000000-0000-4000-8000-000000000094',
        '10000000-0000-4000-8000-000000000092',
        '10000000-0000-4000-8000-000000000093', 'connected'
      );
      SELECT pg_sleep(2);
      COMMIT;
    " >/dev/null

  for _ in $(seq 1 50); do
    if [[ "$(psql_command --tuples-only --no-align -c "
      SELECT COUNT(*)
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND application_name = 'runtime-session-revoke-race'
    ")" == "1" ]]; then
      break
    fi
    sleep 0.1
  done

  expect_sql_failure \
    "runtime credential sessions must close before token revocation" \
    "UPDATE agent_tokens
     SET status = 'revoked', revoked_at = clock_timestamp(), revocation_kind = 'security'
     WHERE id = '10000000-0000-4000-8000-000000000090'"

  psql_command --quiet -c "
    BEGIN;
    UPDATE runtime_session_attachments
    SET detached_at = clock_timestamp(), disconnect_reason = 'TOKEN_REVOKED'
    WHERE runtime_session_id = '10000000-0000-4000-8000-000000000092'
      AND detached_at IS NULL;
    UPDATE runtime_sessions
    SET status = 'revoked',
        attached_core_instance_id = NULL,
        disconnected_at = clock_timestamp(),
        updated_at = clock_timestamp()
    WHERE runtime_session_id = '10000000-0000-4000-8000-000000000092';
    UPDATE runtime_nodes
    SET status = 'revoked',
        revoked_at = clock_timestamp(),
        revoke_reason = 'TOKEN_REVOKED',
        updated_at = clock_timestamp()
    WHERE node_id = '10000000-0000-4000-8000-000000000091';
    UPDATE agent_tokens
    SET status = 'revoked',
        revoked_at = clock_timestamp(),
        revocation_kind = 'security'
    WHERE id = '10000000-0000-4000-8000-000000000090';
    COMMIT;
  "

  expect_sql_failure \
    "runtime node lifecycle cannot move backwards" \
    "UPDATE runtime_nodes
     SET status = 'active', revoked_at = NULL, revoke_reason = NULL
     WHERE node_id = '10000000-0000-4000-8000-000000000091'"
  expect_sql_failure \
    "agent token lifecycle cannot move backwards" \
    "UPDATE agent_tokens
     SET status = 'active_runtime', revoked_at = NULL, revocation_kind = NULL
     WHERE id = '10000000-0000-4000-8000-000000000090'"

  psql_command --quiet -c "
    INSERT INTO agent_tokens (
      id, agent_id, creator_user_id, name, prefix, token_hash, scopes,
      status, expires_at
    ) VALUES (
      '10000000-0000-4000-8000-000000000095', NULL,
      '10000000-0000-4000-8000-000000000001',
      'Pending Redemption', 'ol_agent_abcdef12', 'registration-token-hash',
      ARRAY['agent:call'], 'pending_registration',
      clock_timestamp() + interval '10 minutes'
    );
    UPDATE agent_tokens
    SET agent_id = '10000000-0000-4000-8000-000000000002',
        scopes = ARRAY['agent:call', 'agent:pull'],
        status = 'active_runtime', redeemed_at = clock_timestamp(),
        expires_at = NULL, token_hash = 'redeemed-runtime-token-hash'
    WHERE id = '10000000-0000-4000-8000-000000000095';
  "

  expect_sql_failure \
    "runtime v2 run insert requires complete v2 creation identity" \
    "INSERT INTO runs (
      user_id, agent_id, input, status, cost_cents,
      platform_fee_cents, creator_revenue_cents, source
    ) VALUES (
      '10000000-0000-4000-8000-000000000001',
      '10000000-0000-4000-8000-000000000002',
      '{\"legacy\":true}', 'running', 100, 20, 80, 'web'
    )"
  expect_sql_failure \
    "run runtime contract is immutable" \
    "UPDATE runs
     SET runtime_contract_id = 'legacy.pre-v2'
     WHERE id = '10000000-0000-4000-8000-000000000050'"
  expect_sql_failure \
    "run attempt immutable identity cannot change" \
    "UPDATE run_attempts SET lease_id = gen_random_uuid() WHERE id = '10000000-0000-4000-8000-000000000020'"
  expect_sql_failure \
    "run attempt result identity is immutable" \
    "UPDATE run_attempts
     SET result_id = gen_random_uuid()
     WHERE id = '10000000-0000-4000-8000-000000000020'"
  expect_sql_failure \
    "run offer, attempt, or fence counters do not match attempt history" \
    "INSERT INTO run_attempts (
      id, run_id, agent_id, offer_no, executor_type, lease_id, fencing_token,
      offered_by_core_instance_id, attached_core_instance_id,
      offered_at, offer_expires_at, lease_expires_at, attempt_deadline_at
    ) VALUES (
      '10000000-0000-4000-8000-000000000052',
      '10000000-0000-4000-8000-000000000050',
      '10000000-0000-4000-8000-000000000002',
      1, 'core_http', '10000000-0000-4000-8000-000000000053', 1,
      '10000000-0000-4000-8000-000000000007',
      '10000000-0000-4000-8000-000000000007',
      clock_timestamp(), clock_timestamp() + interval '30 seconds',
      clock_timestamp() + interval '60 seconds',
      clock_timestamp() + interval '5 minutes'
    )"
  expect_sql_failure \
    "terminal cancellation state cannot change" \
    "UPDATE run_cancellations SET state = 'requested', updated_at = clock_timestamp() WHERE id = '10000000-0000-4000-8000-000000000051'"
  expect_sql_failure \
    "cancellation request identity is immutable" \
    "UPDATE run_cancellations
     SET reason = 'rewritten cancellation request'
     WHERE id = '10000000-0000-4000-8000-000000000051'"
  expect_sql_failure \
    "runtime session credential principal mismatch" \
    "INSERT INTO runtime_sessions (
      runtime_session_id, node_id, agent_id, credential_id, worker_id,
      session_epoch, device_certificate_serial, node_version,
      protocol_version, runtime_contract_id, runtime_contract_digest,
      features, capacity
    ) VALUES (
      '10000000-0000-4000-8000-000000000060',
      '10000000-0000-4000-8000-000000000005',
      '10000000-0000-4000-8000-000000000003',
      '10000000-0000-4000-8000-000000000004',
      'wrong-principal', 1, 'serial-runtime-v2', '0.2.0-test', 2,
      'openlinker.runtime.v2',
      'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
      ARRAY[
        'lease_fence', 'assignment_confirm', 'renew', 'resume',
        'event_ack', 'result_ack', 'cancel', 'persistent_spool'
      ], 1
    )"
  expect_sql_failure \
    "runtime_nodes_contract_current" \
    "INSERT INTO runtime_nodes (
      node_id, display_name, device_certificate_serial,
      device_public_key_thumbprint, node_version, protocol_version,
      runtime_contract_id, runtime_contract_digest, features, capacity
    ) VALUES (
      '10000000-0000-4000-8000-000000000061',
      'Wrong Contract Node', 'wrong-contract-serial', 'wrong-contract-thumbprint',
      '0.1.0', 1, 'unknown.runtime',
      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      ARRAY[
        'lease_fence', 'assignment_confirm', 'renew', 'resume',
        'event_ack', 'result_ack', 'cancel', 'persistent_spool'
      ], 1
    )"
  expect_sql_failure \
    "runtime session node contract identity mismatch" \
    "INSERT INTO runtime_sessions (
      runtime_session_id, node_id, agent_id, credential_id, worker_id,
      session_epoch, device_certificate_serial, node_version,
      protocol_version, runtime_contract_id, runtime_contract_digest,
      features, capacity, attached_core_instance_id
    ) VALUES (
      '10000000-0000-4000-8000-000000000062',
      '10000000-0000-4000-8000-000000000005',
      '10000000-0000-4000-8000-000000000002',
      '10000000-0000-4000-8000-000000000004',
      'wrong-contract-session', 2, 'serial-runtime-v2', '0.2.0-test', 1,
      'openlinker.runtime.v2',
      'd83e011870cf40bf67723fac1c58ca785d37954bf83638b8f67f69240d20dd4f',
      ARRAY[
        'lease_fence', 'assignment_confirm', 'renew', 'resume',
        'event_ack', 'result_ack', 'cancel', 'persistent_spool'
      ], 1, '10000000-0000-4000-8000-000000000007'
    )"
  expect_sql_failure \
    "runtime credential sessions must close before token revocation" \
    "UPDATE agent_tokens
     SET status = 'revoked', revoked_at = clock_timestamp(), revocation_kind = 'security'
     WHERE id = '10000000-0000-4000-8000-000000000004'"
  expect_sql_failure \
    "runtime node sessions must close before node revocation" \
    "UPDATE runtime_nodes
     SET status = 'revoked', revoked_at = clock_timestamp(), revoke_reason = 'security'
     WHERE node_id = '10000000-0000-4000-8000-000000000005'"
  expect_sql_failure \
    "runtime node device identity is immutable" \
    "UPDATE runtime_nodes
     SET device_public_key_thumbprint = 'rewritten-thumbprint'
     WHERE node_id = '10000000-0000-4000-8000-000000000005'"
  expect_sql_failure \
    "redeemed agent token credential identity is immutable" \
    "UPDATE agent_tokens
     SET token_hash = 'rewritten-token-hash'
     WHERE id = '10000000-0000-4000-8000-000000000004'"
  expect_sql_failure \
    "terminal Run must have exactly one terminal event" \
    "INSERT INTO run_events (id, run_id, sequence, event_type, payload)
     VALUES (
       '10000000-0000-4000-8000-000000000074',
       '10000000-0000-4000-8000-000000000010',
       4, 'run.failed', '{\"status\":\"failed\",\"terminal\":true}'
     )"
  psql_command --quiet -c "
    UPDATE run_effect_outbox
    SET status = 'succeeded', completed_at = clock_timestamp()
    WHERE id = '10000000-0000-4000-8000-000000000042'
  "
  expect_sql_failure \
    "Run effect delivery identity is immutable" \
    "UPDATE run_effect_outbox
     SET target_key = 'rewritten-target', metadata = '{\"rewritten\":true}'
     WHERE id = '10000000-0000-4000-8000-000000000042'"
  expect_sql_failure \
    "Run effect outbox history cannot be deleted" \
    "DELETE FROM run_effect_outbox
     WHERE id = '10000000-0000-4000-8000-000000000042'"
  expect_sql_failure \
    "Run effect outbox does not match the Run terminal event" \
    "INSERT INTO run_effect_outbox (
       id, run_id, terminal_event_id, effect_type, target_key, metadata
     ) VALUES (
       '10000000-0000-4000-8000-000000000044',
       '10000000-0000-4000-8000-000000000010',
       '10000000-0000-4000-8000-000000000030',
       'webhook.agent', 'wrong-terminal-event', '{}'
     )"
  expect_sql_failure \
    "run_effect_outbox_dead_letter_consistent" \
    "UPDATE run_effect_outbox
     SET status = 'dead_letter', completed_at = NULL
     WHERE id = '10000000-0000-4000-8000-000000000042'"
  expect_sql_failure \
    "run_effect_outbox_last_error_len" \
    "UPDATE run_effect_outbox
     SET last_error = repeat('x', 501)
     WHERE id = '10000000-0000-4000-8000-000000000042'"

  psql_stdin --quiet <<'SQL'
UPDATE run_effect_outbox
SET status = 'processing', completed_at = NULL,
    lease_owner = '10000000-0000-4000-8000-0000000000f1',
    lease_expires_at = clock_timestamp() + interval '1 minute',
    attempt_count = 2
WHERE id = '10000000-0000-4000-8000-000000000042';

DO $$
DECLARE
  stale_owner_rows INTEGER;
  stale_attempt_rows INTEGER;
BEGIN
  UPDATE run_effect_outbox
  SET status = 'dead_letter', lease_owner = NULL, lease_expires_at = NULL,
      dead_lettered_at = clock_timestamp(), last_error = 'stale owner'
  WHERE id = '10000000-0000-4000-8000-000000000042'
    AND status = 'processing'
    AND lease_owner = '10000000-0000-4000-8000-0000000000ff'
    AND attempt_count = 2;
  GET DIAGNOSTICS stale_owner_rows = ROW_COUNT;

  UPDATE run_effect_outbox
  SET status = 'dead_letter', lease_owner = NULL, lease_expires_at = NULL,
      dead_lettered_at = clock_timestamp(), last_error = 'stale attempt'
  WHERE id = '10000000-0000-4000-8000-000000000042'
    AND status = 'processing'
    AND lease_owner = '10000000-0000-4000-8000-0000000000f1'
    AND attempt_count = 1;
  GET DIAGNOSTICS stale_attempt_rows = ROW_COUNT;

  IF stale_owner_rows <> 0 OR stale_attempt_rows <> 0 THEN
    RAISE EXCEPTION 'stale effect worker bypassed owner/attempt fence';
  END IF;
END
$$;

UPDATE run_effect_outbox
SET status = 'dead_letter', lease_owner = NULL, lease_expires_at = NULL,
    completed_at = NULL, dead_lettered_at = clock_timestamp(),
    last_error = 'permanent delivery failure'
WHERE id = '10000000-0000-4000-8000-000000000042'
  AND status = 'processing'
  AND lease_owner = '10000000-0000-4000-8000-0000000000f1'
  AND attempt_count = 2;
SQL

  expect_sql_failure \
    "run_effect_replays_actor_type_valid" \
    "WITH target AS (
       SELECT id FROM run_effect_outbox
       WHERE id = '10000000-0000-4000-8000-000000000042'
         AND status = 'dead_letter'
       FOR UPDATE
     ), replay_audit AS (
       INSERT INTO run_effect_replays (effect_outbox_id, actor_type, actor_id, reason)
       SELECT id, '', NULL, 'operator replay' FROM target
       RETURNING effect_outbox_id
     )
     UPDATE run_effect_outbox effect
     SET status = 'pending', available_at = clock_timestamp(),
         attempt_count = 0, dead_lettered_at = NULL, last_error = NULL
     FROM replay_audit
     WHERE effect.id = replay_audit.effect_outbox_id"
  expect_sql_failure \
    "run_effect_replays_reason_len" \
    "WITH target AS (
       SELECT id FROM run_effect_outbox
       WHERE id = '10000000-0000-4000-8000-000000000042'
         AND status = 'dead_letter'
       FOR UPDATE
     ), replay_audit AS (
       INSERT INTO run_effect_replays (effect_outbox_id, actor_type, actor_id, reason)
       SELECT id, 'admin', NULL, '   ' FROM target
       RETURNING effect_outbox_id
     )
     UPDATE run_effect_outbox effect
     SET status = 'pending', available_at = clock_timestamp(),
         attempt_count = 0, dead_lettered_at = NULL, last_error = NULL
     FROM replay_audit
     WHERE effect.id = replay_audit.effect_outbox_id"

  psql_stdin --quiet <<'SQL'
WITH target AS (
  SELECT id
  FROM run_effect_outbox
  WHERE id = '10000000-0000-4000-8000-000000000042'
    AND status = 'dead_letter'
  FOR UPDATE
), replay_audit AS (
  INSERT INTO run_effect_replays (effect_outbox_id, actor_type, actor_id, reason)
  SELECT id, 'admin', '10000000-0000-4000-8000-000000000001',
         'operator approved replay'
  FROM target
  RETURNING effect_outbox_id
)
UPDATE run_effect_outbox effect
SET status = 'pending', available_at = clock_timestamp(),
    lease_owner = NULL, lease_expires_at = NULL, attempt_count = 0,
    completed_at = NULL, dead_lettered_at = NULL, last_error = NULL
FROM replay_audit
WHERE effect.id = replay_audit.effect_outbox_id;

INSERT INTO run_effect_outbox (
  id, run_id, terminal_event_id, effect_type, target_key, metadata, max_attempts
) VALUES (
  '10000000-0000-4000-8000-0000000000f2',
  '10000000-0000-4000-8000-000000000010',
  '10000000-0000-4000-8000-000000000041',
  'webhook.agent', 'expired-at-limit', '{}', 1
);
UPDATE run_effect_outbox
SET status = 'processing',
    lease_owner = '10000000-0000-4000-8000-0000000000f3',
    lease_expires_at = clock_timestamp() - interval '1 second',
    attempt_count = 1
WHERE id = '10000000-0000-4000-8000-0000000000f2';
UPDATE run_effect_outbox
SET status = 'dead_letter', lease_owner = NULL, lease_expires_at = NULL,
    completed_at = NULL, dead_lettered_at = clock_timestamp(),
    last_error = COALESCE(last_error, 'processing lease expired at retry limit')
WHERE status = 'processing'
  AND lease_expires_at <= clock_timestamp()
  AND attempt_count >= max_attempts;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM run_effect_outbox
    WHERE id = '10000000-0000-4000-8000-000000000042'
      AND status = 'pending'
      AND attempt_count = 0
      AND dead_lettered_at IS NULL
  ) OR (
    SELECT COUNT(*)
    FROM run_effect_replays
    WHERE effect_outbox_id = '10000000-0000-4000-8000-000000000042'
      AND actor_type = 'admin'
      AND reason = 'operator approved replay'
  ) <> 1 OR NOT EXISTS (
    SELECT 1
    FROM run_effect_outbox
    WHERE id = '10000000-0000-4000-8000-0000000000f2'
      AND status = 'dead_letter'
      AND dead_lettered_at IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'effect dead-letter/replay lifecycle evidence is incomplete';
  END IF;
END
$$;
SQL

  expect_sql_failure \
    "Run effect replay audit is immutable" \
    "UPDATE run_effect_replays
     SET reason = 'rewritten replay reason'
     WHERE effect_outbox_id = '10000000-0000-4000-8000-000000000042'"
  expect_sql_failure \
    "Run effect replay audit is immutable" \
    "DELETE FROM run_effect_replays
     WHERE effect_outbox_id = '10000000-0000-4000-8000-000000000042'"
  expect_sql_failure \
    "terminal Run accounting ledger is missing or inconsistent" \
    "BEGIN;
     INSERT INTO runs (
       id, user_id, agent_id, input, status, cost_cents,
       platform_fee_cents, creator_revenue_cents, source,
       idempotency_key_hash, idempotency_fingerprint,
       connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
     ) VALUES (
       '10000000-0000-4000-8000-000000000070',
       '10000000-0000-4000-8000-000000000001',
       '10000000-0000-4000-8000-000000000002',
       '{\"missing\":\"ledger\"}', 'running', 100, 20, 80, 'api',
       decode(repeat('77', 32), 'hex'), decode(repeat('88', 32), 'hex'),
       'runtime_ws', clock_timestamp() + interval '2 minutes',
       clock_timestamp() + interval '10 minutes'
     );
     INSERT INTO run_events (id, run_id, sequence, event_type, payload)
     VALUES (
       '10000000-0000-4000-8000-000000000071',
       '10000000-0000-4000-8000-000000000070',
       1, 'run.failed', '{\"status\":\"timeout\",\"terminal\":true}'
     );
     UPDATE runs
     SET status = 'timeout', dispatch_state = 'terminal',
         error_code = 'RUN_DEADLINE_EXCEEDED', duration_ms = 1,
         finished_at = clock_timestamp(),
         terminal_event_id = '10000000-0000-4000-8000-000000000071'
     WHERE id = '10000000-0000-4000-8000-000000000070';
     COMMIT"
  expect_sql_failure \
    "timeout or canceled Run contradicts its latest attempt" \
    "BEGIN;
     INSERT INTO runs (
       id, user_id, agent_id, input, status, cost_cents,
       platform_fee_cents, creator_revenue_cents, source,
       idempotency_key_hash, idempotency_fingerprint,
       connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
     ) VALUES (
       '10000000-0000-4000-8000-0000000000a0',
       '10000000-0000-4000-8000-000000000001',
       '10000000-0000-4000-8000-000000000002',
       '{\"invalid\":\"timeout-over-success\"}', 'running', 100, 20, 80, 'api',
       decode(repeat('a1', 32), 'hex'), decode(repeat('a2', 32), 'hex'),
       'runtime_ws', transaction_timestamp() + interval '2 minutes',
       transaction_timestamp() + interval '10 minutes'
     );
     INSERT INTO run_attempts (
       id, run_id, agent_id, offer_no, attempt_no, executor_type,
       lease_id, fencing_token, offered_by_core_instance_id,
       attached_core_instance_id, offered_at, offer_expires_at, accepted_at,
       lease_expires_at, attempt_deadline_at, finished_at, outcome,
       result_id, result_fingerprint, result_classification,
       final_client_event_seq
     ) VALUES (
       '10000000-0000-4000-8000-0000000000a1',
       '10000000-0000-4000-8000-0000000000a0',
       '10000000-0000-4000-8000-000000000002',
       1, 1, 'core_http',
       '10000000-0000-4000-8000-0000000000a2', 1,
       '10000000-0000-4000-8000-000000000007',
       '10000000-0000-4000-8000-000000000007',
       transaction_timestamp(), transaction_timestamp() + interval '30 seconds',
       transaction_timestamp(), transaction_timestamp() + interval '60 seconds',
       transaction_timestamp() + interval '5 minutes', transaction_timestamp(),
       'success', '10000000-0000-4000-8000-0000000000a3',
       decode(repeat('a3', 32), 'hex'), 'success', 0
     );
     INSERT INTO run_events (id, run_id, sequence, event_type, payload)
     VALUES (
       '10000000-0000-4000-8000-0000000000a4',
       '10000000-0000-4000-8000-0000000000a0',
       1, 'run.failed', '{\"status\":\"timeout\",\"terminal\":true}'
     );
     INSERT INTO run_accounting_ledger (
       run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
     ) VALUES (
       '10000000-0000-4000-8000-0000000000a0',
       '10000000-0000-4000-8000-0000000000a4',
       '10000000-0000-4000-8000-000000000002', 0, 0
     );
     UPDATE runs
     SET status = 'timeout', dispatch_state = 'terminal',
         error_code = 'RUN_DEADLINE_EXCEEDED', duration_ms = 1,
         finished_at = transaction_timestamp(), offer_count = 1,
         attempt_count = 1, fencing_token = 1,
         latest_attempt_id = '10000000-0000-4000-8000-0000000000a1',
         terminal_event_id = '10000000-0000-4000-8000-0000000000a4'
     WHERE id = '10000000-0000-4000-8000-0000000000a0';
     COMMIT"
  expect_sql_failure \
    "canceled Run target attempt was not ended as canceled" \
    "BEGIN;
     INSERT INTO runs (
       id, user_id, agent_id, input, status, cost_cents,
       platform_fee_cents, creator_revenue_cents, source,
       idempotency_key_hash, idempotency_fingerprint,
       connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
     ) VALUES (
       '10000000-0000-4000-8000-0000000000b0',
       '10000000-0000-4000-8000-000000000001',
       '10000000-0000-4000-8000-000000000002',
       '{\"invalid\":\"cancel-over-success\"}', 'running', 100, 20, 80, 'api',
       decode(repeat('b1', 32), 'hex'), decode(repeat('b2', 32), 'hex'),
       'runtime_ws', transaction_timestamp() + interval '2 minutes',
       transaction_timestamp() + interval '10 minutes'
     );
     INSERT INTO run_attempts (
       id, run_id, agent_id, offer_no, attempt_no, executor_type,
       lease_id, fencing_token, offered_by_core_instance_id,
       attached_core_instance_id, offered_at, offer_expires_at, accepted_at,
       lease_expires_at, attempt_deadline_at, finished_at, outcome,
       result_id, result_fingerprint, result_classification,
       final_client_event_seq
     ) VALUES (
       '10000000-0000-4000-8000-0000000000b1',
       '10000000-0000-4000-8000-0000000000b0',
       '10000000-0000-4000-8000-000000000002',
       1, 1, 'core_http',
       '10000000-0000-4000-8000-0000000000b2', 1,
       '10000000-0000-4000-8000-000000000007',
       '10000000-0000-4000-8000-000000000007',
       transaction_timestamp(), transaction_timestamp() + interval '30 seconds',
       transaction_timestamp(), transaction_timestamp() + interval '60 seconds',
       transaction_timestamp() + interval '5 minutes', transaction_timestamp(),
       'success', '10000000-0000-4000-8000-0000000000b3',
       decode(repeat('b3', 32), 'hex'), 'success', 0
     );
     INSERT INTO run_cancellations (
       id, run_id, target_attempt_id, state,
       requested_by_type, requested_by_id, reason
     ) VALUES (
       '10000000-0000-4000-8000-0000000000b4',
       '10000000-0000-4000-8000-0000000000b0',
       '10000000-0000-4000-8000-0000000000b1',
       'requested', 'system', '10000000-0000-4000-8000-000000000007',
       'invalid cancellation over success'
     );
     INSERT INTO run_events (id, run_id, sequence, event_type, payload)
     VALUES (
       '10000000-0000-4000-8000-0000000000b5',
       '10000000-0000-4000-8000-0000000000b0',
       1, 'run.canceled', '{\"status\":\"canceled\",\"terminal\":true}'
     );
     INSERT INTO run_accounting_ledger (
       run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
     ) VALUES (
       '10000000-0000-4000-8000-0000000000b0',
       '10000000-0000-4000-8000-0000000000b5',
       '10000000-0000-4000-8000-000000000002', 0, 0
     );
     UPDATE runs
     SET status = 'canceled', dispatch_state = 'terminal',
         error_code = 'RUN_CANCEL_REQUESTED', duration_ms = 1,
         finished_at = transaction_timestamp(), offer_count = 1,
         attempt_count = 1, fencing_token = 1,
         latest_attempt_id = '10000000-0000-4000-8000-0000000000b1',
         terminal_event_id = '10000000-0000-4000-8000-0000000000b5',
         cancel_request_id = '10000000-0000-4000-8000-0000000000b4',
         cancel_state = 'requested',
         cancel_requested_at = (
           SELECT requested_at FROM run_cancellations
           WHERE id = '10000000-0000-4000-8000-0000000000b4'
         ),
         cancel_reason = 'invalid cancellation over success'
     WHERE id = '10000000-0000-4000-8000-0000000000b0';
     COMMIT"
  expect_sql_failure \
    "Run replay must reference a real dead-letter Run" \
    "INSERT INTO runs (
      id, user_id, agent_id, input, status, cost_cents,
      platform_fee_cents, creator_revenue_cents, source,
      idempotency_key_hash, idempotency_fingerprint,
      connection_mode_snapshot, dispatch_deadline_at, run_deadline_at,
      replay_of_run_id
    ) VALUES (
      '10000000-0000-4000-8000-000000000072',
      '10000000-0000-4000-8000-000000000001',
      '10000000-0000-4000-8000-000000000002',
      '{\"invalid\":\"replay\"}', 'running', 100, 20, 80, 'api',
      decode(repeat('99', 32), 'hex'), decode(repeat('aa', 32), 'hex'),
      'runtime_ws', clock_timestamp() + interval '2 minutes',
      clock_timestamp() + interval '10 minutes',
      '10000000-0000-4000-8000-000000000010'
    )"
  expect_sql_failure \
    "terminal run facts are immutable" \
    "UPDATE runs
      SET error_message = 'attempted terminal rewrite'
      WHERE id = '10000000-0000-4000-8000-000000000050'"

  psql_command --quiet -c "
    INSERT INTO runs (
      id, user_id, agent_id, input, status, cost_cents,
      platform_fee_cents, creator_revenue_cents, source,
      idempotency_key_hash, idempotency_fingerprint,
      connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
    ) VALUES (
      '10000000-0000-4000-8000-000000000075',
      '10000000-0000-4000-8000-000000000001',
      '10000000-0000-4000-8000-000000000002',
      '{\"cancel\":\"instance-lost\"}', 'running', 100, 20, 80, 'web',
      decode(repeat('dd', 32), 'hex'), decode(repeat('ee', 32), 'hex'),
      'runtime_ws', clock_timestamp() + interval '2 minutes',
      clock_timestamp() + interval '10 minutes'
    );
    BEGIN;
    INSERT INTO run_attempts (
      id, run_id, agent_id, offer_no, executor_type, lease_id, fencing_token,
      runtime_token_id, runtime_worker_id, runtime_session_id, node_id,
      offered_by_core_instance_id, attached_core_instance_id,
      offered_at, offer_expires_at, lease_expires_at, attempt_deadline_at
    ) VALUES (
      '10000000-0000-4000-8000-000000000078',
      '10000000-0000-4000-8000-000000000075',
      '10000000-0000-4000-8000-000000000002',
      1, 'agent_node',
      '10000000-0000-4000-8000-000000000079', 1,
      '10000000-0000-4000-8000-000000000004',
      'worker-runtime-v2',
      '10000000-0000-4000-8000-000000000006',
      '10000000-0000-4000-8000-000000000005',
      '10000000-0000-4000-8000-000000000007',
      '10000000-0000-4000-8000-000000000007',
      clock_timestamp(), clock_timestamp() + interval '30 seconds',
      clock_timestamp() + interval '60 seconds',
      clock_timestamp() + interval '5 minutes'
    );
    UPDATE runs
    SET dispatch_state = 'offered', offer_count = 1, attempt_count = 0,
        latest_attempt_id = '10000000-0000-4000-8000-000000000078',
        active_attempt_id = '10000000-0000-4000-8000-000000000078',
        lease_id = '10000000-0000-4000-8000-000000000079',
        fencing_token = 1, executor_type = 'agent_node',
        active_core_instance_id = '10000000-0000-4000-8000-000000000007',
        runtime_node_id = '10000000-0000-4000-8000-000000000005',
        runtime_worker_id = 'worker-runtime-v2',
        runtime_session_id = '10000000-0000-4000-8000-000000000006',
        lease_token_id = '10000000-0000-4000-8000-000000000004',
        lease_offered_at = (
          SELECT offered_at FROM run_attempts
          WHERE id = '10000000-0000-4000-8000-000000000078'
        ),
        lease_expires_at = (
          SELECT lease_expires_at FROM run_attempts
          WHERE id = '10000000-0000-4000-8000-000000000078'
        ),
        attempt_deadline_at = (
          SELECT attempt_deadline_at FROM run_attempts
          WHERE id = '10000000-0000-4000-8000-000000000078'
        )
    WHERE id = '10000000-0000-4000-8000-000000000075';
    COMMIT;
    BEGIN;
    UPDATE run_attempts
    SET finished_at = clock_timestamp(), outcome = 'canceled',
        error_code = 'RUN_CANCEL_REQUESTED'
    WHERE id = '10000000-0000-4000-8000-000000000078';
    INSERT INTO run_cancellations (
      id, run_id, target_attempt_id, state,
      requested_by_type, requested_by_id, reason
    ) VALUES (
      '10000000-0000-4000-8000-000000000076',
      '10000000-0000-4000-8000-000000000075',
      '10000000-0000-4000-8000-000000000078',
      'requested', 'system', '10000000-0000-4000-8000-000000000007',
      'owning instance lost'
    );
    INSERT INTO run_events (id, run_id, sequence, event_type, payload)
    VALUES (
      '10000000-0000-4000-8000-000000000077',
      '10000000-0000-4000-8000-000000000075',
      1, 'run.canceled', '{\"status\":\"canceled\",\"terminal\":true}'
    );
    INSERT INTO run_accounting_ledger (
      run_id, terminal_event_id, agent_id, success_delta, revenue_delta_cents
    ) VALUES (
      '10000000-0000-4000-8000-000000000075',
      '10000000-0000-4000-8000-000000000077',
      '10000000-0000-4000-8000-000000000002', 0, 0
    );
    UPDATE runs
    SET status = 'canceled', dispatch_state = 'terminal',
        error_code = 'RUN_CANCEL_REQUESTED', duration_ms = 1,
        finished_at = clock_timestamp(),
        terminal_event_id = '10000000-0000-4000-8000-000000000077',
        active_attempt_id = NULL,
        lease_id = NULL,
        executor_type = NULL,
        active_core_instance_id = NULL,
        runtime_node_id = NULL,
        runtime_worker_id = NULL,
        runtime_session_id = NULL,
        lease_token_id = NULL,
        lease_offered_at = NULL,
        lease_accepted_at = NULL,
        lease_expires_at = NULL,
        attempt_deadline_at = NULL,
        cancel_request_id = '10000000-0000-4000-8000-000000000076',
        cancel_state = 'requested',
        cancel_requested_at = (
          SELECT requested_at FROM run_cancellations
          WHERE id = '10000000-0000-4000-8000-000000000076'
        ),
        cancel_reason = 'owning instance lost'
    WHERE id = '10000000-0000-4000-8000-000000000075';
    COMMIT;
    BEGIN;
    UPDATE run_cancellations
    SET state = 'unconfirmed', error_code = 'CANCEL_INSTANCE_LOST',
        updated_at = clock_timestamp()
    WHERE id = '10000000-0000-4000-8000-000000000076';
    UPDATE runs
    SET cancel_state = 'unconfirmed'
    WHERE id = '10000000-0000-4000-8000-000000000075';
    COMMIT;
    DO \$\$
    BEGIN
      IF (SELECT delivered_at IS NOT NULL FROM run_cancellations
          WHERE id = '10000000-0000-4000-8000-000000000076') THEN
        RAISE EXCEPTION 'unconfirmed cancellation fabricated delivered evidence';
      END IF;
    END
    \$\$;
    BEGIN;
    UPDATE run_cancellations
    SET state = 'stopped',
        delivered_at = clock_timestamp(),
        stopped_at = clock_timestamp(),
        acknowledged_at = clock_timestamp(),
        error_code = NULL,
        updated_at = clock_timestamp()
    WHERE id = '10000000-0000-4000-8000-000000000076';
    UPDATE runs
    SET cancel_state = 'stopped',
        cancel_acknowledged_at = (
          SELECT acknowledged_at FROM run_cancellations
          WHERE id = '10000000-0000-4000-8000-000000000076'
        )
    WHERE id = '10000000-0000-4000-8000-000000000075';
    COMMIT;
    DO \$\$
    BEGIN
      IF (SELECT status <> 'canceled'
                 OR dispatch_state <> 'terminal'
                 OR terminal_event_id <> '10000000-0000-4000-8000-000000000077'
          FROM runs WHERE id = '10000000-0000-4000-8000-000000000075')
         OR (SELECT COUNT(*) FROM run_accounting_ledger
             WHERE run_id = '10000000-0000-4000-8000-000000000075') <> 1
         OR (SELECT COUNT(*) FROM run_events
             WHERE run_id = '10000000-0000-4000-8000-000000000075'
               AND payload->>'terminal' = 'true') <> 1 THEN
        RAISE EXCEPTION 'late stop evidence rewrote public cancellation terminal facts';
      END IF;
    END
    \$\$;
  "

  psql_command --quiet -c "
    INSERT INTO runs (
      id, user_id, agent_id, input, request_metadata, status, cost_cents,
      platform_fee_cents, creator_revenue_cents, source,
      idempotency_key_hash, idempotency_fingerprint,
      connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
    ) VALUES (
      '10000000-0000-4000-8000-000000000080',
      '10000000-0000-4000-8000-000000000001',
      '10000000-0000-4000-8000-000000000002',
      '{\"pending\":true}', '{\"protocol\":\"rest\",\"method\":\"POST /api/v1/runs\"}',
      'running', 100, 20, 80, 'web',
      decode(repeat('bb', 32), 'hex'), decode(repeat('cc', 32), 'hex'),
      'runtime_ws', clock_timestamp() + interval '2 minutes',
      clock_timestamp() + interval '10 minutes'
    )
  "

  expect_sql_failure \
    "runs_request_metadata_object" \
    "INSERT INTO runs (
      id, user_id, agent_id, input, request_metadata, status, cost_cents,
      platform_fee_cents, creator_revenue_cents, source,
      idempotency_key_hash, idempotency_fingerprint,
      connection_mode_snapshot, dispatch_deadline_at, run_deadline_at
    ) VALUES (
      '10000000-0000-4000-8000-000000000082',
      '10000000-0000-4000-8000-000000000001',
      '10000000-0000-4000-8000-000000000002',
      '{\"invalid\":\"metadata-shape\"}', '[]', 'running', 100, 20, 80, 'api',
      decode(repeat('f1', 32), 'hex'), decode(repeat('f2', 32), 'hex'),
      'runtime_ws', clock_timestamp() + interval '2 minutes',
      clock_timestamp() + interval '10 minutes'
    )"

  expect_sql_failure \
    "run creation identity is immutable" \
    "UPDATE runs
     SET id = '10000000-0000-4000-8000-000000000081'
     WHERE id = '10000000-0000-4000-8000-000000000080'"
  expect_sql_failure \
    "run creation identity is immutable" \
    "UPDATE runs
     SET request_metadata = '{\"protocol\":\"mcp\",\"method\":\"tools/call\"}'
     WHERE id = '10000000-0000-4000-8000-000000000080'"
  expect_sql_failure \
    "runs_cancel_summary_consistent" \
    "UPDATE runs
     SET cancel_reason = 'phantom cancellation'
     WHERE id = '10000000-0000-4000-8000-000000000080'"
  expect_sql_failure \
    "terminal run cannot acquire a cancellation request" \
    "UPDATE runs
     SET cancel_reason = 'late cancellation rewrite'
     WHERE id = '10000000-0000-4000-8000-000000000010'"
  expect_sql_failure \
    "run attempts are immutable history and cannot be deleted" \
    "DELETE FROM run_attempts
     WHERE id = '10000000-0000-4000-8000-000000000020'"
  expect_sql_failure \
    "run events are append-only" \
    "DELETE FROM run_events
     WHERE id = '10000000-0000-4000-8000-000000000041'"
  expect_sql_failure \
    "run event retention watermark cannot move backwards" \
    "UPDATE run_event_retention_watermarks
     SET retained_through_sequence = 1
     WHERE run_id = '10000000-0000-4000-8000-000000000010'"
  expect_sql_failure \
    "run event retention watermark cannot exceed latest event sequence" \
    "UPDATE run_event_retention_watermarks
     SET retained_through_sequence = 4
     WHERE run_id = '10000000-0000-4000-8000-000000000010'"
  expect_sql_failure \
    "run event retention watermark identity cannot change" \
    "UPDATE run_event_retention_watermarks
     SET run_id = '10000000-0000-4000-8000-000000000099'
     WHERE run_id = '10000000-0000-4000-8000-000000000010'"
  expect_sql_failure \
    "run event retention watermark evidence cannot be deleted" \
    "DELETE FROM run_event_retention_watermarks
     WHERE run_id = '10000000-0000-4000-8000-000000000010'"
  expect_sql_failure \
    "terminal Run artifact is immutable" \
    "UPDATE run_accounting_ledger
     SET created_at = clock_timestamp()
     WHERE run_id = '10000000-0000-4000-8000-000000000010'"

  assert_plan_uses \
    "idx_runs_runtime_pending" \
    "SELECT id FROM runs
     WHERE agent_id = '10000000-0000-4000-8000-000000000002'
       AND status = 'running' AND dispatch_state = 'pending'
     ORDER BY started_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
  assert_plan_uses \
    "idx_runs_runtime_pending_global" \
    "SELECT id FROM runs
     WHERE status = 'running' AND dispatch_state = 'pending'
     ORDER BY started_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
  assert_plan_uses \
    "idx_runs_runtime_retry_due_global" \
    "SELECT id FROM runs
     WHERE status = 'running' AND dispatch_state = 'retry_wait'
       AND next_attempt_at <= clock_timestamp()
     ORDER BY next_attempt_at, started_at, id
     LIMIT 1 FOR UPDATE SKIP LOCKED"
  assert_plan_uses \
    "idx_runtime_signal_outbox_pending" \
    "SELECT id FROM runtime_signal_outbox
     WHERE status = 'pending'
     ORDER BY available_at, created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
  assert_plan_uses \
    "idx_run_effect_outbox_pending" \
    "SELECT id FROM run_effect_outbox
     WHERE status = 'pending'
     ORDER BY available_at, created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"

  set +e
  local down_output down_status
  down_output="$(revert_063 2>&1)"
  down_status=$?
  set -e
  if ((down_status == 0)); then
    fail "down migration unexpectedly accepted post-cutover runtime v2 data"
  fi
  [[ "$down_output" == *"migration 063 down refuses post-cutover runs"* ]] \
    || fail "down migration failed for the wrong reason: $down_output"
}

run_fresh_database_scenario
run_historical_upgrade_scenario
run_preflight_scenarios
run_runtime_v2_invariant_scenario

echo "migration 063 regression suite passed"
