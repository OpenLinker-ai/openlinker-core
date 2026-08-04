#!/bin/sh
set -eu
CDPATH=
export CDPATH

command -v go >/dev/null 2>&1 || {
  echo "go is required" >&2
  exit 1
}

repository_root=$(cd -- "$(dirname -- "$0")/.." && pwd)
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/openlinker-browser-volume.XXXXXX")
container_name="openlinker-browser-volume-$$"
postgres_image="${POSTGRES_DOCKER_IMAGE:-postgres:16-alpine}"
postgres_backend="${OPENLINKER_BROWSER_POSTGRES_BACKEND:-docker}"
native_data="$temporary_root/postgres-data"
native_socket=""
native_log="$temporary_root/postgres.log"
native_started=0

case "$postgres_backend" in
  docker)
    command -v docker >/dev/null 2>&1 || {
      echo "docker is required for OPENLINKER_BROWSER_POSTGRES_BACKEND=docker" >&2
      exit 1
    }
    ;;
  native)
    for command_name in initdb pg_ctl createdb psql; do
      command -v "$command_name" >/dev/null 2>&1 || {
        echo "$command_name is required for OPENLINKER_BROWSER_POSTGRES_BACKEND=native" >&2
        exit 1
      }
    done
    ;;
  *)
    echo "OPENLINKER_BROWSER_POSTGRES_BACKEND must be docker or native" >&2
    exit 1
    ;;
esac

cleanup() {
  if [ "$postgres_backend" = docker ]; then
    docker rm -f "$container_name" >/dev/null 2>&1 || true
  elif [ "$native_started" -eq 1 ]; then
    pg_ctl -D "$native_data" -m fast -w stop >/dev/null 2>&1 || true
  fi
  if [ -n "$native_socket" ]; then
    rm -rf -- "$native_socket"
  fi
  rm -rf -- "$temporary_root"
}
trap cleanup EXIT INT TERM

if [ "$postgres_backend" = docker ]; then
  docker run -d \
    --name "$container_name" \
    --read-only \
    --tmpfs /run/postgresql:rw,nosuid,nodev,size=16m \
    --tmpfs /var/lib/postgresql/data:rw,nosuid,nodev,size=512m \
    -e POSTGRES_DB=openlinker_browser_volume \
    -e POSTGRES_USER=openlinker_browser_volume \
    -e POSTGRES_PASSWORD=openlinker_browser_volume \
    -p 127.0.0.1::5432 \
    "$postgres_image" \
    >/dev/null

  ready_attempt=0
  while [ "$ready_attempt" -lt 60 ]; do
    if docker exec "$container_name" \
      pg_isready \
      -U openlinker_browser_volume \
      -d openlinker_browser_volume \
      >/dev/null 2>&1; then
      break
    fi
    ready_attempt=$((ready_attempt + 1))
    sleep 1
  done
  if [ "$ready_attempt" -ge 60 ]; then
    echo "disposable PostgreSQL did not become ready" >&2
    docker logs "$container_name" >&2 || true
    exit 1
  fi

  published_address=$(docker port "$container_name" 5432/tcp | sed -n '1p')
  published_port=${published_address##*:}
  case "$published_port" in
    ''|*[!0-9]*)
      echo "could not resolve the disposable PostgreSQL port" >&2
      exit 1
      ;;
  esac
  database_url="postgres://openlinker_browser_volume:openlinker_browser_volume@127.0.0.1:${published_port}/openlinker_browser_volume?sslmode=disable"
else
  native_socket=$(mktemp -d /tmp/ol-pg.XXXXXX)
  mkdir -m 0700 "$native_data"
  chmod 0700 "$native_socket"
  initdb \
    -D "$native_data" \
    -A trust \
    -U openlinker_browser_volume \
    --encoding=UTF8 \
    --no-locale \
    >/dev/null
  if ! pg_ctl \
    -D "$native_data" \
    -l "$native_log" \
    -o "-h '' -k $native_socket" \
    -w start \
    >/dev/null; then
    echo "native disposable PostgreSQL did not become ready" >&2
    cat "$native_log" >&2 || true
    exit 1
  fi
  native_started=1
  createdb \
    -h "$native_socket" \
    -U openlinker_browser_volume \
    openlinker_browser_volume
  encoded_socket=$(printf '%s' "$native_socket" | sed 's|/|%2F|g')
  database_url="postgres://openlinker_browser_volume@/openlinker_browser_volume?host=${encoded_socket}&sslmode=disable"
fi
normal_go_cache="$temporary_root/go-cache-normal"
race_go_cache="$temporary_root/go-cache-race"

apply_schema_file() {
  schema_file=$1
  if [ "$postgres_backend" = docker ]; then
    docker exec -i "$container_name" \
      psql \
      -U openlinker_browser_volume \
      -d openlinker_browser_volume \
      -v ON_ERROR_STOP=1 \
      <"$schema_file" \
      >/dev/null
  else
    psql "$database_url" -v ON_ERROR_STOP=1 -f "$schema_file" >/dev/null
  fi
}

run_database_test() {
  cache=$1
  package=$2
  test_name=$3
  shift 3
  test_status=0
  output=$(
    TEST_DATABASE_URL="$database_url" \
      GOCACHE="$cache" \
      go test "$@" "$package" -run "^${test_name}$" -count=1 -v
  ) || test_status=$?
  printf '%s\n' "$output"
  if [ "$test_status" -ne 0 ]; then
    echo "required PostgreSQL test failed with status ${test_status}: ${test_name}" >&2
    return "$test_status"
  fi
  if ! printf '%s\n' "$output" | grep -q -- "--- PASS: ${test_name}"; then
    echo "required PostgreSQL test did not execute successfully: ${test_name}" >&2
    return 1
  fi
}

(
  cd "$repository_root"
  for migration_file in \
    086_current_schema_init.up.sql \
    087_browser_agent_execution_profile.up.sql \
    088_browser_human_control.up.sql \
    089_user_jwt_token_version.up.sql \
    090_task_callback_owner_index.up.sql \
    091_browser_interaction_policy.up.sql; do
    apply_schema_file "$repository_root/migrations/$migration_file"
  done
  apply_schema_file "$repository_root/migrations/086_current_schema_init_verify.sql"

  run_database_test \
    "$normal_go_cache" \
    ./pkg/runtime \
    TestBrowserProjectedEventVolumeIsIndependentOfActionCount
  run_database_test \
    "$normal_go_cache" \
    ./pkg/runtime \
    TestFullBrowserProfileRejectsOldRuntimeSessionWithoutDowngrade
  run_database_test \
    "$normal_go_cache" \
    ./pkg/runtime \
    TestFirstFullBrowserSessionConsumesOwnerStagedPolicy
  run_database_test \
    "$normal_go_cache" \
    ./pkg/runtime \
    TestOwnerStagedBrowserPolicyFencesRunCreationUntilFirstMatchingSession
  run_database_test \
    "$normal_go_cache" \
    ./pkg/agent \
    TestBrowserInteractionPolicyCanBeOwnerStagedBeforeFirstSession
  run_database_test \
    "$normal_go_cache" \
    ./pkg/db/generated \
    TestListCallRecordsReadsBrowserPolicyEvidenceFromRunAuthority
  run_database_test \
    "$normal_go_cache" \
    ./pkg/migrationinit \
    TestBrowserPolicyMigrationConvergesFromFreshReviewedBridgeAndVersion90
  GOCACHE="$normal_go_cache" go clean -cache

  run_database_test \
    "$race_go_cache" \
    ./pkg/runtime \
    TestBrowserProjectedEventVolumeIsIndependentOfActionCount \
    -race
  run_database_test \
    "$race_go_cache" \
    ./pkg/runtime \
    TestFullBrowserProfileRejectsOldRuntimeSessionWithoutDowngrade \
    -race
  run_database_test \
    "$race_go_cache" \
    ./pkg/runtime \
    TestFirstFullBrowserSessionConsumesOwnerStagedPolicy \
    -race
  run_database_test \
    "$race_go_cache" \
    ./pkg/runtime \
    TestOwnerStagedBrowserPolicyFencesRunCreationUntilFirstMatchingSession \
    -race
  run_database_test \
    "$race_go_cache" \
    ./pkg/agent \
    TestBrowserInteractionPolicyCanBeOwnerStagedBeforeFirstSession \
    -race
  run_database_test \
    "$race_go_cache" \
    ./pkg/db/generated \
    TestListCallRecordsReadsBrowserPolicyEvidenceFromRunAuthority \
    -race
  GOCACHE="$race_go_cache" go clean -cache
)

echo "Browser PostgreSQL acceptance passed (fresh/v86/v90 migration, Owner-staged first full Session and Run fence, old-Worker rejection, 1 vs 500 action rows, and CallRecord policy evidence)"
