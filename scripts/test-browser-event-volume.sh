#!/bin/sh
set -eu
CDPATH=
export CDPATH

command -v docker >/dev/null 2>&1 || {
  echo "docker is required" >&2
  exit 1
}
command -v go >/dev/null 2>&1 || {
  echo "go is required" >&2
  exit 1
}

repository_root=$(cd -- "$(dirname -- "$0")/.." && pwd)
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/openlinker-browser-volume.XXXXXX")
container_name="openlinker-browser-volume-$$"
postgres_image="${POSTGRES_DOCKER_IMAGE:-postgres:16-alpine}"

cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
  rm -rf -- "$temporary_root"
}
trap cleanup EXIT INT TERM

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
api_binary="$temporary_root/openlinker-core-api"

(
  cd "$repository_root"
  go build -o "$api_binary" ./cmd/api
  DATABASE_URL="$database_url" \
    MIGRATIONS_DIR="$repository_root/migrations" \
    "$api_binary" migrate up

  TEST_DATABASE_URL="$database_url" \
    go test ./pkg/runtime \
      -run '^TestBrowserProjectedEventVolumeIsIndependentOfActionCount$' \
      -count=1
  TEST_DATABASE_URL="$database_url" \
    go test -race ./pkg/runtime \
      -run '^TestBrowserProjectedEventVolumeIsIndependentOfActionCount$' \
      -count=1
)

echo "Browser event-volume PostgreSQL acceptance passed (1 action vs 500 actions)"
