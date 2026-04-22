#!/usr/bin/env bash
#
# Manage the local test Postgres for the backend pytest suite.
#
# Prod runs on Supabase; this is a disposable Dockerized Postgres that
# only exists while you're running tests. Schema + migrations are applied
# using the same logic as .github/workflows/pytest.yml so CI and local
# can't drift.
#
# Usage:
#   backend/scripts/test_db.sh up      # start container, wait, apply schema
#   backend/scripts/test_db.sh reset   # wipe volume + up again (fresh DB)
#   backend/scripts/test_db.sh psql    # interactive psql shell
#   backend/scripts/test_db.sh down    # stop container (volume preserved)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.test.yml"

# Defaults match docker-compose.test.yml. Override via env if needed.
: "${DB_HOST:=localhost}"
: "${DB_PORT:=5432}"
: "${DB_NAME:=jazz_test}"
: "${DB_USER:=jazztest}"
: "${DB_PASSWORD:=jazztest}"
export PGPASSWORD="$DB_PASSWORD"

# Belt-and-braces: this script runs destructive ops (down -v, CASCADE
# schema replay). The pytest conftest guard already enforces the same
# rule for the suite itself; we re-enforce here so a fat-fingered
# DB_NAME override can't wipe something it shouldn't.
db_name_lower="$(printf '%s' "$DB_NAME" | tr '[:upper:]' '[:lower:]')"
case "$db_name_lower" in
  *test*) ;;
  *)
    echo "ERROR: DB_NAME='$DB_NAME' does not contain 'test'. Refusing." >&2
    exit 1
    ;;
esac

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker not found on PATH. Install Docker Desktop." >&2
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon not reachable. Start Docker Desktop." >&2
    exit 1
  fi
}

wait_for_pg() {
  echo "Waiting for Postgres at $DB_HOST:$DB_PORT..."
  for _ in $(seq 1 60); do
    if pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Postgres did not become ready within 60s." >&2
  exit 1
}

apply_schema() {
  echo "Applying base schema..."
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -v ON_ERROR_STOP=1 -f "$REPO_ROOT/sql/jazz-db-schema.sql" \
    >/dev/null

  echo "Applying numbered migrations..."
  # Matches the CI loop in .github/workflows/pytest.yml. ON_ERROR_STOP=0
  # + `|| true` because some older migrations overlap the base schema;
  # replacing this with `alembic upgrade head` is tracked separately.
  shopt -s nullglob
  for f in "$REPO_ROOT"/sql/migrations/[0-9]*.sql; do
    echo "  $(basename "$f")"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
      -v ON_ERROR_STOP=0 -f "$f" >/dev/null 2>&1 || true
  done
  shopt -u nullglob
}

cmd="${1:-}"
case "$cmd" in
  up)
    require_docker
    docker compose -f "$COMPOSE_FILE" up -d
    wait_for_pg
    apply_schema
    echo "Test DB ready: $DB_HOST:$DB_PORT/$DB_NAME (user=$DB_USER)"
    ;;
  reset)
    require_docker
    docker compose -f "$COMPOSE_FILE" down -v
    docker compose -f "$COMPOSE_FILE" up -d
    wait_for_pg
    apply_schema
    echo "Test DB reset: $DB_HOST:$DB_PORT/$DB_NAME (user=$DB_USER)"
    ;;
  psql)
    exec psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME"
    ;;
  down)
    require_docker
    docker compose -f "$COMPOSE_FILE" down
    ;;
  ""|-h|--help|help)
    sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "unknown command: $cmd" >&2
    echo "usage: $0 {up|reset|psql|down}" >&2
    exit 2
    ;;
esac
