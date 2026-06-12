#!/usr/bin/env bash
# Bring up the local Postgres container and wait until it accepts connections.
# Idempotent: safe to run repeatedly. Data persists in the pattern_cache_pgdata
# named volume; remove with `docker volume rm pattern_cache_pgdata` for a clean
# bootstrap.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found in PATH. Install Docker Desktop or set DB=duckdb." >&2
  exit 1
fi

# `docker compose` (v2) is the modern subcommand; fall back to docker-compose v1.
if docker compose version >/dev/null 2>&1; then
  DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DC=(docker-compose)
else
  echo "Neither 'docker compose' nor 'docker-compose' is available." >&2
  exit 1
fi

echo "[postgres] starting container via $COMPOSE_FILE"
"${DC[@]}" -f "$COMPOSE_FILE" up -d

echo "[postgres] waiting for health…"
for i in $(seq 1 60); do
  status=$(docker inspect -f '{{.State.Health.Status}}' pattern-cache-postgres 2>/dev/null || echo "starting")
  if [[ "$status" == "healthy" ]]; then
    echo "[postgres] healthy on localhost:15432 (user=postgres, db=postgres)"
    exit 0
  fi
  sleep 1
done

echo "[postgres] container did not become healthy in 60s" >&2
docker logs --tail 50 pattern-cache-postgres >&2 || true
exit 1
