#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

mkdir -p airflow/dags airflow/logs airflow/plugins

# Select docker compose command
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  DCMD=(sudo docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DCMD=(sudo docker-compose)
else
  echo "ERROR: docker compose not found. Install Docker with compose plugin or docker-compose."
    exit 1
fi

"${DCMD[@]}" pull || true
"${DCMD[@]}" build --pull
"${DCMD[@]}" up -d

sudo docker system prune -f || true

echo "Docker services are up."
