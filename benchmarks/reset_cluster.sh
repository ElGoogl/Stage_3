#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

DATA_DIR="${ROOT_DIR}/data_repository"

compose_down() {
    if command -v docker-compose >/dev/null 2>&1; then
        docker-compose -f "${ROOT_DIR}/docker-compose.yml" down -v
        return
    fi
    docker compose -f "${ROOT_DIR}/docker-compose.yml" down -v
}

compose_up() {
    if command -v docker-compose >/dev/null 2>&1; then
        docker-compose -f "${ROOT_DIR}/docker-compose.yml" up -d
        return
    fi
    docker compose -f "${ROOT_DIR}/docker-compose.yml" up -d
}

wait_for_url() {
    local name="$1"
    local url="$2"
    local attempts=30
    local wait_seconds=2

    for ((i=1; i<=attempts; i++)); do
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" "${url}" || true)
        if [ "${code}" = "200" ]; then
            echo "Ready: ${name}"
            return 0
        fi
        sleep "${wait_seconds}"
    done

    echo "Timeout waiting for ${name} at ${url}"
    return 1
}

echo "Stopping cluster and removing volumes..."
compose_down

echo "Clearing data_repository contents..."
if [ -d "${DATA_DIR}" ]; then
    find "${DATA_DIR}" -mindepth 1 \
        ! -name ".gitignore" \
        ! -name ".gitkeep" \
        -exec rm -rf {} +
fi

echo "Starting cluster..."
compose_up

echo "Waiting for services..."
wait_for_url "ingestion1" "http://localhost:7001/ingest/list"
wait_for_url "ingestion2" "http://localhost:7002/ingest/list"
wait_for_url "ingestion3" "http://localhost:7003/ingest/list"
wait_for_url "indexing1" "http://localhost:7101/health"
wait_for_url "indexing2" "http://localhost:7102/health"
wait_for_url "indexing3" "http://localhost:7103/health"
wait_for_url "search1" "http://localhost:8000/status"
wait_for_url "search2" "http://localhost:8001/status"
wait_for_url "search3" "http://localhost:8002/status"

echo "Reset complete."
