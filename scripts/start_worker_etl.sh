#!/usr/bin/env bash
# assembler/scripts/start_worker_etl.sh
# ──────────────────────────────────────
# Starts a Prefect process worker in the etl venv, targeting work pool "etl-pool".
# The worker picks up any deployment that targets etl-pool.
#
# Prerequisites:
#   - Prefect server running:  prefect server start
#   - PREFECT_API_URL set (default: http://127.0.0.1:4200/api)
#   - venv built:  bash assembler/scripts/setup_venvs.sh
#
# Usage:
#   bash assembler/scripts/start_worker_etl.sh

set -euo pipefail

ASSEMBLER_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKER="${ASSEMBLER_ROOT}/.venvs/etl/bin/prefect"

export PREFECT_API_URL="${PREFECT_API_URL:-http://127.0.0.1:4200/api}"
export PYTHONPATH="${ASSEMBLER_ROOT}"

echo "Starting ETL worker  (pool: etl-pool, API: ${PREFECT_API_URL})"
exec "${WORKER}" worker start \
    --pool "etl-pool" \
    --type process \
    --name "etl-worker-${HOSTNAME}"
