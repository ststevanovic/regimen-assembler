#!/usr/bin/env bash
# assembler/scripts/start_worker_regression.sh
# ─────────────────────────────────────────────
# Starts a Prefect process worker in the regression venv, targeting "regression-pool".
#
# Prerequisites:
#   - Prefect server running:  prefect server start
#   - PREFECT_API_URL set (default: http://127.0.0.1:4200/api)
#   - venv built:  bash assembler/scripts/setup_venvs.sh
#
# Usage:
#   bash assembler/scripts/start_worker_regression.sh

set -euo pipefail

ASSEMBLER_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKER="${ASSEMBLER_ROOT}/.venvs/regression/bin/prefect"

export PREFECT_API_URL="${PREFECT_API_URL:-http://127.0.0.1:4200/api}"
export PYTHONPATH="${ASSEMBLER_ROOT}"

echo "Starting Regression worker  (pool: regression-pool, API: ${PREFECT_API_URL})"
exec "${WORKER}" worker start \
    --pool "regression-pool" \
    --type process \
    --name "regression-worker-${HOSTNAME}"
