#!/usr/bin/env bash
# assembler/scripts/setup_venvs.sh
# ─────────────────────────────────
# Creates .venvs/etl (Python 3.12) and .venvs/regression (Python 3.11)
# inside the assembler/ directory from the pinned requirements files.
#
# Usage:
#   bash assembler/scripts/setup_venvs.sh
#
# Re-run any time to rebuild. Existing venvs are removed first.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_DIR="${REPO_ROOT}/assembler/.venvs"
REQ_DIR="${REPO_ROOT}/assembler/requirements"

# Use conda env pythons directly — system python3.12/3.11 may not be on PATH
CONDA_BASE="${CONDA_BASE:-${HOME}/miniconda3}"
PY312="${CONDA_BASE}/envs/etl/bin/python"
PY311="${CONDA_BASE}/envs/etl-regression/bin/python"

echo "=== Setting up prefect-etl venv (Python 3.12) ==="
echo "    using: ${PY312}"
rm -rf "${VENV_DIR}/etl"
"${PY312}" -m venv "${VENV_DIR}/etl"
"${VENV_DIR}/etl/bin/pip" install --upgrade pip --quiet
"${VENV_DIR}/etl/bin/pip" install -r "${REQ_DIR}/etl.txt"
echo "✓  .venvs/etl ready"

echo ""
echo "=== Setting up prefect-regression venv (Python 3.11) ==="
echo "    using: ${PY311}"
rm -rf "${VENV_DIR}/regression"
"${PY311}" -m venv "${VENV_DIR}/regression"
"${VENV_DIR}/regression/bin/pip" install --upgrade pip --quiet
"${VENV_DIR}/regression/bin/pip" install -r "${REQ_DIR}/regression.txt"
echo "✓  .venvs/regression ready"

echo ""
echo "=== Done. Start workers with: ==="
echo "  bash assembler/scripts/start_worker_etl.sh"
echo "  bash assembler/scripts/start_worker_regression.sh"
