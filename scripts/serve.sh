#!/usr/bin/env bash
# assembler/scripts/serve.sh
# Usage: bash scripts/serve.sh [stop]

set -euo pipefail

SESSION="hemonc"
ASSEMBLER_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export PREFECT_API_URL="${PREFECT_API_URL:-http://127.0.0.1:4200/api}"
export PREFECT_HOME="${ASSEMBLER_ROOT}/.prefect"
export PYTHONPATH="${ASSEMBLER_ROOT}"
mkdir -p "${PREFECT_HOME}"

PREFECT="${ASSEMBLER_ROOT}/.venvs/etl/bin/prefect"
PREFECT_REG="${ASSEMBLER_ROOT}/.venvs/regression/bin/prefect"

if [[ "${1:-}" == "stop" ]]; then
    tmux kill-session -t "${SESSION}" 2>/dev/null && echo "Stopped." || echo "No session."
    exit 0
fi

if tmux has-session -t "${SESSION}" 2>/dev/null; then
    echo "Already running — attaching."
    exec tmux attach-session -t "${SESSION}"
fi

tmux new-session -d -s "${SESSION}" -x 220 -y 50

tmux send-keys -t "${SESSION}:0.0" \
    "export PREFECT_API_URL=${PREFECT_API_URL} PYTHONPATH=${ASSEMBLER_ROOT} && ${PREFECT} server start" Enter

tmux split-window -h -t "${SESSION}:0.0"
tmux send-keys -t "${SESSION}:0.1" \
    "export PREFECT_API_URL=${PREFECT_API_URL} PYTHONPATH=${ASSEMBLER_ROOT} && sleep 6 && ${PREFECT} worker start --pool etl-pool --type process --name etl-worker-${HOSTNAME}" Enter

tmux split-window -v -t "${SESSION}:0.1"
tmux send-keys -t "${SESSION}:0.2" \
    "export PREFECT_API_URL=${PREFECT_API_URL} PYTHONPATH=${ASSEMBLER_ROOT} && sleep 6 && ${PREFECT_REG} worker start --pool regression-pool --type process --name regression-worker-${HOSTNAME}" Enter

tmux select-pane -t "${SESSION}:0.0"

echo "✓  hemonc session started — UI: http://127.0.0.1:4200"
exec tmux attach-session -t "${SESSION}"
