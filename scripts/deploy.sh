#!/usr/bin/env bash
# deploy.sh — register assembler flows with a Prefect server
# Usage: bash assembler/scripts/deploy.sh [--remote]

set -euo pipefail

PROFILE="${1:---local}"
echo "Deploying with profile: $PROFILE"

prefect --profile "$PROFILE" deploy \
  assembler/flows/master.py:master_flow \
  --name "assembler-main" \
  --pool "default-agent-pool"
