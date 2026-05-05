"""
assembler/scripts/deploy.py
────────────────────────────
Registers Prefect deployments for ETL and Regression flows.
Run once (or after any flow change) to update the server.

Usage:
    # from repo root, with any env that has prefect installed:
    PYTHONPATH=. assembler/.venvs/etl/bin/python assembler/scripts/deploy.py

Each deployment is pinned to a work pool so the right worker picks it up:
  hemonc-etl/prod        → etl-pool        (.venvs/etl,        Python 3.12)
  hemonc-regression/prod → regression-pool (.venvs/regression, Python 3.11)
"""
"""
Register Prefect deployments defined in assembler/prefect.yaml.
Also registers any custom Block types (e.g. FieldSchemaBlock).

Usage (from anywhere):
    PREFECT_API_URL=http://127.0.0.1:4200/api \\
        assembler/.venvs/etl/bin/python assembler/scripts/deploy.py

Or from assembler/ root:
    PREFECT_API_URL=http://127.0.0.1:4200/api .venvs/etl/bin/python scripts/deploy.py
"""

import os
import sys
import subprocess
from pathlib import Path

ASSEMBLER = Path(__file__).resolve().parents[1]   # assembler/
sys.path.insert(0, str(ASSEMBLER))

api_url = os.environ.get("PREFECT_API_URL", "http://127.0.0.1:4200/api")
os.environ["PREFECT_API_URL"] = api_url

prefect_home = ASSEMBLER / ".prefect"
prefect_home.mkdir(exist_ok=True)
os.environ["PREFECT_HOME"] = str(prefect_home)

# ── register custom Block types ───────────────────────────────────────────────
print("Registering blocks...")
from common.schemas import FieldSchemaBlock  # noqa: E402

import asyncio

async def _register_blocks():
    await FieldSchemaBlock.register_type_and_schema()
    # Save a default "prod-schema" instance if one doesn't exist yet
    try:
        await FieldSchemaBlock.load("prod-schema")
        print("  ✓  FieldSchemaBlock 'prod-schema' already exists")
    except Exception:
        block = FieldSchemaBlock()
        await block.save("prod-schema", overwrite=True)
        print("  ✓  FieldSchemaBlock 'prod-schema' saved")

asyncio.run(_register_blocks())

# ── deploy flows ──────────────────────────────────────────────────────────────
prefect_bin = ASSEMBLER / ".venvs" / "etl" / "bin" / "prefect"
cmd = [str(prefect_bin), "deploy", "--all"]
env = {**os.environ, "PREFECT_API_URL": api_url, "PYTHONPATH": str(ASSEMBLER)}

print(f"\nDeploying all flows from {ASSEMBLER}/prefect.yaml → {api_url}")
result = subprocess.run(cmd, cwd=str(ASSEMBLER), env=env)
sys.exit(result.returncode)
