"""
assembler.scripts.run_wrapup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Plain-Python entry point for Phase 3 (data model + R export + validate).
No Prefect imports — runs directly in the etl conda env.

Calls each @task via its .fn attribute to bypass the Prefect engine entirely
while reusing the exact same business logic.

Called by master.py via:
    mamba run -n etl python assembler/scripts/run_wrapup.py --workdir <dir>
"""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


def _fn(task_obj):
    """Unwrap a Prefect @task to its raw callable."""
    return getattr(task_obj, "fn", task_obj)


def main():
    parser = argparse.ArgumentParser(description="Run wrapup (data model + R export + validate)")
    parser.add_argument("--workdir", required=True, help="Output directory for this run")
    args = parser.parse_args()

    workdir       = Path(args.workdir).resolve()
    regimens_full = workdir / "regimens_full.tsv"

    if not regimens_full.exists():
        raise FileNotFoundError(
            f"regimens_full.tsv not found in {workdir} — did run_transform.py complete?"
        )

    from common.config import AssemblerConfig
    from tasks.datamodel import (
        task_build_regimens,
        task_reg_groups,
        task_valid_drugs,
        task_routes,
        task_short_strings,
    )

    cfg = AssemblerConfig(workdir=workdir)

    # ── Step 1: build regimens.tsv (prerequisite, blocking) ──────────────────
    _fn(task_build_regimens)(cfg, regimens_full)
    print(f"[wrapup] regimens.tsv written")

    # ── Step 2: data model fan-out (sequential here; Prefect runs concurrently) ─
    _fn(task_reg_groups)(cfg, regimens_full)
    print("[wrapup] regimengroups.tsv written")

    _fn(task_valid_drugs)(cfg, regimens_full)
    print("[wrapup] validdrugs.tsv written")

    _fn(task_routes)(cfg, regimens_full)
    print("[wrapup] regimens_drugs.tsv + regimens_drugs_deploy.tsv written")

    _fn(task_short_strings)(cfg, regimens_full)
    print("[wrapup] regimens_shortStrings.tsv written")

    # ── Step 3: R export (.rda files) ─────────────────────────────────────────
    # Prefer src/ location; fall back to assembler/tools/
    r_script = ROOT / "src" / "export_artifacts.R"
    if not r_script.exists():
        r_script = ROOT / "assembler" / "tools" / "export_artifacts.R"

    result = subprocess.run(["Rscript", str(r_script), str(workdir)], check=False)
    if result.returncode != 0:
        raise RuntimeError(f"export_artifacts.R exited with code {result.returncode}")
    print("[wrapup] R export (.rda) done")

    # ── Step 4: validate ──────────────────────────────────────────────────────
    from tasks.validate import task_validate
    _fn(task_validate)(cfg, regimens_full)
    print("[wrapup] validation done")


if __name__ == "__main__":
    main()
