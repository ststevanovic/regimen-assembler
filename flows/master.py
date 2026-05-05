"""
assembler.flows.master
~~~~~~~~~~~~~~~~~~~~~~~
Master orchestrator.

Architecture
------------
Two Prefect workers run in isolated venvs:
  - assembler/.venvs/etl        → work pool "etl-pool"       (Python 3.12)
  - assembler/.venvs/regression → work pool "regression-pool" (Python 3.11)

master_runner triggers deployments via run_deployment() so that each flow
executes inside the correct venv/worker, with full Prefect tracking.

Before running:
  1. prefect server start                               (terminal 1)
  2. bash assembler/scripts/start_worker_etl.sh        (terminal 2)
  3. bash assembler/scripts/start_worker_regression.sh (terminal 3)
  4. python assembler/scripts/deploy.py                (once — registers deployments)

CLI usage
---------
  python -m assembler.flows.master --etl --workdir output-assembled
  python -m assembler.flows.master --regression --ref output_baseline --new output-assembled
"""
from __future__ import annotations

import argparse
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

ROOT = Path(__file__).resolve().parents[1]  # assembler/


# ── ETL flow — executed by the etl-pool worker ───────────────────────────────
# The etl worker runs assembler/.venvs/etl which has polars, pandas, prefect.
# Flows are imported and called directly — no subprocess bridge needed.

@flow(name="hemonc-etl")
def etl_flow(
    workdir:    str  = "OUTPUTs/output-assembled",
    sigs:       str  = "sigs_march_2025.csv",
    skip_query: bool = True,
) -> None:
    from common.config import AssemblerConfig
    from flows.preprocess_flow import preprocess_flow
    from flows.transform_flow import transform_flow
    from flows.wrapup_flow import wrapup_flow
    from tasks.inputs import task_load_inputs

    logger = get_run_logger()
    logger.info(f"[etl] workdir={workdir}  sigs={sigs}")

    cfg = AssemblerConfig(workdir=workdir, sigs_file=sigs, skip_query=skip_query)
    cfg.ensure_dirs()

    # Phase 0 — load inputs (simulated Athena queries)
    bundle = task_load_inputs(cfg)

    # Phase 1 — preprocessing (resolve/handle/audit)
    cleaned = preprocess_flow(bundle.sigs_w_conditions)

    # Write s_frame.parquet for the SRE transform phase
    cleaned.write_parquet(cfg.parquet_path)
    logger.info(f"[etl] s_frame written → {cfg.parquet_path}")

    # Phase 2 — SRE transform + write regimens_full.tsv
    sre_parquet = transform_flow(cfg.parquet_path, cfg.regimens_full_path)
    logger.info(f"[etl] SRE transform complete → {sre_parquet}")

    # Phase 3 — data model, R export, validation
    wrapup_flow(cfg)

    logger.info("[etl] all phases complete")


# ── Regression flow — executed by the regression-pool worker ─────────────────

@flow(name="hemonc-regression")
def regression_flow(
    ref: str        = "OUTPUTs/output_baseline",
    new: str        = "OUTPUTs/run2test",
    out: str        = "OUTPUTs/output.regression_tests",
    n:   int | None = None,
) -> None:
    from common.config import RegressionConfig
    from tasks.regression import task_regression_compare

    logger = get_run_logger()
    logger.info(f"[regression] ref={ref}  new={new}  out={out}")

    cfg = RegressionConfig(ref_dir=ref, new_dir=new, output_dir=out, run_n=n)
    cfg.ensure_dirs()

    json_path = task_regression_compare(cfg)
    logger.info(f"[regression] complete → {json_path}")


# ── master runner — submits deployments to the right pools ───────────────────

@flow(name="hemonc-master")
def master_runner(
    run_etl:        bool        = True,
    run_regression: bool        = False,
    etl_args:       dict | None = None,
    regress_args:   dict | None = None,
) -> None:
    logger = get_run_logger()

    if run_etl:
        logger.info("[master] submitting ETL deployment → etl-pool")
        run_deployment(
            name="hemonc-etl/prod",
            parameters=etl_args or {},
        )

    if run_regression:
        logger.info("[master] submitting regression deployment → regression-pool")
        run_deployment(
            name="hemonc-regression/prod",
            parameters=regress_args or {},
        )


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HemOnc master runner")

    parser.add_argument("--etl",        action="store_true")
    parser.add_argument("--regression", action="store_true")

    parser.add_argument("--workdir",    default="OUTPUTs/output-assembled")
    parser.add_argument("--sigs",       default="sigs_march_2025.csv")
    parser.add_argument("--skip-query", action="store_true", default=True,
                        help="Use CSV snapshots instead of live Athena queries")

    parser.add_argument("--ref", default="OUTPUTs/output_baseline")
    parser.add_argument("--new", default="OUTPUTs/run2test")
    parser.add_argument("--out", default="OUTPUTs/output.regression_tests")
    parser.add_argument("--n",   default=None, type=int)

    args = parser.parse_args()

    run_etl = args.etl or (not args.etl and not args.regression)
    run_reg = args.regression

    master_runner(
        run_etl=run_etl,
        run_regression=run_reg,
        etl_args={
            "workdir":    args.workdir,
            "sigs":       args.sigs,
            "skip_query": args.skip_query,
        } if run_etl else None,
        regress_args={
            "ref": args.ref,
            "new": args.new,
            "out": args.out,
            "n":   args.n,
        } if run_reg else None,
    )
