"""
assembler.tasks.export
~~~~~~~~~~~~~~~~~~~~~~~
Stage 5 — R artifact export.

Calls assembler/tools/export_artifacts.R via subprocess to produce .rda bundles.
Waits on all four generate_* tasks before running.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from common.cache_utils import file_hash_cache_key
from datetime import timedelta
from prefect import get_run_logger, task

from common.config import AssemblerConfig

_R_SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "export_artifacts.R"


@task(name="task-export-rda", cache_key_fn=file_hash_cache_key, cache_expiration=timedelta(hours=24))
def task_export_rda(
    cfg: AssemblerConfig,
    *upstream_futures,
) -> Path:
    """
    Run export_artifacts.R <workdir> to produce:
      regimens.rda, validdrugs.rda, regimengroups.rda

    Accepts upstream futures as *args so Prefect tracks dependencies —
    the task blocks until all four generate_* tasks have completed.
    Writes back: none (side-effect only — .rda files written to workdir)
    """
    logger = get_run_logger()

    # resolve any futures passed in
    _ = [f.result() if hasattr(f, "result") else f for f in upstream_futures]

    cmd = ["Rscript", str(_R_SCRIPT), str(cfg.workdir)]
    logger.info(f"[EXPORT] Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        logger.info(result.stdout.strip())
    if result.stderr:
        logger.warning(result.stderr.strip())

    if result.returncode != 0:
        raise RuntimeError(f"export_artifacts.R exited with code {result.returncode}")

    logger.info(f"[EXPORT] .rda bundles written to {cfg.workdir}")
    return cfg.workdir
