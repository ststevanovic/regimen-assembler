"""
assembler.tasks.validate
~~~~~~~~~~~~~~~~~~~~~~~~~
Stage 6 — Regression / legacy validation.

Logic lives in assembler/tools/validate.py.
"""
from __future__ import annotations

from pathlib import Path

from common.cache_utils import file_hash_cache_key
from datetime import timedelta
from prefect import get_run_logger, task

from common.config import AssemblerConfig
from tools.validate import run as _validate_run


@task(
    name="validate",
    description="Compare output regimens against legacy set and write reports.",
    cache_key_fn=file_hash_cache_key,
    cache_expiration=timedelta(hours=24),
)
def task_validate(cfg: AssemblerConfig, regimens_full: Path) -> Path:
    """
    Stage 6 — validation.

    Calls assembler.tools.validate.run(workdir, regimens_full).
    Writes: {workdir}/validation/shared_output_analysis.txt
    Returns: Path to that file.
    """
    logger = get_run_logger()
    logger.info(f"[validate] running against {regimens_full}")

    _validate_run(
        file_dir=str(cfg.workdir),
        file_target_path=str(regimens_full),
    )

    out = cfg.workdir / "validation" / "shared_output_analysis.txt"
    logger.info(f"[validate] wrote {out}")
    return out
