"""
assembler.tasks.sre
~~~~~~~~~~~~~~~~~~~~
Phase 2 SRE @task.

process_regimen_group wraps the RegStringHandler logic for a single
(variant_key) group. The flow calls this with .map() over all groups.
"""
from __future__ import annotations

import logging
import polars as pl

# Pure-logic tools — called inside this task, not decorated themselves
from tools.handler import RegStringHandler  # adapt: wraps _process_group


def process_regimen_group(group_df: pl.DataFrame) -> pl.DataFrame:
    """
    Run full SRE pipeline for one variant_key group.
    Returns a DataFrame with regString and cycleLength columns attached.
    Plain function — called in a loop inside run_sre_task, not as individual Prefect tasks.
    """
    handler = RegStringHandler.__new__(RegStringHandler)
    handler.logger = logging.getLogger(__name__)
    return handler._process_group(group_df)
