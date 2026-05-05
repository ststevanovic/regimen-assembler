"""
assembler.flows.transform_flow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Phase 2 — SRE transform flow.

Architecture:
  - Partition frame by variant_key
  - Single @task runs a plain Python loop over all groups (no per-group task overhead)
  - Concat results, write parquet
  - Post-SRE: make shortString, rename cols to HemOnc schema, write regimens_full.tsv
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import polars as pl
from prefect import flow, task, get_run_logger

from tools.sre_module import RegStringHandler
from tools.seq_collapse import collapse_naive


def process_regimen_group(group_df: pl.DataFrame) -> pl.DataFrame:
    """Plain function — no Prefect overhead per group."""
    handler = RegStringHandler.__new__(RegStringHandler)
    handler.logger = logging.getLogger(__name__)
    return handler._process_group(group_df)


@task(name="sre-all-groups", persist_result=False)
def run_sre_task(parquet_path: Path) -> Path:
    """Process all regimen groups in one Prefect task."""
    logger = get_run_logger()
    frame  = pl.read_parquet(parquet_path)
    groups = frame.partition_by("variant_key", maintain_order=True)
    logger.info(f"[sre] processing {len(groups)} regimen groups")
    results = [process_regimen_group(g) for g in groups]
    final   = pl.concat(results)
    out_path = parquet_path.with_name("s_frame_sre.parquet")
    final.write_parquet(out_path)
    logger.info(f"[sre] wrote {final.height} rows → {out_path}")
    return out_path


@task(name="write-regimens-full", persist_result=False)
def write_regimens_full(sre_parquet: Path, out_path: Path) -> Path:
    """
    Post-SRE sanitisation — mirrors src/transform.py FrameSanitizer.

    1. Drop rows with null regString
    2. Strip @cyclelen markers, collapse regString → shortString
    3. Rename cols to HemOnc schema (condition_cui→conditionCode, etc.)
    4. Add metaCondition = 'all'
    5. Write regimens_full.tsv (selected_columns_raw, undeduped)
    """
    logger = get_run_logger()

    frame = pl.read_parquet(sre_parquet)
    before = frame.height
    frame = frame.filter(pl.col("regString").is_not_null())
    logger.info(f"[write-regimens-full] {before - frame.height} rows dropped (null regString), {frame.height} remain")

    # make shortString
    def _make_short_string(s):
        if not isinstance(s, str):
            return s
        cleaned = re.sub(r"@cyclelen\d+", "", s, flags=re.IGNORECASE)
        return collapse_naive(cleaned)

    frame = frame.with_columns(
        pl.col("regString").map_elements(_make_short_string, return_dtype=pl.Utf8).alias("shortString")
    )

    # rename to HemOnc output schema
    rename_map = {
        "condition_cui": "conditionCode",
        "regimen":       "regName",
        "regimen_cui":   "regCode",
        "component_cui": "componentCode",
    }
    frame = frame.rename(rename_map)

    # add metaCondition
    frame = frame.with_columns(pl.lit("all").alias("metaCondition"))

    selected_columns_raw = [
        "metaCondition",
        "condition",
        "conditionCode",
        "regName",
        "variant",
        "regCode",
        "component",
        "componentCode",
        "cycleLength",
        "route",
        "regString",
        "shortString",
    ]
    # keep only columns that actually exist in the frame
    cols = [c for c in selected_columns_raw if c in frame.columns]
    out_path = Path(out_path)
    frame.select(cols).write_csv(str(out_path), separator="\t")
    logger.info(f"[write-regimens-full] wrote {frame.height} rows → {out_path}")
    return out_path


@flow(name="transform")
def transform_flow(parquet_path: Path, regimens_full_path: Path) -> Path:
    sre_parquet = run_sre_task(parquet_path)
    return write_regimens_full(sre_parquet, regimens_full_path)
