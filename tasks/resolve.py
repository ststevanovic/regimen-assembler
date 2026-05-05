"""
assembler.tasks.resolve
~~~~~~~~~~~~~~~~~~~~~~~~
Phase 1 resolver @tasks.

Each task:
  - receives a frame slice (pre-filtered by mask computed in preprocess_flow)
  - writes back the columns listed in its docstring
  - returns a ResolverPatch — never drops rows
"""
from __future__ import annotations

import polars as pl
from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable

from common.schemas import FieldSchema, ResolverPatch


# ── Blocking prerequisite ─────────────────────────────────────────────────

def resolve_variant_key(frame: pl.DataFrame, schema: FieldSchema) -> pl.DataFrame:
    """
    Compute variant_key = f(condition_cui, regimen_cui, variant).
    Called directly (no .submit()) — all other tasks depend on this column.
    """
    import re

    required = schema.key_source_cols
    missing  = [c for c in required if c not in frame.columns]
    if missing:
        raise ValueError(f"resolve_variant_key: missing columns {missing}")

    def _norm_variant(v):
        if v is None or (isinstance(v, float) and v != v):
            return None
        try:
            return str(int(float(v)))
        except Exception:
            return str(v).strip().lower()

    def _norm_key(v):
        if v is None or (isinstance(v, float) and v != v):
            return "000"
        try:
            return f"{int(float(v)):03d}"
        except Exception:
            return "000"

    variants = [_norm_variant(v) for v in frame["variant"].to_list()]
    uniq     = list(dict.fromkeys(v for v in variants if v is not None))
    variant_map = {v: f"{i:03d}" for i, v in enumerate(uniq, start=1)}

    return (
        frame
        .with_columns([
            pl.col("condition_cui").map_elements(_norm_key, return_dtype=pl.Utf8).alias("_ck"),
            pl.col("regimen_cui").map_elements(_norm_key, return_dtype=pl.Utf8).alias("_rk"),
            pl.col("variant").map_elements(_norm_variant, return_dtype=pl.Utf8).alias("_vn"),
        ])
        .with_columns(
            pl.when(pl.col("_vn").is_null())
            .then(pl.lit("000"))
            .otherwise(pl.col("_vn").replace(variant_map, default="000"))
            .alias("_vc")
        )
        .with_columns(
            pl.concat_str(["_ck", "_rk", "_vc"], separator="_").alias("variant_key")
        )
        .drop(["_ck", "_rk", "_vn", "_vc"])
    )


@task(name="resolve-indef-timing", persist_result=False)
def resolve_indef_timing(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Resolve timing_sequence for indeterminate-cycle rows.
    Strips optional (.*) blocks; normalises comma edges.
    Writes back: ["timing_sequence"]

    """
    import re

    def _fix_timing(value: str | None) -> str | None:
        if value is None:
            return value
        # Entire string is optional blocks e.g. '(3),(4)' → '3,4'
        if re.fullmatch(r"(\(.*?\),?)+", value):
            return re.sub(r"[()]", "", value)
        # Mixed: strip (.*?) blocks, clean up commas
        if re.search(r"\(.*?\)", value):
            cleaned = re.sub(r"\(.*?\)", "", value)
            cleaned = re.sub(r",+", ",", cleaned)
            return cleaned.strip(",")
        return value

    logger = get_run_logger()

    resolved_groups = [
        group_df.with_columns(
            pl.col("timing_sequence").map_elements(_fix_timing, return_dtype=pl.Utf8)
        )
        for _, group_df in slice.group_by(schema.group_keys, maintain_order=True)
    ]
    data = pl.concat(resolved_groups, how="vertical")

    logger.info(f"[REPORT] resolve-indef-timing: {slice.height} rows processed")
    create_table_artifact(
        key="resolve-indef-timing",
        table=data.select(schema.group_keys + ["timing_sequence"]).unique().to_dicts(),
        description="[P] timing_sequence normalized — optional (.*) blocks resolved",
    )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=["timing_sequence"],
        data=data,
        flag=False,
    )


@task(name="resolve-indef-bounds", persist_result=False)
def resolve_indef_bounds(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Patch cycle_length_lb / cycle_length_ub for indeterminate-cycle rows.
    Replaces '(+c)' / 'NUB' and indeterminate units with '1'.
    Writes back: ["cycle_length_lb", "cycle_length_ub"]

    """
    logger = get_run_logger()

    bad_vals          = ["(+c)", "NUB"]
    bad_bounds_mask   = (
        pl.col("cycle_length_lb").is_in(bad_vals)
        | pl.col("cycle_length_ub").is_in(bad_vals)
    )
    indeterminate_mask = pl.col("cycle_length_unit") == "indeterminate"
    patch_mask         = indeterminate_mask | bad_bounds_mask

    patched = slice.with_columns([
        pl.when(patch_mask).then(pl.lit("1")).otherwise(pl.col("cycle_length_lb")).alias("cycle_length_lb"),
        pl.when(patch_mask).then(pl.lit("1")).otherwise(pl.col("cycle_length_ub")).alias("cycle_length_ub"),
    ])

    affected = slice.filter(patch_mask)
    if affected.height:
        log_chunks = (
            affected
            .select(schema.regimen_cols[:1] + ["variant"])
            .unique()
            .with_columns((pl.col(schema.regimen_cols[0]) + " / " + pl.col("variant")).alias("entry"))
            ["entry"].to_list()
        )
        logger.info("[REPORT] resolve-indef-bounds patched:\n" + "\n".join(log_chunks))

    create_table_artifact(
        key="resolve-indef-bounds",
        table=patched.select(schema.group_keys + ["cycle_length_lb", "cycle_length_ub"]).unique().to_dicts(),
        description="[P] cycle_length_lb / cycle_length_ub patched — indeterminate/bad values set to '1'",
    )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=["cycle_length_lb", "cycle_length_ub"],
        data=patched,
        flag=False,
    )


@task(name="resolve-alldays-strip-brackets", persist_result=False)
def resolve_alldays_strip_brackets(slice: pl.DataFrame, schema: FieldSchema) -> pl.DataFrame:
    """
    Step 1 — strip parenthesized optional content from allDays.
    e.g. '1,2,(3),(4)' → '1,2'
    Writes back: allDays (intermediate)
    """
    import re

    def _strip(ds: str) -> str:
        cleaned = re.sub(r"\([^)]*\)", "", ds)
        cleaned = re.sub(r"\s*,\s*", ",", cleaned)
        cleaned = re.sub(r",+", ",", cleaned)
        return cleaned.strip(",")

    return slice.with_columns(
        pl.col("allDays").cast(pl.Utf8).map_elements(_strip, return_dtype=pl.Utf8).alias("allDays")
    )


@task(name="resolve-alldays-resolve-ranges", persist_result=False)
def resolve_alldays_resolve_ranges(slice: pl.DataFrame, schema: FieldSchema) -> pl.DataFrame:
    """
    Step 2 — resolve range notation to lower bound.
    '3~7' → '3'   |   '3|7' → '3'
    Writes back: allDays (intermediate)
    """
    import re

    def _resolve(ds: str) -> str:
        if ds is None:
            return None
        if "~" in ds or "|" in ds:
            return str(re.split(r"[~|]", ds)[0])
        return ds

    return slice.with_columns(
        pl.col("allDays").cast(pl.Utf8).map_elements(_resolve, return_dtype=pl.Utf8).alias("allDays")
    )


@task(name="resolve-alldays-collapse-zero", persist_result=False)
def resolve_alldays_collapse_zero(slice: pl.DataFrame, schema: FieldSchema) -> pl.DataFrame:
    """
    Step 3 — collapse all-zero day lists to [1].
    '0,0,0' → '1'
    Writes back: allDays (intermediate)
    """
    def _collapse(ds: str) -> str:
        if ds is None:
            return None
        try:
            days = list(map(int, ds.split(",")))
            if all(d == 0 for d in days):
                return "1"
        except Exception:
            pass
        return ds

    return slice.with_columns(
        pl.col("allDays").cast(pl.Utf8).map_elements(_collapse, return_dtype=pl.Utf8).alias("allDays")
    )


@task(name="resolve-alldays-shift", persist_result=False)
def resolve_alldays_shift(slice: pl.DataFrame, schema: FieldSchema) -> pl.DataFrame:
    """
    Step 4 — shift all day values per variant group so global_min → 1.
    '3,5,7' (min=3) → '1,3,5'
    Writes back: allDays (final)
    """
    def _shift_group(group_df: pl.DataFrame) -> pl.DataFrame:
        raw    = group_df["allDays"].to_list()
        parsed = [list(map(int, s.split(","))) if s is not None else None for s in raw]
        flat   = [d for lst in parsed if lst is not None for d in lst]
        if not flat:
            return group_df
        global_min  = min(flat)
        scaled_strs = [
            ",".join(str((d - global_min) + 1) for d in lst) if lst is not None else None
            for lst in parsed
        ]
        return group_df.with_columns(pl.Series("allDays", scaled_strs))

    resolved_groups = [
        _shift_group(group_df)
        for _, group_df in slice.group_by(schema.group_keys, maintain_order=True)
    ]
    return pl.concat(resolved_groups, how="vertical")


@task(name="resolve-alldays", persist_result=False)
def resolve_alldays(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Coordinator — chains 4 atomic subtasks sequentially for full allDays normalization.

    Pipeline:
      1. resolve_alldays_strip_brackets   — remove (.*) optional content
      2. resolve_alldays_resolve_ranges   — x~y / x|y → lower bound
      3. resolve_alldays_collapse_zero    — all-zero groups → [1]
      4. resolve_alldays_shift            — shift global_min → 1 per variant group

    Transaction hook:
      - Reads ``alldays_pattern_count_pre`` Prefect Variable written by handle-pattern-alldays
      - Compares pre/post counts; logs delta as resolution audit

    Writes back: ["allDays"]
    """
    logger = get_run_logger()

    # ── transaction hook: read pre-resolution count set by handle-pattern-alldays ──
    tracked_pattern = r"-\d+|\d+\|\d+|\d+~\d+|\(.*?\)|\b0\b"
    pre_count_raw   = Variable.get("alldays_pattern_count_pre", default=None)
    pre_count       = int(pre_count_raw) if pre_count_raw is not None else None

    if pre_count is not None:
        logger.info(f"[TRANSACTION] alldays_pattern_count_pre = {pre_count} (from handle-pattern-alldays)")
    else:
        logger.warning("[TRANSACTION] alldays_pattern_count_pre not set — handle-pattern-alldays may not have run")

    # ── sequential subtask chain ───────────────────────────────────────────────
    step1 = resolve_alldays_strip_brackets.submit(slice,  schema)
    step2 = resolve_alldays_resolve_ranges.submit(step1.result(), schema)
    step3 = resolve_alldays_collapse_zero.submit(step2.result(),  schema)
    data  = resolve_alldays_shift.submit(step3.result(),          schema).result()

    # ── transaction hook: post-resolution leak audit ───────────────────────────
    leaks = data.filter(
        pl.col("allDays").cast(pl.Utf8).str.contains(tracked_pattern, literal=False)
    )
    post_count = leaks.height
    Variable.set("alldays_pattern_count_post", str(post_count), overwrite=True)

    if pre_count is not None:
        resolved_count = pre_count - post_count
        logger.info(
            f"[TRANSACTION] alldays resolution audit: "
            f"pre={pre_count}  post={post_count}  resolved={resolved_count}"
        )
    if post_count > 0:
        logger.warning(f"[LEAK] {post_count} rows still match irregular allDays pattern after normalization")

    create_table_artifact(
        key="resolve-alldays",
        table=data.select(schema.group_keys + ["allDays"]).unique().to_dicts(),
        description="[P] allDays normalized — irregular patterns resolved via 4-step chain",
    )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=["allDays"],
        data=data,
        flag=False,
    )


@task(name="resolve-parted", persist_result=False)
def resolve_parted(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Deduplicate multi-part variant groups by identity columns.
    Duplicate rows within a variant group are flagged for drop via _drop_flag.
    Writes back: ["_drop_flag"]

    """
    logger = get_run_logger()

    data = slice.with_row_index("_rid")

    resolved_groups = []
    dropped_keys: list[pl.DataFrame] = []

    for _, group_df in data.group_by(schema.group_keys, maintain_order=True):
        before   = group_df.height
        deduped  = group_df.unique(subset=schema.identity_cols, keep="first", maintain_order=True)
        after    = deduped.height
        if after != before:
            dropped = group_df.join(deduped.select(["_rid"]), on="_rid", how="anti")
            dropped_keys.append(dropped.select(schema.group_keys).unique())
            logger.info(
                f"[RESOLVE] Dropped {before - after} duplicate rows in group "
                f"{group_df[schema.group_keys[0]][0]} / {group_df[schema.group_keys[1]][0]}"
            )
        resolved_groups.append(deduped)

    resolved = pl.concat(resolved_groups, how="vertical").drop("_rid")

    # Mark dropped rid positions in _drop_flag
    kept_rids = resolved.select("_rid") if "_rid" in resolved.columns else None
    # _rid already dropped above — use row position approach:
    # re-index original and anti-join to get drop candidates
    data_reindexed = slice.clone().with_row_index("_rid")
    resolved_rids  = pl.concat(resolved_groups, how="vertical").select("_rid")
    drop_rids      = data_reindexed.join(resolved_rids, on="_rid", how="anti").select("_rid")

    data_flagged = (
        data_reindexed
        .with_columns(
            pl.col("_rid").is_in(drop_rids["_rid"]).alias("_drop_flag")
        )
        .drop("_rid")
    )

    n_in  = slice.select(schema.group_keys).unique().height
    n_out = resolved.select(schema.group_keys).unique().height
    logger.info(f"[RESOLVED] Multi-parted variants fixed: {n_out} / {n_in}")

    if dropped_keys:
        all_dropped = pl.concat(dropped_keys).unique()
        create_table_artifact(
            key="resolve-parted-dropped",
            table=all_dropped.with_columns(pl.lit("NOT HANDLED").alias("Status")).to_dicts(),
            description="[N] Duplicate rows within multi-part variant groups — flagged for drop",
        )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=["_drop_flag"],
        data=data_flagged,
        flag=True,
    )
