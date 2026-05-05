"""
assembler.tasks.handle
~~~~~~~~~~~~~~~~~~~~~~~
Phase 1 handler @tasks.

Each task:
  - receives a frame slice (pre-filtered by mask computed in preprocess_flow)
  - returns a ResolverPatch
  - NEVER drops rows — _drop_flag=True marks candidates; drop is in merge_and_audit

src/ → Prefect reporting mapping
─────────────────────────────────
  src Reporter.report(df, name, status="H")  →  create_table_artifact(Status="HANDLED")
  src Reporter.report(df, name, status="P")  →  create_table_artifact(Status="PATCHED")
  src Reporter.report(df, name, status="N")  →  create_table_artifact(Status="NOT HANDLED")
  src Logger.info(msg)                        →  logger.info(msg)    [UI Logs tab]
  src Logger.warning(msg)                     →  logger.warning(msg) [UI Logs tab, orange]

H/P/N status is a literal "Status" column in the artifact table —
same as in the .reported.tsv files, visible in the Prefect UI Artifacts tab.
Log severity mirrors status: H→debug, P→info, N→warning.
"""
from __future__ import annotations

import polars as pl
from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable

from common.schemas import FieldSchema, ResolverPatch


def _report_and_flag (
    data: pl.DataFrame,
    artifact_df: pl.DataFrame,
    key: str,
    description: str,
    log_msg: str,
    logger,
    drop_mask: pl.Expr | None = None,
    status: str = "NOT HANDLED",
    ) -> pl.DataFrame:
    """
    Unified report + optional flag helper for all handler tasks.
    - Logs and creates artifact only if artifact_df is non-empty.
    - If drop_mask is provided, accumulates _drop_flag (True wins, never resets to False).
    - If drop_mask is None (PATCHED case), skips flagging.
    """
    if artifact_df.height:
        logger.info(log_msg)
        create_table_artifact(
            key=key,
            table=(
                artifact_df
                .with_columns(pl.lit(status).alias("Status"))
                .to_dicts()
            ),
            description=description,
        )
    if drop_mask is not None:
        data = data.with_columns(
            pl.when(drop_mask).then(pl.lit(True)).otherwise(pl.col("_drop_flag")).alias("_drop_flag")
        )
    return data


@task(name="handle-null", persist_result=False) # maybe to clean this - it would be fun to see this task calls other subtasks. 
def handle_null(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Three-step null handler. 
    
    Writes back: schema.condition_cols + ["_drop_flag"].
    
    Receives FieldSchema from the flow — Prefect validates + renders it in the UI.
    """
    logger = get_run_logger()
    data = slice


    # ─────────────────────────────────
    # Case 1 — handle_nan_in_condition:
    #   Fill null condition cols with "undefined" (don't drop).
    # ─────────────────────────────────
    
    null_cond = data.filter(
        pl.any_horizontal(*[pl.col(c).is_null() for c in schema.condition_cols])
    )
    data = _report_and_flag(
        data,
        artifact_df=null_cond.select("regimen").unique().sort("regimen"),
        key="null-condition-regimens-unique",
        description="[P] condition cols were null — filled with 'undefined', rows retained",
        log_msg=f"[REPORT] Filled {null_cond.height} rows with `undefined` condition — kept ({null_cond['regimen'].n_unique()} unique regimens)",
        logger=logger,
        drop_mask=None,
        status="PATCHED",
    )
    data = data.with_columns(
        [pl.col(c).fill_null("undefined") for c in schema.condition_cols]
    )

    #  ── ──────────────────────────────────────
    # Case 2 — handle_nan_in_group_keys:
    #   Rows with null in any group key — flag for drop.
    # ── ──────────────────────────────────────

    null_key_mask = pl.any_horizontal(*[pl.col(c).is_null() for c in schema.group_keys])
    null_key_rows = data.filter(null_key_mask)
    dropped_groups = null_key_rows.select(schema.group_keys_w_reg).unique()
    data = _report_and_flag(
        data,
        artifact_df=dropped_groups,
        key="null-group-keys",
        description="[N] Groups dropped: null in condition_cui / regimen_cui / variant_key",
        log_msg=f"[REPORT] Removed {dropped_groups.height} groups due to nulls in group keys — flagging for drop",
        logger=logger,
        drop_mask=null_key_mask,
    )

    # ── ──────────────────────────────────────
    # Case 3 — handle_null_in_sigs:
    # Entire variant group flagged if any sig field null anywhere in it.
    # ── ──────────────────────────────────────

    sig_null      = pl.any_horizontal(*[pl.col(c).is_null() for c in schema.sig_cols])
    group_flag    = sig_null.any().over(schema.group_keys)
    affected_variants = data.filter(group_flag).select(schema.group_keys).unique()
    data = _report_and_flag(
        data,
        artifact_df=affected_variants,
        key="null-sig-variants",
        description=f"[N] sig-related fields can not be empty: ({', '.join(schema.sig_cols)}) — Flagged for drop.",
        log_msg=f"[REPORT] {affected_variants.height} variant groups have null sig fields — flagging for drop",
        logger=logger,
        drop_mask=group_flag,
    )

    row_indices = slice["_row_nr"].to_list() # affects all input slice indices... 
    
    return ResolverPatch(
        row_indices=row_indices,
        columns=schema.condition_cols + ["_drop_flag"],
        data=data,
        flag=True,
    )


@task(name="handle-role", persist_result=False)
def handle_role(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Flag secondary_systemic / locoregional component rows for drop.
    Receives the ENTIRE frame — group stats computed here.
    Writes back: ["_drop_flag"]
    """
    logger = get_run_logger()
    data = slice

    total_variants = slice.select(schema.group_keys).unique().height
    all_dropped_keys: list[pl.DataFrame] = []

    col_name, flag_values = next(iter(schema.role_cols.items()))

    for fv in flag_values:
        drop_mask    = pl.col(col_name) == fv
        dropped_rows = data.filter(drop_mask)
        dropped_keys = dropped_rows.select(schema.group_keys).unique()
        all_dropped_keys.append(dropped_keys)

        variants = dropped_keys.height
        ratio    = round((variants / total_variants) * 100, 2) if total_variants else 0.0

        data = _report_and_flag(
            data,
            artifact_df=dropped_keys,
            key=f"component-role-{fv.replace(' ', '-')}",
            description=f"[N] Filtered component by role '{col_name}' == '{fv}'.",
            log_msg=(
                f"[REPORT] Variants with '{fv}' role: {dropped_rows.height} rows / "
                f"{variants}/{total_variants} variant groups ({ratio}%) — flagging for drop"
            ),
            logger=logger,
            drop_mask=drop_mask,
        )

    if all_dropped_keys:
        total_dropped = pl.concat(all_dropped_keys).unique().height
        ratio = round((total_dropped / total_variants) * 100, 2) if total_variants else 0.0
        logger.info(
            f"[REPORT] Total dropped variants: {total_dropped}/{total_variants} ({ratio}%) "
            f"due to disallowed component roles."
        )

    row_indices = slice["_row_nr"].to_list()

    return ResolverPatch(
        row_indices=row_indices,
        columns=["_drop_flag"],
        data=data,
        flag=True,
    )


@task(name="handle-regimen-stats", persist_result=False)
def handle_regimen_stats(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Log regimen-level stats and artifact all unique groups — no rows flagged.
    Run before RT/imbalanced filters so counts reflect the unfiltered frame.
    Patch: none — logs & artifacts only
    """
    logger = get_run_logger()

    unique_regimens = slice.select("regimen_cui").n_unique()
    logger.info(f"[REPORT] Total regimens (unique) - before filtering: {unique_regimens}")

    unique_regimens_per_conditions = (
        slice.group_by("condition_cui")
        .agg(pl.col("regimen_cui").n_unique().alias("n_regimens"))
        .select(pl.col("n_regimens")).sum().item()
    )
    logger.info(f"[REPORT] Total regimens per condition (unique): {unique_regimens_per_conditions}")

    unique_groups = slice.select(schema.group_keys_w_cui).unique()
    create_table_artifact(
        key="handle-regimen-unique-groups",
        table=unique_groups.to_dicts(),
        description="All unique condition/regimen/variant groups before regimen filtering",
    )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=[],
        data=slice,
        flag=False,
    )


@task(name="handle-regimen-rt", persist_result=False)
def handle_regimen_rt(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Flag RT-containing regimens for drop.
    Matches: RT / SCRT / CSRT / WBRT / WB-XRT (space/paren-bounded) and "Whole brain irradiation".
    Writes back: ["_drop_flag"]
    """
    logger = get_run_logger()
    data = slice

    rt_pattern = r"(?:^|[\s,/])\(?(?:RT|SCRT|CSRT|WBRT|WB-XRT)\)?(?:[\s,)\-]|$)" # this is duplicate -read it from mask instead
    rt_mask = (
        pl.col("regimen").cast(pl.Utf8).str.contains(rt_pattern, literal=False)
        | (pl.col("regimen") == "Whole brain irradiation")
    )
    rt_unique = data.filter(rt_mask).select("regimen").unique()
    data = _report_and_flag(
        data,
        artifact_df=rt_unique,
        key="handle-regimen-rt",
        description="[N] Radiotherapy-containing regimens flagged for drop",
        log_msg=f"[REPORT] Regimens containing RT: {rt_unique.height} — flagging for drop",
        logger=logger,
        drop_mask=rt_mask,
    )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=["_drop_flag"],
        data=data,
        flag=True,
    )


@task(name="handle-regimen-imbalanced", persist_result=False)
def handle_regimen_imbalanced(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Flag regimens where component count differs across variants for the same regimen.
    Writes back: ["_drop_flag"]
    """
    logger = get_run_logger()
    data = slice

    bad_groups = (
        data.group_by(schema.group_keys)
        .agg(pl.col("component").n_unique().alias("component_count"))
        .group_by(schema.group_keys_regimen)
        .agg(pl.col("component_count").n_unique().alias("count_of_component_counts"))
        .filter(pl.col("count_of_component_counts") > 1)
        .select(schema.group_keys_regimen)
    )
    imbalanced_rows = data.join(bad_groups, on=schema.group_keys_regimen, how="inner")

    data = (
        data
        .join(bad_groups.with_columns(pl.lit(True).alias("_imb")), on=schema.group_keys_regimen, how="left")
        .with_columns(pl.col("_imb").fill_null(False).alias("_imbalanced"))
        .drop("_imb")
    )

    variant_component_report = (
        imbalanced_rows
        .group_by(schema.group_keys)
        .agg([
            pl.col("component").n_unique().alias("component_count_in_this_variant"),
            pl.col("component").unique().alias("_components_unsorted"),
        ])
        .with_columns(pl.col("_components_unsorted").list.sort().alias("components"))
        .drop("_components_unsorted")
        .join(imbalanced_rows.select(schema.group_keys + ["regimen"]).unique(), on=schema.group_keys, how="left")
        .select(["regimen", "variant_key", "component_count_in_this_variant", "components"])
        .with_columns(pl.col("components").list.join(", "))
        .unique(subset=["regimen", "variant_key", "components"])
        .sort(["regimen", "variant_key"])
    )
    data = _report_and_flag(
        data,
        artifact_df=variant_component_report,
        key="handle-regimen-imbalanced",
        description="[N] Regimens with inconsistent component counts across variants — flagged for drop",
        log_msg=(
            f"[REPORT] Removed {imbalanced_rows.select('regimen').unique().height} regimens "
            f"— inconsistent component count across variants"
        ),
        logger=logger,
        drop_mask=pl.col("_imbalanced"),
    )
    data = data.drop("_imbalanced")

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=["_drop_flag"],
        data=data,
        flag=True,
    )


@task(name="handle-variant-stats", persist_result=False)
def handle_variant_stats(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Log and artifact total variant counts per regimen.
    Patch: none — logs & artifacts only
    """ # refecence to SRc ERR!
    logger = get_run_logger()

    uniq = slice.select(schema.group_keys).unique()
    n_variants = uniq.height
    regimen_variants = (
        uniq.select(["regimen_cui", "variant_key"])
        .unique()
        .group_by("regimen_cui")
        .agg(pl.col("variant_key").n_unique().alias("n_variant"))
    )
    n_regimen_variants = regimen_variants.select(pl.col("n_variant").sum()).item()
    logger.info(f"[REPORT] Total number of variants: {n_variants} ({n_regimen_variants} unique)")

    create_table_artifact(
        key="handle-variant-regimen-variants",
        table=regimen_variants.to_dicts(),
        description="Unique variant count per regimen",
    )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=[],
        data=slice,
        flag=False,
    )


@task(name="handle-variant-multipart", persist_result=False)
def handle_variant_multipart(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Detect and artifact multi-part vs single-part sig variants.
    Multi-part = component is duplicated within a variant group (step_number > 1).
    Patch: none — logs & artifacts only, routes to resolve-parted
    """
    logger = get_run_logger()

    sig_types = (
        slice.with_columns(pl.col("component").is_duplicated().over(schema.group_keys).alias("dup"))
        .group_by(schema.group_keys)
        .agg(pl.col("dup").any().alias("is_multi"))
        .with_columns(
            pl.when("is_multi")
            .then(pl.lit("Multi-part Sig"))
            .otherwise(pl.lit("Single-part Sig"))
            .alias("sig_type")
        )
    )
    s = slice.join(sig_types, on=schema.group_keys, how="left")

    counts = (
        s.group_by("sig_type")
        .agg([
            pl.len().alias("count"),
            pl.col("regimen").n_unique().alias("n_regimens"),
            pl.col("variant_key").n_unique().alias("n_variants"),
        ])
    )
    for row in counts.iter_rows(named=True):
        logger.info(f"[REPORT] {row['sig_type']}: {row['count']} rows / {row['n_variants']} unique variants")

    create_table_artifact(
        key="handle-variant-sig-type-counts",
        table=counts.to_dicts(),
        description="Multi-part vs single-part sig counts",
    )

    multi_keys  = s.filter(pl.col("sig_type") == "Multi-part Sig").select(schema.group_keys + ["regimen"]).unique()
    single_keys = s.filter(pl.col("sig_type") == "Single-part Sig").select(schema.group_keys + ["regimen"]).unique()

    if multi_keys.height:
        create_table_artifact(
            key="handle-variant-multi-part",
            table=multi_keys.with_columns(pl.lit("PATCHED").alias("Status")).to_dicts(),
            description="[P] Multi-part sig variants — will be resolved by resolve-parted",
        )
    if single_keys.height:
        create_table_artifact(
            key="handle-variant-single-part",
            table=single_keys.with_columns(pl.lit("HANDLED").alias("Status")).to_dicts(),
            description="[H] Single-part sig variants — no action needed",
        )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=[],
        data=slice,
        flag=False,
    )


@task(name="handle-pattern-alldays", persist_result=False)
def handle_pattern_alldays(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Log and artifact variant groups with irregular allDays patterns.
    Irregular patterns: -\\d+ | \\d+\\|\\d+ | \\d+~\\d+ | \\(.*?\\) | \\b0\\b
    Groups are NOT modified here — normalized by resolve-alldays.
    Patch: none — logs & artifacts only
    """
    logger = get_run_logger()

    # slice is pre-filtered by compute_alldays_mask — every row here already matched
    invalid_groups = slice.select(schema.group_keys).unique()
    invalid_count  = invalid_groups.height

    # ── transaction hook: publish pre-resolution count for resolve-alldays ────
    # resolve_alldays reads this Variable to compute the resolution delta audit.
    Variable.set("alldays_pattern_count_pre", str(invalid_count), overwrite=True)
    logger.info(f"[TRANSACTION] alldays_pattern_count_pre set → {invalid_count}")

    logger.info(
        f"[REPORT] Variants with irregular allDays pattern: {invalid_count} — routed to resolve-alldays"
    )
    if invalid_count:
        create_table_artifact(
            key="handle-pattern-alldays",
            table=invalid_groups.with_columns(pl.lit("PATCHED").alias("Status")).to_dicts(),
            description="[P] Variant groups with irregular allDays — will be normalized by resolve-alldays",
        )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=[],
        data=slice,
        flag=False,
    )


@task(name="handle-pattern-indef-cycles", persist_result=False)
def handle_pattern_indef_cycles(slice: pl.DataFrame, schema: FieldSchema) -> ResolverPatch:
    """
    Log and artifact variant groups with indeterminate cycle length units or non-numeric bounds.
    Groups are NOT dropped — patched by resolve-indef-timing + resolve-indef-bounds.
    Patch: none — logs & artifacts only
    """
    logger = get_run_logger()

    non_numeric_pattern = r"[^\d\.]" # can this be from mask?
    bad_bounds_mask = (
        pl.col("cycle_length_lb").str.contains(non_numeric_pattern, literal=False)
        | pl.col("cycle_length_ub").str.contains(non_numeric_pattern, literal=False)
    )
    indefinite_mask   = (pl.col("cycle_length_unit") == "indeterminate") | bad_bounds_mask
    group_has_indef   = indefinite_mask.any().over(schema.group_keys)

    invalid_groups  = slice.filter(group_has_indef).select(schema.group_keys).unique()
    invalid_count   = invalid_groups.height
    invalid_variant_count = (
        slice.filter(group_has_indef)
        .group_by("regimen_cui")
        .agg(pl.col("variant_key").n_unique().alias("n_variant"))
        .select(pl.col("n_variant").sum())
        .item()
    ) if invalid_count else 0

    # lb ≠ ub mismatch count (numeric rows only)
    numeric_pattern = r"^\d+(\.\d+)?$"
    lb_ub_mismatch_count = (
        slice
        .filter(
            pl.col("cycle_length_lb").str.contains(numeric_pattern, literal=False)
            & pl.col("cycle_length_ub").str.contains(numeric_pattern, literal=False)
        )
        .with_columns([
            pl.col("cycle_length_lb").cast(pl.Float64).alias("_lb"),
            pl.col("cycle_length_ub").cast(pl.Float64).alias("_ub"),
        ])
        .filter(pl.col("_lb") != pl.col("_ub"))
        ["regimen"].n_unique()
    )

    logger.info(
        f"[REPORT] Variants with indefinite cycle lengths: {invalid_count} groups "
        f"({invalid_variant_count} unique) — will be resolved by resolve-indef-bounds"
    )
    logger.info(f"[REPORT] Unique regimens with lb ≠ ub (numeric): {lb_ub_mismatch_count}")
    if invalid_count:
        create_table_artifact(
            key="handle-pattern-indef-cycles",
            table=invalid_groups.with_columns(pl.lit("PATCHED").alias("Status")).to_dicts(),
            description="[P] Variant groups with indeterminate/non-numeric cycle lengths — routed to resolve-indef-bounds",
        )

    return ResolverPatch(
        row_indices=slice["_row_nr"].to_list(),
        columns=[],
        data=slice,
        flag=False,
    )