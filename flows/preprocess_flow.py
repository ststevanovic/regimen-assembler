"""
assembler.flows.preprocess_flow
-------------------------------
Phase 1 — preprocessing flow.

Architecture:
  1. resolve_variant_key  — blocking prerequisite
  2. Masks computed in flow (pure polars)
  3. Parallel fan-out via .submit()
  4. merge_and_audit — apply ResolverPatches, emit artifact, single drop
"""
from __future__ import annotations

import polars as pl
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect.transactions import transaction

from common.schemas import FieldSchema, ResolverPatch

_SCHEMA = FieldSchema()

from tasks.resolve import (
    resolve_variant_key,
    resolve_indef_timing,
    resolve_indef_bounds,
    resolve_alldays,
    resolve_alldays_strip_brackets,
    resolve_alldays_resolve_ranges,
    resolve_alldays_collapse_zero,
    resolve_alldays_shift,
    resolve_parted,
)
from tasks.handle import (
    handle_null,
    handle_role,
    handle_regimen_stats,
    handle_regimen_rt,
    handle_regimen_imbalanced,
    handle_variant_stats,
    handle_variant_multipart,
    handle_pattern_alldays,
    handle_pattern_indef_cycles,
)


# ───── Masks ─────────────────────────────────────────────────────────────────

def compute_null_mask(frame: pl.DataFrame) -> pl.Series:
    """
    True for any row whose variant group has any null in null_cols.
    Whole group goes to handle_null so the group-level fill/drop is consistent.
    """
    any_null = pl.any_horizontal(*[pl.col(c).is_null() for c in _SCHEMA.null_cols])
    return (
        frame
        .with_columns(any_null.any().over(_SCHEMA.group_keys).alias("_null_mask"))
        ["_null_mask"]
    )


def compute_role_mask(frame: pl.DataFrame) -> pl.Series:
    """
    True for rows whose variant group contains a disallowed component_role.
    """
    col_name, flag_values = next(iter(_SCHEMA.role_cols.items()))
    has_role = pl.col(col_name).is_in(flag_values)
    return (
        frame
        .with_columns(has_role.any().over(_SCHEMA.group_keys).alias("_role_mask"))
        ["_role_mask"]
    )


def compute_parted_mask(frame: pl.DataFrame) -> pl.Series:
    """
    True for variant groups where component is duplicated (multi-part sigs).
    """
    return (
        frame
        .with_columns(
            (pl.col("component").count().over(_SCHEMA.group_keys) !=
             pl.col("component").n_unique().over(_SCHEMA.group_keys))
            .alias("_parted_mask")
        )
        ["_parted_mask"]
    )


def compute_alldays_mask(frame: pl.DataFrame) -> pl.Series:
    """
    True for variant groups where any allDays value matches an irregular pattern.
    Passed as a pre-filtered slice to handle_pattern_alldays and resolve_alldays.
    """
    tracked = r"-\d+|\d+\|\d+|\d+~\d+|\(.*?\)|\b0\b"
    has_pattern = pl.col("allDays").cast(pl.Utf8).str.contains(tracked, literal=False)
    return (
        frame
        .with_columns(has_pattern.any().over(_SCHEMA.group_keys).alias("_alldays_mask"))
        ["_alldays_mask"]
    )


def compute_indef_mask(frame: pl.DataFrame) -> pl.Series:
    """
    True for variant groups with indeterminate cycle unit or non-numeric bounds.
    """
    non_numeric = r"[^\d\.]"
    bad_bounds = (
        pl.col("cycle_length_lb").str.contains(non_numeric, literal=False)
        | pl.col("cycle_length_ub").str.contains(non_numeric, literal=False)
    )
    indef = (pl.col("cycle_length_unit") == "indeterminate") | bad_bounds
    return (
        frame
        .with_columns(indef.any().over(_SCHEMA.group_keys).alias("_indef_mask"))
        ["_indef_mask"]
    )


# ── Merge + audit + drop (transactional) ─────────────────────────────────────
#
# Transaction semantics
# ---------------------
# Prefect 3 `transaction()` wraps the patch-application loop as an atomic unit:
#
#   COMMIT path  — all patches applied cleanly →
#                  drop flagged rows, emit audit artifact, return cleaned frame
#
#   ROLLBACK path — any patch raises →
#                  @rollback_hook fires: emits a rollback artifact so the failure
#                  is visible in the UI, then re-raises so the flow run fails
#                  rather than silently returning a half-patched frame.
#
# Why this matters here:
#   The patch loop mutates `result` column-by-column across N patches.
#   Without a transaction, a crash on patch K leaves columns [0..K-1] written
#   and [K..N] untouched — a split-brain frame that silently propagates.
#   With a transaction, either ALL patches commit or NONE do (rollback to snapshot).
#
# Polars immutability makes the snapshot free: `frame` is a value type —
# each `with_columns()` returns a new frame; `snapshot` holds the original.

def _emit_rollback_artifact(txn) -> None:
    """Fired by Prefect on transaction failure — writes a rollback audit artifact."""
    try:
        create_table_artifact(
            key="merge-and-audit-rollback",
            table=[{"event": "ROLLBACK", "reason": str(txn.get("error", "unknown"))}],
            description="[ROLLBACK] merge_and_audit transaction failed — frame reverted to pre-patch snapshot",
        )
    except Exception:
        pass  # best-effort; don't mask the original error


@task(name="merge-and-audit")
def merge_and_audit(frame: pl.DataFrame, patches: list[ResolverPatch]) -> pl.DataFrame:
    """
    Apply all ResolverPatches to frame atomically.

    Transaction contract:
      - COMMIT  → all patches applied, _drop_flag accumulated, rows dropped, artifact emitted
      - ROLLBACK → any patch failure → original frame returned, rollback artifact emitted

    The `snapshot` is the original immutable frame; Polars `with_columns` never
    mutates in place so rollback is always safe regardless of how far the loop got.
    """
    logger   = get_run_logger()
    snapshot = frame          # Polars is immutable — free rollback target

    with transaction(key="merge-and-audit") as txn:
        txn.on_rollback_hooks.append(_emit_rollback_artifact)
        result = snapshot

        try:
            for patch in patches:
                if not patch.columns:
                    continue
                patch_data = patch.data
                for col in patch.columns:
                    if col not in patch_data.columns:
                        continue
                    if col == "_drop_flag":
                        # accumulate: True wins
                        existing = result["_drop_flag"].to_list()
                        patched  = patch_data["_drop_flag"].to_list()
                        for i, idx in enumerate(patch.row_indices):
                            existing[idx] = existing[idx] or patched[i]
                        result = result.with_columns(pl.Series("_drop_flag", existing))
                    else:
                        # overwrite column values at row_indices
                        updated = result[col].to_list()
                        patched = patch_data[col].to_list()
                        for i, idx in enumerate(patch.row_indices):
                            updated[idx] = patched[i]
                        result = result.with_columns(pl.Series(col, updated))

        except Exception as exc:
            txn["error"] = str(exc)
            logger.error(
                f"[TRANSACTION] Patch application failed: {exc} — "
                "rolling back to pre-patch snapshot"
            )
            raise  # triggers @rollback_hook + marks flow run failed

        # ── COMMIT path ───────────────────────────────────────────────────────
        flagged = result.filter(pl.col("_drop_flag"))
        logger.info(f"[AUDIT] Rows flagged for drop: {flagged.height} / {result.height}")
        if flagged.height:
            create_table_artifact(
                key="merge-and-audit-dropped",
                table=(
                    flagged
                    .select(_SCHEMA.group_keys + ["regimen"])
                    .unique()
                    .with_columns(pl.lit("NOT HANDLED").alias("Status"))
                    .to_dicts()
                ),
                description="[N] All variant groups dropped across all handle/resolve tasks",
            )

        cleaned = result.filter(~pl.col("_drop_flag")).drop(["_drop_flag", "_row_nr"])
        logger.info(f"[AUDIT] Frame after drop: {cleaned.height} rows — COMMITTED")
        return cleaned


# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(name="preprocess", log_prints=True)
def preprocess_flow(frame: pl.DataFrame) -> pl.DataFrame:

    # ── prerequisite (blocking) ───────────────────────────────────────────────
    frame_keyed = resolve_variant_key(frame, _SCHEMA)
    frame_keyed = frame_keyed.with_row_index("_row_nr")
    frame_keyed = frame_keyed.with_columns(pl.lit(False).alias("_drop_flag"))

    # ── masks computed in flow ────────────────────────────────────────────────
    mask_null    = compute_null_mask(frame_keyed)
    mask_role    = compute_role_mask(frame_keyed)
    mask_parted  = compute_parted_mask(frame_keyed)
    mask_alldays = compute_alldays_mask(frame_keyed)
    mask_indef   = compute_indef_mask(frame_keyed)

    # ── parallel fan-out ──────────────────────────────────────────────────────

    # handle phase (log/flag/drop decisions)
    fut_null         = handle_null.submit(frame_keyed.filter(mask_null), _SCHEMA)
    fut_role         = handle_role.submit(frame_keyed, _SCHEMA)
    fut_reg_stats    = handle_regimen_stats.submit(frame_keyed, _SCHEMA)
    fut_reg_rt       = handle_regimen_rt.submit(frame_keyed, _SCHEMA)
    fut_reg_imb      = handle_regimen_imbalanced.submit(frame_keyed, _SCHEMA)
    fut_var_stats    = handle_variant_stats.submit(frame_keyed, _SCHEMA)
    fut_var_multi    = handle_variant_multipart.submit(frame_keyed, _SCHEMA)
    fut_pat_alldays  = handle_pattern_alldays.submit(frame_keyed, _SCHEMA)
    fut_pat_indef    = handle_pattern_indef_cycles.submit(frame_keyed, _SCHEMA)

    # resolve phase (value transforms)
    # NOTE: pass full frame so row_indices in ResolverPatch are absolute positions.
    # _fix_timing and patch_mask are both no-ops on rows that don't need patching.
    fut_timing  = resolve_indef_timing.submit(frame_keyed, _SCHEMA)
    fut_bounds  = resolve_indef_bounds.submit(frame_keyed, _SCHEMA)
    fut_alldays = resolve_alldays.submit(frame_keyed.filter(mask_alldays), _SCHEMA)
    fut_parted  = resolve_parted.submit(frame_keyed.filter(mask_parted), _SCHEMA)

    return merge_and_audit(
        frame_keyed,
        [
            fut_null.result(),
            fut_role.result(),
            fut_reg_rt.result(),
            fut_reg_imb.result(),
            fut_timing.result(),
            fut_bounds.result(),
            fut_alldays.result(),
            fut_parted.result(),
            # read-only tasks — collected for audit completeness
            fut_reg_stats.result(),
            fut_var_stats.result(),
            fut_var_multi.result(),
            fut_pat_alldays.result(),
            fut_pat_indef.result(),
        ],
        return_state=False,
    )
