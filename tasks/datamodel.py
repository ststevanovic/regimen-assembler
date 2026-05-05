"""
assembler.tasks.datamodel
~~~~~~~~~~~~~~~~~~~~~~~~~~
Phase 3 – Data-model tasks.

Execution order inside wrapup_flow:
  1.  task_build_regimens   — prerequisite, runs first (blocking)
        Calls shortstring_mapping_stats (tools/regimen_formatter) for Prefect artifact;
        build_final_regimens logic is inline here.
        Produces regimens.tsv (shortString-deduped schedule index).
        Emits a Prefect table artifact with mapping stats.

  2–5. Concurrent fan-out (all read from regimens_full.tsv):
        task_reg_groups    → regimengroups.tsv
        task_valid_drugs   → validdrugs.tsv
        task_routes        → regimens_drugs.tsv + regimens_drugs_deploy.tsv
        task_short_strings → regimens_shortStrings.tsv

Caching
-------
All fan-out tasks use file_hash_cache_key — same input file bytes + task version
= cache hit regardless of re-run time.  Expiry: 24 h.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact

from common.cache_utils import file_hash_cache_key
from common.config import AssemblerConfig
from tools.regimen_formatter import shortstring_mapping_stats

_CACHE_TTL = timedelta(hours=24)


# ── Prerequisite — runs blocking before fan-out ––––––––––––––––––––––––

@task(name="build-regimens", cache_key_fn=file_hash_cache_key, cache_expiration=_CACHE_TTL)
def task_build_regimens(cfg: AssemblerConfig, regimens_full: Path) -> Path:
    """
    Produce regimens.tsv — the shortString-deduped authoritative schedule index.

    Steps:
      1. Load regimens_full.tsv (all rows, produced at end of transform_flow)
      2. analyze_shortstring_regimen_mapping — compute stats, emit as artifact
      3. build_final_regimens — dedup on shortString, sort, write regimens.tsv

    """
    logger = get_run_logger()

    frame = pd.read_csv(regimens_full, sep="\t")
    logger.info(f"[build-regimens] input shape: {frame.shape}")

    stats = shortstring_mapping_stats(frame)
    logger.info(f"[build-regimens] shortstring mapping stats: {stats}")

    # Emit stats as Prefect artifact visible in UI run page
    create_table_artifact(
        key="shortstring-mapping-stats",
        table=[{"metric": k, "value": str(v)} for k, v in stats.items()],
        description="shortString ↔ regimen many-to-many mapping statistics",
    )

    # Warn on undefined conditions
    n_undefined = int((frame["conditionCode"] == "undefined").sum())
    if n_undefined:
        logger.warning(f"[build-regimens] {n_undefined} rows have undefined conditionCode — retained in output")

    # ── build_final_regimens (inlined) ────────────────────────────────────
    required_cols = ["regCode", "shortString", "regName", "condition", "conditionCode"]
    missing = [c for c in required_cols if c not in frame.columns]
    if missing:
        raise ValueError(f"[build-regimens] missing required columns: {missing}")

    # Deduplicate on shortString alone — one row per unique schedule.
    # Full condition×regimen expansion lives in regimens_shortStrings.tsv
    # (produced by task_short_strings via generate_shortString_table).
    final = (
        frame
        .drop_duplicates(subset=["shortString"], keep="first")
        .sort_values(by=["condition", "regCode", "shortString"])
        .reset_index(drop=True)
    )
    logger.info(f"[build-regimens] deduped {len(frame)} → {len(final)} rows")

    out_path = cfg.workdir / "regimens.tsv"
    final.to_csv(out_path, sep="\t", index=False)
    logger.info(f"[build-regimens] wrote {out_path}")
    return out_path


# ── Concurrent fan-out ─────────────────────────────────────────────────────


@task(name="generate-reg-groups", cache_key_fn=file_hash_cache_key, cache_expiration=_CACHE_TTL)
def task_reg_groups(cfg: AssemblerConfig, regimens_full: Path) -> Path:
    """
    Stage 4a — regimen groups.
    Assigns known regGroup from rgroups_template; new regimens get a random group.
    Writes regimengroups.tsv.
    """
    import random
    logger = get_run_logger()

    df  = pd.read_csv(regimens_full, sep="\t")
    ref = pd.read_csv(cfg.rgroups_template_path, sep="\t")

    required = ["Var1", "regGroup"]
    missing  = set(required) - set(ref.columns)
    if missing:
        raise ValueError(f"[reg-groups] missing columns in template: {missing}")

    known    = set(ref["Var1"].unique())
    new_regs = set(df["regName"].unique()) - known

    if new_regs:
        new_entries = pd.DataFrame({
            "Var1":     list(new_regs),
            "regGroup": [random.choice(ref["regGroup"].dropna().unique()) for _ in new_regs],
        })
        updated = pd.concat([ref, new_entries], ignore_index=True)
        logger.info(f"[reg-groups] {len(new_regs)} new regimens assigned random group")
    else:
        updated = ref
        logger.info("[reg-groups] all regimens already in reference")

    out = cfg.workdir / "regimengroups.tsv"
    updated.to_csv(out, sep="\t", index=False)
    logger.info(f"[reg-groups] wrote {out}")
    create_table_artifact(
        key="reg-groups-new",
        table=[{"regName": r} for r in sorted(new_regs)] or [{"regName": "(none)"}],
        description="New regimens assigned random regGroup",
    )
    return out


@task(name="generate-valid-drugs", cache_key_fn=file_hash_cache_key, cache_expiration=_CACHE_TTL)
def task_valid_drugs(cfg: AssemblerConfig, regimens_full: Path) -> Path:
    """
    Stage 4b — valid drugs.
    Maps drug_concepts vocab to components in regimens_full; logs unmapped.
    Writes validdrugs.tsv.
    """
    logger = get_run_logger()

    RELMAP = {
        "name":             "concept_name",
        "concept_id":       "concept_id",
        "Manual":           "concept_id",
        "concept_me":       "concept_name",
        "valid_concept_id": "valid_concept_id",
        "domain_id":        "domain_id",
        "concept_class_id": "concept_class_id",
        "Manual_Req":       "invalid_reason",
    }

    fin        = pd.read_csv(regimens_full, sep="\t")
    vd_query   = pd.read_csv(cfg.drug_concepts_path)

    unique_components = fin["component"].unique().tolist()
    components_set    = set(c.lower() for c in unique_components)
    vd_concepts       = set(c.lower() for c in vd_query["concept_name"].unique())

    shared   = components_set & vd_concepts
    unmapped = components_set - vd_concepts

    logger.info(f"[valid-drugs] total components: {len(components_set)}")
    logger.info(f"[valid-drugs] mapped: {len(shared)}  unmapped: {len(unmapped)}")
    if unmapped:
        logger.warning(f"\n[WARNING] {len(unmapped)} component(s) not found in valid drugs vocabulary:")
        for comp in sorted(unmapped):
            original = next((c for c in unique_components if c.lower() == comp), comp)
            logger.warning(f"  - {original}")
        logger.info("\n[NOTE] Unmapped components typically include:")
        logger.info("  * New/experimental drugs not yet in Athena")
        logger.info("  * Non-drug interventions (e.g., External beam radiotherapy, BCG vaccine)")
        logger.info("  * Biological/cell therapies (e.g., Allogeneic stem cells)")
        logger.info("  * Treatment categories (e.g., Androgen-deprivation therapy)")
        logger.info("  * All retained in pipeline; unmapped entries will not match OMOP concepts.")
    else:
        logger.info("\n[SUCCESS] All components have valid OMOP concept mappings!")

    for new_col, src_col in RELMAP.items():
        if new_col and src_col in vd_query.columns:
            vd_query[new_col] = vd_query[src_col].values

    out_cols = list(filter(None, RELMAP.keys()))
    missing  = set(out_cols) - set(vd_query.columns)
    if missing:
        raise ValueError(f"[valid-drugs] missing columns: {missing}")

    out = cfg.workdir / "validdrugs.tsv"
    vd_query[out_cols].to_csv(out, sep="\t", index=False)
    logger.info(f"[valid-drugs] wrote {out}")
    create_table_artifact(
        key="valid-drugs-unmapped",
        table=[{"component": c} for c in sorted(unmapped)] or [{"component": "(none)"}],
        description="Components not found in drug_concepts vocabulary",
    )
    return out


@task(name="generate-routes", cache_key_fn=file_hash_cache_key, cache_expiration=_CACHE_TTL)
def task_routes(cfg: AssemblerConfig, regimens_full: Path) -> Path:
    """
    Stage 4c — route table.
    One row per drug–route–regimen combination.
    Writes regimens_drugs.tsv + regimens_drugs_deploy.tsv.
    """
    logger = get_run_logger()

    df = pd.read_csv(regimens_full, sep="\t")

    component2cui = (
        df[["component", "componentCode"]]
        .drop_duplicates()
        .set_index("component")["componentCode"]
        .to_dict()
    )

    exclude = {"Not specified", "nan", None, ""}
    route_data = []

    logger.info(f"\n[ROUTE ASSIGNMENT] Processing route table from {regimens_full}")

    for comp, cui in component2cui.items():
        comp_rows  = df[df["component"] == comp]
        pairs      = comp_rows[["route", "regName"]].drop_duplicates()
        valid      = pairs[~pairs["route"].astype(str).isin(exclude)]

        if len(comp_rows) > len(pairs):
            logger.info(
                f"[ROUTE EXPLOSION] Component '{comp}' (CODE: {cui}) "
                f"has {len(comp_rows)} rows but {len(pairs)} "
                f"unique route-regimen pairs."
            )
            duplicated  = comp_rows.duplicated(subset=["route", "regName"], keep=False)
            dup_summary = (
                comp_rows[duplicated]
                .groupby(["route", "regName"])
                .size()
                .reset_index(name="count")
                .to_dict(orient="records")
            )
            logger.info(
                f"[ROUTE EXPLOSION DETAILS] Component '{comp}' (CUI: {cui}) "
                f"duplicate route-regimen pairs: {dup_summary}"
            )

        if valid.empty:
            route_data.append({"cui": cui, "drug": comp, "route": "Not specified", "regimen": None})
        else:
            for _, row in valid.iterrows():
                route_data.append({"cui": cui, "drug": comp, "route": row["route"], "regimen": row["regName"]})

    route_df = pd.DataFrame(route_data)

    out_full   = cfg.workdir / "regimens_drugs.tsv"
    out_deploy = cfg.workdir / "regimens_drugs_deploy.tsv"
    route_df.to_csv(out_full, sep="\t", index=False)
    route_df[["regimen", "drug", "route"]].to_csv(out_deploy, sep="\t", index=False)
    logger.info(f"[routes] wrote {out_full} + {out_deploy}")
    return out_full


@task(name="generate-short-strings", cache_key_fn=file_hash_cache_key, cache_expiration=_CACHE_TTL)
def task_short_strings(cfg: AssemblerConfig, regimens_full: Path) -> Path:
    """
    Stage 4d — shortString lookup.
    One row per shortString×regimen×condition combination with repeat count.
    Writes regimens_shortStrings.tsv.
    """
    logger = get_run_logger()

    df = pd.read_csv(regimens_full, sep="\t")

    shortstring_df = (
        df.groupby("shortString")[["regName", "condition"]]
        .value_counts()
        .reset_index()
        .rename(columns={"count": "repeats", "regName": "regimen"})
    )
    codes = pd.factorize(shortstring_df["shortString"])[0]
    shortstring_df.insert(0, "shortString_ID", codes + 1)

    out = cfg.workdir / "regimens_shortStrings.tsv"
    shortstring_df.to_csv(out, sep="\t", index=False)
    logger.info(f"[short-strings] wrote {out}")
    return out
