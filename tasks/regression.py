"""
assembler.tasks.regression
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Regression comparison task.

Compares ref_dir vs new_dir TSV outputs, emits results as Prefect artifacts:
  - One markdown artifact: summary (tables compared, score counts, missing)
  - One table artifact per compared table: per-column scores
  - final_output.N.json written to out_dir on disk
"""
from __future__ import annotations

import json
from pathlib import Path

from prefect import get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact

from common.config import RegressionConfig


def _load_tsvs(base_dir: Path) -> dict:
    """Load all TSVs from report_tables/ + regimens_full.tsv + regimens.tsv."""
    import pandas as pd

    tsv_dir  = base_dir / "report_tables"
    extra    = [base_dir / "regimens_full.tsv", base_dir / "regimens.tsv"]
    files    = list(tsv_dir.glob("*.tsv")) if tsv_dir.exists() else []
    files   += [f for f in extra if f.exists()]

    data = {}
    for f in files:
        df = pd.read_csv(f, sep="\t", dtype=str, low_memory=False)
        data[f.name] = df
    return data


@task(name="regression-compare", persist_result=False)
def task_regression_compare(cfg: RegressionConfig) -> Path:
    """
    Compare ref_dir vs new_dir, emit Prefect artifacts, write JSON to disk.
    Returns path to the JSON report.
    """
    from tools.regression.compare import compare_frames

    logger  = get_run_logger()
    run_n   = cfg.resolve_run_n()
    out_dir = cfg.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[regression] run #{run_n}  ref={cfg.ref_dir}  new={cfg.new_dir}")

    ref_data = _load_tsvs(cfg.ref_dir)
    new_data = _load_tsvs(cfg.new_dir)
    logger.info(f"[regression] loaded {len(ref_data)} ref tables, {len(new_data)} new tables")

    all_tables = sorted((set(ref_data) | set(new_data)))
    results    = {}

    score_counts  = {"stable": 0, "warning": 0, "regression": 0}
    missing_new   = []
    missing_ref   = []
    summary_rows  = []   # for the markdown summary table

    for table in all_tables:
        if table not in new_data:
            results[table] = f"skipped — missing in new ({cfg.new_dir})"
            missing_new.append(table)
            continue
        if table not in ref_data:
            results[table] = f"skipped — missing in ref ({cfg.ref_dir})"
            missing_ref.append(table)
            continue

        comparison = compare_frames(ref_data[table], new_data[table])
        results[table] = comparison

        # ── per-table artifact ────────────────────────────────────────────────
        col_rows = []
        t_stable = t_warn = t_reg = 0
        for col, col_result in comparison.items():
            if col == "_schema_drift" or not isinstance(col_result, dict):
                continue
            score = col_result.get("score", 0)
            if score == 1:
                t_stable += 1
            elif score == 0:
                t_warn += 1
            else:
                t_reg += 1

            col_rows.append({
                "column":         col,
                "score":          score,
                "jaccard":        round(col_result.get("jaccard_unique", 0), 4),
                "js_divergence":  round(col_result.get("js_divergence", 0), 6),
                "lost_keys":      col_result.get("lost_key_count", 0),
                "gained_keys":    col_result.get("gained_key_count", 0),
                "size_ref":       col_result.get("size_ref", ""),
                "size_new":       col_result.get("size_new", ""),
            })

        score_counts["stable"]     += t_stable
        score_counts["warning"]    += t_warn
        score_counts["regression"] += t_reg

        schema_drift = comparison.get("_schema_drift", {})
        summary_rows.append({
            "table":       table,
            "cols_stable": t_stable,
            "cols_warn":   t_warn,
            "cols_reg":    t_reg,
            "missing_in_new": ", ".join(schema_drift.get("col_missing_in_new", [])) or "—",
            "missing_in_ref": ", ".join(schema_drift.get("col_missing_in_ref", [])) or "—",
        })

        if col_rows:
            create_table_artifact(
                key=f"regression-{Path(table).stem.lower().replace('_', '-').replace('.', '-')}",
                table=col_rows,
                description=f"[Regression] Column scores: **{table}** — "
                            f"✅ {t_stable}  ⚠️ {t_warn}  ❌ {t_reg}",
            )

    # ── write JSON to disk ────────────────────────────────────────────────────
    json_path = out_dir / f"final_output.{run_n}.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=4)
    logger.info(f"[regression] JSON saved → {json_path}")

    # ── summary markdown artifact ─────────────────────────────────────────────
    total_cols = sum(score_counts.values())
    md = f"""## Regression Run #{run_n}

| | |
|---|---|
| **ref** | `{cfg.ref_dir}` |
| **new** | `{cfg.new_dir}` |
| **tables compared** | {len(summary_rows)} |
| **tables missing in new** | {len(missing_new)} |
| **tables missing in ref** | {len(missing_ref)} |

### Column Score Summary ({total_cols} total)

| Status | Count |
|---|---|
| ✅ Stable | {score_counts['stable']} |
| ⚠️ Warning | {score_counts['warning']} |
| ❌ Regression | {score_counts['regression']} |

### Per-Table Breakdown

| table | ✅ | ⚠️ | ❌ | schema drift (new) | schema drift (ref) |
|---|---|---|---|---|---|
"""
    for r in summary_rows:
        md += f"| `{r['table']}` | {r['cols_stable']} | {r['cols_warn']} | {r['cols_reg']} | {r['missing_in_new']} | {r['missing_in_ref']} |\n"

    if missing_new:
        md += f"\n### Missing in new\n" + "\n".join(f"- `{t}`" for t in missing_new)
    if missing_ref:
        md += f"\n### Missing in ref\n" + "\n".join(f"- `{t}`" for t in missing_ref)

    create_markdown_artifact(
        key=f"regression-summary-run-{run_n}",
        markdown=md,
        description=f"Regression run #{run_n}: ✅ {score_counts['stable']}  ⚠️ {score_counts['warning']}  ❌ {score_counts['regression']}",
    )

    logger.info(f"[regression] complete — ✅ {score_counts['stable']}  ⚠️ {score_counts['warning']}  ❌ {score_counts['regression']}")
    return json_path
