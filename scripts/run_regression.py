"""
assembler.scripts.run_regression
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Plain-Python entry point for regression testing.
No Prefect imports — runs directly in the etl-regression conda env.

Replicates RunRegression.sh but as a proper importable Python module.

Called by master.py via:
    mamba run -n etl-regression python assembler/scripts/run_regression.py \
        --ref <ref_dir> --new <new_dir> [--out <out_dir>] [--n <run_number>]
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

REGRESSION_SRC = ROOT / "src" / "tests" / "regression"
sys.path.insert(0, str(REGRESSION_SRC))


def _build_dataframes(tsvs: list[Path], label: str) -> dict:
    import pandas as pd
    dataframes: dict = {}
    for f in tsvs:
        p = Path(f)
        if not p.exists():
            print(f"  [SKIP] {p.name} not found")
            continue
        df = pd.read_csv(p, sep="\t", low_memory=False)
        dataframes[p.name] = df
        print(f"  Loaded {p.name}, shape={df.shape}")
    dataframes["__metadata__"] = {
        "created_at": datetime.now().isoformat(),
        "table_count": len(dataframes) - 1,
        "label": label,
    }
    return dataframes


def main():
    parser = argparse.ArgumentParser(description="Run regression comparison")
    parser.add_argument("--ref", required=True,           help="Reference (stable) output directory")
    parser.add_argument("--new", required=True,           help="New (trial) output directory")
    parser.add_argument("--out", default="output.regression_tests", help="Output directory for results")
    parser.add_argument("--n",   default=None, type=int,  help="Run number (auto-increments if omitted)")
    args = parser.parse_args()

    ref_dir = Path(args.ref).resolve()
    new_dir = Path(args.new).resolve()
    out_dir = Path(args.out).resolve()

    if not ref_dir.exists():
        raise FileNotFoundError(f"Reference directory not found: {ref_dir}")
    if not new_dir.exists():
        raise FileNotFoundError(f"New directory not found: {new_dir}")

    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Auto-increment run number ─────────────────────────────────────────────
    run_n = args.n
    if run_n is None:
        run_n = 1
        while (out_dir / f"final_output.{run_n}.json").exists():
            run_n += 1

    comparison_json = out_dir / f"final_output.{run_n}.json"
    html_out        = out_dir / f"final_output.{run_n}.html"

    print(f"\n%%% Running Regression: {ref_dir} (ref) vs {new_dir} (new) %%%\n")
    print(f"  Run #       : {run_n}")
    print(f"  Output JSON : {comparison_json}")
    print(f"  Output HTML : {html_out}\n")

    from utils import set_path
    from compare import compare_frames
    from create_report import create_interactive_report

    # ── Load reference ────────────────────────────────────────────────────────
    print(f"* Loading reference: {ref_dir}")
    _, ref_tsvs = set_path(str(ref_dir))
    ref_data = _build_dataframes(ref_tsvs, str(ref_dir))

    # ── Load new ──────────────────────────────────────────────────────────────
    print(f"\n* Loading new:       {new_dir}")
    _, new_tsvs = set_path(str(new_dir))
    new_data = _build_dataframes(new_tsvs, str(new_dir))

    # ── Compare ───────────────────────────────────────────────────────────────
    print("\n* Comparing tables...")
    out: dict = {}
    all_tables = (set(new_data.keys()) | set(ref_data.keys())) - {"__metadata__"}

    for table in sorted(all_tables):
        if table not in new_data:
            out[table] = f"skipped, missing in new ({new_dir})"
        elif table not in ref_data:
            out[table] = f"skipped, missing in ref ({ref_dir})"
        else:
            out[table] = compare_frames(ref_data[table], new_data[table])

    # ── Save JSON ─────────────────────────────────────────────────────────────
    with open(comparison_json, "w") as fh:
        json.dump(out, fh, indent=4)
    print(f"\n* Comparison JSON saved: {comparison_json}")

    # ── Generate HTML report ──────────────────────────────────────────────────
    print("* Generating HTML report...")
    create_interactive_report(
        report_path=comparison_json,
        output_dir=out_dir,
    )

    src_html = out_dir / "final_output.html"
    if src_html.exists() and src_html != html_out:
        src_html.rename(html_out)
    print(f"* HTML report saved:     {html_out}")

    print("\n%%% Regression complete. %%%")


if __name__ == "__main__":
    main()
