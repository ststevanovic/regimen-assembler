"""
assembler.scripts.run_preprocess
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Plain-Python entry point for Phase 1 (preprocess).
No Prefect imports — runs directly in the etl conda env.

Called by master.py via:
    mamba run -n etl python assembler/scripts/run_preprocess.py --workdir <dir> [--sigs <file>]
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from common.config import AssemblerConfig
from common.io_utils import read_sigs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--sigs",    default="sigs_march_2025.csv")
    args = parser.parse_args()

    cfg = AssemblerConfig(workdir=args.workdir, sigs_file=args.sigs)
    cfg.ensure_dirs()

    import shutil
    from common.config import ROOT as REPO_ROOT
    input_dir = REPO_ROOT / "INPUTs"
    for f in ["condition_concepts.csv", "drug_concepts.csv", "sigs_w_conditions.csv"]:
        src = input_dir / f
        if src.exists():
            shutil.copy(src, cfg.workdir / f)

    import pandas as pd
    sigs = pd.read_csv(cfg.sigs_w_conditions_path)
    print(f"[preprocess] loaded sigs: {sigs.shape}")

    import polars as pl
    frame = pl.from_pandas(sigs)
    frame.write_parquet(cfg.workdir / "s_frame.parquet")
    print(f"[preprocess] wrote s_frame.parquet  shape={frame.shape}")


if __name__ == "__main__":
    main()
