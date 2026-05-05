"""
assembler.scripts.run_transform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Plain-Python entry point for Phase 2 (SRE transform).
No Prefect imports — runs directly in the etl conda env.

Called by master.py via:
    mamba run -n etl python assembler/scripts/run_transform.py --workdir <dir>
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workdir", required=True)
    args = parser.parse_args()

    workdir   = Path(args.workdir)
    parquet   = workdir / "s_frame.parquet"
    log_dir   = workdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    import polars as pl
    from tools.handler import RegStringHandler

    handler = RegStringHandler(str(parquet), str(log_dir))
    handler.process()

    import pandas as pd
    out = workdir / "regimens_full.tsv"
    handler.frame.to_csv(out, sep="\t", index=False)
    print(f"[transform] wrote {out}  shape={handler.frame.shape}")


if __name__ == "__main__":
    main()
