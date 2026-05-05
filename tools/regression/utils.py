import pandas as pd
from pathlib import Path
import sys
import pickle
import zipfile
from datetime import datetime


def set_path(base_dir_path):
    base_dir = Path(base_dir_path)
    out_pkl = base_dir / "regression_checkpoint.pkl.zip"

    tsv_dir = base_dir / "report_tables"
    # regimens_full.tsv is the unfiltered source of truth (pre-dedup, all rows).
    # regimens.tsv is the shortString-deduped schedule index — excluded here because
    # its row count is intentionally reduced by build_final_regimens and comparing it
    # against a pre-Action-5 ref produces expected false negatives.
    full_out = base_dir / "regimens_full.tsv"
    main_out = base_dir / "regimens.tsv"
    extra = [f for f in [full_out, main_out] if f.exists()]
    tsv_files = list(tsv_dir.glob("*.tsv")) + extra

    return out_pkl, tsv_files

def to_pkl(pkl_file:Path, tsv_files:list):
    dataframes = {}
    for tsv_file in tsv_files:
        df = pd.read_csv(tsv_file, sep="\t", low_memory=False)
        dataframes[tsv_file.name] = df
        print(f"Loaded {tsv_file.name}, shape={df.shape}")

    metadata = {
        "created_at": datetime.now().isoformat(),
        "table_count": len(dataframes),
        "total_rows": sum(len(df) for df in dataframes.values())
    }
    dataframes["__metadata__"] = metadata

    pkl_inner_file = pkl_file.stem.replace('.pkl', '').replace('.zip', '') + '.pkl'
    with zipfile.ZipFile(pkl_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        pkl_bytes = pickle.dumps(dataframes, protocol=pickle.HIGHEST_PROTOCOL)
        zf.writestr(pkl_inner_file, pkl_bytes)

    print(f"All TSVs stored in {pkl_file}")

def from_pkl(pkl_zip_path:str):
    pkl_zip = Path(pkl_zip_path)
    
    if not pkl_zip.exists():
        print(f"No baseline found at {pkl_zip}")
        return {}
    
    with zipfile.ZipFile(pkl_zip, 'r') as zf:
        pkl_name = [name for name in zf.namelist() if name.endswith('.pkl')][0]
        pkl_bytes = zf.read(pkl_name)
        dataframes = pickle.loads(pkl_bytes)
    
    metadata = dataframes.get("__metadata__", {})
    print(f"Loaded {metadata.get('table_count', 0)} tables from {pkl_zip}")
    return dataframes

def create_baseline(output_dir: Path | str, version: str, baseline_dir: Path | str = "baseline_staging"):
    """
    Create baseline.pkl.zip and append to history.csv.
    
    Args:
        output_dir: Directory containing report_tables/ and regimens.tsv
        version: Version string (e.g., "1.3.0")
        baseline_dir: Where to save files (default: ./baseline_staging)
    """
    baseline_dir = Path(baseline_dir)
    
    baseline_dir.mkdir(parents=True, exist_ok=True)
    
    baseline_file = baseline_dir / "baseline.pkl.zip"
    history_file = baseline_dir / "history.csv"
    
    pkl_file, tsv_files = set_path(output_dir)
    to_pkl(baseline_file, tsv_files)
    
    with open(history_file, "w") as f:
        f.write("date,version\n")
        f.write(f"{datetime.now().strftime('%Y-%m-%d')},{version}\n")
    
    print(f"* Baseline created: {baseline_file}")
    print(f"* History entry created: {history_file}")
    
    return baseline_file, history_file
