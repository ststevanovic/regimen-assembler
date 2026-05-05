import pandas as pd
import numpy as np
from pathlib import Path
import sys
import json
from scipy.spatial.distance import jensenshannon


def compare_frames(ref_frame, new_frame):
    ref_cols = set(ref_frame.columns)
    new_cols = set(new_frame.columns)

    missing_in_new = sorted(list(ref_cols - new_cols))
    missing_in_ref = sorted(list(new_cols - ref_cols))
    common_cols = sorted(list(ref_cols & new_cols))

    results = {
        "_schema_drift": {
            "col_missing_in_new": missing_in_new,
            "col_missing_in_ref": missing_in_ref
        }
    }

    for col in common_cols:
        results[col] = score_column(ref_frame[col], new_frame[col])

    return results


def score_column(ref_col, new_col):
    size_ref = len(ref_col)
    size_new = len(new_col)

    # operates on entire distribution vector
    # Dropna important! - Not to get nan as only new key 
    # and produce wired zero propbability (jsd) score later!
    ref_unique = set(ref_col.dropna().unique()) 
    new_unique = set(new_col.dropna().unique())

    cardinality_ref = len(ref_unique)
    cardinality_new = len(new_unique)

    intersection = ref_unique & new_unique
    union = ref_unique | new_unique

    jaccard_unique = len(intersection) / len(union) if union else 1.0

    lost_keys = ref_unique - new_unique
    gained_keys = new_unique - ref_unique

    js_div = compute_js_divergence(ref_col, new_col)

    if lost_keys:
        score = -1

    # Right now, treats "Adding new data types" (Score 1) 
    # as just as good as "Keeping everything the same" (Score 1).
    elif gained_keys and not lost_keys:
        score = 1
   
    # Gaind or lost operates on cardinality
    # So we also check upon distribution diff.
    elif js_div < 1e-12: # Otherwise microscopic floating noise will misclassify. Essentialy ~= 0
        score = 1
   
    else:
        score = 0

    result = {
        "size_ref": size_ref,
        "size_new": size_new,
        "cardinality_ref": cardinality_ref,
        "cardinality_new": cardinality_new,
        "jaccard_unique": jaccard_unique,
        "lost_key_count": len(lost_keys),
        "gained_key_count": len(gained_keys),
        "js_divergence": js_div,
        "score": score
    }

    if score <= 0:
        result["exact_diff"] = exact_diff(ref_col, new_col)

    return result

def compute_js_divergence(ref_col, new_col):
    ref_counts = ref_col.value_counts(normalize=True)
    new_counts = new_col.value_counts(normalize=True)

    if ref_counts.empty and new_counts.empty:
        return 0.0

    all_keys = ref_counts.index.union(new_counts.index)

    ref_probs = ref_counts.reindex(all_keys, fill_value=0).values
    new_probs = new_counts.reindex(all_keys, fill_value=0).values

    if ref_probs.sum() == 0 or new_probs.sum() == 0:
        return 0.0

    return float(jensenshannon(ref_probs, new_probs, base=2.0) ** 2)


def exact_diff(ref_col, new_col, top_n=10):
    ref_counts = ref_col.value_counts()
    new_counts = new_col.value_counts()

    all_keys = ref_counts.index.union(new_counts.index)

    ref_counts = ref_counts.reindex(all_keys, fill_value=0)
    new_counts = new_counts.reindex(all_keys, fill_value=0)

    diff = new_counts - ref_counts

    changed = diff[diff != 0].sort_values(key=abs, ascending=False)

    return {
        "changed_value_count": int(len(changed)),
        "top_differences": {
            str(k): int(v)
            for k, v in changed.head(top_n).items()
        }
    }


def output_json(new_root, ref_root, output_path):
    ref_root = Path(ref_root)
    new_root = Path(new_root)
    out = {}

    all_files = {f.stem for f in 
                 list((ref_root / "report_tables").glob("*.tsv")) + 
                 list((new_root / "report_tables").glob("*.tsv"))}
    
    all_files = {f"report_tables/{s}.tsv" for s in all_files} | {"regimens.tsv"}

    for fi in sorted(all_files):
        ref_file = ref_root / fi
        new_file = new_root / fi

        if not new_file.exists():
            out[Path(fi).stem] = f"skipped, missing in: {new_root}"
        elif not ref_file.exists():
            out[Path(fi).stem] = f"skipped, missing in: {ref_root}"
        else:
            ref_df = pd.read_csv(ref_file, sep="\t", dtype=str, low_memory=False)
            new_df = pd.read_csv(new_file, sep="\t", dtype=str, low_memory=False)
            out[Path(fi).stem] = compare_frames(ref_df, new_df)

    with open(Path(output_path) / "comparison_report.json", "w") as f:
        json.dump(out, f, indent=4)
    
    print(f"Comparison report saved to {Path(output_path) / 'comparison_report.json'}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python compare.py <new_path>")

    print("[REGRESSION TEST] Comparing new output to reference...")
    ref_root = "/home/stev/proj/ETL_HemOnc_regimens/output.v111.new"

    output_json(sys.argv[1], ref_root, ".")
    print("[REGRESSION TEST] Comparison complete. See comparison_report.json for details.")