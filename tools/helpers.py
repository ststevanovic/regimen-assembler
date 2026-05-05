import re
from typing import List, Tuple, Set, Dict, Optional
from collections import defaultdict
import numpy as np

def convert_to_days(num: float, unit: str, days_range: List[int] = None) -> int:
    num = float(num)
    unit = str(unit or "").lower()

    if "day" in unit:
        return int(num)
    if "week" in unit:
        return int(num * 7)
    if "month" in unit:
        return int(num * 30)
    if "year" in unit:
        return int(num * 365)
    if "indeterminate" in unit:
        if not days_range:
            raise ValueError("indeterminate unit but days_range missing")
        return int(max(int(x) for x in days_range))

    raise ValueError(f"Unhandled cycle_length unit: num={num}, unit={unit}")

def get_last_cycle(unique_list):
    """timing_sequence example: ["15,22,28", "30", "15,22"] -> returns 28"""
    return max([int(e) for e in [l for subli in [s.split(",") for s in unique_list] for l in subli]])

def get_idays(text):
    return list(map(int, re.findall(r"-?\d+", text))) if re.findall(r"-?\d+", text) else 0

def normalize_idays(idays):
    if not idays:
        return []

    idx = [int(d) for d in idays]

    if all(d >= 1 for d in idx):
        return idx

    shift = 1 - min(idx)
    idx = [d + shift for d in idx]

    if min(idx) < 1:
        raise ValueError(f"Cannot normalize idays: {idays}")

    return idx

def normalize_step_depth(
    component_vectors,
    group,
    group_tseq
):

    if not component_vectors:
        return component_vectors

    max_depth = max(len(entries) for entries in component_vectors.values())

    if max_depth <= 1:
        return component_vectors

    normalized = {}

    for drug, entries in component_vectors.items():

        if not entries:
            normalized[drug] = entries
            continue

        padded_entries = []

        for timing_seq_str, vec in entries:

            if len(vec) < group_tseq:
                new_vec = np.zeros(group_tseq, dtype=vec.dtype)
                new_vec[:len(vec)] = vec
                padded_entries.append((timing_seq_str, new_vec))
            else:
                padded_entries.append((timing_seq_str, vec))

        current_depth = len(padded_entries)

        if current_depth < max_depth:

            dtype_ref = padded_entries[0][1].dtype
            zero_vec = np.zeros(group_tseq, dtype=dtype_ref)
            timing_seq_ref = padded_entries[0][0]

            for _ in range(max_depth - current_depth):
                padded_entries.append((timing_seq_ref, zero_vec.copy()))

        normalized[drug] = padded_entries

    return normalized
