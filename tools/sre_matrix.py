from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import numpy as np


# ==========
# CORE TYPES
# ==========

@dataclass(frozen=True)
class VariantEntry:
    """Parse a string index descriptor and numeric vector into a
    validated 1D integer array with associated index set."""

    cycles: Tuple[int, ...]
    vector: np.ndarray

    @staticmethod
    def from_raw(pos_str: str, vec: np.ndarray) -> "VariantEntry":
        cycles = tuple(sorted({int(x) for x in pos_str.split(",") if x}))
        v = np.asarray(vec, dtype=int).reshape(-1)
        if v.ndim != 1:
            raise ValueError("Vector must be 1D")
        return VariantEntry(cycles=cycles, vector=v)


@dataclass
class VariantMatrix:
    components: Dict[str, List[VariantEntry]]

    def __post_init__(self):
        if not self.components:
            raise ValueError("Empty VariantMatrix")

        for name, entries in self.components.items():
            if not name:
                raise ValueError("Invalid component name")
            if not entries:
                raise ValueError(f"Component '{name}' has no entries")
            for e in entries:
                if not isinstance(e, VariantEntry):
                    raise TypeError("Entries must be VariantEntry")

    @classmethod
    def from_raw(cls, raw):
        components = {
            k: [VariantEntry.from_raw(pos, vec) for pos, vec in v]
            for k, v in raw.items()
        }
        return cls(components)


@dataclass(frozen=True)
class TimelineSlice:
    components: Dict[str, np.ndarray]

    def __post_init__(self):
        if not self.components:
            raise ValueError("Empty TimelineSlice")

        lengths = {len(v) for v in self.components.values()}
        if len(lengths) != 1:
            raise ValueError("Component vectors must have equal length")

    @classmethod
    def from_raw(cls, raw):
        components = {
            k: np.asarray(v, dtype=int).reshape(-1)
            for k, v in raw.items()
        }
        return cls(components)


VariantDict = Dict[str, List[VariantEntry]]
VariantSlice = Dict[str, VariantEntry]


# ==========
# NORMALIZATION
# ==========

def normalize_multicycle_spans(matrix: VariantMatrix) -> VariantDict:
    """
    Group entries by identical index patterns and vector length,
    merging overlapping arrays via elementwise maximum.
    """

    out: VariantDict = {}

    for comp, entries in matrix.components.items():
        cycle_patterns = {e.cycles for e in entries}

        if len(cycle_patterns) == 1:
            out[comp] = entries
            continue

        buckets = defaultdict(lambda: {"cycles": [], "vec": None})

        for e in entries:
            key = (len(e.vector), e.cycles)

            if buckets[key]["vec"] is None:
                buckets[key]["vec"] = e.vector.copy()
            else:
                buckets[key]["vec"] = np.maximum(buckets[key]["vec"], e.vector)

            buckets[key]["cycles"].extend(e.cycles)

        for (L, _), data_bucket in buckets.items():
            name = f"{comp}@cycleLen{L}"
            out[name] = [
                VariantEntry(
                    cycles=tuple(sorted(set(data_bucket["cycles"]))),
                    vector=data_bucket["vec"],
                )
            ]

    return out


# ==========
# DEPTH SPLIT
# ==========

def split_variants(
    data: VariantDict,
    allow_multipart: bool = True,
) -> Optional[List[VariantSlice]]:
    """
    Partition a dictionary of array lists into aligned index-wise
    slices based on consistent list depths or broadcast rules.
    """

    lengths = {k: len(v) for k, v in data.items()}
    unique = set(lengths.values())

    if len(unique) == 1:
        n = next(iter(unique))
        return [{k: data[k][i] for k in data} for i in range(n)]

    if len(unique) == 2 and 1 in unique:
        long_key = max(lengths, key=lengths.get)

        if all(l == 1 or k == long_key for k, l in lengths.items()):
            n = lengths[long_key]
            return [
                {k: (data[k][i] if k == long_key else data[k][0]) for k in data}
                for i in range(n)
            ]

        if allow_multipart:
            return None

    raise ValueError(f"Invalid depth structure: {lengths}\n{data}")


# ==========
# MULTIPART FALLBACK
# ==========

def multipart_padding(data: VariantDict) -> List[VariantSlice]:
    """
    Construct a single large array per key by repeating and placing
    smaller arrays into indexed blocks within a global vector.

    block_len and total are computed per-component so that entries
    produced by normalize_multicycle_spans (which may carry different
    vector lengths annotated as @cycleLen<L>) do not cause shape
    mismatches when placed into the global array.
    """

    out: VariantSlice = {}

    for comp, entries in data.items():
        # Determine the dimensions for this component independently.
        comp_max_cycle = 0
        comp_block_len = 0

        for e in entries:
            if e.cycles:
                comp_max_cycle = max(comp_max_cycle, max(e.cycles))
            comp_block_len = max(comp_block_len, len(e.vector))

        if comp_max_cycle == 0 or comp_block_len == 0:
            raise ValueError(f"Cannot infer cycle structure for component '{comp}'")

        comp_total = comp_max_cycle * comp_block_len
        full = np.zeros(comp_total, dtype=int)

        for e in entries:
            vec = e.vector
            if len(vec) < comp_block_len:
                vec = np.pad(vec, (0, comp_block_len - len(vec)))
            elif len(vec) > comp_block_len:
                vec = vec[:comp_block_len]

            for c in e.cycles:
                s = (c - 1) * comp_block_len
                end = s + comp_block_len
                if end > comp_total:
                    # cycle index exceeds allocated total — extend
                    extra = end - comp_total
                    full = np.concatenate([full, np.zeros(extra, dtype=int)])
                    comp_total = len(full)
                full[s:end] = np.maximum(full[s:end], vec)

        out[comp] = VariantEntry(cycles=(), vector=full)

    # All component vectors must share the same length for TimelineSlice.
    # Pad shorter ones to the global maximum.
    if out:
        max_len = max(len(e.vector) for e in out.values())
        out = {
            k: VariantEntry(
                cycles=e.cycles,
                vector=np.pad(e.vector, (0, max_len - len(e.vector)))
                if len(e.vector) < max_len else e.vector,
            )
            for k, e in out.items()
        }

    return [out]


# ==========
# COLLAPSE
# ==========

def collapse_event_matrix(timeline: TimelineSlice) -> str:
    """
    Detect nonzero positions across aligned arrays and encode relative index
    differences into a compact sequential representation.
    """

    components = sorted(timeline.components.keys())
    num_days = len(next(iter(timeline.components.values())))

    unified = np.zeros(num_days, dtype=int)
    for v in timeline.components.values():
        unified |= v

    event_days = np.where(unified == 1)[0]
    if len(event_days) == 0:
        return ""

    tags = []
    used_shift = False
    last_day = event_days[-1]
    shift = num_days - last_day

    for idx, day in enumerate(event_days):
        active = sorted(
            k for k in components
            if timeline.components[k][day] == 1
        )

        main = active[0]

        if not used_shift:
            tags.append(f"{shift}.{main}")
            used_shift = True
        else:
            delta = int(event_days[idx] - event_days[idx - 1])
            tags.append(f"{delta}.{main}")

        for sec in active[1:]:
            tags.append(f"0.{sec}")

    if len({t.split(".")[1] for t in tags}) == 1:
        return ";".join(tags + tags)

    return ";".join(tags)


# ==========
# WRAPPER
# ==========

def collapse_event_matrix_wrapper(matrix: VariantMatrix) -> List[str]:
    """
    Execute normalization, depth alignment, zero-padding,
    and array compression across all structured slices.
    """

    normalized = normalize_multicycle_spans(matrix)
    slices = split_variants(normalized)

    if slices is None:
        slices = multipart_padding(normalized)

    results = []

    for slice_dict in slices:
        max_len = max(len(e.vector) for e in slice_dict.values())

        padded = {
            k: np.pad(e.vector, (0, max_len - len(e.vector)))
            if len(e.vector) < max_len else e.vector
            for k, e in slice_dict.items()
        }

        timeline = TimelineSlice(components=padded)

        if not any(np.any(v) for v in timeline.components.values()):
            raise ValueError("Zero-only variant")

        results.append(collapse_event_matrix(timeline))

    return results
