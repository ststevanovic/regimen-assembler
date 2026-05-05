import re 
from typing import List, Tuple, Set, Dict
import numpy as np

def extract_number_deprecated(text):
    return int(re.search(r"\d+", str(text)).group()) if re.search(r"\d+", str(text)) else 0


def convert_to_days(num:float, unit:str, days_range:list=[1,2]):
    """
    convert input unit to days
    """
    num = float(num)
    if 'day' in unit:
        days = num
    elif 'week' in unit:
        days = num * 7
    elif 'month' in unit:
        days = num * 30
    elif 'year' in unit:
        days = num * 365
    elif 'indeterminate' in unit:
        days = max([int(i) for i in days_range.split(",")])
    else:
        return f"[WARN] Unhandled case - cycle_length: {num} - unit: {unit}"

    return int(days)

def get_last_cycle(unique_list):
    return max([int(e) for e in [l for subli in [s.split(",") for s in unique_list] for l in subli]])

def get_idays(text):
    return list(map(int, re.findall(r"-?\d+", text))) if re.findall(r"-?\d+", text) else 0

# VECTOR

def build_component_vector(idays: list, csig=0) -> dict:
    """
    Build a binary vector for a component based on integer days (idays).
    
    If csig == 0, infer vector length from day range.
    If csig > 0, build vector of length csig using idays positions.

    output: list = 0, 1, 0, ...]
    """

    if type(csig) != int:
        return ValueError(f"[ERR] unhandled csig: {csig}")
   
    vec = np.sum([np.eye(1, csig, k=day - 1)[0] for day in idays], axis=0)
    return vec.astype(int)

# MATRIX

def get_variant_variant(
    variants: List[Tuple[str, np.ndarray]],
    i: int
) -> Tuple[Set[int], np.ndarray]:
    if i < len(variants):
        pos_str, vec = variants[i]
    else:
        pos_str, base_vec = variants[0] if variants else ("", np.array([0]))
        vec = np.zeros_like(base_vec)
    pos_set = set(map(int, pos_str.split(','))) if pos_str else set()
    return pos_set, vec

def build_key_output(
    variants: List[Tuple[str, np.ndarray]],
    i: int,
    sorted_positions: List[int],
    position_to_len: Dict[int, int]
) -> np.ndarray:
    pos_set, vec = get_variant_variant(variants, i)
    return np.concatenate([
        np.pad(vec, (0, position_to_len[pos] - vec.shape[0])) if pos in pos_set
        else np.zeros(position_to_len[pos], dtype=vec.dtype)
        for pos in sorted_positions
    ])


def extract_position_lengths(
    input_dict: Dict[str, List[Tuple[str, np.ndarray]]],
    max_day_limit: int = 10000
) -> Tuple[bool, Tuple[List[int], Dict[int, int]]]:
    """Safely extract max vector length per position, with blockers on insane inputs."""
    position_to_len = {}

    for key, variants in input_dict.items():
        for idx, (pos_str, vec) in enumerate(variants):
            try:
                positions = list(map(int, pos_str.split(',')))
            except Exception as e:
                return False, (f"Invalid position string in key={key}, index={idx}: '{pos_str}' — {e}", {})

            for pos in positions:
                if pos > max_day_limit:
                    return False, (f"Position {pos} in key={key}, index={idx} exceeds max limit {max_day_limit}", {})
                vec_len = vec.shape[0]
                if vec_len > max_day_limit:
                    return False, (f"Vector length {vec_len} in key={key}, index={idx} exceeds max limit {max_day_limit}", {})
                position_to_len[pos] = max(position_to_len.get(pos, 0), vec_len)

    return True, (sorted(position_to_len), position_to_len)

def build_variant_outputs_numpy(
    input_dict: Dict[str, List[Tuple[str, np.ndarray]]]
) -> List[Dict[str, np.ndarray]]:
    sorted_positions, position_to_len = extract_position_lengths(input_dict)
    max_depth = max((len(v) for v in input_dict.values()), default=1)

    return [
        {
            key: build_key_output(variants, i, sorted_positions, position_to_len)
            for key, variants in input_dict.items()
        }
        for i in range(max_depth)
    ]


def collapse_event_matrix(event_string):
    components = sorted(event_string.keys())
    num_days = len(next(iter(event_string.values())))

    for k, v in event_string.items():
        if len(v) != num_days:
            raise ValueError(f"Component '{k}' has mismatched length.")

    # Create a unified event matrix of 1s where any drug is active
    unified_events = [0] * num_days
    for v in event_string.values():
        for i, val in enumerate(v):
            if val == 1:
                unified_events[i] = 1

    # Precompute all event days
    event_days = [i for i, val in enumerate(unified_events) if val == 1]

    tag_entries = []
    for day in event_days:
        active_names = sorted([comp for comp in components if event_string[comp][day] == 1])
        if active_names:
            tag_entries.append((day, active_names))

    if not tag_entries:
        return ""

    last_day = tag_entries[-1][0]
    shift = num_days - last_day  

    output = []
    used_shift = False
    event_index = 0
    component_first_use = set()

    for day, names in tag_entries:
        main = names[0]

        if not used_shift:
            tag = f"{shift}.{main}"
            used_shift = True
        else:
            delta = event_days[event_index] - event_days[event_index - 1]
            tag = f"{delta}.{main}"

        output.append(tag)
        component_first_use.add(main)

        for name in names[1:]:
            tag = f"0.{name}"
            output.append(tag)
            component_first_use.add(name)

        event_index += 1

    if len(component_first_use) == 1:
        return ";".join(output + output)

    return ";".join(output)


def validate_and_split_variants(input_dict: Dict[str, List[Tuple[str, np.ndarray]]]) -> List[Dict[str, Tuple[str, np.ndarray]]] | str:
    lengths = {k: len(v) for k, v in input_dict.items()}
    unique_lengths = set(lengths.values())

    if len(unique_lengths) == 1:
        #  case 1: All keys same length
        n = next(iter(unique_lengths))
        return [
            {k: input_dict[k][i] for k in input_dict}
            for i in range(n)
        ]

    if len(unique_lengths) == 2 and 1 in unique_lengths:
        # case 2: One key has >1, rest have 1
        long_key = max(lengths, key=lengths.get)
        if all(l == 1 or k == long_key for k, l in lengths.items()):
            n = lengths[long_key]
            return [
                {
                    k: input_dict[k][i] if k == long_key else input_dict[k][0]
                    for k in input_dict
                }
                for i in range(n)
            ]
        else:
            raise ValueError("Variant lengths mismatch: mixed variant pattern is invalid.")

    # fallback for all other unexpected combinations
    return None


def pad_variant_dict(variant: Dict[str, Tuple[str, np.ndarray]]) -> Dict[str, np.ndarray]:
    max_len = max(vec.shape[0] for _, vec in variant.values())
    return {
        k: np.pad(vec, (0, max_len - vec.shape[0]))
        for k, (_, vec) in variant.items()
    }

def collapse_event_matrix_wrapper(input_dict: Dict[str, List[Tuple[str, np.ndarray]]]) -> List[str]:
    "Main api endpoint"
    variant_dicts = validate_and_split_variants(input_dict)

    results = []
    for vdict in variant_dicts:
        padded = pad_variant_dict(vdict)
        if not any(np.any(v) for v in padded.values()):
            raise ValueError("All components in variant are zero-only.")

        results.append(collapse_event_matrix(padded))
    return results
