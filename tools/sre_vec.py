from dataclasses import dataclass
import numpy as np
from typing import List

@dataclass(frozen=True)
class ComponentVector:
    """
    Build a binary vector of length csig from 1-based day indices.
    If idays contains values > csig, assume they are absolute-within-parent-cycle
    and shift them into 1..csig by subtracting (min(idays)-1).
    """
    length: int
    vector: np.ndarray

    @classmethod
    def from_idays(
        cls,
        idays: List[int],
        tseq: int,
    ) -> "ComponentVector":

        vec = np.zeros(tseq, dtype=int)

        if not idays:
            raise ValueError("idays list is empty")

        for d in idays:
            d = int(d)

            if d < 1 or d > tseq:
                raise ValueError(f"day {d} out of range 1..{tseq}")

            vec[d - 1] = 1

        return cls(tseq, vec)


def build_component_vector(idays: List[int], csig: int) -> np.ndarray:
    """
    Construct binary vector from 1-based indices using ComponentVector model.
    """
    return ComponentVector.from_idays(idays, csig).vector
