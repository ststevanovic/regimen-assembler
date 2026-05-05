"""
assembler.common.io_utils
~~~~~~~~~~~~~~~~~~~~~~~~~
Simple I/O helpers shared across pipeline stages.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import polars as pl


# ──────────────────────────────────────────────────────────────────────────────
# Existence guards
# ──────────────────────────────────────────────────────────────────────────────

def assert_exists(path: str | Path, label: str = "") -> Path:
    """Raise FileNotFoundError when *path* does not exist."""
    p = Path(path)
    tag = f" ({label})" if label else ""
    if not p.exists():
        raise FileNotFoundError(f"Required file not found{tag}: {p}")
    return p


def ensure_dir(path: str | Path) -> Path:
    """Create directory (and parents) if it does not exist; return Path."""
    p = Path(path)
    os.makedirs(p, exist_ok=True)
    return p


# ──────────────────────────────────────────────────────────────────────────────
# TSV helpers (pandas)
# ──────────────────────────────────────────────────────────────────────────────

def read_tsv(path: str | Path, **kwargs) -> pd.DataFrame:
    """Read a tab-separated file into a pandas DataFrame."""
    return pd.read_csv(path, sep="\t", index_col=False, low_memory=False, **kwargs)


def write_tsv(df: pd.DataFrame, path: str | Path, index: bool = False) -> None:
    """Write a pandas DataFrame to a tab-separated file."""
    ensure_dir(Path(path).parent)
    df.to_csv(path, sep="\t", index=index)


# ──────────────────────────────────────────────────────────────────────────────
# TSV helpers (polars)
# ──────────────────────────────────────────────────────────────────────────────

def read_tsv_pl(path: str | Path, **kwargs) -> pl.DataFrame:
    """Read a TSV into a Polars DataFrame."""
    return pl.read_csv(path, separator="\t", **kwargs)


def write_tsv_pl(df: pl.DataFrame, path: str | Path) -> None:
    """Write a Polars DataFrame to a TSV file."""
    ensure_dir(Path(path).parent)
    df.write_csv(path, separator="\t")
