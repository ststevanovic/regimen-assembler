"""
assembler.common.config
~~~~~~~~~~~~~~~~~~~~~~~
Central configuration dataclasses for the HemOnc ETL pipeline.

ROOT is derived from the location of this file:
  config.py  →  common/  →  assembler/  →  <REPO_ROOT>

Mirrors CLI surface of RunScript.sh and RunRegression.sh.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

# ── assembler root (1 parent up from assembler/common/config.py) ───────────────
# config.py → common/ → assembler/
ROOT: Path = Path(__file__).resolve().parents[1]


# ── ETL pipeline ─────────────────────────────────────────────────────────────

@dataclass
class AssemblerConfig:
    """
    All file-system paths and run-time flags for one ETL run.
    Mirrors the CLI surface of RunScript.sh.
    """

    # ── directories ──────────────────────────────────────────────────────────
    workdir:           Path = ROOT / "OUTPUTs" / "output-assembled"
    input_dir:         Path = ROOT / "INPUTs"
    ref_dir:           Path = ROOT / "INPUTs"  # same dir — sigs + ref sheets co-located
    log_dir:           Path | None = None        # defaults to workdir/logs

    # ── input filenames (relative to input_dir / ref_dir) ────────────────────
    sigs_file:         str = "sigs_march_2025.csv"
    sheet_config_file: str = "sheets_config.json"
    rgroups_template:  str = "rgroups_template.tsv"

    # ── run-time flags ────────────────────────────────────────────────────────
    skip_query: bool = True    # False → live Athena query; True → use snapshot
    debug:      bool = False

    # ── DB credentials (only needed when skip_query=False) ───────────────────
    db_credentials: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.workdir   = Path(self.workdir)
        self.input_dir = Path(self.input_dir)
        self.ref_dir   = Path(self.ref_dir)
        self.log_dir   = Path(self.log_dir) if self.log_dir else self.workdir / "logs"

    def ensure_dirs(self) -> None:
        self.workdir.mkdir(parents=True, exist_ok=True)

    def ensure_log_dir(self) -> Path:
        """Create and return log_dir — call only when a component actually writes logs."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        return self.log_dir

    # ── resolved input paths ──────────────────────────────────────────────────
    @property
    def sigs_path(self) -> Path:
        return self.input_dir / self.sigs_file

    @property
    def sheet_config_path(self) -> Path:
        return self.ref_dir / self.sheet_config_file

    @property
    def rgroups_template_path(self) -> Path:
        return self.ref_dir / self.rgroups_template

    # ── resolved output paths ─────────────────────────────────────────────────
    @property
    def sigs_w_conditions_path(self) -> Path:
        return self.workdir / "sigs_w_conditions.csv"

    @property
    def drug_concepts_path(self) -> Path:
        return self.workdir / "drug_concepts.csv"

    @property
    def parquet_path(self) -> Path:
        return self.workdir / "s_frame.parquet"

    @property
    def regimens_tsv_path(self) -> Path:
        return self.workdir / "regimens.tsv"

    @property
    def regimens_full_path(self) -> Path:
        return self.workdir / "regimens_full.tsv"


# ── Regression ───────────────────────────────────────────────────────────────

@dataclass
class RegressionConfig:
    """
    Paths and options for a regression comparison run.
    Mirrors the CLI surface of RunRegression.sh.
    """

    ref_dir:    Path = ROOT / "OUTPUTs" / "output_baseline"
    new_dir:    Path = ROOT / "OUTPUTs" / "run2test"
    output_dir: Path = ROOT / "OUTPUTs" / "output.regression_tests"
    run_n:      int | None = None   # auto-increments when None
    log_dir:    Path | None = None

    def __post_init__(self) -> None:
        self.ref_dir    = Path(self.ref_dir)
        self.new_dir    = Path(self.new_dir)
        self.output_dir = Path(self.output_dir)
        self.log_dir    = Path(self.log_dir) if self.log_dir else self.output_dir / "logs"

    def ensure_dirs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def resolve_run_n(self) -> int:
        """Auto-increment run number based on existing JSON files."""
        if self.run_n is not None:
            return self.run_n
        n = 1
        while (self.output_dir / f"final_output.{n}.json").exists():
            n += 1
        return n
