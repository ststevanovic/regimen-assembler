"""
assembler.common.schemas
~~~~~~~~~~~~~~~~~~~~~~~~~
Shared data contracts between tasks and flows.
"""
from __future__ import annotations

from typing import List

import polars as pl
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass
from pydantic import ConfigDict


class FieldSchema(BaseModel):
    """
    Single column-name contract for the entire Phase 1 pipeline.
    Passed from flow to tasks — Prefect validates and renders fields in the UI.
    Each task uses only the field groups it needs; others are ignored.
    """
    group_keys: list[str] = [
        "condition_cui",
        "regimen_cui",
        "variant_key",      # dependency created - non-native in Sig tables.
    ]

    group_keys_regimen: list[str] = [
        "condition_cui",
        "regimen_cui",
    ]

    group_keys_w_reg: list[str] = [
        "condition_cui",
        "regimen_cui",
        "variant_key",
        "regimen"
    ]

    condition_cols: list[str] = [
        "condition",
        "condition_cui",
    ]

    sig_cols: list[str] = [
        "allDays",
        "cycle_length_lb",
        "cycle_length_ub",
        "cycle_length_unit",
        "timing_sequence",
    ]

    null_cols: list[str] = [
        "condition",
        "condition_cui",
        "regimen_cui",
        "variant_key",
        "allDays",
        "cycle_length_lb",
        "cycle_length_ub",
        "cycle_length_unit",
        "timing_sequence",
    ]

    group_keys_w_cui: list[str] = ["condition", "condition_cui", "regimen", "regimen_cui", "variant", "variant_key"]


    role_cols: dict[str, list[str]] = {
        "component_role" : ["secondary systemic", "locoregional"]
    }

    regimen_cols: list[str] = [
        "regimen",
        "regimen_cui",
    ]

    identity_cols: list[str] = [
        "component_cui",
        "timing_sequence",
        "step_number",
        "allDays",
        "cycle_length_lb",
        "cycle_length_ub",
        "cycle_length_unit",
    ]

    key_source_cols: list[str] = [
        "condition_cui",
        "regimen_cui",
        "variant",
    ]


# ── Prefect Block registry ────────────────────────────────────────────────────
# Allows FieldSchema to be versioned and referenced by name across flows
# in Prefect Cloud / Server without redeployment.
#
# Register once (e.g. in a setup script or notebook):
#     block = FieldSchemaBlock()
#     block.save("prod-schema", overwrite=True)
#
# Load in any flow (sync context):
#     schema = FieldSchemaBlock.load("prod-schema").field_schema
#
# Or swap _SCHEMA in preprocess_flow.py:
#     _SCHEMA = FieldSchemaBlock.load("prod-schema").schema

from prefect.blocks.core import Block  # noqa: E402


class FieldSchemaBlock(Block):
    """
    FieldSchema stored as a Prefect Block — versioned, named, shareable.

    Fields mirror FieldSchema exactly; the embedded ``schema`` is the
    canonical source of truth for all column contracts in the pipeline.

    _block_type_name / _description surface in the Prefect UI Block catalog.
    """

    _block_type_name = "field-schema"
    _description     = "Column-name contract for the assembler pipeline."

    field_schema: FieldSchema = Field(default_factory=FieldSchema)





@dataclass(config=ConfigDict(arbitrary_types_allowed=True)) # due to pl type
class ResolverPatch:
    """
    Return type for every resolver / handler @task in Phase 1.

    row_indices : positions in frame_keyed (integer index, not variant_key)
    columns     : columns this task owns and wrote (TASK_COLUMN_MAP entry)
    data        : patched slice — same schema as the input slice
    flag        : True → rows are drop candidates (merge_and_audit will set _drop_flag)
    """
    row_indices: List[int]
    columns:     List[str]
    data:        pl.DataFrame
    flag:        bool = False


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class InputBundle:
    """
    Simulates the three DataFrames previously produced by live Athena queries.
    Loaded from INPUTs/ CSVs by task_load_inputs.
    Swap only to a real DB; 
    """
    condition_concepts: pl.DataFrame
    drug_concepts:      pl.DataFrame
    sigs_w_conditions:  pl.DataFrame
