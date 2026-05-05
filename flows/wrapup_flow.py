"""
assembler.flows.wrapup_flow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Phase 3 — data model, validation, R export.

Architecture:
  - Four generate_* tasks fan out concurrently
  - task_export_rda waits for all four, calls export_artifacts.R via subprocess
  - task_validate runs last

Engine stamps — task_runner options
-------------------------------------
Swap the @flow(task_runner=...) argument to explore concurrency models.

① ThreadPoolTaskRunner  [ACTIVE]
   - Default for I/O-bound fan-out (file reads/writes, subprocesses)
   - No extra dependencies
   - from prefect.task_runners import ThreadPoolTaskRunner

② DaskTaskRunner  [CPU-bound / multi-machine]
   - True multi-process execution; distributes tasks across Dask workers
   - Connects to an ephemeral local cluster or an existing scheduler
   - pip install prefect-dask
   - from prefect_dask import DaskTaskRunner
   - @flow(task_runner=DaskTaskRunner())
   - # or remote:
   - @flow(task_runner=DaskTaskRunner(address="tcp://scheduler:8786"))

③ RayTaskRunner  [actor model / heterogeneous resources]
   - Shared-state actors, GPU scheduling, heterogeneous cluster
   - pip install prefect-ray
   - from prefect_ray import RayTaskRunner
   - @flow(task_runner=RayTaskRunner())
   - # or remote:
   - @flow(task_runner=RayTaskRunner(address="ray://head-node:10001"))
"""
from __future__ import annotations

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner

from common.config import AssemblerConfig
from tasks.datamodel import (
    task_build_regimens,
    task_reg_groups,
    task_valid_drugs,
    task_routes,
    task_short_strings,
)
from tasks.export import task_export_rda
from tasks.validate import task_validate


# ── Swap task_runner here to change concurrency engine ────────────────────────
# ① Active  : ThreadPoolTaskRunner()
# ② Dask    : DaskTaskRunner()
# ③ Ray     : RayTaskRunner()

@flow(name="wrapup", task_runner=ThreadPoolTaskRunner())
def wrapup_flow(cfg: AssemblerConfig) -> None:

    regimens_full = cfg.regimens_full_path

    # ── prerequisite: build regimens.tsv (blocking) ──────────────────────
    # Calls build_final_regimens + analyze_shortstring_regimen_mapping from
    # assembler/tools/regimen_formatter.py; emits mapping-stats artifact.
    regimens_tsv = task_build_regimens(cfg, cfg.regimens_full_path)

    # ── concurrent fan-out ────────────────────────────────────────────────
    fut_rgroups  = task_reg_groups.submit(cfg, regimens_tsv)
    fut_vdrugs   = task_valid_drugs.submit(cfg, regimens_tsv)
    fut_routes   = task_routes.submit(cfg, regimens_full)
    fut_sstrings = task_short_strings.submit(cfg, regimens_tsv)

    # ── R export waits for all four ───────────────────────────────────────
    task_export_rda(cfg, fut_rgroups, fut_vdrugs, fut_routes, fut_sstrings)

    # ── validation last ───────────────────────────────────────────────────
    task_validate(cfg, regimens_full)
