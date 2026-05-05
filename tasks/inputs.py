"""
assembler.tasks.inputs
~~~~~~~~~~~~~~~~~~~~~~~
task_load_inputs — simulated DB call, reads from INPUTs/ CSVs.
Replace only this task to re-introduce a live DB; nothing downstream changes.

Simulation design:
  Three threads run concurrently, each sleeping to model Athena query latency:
    sigs_w_conditions  — 10 s  (large extract: sigs joined to conditions)
    condition_concepts —  3 s  (small vocab lookup)
    drug_concepts      —  5 s  (mid vocab lookup)
  timeit measures wall-clock per thread and is logged so the simulated
  query cost is visible in the Prefect run UI.
"""
import threading
import time
import timeit

import polars as pl
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect import get_run_logger, task

from common.config import AssemblerConfig
from common.schemas import InputBundle

# Simulated Athena query latencies (seconds)
_LATENCY = {
    "sigs_w_conditions":  10,
    "condition_concepts":  3,
    "drug_concepts":       5,
}


@task(name="load-inputs", cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=24))
def task_load_inputs(cfg: AssemblerConfig) -> InputBundle:
    """
    Simulates three concurrent Athena query round-trips via threads.
    Each thread sleeps for _LATENCY[name] seconds before reading its CSV,
    modelling extract + store + load time.

    sigs_w_conditions is loaded with all columns forced to Utf8.
    The pipeline treats all values as strings;
    resolvers cast to numeric types only when needed.

    Swap: replace _fetch_* bodies with real DB calls; latency sleeps drop out.
    """
    logger = get_run_logger()
    results: dict = {}
    errors:  dict = {}

    def _load_utf8(path):
        schema = pl.read_csv(path, infer_schema_length=10000).schema
        utf8_schema = {col: pl.Utf8 for col in schema}
        return pl.read_csv(
            path,
            schema_overrides=utf8_schema,
            null_values=["null", "NA", "", "-"],
        )

    def _fetch(name: str, loader):
        latency = _LATENCY[name]
        def _run():
            time.sleep(latency)          # simulate query + network round-trip
            results[name] = loader()
        try:
            elapsed = timeit.timeit(_run, number=1)
            logger.info(f"[load-inputs] {name}: {elapsed:.2f}s (simulated latency={latency}s)")
        except Exception as exc:
            errors[name] = exc
            logger.error(f"[load-inputs] {name} FAILED: {exc}")

    threads = [
        threading.Thread(
            target=_fetch,
            args=("sigs_w_conditions",
                  lambda: _load_utf8(cfg.input_dir / "sigs_w_conditions.csv")),
            daemon=True,
        ),
        threading.Thread(
            target=_fetch,
            args=("condition_concepts",
                  lambda: pl.read_csv(cfg.input_dir / "condition_concepts.csv")),
            daemon=True,
        ),
        threading.Thread(
            target=_fetch,
            args=("drug_concepts",
                  lambda: pl.read_csv(cfg.input_dir / "drug_concepts.csv")),
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        raise RuntimeError(f"[load-inputs] failed queries: {errors}")

    logger.info("[load-inputs] all three queries complete")
    return InputBundle(
        condition_concepts=results["condition_concepts"],
        drug_concepts=results["drug_concepts"],
        sigs_w_conditions=results["sigs_w_conditions"],
    )
