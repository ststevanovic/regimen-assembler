# assembler

Prefect ETL pipeline for HemOnc regimens.

---

## Quick start

### Prerequisites

- [`tmux`](https://github.com/tmux/tmux) must be installed (`apt install tmux` / `brew install tmux`)

### 1 — Start the Prefect server + workers

```bash
bash scripts/serve.sh
```

This opens a `hemonc` tmux session with three panes:

| Pane | Role |
|---|---|
| Left | Prefect server (`http://127.0.0.1:4200`) |
| Top-right | ETL worker (`etl-pool`) |
| Bottom-right | Regression worker (`regression-pool`) |

To stop everything:

```bash
bash scripts/serve.sh stop
```

### 2 — Run ETL

```bash
python -m assembler.flows.master --etl --workdir output-assembled

# Custom sigs file
python -m assembler.flows.master --etl --workdir output-assembled --sigs sigs_june_2025.csv

# Force fresh DB query (default: load from INPUTs/ snapshot)
python -m assembler.flows.master --etl --workdir output-assembled --run-query
```

### 3 — Run regression

```bash
python -m assembler.flows.master --regression --ref output_baseline --new output-assembled

# Custom output dir + explicit run number
python -m assembler.flows.master --regression \
    --ref output_baseline \
    --new output-assembled \
    --out output.regression_tests \
    --n 3

# ETL then regression in one command
python -m assembler.flows.master --etl --regression \
    --workdir output-assembled \
    --ref output_baseline \
    --new output-assembled
```

---

## Flags

### ETL (`--etl`)

| Flag | Default | Notes |
|---|---|---|
| `--workdir` | `output-assembled` | Output directory |
| `--sigs` | `sigs_march_2025.csv` | Sigs input file |
| `--run-query` | off | Live DB query; off = INPUTs/ snapshot |
| `--etl-env` | `etl` | Conda env name to run ETL in |

### Regression (`--regression`)

| Flag | Required | Notes |
|---|---|---|
| `--ref` | ✓ | Reference output directory |
| `--new` | ✓ | New output directory to compare |
| `--out` | no (`output.regression_tests`) | Where comparison JSON/HTML are saved |
| `--n` | no (auto-increments) | Run number for output filename |
| `--regress-env` | `etl-regression` | Conda env name to run regression in |

---

## Architecture

### Flow 1 — Preprocess 
- `resolve_variant_key` — blocking prerequisite; computes `variant_key` from `FieldSchema.key_source_cols`
- Masks computed in flow (pure Polars), slices passed to tasks — no internal re-filtering
- Parallel fan-out via `.submit()` for all handle + resolve tasks
- `handle_null` → 3 atomic tasks: `handle_null_condition` / `handle_null_group_keys` / `handle_null_sigs`
- `resolve_alldays` → 4 atomic tasks: `strip_brackets` → `resolve_ranges` → `collapse_zero` → `shift`
- `merge_and_audit` is **transactional** — wraps patch-application in `prefect.transactions.transaction()`; failure triggers `@rollback_hook`, emits a rollback artifact, reverts to pre-patch snapshot
- `tracked_pattern` Variable hook — `handle_pattern_alldays` writes `alldays_pattern_count_pre`; `resolve_alldays` reads it and logs a pre/post resolution delta

### Flow 2 — Transform 
- `RegStringHandler._process_group` called per `(regimen_cui, variant_cui, condition_cui)` group via Prefect `.map()`
- SRE math in `assembler/tools/sre_tools.py`

### Flow 3 — Wrapup 
- `task_build_regimens` runs first (blocking)
- `task_reg_groups`, `task_valid_drugs`, `task_routes`, `task_short_strings` fan out concurrently
- `task_export_rda` waits for all four, calls `tools/export_artifacts.R` via subprocess
- `task_validate` runs last

#### Concurrency engine — swap one argument

```python
#  Thread  (I/O-bound, default — no extra deps)
@flow(task_runner=ThreadPoolTaskRunner())
#  Dask    (pip install prefect-dask)
@flow(task_runner=DaskTaskRunner())
#  Ray     (pip install prefect-ray)
@flow(task_runner=RayTaskRunner())
```

### Caching
All datamodel tasks use `file_hash_cache_key` (24 h TTL) — same input file bytes + task version = cache hit on re-run.

### Schema registry
`FieldSchema` is the single column-name contract for the entire pipeline. It can be stored as a versioned Prefect Block:

```python
FieldSchemaBlock().save("prod-schema", overwrite=True)   # register once
schema = FieldSchemaBlock.load("prod-schema").schema      # load in any flow
```

---

## Stage → task map

| Stage | Task |
|---|---|
| 1 – preprocess | `preprocess_flow` |
| 2 – transform / SRE | `transform_flow` → `RegStringHandler._process_group` |
| 3a – reg groups | `task_reg_groups` |
| 3b – valid drugs | `task_valid_drugs` |
| 3c – routes | `task_routes` |
| 3d – short strings | `task_short_strings` |
| 4 – R export | `task_export_rda` |
| 5 – validate | `task_validate` |

Stages 3a–3d run concurrently. Stage 4 waits for all four.

