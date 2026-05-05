"""
assembler.tools.sre_module
~~~~~~~~~~~~~~~~~~~~~~~~~~~
SRE engine for per-group regimen string construction.

Classes
-------
Handlers
    Static pre-processing helpers (timing_sequence cleanup, indeterminate-cycle patch).

RegStringHandler
    _process_group(group_df) → pl.DataFrame
        Per (regimen_cui, variant_cui, condition_cui) group: build component vectors →
        collapse matrix → attach regString / cycleLength columns.
"""
import logging

import polars as pl

from tools.sre_tools import (
    get_last_cycle,
    convert_to_days,
    get_idays,
    build_component_vector,
    collapse_event_matrix_wrapper as create_reg_string,
)


class RegStringHandler:
    def __init__(self, frame_path: str, log_dir: str):
        self.frame = pl.read_parquet(frame_path)
        self.logger = self._setup_logging(log_dir)
        print("[INFO] Loaded schema:", self.frame.schema)
        self.logger.info(f"Loaded schema:\n {self.frame.schema}")

    def _setup_logging(self, log_dir):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        info_handler = logging.FileHandler(f"{log_dir}/SRE.process.log", mode='w')
        info_handler.setLevel(logging.INFO)
        info_handler.addFilter(lambda record: record.levelno == logging.INFO)
        info_handler.setFormatter(formatter)

        # All else (debug, warning, error, critical) → output.log
        output_handler = logging.FileHandler(f"{log_dir}/SRE.output.log", mode='w')
        output_handler.setLevel(logging.DEBUG)
        output_handler.addFilter(lambda record: record.levelno != logging.INFO)
        output_handler.setFormatter(formatter)

        logger.addHandler(info_handler)
        logger.addHandler(output_handler)

        return logger

    def _infer_block_tseq(self, block: pl.DataFrame) -> int:
        max_day_from_allDays = 0
        max_day_from_meta = 0
        for row in block.iter_rows(named=True):
            idays = get_idays(row["allDays"])
            if idays:
                max_day_from_allDays = max(max_day_from_allDays, max(idays))
        try:
            lb = float(row["cycle_length_lb"])
            ub = float(row["cycle_length_ub"])
            unit = row["cycle_length_unit"]
            meta_days = max(
                convert_to_days(lb, unit, idays),
                convert_to_days(ub, unit, idays),
            )
            max_day_from_meta = max(max_day_from_meta, meta_days)
        except Exception:
            self.logger.warning(
                f"Failed to convert cycle lengths to days for row: \n{row}"
            )

        tseq = max(max_day_from_allDays, max_day_from_meta)

        if tseq <= 0:
            raise ValueError("Cannot infer tseq for timing_sequence block")

        return int(tseq)

    def _process_group(self, group: pl.DataFrame) -> pl.DataFrame:
        """
        Input: regimen_cui .. variant_cui .. condition_cui group
        timing_sequence and cycle_length patches already applied by preprocess_flow.
        """
        try:
            total_vector_len = get_last_cycle(group.select("timing_sequence").unique().to_series().to_list())
        except Exception:
            print(group.select("timing_sequence").unique().to_series().to_list())
            raise ValueError("Processing total vector length is non-standard.")

        component_groups = group.group_by("component")
        counter_mix = 0
        for g_drug, df in component_groups:
            if df.height > 1:
                counter_mix += 1
                self.logger.debug(f"Component '{g_drug}' has multiple entries ({df.height} rows).")
        if counter_mix == 0:
            self.logger.debug(f"No duplicate components per group. Safe for processing w/o components groups")

        component_vectors = {}
        component_error = False
        days_error = False
        for row in group.iter_rows(named=True):
            drug              = str(row['component']).strip().replace(" ", "").lower().capitalize()
            timing_sequence   = row['timing_sequence']
            allDays           = row['allDays']
            cycle_length_lb   = row['cycle_length_lb']
            cycle_length_ub   = row['cycle_length_ub']
            cycle_length_unit = row['cycle_length_unit']

            idays = get_idays(allDays)

            cycle_lengths = set(map(float, [cycle_length_lb, cycle_length_ub]))

            self.logger.info(f"-----Component: {drug}------")
            self.logger.info(f"cycle size: {cycle_length_lb} {cycle_length_ub} {cycle_length_unit}")
            self.logger.info(f"days within a cycle (parsed): {idays}")
            self.logger.info(f"this component is given in: {timing_sequence} of {total_vector_len}-cycle long regimen.")

            for length in cycle_lengths:
                try:
                    length_in_days = convert_to_days(length, cycle_length_unit, allDays)
                except Exception as e:
                    self.logger.error(f"[SKIPPED days] Length:{length} @ CycLenUnit:{cycle_length_unit} @ allDays={allDays} : [ERR] {e}")
                    days_error = True
                    break

                try:
                    component_vector = build_component_vector(idays, length_in_days)
                except Exception as e:
                    self.logger.error(f"[SKIPPED COMPONENT] Cycle: {length} @ Unit: {cycle_length_unit} @ AllDays {allDays} @ i-AllDays {idays} @ Length in Days - {length_in_days}: [ERR] {e}")
                    component_error = True
                    break

            if component_error or days_error:
                break
            component_vectors.setdefault(drug, []).append((timing_sequence, component_vector))

        try:
            group_reg_string = create_reg_string(component_vectors)
        except Exception as e:
            group_id = group.select(['condition', 'regimen', 'variant']).to_dicts()
            self.logger.error(f"[SKIPPED GROUP] Failed to create reg string {group_id} - [ERR] {e}")
            null_col = pl.Series("regString", [None] * group.height)
            null_cycle = pl.Series("cycleLength", [None] * group.height)
            return group.with_columns([null_col, null_cycle])

        n_strings = len(group_reg_string)

        if n_strings == 0:
            null_col = pl.Series("regString", [None] * group.height)
            null_cycle = pl.Series("cycleLength", [None] * group.height)
            return group.with_columns([null_col, null_cycle])

        if n_strings > 1:
            group_id = group.select(['condition', 'regimen', 'variant']).to_numpy()[0, :]
            self.logger.debug(f"N_STRINGS={n_strings} @ {group_id}")
            self.logger.debug(group_reg_string)

        group_repeated = pl.concat([group] * n_strings, how="vertical")
        reg_string_col = pl.Series("regString", group_reg_string).repeat_by(group.height).explode()
        all_tseqs = [self._infer_block_tseq(block) for _, block in group.group_by("timing_sequence", maintain_order=True)]
        cycle_length_value = max(all_tseqs) if all_tseqs else 1

        cycle_length_col = (
            pl.Series("cycleLength", [cycle_length_value] * len(group_reg_string))
            .repeat_by(group.height)
            .explode()
        )
        group_with_regstrings = group_repeated.with_columns([
            reg_string_col,
            cycle_length_col,
        ])

        return group_with_regstrings


