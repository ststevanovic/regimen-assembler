"""
assembler – self-contained Prefect ETL pipeline for HemOnc regimens.

Entry points
------------
    from flows.etl_flow        import hemonc_etl
    from flows.regression_flow import hemonc_regression
    from flows.master          import master_runner

CLI
---
    python -m assembler.flows.etl_flow        --workdir output.latest
    python -m assembler.flows.regression_flow --ref output.phase1 --new output.next
    python -m assembler.flows.master          --etl --regression --ref ... --new ...
"""
