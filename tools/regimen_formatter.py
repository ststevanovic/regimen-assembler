"""
assembler.tools.regimen_formatter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pure computation helper for shortString statistics.
No logging, no side-effects — called inside task_build_regimens
in assembler/tasks/datamodel.py; results emitted as Prefect artifact there.
"""

import pandas as pd


def shortstring_mapping_stats(frame: pd.DataFrame) -> dict:
    """
    Compute many-to-many mapping statistics between shortStrings and regimens.
    Pure computation — no logging. Caller (task_build_regimens) emits these
    as a Prefect table artifact visible in the UI run page.

    Returns dict with keys:
      regimens_with_multiple_shortstrings, shortstrings_shared_across_regimens,
      avg_shortstrings_per_regimen, avg_regimens_per_shortstring,
      top_shared  (list of (shortString, n_regimens) tuples, top 5)
    """
    shortstrings_per_regimen  = frame.groupby('regName')['shortString'].nunique()
    regimens_per_shortstring  = frame.groupby('shortString')['regName'].nunique()

    return {
        'regimens_with_multiple_shortstrings': int((shortstrings_per_regimen > 1).sum()),
        'shortstrings_shared_across_regimens': int((regimens_per_shortstring > 1).sum()),
        'avg_shortstrings_per_regimen':        round(float(shortstrings_per_regimen.mean()), 2),
        'avg_regimens_per_shortstring':        round(float(regimens_per_shortstring.mean()), 2),
        'top_shared': list(regimens_per_shortstring.nlargest(5).items()),
    }
