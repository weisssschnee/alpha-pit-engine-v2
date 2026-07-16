from __future__ import annotations

from our_system_phase2.services.phase3cm_environment_audit import classify_legacy_hot_path


def test_hot_path_audit_distinguishes_active_parallel_and_installed_only_libraries() -> None:
    source = """
import pyarrow.parquet as pq
import pandas as pd
from numba import njit
@njit(cache=True)
def rank(values): return values
table = pq.read_table(path)
frame = table.to_pandas()
for candidate in candidates:
    frame.groupby('trade_time').rank()
"""
    audit = classify_legacy_hot_path(source)

    assert audit["pyarrow"]["status"] == "ACTIVE_HOT_PATH"
    assert audit["numba"]["parallel_kernel"] is False
    assert audit["pandas"]["status"] == "ACTIVE_HOT_PATH"
    assert audit["polars"]["status"] == "INSTALLED_NOT_CALLED_BY_LEGACY_EVALUATOR"
    assert audit["parallel_axis"] == "NONE_SERIAL_CANDIDATE_AND_SHARD_LOOPS"

