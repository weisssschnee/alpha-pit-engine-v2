"""77o environment and active-hot-path evidence for the Phase3CM repair."""

from __future__ import annotations

import importlib.metadata
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from our_system_phase2.services.candidate_submission_receipt import _combined_file_hash


PACKAGE_NAMES = ("numpy", "pandas", "pyarrow", "numba", "polars", "bottleneck", "numexpr")
THREAD_ENV_NAMES = (
    "NUMBA_NUM_THREADS",
    "ARROW_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_MAX_THREADS",
)


def classify_legacy_hot_path(source: str) -> dict[str, Any]:
    text = str(source)
    numba_active = "@njit" in text or "@numba.njit" in text
    numba_parallel = bool("parallel=True" in text and ("@njit" in text or "@numba.njit" in text))
    return {
        "pyarrow": {
            "status": "ACTIVE_HOT_PATH" if "pq.read_table" in text or "ParquetFile" in text else "NOT_CALLED",
            "calls": [call for call in ("pq.read_table", "pq.ParquetFile", "read_row_group") if call in text],
        },
        "numpy": {
            "status": "ACTIVE_HOT_PATH" if "np." in text else "NOT_CALLED",
            "calls": [call for call in ("np.argsort", "np.nanquantile", "np.corrcoef") if call in text],
        },
        "numba": {
            "status": "ACTIVE_HOT_PATH_SINGLE_RANK_KERNEL" if numba_active else "NOT_CALLED",
            "parallel_kernel": numba_parallel,
            "kernel": "_rank_pct_1d_average_numba" if "_rank_pct_1d_average_numba" in text else None,
        },
        "pandas": {
            "status": "ACTIVE_HOT_PATH" if "pd.DataFrame" in text or ".groupby(" in text else "NOT_CALLED",
            "fallback_paths": [call for call in ("to_pandas", "pd.DataFrame", ".groupby(", ".sort_values(", ".rank(") if call in text],
        },
        "polars": {"status": "INSTALLED_NOT_CALLED_BY_LEGACY_EVALUATOR"},
        "bottleneck": {"status": "INSTALLED_NOT_CALLED_BY_LEGACY_EVALUATOR"},
        "numexpr": {
            "status": "THREAD_ENV_CONFIGURED_NO_EXPLICIT_KERNEL_CALL"
            if "NUMEXPR_MAX_THREADS" in text
            else "INSTALLED_NOT_CALLED_BY_LEGACY_EVALUATOR"
        },
        "parallel_axis": (
            "NUMBA_PARALLEL_KERNEL" if numba_parallel else "NONE_SERIAL_CANDIDATE_AND_SHARD_LOOPS"
        ),
        "full_series_retention_evidence": {
            "rows_by_hash": "rows_by_hash" in text,
            "expression_cache": "expression_cache" in text,
            "feature_matrix_cache": "feature_matrix_cache" in text,
            "pnl_row_dicts": "rows.append(" in text,
        },
    }


def _powershell_json(script: str) -> Any:
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(completed.stdout.strip())


def _package_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in PACKAGE_NAMES:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = "NOT_INSTALLED"
    return versions


def build_environment_audit(
    *,
    repo: Path,
    data_root: Path,
    device_benchmark: dict[str, Any],
) -> dict[str, Any]:
    evaluator = repo / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
    source = evaluator.read_text(encoding="utf-8")
    host = _powershell_json(
        "$cpu=Get-CimInstance Win32_Processor|Select-Object -First 1 Name,NumberOfCores,NumberOfLogicalProcessors,MaxClockSpeed;"
        "$mem=Get-CimInstance Win32_ComputerSystem|Select-Object TotalPhysicalMemory;"
        "$disk=Get-CimInstance Win32_DiskDrive|Select-Object Model,Size,InterfaceType;"
        "$vol=Get-Volume -DriveLetter D|Select-Object DriveLetter,Size,SizeRemaining,FileSystem;"
        "[ordered]@{cpu=$cpu;memory=$mem;disks=$disk;execution_volume=$vol}|ConvertTo-Json -Depth 6 -Compress"
    )
    shards = sorted(data_root.rglob("*.parquet"))
    shard_rows = []
    for shard_index, shard in enumerate(shards):
        parquet = pq.ParquetFile(shard)
        trade_column = parquet.schema_arrow.names.index("trade_time")
        row_group_ranges = []
        for ordinal in range(parquet.metadata.num_row_groups):
            column = parquet.metadata.row_group(ordinal).column(trade_column)
            stats = column.statistics
            row_group_ranges.append((str(stats.min), str(stats.max)))
        shard_rows.append(
            {
                "source_shard": shard_index,
                "path": str(shard),
                "rows": parquet.metadata.num_rows,
                "row_groups": parquet.metadata.num_row_groups,
                "every_row_group_spans_full_development_range": len(set(row_group_ranges)) == 1,
                "trade_time_min": min(item[0] for item in row_group_ranges),
                "trade_time_max": max(item[1] for item in row_group_ranges),
            }
        )
    return {
        "schema_version": "cn_phase3cm_77o_environment_audit_v1",
        "execution_host": "77o",
        "python": {"executable": sys.executable, "version": platform.python_version()},
        "packages": _package_versions(),
        "host": host,
        "thread_environment": {name: os.environ.get(name, "UNSET") for name in THREAD_ENV_NAMES},
        "device_read_benchmark": device_benchmark,
        "legacy_evaluator": {
            "path": str(evaluator),
            "receipt_context_evaluator_hash": _combined_file_hash([evaluator]),
            "active_hot_path": classify_legacy_hot_path(source),
        },
        "physical_release_layout": {
            "shard_count": len(shards),
            "shards": shard_rows,
            "partitioning_finding": "SAME_TIME_RANGE_STOCK_PARTITIONS",
            "legacy_shard_local_cross_section_is_full_market": False,
            "required_repair": "GLOBAL_TRADE_TIME_BARRIER_ACROSS_ALL_16_SHARDS",
        },
    }
