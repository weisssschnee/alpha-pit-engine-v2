from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd


PREFERRED_STOCK_PIT_PARQUET_DATASET_PATH = Path(
    r"G:\Project_V7_Rotation\scripts\data\phase2_stock_tdx_official_20250806_to_20260508_maxopt.parquet"
)
LEGACY_STOCK_PIT_PARQUET_DATASET_PATH = Path(
    r"G:\Project_V7_Rotation\scripts\data\phase2_stock_validation_slice_2026-04-27.parquet"
)
DEFAULT_STOCK_PIT_PARQUET_DATASET_PATH = (
    PREFERRED_STOCK_PIT_PARQUET_DATASET_PATH
    if PREFERRED_STOCK_PIT_PARQUET_DATASET_PATH.exists()
    else LEGACY_STOCK_PIT_PARQUET_DATASET_PATH
)
DEFAULT_STOCK_PIT_CSV_DATASET_PATH = DEFAULT_STOCK_PIT_PARQUET_DATASET_PATH.with_suffix(".csv.gz")
LEGACY_SECTOR_PANEL_CSV_DATASET_PATH = Path(
    r"G:\Project_V7_Rotation\scripts\data\tdx_sector_data_p3_enhanced.csv"
)
LEGACY_SECTOR_PANEL_PARQUET_DATASET_PATH = LEGACY_SECTOR_PANEL_CSV_DATASET_PATH.with_suffix(".parquet")
DEFAULT_REAL_MARKET_DATASET_PATH = Path(
    os.environ.get(
        "PHASE2_REAL_MARKET_DATASET_PATH",
        str(
            DEFAULT_STOCK_PIT_PARQUET_DATASET_PATH
            if DEFAULT_STOCK_PIT_PARQUET_DATASET_PATH.exists()
            else DEFAULT_STOCK_PIT_CSV_DATASET_PATH
        ),
    )
)
REAL_MARKET_PANEL_REQUIRED_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "amount",
    "volume",
    "code",
]
REAL_MARKET_PANEL_OPTIONAL_TARGET_COLUMNS = [
    "daily_ret",
    "return_1d",
    "return_5d",
    "return_20d",
]
REAL_MARKET_VALIDATION_PERIOD_MONTHS = 3


def dataset_role_for_path(path: Path | str | None) -> str:
    if path is None:
        return "unknown_panel"
    panel_path = Path(str(path))
    name = panel_path.name.lower()
    if name.startswith("phase2_stock_"):
        return "stock_pit_panel"
    if name.startswith("tdx_sector_data") or "sector_data" in name:
        return "legacy_sector_panel"
    return "unknown_panel"


def panel_header(path: Path | str) -> list[str]:
    panel_path = Path(path)
    if panel_path.suffix.lower() == ".parquet":
        try:
            import pyarrow.parquet as pq

            return list(pq.ParquetFile(panel_path).schema.names)
        except Exception:
            return list(pd.read_parquet(panel_path, columns=[]).columns)
    return list(pd.read_csv(panel_path, nrows=0).columns)


def _csv_header(path: Path) -> list[str]:
    return panel_header(path)


def scan_real_market_csv_panel(
    path: Path | str = DEFAULT_REAL_MARKET_DATASET_PATH,
    *,
    chunksize: int = 200_000,
) -> dict[str, Any]:
    panel_path = Path(path)
    required_usecols = [
        *REAL_MARKET_PANEL_REQUIRED_COLUMNS,
        *REAL_MARKET_PANEL_OPTIONAL_TARGET_COLUMNS,
    ]
    header = panel_header(panel_path)
    usecols = [column for column in required_usecols if column in header]
    rows = 0
    codes: set[str] = set()
    min_date = None
    max_date = None
    missing_counts = {column: 0 for column in usecols}

    if panel_path.suffix.lower() == ".parquet":
        chunks = [pd.read_parquet(panel_path, columns=usecols)]
    else:
        chunks = pd.read_csv(panel_path, usecols=usecols, chunksize=chunksize)

    for chunk in chunks:
        rows += len(chunk)
        codes.update(chunk["code"].dropna().astype(str).unique().tolist())
        dates = pd.to_datetime(chunk["date"], errors="coerce")
        if dates.notna().any():
            chunk_min = dates.min()
            chunk_max = dates.max()
            min_date = chunk_min if min_date is None or chunk_min < min_date else min_date
            max_date = chunk_max if max_date is None or chunk_max > max_date else max_date
        for column in usecols:
            missing_counts[column] += int(chunk[column].isna().sum())

    return {
        "path": str(panel_path),
        "dataset_role": dataset_role_for_path(panel_path),
        "row_count": rows,
        "instrument_count": len(codes),
        "min_date": str(min_date.date()) if min_date is not None else None,
        "max_date": str(max_date.date()) if max_date is not None else None,
        "missing_counts": missing_counts,
        "missing_requested_columns": sorted(set(required_usecols).difference(usecols)),
    }


def build_real_market_data_contract(
    path: Path | str = DEFAULT_REAL_MARKET_DATASET_PATH,
    *,
    full_scan: bool = False,
) -> dict[str, Any]:
    panel_path = Path(path)
    exists = panel_path.exists()
    columns = _csv_header(panel_path) if exists else []
    missing_required = [
        column for column in REAL_MARKET_PANEL_REQUIRED_COLUMNS if column not in columns
    ]
    available_targets = [
        column for column in REAL_MARKET_PANEL_OPTIONAL_TARGET_COLUMNS if column in columns
    ]
    contract = {
        "dataset_path": str(panel_path),
        "dataset_role": dataset_role_for_path(panel_path),
        "legacy_sector_panel_default_blocked": not panel_path.name.startswith("phase2_stock_"),
        "dataset_kind": "ohlcv_cross_section_panel_parquet"
        if panel_path.suffix.lower() == ".parquet"
        else "ohlcv_cross_section_panel_csv",
        "exists": exists,
        "size_mb": round(panel_path.stat().st_size / (1024 * 1024), 2) if exists else None,
        "required_columns": REAL_MARKET_PANEL_REQUIRED_COLUMNS,
        "optional_target_columns": REAL_MARKET_PANEL_OPTIONAL_TARGET_COLUMNS,
        "available_target_columns": available_targets,
        "missing_required_columns": missing_required,
        "validation_period_months": REAL_MARKET_VALIDATION_PERIOD_MONTHS,
        "validation_period_policy": "quarterly_3_month_windows",
        "can_start_real_validation": exists and not missing_required,
        "full_scan": None,
    }
    if full_scan and contract["can_start_real_validation"]:
        contract["full_scan"] = scan_real_market_csv_panel(panel_path)
    return contract
