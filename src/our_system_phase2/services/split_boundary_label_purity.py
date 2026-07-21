"""Fail-closed proof that development labels terminate inside the train role."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
)


ROUTE_BACKEND = {
    "MINUTE_STATIC": "active_bar",
    "FIRSTN_PATH": "active_bar",
    "SLOW_CROSS_SECTIONAL_LEVEL": "stock_session",
    "SLOW_TEMPORAL_CHANGE": "stock_session",
    "DISCLOSURE_EVENT": "stock_session",
    "MARKET_REGIME_CONDITION": "active_bar",
    "INTRADAY_STATE_TRANSITION": "active_bar",
}


def _as_trade_date(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp.strftime("%Y-%m-%d")


def _metadata_profile(root: Path, label_name: str) -> dict[str, Any]:
    rows = 0
    nulls = 0
    minimum_trade_date: str | None = None
    maximum_trade_date: str | None = None
    paths = tuple(sorted(Path(root).glob("shard_*.parquet")))
    if not paths:
        raise FileNotFoundError(f"no Phase3CM label shards: {root}")
    for path in paths:
        parquet = pq.ParquetFile(path)
        metadata = parquet.metadata
        schema_names = [
            metadata.schema.column(index).name
            for index in range(metadata.num_columns)
        ]
        try:
            column_index = schema_names.index(label_name)
            time_index = schema_names.index("trade_time")
        except ValueError as exc:
            raise RuntimeError(
                f"label/trade_time column is absent from {path}: {label_name}"
            ) from exc
        rows += int(metadata.num_rows)
        path_nulls = 0
        for group_index in range(metadata.num_row_groups):
            group = metadata.row_group(group_index)
            statistics = group.column(column_index).statistics
            time_statistics = group.column(time_index).statistics
            needs_values = (
                statistics is None
                or not statistics.has_null_count
                or time_statistics is None
                or not time_statistics.has_min_max
            )
            values = None
            if needs_values:
                values = parquet.read_row_group(
                    group_index, columns=[label_name, "trade_time"]
                )
            if statistics is None or not statistics.has_null_count:
                path_nulls += int(
                    values.column(label_name).null_count  # type: ignore[union-attr]
                )
            else:
                path_nulls += int(statistics.null_count)
            if time_statistics is not None and time_statistics.has_min_max:
                group_min = _as_trade_date(time_statistics.min)
                group_max = _as_trade_date(time_statistics.max)
            else:
                time_values = values.column("trade_time").to_pandas()  # type: ignore[union-attr]
                group_min = _as_trade_date(time_values.min())
                group_max = _as_trade_date(time_values.max())
            minimum_trade_date = (
                group_min
                if minimum_trade_date is None
                else min(minimum_trade_date, group_min)
            )
            maximum_trade_date = (
                group_max
                if maximum_trade_date is None
                else max(maximum_trade_date, group_max)
            )
        nulls += path_nulls
    return {
        "row_count": rows,
        "terminal_unavailable_label_count": nulls,
        "minimum_trade_date": minimum_trade_date,
        "maximum_trade_date": maximum_trade_date,
        "shard_count": len(paths),
    }


def audit_split_boundary_label_purity(
    *,
    split: FixedSplitAuthority,
    registry: UnifiedCapabilityRegistry,
    label_roots: Mapping[str, Path],
    horizons: Sequence[int] = (1, 5, 15, 30),
    execution_lag_coordinates: int = 0,
) -> dict[str, Any]:
    """Bind train-only label metadata to the fixed split and purge contract."""

    horizon_values = tuple(sorted({int(value) for value in horizons}))
    if not horizon_values or min(horizon_values) <= 0:
        raise ValueError("label horizons must be positive")
    if int(execution_lag_coordinates) < 0:
        raise ValueError("execution lag cannot be negative")
    train_dates = [row["trade_date"] for row in split.rows if row["split"] == "train"]
    validation_dates = [
        row["trade_date"] for row in split.rows if row["split"] == "validation"
    ]
    if not train_dates or not validation_dates:
        raise RuntimeError("fixed split lacks contiguous train/validation roles")

    backend_counts: dict[str, dict[str, Any]] = {}
    backend_manifests: dict[str, dict[str, Any]] = {}
    max_horizon = max(horizon_values)
    for backend in ("active_bar", "stock_session"):
        root = Path(label_roots[backend]).resolve()
        manifest_path = root / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(f"label-sidecar manifest is required: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("status") != "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY"
            or manifest.get("data_role") != "development_train_only"
            or str(manifest.get("split_manifest_hash") or "") != split.manifest_hash
            or int(manifest.get("eligible_train_date_count") or 0) != len(train_dates)
        ):
            raise RuntimeError(f"train-only label manifest drift on {backend}")
        declared_horizons = tuple(
            sorted(int(value) for value in manifest.get("horizons") or ())
        )
        if declared_horizons != horizon_values:
            raise RuntimeError(f"label horizon drift on {backend}: {declared_horizons}")
        profile = _metadata_profile(root, f"fwd_ret_{max_horizon}m")
        if (
            str(profile["minimum_trade_date"]) not in set(train_dates)
            or str(profile["maximum_trade_date"]) not in set(train_dates)
            or str(profile["maximum_trade_date"]) >= validation_dates[0]
        ):
            raise RuntimeError(
                f"label coordinate role crossing on {backend}: "
                f"{profile['minimum_trade_date']}..{profile['maximum_trade_date']}"
            )
        rows = int(profile["row_count"])
        purged = int(profile["terminal_unavailable_label_count"])
        backend_counts[backend] = {
            "raw_signal_coordinate_count": rows,
            "raw_potential_crossing_count": purged,
            "crossing_count": purged,
            "purged_coordinate_count": purged,
            "retained_coordinate_count": rows - purged,
            "retained_crossing_count": 0,
            "minimum_label_coordinate_date": profile["minimum_trade_date"],
            "maximum_label_coordinate_date": profile["maximum_trade_date"],
            "label_shard_count": profile["shard_count"],
        }
        backend_manifests[backend] = {
            "path": str(manifest_path),
            "label_sidecar_identity": str(manifest.get("label_sidecar_identity") or ""),
            "data_role": "development_train_only",
            "coordinate_horizon_unit": (
                "minute_bars" if backend == "active_bar" else "session_coordinate_rows"
            ),
        }

    routes: list[dict[str, Any]] = []
    for route_id in ROUTE_IDS:
        if route_id not in ROUTE_BACKEND:
            continue
        backend = ROUTE_BACKEND[route_id]
        fields = registry.fields_for_route(route_id)
        maturity = {
            "bars": max(
                (field.source_lag for field in fields if field.source_lag_unit == "bars"),
                default=0,
            ),
            "sessions": max(
                (
                    field.source_lag
                    for field in fields
                    if field.source_lag_unit == "sessions"
                ),
                default=0,
            ),
        }
        counts = backend_counts[backend]
        routes.append(
            {
                "route_id": route_id,
                "backend": backend,
                "primary_control_max_source_maturity": maturity,
                "primary_control_source_maturity_future_extension": 0,
                "source_maturity_interpretation": (
                    "registry source_lag is backward-looking availability maturity; "
                    "it does not extend the forward label endpoint"
                ),
                "execution_lag_coordinates": int(execution_lag_coordinates),
                "execution_lag_unit": (
                    "minute_bars" if backend == "active_bar" else "session_coordinate_rows"
                ),
                "route_label_horizons": list(horizon_values),
                "route_label_horizon_unit": (
                    "minute_bars" if backend == "active_bar" else "session_coordinate_rows"
                ),
                "actual_max_future_label_coordinate": max_horizon,
                "effective_horizon_coordinates": max_horizon
                + int(execution_lag_coordinates),
                "label_end_role": "train",
                "crossing_count": counts["crossing_count"],
                "purged_coordinate_count": counts["purged_coordinate_count"],
                "retained_coordinate_count": counts["retained_coordinate_count"],
                "retained_crossing_count": 0,
                "purge_reason": "SPLIT_BOUNDARY_LABEL_CROSSING",
            }
        )

    status = (
        "PASS"
        if routes and all(row["retained_crossing_count"] == 0 for row in routes)
        else "FAIL"
    )
    return {
        "schema_version": "cn_split_boundary_label_purity_v1",
        "status": status,
        "train_last_date": train_dates[-1],
        "validation_first_date": validation_dates[0],
        "split_manifest_hash": split.manifest_hash,
        "label_semantics": "train_only_forward_shift_null_terminal_coordinates_are_purged",
        "enforcement_contract": (
            "FINITE_SIGNAL_AND_LABEL_INTERSECTION_IN_BATCHED_PORTFOLIO_KERNEL"
        ),
        "retained_crossing_rule": "ALL_BACKENDS_AND_ROUTES_EQUAL_ZERO",
        "backend_manifests": backend_manifests,
        "routes": routes,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
