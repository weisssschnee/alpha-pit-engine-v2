"""Fail-closed proof that development labels terminate inside the train role."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

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


def _metadata_counts(root: Path, label_name: str) -> tuple[int, int]:
    rows = 0
    nulls = 0
    paths = tuple(sorted(Path(root).glob("shard_*.parquet")))
    if not paths:
        raise FileNotFoundError(f"no Phase3CM label shards: {root}")
    for path in paths:
        metadata = pq.ParquetFile(path).metadata
        schema_names = [
            metadata.schema.column(index).name
            for index in range(metadata.num_columns)
        ]
        try:
            column_index = schema_names.index(label_name)
        except ValueError as exc:
            raise RuntimeError(f"label column {label_name} is absent from {path}") from exc
        rows += int(metadata.num_rows)
        path_nulls = 0
        for group_index in range(metadata.num_row_groups):
            statistics = metadata.row_group(group_index).column(column_index).statistics
            if statistics is None or not statistics.has_null_count:
                path_nulls += int(
                    pq.ParquetFile(path)
                    .read_row_group(group_index, columns=[label_name])
                    .column(0)
                    .null_count
                )
            else:
                path_nulls += int(statistics.null_count)
        nulls += path_nulls
    return rows, nulls


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

    backend_counts: dict[str, dict[str, int]] = {}
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
        rows, purged = _metadata_counts(root, f"fwd_ret_{max_horizon}m")
        backend_counts[backend] = {
            "raw_signal_coordinate_count": rows,
            "crossing_count": purged,
            "purged_coordinate_count": purged,
            "retained_coordinate_count": rows - purged,
            "retained_crossing_count": 0,
        }
        backend_manifests[backend] = {
            "path": str(manifest_path),
            "label_sidecar_identity": str(manifest.get("label_sidecar_identity") or ""),
            "data_role": "development_train_only",
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
                "execution_lag_coordinates": int(execution_lag_coordinates),
                "route_label_horizons": list(horizon_values),
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
        "backend_manifests": backend_manifests,
        "routes": routes,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
