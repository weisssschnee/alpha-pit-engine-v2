"""Run the development-only CN full-field information census on 77o.

This is a non-performance research job.  It inventories every authoritative
field record, measures only materialized typed capabilities, and selects an
exploratory representative pack without returns, labels, rewards or selector
output.  Raw fundamental columns keep their qualification evidence but are
never promoted to generator roots by this job.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from our_system_phase2.services.field_information_census import (
    InformationCensusPolicy,
    aligned_pairwise_nmi,
    field_metrics,
    normalized_entropy,
    pairwise_nmi,
    quantile_codes,
    select_representative_core_pack,
)
from our_system_phase2.services.fundamental_representations import (
    CanonicalFundamentalMaterializer,
)
from our_system_phase2.services.pit_fundamental_fabric import (
    FundamentalFieldRequest,
    PITFundamentalFabricAdapter,
    load_development_sessions,
    normalize_cn_code,
)


REPO = Path(__file__).resolve().parents[1]
FORWARD_START = pd.Timestamp("2026-01-01")
CHIP_ROLES = {
    "chip_historical_low": "benchmark-only",
    "chip_historical_high": "benchmark-only",
    "chip_cost_p05": "interaction-only",
    "chip_cost_p15": "interaction-only",
    "chip_cost_p50": "interaction-only",
    "chip_cost_p85": "interaction-only",
    "chip_cost_p95": "interaction-only",
    "chip_cost_weighted_mean": "interaction-only",
    "chip_profit_ratio": "condition-only",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                columns.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _stable_positions(length: int, count: int) -> list[int]:
    if length <= 0 or count <= 0:
        return []
    return sorted(set(np.linspace(0, length - 1, min(length, count), dtype="int64").tolist()))


def _parquet_files(release_manifest: Path, manifest: Mapping[str, Any]) -> list[Path]:
    paths: list[Path] = []
    for index, row in enumerate(manifest.get("shards", [])):
        declared = Path(str(row.get("output_path", "")))
        candidates = [declared, release_manifest.parent / f"shard_{index:02d}"]
        chosen: list[Path] = []
        for candidate in candidates:
            if candidate.is_file() and candidate.suffix.lower() == ".parquet":
                chosen = [candidate]
            elif candidate.is_dir():
                chosen = sorted(candidate.rglob("*.parquet"))
            if chosen:
                break
        if not chosen:
            raise FileNotFoundError(f"no parquet file for release shard {index}: {candidates}")
        paths.extend(chosen)
    if not paths:
        paths = sorted(release_manifest.parent.glob("shard_*"))
        paths = [item for root in paths for item in root.rglob("*.parquet")]
    if not paths:
        raise FileNotFoundError(f"release has no parquet files: {release_manifest}")
    return paths


def _sample_diagnostics(sample: pd.DataFrame, field: str) -> dict[str, Any]:
    numeric = pd.to_numeric(sample[field], errors="coerce")
    finite = np.isfinite(numeric.to_numpy(dtype="float64"))
    ordered = pd.DataFrame(
        {
            "code": sample["code"].astype(str),
            "trade_time": pd.to_datetime(sample["trade_time"], errors="coerce"),
            "value": numeric,
        }
    ).sort_values(["code", "trade_time"], kind="mergesort")
    previous = ordered.groupby("code", sort=False)["value"].shift(1)
    comparable = ordered["value"].notna() & previous.notna()
    changed = int((ordered.loc[comparable, "value"].to_numpy() != previous.loc[comparable].to_numpy()).sum())
    cross = pd.DataFrame(
        {"trade_time": pd.to_datetime(sample["trade_time"], errors="coerce"), "value": numeric}
    ).dropna().groupby("trade_time", sort=False)["value"].std()
    return {
        "sample_finite": int(finite.sum()),
        "temporal_comparison_count": int(comparable.sum()),
        "temporal_change_count": changed,
        "cross_sectional_std_mean": float(cross.mean()) if len(cross) else 0.0,
        "minimum": float(numeric[finite].min()) if finite.any() else None,
        "maximum": float(numeric[finite].max()) if finite.any() else None,
    }


def collect_true1min(
    release_manifest: Path,
    master_rows: Iterable[Mapping[str, Any]],
    *,
    row_groups_per_file: int,
    sample_modulus: int,
    field_batch_size: int = 16,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, pd.Series], dict[str, Any]]:
    manifest = json.loads(release_manifest.read_text(encoding="utf-8"))
    if str(manifest.get("data_role", "")).lower() != "development":
        raise PermissionError("true1min release is not declared development-only")
    if manifest.get("forward_2026_present") is not False:
        raise PermissionError("true1min release does not prove FORWARD_2026_SEALED")
    files = _parquet_files(release_manifest, manifest)
    capability = {
        str(row["field_name"]): dict(row)
        for row in master_rows
        if row.get("record_kind") == "CAPABILITY_FIELD"
        and row.get("source_table") == "true1min_augmented_panel"
        and row.get("field_role") != "blocked"
    }
    first_schema = pq.ParquetFile(files[0]).schema_arrow
    numeric_schema = {
        field.name
        for field in first_schema
        if pa.types.is_boolean(field.type)
        or pa.types.is_integer(field.type)
        or pa.types.is_floating(field.type)
        or pa.types.is_decimal(field.type)
    }
    physical = sorted(
        field
        for field, row in capability.items()
        if str(row.get("source_field")) == field and field in numeric_schema
    )
    if {"code", "trade_time"} - set(first_schema.names):
        raise ValueError("true1min parquet lacks code/trade_time coordinates")
    footer: dict[str, dict[str, Any]] = {
        field: {"row_count": 0, "null_count": 0, "null_count_complete": True}
        for field in physical
    }
    coordinate_parts: list[pd.DataFrame] = []
    field_parts: dict[str, list[pd.Series]] = {field: [] for field in physical}
    selected_row_groups = 0
    total_rows = 0
    for file_index, path in enumerate(files, start=1):
        parquet = pq.ParquetFile(path)
        schema_names = parquet.schema_arrow.names
        indices = {name: schema_names.index(name) for name in physical if name in schema_names}
        for row_group in range(parquet.metadata.num_row_groups):
            metadata = parquet.metadata.row_group(row_group)
            total_rows += metadata.num_rows
            for field in physical:
                state = footer[field]
                state["row_count"] += metadata.num_rows
                if field not in indices:
                    state["null_count"] += metadata.num_rows
                    continue
                statistics = metadata.column(indices[field]).statistics
                if statistics is None or statistics.null_count is None:
                    state["null_count_complete"] = False
                else:
                    state["null_count"] += int(statistics.null_count)
        for row_group in _stable_positions(parquet.metadata.num_row_groups, row_groups_per_file):
            selected_row_groups += 1
            coordinates = parquet.read_row_group(row_group, columns=["code", "trade_time"]).to_pandas()
            clocks = pd.to_datetime(coordinates["trade_time"], errors="raise")
            if clocks.max() >= FORWARD_START:
                raise PermissionError("FORWARD_2026_SEALED violation in true1min sample")
            hashes = pd.util.hash_pandas_object(coordinates, index=False).to_numpy(dtype="uint64")
            buckets = (hashes % sample_modulus).astype("int16")
            positions = np.flatnonzero(buckets < 2)
            coordinate_sample = coordinates.iloc[positions].reset_index(drop=True)
            coordinate_sample["sample_bucket"] = buckets[positions]
            coordinate_parts.append(coordinate_sample)
            for start in range(0, len(physical), field_batch_size):
                batch = physical[start : start + field_batch_size]
                available = [field for field in batch if field in parquet.schema_arrow.names]
                values = parquet.read_row_group(row_group, columns=available).to_pandas()
                for field in batch:
                    if field in values:
                        field_parts[field].append(values[field].iloc[positions].reset_index(drop=True))
                    else:
                        field_parts[field].append(pd.Series(np.nan, index=range(len(positions)), name=field))
                del values
                pa.default_memory_pool().release_unused()
            del coordinates, coordinate_sample
            pa.default_memory_pool().release_unused()
        print(
            json.dumps(
                {
                    "phase": "true1min_stream",
                    "completed_files": file_index,
                    "total_files": len(files),
                    "sample_rows_so_far": sum(len(part) for part in coordinate_parts),
                },
                sort_keys=True,
            ),
            flush=True,
        )
    sample = pd.concat(coordinate_parts, ignore_index=True)
    value_frame = pd.DataFrame(
        {
            field: pd.concat(field_parts[field], ignore_index=True).to_numpy()
            for field in physical
        }
    )
    sample = pd.concat([sample.reset_index(drop=True), value_frame], axis=1)
    sample["state_bar_return_sign"] = np.sign(pd.to_numeric(sample.get("ret_1m"), errors="coerce"))
    sample["state_intraday_return_sign"] = np.sign(
        pd.to_numeric(sample.get("intraday_ret_from_open"), errors="coerce")
    )
    sample["state_close_range_location_sign"] = np.sign(
        2.0 * pd.to_numeric(sample.get("close"), errors="coerce")
        - pd.to_numeric(sample.get("high"), errors="coerce")
        - pd.to_numeric(sample.get("low"), errors="coerce")
    )
    fields = [*physical, "state_bar_return_sign", "state_intraday_return_sign", "state_close_range_location_sign"]
    fields = [field for field in fields if field in capability and field in sample]
    full_counts: dict[str, dict[str, Any]] = {}
    coverage_basis: dict[str, str] = {}
    for field in fields:
        diagnostics = _sample_diagnostics(sample, field)
        if field in footer and footer[field]["null_count_complete"]:
            row_count = int(footer[field]["row_count"])
            finite_count = row_count - int(footer[field]["null_count"])
            coverage_basis[field] = "PARQUET_FOOTER_NULL_COUNT_FULL_RELEASE"
        else:
            row_count = total_rows
            rate = diagnostics["sample_finite"] / max(1, len(sample))
            finite_count = int(round(rate * row_count))
            coverage_basis[field] = "DETERMINISTIC_SAMPLE_ESTIMATE"
        full_counts[field] = {
            "row_count": row_count,
            "finite_count": finite_count,
            **{key: value for key, value in diagnostics.items() if key != "sample_finite"},
        }
    policy = InformationCensusPolicy()
    metrics, codes = field_metrics(sample, fields=fields, full_counts=full_counts, policy=policy)
    metadata_by_field = capability
    for row in metrics:
        field = str(row["field_id"])
        source = metadata_by_field[field]
        role = str(source["field_role"])
        unique_floor = 2 if role == "state-only" else 16
        row.update(
            context="TRUE1MIN",
            data_family=str(source["data_family"]),
            field_role=role,
            semantic_role=str(source["semantic_role"]),
            support_unit="minute row deterministic coordinate sample",
            semantic_support_group=f'TRUE1MIN::{source["data_family"]}',
            information_status="EVALUATED_DEVELOPMENT_ONLY",
            information_qualified=(
                float(row["coverage"]) >= 0.60
                and int(row["sample_unique"]) >= unique_floor
                and (
                    role == "state-only"
                    or float(row["temporal_change_rate"]) >= 0.001
                    or float(row["cross_sectional_std_mean"]) > 0.0
                )
            ),
            coverage_basis=coverage_basis[field],
            variation_basis="DETERMINISTIC_ROW_GROUP_AND_COORDINATE_SAMPLE",
        )
    pairs: list[dict[str, Any]] = []
    by_family: dict[str, dict[str, np.ndarray]] = defaultdict(dict)
    for field, field_codes in codes.items():
        by_family[str(metadata_by_field[field]["data_family"])][field] = field_codes
    sample_buckets = sample["sample_bucket"].to_numpy()
    for family, family_codes in sorted(by_family.items()):
        for row in pairwise_nmi(family_codes, sample_buckets=sample_buckets):
            row["semantic_support_group"] = f"TRUE1MIN::{family}"
            pairs.append(row)
    series = {
        field: pd.Series(
            pd.to_numeric(sample[field], errors="coerce").to_numpy(),
            index=pd.MultiIndex.from_frame(sample[["code", "trade_time"]]),
            name=field,
        )
        for field in fields
    }
    evidence = {
        "release_manifest": str(release_manifest),
        "release_manifest_sha256": sha256(release_manifest),
        "release_hash": manifest.get("release_hash"),
        "schema_sha256": manifest.get("schema_sha256"),
        "full_release_row_count": total_rows,
        "file_count": len(files),
        "selected_row_group_count": selected_row_groups,
        "deterministic_sample_row_count": len(sample),
        "sample_rule": f"hash(code,trade_time)%{sample_modulus} in [0,1] within evenly spaced row groups",
        "field_batch_size": field_batch_size,
        "evaluated_field_count": len(metrics),
    }
    return metrics, pairs, series, evidence


class CachingPITAdapter(PITFundamentalFabricAdapter):
    """One-run read/materialization cache; no state survives the census."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._version_cache: dict[tuple[str, str, tuple[str, ...]], pd.DataFrame] = {}
        self._materialized_cache: dict[tuple[str, str, str, int], pd.DataFrame] = {}
        self._episode_cache: dict[tuple[str, str, tuple[str, ...]], pd.DataFrame] = {}

    @staticmethod
    def _codes(codes: Iterable[str]) -> tuple[str, ...]:
        return tuple(sorted({normalize_cn_code(code) for code in codes}))

    def _load_financial(self, request: FundamentalFieldRequest, codes: Iterable[str]) -> pd.DataFrame:
        key = (request.source_table, request.source_field, self._codes(codes))
        if key not in self._version_cache:
            self._version_cache[key] = super()._load_financial(request, key[2])
        return self._version_cache[key]

    def _load_holder(self, request: FundamentalFieldRequest, codes: Iterable[str]) -> pd.DataFrame:
        key = (request.source_table, request.source_field, self._codes(codes))
        if key not in self._version_cache:
            self._version_cache[key] = super()._load_holder(request, key[2])
        return self._version_cache[key]

    def materialize_level(self, request: FundamentalFieldRequest, coordinates: pd.DataFrame) -> pd.DataFrame:
        key = (request.source_table, request.source_field, request.transform, id(coordinates))
        if key not in self._materialized_cache:
            self._materialized_cache[key] = super().materialize_level(request, coordinates)
        return self._materialized_cache[key]

    def materialize_change(self, request: FundamentalFieldRequest, coordinates: pd.DataFrame) -> pd.DataFrame:
        key = (request.source_table, request.source_field, request.transform, id(coordinates))
        if key not in self._materialized_cache:
            self._materialized_cache[key] = super().materialize_change(request, coordinates)
        return self._materialized_cache[key]

    def disclosure_episodes(self, request: FundamentalFieldRequest, *, codes: Iterable[str]) -> pd.DataFrame:
        code_key = self._codes(codes)
        key = (request.source_table, request.source_field, code_key)
        if key not in self._episode_cache:
            self._episode_cache[key] = super().disclosure_episodes(request, codes=code_key)
        return self._episode_cache[key]


def _select_fundamental_codes(source_root: Path, count: int) -> list[str]:
    codes: set[str] = set()
    for table in (
        "balance_sheet_report_em",
        "profit_sheet_report_em",
        "cash_flow_sheet_report_em",
        "main_stock_holder_sina",
    ):
        root = source_root / table
        if not root.exists():
            continue
        for path in root.glob("*.parquet"):
            code = normalize_cn_code(path.stem)
            if code:
                codes.add(code)
    ordered = sorted(codes, key=lambda code: (hashlib.sha256(code.encode()).hexdigest(), code))
    return ordered[:count]


def _series_metric(
    frame: pd.DataFrame,
    field_id: str,
    *,
    spec: Mapping[str, Any],
    status: str = "EVALUATED_DEVELOPMENT_ONLY",
) -> tuple[dict[str, Any], pd.Series]:
    values = pd.to_numeric(frame[field_id], errors="coerce")
    finite = np.isfinite(values.to_numpy(dtype="float64"))
    codes = quantile_codes(values, bins=16)
    coordinate = pd.MultiIndex.from_frame(
        pd.DataFrame(
            {
                "code": frame["code"].astype(str),
                "session_time": pd.to_datetime(frame["session_time"], errors="coerce"),
            }
        )
    )
    indexed = pd.Series(values.to_numpy(), index=coordinate, name=field_id)
    ordered = pd.DataFrame(
        {
            "code": frame["code"].astype(str),
            "session_time": pd.to_datetime(frame["session_time"], errors="coerce"),
            "value": values,
        }
    ).sort_values(["code", "session_time"], kind="mergesort")
    previous = ordered.groupby("code", sort=False)["value"].shift(1)
    comparable = ordered["value"].notna() & previous.notna()
    changed = int((ordered.loc[comparable, "value"].to_numpy() != previous.loc[comparable].to_numpy()).sum())
    cross = ordered.dropna().groupby("session_time", sort=False)["value"].std()
    support_unit = str(spec.get("support_unit", "stock-session cross-section"))
    event_payload = support_unit == "disclosure episode"
    finite_count = int(finite.sum())
    unique = int(values.nunique(dropna=True))
    role = str(spec.get("field_role", "primary"))
    qualified = (
        finite_count >= (32 if event_payload else 64)
        and unique >= (2 if role in {"condition-only", "state-only"} else 8)
        and (event_payload or finite_count / max(1, len(frame)) >= 0.20)
    )
    metric = {
        "field_id": field_id,
        "row_count": len(frame),
        "finite_count": finite_count,
        "coverage": finite_count / max(1, len(frame)),
        "sample_count": finite_count,
        "sample_unique": unique,
        "normalized_entropy": normalized_entropy(codes),
        "temporal_comparison_count": int(comparable.sum()),
        "temporal_change_rate": changed / max(1, int(comparable.sum())),
        "cross_sectional_std_mean": float(cross.mean()) if len(cross) else 0.0,
        "minimum": float(values[finite].min()) if finite.any() else None,
        "maximum": float(values[finite].max()) if finite.any() else None,
        "activation_rate": float((values.fillna(0.0) != 0.0).mean()),
        "context": "FUNDAMENTAL",
        "data_family": str(spec.get("semantic_family", "")),
        "field_role": role,
        "semantic_role": str(spec.get("representation_type", "")),
        "support_unit": support_unit,
        "semantic_support_group": (
            f'FUNDAMENTAL::{spec.get("semantic_family", "")}::{support_unit}::{spec.get("route_id", "")}'
        ),
        "information_status": status,
        "information_qualified": qualified,
        "coverage_basis": "DETERMINISTIC_PIT_MATERIALIZATION_COORDINATES",
        "variation_basis": "DETERMINISTIC_PIT_MATERIALIZATION_COORDINATES",
        "representation_id": str(spec.get("representation_id", "")),
        "route_id": str(spec.get("route_id", "")),
    }
    return metric, indexed


def collect_fundamental(
    fundamental_manifest: Path,
    registry_path: Path,
    split_manifest: Path,
    *,
    code_count: int,
    session_count: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, pd.Series], dict[str, Any]]:
    manifest = json.loads(fundamental_manifest.read_text(encoding="utf-8"))
    source_root = Path(str(manifest["source_root"]))
    if not source_root.exists():
        raise FileNotFoundError(f"fundamental source root absent: {source_root}")
    sessions = load_development_sessions(split_manifest)
    if sessions.max() >= FORWARD_START:
        raise PermissionError("split manifest exposes 2026 to fundamental census")
    selected_sessions = sessions[_stable_positions(len(sessions), session_count)]
    codes = _select_fundamental_codes(source_root, code_count)
    if not codes:
        raise ValueError("no fundamental symbol partitions found")
    coordinates = pd.MultiIndex.from_product(
        [codes, selected_sessions + pd.Timedelta(hours=15)], names=["code", "session_time"]
    ).to_frame(index=False)
    daily_coordinates = pd.MultiIndex.from_product(
        [codes, sessions + pd.Timedelta(hours=15)], names=["code", "session_time"]
    ).to_frame(index=False)
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    specs = [
        dict(row.get("metadata", {}).get("canonical_representation", {}))
        for row in registry["fields"]
        if row.get("metadata", {}).get("canonical_representation")
    ]
    specs = [spec for spec in specs if bool(spec.get("search_eligible"))]
    adapter = CachingPITAdapter(
        source_root=source_root,
        sessions=sessions,
        maximum_observable_time=str(manifest["development_maximum_observable_time"]),
    )
    materializer = CanonicalFundamentalMaterializer(adapter)
    metrics: list[dict[str, Any]] = []
    series: dict[str, pd.Series] = {}
    errors: list[dict[str, str]] = []
    episode_coordinates: dict[tuple[str, str], pd.DataFrame] = {}
    for spec in sorted(specs, key=lambda item: str(item["field_id"])):
        field_id = str(spec["field_id"])
        representation_type = str(spec.get("representation_type", ""))
        try:
            if representation_type.startswith("disclosure_payload__"):
                source = spec["source_fields"][0]
                episode_key = (str(source["source_table"]), str(source["source_field"]))
                if episode_key not in episode_coordinates:
                    request = FundamentalFieldRequest(
                        source_table=episode_key[0],
                        source_field=episode_key[1],
                        route="DISCLOSURE_EVENT",
                    )
                    episodes = adapter.disclosure_episodes(request, codes=codes)
                    episode_coordinates[episode_key] = (
                        episodes[["code", "maturity_time"]]
                        .rename(columns={"maturity_time": "session_time"})
                        .drop_duplicates(["code", "session_time"])
                    )
                selected = episode_coordinates[episode_key]
            elif str(spec.get("operation")) == "disclosure_pulse":
                selected = daily_coordinates
            else:
                selected = coordinates
            frame = materializer.materialize(spec, selected)
            if field_id not in frame:
                raise KeyError(f"materializer did not return {field_id}")
            metric, indexed = _series_metric(frame, field_id, spec=spec)
            metrics.append(metric)
            series[field_id] = indexed
        except Exception as exc:  # isolate one representation without falsifying it
            errors.append({"field_id": field_id, "error_type": type(exc).__name__, "message": str(exc)})
            metrics.append(
                {
                    "field_id": field_id,
                    "row_count": 0,
                    "finite_count": 0,
                    "coverage": 0.0,
                    "sample_count": 0,
                    "sample_unique": 0,
                    "normalized_entropy": 0.0,
                    "temporal_comparison_count": 0,
                    "temporal_change_rate": 0.0,
                    "cross_sectional_std_mean": 0.0,
                    "minimum": None,
                    "maximum": None,
                    "context": "FUNDAMENTAL",
                    "data_family": str(spec.get("semantic_family", "")),
                    "field_role": str(spec.get("field_role", "primary")),
                    "semantic_role": representation_type,
                    "support_unit": str(spec.get("support_unit", "")),
                    "semantic_support_group": (
                        f'FUNDAMENTAL::{spec.get("semantic_family", "")}::{spec.get("support_unit", "")}::{spec.get("route_id", "")}'
                    ),
                    "information_status": "MATERIALIZATION_ERROR_FAIL_CLOSED",
                    "information_qualified": False,
                    "coverage_basis": "NOT_AVAILABLE",
                    "variation_basis": "NOT_AVAILABLE",
                    "representation_id": str(spec.get("representation_id", "")),
                    "route_id": str(spec.get("route_id", "")),
                }
            )
        if len(metrics) % 25 == 0:
            print(
                json.dumps(
                    {
                        "phase": "fundamental_materialization",
                        "completed_representations": len(metrics),
                        "total_representations": len(specs),
                        "errors": len(errors),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
    groups = {
        str(row["field_id"]): str(row["semantic_support_group"])
        for row in metrics
        if str(row["field_id"]) in series
    }
    pairs = aligned_pairwise_nmi(series, groups=groups, bins=16, maximum_rows=40_000)
    evidence = {
        "fabric_manifest": str(fundamental_manifest),
        "fabric_manifest_sha256": sha256(fundamental_manifest),
        "fabric_manifest_hash": manifest.get("manifest_hash"),
        "source_root": str(source_root),
        "split_manifest": str(split_manifest),
        "split_manifest_sha256": sha256(split_manifest),
        "selected_code_count": len(codes),
        "selected_session_count": len(selected_sessions),
        "level_coordinate_count": len(coordinates),
        "daily_pulse_coordinate_count": len(daily_coordinates),
        "evaluated_representation_count": len(metrics) - len(errors),
        "materialization_error_count": len(errors),
        "materialization_errors": errors,
        "raw_source_columns_opened_to_generator": 0,
        "zygc_status": "PIT_CONTRACT_UNRESOLVED_NOT_ACCESSED",
    }
    return metrics, pairs, series, evidence


def import_chip(census_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    metrics = json.loads((census_root / "field_information_metrics.json").read_text(encoding="utf-8"))
    pairs = json.loads((census_root / "pairwise_nmi.json").read_text(encoding="utf-8"))
    for row in metrics:
        field = str(row["field_id"])
        role = CHIP_ROLES[field]
        row.update(
            context="CHIP",
            data_family="chip_distribution",
            field_role=role,
            semantic_role=role,
            support_unit="stock-session",
            semantic_support_group="CHIP::chip_distribution",
            information_status="EVALUATED_DEVELOPMENT_ONLY_IMPORTED_VERIFIED_CENSUS",
            information_qualified=(
                float(row["coverage"]) >= 0.60
                and int(row["sample_unique"]) >= (2 if role in {"condition-only", "benchmark-only"} else 16)
                and float(row["temporal_change_rate"]) >= 0.001
            ),
            coverage_basis="FULL_CHIP_SIDECAR_SCAN",
            variation_basis="DETERMINISTIC_COORDINATE_SAMPLE",
        )
    for row in pairs:
        row["semantic_support_group"] = "CHIP::chip_distribution"
    manifest = census_root / "run_manifest.json"
    evidence = {
        "census_manifest": str(manifest),
        "census_manifest_sha256": sha256(manifest),
        "evaluated_field_count": len(metrics),
        "pair_count": len(pairs),
    }
    return metrics, pairs, evidence


def import_broad_event_support(pack_path: Path, master_rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    pack = json.loads(pack_path.read_text(encoding="utf-8"))
    mechanisms = {str(row["mechanism_id"]): row for row in pack.get("mechanisms", [])}
    metrics: list[dict[str, Any]] = []
    for row in master_rows:
        if row.get("record_kind") != "CAPABILITY_FIELD" or row.get("source_table") != "frozen_event_episode_registry":
            continue
        mechanism_id = str(row["field_name"]).removeprefix("broad_event_")
        mechanism = mechanisms.get(mechanism_id)
        support = 0
        if mechanism:
            support = min(int(item.get("support", 0)) for item in mechanism.get("seed_evidence", []))
        metrics.append(
            {
                "field_id": str(row["field_name"]),
                "row_count": support,
                "finite_count": support,
                "coverage": 1.0 if support else 0.0,
                "sample_count": support,
                "sample_unique": 0,
                "normalized_entropy": 0.0,
                "temporal_comparison_count": 0,
                "temporal_change_rate": 0.0,
                "cross_sectional_std_mean": 0.0,
                "minimum": None,
                "maximum": None,
                "context": "BROAD_EVENT_FROZEN_ENTRY",
                "data_family": "broad_event_frozen_entry",
                "field_role": str(row["field_role"]),
                "semantic_role": "frozen_event_entry",
                "support_unit": "event episode",
                "semantic_support_group": "BROAD_EVENT::frozen_entry",
                "information_status": "IMPORTED_FROZEN_EPISODE_SUPPORT_ONLY",
                "information_qualified": False,
                "coverage_basis": "FROZEN_DISCOVERY_ENTRY_PACK_SUPPORT",
                "variation_basis": "NOT_REEVALUATED_PACK_REMAINS_FROZEN",
            }
        )
    return metrics, {
        "pack_path": str(pack_path),
        "pack_sha256": sha256(pack_path),
        "mechanism_count": len(mechanisms),
        "performance_fields_ignored": True,
        "pack_modified": False,
    }


def build_universe(
    master: Mapping[str, Any],
    qualification_rows: Iterable[Mapping[str, Any]],
    metrics: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    qualification = {
        (str(row["source_table"]), str(row["source_field"])): dict(row)
        for row in qualification_rows
    }
    metric_by_field = {str(row["field_id"]): dict(row) for row in metrics}
    output: list[dict[str, Any]] = []
    for source in master["rows"]:
        row = dict(source)
        record_kind = str(row["record_kind"])
        metric = metric_by_field.get(str(row["field_name"])) if record_kind == "CAPABILITY_FIELD" else None
        qualified = qualification.get((str(row["source_table"]), str(row["source_field"])))
        if metric:
            status = str(metric["information_status"])
        elif record_kind == "SOURCE_FIELD" and qualified:
            status = "SOURCE_QUALIFICATION_EVIDENCE_" + str(qualified["qualification_state"])
        elif str(row["pit_status"]) == "PIT_CONTRACT_UNRESOLVED":
            status = "PIT_CONTRACT_UNRESOLVED"
        elif str(row["source_table"]) in {"true1min_plate_sparse", "plate_market_context", "plate_membership_release"}:
            status = "NOT_EVALUATED_REAL_PIT_PLATE_MATERIALIZATION_ABSENT_ON_77O"
        elif str(row["field_role"]) == "blocked" or str(row["semantic_role"]) == "METADATA_BLOCKED":
            status = "METADATA_OR_SEARCH_BLOCKED"
        else:
            status = "NOT_EVALUATED_NO_MATERIALIZATION_EVIDENCE"
        result = {
            "field_uid": row["field_uid"],
            "field_name": row["field_name"],
            "record_kind": record_kind,
            "data_family": row["data_family"],
            "source_table": row["source_table"],
            "source_field": row["source_field"],
            "entity_scope": row["entity_scope"],
            "semantic_role": row["semantic_role"],
            "field_role": row["field_role"],
            "pit_status": row["pit_status"],
            "materialization_status": row["materialization_status"],
            "search_status": row["search_status"],
            "recommended_routes": row["recommended_routes"],
            "information_status": status,
            "information_qualified": bool(metric and metric.get("information_qualified")),
            "core_pack_selected": False,
            "coverage": metric.get("coverage") if metric else (qualified.get("coverage_ratio") if qualified else None),
            "sample_unique": metric.get("sample_unique") if metric else None,
            "normalized_entropy": metric.get("normalized_entropy") if metric else None,
            "temporal_change_rate": metric.get("temporal_change_rate") if metric else None,
            "semantic_support_group": metric.get("semantic_support_group", "") if metric else "",
            "qualification_state": qualified.get("qualification_state", "") if qualified else "",
            "qualification_reason": qualified.get("qualification_reason", "") if qualified else "",
            "raw_generator_exposure": qualified.get("raw_generator_exposure", "") if qualified else "",
            "blocked_reason": row["blocked_reason"],
        }
        output.append(result)
    return sorted(output, key=lambda item: str(item["field_uid"]))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--qualification", type=Path, required=True)
    parser.add_argument("--true1min-release-manifest", type=Path, required=True)
    parser.add_argument("--fundamental-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--chip-census", type=Path, required=True)
    parser.add_argument("--broad-event-pack", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--row-groups-per-file", type=int, default=3)
    parser.add_argument("--true1min-sample-modulus", type=int, default=256)
    parser.add_argument("--true1min-field-batch-size", type=int, default=16)
    parser.add_argument("--fundamental-code-count", type=int, default=384)
    parser.add_argument("--fundamental-session-count", type=int, default=72)
    args = parser.parse_args()
    started = time.perf_counter()
    args.output.mkdir(parents=True, exist_ok=True)
    master = json.loads(args.master.read_text(encoding="utf-8"))
    qualification_rows = list(csv.DictReader(args.qualification.open(encoding="utf-8", newline="")))
    frozen_inputs = {
        "master_registry": {"path": str(args.master), "sha256": sha256(args.master)},
        "capability_registry": {"path": str(args.registry), "sha256": sha256(args.registry)},
        "fundamental_qualification": {"path": str(args.qualification), "sha256": sha256(args.qualification)},
        "true1min_release_manifest": {
            "path": str(args.true1min_release_manifest),
            "sha256": sha256(args.true1min_release_manifest),
        },
        "fundamental_manifest": {"path": str(args.fundamental_manifest), "sha256": sha256(args.fundamental_manifest)},
        "split_manifest": {"path": str(args.split_manifest), "sha256": sha256(args.split_manifest)},
        "chip_census_manifest": {
            "path": str(args.chip_census / "run_manifest.json"),
            "sha256": sha256(args.chip_census / "run_manifest.json"),
        },
        "broad_event_pack": {"path": str(args.broad_event_pack), "sha256": sha256(args.broad_event_pack)},
    }
    contract = {
        "contract_id": "CN_FULL_FIELD_INFORMATION_RESEARCH_V1",
        "repo_sha": args.repo_sha,
        "field_authority_count": int(master["field_record_count"]),
        "data_role": "development_only",
        "allowed_inputs": ["field values", "PIT clocks", "coverage", "missingness", "field-to-field redundancy"],
        "forbidden_inputs": ["future return", "label", "reward", "selector", "validation", "holdout", "2026_forward"],
        "raw_fundamental_generator_exposure": "FORBIDDEN_QUALIFIED_CANONICAL_REPRESENTATIONS_ONLY",
        "plate_policy": "NOT_EVALUATED_UNTIL_REAL_PIT_MINUTE_MATERIALIZATION_EXISTS_ON_77O",
        "core_pack_authority": "EXPLORATORY_NON_PERFORMANCE_ONLY_NO_GENERATOR_PROMOTION",
        "broad_event_pack_policy": "FROZEN_UNMODIFIED_SUPPORT_COUNTS_ONLY_PERFORMANCE_FIELDS_IGNORED",
        "frozen_inputs": frozen_inputs,
        "sampling_parameters": {
            "row_groups_per_file": args.row_groups_per_file,
            "true1min_sample_modulus": args.true1min_sample_modulus,
            "true1min_field_batch_size": args.true1min_field_batch_size,
            "fundamental_code_count": args.fundamental_code_count,
            "fundamental_session_count": args.fundamental_session_count,
        },
    }
    atomic_json(args.output / "information_research_contract.json", contract)

    true_metrics, true_pairs, _, true_evidence = collect_true1min(
        args.true1min_release_manifest,
        master["rows"],
        row_groups_per_file=args.row_groups_per_file,
        sample_modulus=args.true1min_sample_modulus,
        field_batch_size=args.true1min_field_batch_size,
    )
    fundamental_metrics, fundamental_pairs, _, fundamental_evidence = collect_fundamental(
        args.fundamental_manifest,
        args.registry,
        args.split_manifest,
        code_count=args.fundamental_code_count,
        session_count=args.fundamental_session_count,
    )
    chip_metrics, chip_pairs, chip_evidence = import_chip(args.chip_census)
    event_metrics, event_evidence = import_broad_event_support(args.broad_event_pack, master["rows"])
    metrics = [*true_metrics, *fundamental_metrics, *chip_metrics, *event_metrics]
    pairs = [*true_pairs, *fundamental_pairs, *chip_pairs]
    core_pack = select_representative_core_pack(metrics, pairs)
    selected = set(core_pack["selected_field_ids"])
    universe = build_universe(master, qualification_rows, metrics)
    for row in universe:
        row["core_pack_selected"] = str(row["field_name"]) in selected and row["record_kind"] == "CAPABILITY_FIELD"

    write_csv(args.output / "field_universe_information.csv", universe)
    write_csv(args.output / "capability_information_metrics.csv", metrics)
    write_csv(args.output / "pairwise_nmi.csv", pairs)
    atomic_json(args.output / "field_universe_information.json", universe)
    atomic_json(args.output / "capability_information_metrics.json", metrics)
    atomic_json(args.output / "pairwise_nmi.json", pairs)
    atomic_json(args.output / "core_pack.json", core_pack)
    evidence = {
        "true1min": true_evidence,
        "fundamental": fundamental_evidence,
        "chip": chip_evidence,
        "broad_event": event_evidence,
        "plate": {
            "status": "NOT_EVALUATED_REAL_PIT_MINUTE_MATERIALIZATION_ABSENT_ON_77O",
            "placeholder_or_current_snapshot_substitution_used": False,
        },
    }
    atomic_json(args.output / "evidence_summary.json", evidence)
    access_ledger = {
        "development_true1min_accessed": True,
        "development_pit_fundamental_accessed": True,
        "development_chip_evidence_imported": True,
        "frozen_broad_event_support_imported": True,
        "broad_event_pack_modified": False,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "reward_or_performance_accessed": False,
        "candidate_promotion": False,
        "generator_authority_changed": False,
    }
    atomic_json(args.output / "access_ledger.json", access_ledger)
    status_counts = Counter(str(row["information_status"]) for row in universe)
    context_counts = Counter(str(row.get("context", "")) for row in metrics)
    coverage_summary = {
        "authoritative_field_record_count": len(universe),
        "source_field_count": sum(row["record_kind"] == "SOURCE_FIELD" for row in universe),
        "capability_field_count": sum(row["record_kind"] == "CAPABILITY_FIELD" for row in universe),
        "plate_membership_metadata_count": sum(row["record_kind"] == "PLATE_MEMBERSHIP_METADATA" for row in universe),
        "materialized_capability_metric_count": len(metrics),
        "information_qualified_capability_count": sum(bool(row.get("information_qualified")) for row in metrics),
        "core_pack_selected_count": len(selected),
        "pairwise_nmi_count": len(pairs),
        "information_status_counts": dict(sorted(status_counts.items())),
        "metric_context_counts": dict(sorted(context_counts.items())),
        "raw_fundamental_qualification_state_counts": dict(
            sorted(Counter(row["qualification_state"] for row in qualification_rows).items())
        ),
        "raw_fundamental_columns_opened_to_generator": 0,
        "plate_capability_status": "NOT_EVALUATED_MATERIALIZATION_ABSENT",
        "performance_claim_allowed": False,
    }
    atomic_json(args.output / "coverage_summary.json", coverage_summary)
    primary_names = {
        "access_ledger.json",
        "capability_information_metrics.csv",
        "capability_information_metrics.json",
        "core_pack.json",
        "coverage_summary.json",
        "evidence_summary.json",
        "field_universe_information.csv",
        "field_universe_information.json",
        "information_research_contract.json",
        "pairwise_nmi.csv",
        "pairwise_nmi.json",
    }
    artifacts = [
        {"path": name, "sha256": sha256(args.output / name), "size": (args.output / name).stat().st_size}
        for name in sorted(primary_names)
    ]
    atomic_json(args.output / "artifact_index.json", {"artifacts": artifacts})
    manifest = {
        "status": "CN_FULL_FIELD_INFORMATION_RESEARCH_V1_COMPLETED",
        "repo_sha": args.repo_sha,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "runtime_seconds": time.perf_counter() - started,
        "authority_field_count": len(universe),
        "capability_metric_count": len(metrics),
        "pairwise_nmi_count": len(pairs),
        "core_pack_selected_count": len(selected),
        "plate_status": "NOT_EVALUATED_REAL_PIT_MINUTE_MATERIALIZATION_ABSENT_ON_77O",
        "fundamental_materialization_error_count": fundamental_evidence["materialization_error_count"],
        "performance_claim_allowed": False,
        "generator_authority_changed": False,
        "reproducibility": "YES_FIXED_INPUTS_AND_DETERMINISTIC_COORDINATES",
        "frozen_input_hashes": {name: item["sha256"] for name, item in frozen_inputs.items()},
        "outputs": artifacts,
    }
    atomic_json(args.output / "run_manifest.json", manifest)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
