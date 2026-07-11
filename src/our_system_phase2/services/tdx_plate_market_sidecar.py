"""PIT-safe daily TDX plate market context isolated from formal search."""

from __future__ import annotations

import csv
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping, Sequence
from zipfile import ZipFile, ZipInfo

import numpy as np
import pandas as pd

from our_system_phase2.services.atomic_checkpoint import atomic_write_json, durable_flush
from our_system_phase2.services.feature_state_fabric import (
    FieldRole,
    FieldSpec,
    MissingPolicy,
    ObservableClock,
)
from our_system_phase2.services.pit_group_release import FORWARD_SEALED_FROM


TDX_PLATE_MARKET_VERSION = "nextgen_dark_tdx_plate_market_v1"
PLATE_MARKET_FIELDS = {
    "开盘": "plate_open",
    "收盘": "plate_close",
    "最高": "plate_high",
    "最低": "plate_low",
    "成交量": "plate_volume",
    "成交额": "plate_turnover",
    "上涨家数": "plate_up_count",
    "下跌家数": "plate_down_count",
}
_HEADER = ("日期", "代码", "名称", *PLATE_MARKET_FIELDS.keys())
_CATEGORIES = ("region", "concept", "industry", "style")
_CATEGORY_TOKENS = {
    "地区板块": "region",
    "概念板块": "concept",
    "行业板块": "industry",
    "风格板块": "style",
}
MAX_MARKET_MEMBERS = 2_000
MAX_MARKET_MEMBER_BYTES = 10 * 1024 * 1024
MAX_MARKET_TOTAL_BYTES = 1024 * 1024 * 1024
MAX_MARKET_COMPRESSION_RATIO = 250.0


@dataclass(frozen=True, slots=True)
class TDXPlateMarketResult:
    manifest_path: Path
    field_registry_path: Path
    shard_paths: tuple[Path, ...]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_group_id(value: Any) -> str:
    matches = re.findall(r"\d{6}", str(value))
    if len(matches) != 1:
        raise ValueError(f"invalid TDX plate code: {value!r}")
    return f"{matches[0]}.TDX"


def discover_tdx_plate_market_archives(root: str | Path) -> dict[str, Path]:
    discovered: dict[str, Path] = {}
    for path in sorted(Path(root).rglob("*.zip")):
        if "日k_K线" not in path.name:
            continue
        category = next(
            (value for token, value in _CATEGORY_TOKENS.items() if token in str(path)),
            None,
        )
        if category is None:
            continue
        if category in discovered:
            raise ValueError(f"duplicate TDX {category} daily market archive")
        discovered[category] = path
    missing = sorted(set(_CATEGORIES) - set(discovered))
    if missing:
        raise ValueError(f"missing TDX daily market archives: {missing}")
    return {category: discovered[category] for category in _CATEGORIES}


def _validate_archive(archive: ZipFile, path: Path) -> list[ZipInfo]:
    infos = sorted(
        [info for info in archive.infolist() if not info.is_dir() and info.filename.lower().endswith(".csv")],
        key=lambda info: info.filename,
    )
    if not infos:
        raise ValueError(f"TDX plate market archive has no CSV members: {path}")
    if len(infos) > MAX_MARKET_MEMBERS:
        raise ValueError(f"TDX plate market member limit exceeded: {path}")
    if any(info.file_size > MAX_MARKET_MEMBER_BYTES for info in infos):
        raise ValueError(f"TDX plate market member size limit exceeded: {path}")
    total_bytes = sum(info.file_size for info in infos)
    if total_bytes > MAX_MARKET_TOTAL_BYTES:
        raise ValueError(f"TDX plate market expansion limit exceeded: {path}")
    compressed = sum(max(1, info.compress_size) for info in infos)
    if total_bytes / max(1, compressed) > MAX_MARKET_COMPRESSION_RATIO:
        raise ValueError(f"TDX plate market compression ratio limit exceeded: {path}")
    return infos


def _member_encoding(archive: ZipFile, info: ZipInfo) -> str:
    with archive.open(info) as stream:
        header = stream.readline(64 * 1024 + 1)
    if len(header) > 64 * 1024:
        raise ValueError(f"TDX plate market header exceeds 64 KiB: {info.filename}")
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            header.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    raise ValueError(f"TDX plate market header is not UTF-8/GB18030: {info.filename}")


def _float(value: str) -> float:
    return float(value) if value.strip() else np.nan


def _group_span_stats(frame: pd.DataFrame) -> dict[str, Any]:
    spans = frame.groupby("group_id", sort=False)["source_session"].agg(["min", "max"])
    spanning = spans["min"].le(pd.Timestamp("2024-01-01")) & spans["max"].ge(
        pd.Timestamp("2025-12-30")
    )
    return {
        "group_ids": sorted(frame["group_id"].unique().tolist()),
        "groups_spanning_2024_2025": int(spanning.sum()),
        "all_groups_span_2024_2025": bool(spanning.all()),
        "group_span_coverage_ratio": round(float(spanning.mean()), 8),
    }


def _parse_archive(
    path: Path,
    category: str,
    *,
    minimum: pd.Timestamp,
    cutoff: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    excluded_2026 = 0
    excluded_unobservable = 0
    member_count = 0
    with ZipFile(path) as archive:
        infos = _validate_archive(archive, path)
        member_count = len(infos)
        for info in infos:
            encoding = _member_encoding(archive, info)
            with archive.open(info) as raw_stream:
                wrapper = io.TextIOWrapper(raw_stream, encoding=encoding, newline="")
                reader = csv.reader(wrapper)
                header = tuple(next(reader, ()))
                if header != _HEADER:
                    raise ValueError(f"unexpected TDX plate market header in {path}!{info.filename}")
                for raw in reader:
                    if not raw:
                        continue
                    if len(raw) != len(_HEADER):
                        raise ValueError(f"malformed TDX plate market row in {path}!{info.filename}")
                    source_session = pd.to_datetime(raw[0], format="%Y-%m-%d")
                    if source_session >= FORWARD_SEALED_FROM:
                        excluded_2026 += 1
                        continue
                    if source_session < minimum or source_session > cutoff:
                        continue
                    source_observed_at = source_session + pd.Timedelta(days=1)
                    if source_observed_at >= FORWARD_SEALED_FROM:
                        excluded_unobservable += 1
                        continue
                    row: dict[str, Any] = {
                        "group_id": _normalize_group_id(raw[1]),
                        "group_name": raw[2].strip(),
                        "group_category": category,
                        "source_session": source_session,
                        "source_observed_at": source_observed_at,
                    }
                    for index, canonical in enumerate(PLATE_MARKET_FIELDS.values(), start=3):
                        row[canonical] = _float(raw[index])
                    rows.append(row)
    columns = [
        "group_id",
        "group_name",
        "group_category",
        "source_session",
        "source_observed_at",
        *PLATE_MARKET_FIELDS.values(),
    ]
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        raise ValueError(f"TDX plate market archive has no safe rows: {path}")
    if frame.duplicated(["group_id", "source_session"]).any():
        raise ValueError(f"duplicate TDX plate market group/session: {path}")
    if frame["group_name"].eq("").any():
        raise ValueError(f"empty TDX plate market group name: {path}")
    frame = frame.sort_values(["group_id", "source_session"], kind="mergesort").reset_index(drop=True)
    metadata = {
        "category": category,
        "member_count": member_count,
        "row_count": len(frame),
        "excluded_2026_row_count": excluded_2026,
        "excluded_unobservable_row_count": excluded_unobservable,
        "minimum_source_session": frame["source_session"].min().isoformat(),
        "maximum_source_session": frame["source_session"].max().isoformat(),
        "distinct_group_count": int(frame["group_id"].nunique()),
    }
    metadata.update(_group_span_stats(frame))
    return frame, metadata


def _atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(name)
    try:
        frame.to_parquet(temporary, index=False)
        with temporary.open("r+b") as handle:
            durable_flush(handle)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _build_category_job(
    arguments: tuple[Path, Path, str, str, str, str],
) -> tuple[Path, dict[str, Any]]:
    source, output, category, source_sha256, minimum_text, cutoff_text = arguments
    identity = hashlib.sha256(
        json.dumps(
            {
                "version": TDX_PLATE_MARKET_VERSION,
                "category": category,
                "source_sha256": source_sha256,
                "minimum": minimum_text,
                "cutoff": cutoff_text,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    shard_path = output / "shards" / f"plate_market_{category}_{identity[:12]}.parquet"
    meta_path = output / "shard_manifests" / f"plate_market_{category}_{identity[:12]}.json"
    if shard_path.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("input_identity") == identity and meta.get("output_sha256") == _file_sha256(shard_path):
            if "groups_spanning_2024_2025" not in meta or "group_ids" not in meta:
                meta.update(_group_span_stats(pd.read_parquet(shard_path)))
                atomic_write_json(meta_path, meta)
            return shard_path, meta
    frame, meta = _parse_archive(
        source,
        category,
        minimum=pd.Timestamp(minimum_text),
        cutoff=pd.Timestamp(cutoff_text),
    )
    _atomic_parquet(frame, shard_path)
    meta.update(
        {
            "input_identity": identity,
            "source_path": str(source),
            "source_sha256": source_sha256,
            "output_path": str(shard_path),
            "output_sha256": _file_sha256(shard_path),
        }
    )
    atomic_write_json(meta_path, meta)
    return shard_path, meta


def plate_market_field_specs() -> tuple[FieldSpec, ...]:
    specs: list[FieldSpec] = []
    for name in PLATE_MARKET_FIELDS.values():
        role = (
            FieldRole.CONDITION_ONLY
            if name in {"plate_up_count", "plate_down_count"}
            else FieldRole.BENCHMARK_ONLY
        )
        specs.append(
            FieldSpec(
                name=name,
                dtype="float64",
                family="plate_market_context",
                role=role,
                observable_clock=ObservableClock.PREVIOUS_SESSION,
                maturity=1,
                maturity_unit="sessions",
                missing_policy=MissingPolicy.PROPAGATE,
                source_fields=(name,),
                transform="identity",
                source_lag=1,
                source_lag_unit="sessions",
                source_session_field="plate_market_source_session",
            )
        )
    return tuple(specs)


def build_tdx_plate_market_sidecar(
    archives: Mapping[str, str | Path],
    output_root: str | Path,
    *,
    minimum_source_date: str = "2005-01-01",
    cutoff: str = "2025-12-31",
    workers: int = 1,
) -> TDXPlateMarketResult:
    if not archives:
        raise ValueError("at least one TDX plate market archive is required")
    unknown = sorted(set(archives) - set(_CATEGORIES))
    if unknown:
        raise ValueError(f"unknown TDX plate market categories: {unknown}")
    if workers <= 0:
        raise ValueError("TDX plate market workers must be positive")
    minimum = pd.Timestamp(minimum_source_date)
    cutoff_time = pd.Timestamp(cutoff)
    if cutoff_time >= FORWARD_SEALED_FROM:
        raise ValueError("TDX plate market cutoff cannot enter sealed 2026")
    output = Path(output_root)
    source_rows = []
    jobs = []
    for category in _CATEGORIES:
        if category not in archives:
            continue
        path = Path(archives[category])
        source_sha256 = _file_sha256(path)
        source_rows.append(
            {
                "category": category,
                "path": str(path),
                "size": path.stat().st_size,
                "sha256": source_sha256,
            }
        )
        jobs.append(
            (
                path,
                output,
                category,
                source_sha256,
                minimum.isoformat(),
                cutoff_time.isoformat(),
            )
        )
    if workers == 1:
        built = [_build_category_job(job) for job in jobs]
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            built = list(executor.map(_build_category_job, jobs))
    shard_paths = [item[0] for item in built]
    shard_manifests = [item[1] for item in built]
    category_by_group: dict[str, str] = {}
    for item in shard_manifests:
        for group_id in item["group_ids"]:
            if group_id in category_by_group:
                raise ValueError(
                    "TDX plate market group appears in multiple categories: "
                    f"{group_id} ({category_by_group[group_id]}, {item['category']})"
                )
            category_by_group[group_id] = item["category"]
    registry_path = output / "plate_market_field_registry_v1.json"
    atomic_write_json(
        registry_path,
        {
            "registry_version": "nextgen_dark_plate_market_field_registry_v1",
            "fields": [spec.canonical() for spec in plate_market_field_specs()],
            "formal_performance_search_allowed": False,
        },
    )
    coverage = {
        item["category"]: {
            "minimum_source_session": item["minimum_source_session"],
            "maximum_source_session": item["maximum_source_session"],
            "archive_date_span_covers_2024_2025": (
                item["minimum_source_session"] <= "2024-01-01T00:00:00"
                and item["maximum_source_session"] >= "2025-12-30T00:00:00"
            ),
            "distinct_group_count": item["distinct_group_count"],
            "groups_spanning_2024_2025": item["groups_spanning_2024_2025"],
            "all_groups_span_2024_2025": item["all_groups_span_2024_2025"],
            "group_span_coverage_ratio": item["group_span_coverage_ratio"],
        }
        for item in shard_manifests
    }
    manifest = {
        "sidecar_version": TDX_PLATE_MARKET_VERSION,
        "archives": source_rows,
        "minimum_source_date": minimum.isoformat(),
        "cutoff": cutoff_time.isoformat(),
        "category_count": len(shard_manifests),
        "distinct_group_count": len(category_by_group),
        "row_count": sum(item["row_count"] for item in shard_manifests),
        "excluded_2026_row_count": sum(item["excluded_2026_row_count"] for item in shard_manifests),
        "excluded_unobservable_row_count": sum(
            item["excluded_unobservable_row_count"] for item in shard_manifests
        ),
        "coverage_by_category": coverage,
        "all_archives_span_2024_2025": all(
            item["archive_date_span_covers_2024_2025"] for item in coverage.values()
        ),
        "all_groups_span_2024_2025": all(
            item["all_groups_span_2024_2025"] for item in coverage.values()
        ),
        "shards": shard_manifests,
        "field_registry": str(registry_path),
        "capability_scope": "BOARD_MARKET_CONTEXT_ONLY_NOT_STOCK_MEMBERSHIP_HISTORY",
        "extends_stock_membership_history": False,
        "stock_membership_survivorship_evidence": False,
        "reward_or_performance_used": False,
        "formal_performance_search_allowed": False,
        "forward_2026_performance_accessed": False,
        "sealed_2026_rows_scanned_for_date_filter": sum(
            item["excluded_2026_row_count"] for item in shard_manifests
        ),
        "sealed_2026_values_converted_or_used": False,
        "source_observed_policy": "CONSERVATIVE_ASSUMPTION_NEXT_CALENDAR_DAY_00_NOT_VENDOR_TIMESTAMP",
        "reproducibility": "YES_WITH_PATH_INDEPENDENT_CONTENT_HASHES",
    }
    manifest_path = output / "plate_market_sidecar_manifest_v1.json"
    atomic_write_json(manifest_path, manifest)
    return TDXPlateMarketResult(manifest_path, registry_path, tuple(shard_paths))


def plate_market_linkage_report(
    membership: pd.DataFrame,
    market: pd.DataFrame,
) -> dict[str, Any]:
    if "group_id" not in membership.columns or "group_id" not in market.columns:
        raise ValueError("membership and market frames both require group_id")
    membership_groups = {_normalize_group_id(value) for value in membership["group_id"].dropna()}
    market_groups = {_normalize_group_id(value) for value in market["group_id"].dropna()}
    shared = membership_groups & market_groups
    return {
        "validation_version": "nextgen_dark_plate_market_linkage_v1",
        "membership_group_count": len(membership_groups),
        "market_group_count": len(market_groups),
        "shared_group_count": len(shared),
        "membership_group_coverage_ratio": (
            round(len(shared) / len(membership_groups), 8) if membership_groups else 0.0
        ),
        "membership_without_market": sorted(membership_groups - market_groups),
        "market_without_membership": sorted(market_groups - membership_groups),
        "reward_or_performance_used": False,
        "forward_2026_performance_accessed": False,
    }


def point_in_time_plate_market_context(
    rows: pd.DataFrame,
    market: pd.DataFrame,
    *,
    fields: Sequence[str],
    data_role: str,
) -> pd.DataFrame:
    if data_role != "development":
        raise PermissionError("TDX plate market context is development-only")
    required_rows = {"group_id", "trade_time"}
    required_market = {"group_id", "source_session", "source_observed_at", *fields}
    if not required_rows <= set(rows.columns):
        raise ValueError(f"rows missing fields: {sorted(required_rows - set(rows.columns))}")
    if not required_market <= set(market.columns):
        raise ValueError(f"plate market sidecar missing fields: {sorted(required_market - set(market.columns))}")
    left = rows.copy()
    left["group_id"] = left["group_id"].map(_normalize_group_id)
    left["trade_time"] = pd.to_datetime(
        left["trade_time"], errors="coerce", format="mixed"
    ).astype("datetime64[ns]")
    if left["trade_time"].isna().any() or left["trade_time"].ge(FORWARD_SEALED_FROM).any():
        raise ValueError("TDX plate market context cannot use invalid or sealed 2026 trade_time")
    left["_exec_session"] = left["trade_time"].dt.normalize().astype("datetime64[ns]")
    left["_row_id"] = np.arange(len(left))
    right = market[["group_id", "source_session", "source_observed_at", *fields]].copy()
    right["group_id"] = right["group_id"].map(_normalize_group_id)
    right["source_session"] = pd.to_datetime(
        right["source_session"], errors="coerce", format="mixed"
    ).dt.normalize().astype("datetime64[ns]")
    right["source_observed_at"] = pd.to_datetime(
        right["source_observed_at"], errors="coerce", format="mixed"
    ).astype("datetime64[ns]")
    if right[["source_session", "source_observed_at"]].isna().any().any():
        raise ValueError("plate market source clocks must be valid timestamps")
    right = right.rename(
        columns={
            "source_session": "_market_source_session",
            "source_observed_at": "_market_source_observed_at",
        }
    )
    market_by_group = {
        group_id: group_rows.sort_values("_market_source_observed_at")
        for group_id, group_rows in right.groupby("group_id", sort=False)
    }
    pieces: list[pd.DataFrame] = []
    for group_id, local_rows in left.groupby("group_id", sort=False):
        source = market_by_group.get(group_id)
        local = local_rows.sort_values("trade_time")
        if source is None:
            merged = local.copy()
            merged["_market_source_session"] = pd.NaT
            merged["_market_source_observed_at"] = pd.NaT
            for field in fields:
                merged[field] = np.nan
        else:
            merged = pd.merge_asof(
                local,
                source.drop(columns="group_id"),
                left_on="trade_time",
                right_on="_market_source_observed_at",
                direction="backward",
                allow_exact_matches=True,
            )
            merged["group_id"] = group_id
            invalid_lag = merged["_market_source_session"].notna() & merged[
                "_market_source_session"
            ].ge(merged["_exec_session"])
            if invalid_lag.any():
                merged.loc[
                    invalid_lag,
                    ["_market_source_session", "_market_source_observed_at", *fields],
                ] = np.nan
        pieces.append(merged)
    joined = pd.concat(pieces, ignore_index=True).sort_values("_row_id", kind="mergesort")
    joined = joined.rename(
        columns={
            "_market_source_session": "plate_market_source_session",
            "_market_source_observed_at": "plate_market_source_observed_at",
        }
    )
    return joined.drop(columns=["_exec_session", "_row_id"]).reset_index(drop=True)
