"""PIT-safe daily chip-distribution sidecar with sealed-2026 filtering."""

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
from typing import Any, Iterable, Sequence
from zipfile import ZipFile, ZipInfo

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.atomic_checkpoint import atomic_write_json, durable_flush
from our_system_phase2.services.feature_state_fabric import (
    FieldRole,
    FieldSpec,
    MissingPolicy,
    ObservableClock,
)
from our_system_phase2.services.pit_group_release import FORWARD_SEALED_FROM


CHIP_SIDECAR_VERSION = "nextgen_dark_chip_sidecar_v3"
CHIP_FIELDS = {
    "历史最低价": "chip_historical_low",
    "历史最高价": "chip_historical_high",
    "5分位成本": "chip_cost_p05",
    "15分位成本": "chip_cost_p15",
    "50分位成本": "chip_cost_p50",
    "85分位成本": "chip_cost_p85",
    "95分位成本": "chip_cost_p95",
    "加权平均成本": "chip_cost_weighted_mean",
    "胜率": "chip_profit_ratio",
}
_HEADER = ("股票代码", "交易日期", *CHIP_FIELDS.keys())
PROFIT_RATIO_ROUNDING_TOLERANCE = 1.0
MAX_CHIP_MEMBERS = 10_000
MAX_CHIP_MEMBER_BYTES = 5 * 1024 * 1024
MAX_CHIP_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
MAX_CHIP_COMPRESSION_RATIO = 250.0


@dataclass(frozen=True, slots=True)
class ChipSidecarResult:
    manifest_path: Path
    field_registry_path: Path
    shard_paths: tuple[Path, ...]


def load_chip_context(
    root: Path,
    *,
    allowed_codes: set[str],
    fields: Sequence[str],
    maximum_observable_time: str,
    data_role: str = "development",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load only requested PIT chip rows under a sealed-2026 cutoff."""
    root = Path(root)
    requested = tuple(sorted(set(map(str, fields))))
    unknown = sorted(set(requested) - set(CHIP_FIELDS.values()))
    if unknown:
        raise ValueError(f"unknown chip fields: {unknown}")
    maximum = validate_chip_context_role(
        data_role=data_role,
        maximum_observable_time=maximum_observable_time,
    )
    manifest_path = root / "chip_sidecar_manifest_v1.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"chip sidecar manifest missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if bool(manifest.get("forward_2026_performance_accessed")) or bool(
        manifest.get("sealed_2026_values_converted_or_used")
    ):
        raise PermissionError("chip sidecar manifest reports sealed 2026 value access")
    paths = sorted((root / "shards").glob("*.parquet"))
    if len(paths) != int(manifest.get("shard_count") or -1):
        raise RuntimeError("chip sidecar shard closure does not match manifest")
    columns = ["code", "source_session", "source_observed_at", *requested]
    normalized_codes = {_normalize_code(value) for value in allowed_codes}
    parts: list[pd.DataFrame] = []
    for path in paths:
        table = pq.read_table(
            path,
            columns=columns,
            filters=[("code", "in", sorted(normalized_codes))],
        )
        if table.num_rows:
            parts.append(table.to_pandas())
    chip = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=columns)
    if not chip.empty:
        chip["code"] = chip["code"].map(_normalize_code)
        observed = pd.to_datetime(chip["source_observed_at"], errors="raise")
        chip = chip.loc[observed.le(maximum)].copy()
        if pd.to_datetime(chip["source_observed_at"], errors="raise").dt.year.ge(2026).any():
            raise PermissionError("chip sidecar exposes sealed 2026 observation")
    return chip, {
        "manifest_path": str(manifest_path),
        "manifest_sha256": _file_sha256(manifest_path),
        "sidecar_version": str(manifest.get("sidecar_version") or ""),
        "shard_count": len(paths),
        "loaded_row_count": len(chip),
        "maximum_observable_time": maximum_observable_time,
        "data_role": data_role,
    }


def validate_chip_context_role(
    *,
    data_role: str,
    maximum_observable_time: str,
) -> pd.Timestamp:
    """Validate the chip observation boundary before any forward source read."""
    allowed_roles = {
        "development",
        "forward_2026_report_only",
        "historical_challenge_report_only",
    }
    if data_role not in allowed_roles:
        raise PermissionError(f"chip PIT context rejects data role: {data_role}")
    maximum = pd.Timestamp(maximum_observable_time)
    enters_forward = maximum >= FORWARD_SEALED_FROM
    if enters_forward and data_role != "forward_2026_report_only":
        raise PermissionError("chip maximum observable time cannot enter sealed 2026")
    if not enters_forward and data_role == "forward_2026_report_only":
        raise PermissionError("forward chip role requires a 2026 observation boundary")
    return maximum


def _decode(payload: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            return payload.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise ValueError("chip CSV is not UTF-8/GB18030")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_code(value: str) -> str:
    matches = re.findall(r"\d{6}", str(value))
    if len(matches) != 1:
        raise ValueError(f"invalid chip security code: {value!r}")
    return matches[0]


def _validate_chip_frame(frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    quantiles = frame[[
        "chip_cost_p05", "chip_cost_p15", "chip_cost_p50", "chip_cost_p85", "chip_cost_p95",
    ]].to_numpy(dtype=float)
    if (np.diff(quantiles, axis=1) < -1e-9).any():
        raise ValueError("chip cost quantiles are not monotonic")
    if (frame["chip_historical_low"] > frame["chip_historical_high"]).any():
        raise ValueError("chip historical low exceeds high")
    if (~frame["chip_profit_ratio"].between(0.0, 100.0)).any():
        raise ValueError("chip profit ratio must be in [0, 100]")
    if frame.duplicated(["code", "source_session"]).any():
        raise ValueError("duplicate chip code/source_session")


def _parse_members(
    archive: ZipFile,
    infos: Sequence[ZipInfo],
    *,
    minimum: pd.Timestamp,
    cutoff: pd.Timestamp,
) -> tuple[pd.DataFrame, int, int, int, int, float | None]:
    rows: list[dict[str, Any]] = []
    excluded_2026 = 0
    excluded_unobservable = 0
    clipped_profit_ratio_count = 0
    sanitized_nonpositive_historical_low_count = 0
    maximum_raw_profit_ratio: float | None = None
    for info in infos:
        if info.is_dir() or not info.filename.lower().endswith(".csv"):
            continue
        with archive.open(info) as sample_stream:
            sample = sample_stream.readline(64 * 1024 + 1)
        if len(sample) > 64 * 1024:
            raise ValueError(f"chip CSV header exceeds 64 KiB in {info.filename}")
        _, encoding = _decode(sample)
        with archive.open(info) as raw_stream:
            wrapper = io.TextIOWrapper(raw_stream, encoding=encoding, newline="")
            reader = csv.reader(wrapper)
            header = tuple(next(reader, ()))
            if header != _HEADER:
                raise ValueError(f"unexpected chip header in {info.filename}: {header}")
            for raw in reader:
                if not raw:
                    continue
                if len(raw) != len(_HEADER):
                    raise ValueError(f"malformed chip row in {info.filename}")
                source_session = pd.to_datetime(raw[1], format="%Y%m%d")
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
                    "code": _normalize_code(raw[0]),
                    "source_session": source_session,
                    "source_observed_at": source_observed_at,
                }
                for index, canonical in enumerate(CHIP_FIELDS.values(), start=2):
                    row[canonical] = float(raw[index]) if raw[index].strip() else np.nan
                if np.isfinite(row["chip_historical_low"]) and row["chip_historical_low"] <= 0.0:
                    row["chip_historical_low"] = np.nan
                    sanitized_nonpositive_historical_low_count += 1
                raw_profit_ratio = row["chip_profit_ratio"]
                if np.isfinite(raw_profit_ratio):
                    maximum_raw_profit_ratio = (
                        raw_profit_ratio
                        if maximum_raw_profit_ratio is None
                        else max(maximum_raw_profit_ratio, raw_profit_ratio)
                    )
                    if raw_profit_ratio < 0.0 or raw_profit_ratio > 100.0 + PROFIT_RATIO_ROUNDING_TOLERANCE:
                        raise ValueError(
                            "chip profit ratio exceeds auditable rounding tolerance: "
                            f"{raw_profit_ratio}"
                        )
                    if raw_profit_ratio > 100.0:
                        row["chip_profit_ratio"] = 100.0
                        clipped_profit_ratio_count += 1
                rows.append(row)
    frame = pd.DataFrame(rows, columns=["code", "source_session", "source_observed_at", *CHIP_FIELDS.values()])
    _validate_chip_frame(frame)
    return (
        frame.sort_values(["code", "source_session"], kind="mergesort").reset_index(drop=True),
        excluded_2026,
        excluded_unobservable,
        clipped_profit_ratio_count,
        sanitized_nonpositive_historical_low_count,
        maximum_raw_profit_ratio,
    )


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


def _build_chip_shard_job(
    arguments: tuple[Path, Path, tuple[str, ...], str, int, str, str],
) -> tuple[Path, dict[str, Any]]:
    source, output, member_names, identity, shard_number, minimum_text, cutoff_text = arguments
    shard_path = output / "shards" / f"chip_{shard_number:05d}_{identity[:12]}.parquet"
    meta_path = output / "shard_manifests" / f"chip_{shard_number:05d}_{identity[:12]}.json"
    if shard_path.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("input_identity") == identity and meta.get("output_sha256") == _file_sha256(shard_path):
            if "sanitized_nonpositive_historical_low_count" not in meta:
                frame = pd.read_parquet(shard_path)
                invalid_low = frame["chip_historical_low"].le(0.0)
                sanitized_count = int(invalid_low.sum())
                if sanitized_count:
                    frame.loc[invalid_low, "chip_historical_low"] = np.nan
                    _validate_chip_frame(frame)
                    _atomic_parquet(frame, shard_path)
                meta["sanitized_nonpositive_historical_low_count"] = sanitized_count
                meta["sidecar_version"] = CHIP_SIDECAR_VERSION
                meta["output_sha256"] = _file_sha256(shard_path)
                atomic_write_json(meta_path, meta)
            return shard_path, meta
    with ZipFile(source) as archive:
        infos = [archive.getinfo(name) for name in member_names]
        (
            frame,
            excluded_2026,
            excluded_unobservable,
            clipped_profit_ratio_count,
            sanitized_nonpositive_historical_low_count,
            maximum_raw_profit_ratio,
        ) = _parse_members(
            archive,
            infos,
            minimum=pd.Timestamp(minimum_text),
            cutoff=pd.Timestamp(cutoff_text),
        )
    _atomic_parquet(frame, shard_path)
    meta = {
        "input_identity": identity,
        "member_count": len(member_names),
        "member_names": list(member_names),
        "row_count": len(frame),
        "excluded_2026_row_count": excluded_2026,
        "excluded_unobservable_row_count": excluded_unobservable,
        "clipped_profit_ratio_count": clipped_profit_ratio_count,
        "sanitized_nonpositive_historical_low_count": (
            sanitized_nonpositive_historical_low_count
        ),
        "maximum_raw_profit_ratio": maximum_raw_profit_ratio,
        "minimum_source_session": frame["source_session"].min().isoformat() if len(frame) else None,
        "maximum_source_session": frame["source_session"].max().isoformat() if len(frame) else None,
        "output_path": str(shard_path),
        "output_sha256": _file_sha256(shard_path),
    }
    atomic_write_json(meta_path, meta)
    return shard_path, meta


def chip_field_specs() -> tuple[FieldSpec, ...]:
    specs: list[FieldSpec] = []
    for name in CHIP_FIELDS.values():
        if name in {"chip_historical_low", "chip_historical_high"}:
            role = FieldRole.BENCHMARK_ONLY
        elif name == "chip_profit_ratio":
            role = FieldRole.CONDITION_ONLY
        else:
            role = FieldRole.INTERACTION_ONLY
        specs.append(
            FieldSpec(
                name=name,
                dtype="float64",
                family="chip_distribution",
                role=role,
                observable_clock=ObservableClock.PREVIOUS_SESSION,
                maturity=1,
                maturity_unit="sessions",
                missing_policy=MissingPolicy.PROPAGATE,
                source_fields=(name,),
                transform="identity",
                source_lag=1,
                source_lag_unit="sessions",
                source_session_field="chip_source_session",
            )
        )
    return tuple(specs)


def _central_directory_hash(infos: Iterable[ZipInfo]) -> str:
    rows = [
        {"name": info.filename, "size": info.file_size, "compressed": info.compress_size, "crc": info.CRC}
        for info in infos
    ]
    return hashlib.sha256(
        json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def build_chip_sidecar(
    archive_path: str | Path,
    output_root: str | Path,
    *,
    minimum_source_date: str = "2023-01-01",
    cutoff: str = "2025-12-31",
    members_per_shard: int = 128,
    workers: int = 1,
) -> ChipSidecarResult:
    if members_per_shard <= 0:
        raise ValueError("members_per_shard must be positive")
    if workers <= 0:
        raise ValueError("chip workers must be positive")
    minimum = pd.Timestamp(minimum_source_date)
    cutoff_time = pd.Timestamp(cutoff)
    if cutoff_time >= FORWARD_SEALED_FROM:
        raise ValueError("chip cutoff cannot enter sealed 2026")
    source = Path(archive_path)
    output = Path(output_root)
    with ZipFile(source) as archive:
        infos = sorted(
            [item for item in archive.infolist() if not item.is_dir() and item.filename.lower().endswith(".csv")],
            key=lambda item: item.filename,
        )
        if len(infos) > MAX_CHIP_MEMBERS:
            raise ValueError("chip archive member limit exceeded")
        if any(info.file_size > MAX_CHIP_MEMBER_BYTES for info in infos):
            raise ValueError("chip archive member size limit exceeded")
        total_bytes = sum(info.file_size for info in infos)
        if total_bytes > MAX_CHIP_TOTAL_BYTES:
            raise ValueError("chip archive expansion limit exceeded")
        compressed = sum(max(1, info.compress_size) for info in infos)
        if total_bytes / max(1, compressed) > MAX_CHIP_COMPRESSION_RATIO:
            raise ValueError("chip archive compression ratio limit exceeded")
        jobs: list[tuple[Path, Path, tuple[str, ...], str, int, str, str]] = []
        for batch_index in range(0, len(infos), members_per_shard):
            batch = infos[batch_index : batch_index + members_per_shard]
            identity = hashlib.sha256(
                json.dumps(
                    [(item.filename, item.file_size, item.CRC) for item in batch],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            jobs.append(
                (
                    source,
                    output,
                    tuple(item.filename for item in batch),
                    identity,
                    batch_index // members_per_shard,
                    minimum.isoformat(),
                    cutoff_time.isoformat(),
                )
            )
        directory_hash = _central_directory_hash(infos)

    if workers == 1:
        built = [_build_chip_shard_job(job) for job in jobs]
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            built = list(executor.map(_build_chip_shard_job, jobs))
    shard_paths = [item[0] for item in built]
    shard_manifests = [item[1] for item in built]

    registry_path = output / "chip_field_registry_v1.json"
    atomic_write_json(
        registry_path,
        {
            "registry_version": "nextgen_dark_chip_field_registry_v1",
            "fields": [spec.canonical() for spec in chip_field_specs()],
            "formal_performance_search_allowed": False,
        },
    )
    manifest = {
        "sidecar_version": CHIP_SIDECAR_VERSION,
        "source_archive": str(source),
        "source_archive_size": source.stat().st_size,
        "source_archive_sha256": _file_sha256(source),
        "source_central_directory_sha256": directory_hash,
        "minimum_source_date": minimum.isoformat(),
        "cutoff": cutoff_time.isoformat(),
        "member_count": sum(item["member_count"] for item in shard_manifests),
        "shard_count": len(shard_manifests),
        "row_count": sum(item["row_count"] for item in shard_manifests),
        "excluded_2026_row_count": sum(item["excluded_2026_row_count"] for item in shard_manifests),
        "excluded_unobservable_row_count": sum(
            item["excluded_unobservable_row_count"] for item in shard_manifests
        ),
        "clipped_profit_ratio_count": sum(
            item["clipped_profit_ratio_count"] for item in shard_manifests
        ),
        "sanitized_nonpositive_historical_low_count": sum(
            item["sanitized_nonpositive_historical_low_count"] for item in shard_manifests
        ),
        "maximum_raw_profit_ratio": max(
            (
                item["maximum_raw_profit_ratio"]
                for item in shard_manifests
                if item["maximum_raw_profit_ratio"] is not None
            ),
            default=None,
        ),
        "profit_ratio_rounding_policy": {
            "allowed_raw_range": [0.0, 100.0 + PROFIT_RATIO_ROUNDING_TOLERANCE],
            "clipped_output_range": [0.0, 100.0],
            "tolerance": PROFIT_RATIO_ROUNDING_TOLERANCE,
        },
        "historical_low_missing_policy": {
            "nonpositive_source_values": "MASK_TO_MISSING",
            "reason": "zero/nonpositive historical low is an invalid price sentinel",
        },
        "minimum_source_session": min(
            (item["minimum_source_session"] for item in shard_manifests if item["minimum_source_session"]),
            default=None,
        ),
        "maximum_source_session": max(
            (item["maximum_source_session"] for item in shard_manifests if item["maximum_source_session"]),
            default=None,
        ),
        "shards": shard_manifests,
        "field_registry": str(registry_path),
        "reward_or_performance_used": False,
        "forward_2026_performance_accessed": False,
        "sealed_2026_rows_scanned_for_date_filter": sum(
            item["excluded_2026_row_count"] for item in shard_manifests
        ),
        "sealed_2026_values_converted_or_used": False,
        "source_observed_policy": "CONSERVATIVE_ASSUMPTION_NEXT_CALENDAR_DAY_00_NOT_VENDOR_TIMESTAMP",
        "reproducibility": "YES_WITH_PATH_INDEPENDENT_CONTENT_HASHES",
    }
    manifest_path = output / "chip_sidecar_manifest_v1.json"
    atomic_write_json(manifest_path, manifest)
    return ChipSidecarResult(manifest_path, registry_path, tuple(shard_paths))


def point_in_time_chip_context(
    bars: pd.DataFrame,
    chip: pd.DataFrame,
    *,
    fields: Sequence[str],
    data_role: str,
) -> pd.DataFrame:
    required_bars = {"code", "trade_time"}
    required_chip = {"code", "source_session", *fields}
    if not required_bars <= set(bars.columns):
        raise ValueError(f"bars missing fields: {sorted(required_bars - set(bars.columns))}")
    if not required_chip <= set(chip.columns):
        raise ValueError(f"chip sidecar missing fields: {sorted(required_chip - set(chip.columns))}")
    left = bars.copy()
    left["_original_code"] = left["code"].astype(str)
    left["code"] = left["code"].map(_normalize_code)
    left["trade_time"] = pd.to_datetime(left["trade_time"], errors="coerce", format="mixed")
    if left["trade_time"].isna().any():
        raise ValueError("chip PIT context cannot use invalid trade_time")
    validate_chip_context_role(
        data_role=data_role,
        maximum_observable_time=str(left["trade_time"].max()),
    )
    if (
        data_role == "forward_2026_report_only"
        and left["trade_time"].lt(FORWARD_SEALED_FROM).any()
    ):
        raise ValueError("forward chip role cannot mix pre-2026 trade_time")
    left["_exec_session"] = left["trade_time"].dt.normalize()
    left["_row_id"] = np.arange(len(left))
    right = chip[["code", "source_session", *fields]].copy()
    right["code"] = right["code"].map(_normalize_code)
    right["source_session"] = pd.to_datetime(
        right["source_session"], errors="coerce", format="mixed"
    ).dt.normalize()
    if right["source_session"].ge(FORWARD_SEALED_FROM).any():
        raise PermissionError("chip PIT context exposes sealed 2026 source session")
    pieces: list[pd.DataFrame] = []
    for code, rows in left.groupby("code", sort=False):
        source = right[right["code"].eq(code)].sort_values("source_session")
        local = rows.sort_values("_exec_session")
        if source.empty:
            merged = local.copy()
            merged["source_session"] = pd.NaT
            for field in fields:
                merged[field] = np.nan
        else:
            merged = pd.merge_asof(
                local,
                source.drop(columns="code"),
                left_on="_exec_session",
                right_on="source_session",
                direction="backward",
                allow_exact_matches=False,
            )
            merged["code"] = code
        pieces.append(merged)
    joined = pd.concat(pieces, ignore_index=True).sort_values("_row_id", kind="mergesort")
    joined = joined.rename(columns={"source_session": "chip_source_session"})
    joined["code"] = joined["_original_code"]
    return joined.drop(columns=["_exec_session", "_row_id", "_original_code"]).reset_index(drop=True)
