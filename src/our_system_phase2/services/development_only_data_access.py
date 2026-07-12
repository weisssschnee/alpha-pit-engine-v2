"""Fail-closed access to a physically isolated development-only true1min release."""

from __future__ import annotations

from dataclasses import dataclass
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from our_system_phase2.services.atomic_checkpoint import atomic_write_json


RELEASE_MANIFEST_VERSION = "cn_true1min_development_only_release_v1"
PANEL_RELATIVE_PATH = Path(
    "phase3aq_wide_true1min/canary/phase3aq_true_1min_formula_canary.parquet"
)
DEVELOPMENT_ROLES = frozenset({"train", "development"})
DERIVED_PIT_COLUMNS = frozenset({"ctx_source_session_upper_bound"})
FORBIDDEN_ROLES = frozenset({"validation", "holdout", "spent", "sealed", "forward"})


def sha256_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def schema_hash(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def release_hash(manifest: Mapping[str, Any]) -> str:
    payload = dict(manifest)
    payload.pop("release_hash", None)
    return canonical_json_hash(payload)


def split_roles(path: Path) -> dict[pd.Timestamp, str]:
    frame = pd.read_csv(path)
    required = {"trade_date", "split"}
    if not required.issubset(frame.columns):
        raise ValueError(f"split manifest missing columns: {sorted(required - set(frame.columns))}")
    dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.normalize()
    if dates.duplicated().any():
        raise ValueError("split manifest contains duplicate trade_date values")
    return {date: str(role).strip().lower() for date, role in zip(dates, frame["split"], strict=True)}


def _metadata_date_range(parquet: pq.ParquetFile, row_group_id: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    try:
        column_index = parquet.schema_arrow.names.index("trade_time")
    except ValueError as exc:
        raise ValueError("Parquet file lacks trade_time") from exc
    column = parquet.metadata.row_group(row_group_id).column(column_index)
    statistics = column.statistics
    if statistics is None or not statistics.has_min_max:
        raise ValueError(f"row group {row_group_id} lacks trade_time min/max metadata")
    minimum = pd.Timestamp(statistics.min).normalize()
    maximum = pd.Timestamp(statistics.max).normalize()
    if pd.isna(minimum) or pd.isna(maximum):
        raise ValueError(f"row group {row_group_id} has unknown trade_time range")
    return minimum, maximum


def _assert_development_range(
    minimum: pd.Timestamp,
    maximum: pd.Timestamp,
    roles: Mapping[pd.Timestamp, str],
) -> None:
    if minimum > maximum:
        raise ValueError(f"invalid row-group date range: {minimum.date()}..{maximum.date()}")
    if minimum not in roles or maximum not in roles:
        raise ValueError(
            f"row-group date endpoints are absent from split manifest: {minimum.date()}..{maximum.date()}"
        )
    covered = [role for date, role in roles.items() if minimum <= date <= maximum]
    if not covered:
        raise ValueError(f"row-group date range is absent from split manifest: {minimum.date()}..{maximum.date()}")
    forbidden = sorted({role for role in covered if role not in DEVELOPMENT_ROLES})
    if forbidden:
        raise PermissionError(
            f"row group crosses forbidden data roles {forbidden}: {minimum.date()}..{maximum.date()}"
        )


@dataclass(frozen=True, slots=True)
class ValidatedReleaseFile:
    path: Path
    relative_path: str
    sha256: str
    size: int
    mtime_ns: int
    row_groups: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class ValidatedDevelopmentRelease:
    root: Path
    manifest_path: Path
    manifest: dict[str, Any]
    release_hash: str
    split_manifest_sha256: str
    files: tuple[ValidatedReleaseFile, ...]


def validate_development_release(
    root: Path,
    manifest_path: Path,
    split_manifest: Path,
    *,
    expected_release_hash: str | None = None,
) -> ValidatedDevelopmentRelease:
    """Validate every guard before any Parquet data page is decoded."""

    root = root.resolve()
    manifest_path = manifest_path.resolve()
    split_manifest = split_manifest.resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("manifest_version") != RELEASE_MANIFEST_VERSION:
        raise ValueError("unapproved development-only release manifest version")
    if manifest.get("data_role") != "development":
        raise PermissionError("release is not assigned to the development role")
    if Path(manifest.get("release_root", "")).resolve() != root:
        raise PermissionError("loader received a root different from the approved release root")
    computed_release_hash = release_hash(manifest)
    if manifest.get("release_hash") != computed_release_hash:
        raise ValueError("release manifest self-hash mismatch")
    if expected_release_hash and computed_release_hash != expected_release_hash:
        raise ValueError("release hash does not match the frozen CANARY contract")
    split_hash = sha256_file(split_manifest)
    if manifest.get("split_manifest_sha256") != split_hash:
        raise ValueError("split manifest hash mismatch")

    roles = split_roles(split_manifest)
    file_rows = list(manifest.get("files", []))
    if not file_rows:
        raise ValueError("development-only release manifest contains no files")
    declared = {str(row["relative_path"]).replace("\\", "/") for row in file_rows}
    actual = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*.parquet")
        if path.is_file()
    }
    if actual != declared:
        raise PermissionError(
            f"mixed or incomplete release root: extra={sorted(actual - declared)}, missing={sorted(declared - actual)}"
        )

    # File paths and declared roles are checked before any Parquet file is opened.
    for row in file_rows:
        if row.get("data_role") != "development":
            raise PermissionError(f"manifest declares a non-development file: {row.get('relative_path')}")
        relative = Path(str(row["relative_path"]))
        candidate = (root / relative).resolve()
        if root not in candidate.parents:
            raise PermissionError(f"release file escapes approved root: {relative}")
        if any(token in relative.as_posix().lower() for token in ("validation", "holdout", "forward", "2026")):
            raise PermissionError(f"forbidden role token in release path: {relative}")

    validated_files: list[ValidatedReleaseFile] = []
    for row in sorted(file_rows, key=lambda item: str(item["relative_path"])):
        relative_text = str(row["relative_path"]).replace("\\", "/")
        path = (root / relative_text).resolve()
        parquet = pq.ParquetFile(path)
        if schema_hash(parquet.schema_arrow) != manifest.get("schema_sha256"):
            raise ValueError(f"schema hash mismatch: {relative_text}")
        expected_groups = list(row.get("row_groups", []))
        if parquet.metadata.num_row_groups != len(expected_groups):
            raise ValueError(f"row-group count mismatch: {relative_text}")
        if parquet.metadata.num_rows != int(row.get("rows", -1)):
            raise ValueError(f"row count mismatch: {relative_text}")
        observed_groups: list[dict[str, Any]] = []
        for row_group_id, expected in enumerate(expected_groups):
            minimum, maximum = _metadata_date_range(parquet, row_group_id)
            _assert_development_range(minimum, maximum, roles)
            if expected.get("data_role") != "development":
                raise PermissionError(f"row group is not assigned development: {relative_text}#{row_group_id}")
            if str(minimum.date()) != expected.get("min_trade_date") or str(maximum.date()) != expected.get("max_trade_date"):
                raise ValueError(f"row-group date metadata mismatch: {relative_text}#{row_group_id}")
            rows = parquet.metadata.row_group(row_group_id).num_rows
            if rows != int(expected.get("rows", -1)):
                raise ValueError(f"row-group row count mismatch: {relative_text}#{row_group_id}")
            observed_groups.append(dict(expected))

        stat = path.stat()
        if stat.st_size != int(row.get("size", -1)):
            raise ValueError(f"release file size mismatch: {relative_text}")
        observed_hash = sha256_file(path)
        if observed_hash != row.get("sha256"):
            raise ValueError(f"release file hash mismatch: {relative_text}")
        validated_files.append(
            ValidatedReleaseFile(
                path=path,
                relative_path=relative_text,
                sha256=observed_hash,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                row_groups=tuple(observed_groups),
            )
        )

    expected_count = int(manifest.get("file_count", -1))
    if len(validated_files) != expected_count:
        raise ValueError(f"release file-count mismatch: {len(validated_files)} != {expected_count}")
    return ValidatedDevelopmentRelease(
        root=root,
        manifest_path=manifest_path,
        manifest=manifest,
        release_hash=computed_release_hash,
        split_manifest_sha256=split_hash,
        files=tuple(validated_files),
    )


def read_development_panel(
    release: ValidatedDevelopmentRelease,
    *,
    trade_date: pd.Timestamp,
    row_group_index: int,
    columns: Iterable[str],
    read_ledger_path: Path,
    loader_sha: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Read only prevalidated development row groups and atomically write a read ledger."""

    trade_date = pd.Timestamp(trade_date).normalize()
    roles = split_roles(Path(release.manifest["split_manifest_path"]))
    role = roles.get(trade_date)
    if role not in DEVELOPMENT_ROLES:
        raise PermissionError(f"requested date is not development: {trade_date.date()} role={role}")
    requested = set(columns)
    required = sorted((requested - DERIVED_PIT_COLUMNS) | {"code", "trade_time", "date", "close", "signal_time"})
    derived_columns: dict[str, dict[str, str]] = {}
    if "ctx_source_session_upper_bound" in requested:
        ordered_dates = sorted(roles)
        try:
            date_index = ordered_dates.index(trade_date)
        except ValueError as exc:
            raise PermissionError(f"requested date is absent from split manifest: {trade_date.date()}") from exc
        if date_index == 0:
            raise PermissionError("context source-session upper bound has no preceding split session")
        previous_session = ordered_dates[date_index - 1]
        previous_role = roles[previous_session]
        if previous_role not in DEVELOPMENT_ROLES:
            raise PermissionError(
                "context source-session upper bound crosses a forbidden split role: "
                f"{previous_session.date()} role={previous_role}"
            )
        derived_columns["ctx_source_session_upper_bound"] = {
            "value": str(previous_session.date()),
            "method": "immediately_preceding_approved_split_session",
            "evidence": "source release hard rule requires ctx source_date < exec_date",
        }
    frames: list[pd.DataFrame] = []
    entries: list[dict[str, Any]] = []
    for item in sorted(release.files, key=lambda value: value.relative_path):
        stat = item.path.stat()
        if stat.st_size != item.size or stat.st_mtime_ns != item.mtime_ns:
            raise RuntimeError(f"release file changed after preflight: {item.relative_path}")
        parquet = pq.ParquetFile(item.path)
        if not 0 <= row_group_index < parquet.metadata.num_row_groups:
            raise IndexError(f"row group unavailable: {item.relative_path}#{row_group_index}")
        missing = sorted(set(required) - set(parquet.schema_arrow.names))
        if missing:
            raise ValueError(f"development panel missing fields {missing}: {item.relative_path}")
        group_manifest = item.row_groups[row_group_index]
        if group_manifest.get("data_role") != "development":
            raise PermissionError(f"refusing non-development row group: {item.relative_path}#{row_group_index}")
        table = parquet.read_row_group(row_group_index, columns=required)
        times = pd.to_datetime(
            table["trade_time"].combine_chunks().to_pandas(), errors="coerce", format="mixed"
        )
        observed_roles = times.dt.normalize().map(roles)
        if observed_roles.isna().any():
            raise PermissionError(f"read row group contains dates absent from split manifest: {item.relative_path}#{row_group_index}")
        observed_role_counts = {
            str(name): int(count) for name, count in observed_roles.value_counts().items()
        }
        observed_forbidden = sorted(
            role_name for role_name in observed_role_counts if role_name not in DEVELOPMENT_ROLES
        )
        if observed_forbidden:
            raise PermissionError(
                f"read row group contains forbidden roles {observed_forbidden}: {item.relative_path}#{row_group_index}"
            )
        mask = times.dt.normalize().eq(trade_date).to_numpy()
        selected = table.filter(pa.array(mask)).to_pandas()
        selected["trade_time"] = pd.to_datetime(selected["trade_time"], errors="coerce", format="mixed")
        for name, derivation in derived_columns.items():
            selected[name] = pd.Timestamp(derivation["value"])
        if not selected.empty:
            frames.append(selected)
        entries.append(
            {
                "file_path": str(item.path),
                "file_hash": item.sha256,
                "row_group_id": row_group_index,
                "min_trade_date": group_manifest["min_trade_date"],
                "max_trade_date": group_manifest["max_trade_date"],
                "assigned_data_role": "development",
                "observed_rows_by_role": observed_role_counts,
                "rows_read": len(table),
                "rows_selected": len(selected),
                "read_timestamp": datetime.now(timezone.utc).isoformat(),
                "release_hash": release.release_hash,
                "loader_sha": loader_sha,
                "derived_pit_columns": derived_columns,
            }
        )
    if not frames:
        raise ValueError(f"development release contains no rows for {trade_date.date()}")
    output = pd.concat(frames, ignore_index=True).sort_values(
        ["code", "trade_time"], kind="mergesort"
    ).reset_index(drop=True)
    opened_file_roles = Counter(str(row["assigned_data_role"]) for row in entries)
    rows_by_role: Counter[str] = Counter()
    for row in entries:
        rows_by_role.update(
            {str(role_name): int(count) for role_name, count in row["observed_rows_by_role"].items()}
        )
    forbidden_file_opens = sum(
        count for role_name, count in opened_file_roles.items() if role_name not in DEVELOPMENT_ROLES
    )
    forbidden_row_group_reads = sum(
        1 for row in entries if str(row["assigned_data_role"]) not in DEVELOPMENT_ROLES
    )
    ledger = {
        "ledger_version": "cn_development_only_read_ledger_v1",
        "release_hash": release.release_hash,
        "split_manifest_sha256": release.split_manifest_sha256,
        "loader_sha": loader_sha,
        "requested_trade_date": str(trade_date.date()),
        "requested_data_role": "development",
        "entries": entries,
        "opened_file_count_by_role": dict(sorted(opened_file_roles.items())),
        "rows_read_by_role": dict(sorted(rows_by_role.items())),
        "forbidden_file_open_count": forbidden_file_opens,
        "forbidden_row_group_read_count": forbidden_row_group_reads,
        "validation_rows_read": int(rows_by_role["validation"]),
        "holdout_rows_read": int(rows_by_role["holdout"]),
        "forward_rows_read": int(rows_by_role["forward"]),
        "total_rows_read": sum(int(row["rows_read"]) for row in entries),
        "selected_rows": len(output),
        "derived_pit_columns": derived_columns,
    }
    atomic_write_json(read_ledger_path, ledger)
    return output, entries


def cache_provenance(
    *,
    release_hash_value: str,
    split_manifest_sha256: str,
    loader_code_hash: str,
    field_registry_hash: str,
    data_role: str,
    materializer_hash: str,
) -> dict[str, str]:
    payload = {
        "release_hash": release_hash_value,
        "split_manifest_hash": split_manifest_sha256,
        "loader_code_hash": loader_code_hash,
        "field_registry_hash": field_registry_hash,
        "data_role": data_role,
        "materializer_hash": materializer_hash,
    }
    return {**payload, "cache_namespace": canonical_json_hash(payload)}


def initialize_cache_root(cache_root: Path, expected: Mapping[str, str], *, require_fresh: bool) -> None:
    provenance_path = cache_root / "cache_provenance.json"
    if cache_root.exists():
        existing_files = [path for path in cache_root.rglob("*") if path.is_file()]
        if require_fresh and existing_files:
            raise PermissionError("formal development-only rerun requires a fresh cache root")
        if existing_files:
            if not provenance_path.exists():
                raise PermissionError("cache lacks data-role provenance")
            actual = json.loads(provenance_path.read_text(encoding="utf-8"))
            if actual != dict(expected):
                raise PermissionError("cache provenance mismatch")
            return
    cache_root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(provenance_path, dict(expected))
