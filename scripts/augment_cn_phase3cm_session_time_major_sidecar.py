"""Add PIT fundamental, chip, and pre-lagged daily context to a session sidecar."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from our_system_phase2.services.chip_sidecar import (
    CHIP_FIELDS,
    load_chip_context,
    point_in_time_chip_context,
)
from our_system_phase2.services.fundamental_representations import CanonicalFundamentalMaterializer
from our_system_phase2.services.pit_fundamental_fabric import PITFundamentalFabricAdapter, normalize_cn_code
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


STABLE_KEY = ("trade_time", "code", "source_shard", "source_row_identity", "duplicate_ordinal")


def _progress(path: Path | None, event: str, *, started: float, **payload: Any) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "event": event,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _sessions(path: Path) -> pd.DatetimeIndex:
    values = sorted(
        pd.Timestamp(row["trade_date"])
        for row in _read_csv(path)
        if str(row.get("split") or "").lower() == "train"
    )
    if not values or len(values) != len(set(values)):
        raise ValueError("split manifest must define unique development sessions")
    return pd.DatetimeIndex(values)


def _frame_digest(frame: pd.DataFrame, columns: list[str]) -> str:
    values = pd.util.hash_pandas_object(frame[columns], index=False).to_numpy().tobytes()
    return hashlib.sha256(values).hexdigest()


def _bar_source_path(root: Path, shard_index: int) -> Path:
    paths = sorted((root / f"shard_{shard_index:02d}").rglob("*.parquet"))
    if len(paths) != 1:
        raise ValueError(
            f"expected one development bar source for shard {shard_index}, found {len(paths)}"
        )
    return paths[0]


def _materialize_lagged_daily_context(
    frame: pd.DataFrame,
    *,
    fields: list[str],
    bar_source_root: Path,
    shard_index: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not fields:
        return frame, {}
    manifest_path = bar_source_root / "development_only_release_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if manifest.get("forbidden_roles_present") or bool(manifest.get("forward_2026_present")):
        raise PermissionError("bar context source contains forbidden or sealed roles")
    source = _bar_source_path(bar_source_root, shard_index)
    source_schema = set(pl.read_parquet_schema(source))
    missing = sorted(set(fields) - source_schema)
    if missing:
        raise ValueError(f"bar context source fields are missing: {missing}")

    aggregations: list[pl.Expr] = []
    for field in fields:
        aggregations.extend(
            (
                pl.col(field).sort_by("trade_time").last().alias(field),
                pl.col(field).n_unique().alias(f"__nunique_{field}"),
            )
        )
    daily = (
        pl.scan_parquet(source)
        .select("code", "trade_time", *fields)
        .with_columns(pl.col("trade_time").dt.date().alias("__session_date"))
        .group_by("code", "__session_date")
        .agg(*aggregations)
        .collect(engine="streaming")
    )
    variation = {
        field: int(daily[f"__nunique_{field}"].max() or 0) for field in fields
    }
    invalid = {field: count for field, count in variation.items() if count > 1}
    if invalid:
        raise ValueError(f"lagged daily context changes within a session: {invalid}")
    daily = daily.select("code", "__session_date", *fields).with_columns(
        (
            pl.col("__session_date").cast(pl.Datetime("us"))
            + pl.duration(hours=15)
        ).alias("trade_time")
    )
    context = daily.select("code", "trade_time", *fields).to_pandas()
    if context.duplicated(["code", "trade_time"]).any():
        raise ValueError("lagged daily context has duplicate stock-session coordinates")
    output = frame.merge(
        context,
        on=["code", "trade_time"],
        how="left",
        validate="one_to_one",
        sort=False,
    )
    return output, {
        "source": str(source),
        "source_sha256": _sha256(source),
        "fields": fields,
        "maximum_intraday_unique_values": variation,
        "join_policy": "same_session_1500_value_of_pre_lagged_daily_context",
    }


def _materialize_market_session_context(
    frame: pd.DataFrame,
    *,
    fields: list[str],
    bar_source_root: Path,
    shard_index: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Broadcast registered pre-lagged MARKET state to stock-session rows."""

    if not fields:
        return frame, {}
    manifest_path = bar_source_root / "development_only_release_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if manifest.get("forbidden_roles_present") or bool(manifest.get("forward_2026_present")):
        raise PermissionError("bar context source contains forbidden or sealed roles")
    source = _bar_source_path(bar_source_root, shard_index)
    source_schema = set(pl.read_parquet_schema(source))
    missing = sorted(set(fields) - source_schema)
    if missing:
        raise ValueError(f"market context source fields are missing: {missing}")

    aggregations: list[pl.Expr] = []
    for field in fields:
        aggregations.extend(
            (
                pl.col(field).sort_by("trade_time").last().alias(field),
                pl.col(field).drop_nulls().n_unique().alias(f"__nunique_{field}"),
            )
        )
    daily = (
        pl.scan_parquet(source)
        .select("trade_time", *fields)
        .with_columns(pl.col("trade_time").dt.date().alias("__session_date"))
        .group_by("__session_date")
        .agg(*aggregations)
        .collect(engine="streaming")
    )
    variation = {field: int(daily[f"__nunique_{field}"].max() or 0) for field in fields}
    invalid = {field: count for field, count in variation.items() if count > 1}
    if invalid:
        raise ValueError(f"market context is not constant within a session: {invalid}")
    daily = daily.select("__session_date", *fields).with_columns(
        (pl.col("__session_date").cast(pl.Datetime("us")) + pl.duration(hours=15)).alias(
            "trade_time"
        )
    )
    context = (
        daily.select("trade_time", *fields)
        .sort("trade_time")
        .to_pandas()
        .reset_index(drop=True)
    )
    if context.duplicated(["trade_time"]).any():
        raise ValueError("market context has duplicate session coordinates")
    output = frame.merge(
        context,
        on=["trade_time"],
        how="left",
        validate="many_to_one",
        sort=False,
    )
    return output, {
        "source": str(source),
        "source_sha256": _sha256(source),
        "fields": fields,
        "entity_scope": "MARKET",
        "session_value_digest": _frame_digest(context, ["trade_time", *fields]),
        "session_count": len(context),
        "maximum_intraday_cross_sectional_unique_values": variation,
        "join_policy": "same_session_1500_broadcast_of_pre_lagged_market_state",
        "lag_reapplied": False,
    }


def _materialize_stock_session_close(
    frame: pd.DataFrame,
    *,
    fields: list[str],
    bar_source_root: Path,
    shard_index: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Reduce registered STOCK BAR_VALUE fields to the exact session close row."""

    if not fields:
        return frame, {}
    manifest_path = bar_source_root / "development_only_release_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if manifest.get("forbidden_roles_present") or bool(manifest.get("forward_2026_present")):
        raise PermissionError("bar value source contains forbidden or sealed roles")
    source = _bar_source_path(bar_source_root, shard_index)
    source_schema = set(pl.read_parquet_schema(source))
    missing = sorted(set(fields) - source_schema)
    if missing:
        raise ValueError(f"stock session-close source fields are missing: {missing}")

    daily = (
        pl.scan_parquet(source)
        .select("code", "trade_time", *fields)
        .with_columns(pl.col("trade_time").dt.date().alias("__session_date"))
        .group_by("code", "__session_date")
        .agg(*(pl.col(field).sort_by("trade_time").last().alias(field) for field in fields))
        .collect(engine="streaming")
        .with_columns(
            (pl.col("__session_date").cast(pl.Datetime("us")) + pl.duration(hours=15)).alias(
                "trade_time"
            )
        )
    )
    context = daily.select("code", "trade_time", *fields).to_pandas()
    if context.duplicated(["code", "trade_time"]).any():
        raise ValueError("stock session-close context has duplicate stock-session coordinates")
    output = frame.merge(
        context,
        on=["code", "trade_time"],
        how="left",
        validate="one_to_one",
        sort=False,
    )
    return output, {
        "source": str(source),
        "source_sha256": _sha256(source),
        "fields": fields,
        "entity_scope": "STOCK",
        "join_policy": "same_stock_same_session_exact_last_bar_value_at_1500",
        "lag_reapplied": False,
    }


def _materialize_incremental_chip_context(
    frame: pd.DataFrame,
    chip: pd.DataFrame,
    *,
    fields: list[str],
) -> pd.DataFrame:
    """Add chip fields without rewriting an existing PIT source-session column."""
    if "chip_source_session" not in frame.columns:
        return point_in_time_chip_context(frame, chip, fields=fields, data_role="development")
    existing_source_session = frame["chip_source_session"].copy().reset_index(drop=True)
    joined = point_in_time_chip_context(
        frame.drop(columns=["chip_source_session"]),
        chip,
        fields=fields,
        data_role="development",
    )
    generated_source_session = joined["chip_source_session"].reset_index(drop=True)
    existing_normalized = pd.to_datetime(existing_source_session, errors="coerce")
    generated_normalized = pd.to_datetime(generated_source_session, errors="coerce")
    equal = existing_normalized.eq(generated_normalized) | (
        existing_normalized.isna() & generated_normalized.isna()
    )
    if not bool(equal.all()):
        raise RuntimeError("incremental chip PIT source-session drift")
    joined["chip_source_session"] = existing_source_session.to_numpy()
    return joined


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--fundamental-root", type=Path, required=True)
    parser.add_argument("--chip-root", type=Path)
    parser.add_argument("--bar-source-root", type=Path)
    parser.add_argument("--maximum-observable-time", required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--progress-log", type=Path)
    args = parser.parse_args()
    started = time.perf_counter()
    if args.progress_log is not None:
        args.progress_log.parent.mkdir(parents=True, exist_ok=True)
        args.progress_log.write_text("", encoding="utf-8")
    _progress(args.progress_log, "START", started=started, shard_index=int(args.shard_index))

    if not 0 <= int(args.shard_index) < 16:
        raise ValueError("session sidecar shard index must be in [0, 15]")
    source_manifest_path = args.source_root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if str(source_manifest.get("data_role")) != "development_train_only":
        raise PermissionError("source session sidecar is not development/train-only")
    if any(int(source_manifest.get(key) or 0) for key in ("validation_reads", "holdout_reads", "forward_2026_reads")):
        raise PermissionError("source session sidecar records forbidden data access")
    _progress(args.progress_log, "SOURCE_CONTRACT_VALIDATED", started=started)

    source = args.source_root / f"shard_{int(args.shard_index):02d}.parquet"
    frame = pd.read_parquet(source)
    _progress(
        args.progress_log,
        "SOURCE_FRAME_LOADED",
        started=started,
        rows=len(frame),
        columns=len(frame.columns),
    )
    missing_key = sorted(set(STABLE_KEY) - set(frame.columns))
    if missing_key:
        raise ValueError(f"source session sidecar stable key is incomplete: {missing_key}")
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="raise")
    if frame["trade_time"].dt.year.ge(2026).any():
        raise PermissionError("source session sidecar enters sealed 2026")
    if frame.duplicated(list(STABLE_KEY)).any():
        raise ValueError("source session sidecar has duplicate stable keys")

    candidates = _read_csv(args.candidate_table)
    required_fields = sorted(
        {field for row in candidates for field in expression_fields(str(row.get("expression") or ""))}
    )
    missing_fields = sorted(set(required_fields) - set(frame.columns))
    chip_fields = sorted(set(missing_fields) & set(CHIP_FIELDS.values()))
    if chip_fields and args.chip_root is None:
        raise PermissionError("chip fields require --chip-root")
    _progress(
        args.progress_log,
        "FIELDS_CLASSIFIED",
        started=started,
        required_field_count=len(required_fields),
        missing_field_count=len(missing_fields),
    )

    original_columns = list(frame.columns)
    original_key_digest = _frame_digest(frame, list(STABLE_KEY))
    original_payload_digest = _frame_digest(frame, original_columns)
    sessions = _sessions(args.split_manifest)
    registry = UnifiedCapabilityRegistry.read(args.registry)
    bar_context_fields: list[str] = []
    market_context_fields: list[str] = []
    session_close_stock_fields: list[str] = []
    session_close_market_fields: list[str] = []
    fundamental_fields: list[str] = []
    for field_id in sorted(set(missing_fields) - set(chip_fields)):
        capability = registry.resolve(field_id)
        if (
            capability.source_family == "lagged_daily_context"
            and capability.temporal_semantics == "PREVIOUS_SESSION_STOCK_CONTEXT"
        ):
            bar_context_fields.append(field_id)
        elif (
            capability.entity_scope == "MARKET"
            and capability.source_family == "lagged_daily_context"
            and capability.temporal_semantics == "PREVIOUS_SESSION_MARKET_STATE"
            and capability.observable_clock == "previous_session"
            and capability.source_lag == 1
            and capability.source_lag_unit == "sessions"
        ):
            market_context_fields.append(field_id)
        elif (
            capability.entity_scope == "STOCK"
            and capability.source_family == "raw_1min"
            and capability.temporal_semantics == "BAR_VALUE"
            and capability.observable_clock == "bar_close"
            and capability.source_lag == 0
            and capability.source_lag_unit == "bars"
        ):
            session_close_stock_fields.append(field_id)
        elif (
            capability.entity_scope == "MARKET"
            and capability.source_family == "raw_1min"
            and capability.temporal_semantics == "BAR_VALUE"
            and capability.observable_clock == "bar_close"
            and capability.source_lag == 0
            and capability.source_lag_unit == "bars"
        ):
            session_close_market_fields.append(field_id)
        else:
            fundamental_fields.append(field_id)
    if (
        bar_context_fields
        or market_context_fields
        or session_close_stock_fields
        or session_close_market_fields
    ) and args.bar_source_root is None:
        raise PermissionError("registered bar/context fields require --bar-source-root")
    _progress(
        args.progress_log,
        "ROUTES_CLASSIFIED",
        started=started,
        fundamental_field_count=len(fundamental_fields),
        chip_field_count=len(chip_fields),
        bar_context_field_count=len(bar_context_fields),
        market_context_field_count=len(market_context_fields),
        session_close_stock_field_count=len(session_close_stock_fields),
        session_close_market_field_count=len(session_close_market_fields),
    )

    specs: dict[str, dict[str, Any]] = {}
    prefetch_fields: dict[str, set[str]] = {}
    for field_id in fundamental_fields:
        capability = registry.resolve(field_id)
        spec = dict((capability.metadata or {}).get("canonical_representation") or {})
        if not spec or not bool(spec.get("search_eligible")):
            raise PermissionError(f"session field is not PIT search eligible: {field_id}")
        specs[field_id] = spec
        for source_spec in spec.get("source_fields") or []:
            prefetch_fields.setdefault(str(source_spec["source_table"]), set()).add(
                str(source_spec["source_field"])
            )
    adapter = PITFundamentalFabricAdapter(
        source_root=args.fundamental_root,
        sessions=sessions,
        maximum_observable_time=args.maximum_observable_time,
        prefetch_source_fields_by_table=prefetch_fields,
    )
    materializer = CanonicalFundamentalMaterializer(adapter)
    coordinates = frame[["code", "trade_time"]].rename(columns={"trade_time": "session_time"}).copy()
    coordinates["code"] = coordinates["code"].map(normalize_cn_code)
    coordinate_index = pd.MultiIndex.from_frame(coordinates)
    coverage: dict[str, float] = {}
    bar_context_input: dict[str, Any] = {}
    market_context_input: dict[str, Any] = {}
    session_close_stock_input: dict[str, Any] = {}
    session_close_market_input: dict[str, Any] = {}
    if bar_context_fields:
        _progress(args.progress_log, "BAR_CONTEXT_START", started=started)
        frame, bar_context_input = _materialize_lagged_daily_context(
            frame,
            fields=bar_context_fields,
            bar_source_root=args.bar_source_root,
            shard_index=int(args.shard_index),
        )
        for field_id in bar_context_fields:
            coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)
        _progress(args.progress_log, "BAR_CONTEXT_END", started=started)

    if market_context_fields:
        _progress(args.progress_log, "MARKET_CONTEXT_START", started=started)
        frame, market_context_input = _materialize_market_session_context(
            frame,
            fields=market_context_fields,
            bar_source_root=args.bar_source_root,
            shard_index=int(args.shard_index),
        )
        for field_id in market_context_fields:
            coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)
        _progress(args.progress_log, "MARKET_CONTEXT_END", started=started)

    if session_close_stock_fields:
        _progress(args.progress_log, "STOCK_SESSION_CLOSE_START", started=started)
        frame, session_close_stock_input = _materialize_stock_session_close(
            frame,
            fields=session_close_stock_fields,
            bar_source_root=args.bar_source_root,
            shard_index=int(args.shard_index),
        )
        for field_id in session_close_stock_fields:
            coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)
        _progress(args.progress_log, "STOCK_SESSION_CLOSE_END", started=started)

    if session_close_market_fields:
        _progress(args.progress_log, "MARKET_SESSION_CLOSE_START", started=started)
        frame, session_close_market_input = _materialize_market_session_context(
            frame,
            fields=session_close_market_fields,
            bar_source_root=args.bar_source_root,
            shard_index=int(args.shard_index),
        )
        for field_id in session_close_market_fields:
            coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)
        _progress(args.progress_log, "MARKET_SESSION_CLOSE_END", started=started)

    for ordinal, field_id in enumerate(fundamental_fields, start=1):
        _progress(
            args.progress_log,
            "FUNDAMENTAL_FIELD_START",
            started=started,
            field_id=field_id,
            field_ordinal=ordinal,
            field_count=len(fundamental_fields),
        )
        spec = specs[field_id]
        materialized = materializer.materialize(spec, coordinates)
        values = materialized.set_index(["code", "session_time"])[field_id]
        if values.index.has_duplicates:
            raise ValueError(f"duplicate PIT materialization coordinates: {field_id}")
        frame[field_id] = values.reindex(coordinate_index).to_numpy()
        coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)
        _progress(
            args.progress_log,
            "FUNDAMENTAL_FIELD_END",
            started=started,
            field_id=field_id,
            field_ordinal=ordinal,
            coverage=coverage[field_id],
        )

    chip_input: dict[str, Any] | None = None
    if chip_fields:
        _progress(args.progress_log, "CHIP_START", started=started)
        chip, chip_input = load_chip_context(
            args.chip_root,
            allowed_codes=set(frame["code"].astype(str)),
            fields=chip_fields,
            maximum_observable_time=args.maximum_observable_time,
        )
        frame = _materialize_incremental_chip_context(
            frame,
            chip,
            fields=chip_fields,
        )
        for field_id in chip_fields:
            coverage[field_id] = round(float(frame[field_id].notna().mean()), 8)
        _progress(args.progress_log, "CHIP_END", started=started)

    if len(frame) != int(source_manifest["shards"][int(args.shard_index)]["rows"]):
        raise RuntimeError("session augmentation row count drift")
    _progress(args.progress_log, "ROW_COUNT_VALIDATED", started=started, rows=len(frame))
    _progress(args.progress_log, "STABLE_KEY_DIGEST_START", started=started)
    current_key_digest = _frame_digest(frame, list(STABLE_KEY))
    if current_key_digest != original_key_digest:
        raise RuntimeError("session augmentation stable-key digest drift")
    _progress(args.progress_log, "STABLE_KEY_DIGEST_END", started=started)
    _progress(args.progress_log, "SOURCE_PAYLOAD_DIGEST_START", started=started)
    current_payload_digest = _frame_digest(frame, original_columns)
    if current_payload_digest != original_payload_digest:
        raise RuntimeError("session augmentation changed source payload columns")
    _progress(args.progress_log, "SOURCE_PAYLOAD_DIGEST_END", started=started)
    _progress(args.progress_log, "PARITY_VALIDATED", started=started)

    args.output_root.mkdir(parents=True, exist_ok=True)
    target = args.output_root / f"shard_{int(args.shard_index):02d}.parquet"
    temporary = target.with_suffix(".tmp.parquet")
    _progress(args.progress_log, "OUTPUT_WRITE_START", started=started)
    frame.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(target)
    _progress(args.progress_log, "OUTPUT_WRITE_END", started=started, output_bytes=target.stat().st_size)
    summary = {
        "schema_version": "cn_phase3cm_session_sidecar_augmentation_v1",
        "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "shard_index": int(args.shard_index),
        "data_role": "development_train_only",
        "source": {"path": str(source), "sha256": _sha256(source), "rows": len(frame)},
        "output": {"path": str(target), "sha256": _sha256(target), "bytes": target.stat().st_size},
        "source_stable_key_digest": original_key_digest,
        "source_payload_digest": original_payload_digest,
        "required_field_count": len(required_fields),
        "already_present_field_count": len(set(required_fields) & set(original_columns)),
        "fundamental_fields": fundamental_fields,
        "chip_fields": chip_fields,
        "bar_context_fields": bar_context_fields,
        "bar_context_source": bar_context_input,
        "market_context_fields": market_context_fields,
        "market_context_source": market_context_input,
        "session_close_stock_fields": session_close_stock_fields,
        "session_close_stock_source": session_close_stock_input,
        "session_close_market_fields": session_close_market_fields,
        "session_close_market_source": session_close_market_input,
        "coverage": coverage,
        "chip_sidecar": chip_input,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    summary_path = args.output_root / f"shard_{int(args.shard_index):02d}.augmentation.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _progress(args.progress_log, "COMPLETED", started=started, receipt=str(summary_path))
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
