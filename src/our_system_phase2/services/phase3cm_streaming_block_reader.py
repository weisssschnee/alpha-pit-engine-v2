"""Exact forward-label sidecars and complete-market time-major block reads."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from collections import defaultdict
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import polars as pl

from our_system_phase2.services.phase3cm_time_major_sidecar import STABLE_KEY


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_forward_label_shard(
    *,
    source_path: Path,
    output_path: Path,
    source_shard: int,
    horizons: Sequence[int],
    row_group_size: int = 262_144,
    eligible_trade_dates: Sequence[str] | None = None,
    split_manifest_hash: str | None = None,
) -> dict[str, Any]:
    """Materialize the legacy per-symbol group-shift labels without Pandas."""

    source = Path(source_path)
    output = Path(output_path)
    horizon_values = tuple(sorted(set(int(value) for value in horizons)))
    if not horizon_values or min(horizon_values) <= 0:
        raise ValueError("forward horizons must be positive")
    started = time.perf_counter()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp.parquet")
    eligible = tuple(sorted({date.fromisoformat(str(value)) for value in eligible_trade_dates or ()}))
    code_major = pl.scan_parquet(source, row_index_name="source_row_identity", rechunk=False, low_memory=True)
    if eligible:
        code_major = code_major.filter(pl.col("trade_time").dt.date().is_in(eligible))
    code_major = (
        code_major
        .select("trade_time", "code", "close", pl.col("source_row_identity").cast(pl.UInt64))
        .sort(["code", "trade_time", "source_row_identity"], maintain_order=True)
        .with_columns(
            *[
                (
                    pl.col("close").shift(-horizon).over("code")
                    / pl.when(pl.col("close") == 0).then(None).otherwise(pl.col("close"))
                    - 1.0
                ).alias(f"fwd_ret_{horizon}m")
                for horizon in horizon_values
            ],
            pl.lit(int(source_shard), dtype=pl.UInt16).alias("source_shard"),
        )
        .sort(["trade_time", "code", "source_row_identity"], maintain_order=True)
        .with_columns(
            (pl.col("source_row_identity").cum_count().over(["trade_time", "code"]) - 1)
            .cast(pl.UInt32)
            .alias("duplicate_ordinal")
        )
        .select(*STABLE_KEY, *[f"fwd_ret_{horizon}m" for horizon in horizon_values])
    )
    code_major.sink_parquet(
        temporary,
        compression="zstd",
        compression_level=3,
        statistics=True,
        row_group_size=int(row_group_size),
        maintain_order=True,
        mkdir=True,
        engine="streaming",
    )
    temporary.replace(output)
    identity_payload = {
        "schema_version": "cn_phase3cm_forward_label_sidecar_v2_train_only",
        "source_sha256": _sha256(source),
        "source_shard": int(source_shard),
        "horizons": list(horizon_values),
        "eligible_trade_dates": [value.isoformat() for value in eligible],
        "split_manifest_hash": str(split_manifest_hash or ""),
        "legacy_semantics": "close.groupby(code,sort=False).shift(-h)/close-1",
    }
    return {
        **identity_payload,
        "label_sidecar_identity": hashlib.sha256(
            json.dumps(identity_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "source_path": str(source),
        "output_path": str(output),
        "output_sha256": _sha256(output),
        "output_bytes": output.stat().st_size,
        "rows": int(pl.scan_parquet(output).select(pl.len()).collect(engine="streaming").item()),
        "wall_seconds": time.perf_counter() - started,
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _stable_key_digest(path: Path) -> dict[str, Any]:
    aggregate = (
        pl.scan_parquet(path, low_memory=True)
        .select(
            pl.len().alias("rows"),
            pl.struct(list(STABLE_KEY)).hash(seed=20260716).sum().alias("stable_key_hash_sum_a"),
            pl.struct(list(STABLE_KEY)).hash(seed=20260717).sum().alias("stable_key_hash_sum_b"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    return {
        "rows": int(aggregate["rows"]),
        "stable_key_hash_sum_a": int(aggregate["stable_key_hash_sum_a"]),
        "stable_key_hash_sum_b": int(aggregate["stable_key_hash_sum_b"]),
    }


def build_global_continuity_forward_label_sidecars(
    *,
    field_sidecars: Sequence[Path],
    output_root: Path,
    horizons: Sequence[int],
    split_manifest_hash: str,
    row_group_size: int = 262_144,
) -> dict[str, Any]:
    """Build aligned labels while preserving per-symbol continuity across physical shards.

    Physical release shards split most symbols into two strictly ordered time
    fragments.  A shard-local shift therefore loses labels at the fragment
    boundary.  The bounded stitch appends only the first ``max(horizon)`` rows
    of the later fragment while evaluating the earlier fragment.
    """

    paths = tuple(Path(path).resolve() for path in field_sidecars)
    if not paths:
        raise ValueError("field sidecars are required")
    horizon_values = tuple(sorted(set(int(value) for value in horizons)))
    if not horizon_values or min(horizon_values) <= 0:
        raise ValueError("forward horizons must be positive")
    max_horizon = max(horizon_values)
    started = time.perf_counter()

    fragments_by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_digests: list[dict[str, Any]] = []
    for shard, path in enumerate(paths):
        schema = set(pl.scan_parquet(path).collect_schema().names())
        missing = sorted((set(STABLE_KEY) | {"close"}) - schema)
        if missing:
            raise RuntimeError(f"field sidecar missing label inputs {missing}: {path}")
        fragments = (
            pl.scan_parquet(path, low_memory=True)
            .group_by(pl.col("code").cast(pl.String).alias("code"))
            .agg(
                pl.len().alias("rows"),
                pl.col("trade_time").min().alias("min_trade_time"),
                pl.col("trade_time").max().alias("max_trade_time"),
            )
            .collect(engine="streaming")
        )
        for row in fragments.iter_rows(named=True):
            fragments_by_code[str(row["code"])].append(
                {
                    "shard": shard,
                    "rows": int(row["rows"]),
                    "min_trade_time": row["min_trade_time"],
                    "max_trade_time": row["max_trade_time"],
                }
            )
        source_digests.append(
            {
                "shard": shard,
                "path": str(path),
                "sha256": _sha256(path),
                **_stable_key_digest(path),
            }
        )

    earlier_codes_by_shard: dict[int, list[str]] = defaultdict(list)
    later_codes_by_shard: dict[int, list[str]] = defaultdict(list)
    fragment_pairs: list[dict[str, Any]] = []
    single_fragment_codes = 0
    for code, fragments in sorted(fragments_by_code.items()):
        ordered = sorted(fragments, key=lambda row: (row["min_trade_time"], row["shard"]))
        if len(ordered) == 1:
            single_fragment_codes += 1
            continue
        if len(ordered) != 2:
            raise RuntimeError(f"global continuity requires at most two fragments per symbol: {code}")
        early, late = ordered
        if early["max_trade_time"] >= late["min_trade_time"]:
            raise RuntimeError(f"symbol fragments overlap or interleave: {code}")
        earlier_codes_by_shard[int(early["shard"])].append(code)
        later_codes_by_shard[int(late["shard"])].append(code)
        fragment_pairs.append(
            {
                "code": code,
                "earlier_shard": int(early["shard"]),
                "later_shard": int(late["shard"]),
                "earlier_max_trade_time": str(early["max_trade_time"]),
                "later_min_trade_time": str(late["min_trade_time"]),
            }
        )

    lookahead_frames: list[pl.DataFrame] = []
    for shard, codes in sorted(later_codes_by_shard.items()):
        if not codes:
            continue
        lookahead = (
            pl.scan_parquet(paths[shard], low_memory=True)
            .filter(pl.col("code").cast(pl.String).is_in(codes))
            .select(*STABLE_KEY, "close")
            .sort(["code", "trade_time", "source_shard", "source_row_identity", "duplicate_ordinal"], maintain_order=True)
            .group_by("code", maintain_order=True)
            .head(max_horizon)
            .select(*STABLE_KEY, "close")
            .collect(engine="streaming")
        )
        lookahead_frames.append(lookahead)
    lookahead_all = (
        pl.concat(lookahead_frames, how="vertical_relaxed", rechunk=False)
        if lookahead_frames
        else pl.DataFrame(schema={
            "trade_time": pl.Datetime,
            "code": pl.String,
            "source_shard": pl.UInt16,
            "source_row_identity": pl.UInt64,
            "duplicate_ordinal": pl.UInt32,
            "close": pl.Float64,
        })
    )

    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    label_names = [f"fwd_ret_{horizon}m" for horizon in horizon_values]
    for shard, source in enumerate(paths):
        output = root / f"shard_{shard:02d}.parquet"
        temporary = output.with_name(output.name + ".tmp.parquet")
        original = (
            pl.scan_parquet(source, low_memory=True)
            .select(*STABLE_KEY, "close")
            .with_columns(pl.lit(True).alias("_is_original"))
        )
        early_codes = earlier_codes_by_shard.get(shard, [])
        if early_codes:
            extra = (
                lookahead_all.lazy()
                .filter(pl.col("code").is_in(early_codes))
                .with_columns(pl.lit(False).alias("_is_original"))
            )
            combined = pl.concat([original, extra], how="vertical_relaxed", rechunk=False)
        else:
            combined = original
        labeled = (
            combined
            .sort(["code", "trade_time", "source_shard", "source_row_identity", "duplicate_ordinal"], maintain_order=True)
            .with_columns(
                *[
                    (
                        pl.col("close").shift(-horizon).over("code")
                        / pl.when(pl.col("close") == 0).then(None).otherwise(pl.col("close"))
                        - 1.0
                    ).alias(f"fwd_ret_{horizon}m")
                    for horizon in horizon_values
                ]
            )
            .filter(pl.col("_is_original"))
            .sort(list(STABLE_KEY), maintain_order=True)
            .select(*STABLE_KEY, *label_names)
        )
        shard_started = time.perf_counter()
        labeled.sink_parquet(
            temporary,
            compression="zstd",
            compression_level=3,
            statistics=True,
            row_group_size=int(row_group_size),
            maintain_order=True,
            mkdir=True,
            engine="streaming",
        )
        temporary.replace(output)
        source_digest = source_digests[shard]
        output_digest = _stable_key_digest(output)
        if output_digest != {
            key: source_digest[key]
            for key in ("rows", "stable_key_hash_sum_a", "stable_key_hash_sum_b")
        }:
            raise RuntimeError(f"global-continuity label stable-key drift: shard={shard}")
        records.append(
            {
                "shard": shard,
                "source_path": str(source),
                "source_sha256": source_digest["sha256"],
                "output_path": str(output),
                "output_sha256": _sha256(output),
                "output_bytes": output.stat().st_size,
                "rows": output_digest["rows"],
                "stable_key_digest": output_digest,
                "earlier_fragment_code_count": len(early_codes),
                "lookahead_row_count": int(
                    lookahead_all.filter(pl.col("code").is_in(early_codes)).height
                    if early_codes
                    else 0
                ),
                "wall_seconds": time.perf_counter() - shard_started,
            }
        )

    identity_payload = {
        "schema_version": "cn_phase3cm_forward_label_sidecars_v3_global_symbol_continuity",
        "source_sidecar_hashes": [row["sha256"] for row in source_digests],
        "split_manifest_hash": str(split_manifest_hash),
        "horizons": list(horizon_values),
        "fragment_pair_count": len(fragment_pairs),
        "single_fragment_code_count": single_fragment_codes,
        "lookahead_policy": f"first_{max_horizon}_rows_of_strictly_later_symbol_fragment",
    }
    return {
        **identity_payload,
        "status": "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY",
        "label_sidecar_identity": hashlib.sha256(
            json.dumps(identity_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "source_shard_count": len(paths),
        "source_rows": sum(int(row["rows"]) for row in source_digests),
        "output_rows": sum(int(row["rows"]) for row in records),
        "output_bytes": sum(int(row["output_bytes"]) for row in records),
        "lookahead_rows": int(lookahead_all.height),
        "fragment_pair_digest": hashlib.sha256(
            json.dumps(fragment_pairs, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "shards": records,
        "wall_seconds": time.perf_counter() - started,
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


@dataclass(slots=True)
class TimeMajorBlock:
    raw_fields: dict[str, np.ndarray]
    labels: dict[int, np.ndarray]
    code_ids: np.ndarray
    time_ids: np.ndarray
    day_ids: np.ndarray
    trade_times_ns: np.ndarray
    day_labels: tuple[str, ...]
    source_shards: np.ndarray
    source_row_identity: np.ndarray
    duplicate_ordinal: np.ndarray
    audit: dict[str, Any]

    @property
    def row_count(self) -> int:
        return int(len(self.code_ids))

    @property
    def trade_time_count(self) -> int:
        return int(len(np.unique(self.time_ids)))


class TimeMajorBlockReader:
    """Read aligned per-shard sidecars and release one global time block."""

    def __init__(
        self,
        *,
        field_sidecars: Sequence[Path],
        label_sidecars: Sequence[Path],
        raw_fields: Sequence[str],
        horizons: Sequence[int],
        symbol_registry: Sequence[str],
        eligible_trade_dates: Sequence[str] | None = None,
    ) -> None:
        self.field_sidecars = tuple(Path(path) for path in field_sidecars)
        self.label_sidecars = tuple(Path(path) for path in label_sidecars)
        if not self.field_sidecars or len(self.field_sidecars) != len(self.label_sidecars):
            raise ValueError("field and label sidecars must form non-empty shard pairs")
        self.raw_fields = tuple(dict.fromkeys(str(field) for field in raw_fields))
        self.horizons = tuple(sorted(set(int(value) for value in horizons)))
        self.symbol_registry = tuple(str(value) for value in symbol_registry)
        if len(set(self.symbol_registry)) != len(self.symbol_registry):
            raise ValueError("symbol registry contains duplicates")
        self.symbol_to_id = {symbol: index for index, symbol in enumerate(self.symbol_registry)}
        self.eligible_trade_dates = tuple(
            sorted({date.fromisoformat(str(value)) for value in eligible_trade_dates or ()})
        )

    def read_block(self, *, start_time: pd.Timestamp, end_time: pd.Timestamp) -> TimeMajorBlock:
        start = np.datetime64(pd.Timestamp(start_time).to_datetime64())
        end = np.datetime64(pd.Timestamp(end_time).to_datetime64())
        if start >= end:
            raise ValueError("block start must precede block end")
        started = time.perf_counter()
        shard_frames: list[pl.DataFrame] = []
        source_bytes = 0
        for field_path, label_path in zip(self.field_sidecars, self.label_sidecars):
            source_bytes += field_path.stat().st_size + label_path.stat().st_size
            condition = (pl.col("trade_time") >= pl.lit(start)) & (pl.col("trade_time") < pl.lit(end))
            if self.eligible_trade_dates:
                condition &= pl.col("trade_time").dt.date().is_in(self.eligible_trade_dates)
            field_frame = (
                pl.scan_parquet(field_path, rechunk=False, low_memory=True)
                .filter(condition)
                .select(*STABLE_KEY, *self.raw_fields)
                .collect(engine="streaming")
            )
            label_names = [f"fwd_ret_{horizon}m" for horizon in self.horizons]
            label_frame = (
                pl.scan_parquet(label_path, rechunk=False, low_memory=True)
                .filter(condition)
                .select(*STABLE_KEY, *label_names)
                .collect(engine="streaming")
            )
            if field_frame.height != label_frame.height or not field_frame.select(STABLE_KEY).equals(
                label_frame.select(STABLE_KEY)
            ):
                raise RuntimeError("field/label sidecar stable-key drift")
            shard_frames.append(field_frame.hstack(label_frame.select(label_names)))
        if not shard_frames:
            raise RuntimeError("no field/label shard pairs were read")
        frame = pl.concat(shard_frames, how="vertical_relaxed", rechunk=False).sort(
            list(STABLE_KEY), maintain_order=True
        )
        times_ns = frame["trade_time"].to_numpy().astype("datetime64[ns]").astype(np.int64)
        unique_times, time_ids = np.unique(times_ns, return_inverse=True)
        codes = frame["code"].cast(pl.String).to_list()
        try:
            code_ids = np.fromiter((self.symbol_to_id[code] for code in codes), dtype=np.int32, count=len(codes))
        except KeyError as exc:
            raise RuntimeError(f"block code is absent from frozen symbol registry: {exc}") from exc
        # ``unique_times`` is an int64 nanosecond epoch array.  Converting it
        # directly to datetime64[D] interprets each nanosecond count as a day
        # count and silently turns every minute into a distinct pseudo-day.
        dates = unique_times.astype("datetime64[ns]").astype("datetime64[D]")
        unique_days, time_day_ids = np.unique(dates, return_inverse=True)
        day_ids = time_day_ids[time_ids].astype(np.int32)
        raw = {
            field: frame[field].cast(pl.Float64, strict=False).to_numpy()
            for field in self.raw_fields
        }
        labels = {
            horizon: frame[f"fwd_ret_{horizon}m"].cast(pl.Float64, strict=False).to_numpy()
            for horizon in self.horizons
        }
        audit = {
            "global_barrier_status": "COMPLETE_MARKET_TIME_BLOCK",
            "physical_shard_count": len(shard_frames),
            "row_count": frame.height,
            "trade_time_count": len(unique_times),
            "raw_field_count": len(raw),
            "horizon_count": len(labels),
            "source_sidecar_bytes": source_bytes,
            "wall_seconds": time.perf_counter() - started,
            "python_pandas_hot_path_calls": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        day_labels = tuple(str(value) for value in unique_days.astype(str))
        if self.eligible_trade_dates and not set(day_labels) <= {
            value.isoformat() for value in self.eligible_trade_dates
        }:
            raise RuntimeError("block contains a trade date outside the frozen eligible calendar")
        return TimeMajorBlock(
            raw_fields=raw,
            labels=labels,
            code_ids=code_ids,
            time_ids=time_ids.astype(np.int64),
            day_ids=day_ids,
            trade_times_ns=times_ns,
            day_labels=day_labels,
            source_shards=frame["source_shard"].to_numpy(),
            source_row_identity=frame["source_row_identity"].to_numpy(),
            duplicate_ordinal=frame["duplicate_ordinal"].to_numpy(),
            audit=audit,
        )
