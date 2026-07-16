"""Exact forward-label sidecars and complete-market time-major block reads."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
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
    code_major = (
        pl.scan_parquet(source, row_index_name="source_row_identity", rechunk=False, low_memory=True)
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
        "schema_version": "cn_phase3cm_forward_label_sidecar_v1",
        "source_sha256": _sha256(source),
        "source_shard": int(source_shard),
        "horizons": list(horizon_values),
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
        return TimeMajorBlock(
            raw_fields=raw,
            labels=labels,
            code_ids=code_ids,
            time_ids=time_ids.astype(np.int64),
            day_ids=day_ids,
            trade_times_ns=times_ns,
            day_labels=tuple(str(value) for value in unique_days.astype(str)),
            source_shards=frame["source_shard"].to_numpy(),
            source_row_identity=frame["source_row_identity"].to_numpy(),
            duplicate_ordinal=frame["duplicate_ordinal"].to_numpy(),
            audit=audit,
        )
