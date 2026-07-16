from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from our_system_phase2.services.phase3cm_streaming_block_reader import (
    TimeMajorBlockReader,
    build_forward_label_shard,
    build_global_continuity_forward_label_sidecars,
)
from our_system_phase2.services.phase3cm_time_major_sidecar import build_time_major_shard


def _source(path: Path, codes: tuple[str, ...], offset: float) -> None:
    rows = []
    for code_index, code in enumerate(codes):
        for minute in range(6):
            rows.append(
                {
                    "trade_time": pd.Timestamp("2025-01-02 09:30") + pd.Timedelta(minutes=minute),
                    "code": code,
                    "close": offset + code_index * 10 + minute + 1.0,
                    "x": float(minute + code_index),
                }
            )
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False), path)


def _two_day_source(path: Path) -> None:
    rows = []
    for day in ("2025-01-02", "2025-01-03"):
        for minute in range(2):
            rows.append(
                {
                    "trade_time": pd.Timestamp(f"{day} 09:30") + pd.Timedelta(minutes=minute),
                    "code": "A",
                    "close": 10.0 + minute,
                    "x": float(minute),
                }
            )
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False), path)


def test_forward_label_sidecar_matches_group_shift_and_reader_has_global_barrier(tmp_path: Path) -> None:
    field_paths = []
    label_paths = []
    sources = []
    for shard, codes in enumerate((("A", "B"), ("C", "D"))):
        source = tmp_path / f"source_{shard}.parquet"
        field = tmp_path / f"field_{shard}.parquet"
        label = tmp_path / f"label_{shard}.parquet"
        _source(source, codes, float(shard * 100))
        build_time_major_shard(
            source_path=source,
            output_path=field,
            source_shard=shard,
            fields=("trade_time", "code", "close", "x"),
            row_group_size=8,
        )
        build_forward_label_shard(
            source_path=source,
            output_path=label,
            source_shard=shard,
            horizons=(1, 2),
            row_group_size=8,
        )
        field_paths.append(field)
        label_paths.append(label)
        sources.append(pd.read_parquet(source))

    reader = TimeMajorBlockReader(
        field_sidecars=field_paths,
        label_sidecars=label_paths,
        raw_fields=("close", "x"),
        horizons=(1, 2),
        symbol_registry=("A", "B", "C", "D"),
    )
    block = reader.read_block(
        start_time=pd.Timestamp("2025-01-02 09:30"),
        end_time=pd.Timestamp("2025-01-02 09:36"),
    )
    assert block.row_count == 24
    assert block.trade_time_count == 6
    assert block.day_labels == ("2025-01-02",)
    assert np.unique(block.day_ids).tolist() == [0]
    assert np.all(block.time_ids[1:] >= block.time_ids[:-1])
    for time_id in range(6):
        assert set(block.code_ids[block.time_ids == time_id].tolist()) == {0, 1, 2, 3}

    source = pd.concat(sources, ignore_index=True).sort_values(["code", "trade_time"])
    for horizon in (1, 2):
        source[f"fwd_ret_{horizon}m"] = source.groupby("code", sort=False)["close"].shift(-horizon) / source["close"] - 1.0
    expected = source.sort_values(["trade_time", "code"])
    for horizon in (1, 2):
        np.testing.assert_allclose(
            block.labels[horizon],
            expected[f"fwd_ret_{horizon}m"].to_numpy(dtype=float),
            rtol=1e-12,
            atol=1e-12,
            equal_nan=True,
        )
    assert block.audit["global_barrier_status"] == "COMPLETE_MARKET_TIME_BLOCK"
    assert block.audit["python_pandas_hot_path_calls"] == 0


def test_reader_hard_filters_to_frozen_eligible_trade_calendar(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    field = tmp_path / "field.parquet"
    label = tmp_path / "label.parquet"
    _two_day_source(source)
    field_record = build_time_major_shard(
        source_path=source,
        output_path=field,
        source_shard=0,
        fields=("trade_time", "code", "close", "x"),
        eligible_trade_dates=("2025-01-02",),
        split_manifest_hash="a" * 64,
    )
    label_record = build_forward_label_shard(
        source_path=source,
        output_path=label,
        source_shard=0,
        horizons=(1,),
        eligible_trade_dates=("2025-01-02",),
        split_manifest_hash="a" * 64,
    )
    reader = TimeMajorBlockReader(
        field_sidecars=(field,),
        label_sidecars=(label,),
        raw_fields=("close", "x"),
        horizons=(1,),
        symbol_registry=("A",),
        eligible_trade_dates=("2025-01-02",),
    )

    block = reader.read_block(
        start_time=pd.Timestamp("2025-01-02"),
        end_time=pd.Timestamp("2025-01-04"),
    )

    assert block.day_labels == ("2025-01-02",)
    assert block.row_count == 2
    assert field_record["source_total_rows"] == 4
    assert field_record["rows"] == 2
    assert label_record["rows"] == 2


def test_global_continuity_labels_stitch_strictly_later_symbol_fragment(tmp_path: Path) -> None:
    field_paths = []
    source_frames = []
    for shard, dates in enumerate((("2024-12-30", "2024-12-31"), ("2025-01-02", "2025-01-03"))):
        rows = []
        for offset, day in enumerate(dates):
            rows.append(
                {
                    "trade_time": pd.Timestamp(f"{day} 15:00"),
                    "code": "A",
                    "close": np.float32(10 + 2 * shard + offset),
                    "x": float(shard),
                }
            )
        source = tmp_path / f"fragment_{shard}.parquet"
        field = tmp_path / f"field_{shard}.parquet"
        frame = pd.DataFrame(rows)
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), source)
        build_time_major_shard(
            source_path=source,
            output_path=field,
            source_shard=shard,
            fields=("trade_time", "code", "close", "x"),
        )
        field_paths.append(field)
        source_frames.append(frame)

    label_root = tmp_path / "labels"
    manifest = build_global_continuity_forward_label_sidecars(
        field_sidecars=field_paths,
        output_root=label_root,
        horizons=(1, 2),
        split_manifest_hash="b" * 64,
    )
    reader = TimeMajorBlockReader(
        field_sidecars=field_paths,
        label_sidecars=(label_root / "shard_00.parquet", label_root / "shard_01.parquet"),
        raw_fields=("close",),
        horizons=(1, 2),
        symbol_registry=("A",),
    )

    block = reader.read_block(start_time=pd.Timestamp("2024-12-30"), end_time=pd.Timestamp("2025-01-04"))

    expected = pd.concat(source_frames, ignore_index=True).sort_values(["code", "trade_time"])
    for horizon in (1, 2):
        expected[f"fwd_ret_{horizon}m"] = (
            expected.groupby("code", sort=False)["close"].shift(-horizon) / expected["close"] - 1.0
        )
        np.testing.assert_allclose(
            block.labels[horizon],
            expected.sort_values(["trade_time", "code"])[f"fwd_ret_{horizon}m"].to_numpy(dtype=float),
            rtol=1e-12,
            atol=1e-12,
            equal_nan=True,
        )
    assert manifest["status"] == "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY"
    assert manifest["fragment_pair_count"] == 1
    assert manifest["lookahead_rows"] == 2
