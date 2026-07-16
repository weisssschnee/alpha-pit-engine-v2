from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from our_system_phase2.services.phase3cm_streaming_block_reader import (
    TimeMajorBlockReader,
    build_forward_label_shard,
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
