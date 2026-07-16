from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from our_system_phase2.services.phase3cm_time_major_sidecar import (
    audit_sidecar_parity,
    build_time_major_shard,
)


def test_time_major_sidecar_preserves_rows_values_and_stable_identity(tmp_path) -> None:
    source = tmp_path / "source.parquet"
    frame = pd.DataFrame(
        {
            "code": ["B", "B", "A", "A", "A"],
            "trade_time": pd.to_datetime(
                ["2024-01-02 09:31", "2024-01-02 09:30", "2024-01-02 09:31", "2024-01-02 09:30", "2024-01-02 09:30"]
            ),
            "signal_time": pd.to_datetime(
                ["2024-01-02 09:31", "2024-01-02 09:30", "2024-01-02 09:31", "2024-01-02 09:30", "2024-01-02 09:30"]
            ),
            "close": [2.0, 1.0, 4.0, 3.0, None],
        }
    )
    table = pa.Table.from_pandas(frame, preserve_index=False).set_column(
        0, "code", pa.array(frame["code"], type=pa.large_string())
    )
    pq.write_table(table, source)
    output = tmp_path / "sidecar.parquet"

    record = build_time_major_shard(
        source_path=source,
        output_path=output,
        source_shard=7,
        fields=("code", "trade_time", "signal_time", "close"),
        row_group_size=3,
    )
    sidecar = pd.read_parquet(output)
    parity = audit_sidecar_parity(
        source_path=source,
        sidecar_path=output,
        source_shard=7,
        fields=("code", "trade_time", "signal_time", "close"),
    )

    assert list(sidecar["trade_time"]) == sorted(sidecar["trade_time"])
    assert sorted(sidecar["source_row_identity"].tolist()) == list(range(5))
    assert sidecar.loc[sidecar["code"].eq("A") & sidecar["trade_time"].eq(pd.Timestamp("2024-01-02 09:30")), "duplicate_ordinal"].tolist() == [0, 1]
    assert record["rows"] == 5
    assert parity["status"] == "SIDECAR_PARITY_PASS"
    assert parity["row_count_match"] is True
    assert parity["stable_key_unique"] is True
    assert parity["null_bitmap_match"] is True
    assert parity["field_value_digest_match"] is True
