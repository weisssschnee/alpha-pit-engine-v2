from pathlib import Path

import pandas as pd

from scripts.convert_cn_yearly_1min_zip_to_session_shards import _canonical_payload_sha256
from scripts.verify_cn_sessionized_archive_parity import _sha256, verify_parity


def test_sessionized_archive_parity_aggregates_reference_minutes(tmp_path: Path) -> None:
    sessionized = tmp_path / "sessionized"
    sessionized.mkdir()
    shard = sessionized / "shard_00.parquet"
    pd.DataFrame(
        {
            "code": ["000001.SZ"],
            "trade_time": pd.to_datetime(["2024-01-02 15:00:00"]),
            "date": pd.to_datetime(["2024-01-02"]),
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "close": [10.3],
            "vol": [300],
            "amount": [3100.0],
        }
    ).to_parquet(shard, index=False)
    manifest = {
        "status": "SESSIONIZED_ARCHIVE_COMPLETE",
        "artifacts": [{"path": shard.name, "sha256": _sha256(shard)}],
    }
    manifest["manifest_payload_sha256"] = _canonical_payload_sha256(manifest)
    manifest_path = sessionized / "SESSIONIZED_ARCHIVE_COMPLETE.json"
    manifest_path.write_text(__import__("json").dumps(manifest), encoding="utf-8")
    reference = tmp_path / "reference"
    reference.mkdir()
    pd.DataFrame(
        {
            "code": ["000001.SZ", "000001.SZ"],
            "trade_time": pd.to_datetime(
                ["2024-01-02 09:30:00", "2024-01-02 15:00:00"]
            ),
            "open": [10.0, 10.2],
            "high": [10.2, 10.5],
            "low": [9.8, 10.0],
            "close": [10.1, 10.3],
            "vol": [100, 200],
            "amount": [1000.0, 2100.0],
        }
    ).to_parquet(reference / "shard_00.parquet", index=False)
    receipt = verify_parity(
        sessionized_root=sessionized,
        expected_manifest_sha256=_sha256(manifest_path),
        reference_root=reference,
        reference_pattern="*.parquet",
        output_path=tmp_path / "parity.json",
    )
    assert receipt["status"] == "SESSIONIZED_ARCHIVE_PARITY_PASS"
    assert receipt["coordinate_count"] == 1
    assert receipt["max_absolute_differences"]["vol"] == 0.0
