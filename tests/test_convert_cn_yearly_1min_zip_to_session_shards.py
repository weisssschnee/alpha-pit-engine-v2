from __future__ import annotations

import hashlib
from pathlib import Path
import zipfile

import pandas as pd
import pytest

from scripts.convert_cn_yearly_1min_zip_to_session_shards import convert_archive


def _archive(path: Path) -> str:
    header = "时间,代码,名称,开盘价,收盘价,最高价,最低价,成交量,成交额,涨幅,振幅\n"
    members = {
        "sz000001_2024.csv": [
            "2024-01-02 09:30:00,sz000001,A,10,10.1,10.2,9.9,100,1000,0,0",
            "2024-01-02 15:00:00,sz000001,A,10.2,10.3,10.4,10.0,200,2100,0,0",
            "2024-01-03 09:30:00,sz000001,A,11,11.1,11.2,10.9,300,3300,0,0",
            "2024-01-03 15:00:00,sz000001,A,11.2,11.3,11.4,11.0,400,4500,0,0",
        ],
        "sh600000_2024.csv": [
            "2024-01-02 09:30:00,sh600000,B,20,20.1,20.2,19.9,10,200,0,0",
            "2024-01-02 15:00:00,sh600000,B,20.2,20.3,20.4,20.0,20,410,0,0",
        ],
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, rows in members.items():
            archive.writestr(name, (header + "\n".join(rows) + "\n").encode("utf-8"))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_sessionization_is_deterministic_and_economically_lossless(tmp_path: Path) -> None:
    archive = tmp_path / "2024.zip"
    archive_sha = _archive(archive)
    output = tmp_path / "sessionized"
    manifest = convert_archive(
        archive_path=archive,
        expected_archive_sha256=archive_sha,
        expected_year=2024,
        output_root=output,
        shard_count=2,
        worker_count=1,
        date_min="2024-01-02",
        date_max="2024-12-31",
        member_limit=None,
    )

    assert manifest["status"] == "SESSIONIZED_ARCHIVE_COMPLETE"
    assert manifest["session_row_count"] == 3
    assert manifest["security_count"] == 2
    frame = pd.concat(
        [pd.read_parquet(output / artifact["path"]) for artifact in manifest["artifacts"]],
        ignore_index=True,
    ).sort_values(["date", "code"])
    first = frame.loc[
        (frame["code"] == "000001.SZ")
        & (frame["date"] == pd.Timestamp("2024-01-02"))
    ].iloc[0]
    assert first["open"] == 10.0
    assert first["close"] == pytest.approx(10.3)
    assert first["high"] == pytest.approx(10.4)
    assert first["low"] == pytest.approx(9.9)
    assert first["vol"] == 300
    assert first["amount"] == 3100.0
    assert first["trade_time"] == pd.Timestamp("2024-01-02 15:00:00")
