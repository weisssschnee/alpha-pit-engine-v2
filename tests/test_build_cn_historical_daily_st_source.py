from pathlib import Path

import pandas as pd

from scripts.build_cn_historical_daily_st_source import build_daily_st_source


def test_historical_daily_st_source_reads_only_identity_and_st(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for date_text in ("2024-01-02", "2024-01-03"):
        pd.DataFrame(
            {
                "日期": [date_text, date_text],
                "代码": ["000001", "600000"],
                "名称": ["平安银行", "ST浦发"],
                "是否ST": ["否", "是"],
                "收盘价": [10.0, 20.0],
            }
        ).to_csv(source / f"{date_text}_fixture.csv", index=False, encoding="utf-8-sig")
    output = tmp_path / "output"
    manifest = build_daily_st_source(
        input_root=source,
        expected_year=2024,
        output_root=output,
        worker_count=1,
    )
    assert manifest["status"] == "HISTORICAL_DAILY_ST_SOURCE_COMPLETE"
    assert manifest["row_count"] == 4
    assert manifest["date_count"] == 2
    assert manifest["performance_columns_read"] == []
    result = pd.read_parquet(output / manifest["artifact"]["path"])
    assert result["is_st"].tolist() == [False, True, False, True]
