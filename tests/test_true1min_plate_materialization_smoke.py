from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from our_system_phase2.runtime.nextgen_true1min_plate_materialization_smoke import main
from our_system_phase2.services.pit_group_sidecar import (
    PITGroupContract,
    membership_content_sha256,
)


def _panel(root: Path, shard: int, codes: list[str]) -> None:
    path = (
        root
        / f"shard_{shard:02d}"
        / "phase3aq_wide_true1min"
        / "canary"
        / "phase3aq_true_1min_formula_canary.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for minute in ("09:31", "09:32"):
        for index, code in enumerate(codes, start=1):
            rows.append(
                {
                    "code": code,
                    "trade_time": pd.Timestamp(f"2025-04-01 {minute}"),
                    "ret_1m": index / 100.0,
                    "amount_yuan": index * 1000.0,
                    "volume": index * 100.0,
                }
            )
    pd.DataFrame(rows).to_parquet(path, index=False, row_group_size=len(rows))


def _membership(path: Path) -> None:
    frame = pd.DataFrame(
        {
            "code": ["000001", "000002", "000003", "000004"],
            "group_id": ["G1", "G1", "G1", "G1"],
            "effective_from": pd.to_datetime(["2025-03-28"] * 4),
            "effective_to": [pd.NaT] * 4,
            "source_observed_at": pd.to_datetime(["2025-03-29"] * 4),
            "source_observed_to": [pd.NaT] * 4,
        }
    )
    frame.to_parquet(path, index=False)
    contract = PITGroupContract("test", "v1", "plate", "multi")
    path.with_suffix(".manifest.json").write_text(
        json.dumps(
            {
                "source_name": "test",
                "source_version": "v1",
                "group_type": "plate",
                "membership_policy": "multi",
                "row_count": len(frame),
                "membership_sha256": membership_content_sha256(frame, contract),
                "survivorship_guard": True,
                "reward_or_performance_used": False,
                "coverage_gate": "PASS",
                "required_start": "2025-03-29",
                "required_end": "2025-12-31",
            }
        ),
        encoding="utf-8",
    )


def test_materialization_smoke_writes_auditable_nonperformance_artifacts(tmp_path: Path) -> None:
    panel_root = tmp_path / "panels"
    _panel(panel_root, 0, ["000001.SZ", "000002.SZ"])
    _panel(panel_root, 1, ["000003.SZ", "000004.SZ"])
    membership = tmp_path / "membership.parquet"
    _membership(membership)
    split = tmp_path / "split.csv"
    pd.DataFrame({"trade_date": ["2025-04-01"], "split": ["train"]}).to_csv(split, index=False)
    output = tmp_path / "output"
    argv = [
        "--panel-root",
        str(panel_root),
        "--membership",
        str(membership),
        "--split-manifest",
        str(split),
        "--output-root",
        str(output),
        "--trade-date",
        "2025-04-01",
        "--row-group-index",
        "0",
        "--minimum-active-peers",
        "1",
    ]

    assert main(argv) == 0

    latest = json.loads(
        (output / "run_records/nextgen_true1min_plate_smoke.latest.json").read_text(
            encoding="utf-8"
        )
    )
    attempt = json.loads(Path(latest["latest_attempt"]).read_text(encoding="utf-8"))
    outputs = {item["purpose"]: Path(item["path"]) for item in attempt["outputs"]}
    group = pd.read_parquet(outputs["sampled group-minute panel"])
    stock = pd.read_parquet(outputs["sampled leave-one-out stock context"])
    diagnostics = json.loads(outputs["structural diagnostics"].read_text(encoding="utf-8"))

    assert len(group) == 2
    assert len(stock) == 8
    assert stock["plate_peer_return_mean"].notna().all()
    assert diagnostics["sampled_code_universe"] is True
    assert diagnostics["formal_performance_search_allowed"] is False
    assert diagnostics["forward_2026_performance_accessed"] is False
    assert attempt["status"] == "COMPLETED"
    assert attempt["decision"] == "N/A_NO_PERFORMANCE_EVALUATION"
    assert attempt["commands"][0].startswith("$env:PYTHONPATH='src'; python app.py ")
    assert attempt["reproducibility"] == "YES_FOR_CONSUMED_SAMPLED_ROWS_WITH_CONTENT_HASHES"
    assert all("selected_rows_sha256" in item for item in diagnostics["panel_inputs"])
    assert all(str(path).startswith(str(output / "artifacts")) for path in outputs.values())
    assert len(attempt["outputs"]) == 4

    first_paths = {Path(item["path"]) for item in attempt["outputs"]}
    assert main(argv) == 0
    second_latest = json.loads(
        (output / "run_records/nextgen_true1min_plate_smoke.latest.json").read_text(
            encoding="utf-8"
        )
    )
    second = json.loads(Path(second_latest["latest_attempt"]).read_text(encoding="utf-8"))
    second_paths = {Path(item["path"]) for item in second["outputs"]}
    assert first_paths.isdisjoint(second_paths)
    assert all(path.exists() for path in first_paths)


def test_materialization_smoke_rejects_non_development_date(tmp_path: Path) -> None:
    split = tmp_path / "split.csv"
    pd.DataFrame({"trade_date": ["2025-10-01"], "split": ["validation"]}).to_csv(
        split, index=False
    )

    with pytest.raises(PermissionError, match="development/train"):
        main(
            [
                "--panel-root",
                str(tmp_path / "panels"),
                "--membership",
                str(tmp_path / "membership.parquet"),
                "--split-manifest",
                str(split),
                "--output-root",
                str(tmp_path / "output"),
                "--trade-date",
                "2025-10-01",
            ]
        )
