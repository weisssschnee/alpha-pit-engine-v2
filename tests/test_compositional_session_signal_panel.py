from __future__ import annotations

import hashlib
import json
from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest

import scripts.prepare_cn_compositional_session_signal_panel as session_panel_script
from scripts.prepare_cn_compositional_session_signal_panel import (
    _load_chip_context,
    prepare_sidecar_base,
)
from our_system_phase2.services.chip_sidecar import load_chip_context
from our_system_phase2.services.compositional_session_signal_panel import (
    attach_coordinate_row_indices,
    build_full_session_coordinate_rows,
    build_session_coordinate_rows,
    field_partition,
)


def test_session_coordinates_collapse_intraday_slots_without_mixing_ab() -> None:
    active = [
        {
            "coordinate_set": coordinate_set,
            "code": "000001.SZ",
            "trade_date": "2025-01-02",
            "trade_time": f"2025-01-02 {clock}",
        }
        for coordinate_set in ("A", "B")
        for clock in ("09:35:00", "14:30:00")
    ]
    rows = build_session_coordinate_rows(
        active,
        available_keys={("000001.SZ", pd.Timestamp("2025-01-02"))},
    )

    assert len(rows) == 2
    assert {row["coordinate_set"] for row in rows} == {"A", "B"}
    assert all(row["trade_time"].startswith("2025-01-02T15:00:00") for row in rows)
    assert all(row["data_role"] == "development" for row in rows)
    assert all(row["row_index"] == 0 for row in rows)


def test_coordinate_indices_and_field_partitions_are_deterministic() -> None:
    panel = pd.DataFrame(
        {
            "code": ["000001.SZ", "000002.SZ"],
            "trade_time": pd.to_datetime(["2025-01-02 15:00", "2025-01-02 15:00"]),
        }
    )
    rows = [
        {"code": "000002.SZ", "trade_time": "2025-01-02T15:00:00", "coordinate_set": "A"},
        {"code": "000003.SZ", "trade_time": "2025-01-02T15:00:00", "coordinate_set": "B"},
    ]

    indexed = attach_coordinate_row_indices(rows, panel)

    assert [row["row_index"] for row in indexed] == [1, -1]
    assert field_partition("fund_example", 7) == field_partition("fund_example", 7)
    assert 0 <= field_partition("fund_example", 7) < 7


def test_full_session_coordinates_use_every_development_date_once_per_stock() -> None:
    active = [
        {
            "coordinate_set": "A",
            "code": code,
            "trade_date": "2025-01-02",
            "stock_coverage_interval": "q1",
            "activation_density_bucket": "mid",
        }
        for code in ("000001.SZ", "000002.SZ")
    ]
    sessions = pd.DatetimeIndex(pd.date_range("2025-01-02", periods=6, freq="B"))
    available = {(code, session) for code in ("000001.SZ", "000002.SZ") for session in sessions}

    rows = build_full_session_coordinate_rows(
        active, sessions=sessions, available_keys=available
    )

    assert len(rows) == 12
    assert {row["coordinate_set"] for row in rows} == {"A", "B"}
    assert all(row["row_index"] == 0 for row in rows)
    assert all(
        row["coordinate_selection_policy"]
        == "ALL_DEVELOPMENT_SESSIONS_MONTHLY_ALTERNATING_AB"
        for row in rows
    )
    assert len({(row["code"], row["trade_date"]) for row in rows}) == 12


def test_session_panel_loads_only_requested_development_chip_context(tmp_path) -> None:
    root = tmp_path / "chip"
    (root / "shards").mkdir(parents=True)
    (root / "chip_sidecar_manifest_v1.json").write_text(
        json.dumps(
            {
                "sidecar_version": "nextgen_dark_chip_sidecar_v3",
                "shard_count": 1,
                "forward_2026_performance_accessed": False,
                "sealed_2026_values_converted_or_used": False,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "code": ["000001", "000001", "000002"],
            "source_session": pd.to_datetime(
                ["2025-01-02", "2025-01-06", "2025-01-02"]
            ),
            "source_observed_at": pd.to_datetime(
                ["2025-01-03", "2025-01-07", "2025-01-03"]
            ),
            "chip_cost_p50": [10.0, 11.0, 20.0],
        }
    ).to_parquet(root / "shards" / "chip_00000.parquet", index=False)

    frame, evidence = _load_chip_context(
        root,
        allowed_codes={"000001.SZ"},
        fields=["chip_cost_p50"],
        maximum_observable_time="2025-01-05T15:00:00",
    )

    assert frame[["code", "chip_cost_p50"]].to_dict("records") == [
        {"code": "000001", "chip_cost_p50": 10.0}
    ]
    direct, direct_evidence = load_chip_context(
        root,
        allowed_codes={"000001"},
        fields=["chip_cost_p50"],
        maximum_observable_time="2025-01-05T15:00:00",
    )
    assert direct.equals(frame)
    assert direct_evidence == evidence
    assert evidence["loaded_row_count"] == 1
    assert evidence["shard_count"] == 1


def _sidecar_base_fixture(
    tmp_path: Path,
    *,
    shard_rows: list[list[tuple[str, str]]] | None = None,
    access: dict[str, object] | None = None,
    shard_access: dict[str, object] | None = None,
) -> Namespace:
    split = tmp_path / "split.csv"
    pd.DataFrame(
        {
            "trade_date": ["2024-01-02", "2024-01-03"],
            "split": ["train", "train"],
        }
    ).to_csv(split, index=False)
    rows = shard_rows or [
        [("000001.SZ", "2024-01-02 15:00"), ("000001.SZ", "2024-01-03 15:00")],
        [("000002.SZ", "2024-01-02 15:00"), ("000002.SZ", "2024-01-03 15:00")],
    ]
    sidecar = tmp_path / "sidecar"
    sidecar.mkdir()
    records = []
    for shard_index, values in enumerate(rows):
        path = sidecar / f"shard_{shard_index:02d}.parquet"
        pd.DataFrame(
            {
                "code": [value[0] for value in values],
                "trade_time": pd.to_datetime([value[1] for value in values]),
                "close": range(len(values)),
                "fwd_ret_30m": range(len(values)),
            }
        ).to_parquet(path, index=False)
        records.append(
            {
                "shard_index": shard_index,
                "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
                "data_role": "development_train_only",
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
                **(shard_access or {}),
                "output": {
                    "path": str(path.resolve()),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "bytes": path.stat().st_size,
                },
                "source": {"path": f"source_{shard_index}.parquet", "rows": len(values), "sha256": "a" * 64},
            }
        )
    access_values = {
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        **(access or {}),
    }
    (sidecar / "CN_SESSION_SIDECAR_AUGMENTATION_MANIFEST.json").write_text(
        json.dumps(
            {
                "schema_version": "cn_phase3cm_session_sidecar_augmentation_manifest_v1",
                "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
                "output_root": str(sidecar.resolve()),
                "source_root": "synthetic_source",
                "row_count": sum(len(values) for values in rows),
                "shard_count": len(rows),
                "shards": records,
                **access_values,
            }
        ),
        encoding="utf-8",
    )
    return Namespace(
        session_sidecar_root=sidecar,
        split_manifest=split,
        expected_shard_count=len(rows),
        output_root=tmp_path / "output",
    )


def test_prepare_sidecar_base_reads_only_coordinates_and_publishes_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _sidecar_base_fixture(tmp_path)
    original_read = session_panel_script.pd.read_parquet
    selected_columns: list[list[str]] = []

    def observed_read(path: Path, *, columns: list[str]) -> pd.DataFrame:
        selected_columns.append(columns)
        return original_read(path, columns=columns)

    monkeypatch.setattr(session_panel_script.pd, "read_parquet", observed_read)

    assert prepare_sidecar_base(args) == 0

    base = original_read(args.output_root / "session_signal_base_panel.parquet")
    manifest = json.loads(
        (args.output_root / "session_signal_base_manifest.json").read_text(encoding="utf-8")
    )
    assert selected_columns == [["code", "trade_time"], ["code", "trade_time"]]
    assert list(base.columns) == ["code", "trade_time", "session_time", "date"]
    assert len(base) == 4
    assert base["trade_time"].equals(base["session_time"])
    assert manifest["status"] == "FULL_DEVELOPMENT_SESSION_BASE_PREPARED"
    assert manifest["base_panel_rows"] == 4
    assert not list(args.output_root.glob("*.tmp"))
    assert not list(args.output_root.glob(".*.tmp"))


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("validation_reads", 1),
        ("holdout_reads", "0"),
        ("forward_2026_reads", False),
    ],
)
def test_prepare_sidecar_base_rejects_nonzero_or_untyped_access(
    tmp_path: Path, key: str, value: object
) -> None:
    args = _sidecar_base_fixture(tmp_path, access={key: value})

    with pytest.raises(PermissionError, match="exact integer zero"):
        prepare_sidecar_base(args)


def test_prepare_sidecar_base_rejects_restricted_shard_access(tmp_path: Path) -> None:
    args = _sidecar_base_fixture(tmp_path, shard_access={"holdout_reads": 1})

    with pytest.raises(PermissionError, match="exact integer zero"):
        prepare_sidecar_base(args)


def test_prepare_sidecar_base_rejects_split_date_drift(tmp_path: Path) -> None:
    args = _sidecar_base_fixture(
        tmp_path,
        shard_rows=[
            [("000001.SZ", "2024-01-02 15:00")],
            [("000002.SZ", "2024-01-04 15:00")],
        ],
    )

    with pytest.raises(ValueError, match="date set differs"):
        prepare_sidecar_base(args)


def test_prepare_sidecar_base_rejects_duplicate_coordinates(tmp_path: Path) -> None:
    duplicate = ("000001.SZ", "2024-01-02 15:00")
    args = _sidecar_base_fixture(
        tmp_path,
        shard_rows=[
            [duplicate, ("000001.SZ", "2024-01-03 15:00")],
            [duplicate, ("000002.SZ", "2024-01-03 15:00")],
        ],
    )

    with pytest.raises(ValueError, match="duplicate stock-session coordinates"):
        prepare_sidecar_base(args)


def test_prepare_sidecar_base_cleans_staging_on_publish_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _sidecar_base_fixture(tmp_path)
    original_replace = session_panel_script.os.replace

    def fail_manifest_publish(source: Path, destination: Path) -> None:
        if Path(destination).name == "session_signal_base_manifest.json":
            raise OSError("synthetic manifest publish failure")
        original_replace(source, destination)

    monkeypatch.setattr(session_panel_script.os, "replace", fail_manifest_publish)

    with pytest.raises(OSError, match="synthetic manifest publish"):
        prepare_sidecar_base(args)
    assert not (args.output_root / "session_signal_base_panel.parquet").exists()
    assert not (args.output_root / "session_signal_base_manifest.json").exists()
    assert not list(args.output_root.iterdir())
