import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.split_boundary_label_purity import (
    audit_split_boundary_label_purity,
)
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry
from scripts.run_cn_phase3cm_streaming_qualification import (
    _evaluation_calendar,
    _verify_split_boundary_purity,
)
from scripts.build_cn_phase3cm_forward_label_sidecars import (
    _split_dates as _label_split_dates,
)
from scripts.build_cn_phase3cm_time_major_sidecar import (
    _split_dates as _field_split_dates,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = REPO / "runtime/field_registry/cn_unified_capability_registry_v3_20260717/unified_capability_registry.json"


def test_streaming_calendar_supports_sequential_report_only_validation(tmp_path: Path) -> None:
    split_path = tmp_path / "split.csv"
    split_path.write_text(
        "trade_date,split,optimizer_usage\n"
        "2025-01-02,train,allowed\n"
        "2025-07-08,validation,report_only\n"
        "2025-10-20,holdout,report_only\n",
        encoding="utf-8",
    )
    binding = {"split_manifest_hash": hashlib.sha256(split_path.read_bytes()).hexdigest()}

    assert _evaluation_calendar(split_path, binding, evaluation_role="train") == (
        "2025-01-02",
    )
    assert _evaluation_calendar(split_path, binding, evaluation_role="validation") == (
        "2025-07-08",
    )
    assert _field_split_dates(
        split_path,
        binding["split_manifest_hash"],
        evaluation_role="validation",
    ) == ("2025-07-08",)
    assert _label_split_dates(
        split_path,
        binding["split_manifest_hash"],
        evaluation_role="validation",
    ) == ("2025-07-08",)
    assert _evaluation_calendar(
        split_path,
        binding,
        evaluation_role="holdout",
    ) == ("2025-10-20",)
    assert _field_split_dates(
        split_path,
        binding["split_manifest_hash"],
        evaluation_role="holdout",
    ) == ("2025-10-20",)
    assert _label_split_dates(
        split_path,
        binding["split_manifest_hash"],
        evaluation_role="holdout",
    ) == ("2025-10-20",)


def test_train_only_terminal_nulls_are_bound_as_purged_crossings(tmp_path: Path) -> None:
    split_path = tmp_path / "split.csv"
    split_path.write_text(
        "trade_date,split,optimizer_usage\n"
        "2025-01-02,train,allowed\n"
        "2025-01-03,train,allowed\n"
        "2025-01-06,validation,forbidden\n"
        "2025-01-07,holdout,forbidden\n",
        encoding="utf-8",
    )
    split = FixedSplitAuthority.read(split_path, require_official=False)
    roots = {}
    for backend in ("active_bar", "stock_session"):
        root = tmp_path / backend
        root.mkdir()
        pd.DataFrame(
            {
                "trade_time": pd.to_datetime(["2025-01-02", "2025-01-03"]),
                "code": ["000001", "000001"],
                "source_shard": [0, 0],
                "source_row_identity": [0, 1],
                "duplicate_ordinal": [0, 0],
                "fwd_ret_1m": [0.01, None],
                "fwd_ret_5m": [None, None],
                "fwd_ret_15m": [None, None],
                "fwd_ret_30m": [None, None],
            }
        ).to_parquet(root / "shard_00.parquet", index=False)
        (root / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json").write_text(
            json.dumps(
                {
                    "status": "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY",
                    "data_role": "development_train_only",
                    "split_manifest_hash": split.manifest_hash,
                    "eligible_train_date_count": 2,
                    "horizons": [1, 5, 15, 30],
                    "label_sidecar_identity": backend,
                }
            ),
            encoding="utf-8",
        )
        roots[backend] = root

    result = audit_split_boundary_label_purity(
        split=split,
        registry=UnifiedCapabilityRegistry.read(REGISTRY),
        label_roots=roots,
    )

    assert result["status"] == "PASS"
    assert result["train_last_date"] == "2025-01-03"
    assert result["validation_first_date"] == "2025-01-06"
    assert all(
        row["crossing_count"] == row["purged_coordinate_count"] == 2
        for row in result["routes"]
    )
    assert all(row["retained_crossing_count"] == 0 for row in result["routes"])
    assert result["enforcement_contract"] == (
        "FINITE_SIGNAL_AND_LABEL_INTERSECTION_IN_BATCHED_PORTFOLIO_KERNEL"
    )
    assert all(
        row["primary_control_source_maturity_future_extension"] == 0
        for row in result["routes"]
    )
    assert (
        result["validation_reads"]
        == result["holdout_reads"]
        == result["forward_2026_reads"]
        == 0
    )

    purity_path = tmp_path / "split_boundary_purity.json"
    payload = json.dumps(result, sort_keys=True)
    purity_path.write_text(payload, encoding="utf-8")
    binding = {
        "split_manifest_hash": split.manifest_hash,
        "retained_label_crossing_count": 0,
        "split_boundary_purity": {
            "sha256": hashlib.sha256(purity_path.read_bytes()).hexdigest()
        },
    }
    assert _verify_split_boundary_purity(purity_path, binding=binding)["status"] == "PASS"
    purity_path.write_text(payload.replace('"status": "PASS"', '"status": "FAIL"'), encoding="utf-8")
    with pytest.raises(RuntimeError, match="split purity hash"):
        _verify_split_boundary_purity(purity_path, binding=binding)
