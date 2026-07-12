from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.runtime.cn_b1s_development_canary import (
    ADAPTIVE_LANES,
    _validate_contract,
    build_admissions,
    fast_group_ic,
    fast_group_spread,
    fast_turnover,
    generate_adaptive_proposals,
    generate_initial_proposals,
    main,
)
from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    _mean_ic,
    _rank_by_group,
    _spread,
    _turnover,
)


REPO = Path(__file__).resolve().parents[1]
CONTRACT_PATH = REPO / "runtime/run_plans/cn_b1s_canary_contract_v1.json"
REPAIRED_CONTRACT_PATH = REPO / "runtime/run_plans/cn_b1s_canary_contract_v2_data_access_repaired.json"


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_repaired_contract_changes_only_data_access_identity() -> None:
    invalidated = _contract()
    repaired = json.loads(REPAIRED_CONTRACT_PATH.read_text(encoding="utf-8"))
    _validate_contract(repaired)

    for key in ("contract_version", "baseline_tag"):
        invalidated.pop(key)
        repaired.pop(key)
    data_access = repaired.pop("data_access_contract")
    assert repaired == invalidated
    assert data_access["development_only_release_hash"] == (
        "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827"
    )
    assert data_access["release_manifest_sha256"] == (
        "3d82f4178277d53f588cc516af3c2c4317046ceac504333a91de5fb716a76255"
    )
    assert data_access["cache_provenance"]["cache_namespace"] == (
        "e623cf26788029ef4598314dbd5ef6f204ec3b134e086194079131b61ef0645b"
    )
    assert data_access["read_ledger_contract"] == {
        "ledger_version": "cn_development_only_read_ledger_v1",
        "requested_data_role": "development",
        "forbidden_file_open_count": 0,
        "forbidden_row_group_read_count": 0,
        "validation_rows_read": 0,
        "holdout_rows_read": 0,
        "forward_rows_read": 0,
    }


def test_b1s_contract_freezes_all_requested_lanes_and_budgets() -> None:
    contract = _contract()
    _validate_contract(contract)

    assert sum(row["proposal"] for row in contract["lane_specs"].values()) == 1344
    assert sum(row["admission"] for row in contract["lane_specs"].values()) == 168
    assert sum(row["strict"] for row in contract["lane_specs"].values()) == 64
    assert len(contract["lane_specs"]) == 15
    assert contract["capability_matrix"]["plate_industry_membership"].startswith("DISABLED")
    assert contract["data_boundary"]["forward_2026_allowed"] is False
    assert contract["adaptation_contract"]["cross_epoch_memory_allowed"] is False


def test_initial_and_adaptive_generation_obey_frozen_1344_budget() -> None:
    contract = _contract()
    initial = generate_initial_proposals(contract)
    for index, row in enumerate(initial):
        row["proxy_reward"] = (index % 97) / 97.0
    adaptive = generate_adaptive_proposals(contract, initial)
    combined = initial + adaptive

    assert len(initial) == 1120
    assert len(adaptive) == 224
    assert len(combined) == 1344
    assert len({row["candidate_id"] for row in combined}) == 1344
    for lane in ADAPTIVE_LANES:
        lane_rows = [row for row in combined if row["lane_id"] == lane]
        assert sum(row["proposal_stage"] == "control" for row in lane_rows) == contract["lane_specs"][lane]["control"]
        assert sum(row["proposal_stage"] == "adaptive" for row in lane_rows) == contract["lane_specs"][lane]["adaptive"]


def test_vectorized_metrics_match_legacy_reference() -> None:
    rng = np.random.default_rng(17)
    times = pd.date_range("2025-04-01 09:30", periods=8, freq="min")
    rows = []
    for time in times:
        for code in range(30):
            rows.append({"code": f"{code:06d}", "trade_time": time})
    frame = pd.DataFrame(rows)
    signal = pd.Series(rng.normal(size=len(frame)))
    label = pd.Series(rng.normal(size=len(frame)))
    signal.iloc[::37] = np.nan
    label.iloc[::41] = np.nan
    signal_rank = _rank_by_group(signal, frame["trade_time"])
    label_rank = _rank_by_group(label, frame["trade_time"])
    groups, _ = pd.factorize(frame["trade_time"], sort=False)

    legacy_ic = _mean_ic(signal_rank, label_rank, frame["trade_time"], 20)
    fast_ic = fast_group_ic(signal_rank.to_numpy(), label_rank.to_numpy(), groups, min_obs=20)
    legacy_spread = _spread(signal_rank, label, frame["trade_time"], 20)
    fast_spread = fast_group_spread(signal_rank.to_numpy(), label.to_numpy(), groups, min_obs=20)
    legacy_turnover = _turnover(signal_rank, frame)
    fast_turn = fast_turnover(signal_rank.to_numpy(), frame)

    assert fast_ic["ic_count"] == legacy_ic["ic_count"]
    assert fast_ic["ic_mean"] == pytest.approx(legacy_ic["ic_mean"], abs=1e-12)
    assert fast_ic["ic_abs_mean"] == pytest.approx(legacy_ic["ic_abs_mean"], abs=1e-12)
    assert fast_spread["spread_count"] == legacy_spread["spread_count"]
    assert fast_spread["spread_mean"] == pytest.approx(legacy_spread["spread_mean"], abs=1e-12)
    assert fast_turn["turnover_count"] == legacy_turnover["turnover_count"]
    assert fast_turn["mean_one_way_turnover"] == pytest.approx(legacy_turnover["mean_one_way_turnover"], abs=1e-12)


def test_three_admission_strategies_respect_fixed_cap() -> None:
    contract = _contract()
    rows = []
    cluster = 1
    for lane, spec in contract["lane_specs"].items():
        for index in range(spec["proposal"]):
            stage = "fixed"
            if lane in ADAPTIVE_LANES:
                stage = "control" if index < spec["control"] else "adaptive"
            rows.append(
                {
                    "candidate_id": f"{lane}_{index}",
                    "lane_id": lane,
                    "canonical_identity": f"canonical_{lane}_{index}",
                    "signal_cluster_id": cluster,
                    "survivor": True,
                    "proxy_reward": 1.0 - cluster / 10000.0,
                    "family_id": f"{lane}:family:{index // 8}",
                    "semantic_bucket": f"{lane}:bucket:{index // 4}",
                    "parent_id": "",
                    "proposal_stage": stage,
                }
            )
            cluster += 1
    admissions = build_admissions(rows, contract)

    assert len(admissions["stratified"]) == 168
    assert len(admissions["global_top_k"]) == 168
    assert len(admissions["hybrid"]) == 168
    assert all(len({row["signal_cluster_id"] for row in selected}) == len(selected) for selected in admissions.values())


def test_b1s_rejects_wrong_authorization_before_inputs(tmp_path: Path) -> None:
    with pytest.raises(PermissionError, match="authorization token"):
        main(
            [
                "--panel-root", str(tmp_path),
                "--release-manifest", str(tmp_path / "release.json"),
                "--cache-root", str(tmp_path / "cache"),
                "--split-manifest", str(tmp_path / "split.csv"),
                "--field-registry", str(tmp_path / "fields.json"),
                "--contract", str(CONTRACT_PATH),
                "--benchmark-registry", str(tmp_path / "bench.json"),
                "--augmentation-summary", str(tmp_path / "aug.json"),
                "--output-root", str(tmp_path / "out"),
                "--authorization", "wrong",
                "--frozen-sha", "0" * 40,
            ]
        )
