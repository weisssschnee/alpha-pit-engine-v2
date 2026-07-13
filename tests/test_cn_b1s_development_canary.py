from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.runtime.cn_b1s_development_canary import (
    ADAPTIVE_LANES,
    apply_strict_priority_layer,
    _select_seed_set,
    _validate_contract,
    _materialization_input_columns,
    _bottleneck,
    build_admissions,
    apply_typed_gate,
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
from our_system_phase2.services.feature_state_fabric import FieldRegistry
from our_system_phase2.services.strict_priority_selector import StrictPriorityModel


REPO = Path(__file__).resolve().parents[1]
CONTRACT_PATH = REPO / "runtime/run_plans/cn_b1s_canary_contract_v1.json"
REPAIRED_CONTRACT_PATH = REPO / "runtime/run_plans/cn_b1s_canary_contract_v2_data_access_repaired.json"
CAPABILITY_CONTRACT_PATH = REPO / "runtime/run_plans/cn_generator_capability_canary_sprint1_v1.json"
SPRINT2_CONTRACT_PATH = REPO / "runtime/run_plans/cn_sprint2_repair_capability_canary_v1.json"
EPOCH_A_CONTRACT_PATH = REPO / "runtime/run_plans/cn_generator_epoch_a_sprint1_v1.json"


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_epoch_a_contract_freezes_two_disjoint_seed_sets_and_combined_budget() -> None:
    contract = json.loads(EPOCH_A_CONTRACT_PATH.read_text(encoding="utf-8"))
    assert contract["epoch_budget"] == {
        "seed_count": 2,
        "proposal_total": 16384,
        "admission_total": 2048,
        "strict_eval_total": 512,
    }
    assert sum(int(spec["proposal"]) for spec in contract["lane_specs"].values()) == 8192
    assert sum(int(spec["admission"]) for spec in contract["lane_specs"].values()) == 1024
    assert sum(int(spec["strict"]) for spec in contract["lane_specs"].values()) == 256
    assert set(contract["seed_sets"]) == {"seed_a", "seed_b"}
    assert set(contract["seed_sets"]["seed_a"].values()).isdisjoint(
        set(contract["seed_sets"]["seed_b"].values())
    )


def test_seed_set_selection_is_frozen_and_does_not_mutate_contract() -> None:
    contract = json.loads(EPOCH_A_CONTRACT_PATH.read_text(encoding="utf-8"))
    original = dict(contract["seeds"])
    selected = _select_seed_set(contract, "seed_b")
    assert selected["seeds"] == contract["seed_sets"]["seed_b"]
    assert selected["selected_seed_set"] == "seed_b"
    assert contract["seeds"] == original
    with pytest.raises(ValueError, match="requires --seed-set"):
        _select_seed_set(contract, None)


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
        "655be0b04e8ebd0b5260cdde1e6365a08e0270a5feb5e7829b863ef60e80e654"
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


def test_canary_reads_registry_declared_pit_clock_dependencies() -> None:
    registry = FieldRegistry.read(REPO / "runtime/field_registry/nextgen_dark_field_registry_v2.json")
    columns = _materialization_input_columns(
        registry,
        ["ctx_billboard_deal_net_ratio", "evt_uplimit_active", "close"],
    )

    assert set(columns) == {
        "ctx_billboard_deal_net_ratio",
        "evt_uplimit_active",
        "close",
        "ctx_source_session_upper_bound",
        "signal_time",
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


def test_generator_capability_contract_is_inside_authorized_budget_and_uses_pareto() -> None:
    contract = json.loads(CAPABILITY_CONTRACT_PATH.read_text(encoding="utf-8"))
    _validate_contract(contract)
    assert sum(row["proposal"] for row in contract["lane_specs"].values()) == 4144
    assert 4096 <= contract["budgets"]["proposal_total"] <= 8192
    assert 128 <= contract["budgets"]["strict_eval_total"] <= 256
    assert contract["strict_contract"]["selection_source"] == "pareto_hybrid"
    assert contract["development_objective"]["formula"].startswith("hard gates then Pareto")
    assert contract["capability_matrix"]["down_limit_event_history"].startswith("DISABLED")
    assert contract["capability_matrix"]["event_conditioned"].endswith("CONTROL_ONLY")


def test_sprint2_repair_contract_freezes_shared_backbone_two_seeds_and_total_budget() -> None:
    contract = json.loads(SPRINT2_CONTRACT_PATH.read_text(encoding="utf-8"))

    assert contract["budgets"]["proposal_total"] == 4096
    assert contract["budgets"]["strict_eval_total"] == 128
    assert sum(spec["proposal"] for spec in contract["lane_specs"].values()) == 4096
    assert sum(spec["strict"] for spec in contract["lane_specs"].values()) == 128
    assert set(contract["seed_sets"]) == {"seed_a", "seed_b"}
    assert contract["seed_sets"]["seed_a"]["seeds"] == contract["seed_sets"]["seed_b"]["seeds"]
    assert contract["seed_sets"]["seed_a"]["adaptive_seeds"]["rx_ucb"] != contract["seed_sets"]["seed_b"]["adaptive_seeds"]["rx_ucb"]
    assert contract["adaptation_contract"]["only_primary_adaptive"] == "rx_ucb"
    assert contract["strict_contract"]["selection_source"] == "quality_diversity_hybrid"
    assert contract["capability_matrix"]["event_conditioned"] == "ENABLED_ENTRY_PATH_ONLY_EXIT_RESEAL_DISABLED"


def test_sprint2_shared_backbone_is_identical_and_adaptive_expansion_is_seed_isolated() -> None:
    source = json.loads(SPRINT2_CONTRACT_PATH.read_text(encoding="utf-8"))
    seed_a = _select_seed_set(source, "seed_a")
    seed_b = _select_seed_set(source, "seed_b")
    initial_a = generate_initial_proposals(seed_a)
    initial_b = generate_initial_proposals(seed_b)

    assert [row["exact_identity"] for row in initial_a] == [row["exact_identity"] for row in initial_b]
    for index, row in enumerate(initial_a):
        row.update(proxy_reward=(index % 211) / 211.0, signal_cluster_id=index + 1, strict_priority_score=(index % 101) / 101.0)
    for index, row in enumerate(initial_b):
        row.update(proxy_reward=(index % 211) / 211.0, signal_cluster_id=index + 1, strict_priority_score=(index % 101) / 101.0)
    adaptive_a = generate_adaptive_proposals(seed_a, initial_a)
    adaptive_b = generate_adaptive_proposals(seed_b, initial_b)

    assert len(initial_a) + len(adaptive_a) == 4096
    assert len(initial_b) + len(adaptive_b) == 4096
    rx_a = {row["exact_identity"] for row in adaptive_a if row["lane_id"] == "rx_ucb"}
    rx_b = {row["exact_identity"] for row in adaptive_b if row["lane_id"] == "rx_ucb"}
    assert rx_a != rx_b
    assert all(str(row["seed_statistics_scope"]).startswith("current_run_only:") for row in adaptive_a if row["lane_id"] == "rx_ucb")


def test_strict_priority_layer_renames_evaluability_and_contracts_to_top_decile() -> None:
    contract = json.loads(SPRINT2_CONTRACT_PATH.read_text(encoding="utf-8"))
    model = StrictPriorityModel.from_artifact(
        json.loads((REPO / contract["strict_priority_contract"]["model_path"]).read_text(encoding="utf-8"))
    )
    rows = []
    for index in range(20):
        rows.append(
            {
                "candidate_id": f"candidate_{index}",
                "lane_id": "event_conditioned" if index == 0 else "static_cross_sectional",
                "exact_identity": f"exact_{index}",
                "legal": True,
                "materialized": True,
                "survivor": True,
                "proxy_finite_ratio": 1.0,
                "proxy_signal_unique": 2 if index == 0 else 20 + index,
                "proxy_ic_count": 60,
                "proxy_reward": index / 100.0,
                "proxy_ic_abs_lcb95": index / 100.0,
                "proxy_worst_time_block_abs_ic": index / 200.0,
                "proxy_time_block_stability": 0.8,
                "proxy_turnover": 0.2,
                "proxy_signal_concentration": 0.1,
                "complexity": 4,
                "primitive_family": "test",
                "hypothesis_arm": "test",
                "proposal_stage": "fixed",
                "field_family": "raw_1min",
            }
        )

    apply_strict_priority_layer(rows, contract, model)

    assert all(row["development_eligible"] for row in rows)
    assert sum(row["strict_priority_eligible"] for row in rows) == 2
    assert all("legacy_survivor" in row for row in rows)


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


def test_capability_generation_fills_budget_with_high_exact_identity_rate() -> None:
    contract = json.loads(CAPABILITY_CONTRACT_PATH.read_text(encoding="utf-8"))
    initial = generate_initial_proposals(contract)
    for index, row in enumerate(initial):
        row["proxy_reward"] = (index % 211) / 211.0
        row["signal_cluster_id"] = index + 1
    adaptive = generate_adaptive_proposals(contract, initial)
    combined = initial + adaptive
    apply_typed_gate(combined)

    assert len(combined) == 4144
    assert len({row["candidate_id"] for row in combined}) == 4144
    assert len({row["exact_identity"] for row in combined}) / len(combined) >= 0.80
    assert sum(bool(row["legal"]) for row in combined) / len(combined) >= 0.90
    assert len({row["exact_identity"] for row in combined if row["lane_id"] == "llm_proposal_repair"}) >= 220
    rx_control_arms = {
        (row["hypothesis_arm"], row["primitive_family"])
        for row in initial if row["lane_id"] == "rx_ucb"
    }
    rx_adaptive = [row for row in adaptive if row["lane_id"] == "rx_ucb"]
    rx_adaptive_arms = {(row["hypothesis_arm"], row["primitive_family"]) for row in rx_adaptive}
    assert len(rx_adaptive_arms) <= len(rx_control_arms)
    assert all(row["focused_arm_count"] == len(rx_adaptive_arms) for row in rx_adaptive)
    assert all(row["arm_hierarchy"].count("|") == 4 for row in rx_adaptive)
    assert all(row["fresh_exploration_floor"] == pytest.approx(0.20) for row in rx_adaptive)
    assert all(row["delayed_feedback_source"] == "frozen_cross_fitted_strict_priority_model" for row in rx_adaptive)


def test_bottleneck_reports_cost_and_four_time_blocks_when_strict_metrics_supply_them() -> None:
    funnel = [{"proposal_count": 10, "legal_count": 9, "survivor_count": 5}]
    admissions = {
        "hybrid": [{"signal_cluster_id": 1}],
        "global_top_k": [{"signal_cluster_id": 1}],
    }
    strict = pd.DataFrame(
        [
            {
                "horizon_bars": 5,
                "ic_mean": 0.1,
                "proxy_reward": 0.1,
                "cost_adjusted_abs_ic": 0.09,
                "mean_one_way_turnover": 0.2,
                "proxy_time_block_count": 4,
            }
            for _ in range(5)
        ]
    )
    result = _bottleneck(funnel, admissions, strict)
    assert result["cost_model_available"] is True
    assert result["stability_window_count"] == 4


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
    assert fast_ic["ic_standard_error"] is not None
    assert 0.0 <= fast_ic["ic_abs_lcb95"] <= abs(fast_ic["ic_mean"])
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
                    "exact_identity": f"exact_{lane}_{index}",
                    "signal_cluster_id": cluster,
                    "survivor": True,
                    "legal": True,
                    "proxy_reward": 1.0 - cluster / 10000.0,
                    "proxy_ic_mean": 0.2 - cluster / 100000.0,
                    "proxy_ic_abs_lcb95": 0.15 - cluster / 200000.0,
                    "proxy_turnover": 0.2,
                    "proxy_worst_time_block_abs_ic": 0.08,
                    "proxy_time_block_stability": 0.8,
                    "proxy_signal_concentration": 0.1,
                    "complexity": 6,
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
    assert len(admissions["pareto_hybrid"]) == 168
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
                "--strict-priority-model", str(tmp_path / "selector.json"),
                "--output-root", str(tmp_path / "out"),
                "--authorization", "wrong",
                "--frozen-sha", "0" * 40,
            ]
        )
