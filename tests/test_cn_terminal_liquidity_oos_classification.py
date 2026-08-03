from __future__ import annotations

import json

import pandas as pd

from scripts import build_cn_terminal_liquidity_oos_classification as subject


def test_classifies_positive_blocked_pairs_without_sealed_reads(tmp_path) -> None:
    pair_ids = [f"pair-{index}" for index in range(32)]
    finalists = []
    candidates = []
    replay_pairs = []
    oos_pairs = []
    for index, pair_id in enumerate(pair_ids):
        primary = f"candidate-{index}"
        control = f"candidate-{index}-control"
        finalists.append(
            {
                "pair_id": pair_id,
                "primary_candidate_id": primary,
                "control_candidate_id": control,
                "economic_mechanism_id": f"mechanism-{index}",
                "portfolio_exposure_family_id": f"exposure-{index % 4}",
                "skeleton_id": "skeleton",
                "financial_hypothesis": "hypothesis",
                "source_checkpoint": "checkpoint_001",
                "source_backend": "stock_session",
                "search_score": 0.1,
                "train_mean_one_way_turnover": 0.02,
                "train_regime_positive_share": 0.5,
                "train_regime_worst_day_sortino": -0.3,
            }
        )
        for candidate_id, role in ((primary, "PRIMARY"), (control, "CONTROL")):
            candidates.append(
                {
                    "candidate_id": candidate_id,
                    "pair_id": pair_id,
                    "pair_member_role": role,
                    "candidate_replay_status": "CANDIDATE_REPLAY_BLOCKED",
                    "blocker_code": "FINAL_SESSION_UNLIQUIDATED_HOLDINGS",
                    "result_payload_json": json.dumps(
                        {"remaining_holdings": ["000545", f"00{index:04d}"]}
                    ),
                }
            )
        replay_pairs.append(
            {"pair_id": pair_id, "a_share_replay_status": "PAIR_REPLAY_BLOCKED"}
        )
        oos_pairs.append(
            {
                "pair_id": pair_id,
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "primary_candidate_id": primary,
                "control_candidate_id": control,
                "a_share_replay_status": "PAIR_REPLAY_BLOCKED",
                "validation_pair_status": "PAIR_EVALUATED",
                "interstage_filter_applied": False,
                "oos_positive_transfer": index < 21,
                "validation_search_score": 0.2 if index < 21 else -0.1,
                "validation_mean_one_way_turnover": 0.04,
                "validation_regime_positive_share": 0.0,
                "validation_regime_worst_day_sortino": -0.7,
                "validation_worst_horizon_day_sortino": 0.1,
            }
        )

    paths = {}
    for name, rows in (
        ("candidate", candidates),
        ("replay", replay_pairs),
        ("oos", oos_pairs),
        ("finalists", finalists),
    ):
        path = tmp_path / f"{name}.parquet"
        pd.DataFrame(rows).to_parquet(path, index=False)
        paths[name] = path

    result = subject.classify(
        candidate_replay_results=paths["candidate"],
        pair_replay_results=paths["replay"],
        oos_pair_results=paths["oos"],
        finalist_pairs=paths["finalists"],
        output_root=tmp_path / "output",
    )
    assert result["oos_positive_blocked_pair_count"] == 21
    assert result["unique_economic_mechanism_count"] == 21
    assert result["new_financial_data_reads"] == 0
    assert result["new_validation_data_reads"] == 0
    closure = json.loads(
        (tmp_path / "output" / "CLASSIFICATION_COMPLETE.json").read_text()
    )
    claimed = closure.pop("manifest_body_sha256")
    assert claimed == subject._stable_hash(closure)


def test_mark_to_market_wrapper_uses_shared_validation_lease() -> None:
    script = (
        subject.Path(__file__).parents[1]
        / "scripts"
        / "run_cn_finalist_mark_to_market_replay_77o.ps1"
    ).read_text(encoding="utf-8")
    assert "VALIDATION_DUAL_8" in script
    assert "manage_cn_node_resource_lease.py" in script
    assert "CN_NODE_CPU_ENTITLEMENT" in script
    assert "run_cn_finalist_mark_to_market_replay.py" in script
    assert "validation_reads = 0" in script


def test_continuity_contract_uses_only_remaining_high_density_supply() -> None:
    contract_path = (
        subject.Path(__file__).parents[1]
        / "runtime"
        / "run_plans"
        / "cn_terminal_liquidity_search_continuity_v1_contract.json"
    )
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    lane = contract["search_lane"]
    assert lane["maximum_formal_fresh_exact_asks"] == 184
    assert lane["fixed_route_formal_asks_per_checkpoint"] == {
        "SLOW_TEMPORAL_CHANGE": 144,
        "FIRSTN_PATH": 0,
        "SLOW_CROSS_SECTIONAL_LEVEL": 40,
        "MARKET_REGIME_CONDITION": 0,
        "DISCLOSURE_EVENT": 0,
    }
    assert lane["supply_basis"]["SLOW_TEMPORAL_CHANGE"][
        "required_at_1_20_margin"
    ] <= lane["supply_basis"]["SLOW_TEMPORAL_CHANGE"]["remaining_exact"]
    assert lane["supply_basis"]["SLOW_CROSS_SECTIONAL_LEVEL"][
        "required_at_1_20_margin"
    ] <= lane["supply_basis"]["SLOW_CROSS_SECTIONAL_LEVEL"][
        "remaining_exact"
    ]
    assert lane["supply_basis"]["FIRSTN_PATH"]["formal_asks"] == 0
