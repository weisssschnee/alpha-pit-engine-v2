from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.run_cn_hybrid_policy_report_only_validation import (
    ARMS,
    ROUTE_QUOTAS_PER_ARM,
    _selection_payload,
    _stable_hash,
    _validation_report,
)


REPO = Path(__file__).resolve().parents[1]
AUTHORIZATION = (
    REPO
    / "runtime/run_plans/cn_hybrid_policy_report_only_validation_20260729.json"
)
LAUNCHER = REPO / "scripts/run_cn_hybrid_policy_report_only_validation_77o.ps1"


def test_authorization_keeps_hybrid_policy_fixed_and_validation_report_only() -> None:
    authorization = json.loads(AUTHORIZATION.read_text(encoding="utf-8"))
    assert authorization["status"] == "ACTIVE_REPORT_ONLY_VALIDATION_AUTHORIZATION"
    assert authorization["accepted_development_policy"] == "HYBRID_TPE_AVAILABILITY"
    assert authorization["policy_selection_reopened"] is False
    assert authorization["arm_pair_counts"] == {arm: 128 for arm in ARMS}
    assert authorization["route_quotas_per_arm"] == ROUTE_QUOTAS_PER_ARM
    assert authorization["evaluation_role"] == "validation"
    assert authorization["validation_usage"] == "report_only"
    assert authorization["feedback_write"] == "FORBIDDEN"
    assert authorization["scheduler_write"] == "FORBIDDEN"
    assert authorization["archive_write"] == "FORBIDDEN"
    assert authorization["promotion"] == "FORBIDDEN"
    assert authorization["holdout_reads"] == 0
    assert authorization["forward_2026_reads"] == 0


def test_selection_is_arm_route_stratified_and_train_ranked() -> None:
    policy_rows = []
    observation_rows = []
    ordinal = 0
    for arm in ARMS:
        for route_id, quota in ROUTE_QUOTAS_PER_ARM.items():
            for rank in range(quota + 2):
                ordinal += 1
                pair_id = f"pair.{ordinal:04d}"
                score = float(quota + 2 - rank)
                policy_rows.append(
                    {
                        "checkpoint": "checkpoint_001",
                        "route_id": route_id,
                        "proposal_id": f"proposal.{ordinal:04d}",
                        "pair_id": pair_id,
                        "intention_to_treat_arm": arm,
                        "pair_evaluated": True,
                        "search_score": score,
                        "matched_train_increment": score + 1,
                        "productive_candidate": score > 0,
                        "portfolio_behavior_family_id": f"family.{ordinal:04d}",
                    }
                )
                observation_rows.append(
                    {
                        "pair_id": pair_id,
                        "pair_evaluation_status": "PAIR_EVALUATED",
                        "primary_composite_reward": score + 2,
                        "matched_train_increment": score + 1,
                        "primary_standalone_train_reward_decision": (
                            "TRAIN_REWARD_FOLLOWUP_READY"
                        ),
                    }
                )
    selection = _selection_payload(
        policy_rows=pd.DataFrame(policy_rows),
        observation_rows=pd.DataFrame(observation_rows),
    )
    assert len(selection) == 256
    counts = pd.DataFrame(selection).groupby(
        ["intention_to_treat_arm", "route_id"]
    ).size()
    assert {
        (arm, route): int(counts.loc[(arm, route)])
        for arm in ARMS
        for route in ROUTE_QUOTAS_PER_ARM
    } == {
        (arm, route): quota
        for arm in ARMS
        for route, quota in ROUTE_QUOTAS_PER_ARM.items()
    }
    assert len({row["pair_id"] for row in selection}) == 256


def test_validation_report_is_diagnostic_and_never_reopens_policy() -> None:
    freeze = {
        "candidates": [
            {
                "pair_id": "pair.1",
                "candidate_id": "primary.1",
                "control_candidate_id": "control.1",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "intention_to_treat_arm": "HYBRID_TPE_AVAILABILITY",
                "search_score": 0.2,
                "productive_candidate": True,
            },
            {
                "pair_id": "pair.2",
                "candidate_id": "primary.2",
                "control_candidate_id": "control.2",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "intention_to_treat_arm": "AVAILABILITY_AWARE_UNIFORM",
                "search_score": 0.1,
                "productive_candidate": True,
            },
        ]
    }
    result = {
        "candidate_rewards": [
            {
                "candidate_id": candidate_id,
                "validation_report_metric": metric,
                "validation_day_sortino": metric,
                "train_worst_horizon_day_sortino": metric,
                "train_mean_one_way_turnover": 0.1,
                "train_regime_positive_share": 0.5,
                "train_regime_worst_day_sortino": -0.2,
            }
            for candidate_id, metric in (
                ("primary.1", 0.3),
                ("primary.2", -0.1),
            )
        ],
        "pair_results": [
            {
                "pair_id": "pair.1",
                "pair_evaluation_status": "PAIR_EVALUATED",
                "pair_evaluation_blockers": "",
                "pair_support_count": 100,
                "pair_validation_report_metric": 0.2,
                "control_validation_report_metric": 0.1,
            },
            {
                "pair_id": "pair.2",
                "pair_evaluation_status": "PAIR_EVALUATED",
                "pair_evaluation_blockers": "",
                "pair_support_count": 100,
                "pair_validation_report_metric": -0.2,
                "control_validation_report_metric": 0.1,
            },
        ],
    }
    rows, report = _validation_report(freeze=freeze, results=[result])
    assert rows[0]["oos_positive_transfer"] is True
    assert rows[1]["oos_positive_transfer"] is False
    assert report["status"] == "REPORT_ONLY_VALIDATION_COMPLETE"
    assert report["accepted_development_policy"] == "HYBRID_TPE_AVAILABILITY"
    assert report["policy_selection_reopened"] is False
    assert report["feedback_write"] == "FORBIDDEN"
    assert report["scheduler_write"] == "FORBIDDEN"
    assert report["archive_write"] == "FORBIDDEN"
    assert report["promotion"] == "FORBIDDEN"


def test_launcher_builds_candidate_bound_validation_sidecars() -> None:
    text = LAUNCHER.read_text(encoding="utf-8")
    assert "build_cn_phase3cm_time_major_sidecar.py" in text
    assert "build_cn_core_pack_validation_session_sidecar.py" in text
    assert "--evaluation-role validation" in text
    assert "holdout = 'SEALED'" in text
    assert "forward_2026 = 'SEALED'" in text


def test_launcher_scopes_thread_envelopes_by_build_and_evaluation_phase() -> None:
    text = LAUNCHER.read_text(encoding="utf-8")
    serial_numba = text.index("$env:NUMBA_NUM_THREADS = '1'")
    active_sidecar = text.index("build_cn_phase3cm_time_major_sidecar.py")
    session_sidecar = text.index("build_cn_core_pack_validation_session_sidecar.py")
    compute_numba = text.index("$env:NUMBA_NUM_THREADS = '32'")
    execute = text.index("& $python $runner execute")
    assert serial_numba < active_sidecar < session_sidecar < compute_numba < execute
    assert "$ErrorActionPreference = 'Continue'" in text


def test_stable_hash_is_key_order_invariant() -> None:
    assert _stable_hash({"b": 2, "a": 1}) == _stable_hash({"a": 1, "b": 2})
