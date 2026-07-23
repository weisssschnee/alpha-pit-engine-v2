import hashlib
import json
from pathlib import Path

from scripts.run_cn_core_pack_fixed_holdout import _holdout_summary
from scripts.run_cn_phase3cm_streaming_qualification import (
    _stable_hash,
    _verify_binding,
)


REPO = Path(__file__).resolve().parents[1]
FREEZE = (
    REPO
    / "runtime/run_plans/cn_core_pack_fixed_holdout_candidate_freeze_20260723.json"
)
SPLIT = (
    REPO
    / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
)
LAUNCHER = REPO / "scripts/run_cn_core_pack_fixed_holdout_77o.ps1"


def test_fixed_holdout_freeze_is_closed_and_behavior_exact_unique() -> None:
    freeze = json.loads(FREEZE.read_text(encoding="utf-8"))
    candidates = freeze["candidates"]
    assert freeze["status"] == "FROZEN_BEFORE_HOLDOUT_ACCESS"
    assert freeze["frozen_pair_count"] == len(candidates) == 12
    assert freeze["frozen_candidate_member_count"] == 24
    assert len({row["candidate_id"] for row in candidates}) == 12
    assert len({row["pair_id"] for row in candidates}) == 12
    assert (
        len({row["portfolio_behavior_signature_id"] for row in candidates})
        == freeze["exact_behavior_signature_count"]
        == 12
    )
    assert (
        len({row["portfolio_behavior_family_id"] for row in candidates})
        == freeze["approximate_behavior_family_count"]
        == 11
    )
    assert freeze["split_manifest_sha256"] == hashlib.sha256(
        SPLIT.read_bytes()
    ).hexdigest()
    assert freeze["holdout_contract"] == {
        "fixed_trade_date_count": 48,
        "evaluation_role": "holdout",
        "usage": "report_only",
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "automatic_promotion": "FORBIDDEN",
        "forward_2026_reads": 0,
    }


def test_holdout_summary_is_report_only_and_does_not_promote() -> None:
    freeze = {
        "candidates": [
            {
                "candidate_id": "primary",
                "pair_id": "pair",
                "route_id": "MINUTE_STATIC",
                "expression": "CSRank($close)",
                "portfolio_behavior_signature_id": "signature",
                "portfolio_behavior_family_id": "family",
            }
        ]
    }
    results = [
        {
            "candidate_rewards": [
                {
                    "candidate_id": "primary",
                    "train_day_sortino": 0.2,
                    "train_worst_horizon_day_sortino": 0.1,
                    "holdout_report_metric": 0.3,
                    "portfolio_mode": "long_only_top",
                    "short_allowed": False,
                    "train_mean_one_way_turnover": 0.4,
                }
            ],
            "pair_results": [
                {
                    "pair_id": "pair",
                    "pair_holdout_report_metric": 0.5,
                    "pair_evaluation_status": "PAIR_EVALUATED",
                    "pair_evaluation_blockers": "",
                    "pair_support_count": 100,
                }
            ],
        }
    ]
    summary = _holdout_summary(results, freeze=freeze)
    assert summary["status"] == "FIXED_HOLDOUT_REPORT_COMPLETE"
    assert summary["bias_audit_decision"] == "HOLD_RESEARCH"
    assert summary["automatic_promotion"] == "FORBIDDEN"
    assert summary["feedback_write"] == "FORBIDDEN"
    assert summary["scheduler_write"] == "FORBIDDEN"
    assert summary["archive_write"] == "FORBIDDEN"
    assert summary["candidates"][0]["pair_holdout_report_metric"] == 0.5
    assert summary["candidates"][0]["short_allowed"] is False


def test_streaming_binding_accepts_only_report_only_holdout_role(
    tmp_path: Path,
) -> None:
    binding = {
        "status": "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND",
        "data_role": "holdout_report_only",
        "evaluation_role": "holdout",
        "sealed_reads": {"forward_2026": 0},
        "artifacts": [],
    }
    binding["binding_hash"] = _stable_hash(binding)
    path = tmp_path / "binding.json"
    path.write_text(json.dumps(binding), encoding="utf-8")
    observed = _verify_binding(
        path,
        tmp_path,
        evaluation_role="holdout",
    )
    assert observed["sealed_reads"] == {"forward_2026": 0}


def test_77o_launcher_defaults_to_full_period_authority() -> None:
    text = LAUNCHER.read_text(encoding="utf-8")
    assert (
        r"D:\ChengboRemote\data\phase3dz_true1min_sidecar_augmented_full16_20260702"
        in text
    )
    assert "cn_true1min_development_only_release" not in text
