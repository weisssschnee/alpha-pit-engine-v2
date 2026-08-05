from pathlib import Path

from scripts.validate_evaluation_ledgers import validate_ledgers


REPO = Path(__file__).resolve().parents[1]


def test_committed_evaluation_ledgers_are_internally_consistent() -> None:
    summary = validate_ledgers(
        REPO / "runtime/run_plans/evaluation_data_roles_v1.json",
        REPO / "runtime/run_plans/evaluation_access_ledger_v1.csv",
        REPO / "runtime/run_plans/oos_burn_ledger_v1.csv",
    )

    assert summary["role_count"] == 5
    assert summary["forward_access_violation_count"] == 0
    assert summary["validation_holdout_spent_records"] == 2
    assert summary["spent_forward_2026_registered"] is True
    assert summary["historical_challenge_2023_registered"] is True
    assert summary["forward_b_registered"] is True
    assert summary["contract_self_hashes_verified"] == 2
