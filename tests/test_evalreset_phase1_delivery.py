from __future__ import annotations

from pathlib import Path

from scripts.validate_evalreset_phase1_delivery import validate_delivery


REPO = Path(__file__).resolve().parents[1]


def test_in_progress_phase_a_delivery_has_all_formal_asset_types() -> None:
    result = validate_delivery(REPO, require_complete=False)

    assert result["required_file_count"] >= 10
    assert result["forbidden_edge_count"] >= 6


def test_committed_phase_a_has_complete_delivery() -> None:
    result = validate_delivery(REPO, require_complete=True)

    assert result["require_complete"] is True
