from __future__ import annotations

import json
from pathlib import Path

from scripts.refresh_evalreset_artifact_index import refresh


REPO = Path(__file__).resolve().parents[1]


def test_implemented_artifacts_exist_and_receive_hashes() -> None:
    index = json.loads((REPO / "reports/evalreset_phase1_20260711/ARTIFACT_INDEX.json").read_text(encoding="utf-8"))

    refreshed = refresh(index, repo=REPO)

    implemented = [row for row in refreshed["artifacts"] if row["state"] == "IMPLEMENTED"]
    assert implemented
    assert all(row["exists"] and row["last_verified_sha"] for row in implemented)
