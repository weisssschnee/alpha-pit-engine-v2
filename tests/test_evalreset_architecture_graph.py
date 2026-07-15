from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def test_historical_phase_a_registry_is_superseded_by_graphskill_current() -> None:
    historical = json.loads(
        (REPO / "runtime/run_plans/evalreset_phase1_architecture_registry_v1.json").read_text(encoding="utf-8")
    )
    overlay = json.loads((REPO / "config/architecture_overlay.json").read_text(encoding="utf-8"))
    current = json.loads((REPO / ".planning/graphs/current.json").read_text(encoding="utf-8"))

    assert historical["registry_version"] == "evalreset_phase1_architecture_registry_v1"
    assert not (REPO / ".planning/architecture").exists()
    assert not (REPO / ".planning/codegraph").exists()
    assert current["overlay"]["path"] == "config/architecture_overlay.json"
    assert {node["id"] for node in overlay["nodes"]} == {
        node["id"] for node in current["nodes"]
    }
    forbidden = {edge["id"] for edge in current["edges"] if edge["forbidden"]}
    assert {
        "validation_to_feedback_forbidden",
        "validation_to_memory_forbidden",
        "forward_to_search_forbidden",
        "forward_to_memory_forbidden",
        "feedback_to_memory_forbidden",
    } <= forbidden
