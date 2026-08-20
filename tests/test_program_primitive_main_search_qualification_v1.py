from __future__ import annotations

import json
from pathlib import Path

from our_system_phase2.services.project_control_admission import sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash


REPO = Path(__file__).resolve().parents[1]
QUAL = REPO / "runtime/run_plans/cn_program_primitive_main_search_qualification_v1.json"


def test_primitive_main_search_qualification_is_self_hashed_and_code_bound() -> None:
    payload = json.loads(QUAL.read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop("qualification_payload_sha256"))
    assert stable_hash(body) == claimed
    assert payload["status"] == "PRIMITIVE_LOCAL_MAIN_SEARCH_QUALIFIED_V1"
    assert payload["decision"] == "PRIMITIVE_PRIMARY_UNIFORM_RESERVE_EVOLUTION_CHALLENGER"
    assert payload["execution_authorized_by_qualification"] is False
    assert payload["old_v2_catalog_launch_authorized"] is False
    assert payload["bootstrap_primitive_stats"]["stage_c_results_imported"] is False
    assert payload["bootstrap_primitive_stats"]["stage_d_results_imported"] is False
    assert payload["production_equivalence"]["stage_d_candidate_count"] == 2016
    assert payload["production_equivalence"]["numeric_max_abs_error"] <= 1e-15
    assert payload["production_equivalence"]["all_template_orders_match"] is True
    assert payload["stage_d_confirmation"]["primitive_productive_at_1008"] == 355
    assert payload["stage_d_confirmation"]["typed_evolution_productive_at_1008"] == 296
    assert payload["stage_d_confirmation"]["uniform_seed_mean_productive_at_1008"] == 254.0
    assert payload["stage_d_confirmation"]["templates_won_vs_uniform_at_1008"] == 7
    assert payload["stage_d_confirmation"]["templates_won_vs_evolution_at_1008"] == 6
    for relative_key, hash_key in (
        ("primitive_scorer_relative_path", "primitive_scorer_file_sha256"),
        ("large_fresh_v3_bandit_relative_path", "large_fresh_v3_bandit_file_sha256"),
        ("large_fresh_v3_runner_relative_path", "large_fresh_v3_runner_file_sha256"),
    ):
        binding = payload["implementation"]
        assert sha256_file(REPO / binding[relative_key]) == binding[hash_key]


def test_v3_budget_scheduler_keeps_both_controls() -> None:
    payload = json.loads(QUAL.read_text(encoding="utf-8-sig"))
    scheduler = payload["v3_budget_scheduler"]
    assert scheduler["primitive_checkpoints_per_7_template_macro"] == 5
    assert scheduler["uniform_reserve_checkpoints_per_macro"] == 1
    assert scheduler["typed_evolution_challenger_checkpoints_per_macro"] == 1
    assert all(
        row == {"primitive": 5, "uniform": 1, "evolution": 1}
        for row in scheduler["rotation_verified_macros"].values()
    )
