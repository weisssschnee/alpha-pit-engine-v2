from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

from scripts import run_cn_joint_program_phase_d_v0 as runner
from scripts.audit_cn_joint_program_phase_d_v0 import evaluate_decision_gates_v0
from our_system_phase2.runtime.cn_joint_program_phase_d_v0 import (
    CHECKPOINT_COUNT,
    DECISION_GATES,
    EXPECTED_RECORDS,
    FREE_MEMORY_ENFORCEMENT,
    IMPROVED_TEMPLATES,
    RESOURCE_PROFILE,
    WEAK_TEMPLATES,
    build_ask_plan_v0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_phase_d_plan_is_exact_fixed_512_with_template_scoped_exploit() -> None:
    rows = build_ask_plan_v0()
    assert len(rows) == EXPECTED_RECORDS == 512
    assert [row["main_record_ordinal"] for row in rows] == list(range(512))
    assert Counter(row["template_id"] for row in rows) == Counter(
        {template_id: 64 for template_id in (
            "BASE",
            "BASE_TEMPORAL",
            "BASE_MARKET",
            "BASE_EVENT",
            "BASE_TEMPORAL_MARKET",
            "BASE_TEMPORAL_EVENT",
            "BASE_MARKET_EVENT",
            "BASE_TEMPORAL_MARKET_EVENT",
        )}
    )
    assert Counter(row["generation_arm"] for row in rows) == Counter(
        {"UNIFORM_FRESH": 304, "REVISED_EXPLOIT": 96, "NOVELTY_RESERVE": 112}
    )
    assert all(
        row["template_id"] in IMPROVED_TEMPLATES
        for row in rows
        if row["generation_arm"] == "REVISED_EXPLOIT"
    )
    assert not any(
        row["generation_arm"] == "REVISED_EXPLOIT"
        for row in rows
        if row["template_id"] in WEAK_TEMPLATES
    )


def test_phase_d_checkpoint_shapes_are_fixed_before_financial_reads() -> None:
    rows = build_ask_plan_v0()
    assert CHECKPOINT_COUNT == 64
    for checkpoint in range(CHECKPOINT_COUNT):
        batch = rows[checkpoint * 8 : (checkpoint + 1) * 8]
        assert len({row["template_id"] for row in batch}) == 1
        template_id = batch[0]["template_id"]
        shape = Counter(row["generation_arm"] for row in batch)
        if template_id == "BASE":
            assert shape == Counter({"UNIFORM_FRESH": 8})
        elif template_id in IMPROVED_TEMPLATES:
            assert shape == Counter(
                {"UNIFORM_FRESH": 3, "REVISED_EXPLOIT": 3, "NOVELTY_RESERVE": 2}
            )
        else:
            assert shape == Counter({"UNIFORM_FRESH": 6, "NOVELTY_RESERVE": 2})


def test_phase_d_gates_add_matched_return_and_template_blocker_guards() -> None:
    assert DECISION_GATES[
        "matched_return_increment_mean_delta_vs_uniform_minimum"
    ] == 0.0
    assert DECISION_GATES[
        "matched_return_increment_median_delta_vs_uniform_minimum"
    ] == 0.0
    assert DECISION_GATES["per_template_blocked_rate_worsening_allowed"] is False
    assert tuple(DECISION_GATES["revised_exploit_allowed_templates"]) == (
        IMPROVED_TEMPLATES
    )
    assert tuple(DECISION_GATES["revised_exploit_prohibited_templates"]) == (
        WEAK_TEMPLATES
    )


def test_phase_d_resource_manifest_removes_fixed_24_gib_gate_only_here() -> None:
    path = (
        PROJECT_ROOT
        / "runtime"
        / "run_plans"
        / "cn_alpha_node_resource_profiles_phase_d_v0.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    body = dict(payload)
    claimed = body.pop("capacity_manifest_sha256")
    assert claimed == stable_hash(body)
    assert payload["free_memory_enforcement"] == FREE_MEMORY_ENFORCEMENT
    profile = payload["profiles"][RESOURCE_PROFILE]
    assert profile["cpu_threads"] == 32
    assert profile["memory_claim_bytes"] == 1
    assert profile["minimum_free_memory_bytes"] == 1
    assert profile["concurrency_contract"] == "EXCLUSIVE_HEAVY_LANE"


def test_phase_d_runner_and_launcher_keep_telemetry_without_24_gib_fail() -> None:
    assert runner.MINIMUM_FREE_MEMORY_BYTES == 1
    runner_source = (
        PROJECT_ROOT / "scripts" / "run_cn_joint_program_phase_d_v0.py"
    ).read_text(encoding="utf-8")
    assert "engine.MINIMUM_FREE_MEMORY_BYTES = MINIMUM_FREE_MEMORY_BYTES" in runner_source
    launcher = (
        PROJECT_ROOT / "scripts" / "run_cn_joint_program_phase_d_77o.ps1"
    ).read_text(encoding="utf-8")
    assert "VALIDATION_EXCLUSIVE_32_PHASE_D_TELEMETRY" in launcher
    assert "node_resource_governor_phase_d_v0" in launcher
    assert "fixed_24_gib_hard_gate_applied = $false" in launcher
    assert "free_memory_bytes_at_launch" in launcher
    assert "free memory below 24 GiB gate" not in launcher
    assert "joint_program_phase_c.stdout.log" in launcher
    assert "joint_program_phase_c.stderr.log" in launcher


def test_shared_engine_accepts_capacity_hash_from_frozen_contract() -> None:
    source = (
        PROJECT_ROOT / "scripts" / "run_cn_joint_program_phase_c_v0.py"
    ).read_text(encoding="utf-8")
    assert 'contract.get("node_resource_capacity_file_sha256")' in source
    assert 'contract.get("resource_profile")' in source


def test_phase_d_decision_fails_matched_return_or_one_template_blocker() -> None:
    uniform = {
        "productive_rate": 0.4,
        "all_four_positive_rate": 0.2,
        "primary_reward_positive_rate": 0.5,
        "primary_return_positive_rate": 0.5,
        "three_window_positive_rate": 0.4,
        "median_return_per_turnover": 0.1,
        "blocked_rate": 0.1,
        "matched_return_increment_mean": 0.05,
        "matched_return_increment_median": 0.05,
    }
    revised = dict(uniform)
    revised.update(
        {
            "productive_rate": 0.5,
            "all_four_positive_rate": 0.3,
            "matched_return_increment_mean": 0.04,
            "matched_return_increment_median": 0.04,
        }
    )
    template_metrics = {
        template_id: {
            "UNIFORM_FRESH": {
                "all_four_positive_rate": 0.2,
                "blocked_rate": 0.1,
                "matched_return_increment_mean": 0.05,
                "matched_return_increment_median": 0.05,
            },
            "REVISED_EXPLOIT": {
                "all_four_positive_rate": 0.3,
                "blocked_rate": 0.2 if index == 0 else 0.1,
                "matched_return_increment_mean": 0.04,
                "matched_return_increment_median": 0.04,
            },
        }
        for index, template_id in enumerate(IMPROVED_TEMPLATES)
    }
    decision = evaluate_decision_gates_v0(
        revised=revised,
        uniform=uniform,
        template_metrics=template_metrics,
        gates=DECISION_GATES,
    )
    assert decision["all_gates_pass"] is False
    assert "matched_return_increment_mean_delta" in decision["failed_checks"]
    assert "matched_return_increment_median_delta" in decision["failed_checks"]
    assert "per_template_blocker_non_worsening" in decision["failed_checks"]
