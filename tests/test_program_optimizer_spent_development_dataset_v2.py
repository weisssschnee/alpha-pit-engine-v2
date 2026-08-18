from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.build_cn_program_optimizer_spent_development_dataset_v2 import _collect_wave_run


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _admission(exact: str) -> dict:
    return {
        "policy_id": "ABS",
        "admitted": True,
        "pair_id": f"pair-{exact}",
        "program_id": f"program-{exact}",
        "control_program_id": f"control-{exact}",
    }


def _uplift(ret: float = 0.2, reward: float = 0.3) -> dict:
    return {
        "program_credit": {
            "matched_cumulative_net_return_increment": ret,
            "matched_net_reward_increment": reward,
            "window_return_increments": [0.1, 0.2, 0.3],
            "lower_tail_window_return_increment": 0.1,
            "robust_median_window_return_increment": 0.2,
            "cross_window_matched_consistency": 1.0,
        }
    }


def _schedule(exact: str, ordinal: int, template: str = "BASE_TEMPORAL") -> dict:
    return {
        "successor_exact_identity": exact,
        "main_record_ordinal": ordinal,
        "template_id": template,
    }


def _ask(exact: str, kind: str = "TPE_OPTIMIZED", template: str = "BASE_TEMPORAL") -> dict:
    return {
        "exact_identity": exact,
        "selection_kind": kind,
        "policy": "FEASIBILITY_GATED_TPE",
        "template_id": template,
    }


def _result(exact: str, source_sha: str, physical_hash: str, *, cache_hit: bool) -> dict:
    return {
        "exact_identity": exact,
        "source_record_sha256": source_sha,
        "physical_result_hash": physical_hash,
        "cache_hit": cache_hit,
        "admission": _admission(exact),
        "uplift": _uplift(),
    }


def _record(path: Path, source_sha: str, exact: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "record_payload_sha256": source_sha,
                "primary": {"behavior_identity": f"p-{exact}"},
                "base_control": {"behavior_identity": f"c-{exact}"},
            }
        ),
        encoding="utf-8",
    )


def test_successor_cache_hit_is_verified_and_not_counted_twice(tmp_path: Path) -> None:
    run = tmp_path / "successor"
    w0 = run / "wave_000"
    _write_jsonl(w0 / "logical_asks.jsonl", [_ask("A")])
    _write_jsonl(w0 / "physical_schedules.jsonl", [_schedule("A", 0)])
    _write_jsonl(w0 / "physical_results.jsonl", [_result("A", "rA", "hA", cache_hit=False)])
    _record(w0 / "records" / "record_0000.json", "rA", "A")

    w1 = run / "wave_001"
    _write_jsonl(w1 / "logical_asks.jsonl", [_ask("A"), _ask("B")])
    _write_jsonl(w1 / "physical_schedules.jsonl", [_schedule("B", 1)])
    _write_jsonl(
        w1 / "physical_results.jsonl",
        [
            _result("A", "rA", "hA", cache_hit=True),
            _result("B", "rB", "hB", cache_hit=False),
        ],
    )
    _record(w1 / "records" / "record_0001.json", "rB", "B")

    rows, evidence = _collect_wave_run(run, "SUCCESSOR_D1", 1)
    assert [row["exact_identity"] for row in rows] == ["A", "B"]
    assert len(rows) == 2
    assert sum("logical_asks.jsonl" in row["path"] for row in evidence) == 2


def test_successor_common_floor_logical_policies_share_one_physical_exact(tmp_path: Path) -> None:
    run = tmp_path / "successor"
    wave = run / "wave_000"
    policies = (
        "UNIFORM",
        "TPE_CONTROL",
        "TPE_TO_SURROGATE",
        "FEASIBILITY_GATED_TPE",
    )
    asks = []
    for policy in policies:
        ask = _ask("A", kind="PURE_UNIFORM" if policy == "UNIFORM" else "COMMON_UNIFORM_FLOOR")
        ask["policy"] = policy
        asks.append(ask)
    schedule = _schedule("A", 0)
    schedule["successor_logical_policies"] = list(policies)
    _write_jsonl(wave / "logical_asks.jsonl", asks)
    _write_jsonl(wave / "physical_schedules.jsonl", [schedule])
    _write_jsonl(
        wave / "physical_results.jsonl",
        [_result("A", "rA", "hA", cache_hit=False)],
    )
    _record(wave / "records" / "record_0000.json", "rA", "A")

    rows, _ = _collect_wave_run(run, "SUCCESSOR_D1", 1)
    assert len(rows) == 1
    assert rows[0]["exact_identity"] == "A"
    assert rows[0]["selection_kind"] == "+".join(policies)


def test_successor_cache_hit_provenance_drift_fails_closed(tmp_path: Path) -> None:
    run = tmp_path / "successor"
    w0 = run / "wave_000"
    _write_jsonl(w0 / "logical_asks.jsonl", [_ask("A")])
    _write_jsonl(w0 / "physical_schedules.jsonl", [_schedule("A", 0)])
    _write_jsonl(w0 / "physical_results.jsonl", [_result("A", "rA", "hA", cache_hit=False)])
    _record(w0 / "records" / "record_0000.json", "rA", "A")

    w1 = run / "wave_001"
    _write_jsonl(w1 / "logical_asks.jsonl", [_ask("A")])
    _write_jsonl(w1 / "physical_schedules.jsonl", [])
    _write_jsonl(w1 / "physical_results.jsonl", [_result("A", "rA", "WRONG", cache_hit=True)])

    with pytest.raises(RuntimeError, match="cache-hit provenance drift"):
        _collect_wave_run(run, "SUCCESSOR_D1", 1)


def test_non_successor_still_requires_schedule_result_exact_equality(tmp_path: Path) -> None:
    run = tmp_path / "d1"
    wave = run / "wave_000"
    _write_jsonl(wave / "physical_schedules.jsonl", [_schedule("A", 0)])
    _write_jsonl(
        wave / "physical_results.jsonl",
        [
            _result("A", "rA", "hA", cache_hit=False),
            _result("B", "rB", "hB", cache_hit=True),
        ],
    )
    _record(wave / "records" / "record_0000.json", "rA", "A")

    with pytest.raises(RuntimeError, match="schedule/result exact drift"):
        _collect_wave_run(run, "D1_FRESH", 2)
