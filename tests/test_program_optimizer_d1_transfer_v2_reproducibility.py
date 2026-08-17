from __future__ import annotations

import json
from pathlib import Path

from scripts.audit_cn_program_optimizer_d1_transfer_v2_reproducibility import build_audit
from scripts.freeze_cn_program_optimizer_d1_transfer_validation_labels_v1 import freeze_labels
from our_system_phase2.services.unified_capability_registry import stable_hash

REPO = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def test_current_v2_reproducibility_gap_is_explicit_and_fail_closed() -> None:
    report = build_audit(REPO)
    assert report["status"] == "FAIL_CLOSED_V2_REPRODUCIBILITY_EVIDENCE_GAP"
    assert report["durable_coverage"] == {
        "old_120_validation_label_rows": 120,
        "old_120_window_feature_rows": 0,
        "B_63_validation_label_rows": 0,
        "B_63_window_feature_rows": 0,
    }
    missing = {row["requirement"]: row for row in report["missing_requirements"]}
    assert missing["OLD_120_CHRONOLOGICAL_DEVELOPMENT_WINDOWS"]["missing_rows"] == 120
    assert missing["B_63_CHRONOLOGICAL_DEVELOPMENT_WINDOWS"]["missing_rows"] == 63
    assert missing["B_63_PER_EXACT_VALIDATION_LABELS"]["missing_rows"] == 63
    assert missing["B_63_PER_EXACT_VALIDATION_LABELS"]["aggregate_positive_count_available"] == 20
    assert report["C_development_execution_authorized_by_this_audit"] is False
    assert report["C_validation_execution_authorized_by_this_audit"] is False
    assert report["holdout_reads"] == 0
    assert report["forward_2026_reads"] == 0


def test_validation_label_freezer_persists_exact_labels_without_financial_reads(tmp_path: Path) -> None:
    exacts = ["a" * 64, "b" * 64]
    members = [
        {"exact_identity": exacts[0], "source_cohort": "D1_CONTINUATION_C"},
        {"exact_identity": exacts[1], "source_cohort": "D1_CONTINUATION_C"},
    ]
    members_path = tmp_path / "freeze" / "validation_candidate_members.jsonl"
    _write_jsonl(members_path, members)

    freeze = {
        "schema_version": "cn_program_optimizer_d1_transfer_prospective_validation_freeze_v1",
        "status": "FROZEN_BEFORE_VALIDATION_ACCESS",
        "candidate_count": 2,
        "candidate_exact_identities_sha256": stable_hash(sorted(exacts)),
        "candidate_members_payload_sha256": stable_hash(members),
    }
    freeze["freeze_payload_sha256"] = stable_hash(freeze)
    freeze_path = tmp_path / "freeze" / "validation_candidate_freeze.json"
    _write_json(freeze_path, freeze)

    formal_root = tmp_path / "formal"
    records = formal_root / "records"
    records.mkdir(parents=True)
    positives = {exacts[0]: True, exacts[1]: False}
    admitted = {exacts[0]: True, exacts[1]: True}
    for ordinal, exact in enumerate(exacts):
        result = {
            "schema_version": "cn_program_optimizer_d1_validation_result_v1",
            "status": "D1_VALIDATION_RESULT_CLOSED_IMMUTABLE",
            "validation_record_ordinal": ordinal,
            "exact_identity": exact,
            "source_cohort": "D1_CONTINUATION_C",
            "validation_productive": positives[exact],
            "admission": {"admitted": admitted[exact]},
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        result["result_payload_sha256"] = stable_hash(result)
        _write_json(records / f"candidate_{ordinal:03d}_{exact[:12]}.json", {"validation_result": result})

    closure = {
        "schema_version": "cn_program_optimizer_d1_transfer_prospective_validation_v1",
        "status": "CN_PROGRAM_OPTIMIZER_D1_TRANSFER_PROSPECTIVE_VALIDATION_COMPLETE",
        "candidate_count": 2,
        "candidate_exact_identities_sha256": stable_hash(sorted(exacts)),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    closure["closure_payload_sha256"] = stable_hash(closure)
    _write_json(formal_root / "CN_PROGRAM_OPTIMIZER_D1_TRANSFER_PROSPECTIVE_VALIDATION_COMPLETE.json", closure)

    output = tmp_path / "labels.json"
    payload = freeze_labels(
        formal_root=formal_root,
        candidate_freeze_path=freeze_path,
        candidate_members_path=members_path,
        output_path=output,
    )
    assert payload["status"] == "FROZEN_DURABLE_REPORT_ONLY_VALIDATION_LABEL_EVIDENCE"
    assert payload["candidate_count"] == 2
    assert payload["validation_productive_count"] == 1
    assert payload["validation_admitted_count"] == 2
    assert [row["exact_identity"] for row in payload["label_rows"]] == sorted(exacts)
    assert [row["validation_productive"] for row in payload["label_rows"]] == [True, False]
    assert payload["financial_evaluation_performed_by_this_freeze"] is False
    assert payload["holdout_reads_by_this_freeze"] == 0
    assert payload["forward_2026_reads_by_this_freeze"] == 0
    assert output.is_file()


def test_development_window_feature_freezer_replays_only_closed_result_files(tmp_path: Path) -> None:
    from scripts.freeze_cn_program_optimizer_d1_transfer_development_window_features_v2 import freeze_features

    exact = "c" * 64
    run_root = tmp_path / "development"
    wave_root = run_root / "wave_000"
    wave_root.mkdir(parents=True)
    result = {
        "exact_identity": exact,
        "source_record_sha256": "1" * 64,
        "physical_result_hash": "2" * 64,
        "admission": {
            "admitted": True,
            "metrics": {"development_window_ids": ["development_1", "development_2", "development_3"]},
        },
        "uplift": {
            "program_credit": {
                "matched_cumulative_net_return_increment": 0.4,
                "matched_net_reward_increment": 1.5,
                "window_return_increments": [0.1, -0.2, 0.5],
            }
        },
    }
    _write_jsonl(wave_root / "physical_results.jsonl", [result])
    members = [{
        "exact_identity": exact,
        "source_cohort": "D1_FRESH",
        "source_root": str(run_root),
        "source_wave": 0,
        "source_record_sha256": "1" * 64,
        "physical_result_hash": "2" * 64,
        "development_matched_cumulative_net_return_increment": 0.4,
        "development_matched_net_reward_increment": 1.5,
    }]
    members_path = tmp_path / "members.jsonl"
    _write_jsonl(members_path, members)
    output = tmp_path / "window_evidence.json"

    payload = freeze_features(member_paths=[members_path], output_path=output)
    assert payload["status"] == "FROZEN_DURABLE_DEVELOPMENT_WINDOW_FEATURE_EVIDENCE"
    assert payload["candidate_count"] == 1
    assert payload["feature_rows"][0]["development_window_return_increments"] == [0.1, -0.2, 0.5]
    assert payload["financial_evaluation_performed_by_this_freeze"] is False
    assert payload["validation_reads_by_this_freeze"] == 0
    assert payload["holdout_reads_by_this_freeze"] == 0
    assert payload["forward_2026_reads_by_this_freeze"] == 0


def test_v2_model_reproduction_checker_uses_exact_frozen_model_contract() -> None:
    from scripts.check_cn_program_optimizer_d1_transfer_v2_model_reproduction import FEATURES, _model

    model = _model()
    params = model.get_params()
    assert FEATURES == (
        "dev_matched_return",
        "dev_matched_reward",
        "dev_window_1_increment",
        "dev_window_2_increment",
        "dev_window_3_increment",
    )
    assert params["C"] == 0.5
    assert params["class_weight"] == "balanced"
    assert params["solver"] == "liblinear"
    assert params["random_state"] == 82617


def test_development_window_feature_freezer_accepts_successor_wave_physical_results(tmp_path: Path) -> None:
    from scripts.freeze_cn_program_optimizer_d1_transfer_development_window_features_v2 import freeze_features

    exact = "e" * 64
    run_root = tmp_path / "successor"
    wave_root = run_root / "wave_000"
    wave_root.mkdir(parents=True)
    result = {
        "exact_identity": exact,
        "source_record_sha256": "3" * 64,
        "physical_result_hash": "4" * 64,
        "admission": {
            "admitted": True,
            "metrics": {"development_window_ids": ["development_1", "development_2", "development_3"]},
        },
        "uplift": {
            "program_credit": {
                "matched_cumulative_net_return_increment": 0.6,
                "matched_net_reward_increment": 1.8,
                "window_return_increments": [0.3, 0.2, 0.1],
            }
        },
    }
    _write_jsonl(wave_root / "wave_physical_results.jsonl", [result])
    members = [{
        "exact_identity": exact,
        "source_cohort": "SUCCESSOR_D1",
        "source_root": str(run_root),
        "source_wave": 0,
        "source_record_sha256": "3" * 64,
        "physical_result_hash": "4" * 64,
        "development_matched_cumulative_net_return_increment": 0.6,
        "development_matched_net_reward_increment": 1.8,
    }]
    members_path = tmp_path / "successor_members.jsonl"
    _write_jsonl(members_path, members)
    payload = freeze_features(member_paths=[members_path], output_path=tmp_path / "successor_windows.json")
    assert payload["candidate_count"] == 1
    assert payload["feature_rows"][0]["development_window_return_increments"] == [0.3, 0.2, 0.1]
    assert payload["source_physical_result_files"][0]["path"].endswith("wave_physical_results.jsonl")
