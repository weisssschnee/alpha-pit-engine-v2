import json
from pathlib import Path

from scripts import build_cn_search_core_v2_production_wave1_development_archive as archive
from our_system_phase2.services.unified_capability_registry import stable_hash


def _row(exact, behavior, region, template, *, stable, positive, consistency, lower, median, ret, reward, turnover):
    row = {
        "schema_version": "cn_search_core_v2_stage1_result_v1",
        "exact_identity": exact,
        "template_id": template,
        "base_component_id": "base-" + exact,
        "behavior_pair_identity": behavior,
        "structural_region_identity": region,
        "productive": True,
        "stable": stable,
        "uplift": {"program_credit": {
            "matched_net_reward_increment": reward,
            "matched_cumulative_net_return_increment": ret,
            "cross_window_positive_increment_count": positive,
            "cross_window_matched_consistency": consistency,
            "robust_median_window_return_increment": median,
            "lower_tail_window_return_increment": lower,
            "turnover_differential": turnover,
        }},
        "result_payload_sha256": "result-" + exact,
    }
    return row


def _write_jsonl(path: Path, rows):
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _audit(productive_path: Path, stable_path: Path, productive_count: int, stable_count: int):
    payload = {
        "schema_version": "cn_search_core_v2_production_wave1_postrun_audit_v1",
        "status": "SEARCH_CORE_V2_PRODUCTION_WAVE1_POSTRUN_AUDIT_COMPLETE_ARCHIVE_READY",
        "candidate_archives": {
            "productive_count": productive_count,
            "stable_count": stable_count,
            "productive_file_sha256": archive._sha(productive_path),
            "stable_file_sha256": archive._sha(stable_path),
        },
        "project_control_recommendation": {"automatic_validation_authorized": False},
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def test_behavior_cluster_keeps_best_stable_representative(tmp_path):
    weak = _row("a", "beh-1", "reg-1", "BASE_EVENT", stable=False, positive=3, consistency=1.0, lower=0.2, median=0.3, ret=0.5, reward=1.0, turnover=0.1)
    strong = _row("b", "beh-1", "reg-2", "BASE_EVENT", stable=True, positive=2, consistency=2/3, lower=0.1, median=0.2, ret=0.4, reward=0.8, turnover=0.2)
    other = _row("c", "beh-2", "reg-3", "BASE_MARKET", stable=True, positive=3, consistency=1.0, lower=0.4, median=0.5, ret=0.6, reward=1.2, turnover=0.3)
    productive_path = tmp_path / "productive.jsonl"
    stable_path = tmp_path / "stable.jsonl"
    _write_jsonl(productive_path, [weak, strong, other])
    _write_jsonl(stable_path, [strong, other])
    audit_path = tmp_path / "audit.json"
    audit_path.write_text(json.dumps(_audit(productive_path, stable_path, 3, 2)), encoding="utf-8")
    summary, representatives, frontier = archive.build(postrun_audit=audit_path, productive_archive=productive_path, stable_archive=stable_path)
    assert summary["behavior_cluster_count"] == 2
    assert [row["exact_identity"] for row in representatives] == ["c", "b"]
    assert all(row["alpha_qualified"] is False for row in representatives)
    assert len(frontier) == 2


def test_stable_archive_must_be_productive_subset(tmp_path):
    productive = _row("a", "beh-1", "reg-1", "BASE_EVENT", stable=False, positive=2, consistency=2/3, lower=0.1, median=0.2, ret=0.3, reward=0.4, turnover=0.1)
    alien = _row("z", "beh-z", "reg-z", "BASE_EVENT", stable=True, positive=3, consistency=1.0, lower=0.2, median=0.3, ret=0.4, reward=0.5, turnover=0.1)
    productive_path = tmp_path / "productive.jsonl"
    stable_path = tmp_path / "stable.jsonl"
    _write_jsonl(productive_path, [productive])
    _write_jsonl(stable_path, [alien])
    audit_path = tmp_path / "audit.json"
    audit_path.write_text(json.dumps(_audit(productive_path, stable_path, 1, 1)), encoding="utf-8")
    try:
        archive.build(postrun_audit=audit_path, productive_archive=productive_path, stable_archive=stable_path)
    except RuntimeError as exc:
        assert "subset" in str(exc)
    else:
        raise AssertionError("expected stable-subset failure")
