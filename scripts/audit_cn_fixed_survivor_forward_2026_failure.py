"""Independently audit the spent-but-unclosed fixed10 2026 forward attempt."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

import pandas as pd


EXECUTION_SHA = "0d2650839638250354d86fbb2073c89a6de3890b"
SELECTION_SHA = "7cfc2e454da7ae7561b57979db8010324422cd87ca3eef42167809400d59ef77"
INCIDENT_SHA = "74d77425c2eb122343ee10568107c9138fdcdcb744927d03c9165166335960bc"
PRESERVATION_SHA = "c1488121537f9f26270c1dc387aa4a5f29edd8043cf7984986edafc3c11dc985"
ACCESS_MARKER_SHA = "e260181d458b42a3c7ac05beca4b5027516817b5ffe35bc218b07f0acd4981d6"
DEPLOYMENT_BINDING_SHA = "e3a229d72fcad6feff3780c3c7c21d31678efe1dbdd2498096f0a2030f711c87"
EXPECTED_ORDERS = [2, 6, 7, 8, 9, 10, 11, 15, 18, 21]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: dict) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_text(path: Path) -> str:
    payload = path.read_bytes()
    encoding = "utf-16" if payload.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    return payload.decode(encoding, errors="replace")


def _git_file(repo: Path, revision: str, relative_path: str) -> str:
    completed = subprocess.run(
        ["git", "show", f"{revision}:{relative_path}"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout


def audit(*, repo: Path, evidence_root: Path, output_root: Path) -> dict:
    incident_root = evidence_root / (
        "20260805T120814_fixed10_forward_chip_guard_after_forward_access"
    )
    run_root = evidence_root / (
        "cn_fixed10_forward_2026_confirmation_10_20260805_1f999bd"
    )
    incident_path = incident_root / "incident.json"
    preservation_path = incident_root / "preservation_manifest.csv"
    access_path = run_root / "forward_access_started.json"
    binding_path = run_root / "deployment_binding.json"
    prepared_path = run_root / "prepared" / "PREPARED_ZERO_READ.json"
    pair_path = run_root / "prepared" / "confirmation_pairs.parquet"
    candidate_path = run_root / "prepared" / "confirmation_candidates.csv"
    process_exit_path = run_root / "process_exit.json"

    assert _sha256(incident_path) == INCIDENT_SHA
    assert _sha256(preservation_path) == PRESERVATION_SHA
    assert _sha256(access_path) == ACCESS_MARKER_SHA
    assert _sha256(binding_path) == DEPLOYMENT_BINDING_SHA
    incident = _read_json(incident_path)
    access = _read_json(access_path)
    binding = _read_json(binding_path)
    prepared = _read_json(prepared_path)
    process_exit = _read_json(process_exit_path)
    pairs = pd.read_parquet(pair_path)
    candidates = pd.read_csv(candidate_path)

    assert incident["status"] == "FORWARD_2026_SPENT_RUN_FAILED_NO_CONFIRMATION_RESULT"
    assert incident["positive_forward_row_access"] is True
    assert incident["retry_authorized"] is False
    assert incident["financial_results_reused"] is False
    assert access["status"] == "FORWARD_2026_SPENT_ON_FIRST_FINANCIAL_READ"
    assert binding["selection_payload_sha256"] == SELECTION_SHA
    assert binding["repo_sha"] == EXECUTION_SHA
    assert prepared["forward_2026_reads"] == 0
    assert prepared["pair_count"] == 10
    assert prepared["candidate_member_count"] == 20
    assert len(pairs) == 10
    assert len(candidates) == 20
    assert pairs["finalist_order"].astype(int).tolist() == EXPECTED_ORDERS
    assert process_exit["status"] == "FAILED"
    assert process_exit["forward_asset_state"] == "SPENT"
    assert not (run_root / "sidecars" / "forward_2026_session_fields").exists()
    assert not (run_root / "sidecars" / "forward_2026_session_labels").exists()
    assert not (run_root / "confirmation" / "FORWARD_2026_COMPLETE.json").exists()

    stderr = _read_text(run_root / "forward.stderr.log")
    stdout = _read_text(run_root / "forward.stdout.log")
    failure = "chip maximum observable time cannot enter sealed 2026"
    assert failure in stdout
    assert "forward field sidecar build failed" in stderr

    executed_builder = _git_file(
        repo,
        EXECUTION_SHA,
        "scripts/build_cn_core_pack_validation_session_sidecar.py",
    )
    collect_index = executed_builder.index('.collect(engine="streaming")["code"]')
    chip_index = executed_builder.index("chip_context, chip_receipt = load_chip_context(")
    assert collect_index < chip_index

    output_root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": "cn_fixed10_forward_2026_failure_independent_audit_v1",
        "status": "PASS",
        "audited_at_utc": datetime.now(timezone.utc).isoformat(),
        "execution_sha": EXECUTION_SHA,
        "selection_payload_sha256": SELECTION_SHA,
        "pair_count": 10,
        "candidate_member_count": 20,
        "source_finalist_orders": EXPECTED_ORDERS,
        "forward_access_started": True,
        "positive_forward_row_access": True,
        "forward_asset_state": "SPENT",
        "confirmation_result_status": "UNAVAILABLE_RUN_FAILED",
        "candidate_result_count": 0,
        "pair_result_count": 0,
        "field_sidecar_written": False,
        "label_sidecar_written": False,
        "confirmation_closure_written": False,
        "failure_reason": failure,
        "failure_order_verified": (
            "FORWARD_ALLOWED_CODE_COLLECTION_PRECEDED_CHIP_CONTEXT_GUARD"
        ),
        "economics_available": False,
        "absolute_return_available": False,
        "matched_increment_available": False,
        "uncertainty_available": False,
        "retry_authorized": False,
        "promotion_authorized": False,
        "financial_results_reused": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "incident_file_sha256": INCIDENT_SHA,
        "preservation_manifest_sha256": PRESERVATION_SHA,
        "access_marker_sha256": ACCESS_MARKER_SHA,
        "deployment_binding_sha256": DEPLOYMENT_BINDING_SHA,
    }
    report["audit_payload_sha256"] = _stable_hash(report)
    audit_path = output_root / "audit.json"
    audit_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "status": report["status"],
        "audit": str(audit_path),
        "audit_file_sha256": _sha256(audit_path),
        "audit_payload_sha256": report["audit_payload_sha256"],
        "forward_asset_state": report["forward_asset_state"],
        "economics_available": report["economics_available"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = audit(
        repo=args.repo.resolve(),
        evidence_root=args.evidence_root.resolve(),
        output_root=args.output_root.resolve(),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
