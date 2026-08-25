"""Freeze audited Search Core V2 Production Wave 3 evidence into repo run-plans."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE3_EVIDENCE_FROZEN_DEVELOPMENT_ONLY"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def freeze(*, repo: Path, output_root: Path, postrun_audit: Path, source_run_id: str) -> dict[str, Any]:
    repo = repo.resolve()
    output_root = output_root.resolve()
    audit = _read(postrun_audit.resolve())
    audit_hash = _verify(audit, "audit_payload_sha256", "Production Wave 3 postrun audit")
    if audit.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE3_POSTRUN_AUDIT_COMPLETE_DEVELOPMENT_ARCHIVE_READY":
        raise RuntimeError("Production Wave 3 postrun audit not frozen-ready")
    if audit["project_control_recommendation"].get("automatic_validation_authorized") is not False:
        raise RuntimeError("Production Wave 3 evidence freeze cannot carry validation authority")

    sources = {
        "terminal": output_root / "CN_SEARCH_CORE_V2_PRODUCTION_WAVE3_COMPLETE.json",
        "final_optimizer_state": output_root / "FINAL_MATURE_OPTIMIZER_STATE.json",
        "productive_candidates": output_root / "PRODUCTIVE_CANDIDATES.jsonl",
        "stable_candidates": output_root / "STABLE_CANDIDATES.jsonl",
        "pair_native_challenger_candidates": output_root / "PAIR_NATIVE_CHALLENGER_CANDIDATES.jsonl",
    }
    for path in sources.values():
        if not path.is_file():
            raise FileNotFoundError(path)
    if _sha(sources["terminal"]) != str(audit["terminal"]["file_sha256"]):
        raise RuntimeError("terminal file drift before freeze")
    if _sha(sources["productive_candidates"]) != str(audit["candidate_archives"]["productive_file_sha256"]):
        raise RuntimeError("productive archive drift before freeze")
    if _sha(sources["stable_candidates"]) != str(audit["candidate_archives"]["stable_file_sha256"]):
        raise RuntimeError("stable archive drift before freeze")
    if _sha(sources["pair_native_challenger_candidates"]) != str(audit["pair_native_annotation"]["challenger_archive_file_sha256"]):
        raise RuntimeError("pair-native challenger archive drift before freeze")
    final_state = _read(sources["final_optimizer_state"])
    final_state_hash = _verify(final_state, "snapshot_hash", "Production Wave 3 final state")
    if final_state_hash != str(audit["mature_state_lineage"]["final_snapshot_payload_sha256"]):
        raise RuntimeError("final state payload drift before freeze")

    target_root = repo / "runtime/run_plans"
    suffix = "79592cb_20260826"
    targets = {
        "terminal": target_root / f"cn_search_core_v2_production_wave3_complete_{suffix}.json",
        "final_optimizer_state": target_root / f"cn_search_core_v2_production_wave3_final_optimizer_state_{suffix}.json",
        "productive_candidates": target_root / f"cn_search_core_v2_production_wave3_productive_candidates_{suffix}.jsonl",
        "stable_candidates": target_root / f"cn_search_core_v2_production_wave3_stable_candidates_{suffix}.jsonl",
        "pair_native_challenger_candidates": target_root / f"cn_search_core_v2_production_wave3_pair_native_challenger_candidates_{suffix}.jsonl",
    }
    if any(path.exists() for path in targets.values()):
        existing = [str(path) for path in targets.values() if path.exists()]
        raise FileExistsError(f"Production Wave 3 frozen evidence destination exists: {existing}")
    target_root.mkdir(parents=True, exist_ok=True)
    for key in sources:
        shutil.copyfile(sources[key], targets[key])
        if _sha(sources[key]) != _sha(targets[key]):
            raise RuntimeError(f"Production Wave 3 frozen copy drift: {key}")

    manifest = {
        "schema_version": "cn_search_core_v2_production_wave3_evidence_freeze_v1",
        "status": STATUS,
        "source_run_id": str(source_run_id),
        "source_output_root": str(output_root),
        "postrun_audit_file_sha256": _sha(postrun_audit.resolve()),
        "postrun_audit_payload_sha256": audit_hash,
        "artifacts": {
            key: {
                "relative_path": str(path.relative_to(repo)).replace("\\", "/"),
                "file_sha256": _sha(path),
                "bytes": path.stat().st_size,
            }
            for key, path in targets.items()
        },
        "productive_candidate_count": int(audit["candidate_archives"]["productive_count"]),
        "stable_candidate_count": int(audit["candidate_archives"]["stable_count"]),
        "pair_native_challenger_candidate_count": int(audit["pair_native_annotation"]["productive_rule_hit_count"]),
        "final_mature_observations": int(audit["mature_state_lineage"]["final_memory_observations"]),
        "classification": "DEVELOPMENT_ONLY_NOT_ALPHA_QUALIFIED",
        "validation_read": False,
        "holdout_read": False,
        "forward_read": False,
        "oos_authority": "NONE",
        "automatic_promotion_authorized": False,
        "financial_evaluation_executed_by_freeze": False,
    }
    manifest["evidence_freeze_payload_sha256"] = stable_hash(manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--postrun-audit", type=Path, required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--manifest-output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_production_wave3_evidence_freeze_20260826.json"))
    args = parser.parse_args(argv)
    manifest = freeze(
        repo=args.repo_root,
        output_root=args.output_root,
        postrun_audit=args.postrun_audit,
        source_run_id=args.source_run_id,
    )
    out = args.manifest_output if args.manifest_output.is_absolute() else args.repo_root / args.manifest_output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": manifest["status"], "productive": manifest["productive_candidate_count"], "stable": manifest["stable_candidate_count"], "final_observations": manifest["final_mature_observations"], "payload": manifest["evidence_freeze_payload_sha256"], "output": str(out.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
