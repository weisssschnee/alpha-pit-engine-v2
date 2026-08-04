"""Independently verify Decoder V2 unchanged-cohort report-only OOS evidence."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1
from scripts import build_cn_validation_session_authority as validation_authority
from scripts import freeze_cn_decoder_v2_finalists as freeze
from scripts import run_cn_portfolio_decoder_v2_oos as oos


def verify_oos(
    *,
    oos_root: Path,
    finalist_root: Path,
    validation_session_authority_root: Path,
    output_root: Path,
    expected_selection_payload_sha256: str,
    expected_decoder_policy_sha256: str,
) -> dict[str, Any]:
    oos_root = Path(oos_root).resolve()
    finalist_root = Path(finalist_root).resolve()
    validation_session_authority_root = Path(
        validation_session_authority_root
    ).resolve()
    output_root = Path(output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=False)

    closure_path = oos_root / "OOS_COMPLETE.json"
    closure = v1._read_json(closure_path)
    base = dict(closure)
    expected_hash = str(base.pop("manifest_body_sha256", ""))
    if not expected_hash or v1._stable_hash(base) != expected_hash:
        raise RuntimeError("OOS closure self-hash mismatch")
    required = {
        "status": "DECODER_V2_TOPK10_EQUAL_REPORT_ONLY_OOS_CLOSED_IMMUTABLE",
        "evidence_scope": oos.EVIDENCE_SCOPE,
        "selection_payload_sha256": expected_selection_payload_sha256,
        "pair_count": oos.EXPECTED_PAIR_COUNT,
        "candidate_member_count": oos.EXPECTED_MEMBER_COUNT,
        "decoder_id": oos.DECODER_ID,
        "decoder_policy_sha256": expected_decoder_policy_sha256,
        "interstage_filter_applied": False,
        "train_recomputed": False,
        "accounting_invariants_status": "PASS",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "promotion_authorized": False,
    }
    drift = [key for key, value in required.items() if closure.get(key) != value]
    if drift:
        raise RuntimeError("OOS closure drift: " + ",".join(drift))
    if int(closure.get("validation_reads") or 0) <= 0:
        raise RuntimeError("OOS closure has no validation reads")
    authority_manifest_path = (
        validation_session_authority_root
        / "validation_session_authority_manifest.json"
    )
    if v1._sha256(authority_manifest_path) != str(
        closure.get("validation_session_authority_manifest_sha256")
    ):
        raise RuntimeError("OOS validation session authority binding drift")
    authority_manifest = v1._read_json(authority_manifest_path)
    authority_body = dict(authority_manifest)
    authority_hash = str(authority_body.pop("manifest_payload_sha256", ""))
    if not authority_hash or v1._stable_hash(authority_body) != authority_hash:
        raise RuntimeError("OOS validation session authority self-hash drift")
    if authority_manifest.get("schema_version") != validation_authority.SCHEMA_VERSION:
        raise RuntimeError("OOS validation session authority schema drift")
    if int(authority_manifest.get("missing_exact_st_fail_closed_session_count", -1)) != 0:
        raise RuntimeError("OOS validation session authority has missing ST states")
    if int(authority_manifest.get("non_st_authority_session_count") or 0) <= 0:
        raise RuntimeError("OOS validation session authority has no non-ST sessions")
    if int(closure.get("prepared_eligible_session_count") or 0) <= 0:
        raise RuntimeError("OOS closure has no eligible validation sessions")
    if int(closure.get("minimum_free_memory_bytes") or 0) < (
        oos.MINIMUM_FREE_MEMORY_BYTES
    ):
        raise RuntimeError("OOS closure memory gate failed")

    for artifact in closure.get("artifacts") or ():
        path = (oos_root / str(artifact["path"])).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"OOS artifact size drift: {path}")
        if v1._sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"OOS artifact hash drift: {path}")

    manifest = freeze._read_json(finalist_root / "finalist_manifest.json")
    freeze._verify_self_hash(
        manifest,
        field="manifest_payload_sha256",
        label="finalist manifest",
    )
    freeze._verify_artifacts(
        manifest, root=finalist_root, label="finalist manifest"
    )
    artifacts = list(manifest.get("artifacts") or ())
    pair_source_path = oos._artifact_path(
        finalist_root, artifacts, "finalist_pairs.parquet"
    )
    pairs = pd.read_parquet(pair_source_path).where(pd.notna, None)
    candidate_metrics = pd.read_parquet(
        oos_root / "oos_candidate_metrics.parquet"
    ).where(pd.notna, None)
    pair_metrics = pd.read_parquet(
        oos_root / "oos_pair_metrics.parquet"
    ).where(pd.notna, None)
    if len(candidate_metrics) != oos.EXPECTED_MEMBER_COUNT:
        raise RuntimeError("OOS candidate metric cardinality drift")
    if len(pair_metrics) != oos.EXPECTED_PAIR_COUNT:
        raise RuntimeError("OOS pair metric cardinality drift")
    if not candidate_metrics["accounting_invariants_status"].eq("PASS").all():
        raise RuntimeError("OOS candidate accounting invariant failure")
    if not candidate_metrics["decoder_policy_sha256"].eq(
        expected_decoder_policy_sha256
    ).all():
        raise RuntimeError("OOS candidate decoder policy drift")
    if not candidate_metrics["evaluation_role"].eq("validation").all():
        raise RuntimeError("OOS candidate evaluation role drift")
    if not candidate_metrics["data_role"].eq("validation_report_only").all():
        raise RuntimeError("OOS candidate data role drift")

    recomputed = oos._pair_metrics(
        pairs=pairs, candidate_metrics=candidate_metrics
    )
    pd.testing.assert_frame_equal(
        recomputed.reset_index(drop=True),
        pair_metrics.reset_index(drop=True),
        check_dtype=False,
        check_exact=True,
    )
    if pair_metrics["pair_id"].astype(str).tolist() != pairs["pair_id"].astype(
        str
    ).tolist():
        raise RuntimeError("OOS pair order drift")
    candidate_files = sorted((oos_root / "candidates").glob("*.json"))
    if len(candidate_files) != oos.EXPECTED_MEMBER_COUNT:
        raise RuntimeError("OOS immutable candidate file count drift")
    for path in candidate_files:
        payload = v1._read_json(path)
        observed = str(payload.pop("payload_sha256", ""))
        if not observed or v1._stable_hash(payload) != observed:
            raise RuntimeError(f"OOS candidate self-hash mismatch: {path}")

    receipt = {
        "schema_version": "cn_portfolio_decoder_v2_oos_audit_v1",
        "status": "PASS_INDEPENDENT_DECODER_V2_REPORT_ONLY_OOS_VERIFICATION",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "oos_closure_sha256": v1._sha256(closure_path),
        "oos_closure_payload_sha256": expected_hash,
        "selection_payload_sha256": expected_selection_payload_sha256,
        "decoder_policy_sha256": expected_decoder_policy_sha256,
        "pair_count": len(pair_metrics),
        "candidate_member_count": len(candidate_metrics),
        "candidate_self_hash_count": len(candidate_files),
        "prepared_eligible_session_count": int(
            closure["prepared_eligible_session_count"]
        ),
        "total_fill_count": int(closure["total_fill_count"]),
        "candidate_accounting_invariants_status": "PASS",
        "pair_recomputation_status": "PASS",
        "identity_order_status": "PASS",
        "validation_reads": int(closure["validation_reads"]),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    receipt["receipt_payload_sha256"] = v1._stable_hash(receipt)
    receipt_path = v1._write_json(output_root / "audit.json", receipt)
    return {
        **receipt,
        "audit_path": str(receipt_path),
        "audit_file_sha256": v1._sha256(receipt_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--oos-root", type=Path, required=True)
    parser.add_argument("--finalist-root", type=Path, required=True)
    parser.add_argument(
        "--validation-session-authority-root", type=Path, required=True
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-selection-payload-sha256", required=True)
    parser.add_argument("--expected-decoder-policy-sha256", required=True)
    args = parser.parse_args()
    result = verify_oos(
        oos_root=args.oos_root,
        finalist_root=args.finalist_root,
        validation_session_authority_root=(
            args.validation_session_authority_root
        ),
        output_root=args.output_root,
        expected_selection_payload_sha256=(
            args.expected_selection_payload_sha256
        ),
        expected_decoder_policy_sha256=args.expected_decoder_policy_sha256,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
