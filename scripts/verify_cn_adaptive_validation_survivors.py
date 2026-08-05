"""Independently verify the fixed adaptive-validation survivor cohort."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from scripts.freeze_cn_adaptive_validation_survivors import (
    DECODER_ID,
    EXPECTED_SELECTED_PAIRS,
    _artifact_by_name,
    _holdout_calendar,
    _read_json,
    _verify_self_hash,
)
from scripts.freeze_cn_productive_keep_review_cohort import (
    _payload_sha256,
    _sha256,
    _write_json,
)


def _verify_artifacts(payload: Mapping[str, Any], root: Path) -> None:
    for row in payload.get("artifacts") or ():
        path = (root / str(row["path"])).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(row["bytes"]):
            raise RuntimeError(f"confirmation artifact size drift: {path}")
        if _sha256(path) != str(row["sha256"]):
            raise RuntimeError(f"confirmation artifact hash drift: {path}")


def verify_survivors(
    *,
    confirmation_root: Path,
    train_finalist_root: Path,
    oos_root: Path,
    split_manifest: Path,
    receipt_path: Path,
) -> dict[str, Any]:
    confirmation_root = Path(confirmation_root).resolve()
    manifest_path = confirmation_root / "CONFIRMATION_COHORT_FROZEN.json"
    manifest = _read_json(manifest_path)
    _verify_self_hash(
        manifest, field="manifest_payload_sha256", label="confirmation manifest"
    )
    if manifest.get("status") != "FIXED_SURVIVOR_CONFIRMATORY_FREEZE_CLOSED_IMMUTABLE":
        raise RuntimeError("confirmation freeze is not immutable")
    _verify_artifacts(manifest, confirmation_root)
    if int(manifest.get("selected_pairs") or 0) != EXPECTED_SELECTED_PAIRS:
        raise RuntimeError("confirmation selected pair count drift")
    if int(manifest.get("selected_candidate_members") or 0) != 20:
        raise RuntimeError("confirmation selected member count drift")
    if any(
        int(manifest.get(field) or 0) != 0
        for field in ("holdout_market_or_label_rows_read", "forward_2026_reads")
    ):
        raise RuntimeError("confirmation freeze crossed sealed boundary")

    contract = _read_json(confirmation_root / "confirmation_contract.json")
    summary = _read_json(confirmation_root / "confirmation_summary.json")
    _verify_self_hash(
        contract, field="contract_payload_sha256", label="confirmation contract"
    )
    if contract.get("selection_basis") != "EXACT_ALL_FOUR_ADAPTIVE_VALIDATION_SURVIVORS":
        raise RuntimeError("confirmation selection basis drift")
    for field in ("replacement", "backfill", "post_freeze_filtering"):
        if contract.get(field) != "FORBIDDEN":
            raise RuntimeError(f"confirmation cohort mutation boundary drift: {field}")
    for field in (
        "optimizer_feedback_write",
        "scheduler_write",
        "archive_write",
        "automatic_promotion",
    ):
        if contract.get(field) != "FORBIDDEN":
            raise RuntimeError(f"confirmation prohibited write drift: {field}")
    if contract.get("confirmation_access_count") != "EXACTLY_ONE":
        raise RuntimeError("confirmation one-shot boundary drift")
    if contract.get("selector_claim_authorized") is not False:
        raise RuntimeError("selector claim must remain unauthorized")

    holdout_dates, split_hash = _holdout_calendar(split_manifest)
    if split_hash != manifest.get("fixed_split_manifest_sha256"):
        raise RuntimeError("confirmation split binding drift")
    if int(contract.get("fixed_holdout_trade_date_count") or 0) != len(holdout_dates):
        raise RuntimeError("confirmation holdout date count drift")

    train_manifest = _read_json(train_finalist_root / "finalist_manifest.json")
    train_pairs_path = _artifact_by_name(
        train_finalist_root,
        list(train_manifest.get("artifacts") or ()),
        "finalist_pairs.parquet",
    )
    train_candidates_path = _artifact_by_name(
        train_finalist_root,
        list(train_manifest.get("artifacts") or ()),
        "finalist_candidates.parquet",
    )
    oos_closure = _read_json(oos_root / "OOS_COMPLETE.json")
    oos_pairs_path = _artifact_by_name(
        oos_root,
        list(oos_closure.get("artifacts") or ()),
        "oos_pair_metrics.parquet",
    )
    if _sha256(train_finalist_root / "finalist_manifest.json") != manifest.get("source_train_manifest_sha256"):
        raise RuntimeError("independent train manifest binding drift")
    if _sha256(oos_root / "OOS_COMPLETE.json") != manifest.get("source_oos_closure_sha256"):
        raise RuntimeError("independent OOS closure binding drift")

    train_pairs = pd.read_parquet(train_pairs_path).where(pd.notna, None)
    train_candidates = pd.read_parquet(train_candidates_path).where(pd.notna, None)
    oos_pairs = pd.read_parquet(oos_pairs_path).where(pd.notna, None)
    expected_oos = oos_pairs.loc[
        oos_pairs["all_four_economic_gates_positive"].map(bool)
    ].sort_values(["finalist_order", "pair_id"], kind="mergesort")
    expected_ids = expected_oos["pair_id"].astype(str).tolist()
    observed_pairs = pd.read_parquet(
        confirmation_root / "confirmation_pairs.parquet"
    ).where(pd.notna, None)
    observed_candidates = pd.read_parquet(
        confirmation_root / "confirmation_candidates.parquet"
    ).where(pd.notna, None)
    observed_evidence = pd.read_parquet(
        confirmation_root / "adaptive_validation_selection_evidence.parquet"
    ).where(pd.notna, None)
    if observed_pairs["pair_id"].astype(str).tolist() != expected_ids:
        raise RuntimeError("independent confirmation pair selection/order drift")
    if observed_evidence["pair_id"].astype(str).tolist() != expected_ids:
        raise RuntimeError("independent validation evidence selection/order drift")
    if not observed_evidence["all_four_economic_gates_positive"].map(bool).all():
        raise RuntimeError("non-survivor entered confirmation cohort")
    source_selected = train_pairs[
        train_pairs["pair_id"].astype(str).isin(expected_ids)
    ].sort_values(["finalist_order", "pair_id"], kind="mergesort")
    if source_selected["pair_id"].astype(str).tolist() != expected_ids:
        raise RuntimeError("independent train pair source order drift")
    expected_candidate_ids: list[str] = []
    for row in source_selected.to_dict(orient="records"):
        expected_candidate_ids.extend(
            [str(row["primary_candidate_id"]), str(row["control_candidate_id"])]
        )
    if observed_candidates["candidate_id"].astype(str).tolist() != expected_candidate_ids:
        raise RuntimeError("independent confirmation candidate identity/order drift")
    if not set(expected_candidate_ids).issubset(
        set(train_candidates["candidate_id"].astype(str))
    ):
        raise RuntimeError("independent confirmation candidate source drift")
    if summary.get("selected_pair_ids") != expected_ids:
        raise RuntimeError("confirmation summary pair identity drift")
    if int(summary.get("blocked_rows_backfilled") or 0) != 0:
        raise RuntimeError("confirmation backfill detected")
    if summary.get("selector_frozen") is not False:
        raise RuntimeError("confirmation freeze must not create selector")
    if summary.get("selection_payload_sha256") != manifest.get("selection_payload_sha256"):
        raise RuntimeError("confirmation selection payload binding drift")

    receipt = {
        "schema_version": "cn_fixed_survivor_confirmatory_freeze_verification_v1",
        "status": "PASS_INDEPENDENT_FIXED_SURVIVOR_CONFIRMATORY_FREEZE",
        "confirmation_manifest_sha256": _sha256(manifest_path),
        "confirmation_manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "selection_payload_sha256": manifest["selection_payload_sha256"],
        "decoder_id": DECODER_ID,
        "selected_pairs": len(expected_ids),
        "selected_candidate_members": len(expected_candidate_ids),
        "selected_pair_ids": expected_ids,
        "source_finalist_orders": expected_oos["finalist_order"].astype(int).tolist(),
        "blocked_rows_backfilled": 0,
        "selector_frozen": False,
        "holdout_market_or_label_rows_read": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    receipt["receipt_payload_sha256"] = _payload_sha256(receipt)
    _write_json(receipt_path, receipt)
    return {
        "status": receipt["status"],
        "receipt_path": str(receipt_path),
        "receipt_file_sha256": _sha256(receipt_path),
        "receipt_payload_sha256": receipt["receipt_payload_sha256"],
        "selection_payload_sha256": receipt["selection_payload_sha256"],
        "selected_pair_ids": expected_ids,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirmation-root", type=Path, required=True)
    parser.add_argument("--train-finalist-root", type=Path, required=True)
    parser.add_argument("--oos-root", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--receipt-path", type=Path, required=True)
    args = parser.parse_args()
    result = verify_survivors(
        confirmation_root=args.confirmation_root,
        train_finalist_root=args.train_finalist_root,
        oos_root=args.oos_root,
        split_manifest=args.split_manifest,
        receipt_path=args.receipt_path,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
