"""Freeze the exact adaptive-validation survivors for one-shot confirmation.

This is intentionally not a selector.  It preserves the original finalist
order, keeps every pair that passed all four already-spent validation economic
gates, and forbids replacement or backfill before the fixed holdout is opened.
Only immutable manifests and small result tables are read here.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from scripts.freeze_cn_productive_keep_review_cohort import (
    _artifact,
    _payload_sha256,
    _sha256,
    _write_json,
)


EXPECTED_SOURCE_PAIRS = 22
EXPECTED_SELECTED_PAIRS = 10
DECODER_ID = "TOPK_10_EQUAL"
OOS_STATUS = "DECODER_V2_TOPK10_EQUAL_REPORT_ONLY_OOS_CLOSED_IMMUTABLE"
FINALIST_STATUS = "DECODER_V2_TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _verify_self_hash(
    payload: Mapping[str, Any], *, field: str, label: str
) -> None:
    declared = str(payload.get(field) or "")
    body = dict(payload)
    body.pop(field, None)
    observed = _payload_sha256(body)
    if not declared or declared != observed:
        raise RuntimeError(
            f"{label} self-hash mismatch: expected={declared} observed={observed}"
        )


def _artifact_by_name(
    root: Path, artifacts: list[Mapping[str, Any]], name: str
) -> Path:
    matches = [row for row in artifacts if Path(str(row["path"])).name == name]
    if len(matches) != 1:
        raise RuntimeError(f"artifact cardinality drift for {name}")
    row = matches[0]
    path = (root / str(row["path"])).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size != int(row["bytes"]):
        raise RuntimeError(f"artifact size drift: {path}")
    if _sha256(path) != str(row["sha256"]):
        raise RuntimeError(f"artifact hash drift: {path}")
    return path


def _holdout_calendar(split_manifest: Path) -> tuple[list[str], str]:
    split_manifest = Path(split_manifest).resolve()
    with split_manifest.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    dates = [str(row["trade_date"]) for row in rows if row["split"] == "holdout"]
    if len(dates) != 48 or len(set(dates)) != 48:
        raise RuntimeError("fixed holdout calendar drift")
    if dates != sorted(dates):
        raise RuntimeError("fixed holdout calendar order drift")
    if any(str(row.get("optimizer_usage") or "") != "report_only" for row in rows if row["split"] == "holdout"):
        raise RuntimeError("holdout optimizer usage drift")
    return dates, _sha256(split_manifest)


def freeze_survivors(
    *,
    train_finalist_root: Path,
    oos_root: Path,
    split_manifest: Path,
    output_root: Path,
    generator_repo_sha: str,
    source_train_authority_root: str,
    source_oos_authority_root: str,
    expected_train_manifest_sha256: str,
    expected_oos_closure_sha256: str,
) -> dict[str, Any]:
    train_finalist_root = Path(train_finalist_root).resolve()
    oos_root = Path(oos_root).resolve()
    output_root = Path(output_root).resolve()
    if output_root.exists():
        raise FileExistsError(output_root)
    if len(generator_repo_sha) != 40:
        raise ValueError("generator_repo_sha must be a 40-character Git SHA")

    train_manifest_path = train_finalist_root / "finalist_manifest.json"
    oos_closure_path = oos_root / "OOS_COMPLETE.json"
    if _sha256(train_manifest_path) != expected_train_manifest_sha256:
        raise RuntimeError("train finalist manifest binding drift")
    if _sha256(oos_closure_path) != expected_oos_closure_sha256:
        raise RuntimeError("OOS closure binding drift")

    train_manifest = _read_json(train_manifest_path)
    _verify_self_hash(
        train_manifest,
        field="manifest_payload_sha256",
        label="train finalist manifest",
    )
    if train_manifest.get("status") != FINALIST_STATUS:
        raise RuntimeError("train finalist source is not immutable")
    if int(train_manifest.get("selected_pairs") or 0) != EXPECTED_SOURCE_PAIRS:
        raise RuntimeError("train finalist pair count drift")
    if int(train_manifest.get("selected_candidate_members") or 0) != 44:
        raise RuntimeError("train finalist member count drift")
    if any(
        int(train_manifest.get(field) or 0) != 0
        for field in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise RuntimeError("train finalist sealed reads detected")
    train_artifacts = list(train_manifest.get("artifacts") or ())
    train_pairs_path = _artifact_by_name(
        train_finalist_root, train_artifacts, "finalist_pairs.parquet"
    )
    train_candidates_path = _artifact_by_name(
        train_finalist_root, train_artifacts, "finalist_candidates.parquet"
    )

    oos_closure = _read_json(oos_closure_path)
    _verify_self_hash(
        oos_closure, field="manifest_body_sha256", label="OOS closure"
    )
    if oos_closure.get("status") != OOS_STATUS:
        raise RuntimeError("adaptive OOS source is not immutable")
    required_oos = {
        "pair_count": EXPECTED_SOURCE_PAIRS,
        "candidate_member_count": 44,
        "decoder_id": DECODER_ID,
        "interstage_filter_applied": False,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    drift = [key for key, value in required_oos.items() if oos_closure.get(key) != value]
    if drift:
        raise RuntimeError("adaptive OOS closure drift: " + ",".join(drift))
    oos_pairs_path = _artifact_by_name(
        oos_root,
        list(oos_closure.get("artifacts") or ()),
        "oos_pair_metrics.parquet",
    )

    train_pairs = pd.read_parquet(train_pairs_path).where(pd.notna, None)
    train_candidates = pd.read_parquet(train_candidates_path).where(pd.notna, None)
    oos_pairs = pd.read_parquet(oos_pairs_path).where(pd.notna, None)
    if len(train_pairs) != EXPECTED_SOURCE_PAIRS or len(oos_pairs) != EXPECTED_SOURCE_PAIRS:
        raise RuntimeError("source pair cardinality drift")
    if train_pairs["finalist_order"].astype(int).tolist() != list(range(1, 23)):
        raise RuntimeError("train finalist order drift")
    if oos_pairs["finalist_order"].astype(int).tolist() != list(range(1, 23)):
        raise RuntimeError("adaptive OOS order drift")
    if train_pairs["pair_id"].astype(str).tolist() != oos_pairs["pair_id"].astype(str).tolist():
        raise RuntimeError("train/OOS pair identity or order drift")

    gate = oos_pairs["all_four_economic_gates_positive"].map(bool)
    selected_evidence = oos_pairs.loc[gate].copy().reset_index(drop=True)
    if len(selected_evidence) != EXPECTED_SELECTED_PAIRS:
        raise RuntimeError(
            "adaptive survivor count drift: "
            f"expected={EXPECTED_SELECTED_PAIRS} observed={len(selected_evidence)}"
        )
    selected_ids = selected_evidence["pair_id"].astype(str).tolist()
    selected_orders = selected_evidence["finalist_order"].astype(int).tolist()
    selected_pairs = train_pairs[
        train_pairs["pair_id"].astype(str).isin(selected_ids)
    ].copy()
    selected_pairs = selected_pairs.sort_values(
        ["finalist_order", "pair_id"], kind="mergesort"
    ).reset_index(drop=True)
    if selected_pairs["pair_id"].astype(str).tolist() != selected_ids:
        raise RuntimeError("confirmation pair order drift")
    selected_pairs["confirmation_order"] = range(1, 11)
    selected_pairs["confirmation_cohort_outcome"] = (
        "FROZEN_ADAPTIVE_VALIDATION_SURVIVOR_INTENTION_TO_TREAT"
    )
    selected_pairs["confirmation_evidence_scope"] = (
        "VALIDATION_INFORMED_FIXED_COHORT_PENDING_UNTOUCHED_CONFIRMATION"
    )
    selected_pairs["promotion_eligible"] = False

    role_order = {"PRIMARY": 0, "CONTROL": 1}
    pair_order = dict(zip(selected_ids, range(1, 11)))
    selected_candidates = train_candidates[
        train_candidates["pair_id"].astype(str).isin(selected_ids)
    ].copy()
    if len(selected_candidates) != 20:
        raise RuntimeError("confirmation candidate member count drift")
    selected_candidates["_pair_order"] = selected_candidates["pair_id"].astype(str).map(pair_order)
    selected_candidates["_role_order"] = selected_candidates["pair_member_role"].astype(str).map(role_order)
    if selected_candidates[["_pair_order", "_role_order"]].isna().any().any():
        raise RuntimeError("confirmation candidate role/order drift")
    selected_candidates = selected_candidates.sort_values(
        ["_pair_order", "_role_order", "candidate_id"], kind="mergesort"
    ).drop(columns=["_pair_order", "_role_order"]).reset_index(drop=True)
    expected_candidate_ids: list[str] = []
    for row in selected_pairs.to_dict(orient="records"):
        expected_candidate_ids.extend(
            [str(row["primary_candidate_id"]), str(row["control_candidate_id"])]
        )
    if selected_candidates["candidate_id"].astype(str).tolist() != expected_candidate_ids:
        raise RuntimeError("confirmation candidate identity/order drift")
    selected_candidates["confirmation_order"] = selected_candidates["pair_id"].astype(str).map(pair_order)
    selected_candidates["confirmation_cohort_outcome"] = (
        "FROZEN_ADAPTIVE_VALIDATION_SURVIVOR_INTENTION_TO_TREAT"
    )
    selected_candidates["confirmation_evidence_scope"] = (
        "VALIDATION_INFORMED_FIXED_COHORT_PENDING_UNTOUCHED_CONFIRMATION"
    )
    selected_candidates["promotion_eligible"] = False

    holdout_dates, split_hash = _holdout_calendar(split_manifest)
    output_root.mkdir(parents=True)
    contract = {
        "schema_version": "cn_fixed_survivor_confirmatory_freeze_v1",
        "status": "FIXED_SURVIVOR_CONFIRMATORY_CONTRACT_ACTIVE_BEFORE_HOLDOUT_ACCESS",
        "generator_repo_sha": generator_repo_sha,
        "source_train_authority_root": source_train_authority_root,
        "source_oos_authority_root": source_oos_authority_root,
        "source_train_manifest_sha256": expected_train_manifest_sha256,
        "source_oos_closure_sha256": expected_oos_closure_sha256,
        "source_train_pairs_sha256": _sha256(train_pairs_path),
        "source_train_candidates_sha256": _sha256(train_candidates_path),
        "source_oos_pair_metrics_sha256": _sha256(oos_pairs_path),
        "decoder_id": DECODER_ID,
        "selection_basis": "EXACT_ALL_FOUR_ADAPTIVE_VALIDATION_SURVIVORS",
        "claim_under_test": "FIXED_CURRENT_ALPHA_COHORT_SURVIVES_UNTOUCHED_CONFIRMATION",
        "selector_claim_authorized": False,
        "source_finalist_orders": selected_orders,
        "selection_order_policy": "ORIGINAL_FINALIST_ORDER",
        "replacement": "FORBIDDEN",
        "backfill": "FORBIDDEN",
        "post_freeze_filtering": "FORBIDDEN",
        "fixed_split_manifest_sha256": split_hash,
        "fixed_holdout_trade_date_count": len(holdout_dates),
        "fixed_holdout_first_trade_date": holdout_dates[0],
        "fixed_holdout_last_trade_date": holdout_dates[-1],
        "confirmation_role": "holdout_confirmatory_report_only",
        "confirmation_access_count": "EXACTLY_ONE",
        "holdout_market_or_label_rows_read_before_freeze": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "automatic_promotion": "FORBIDDEN",
        "evidence_grade_after_run": "WEAK_DUE_TO_48_DAILY_SESSIONS",
    }
    contract["contract_payload_sha256"] = _payload_sha256(contract)
    contract_path = _write_json(output_root / "confirmation_contract.json", contract)
    pair_path = output_root / "confirmation_pairs.parquet"
    candidate_path = output_root / "confirmation_candidates.parquet"
    evidence_path = output_root / "adaptive_validation_selection_evidence.parquet"
    selected_pairs.to_parquet(pair_path, index=False)
    selected_candidates.to_parquet(candidate_path, index=False)
    selected_evidence.to_parquet(evidence_path, index=False)

    selection_payload = {
        "pair_ids": selected_ids,
        "candidate_ids": expected_candidate_ids,
        "source_finalist_orders": selected_orders,
        "decoder_id": DECODER_ID,
        "source_oos_closure_sha256": expected_oos_closure_sha256,
        "fixed_split_manifest_sha256": split_hash,
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "selection_order_policy": "ORIGINAL_FINALIST_ORDER",
    }
    summary = {
        "schema_version": "cn_fixed_survivor_confirmatory_freeze_v1",
        "status": "FIXED_TEN_ADAPTIVE_VALIDATION_SURVIVORS_FROZEN_NO_BACKFILL",
        "source_pairs": EXPECTED_SOURCE_PAIRS,
        "selected_pairs": len(selected_pairs),
        "selected_candidate_members": len(selected_candidates),
        "selected_pair_ids": selected_ids,
        "selected_candidate_ids": expected_candidate_ids,
        "source_finalist_orders": selected_orders,
        "selection_payload_sha256": _payload_sha256(selection_payload),
        "validation_labels_used_for_freeze": True,
        "selector_frozen": False,
        "blocked_rows_backfilled": 0,
        "holdout_market_or_label_rows_read": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    summary_path = _write_json(output_root / "confirmation_summary.json", summary)
    artifacts = [
        _artifact(path, root=output_root)
        for path in (contract_path, pair_path, candidate_path, evidence_path, summary_path)
    ]
    manifest = {
        "schema_version": "cn_fixed_survivor_confirmatory_freeze_v1",
        "status": "FIXED_SURVIVOR_CONFIRMATORY_FREEZE_CLOSED_IMMUTABLE",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "generator_repo_sha": generator_repo_sha,
        "source_train_manifest_sha256": expected_train_manifest_sha256,
        "source_oos_closure_sha256": expected_oos_closure_sha256,
        "fixed_split_manifest_sha256": split_hash,
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "selected_pairs": len(selected_pairs),
        "selected_candidate_members": len(selected_candidates),
        "artifacts": artifacts,
        "holdout_market_or_label_rows_read": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    manifest_path = _write_json(output_root / "CONFIRMATION_COHORT_FROZEN.json", manifest)
    return {
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "selected_pair_ids": selected_ids,
        "selected_pairs": len(selected_pairs),
        "selected_candidate_members": len(selected_candidates),
        "holdout_market_or_label_rows_read": 0,
        "forward_2026_reads": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-finalist-root", type=Path, required=True)
    parser.add_argument("--oos-root", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--generator-repo-sha", required=True)
    parser.add_argument("--source-train-authority-root", required=True)
    parser.add_argument("--source-oos-authority-root", required=True)
    parser.add_argument("--expected-train-manifest-sha256", required=True)
    parser.add_argument("--expected-oos-closure-sha256", required=True)
    args = parser.parse_args()
    result = freeze_survivors(
        train_finalist_root=args.train_finalist_root,
        oos_root=args.oos_root,
        split_manifest=args.split_manifest,
        output_root=args.output_root,
        generator_repo_sha=args.generator_repo_sha,
        source_train_authority_root=args.source_train_authority_root,
        source_oos_authority_root=args.source_oos_authority_root,
        expected_train_manifest_sha256=args.expected_train_manifest_sha256,
        expected_oos_closure_sha256=args.expected_oos_closure_sha256,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
