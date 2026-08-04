"""Independently verify a train-only Decoder V2 finalist freeze."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from scripts.freeze_cn_productive_keep_review_cohort import (
    _payload_sha256,
    _sha256,
    _write_json,
)


DECODER_ID = "TOPK_10_EQUAL"
GATES = (
    "primary_continuous_book_net_reward",
    "matched_continuous_book_net_reward_increment",
    "primary_cumulative_net_return",
    "matched_cumulative_net_return_increment",
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    expected = str(payload.get(field) or "")
    body = dict(payload)
    body.pop(field, None)
    observed = _payload_sha256(body)
    if not expected or expected != observed:
        raise RuntimeError(f"{label} self-hash mismatch")


def _verify_artifacts(
    payload: Mapping[str, Any], *, root: Path, label: str
) -> None:
    for artifact in payload.get("artifacts") or []:
        path = (root / str(artifact["path"])).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"{label} artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"{label} artifact hash mismatch: {path}")


def _verify_source_artifact(
    *, source_root: Path, artifact: Mapping[str, Any], label: str
) -> Path:
    path = (source_root / str(artifact["path"])).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size != int(artifact["bytes"]):
        raise RuntimeError(f"{label} size mismatch: {path}")
    if _sha256(path) != str(artifact["sha256"]):
        raise RuntimeError(f"{label} hash mismatch: {path}")
    return path


def _positive(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0.0


def verify_decoder_v2_finalists(
    *, finalist_root: Path, receipt_path: Path
) -> dict[str, Any]:
    finalist_root = Path(finalist_root).resolve()
    receipt_path = Path(receipt_path).resolve()
    manifest_path = finalist_root / "finalist_manifest.json"
    manifest = _read_json(manifest_path)
    _self_hash(manifest, "manifest_payload_sha256", "finalist manifest")
    if str(manifest.get("status") or "") != (
        "DECODER_V2_TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("finalist manifest is not immutable")
    for artifact in manifest.get("artifacts") or []:
        path = finalist_root / str(artifact["path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"finalist artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"finalist artifact hash mismatch: {path}")

    contract = _read_json(finalist_root / "finalist_contract.json")
    summary = _read_json(finalist_root / "finalist_summary.json")
    _self_hash(contract, "contract_payload_sha256", "finalist contract")
    boundary = contract["authority_boundary"]
    if str(boundary.get("evidence_scope") or "") != "DEVELOPMENT_TRAIN_ONLY":
        raise RuntimeError("finalist evidence scope drift")
    if any(
        int(boundary.get(field) or 0) != 0
        for field in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise RuntimeError("sealed reads in finalist contract")
    for field in (
        "optimizer_feedback_write",
        "scheduler_write",
        "archive_write",
        "promotion",
    ):
        if str(boundary.get(field) or "") != "FORBIDDEN":
            raise RuntimeError(f"prohibited write boundary drift: {field}")
    if str(contract.get("decoder_id") or "") != DECODER_ID:
        raise RuntimeError("finalist decoder drift")
    if str(contract.get("selection_order_policy") or "") != (
        "ORIGINAL_FROZEN_PAIR_ORDER"
    ):
        raise RuntimeError("finalist order policy drift")

    decoder_root = Path(contract["decoder_root"])
    source_root = Path(contract["source_replay_root"])
    decoder_closure_path = decoder_root / "DECODER_V2_COMPLETE.json"
    source_freeze_path = (
        source_root / "prepared" / "finalist_replay_then_oos_freeze.json"
    )
    if _sha256(decoder_closure_path) != str(contract["decoder_closure_sha256"]):
        raise RuntimeError("decoder closure binding drift")
    if _sha256(source_freeze_path) != str(contract["source_freeze_sha256"]):
        raise RuntimeError("source freeze binding drift")
    source_freeze = _read_json(source_freeze_path)
    _self_hash(source_freeze, "manifest_body_sha256", "source freeze")
    if str(source_freeze["selection_payload_sha256"]) != str(
        contract["source_selection_payload_sha256"]
    ):
        raise RuntimeError("source selection binding drift")

    decoder_closure = _read_json(decoder_closure_path)
    _self_hash(decoder_closure, "manifest_body_sha256", "decoder closure")
    _verify_artifacts(
        decoder_closure, root=decoder_root, label="decoder closure"
    )
    if str(decoder_closure.get("status") or "") != (
        "CN_PORTFOLIO_DECODER_V2_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY"
    ):
        raise RuntimeError("independent decoder closure status drift")
    if any(
        int(decoder_closure.get(field) or 0) != 0
        for field in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise RuntimeError("independent decoder sealed reads detected")
    binding = _read_json(decoder_root / "input_binding.json")
    decoder_contracts = {
        str(item["decoder_id"]): item
        for item in binding.get("decoder_contract") or []
    }
    decoder_contract = decoder_contracts.get(DECODER_ID)
    if decoder_contract is None:
        raise RuntimeError("independent decoder contract missing")
    if str(decoder_contract.get("payload_sha256") or "") != str(
        contract["decoder_contract_payload_sha256"]
    ):
        raise RuntimeError("independent decoder contract binding drift")

    source_pair_path = _verify_source_artifact(
        source_root=source_root,
        artifact=source_freeze["pair_artifact"],
        label="independent source pair artifact",
    )
    source_candidate_path = _verify_source_artifact(
        source_root=source_root,
        artifact=source_freeze["candidate_artifact"],
        label="independent source candidate artifact",
    )
    if _sha256(source_pair_path) != str(contract["source_pair_artifact_sha256"]):
        raise RuntimeError("independent source pair contract binding drift")
    if _sha256(source_candidate_path) != str(
        contract["source_candidate_artifact_sha256"]
    ):
        raise RuntimeError("independent source candidate contract binding drift")
    source_pairs = pd.read_parquet(source_pair_path).where(pd.notna, None)
    source_candidates = pd.read_parquet(source_candidate_path).where(
        pd.notna, None
    )
    metrics = pd.read_parquet(
        decoder_root / "decoder_pair_metrics.parquet"
    ).where(pd.notna, None)
    metrics = metrics[metrics["decoder_id"].eq(DECODER_ID)].copy()
    if len(metrics) != 32 or metrics["pair_id"].astype(str).duplicated().any():
        raise RuntimeError("independent decoder metric slice drift")
    if "keep_review_rank" not in source_pairs.columns:
        raise RuntimeError("independent source rank authority missing")
    if source_pairs["keep_review_rank"].duplicated().any():
        raise RuntimeError("independent source rank duplicates")
    source_pairs = source_pairs.sort_values(
        ["keep_review_rank", "pair_id"], kind="mergesort"
    ).reset_index(drop=True)
    source_ids = source_pairs["pair_id"].astype(str).tolist()
    if set(source_ids) != set(map(str, source_freeze["pair_ids"])):
        raise RuntimeError("independent source pair identity drift")
    if set(metrics["pair_id"].astype(str)) != set(source_ids):
        raise RuntimeError("independent decoder/source identity drift")
    joined = source_pairs.merge(
        metrics,
        on="pair_id",
        how="left",
        sort=False,
        validate="one_to_one",
        suffixes=("", "_decoder"),
    )
    expected_ids: list[str] = []
    seen: set[str] = set()
    for _, row in joined.iterrows():
        if not all(_positive(row.get(column)) for column in GATES):
            continue
        mechanism = str(row.get("economic_mechanism_id") or "")
        if not mechanism:
            raise RuntimeError("independent eligible mechanism is empty")
        if mechanism in seen:
            continue
        expected_ids.append(str(row["pair_id"]))
        seen.add(mechanism)

    finalist_pairs = pd.read_parquet(
        finalist_root / "finalist_pairs.parquet"
    ).where(pd.notna, None)
    finalist_candidates = pd.read_parquet(
        finalist_root / "finalist_candidates.parquet"
    ).where(pd.notna, None)
    observed_ids = finalist_pairs["pair_id"].astype(str).tolist()
    if observed_ids != expected_ids:
        raise RuntimeError(
            f"finalist deterministic selection mismatch: expected={expected_ids} "
            f"observed={observed_ids}"
        )
    if not all(
        finalist_pairs[column].map(_positive).all() for column in GATES
    ):
        raise RuntimeError("nonpositive four-gate finalist detected")
    if finalist_pairs["economic_mechanism_id"].astype(str).nunique() != len(
        finalist_pairs
    ):
        raise RuntimeError("finalist mechanisms are not unique")
    if len(finalist_candidates) != len(finalist_pairs) * 2:
        raise RuntimeError("finalist candidate member count drift")
    expected_candidate_ids: list[str] = []
    for row in finalist_pairs.to_dict(orient="records"):
        expected_candidate_ids.extend(
            [str(row["primary_candidate_id"]), str(row["control_candidate_id"])]
        )
    if finalist_candidates["candidate_id"].astype(str).tolist() != (
        expected_candidate_ids
    ):
        raise RuntimeError("finalist candidate identity/order drift")
    if int(summary.get("blocked_rows_backfilled") or 0) != 0:
        raise RuntimeError("blocked row backfill detected")
    if int(summary.get("selected_pairs") or 0) != len(observed_ids):
        raise RuntimeError("finalist summary count drift")
    expected_selected_pairs = contract.get("expected_selected_pairs")
    if expected_selected_pairs is not None and int(expected_selected_pairs) != len(
        observed_ids
    ):
        raise RuntimeError("finalist expected pair count drift")
    if int(contract.get("actual_selected_pairs") or 0) != len(observed_ids):
        raise RuntimeError("finalist contract actual count drift")
    if str(summary["selection_payload_sha256"]) != str(
        manifest["selection_payload_sha256"]
    ):
        raise RuntimeError("finalist selection payload drift")

    receipt = {
        "schema_version": "cn_decoder_v2_finalist_verification_v1",
        "status": "PASS_INDEPENDENT_TRAIN_ONLY_FREEZE_VERIFICATION",
        "finalist_root": str(finalist_root),
        "finalist_manifest_sha256": _sha256(manifest_path),
        "finalist_manifest_payload_sha256": str(
            manifest["manifest_payload_sha256"]
        ),
        "decoder_closure_sha256": _sha256(decoder_closure_path),
        "source_freeze_sha256": _sha256(source_freeze_path),
        "decoder_id": DECODER_ID,
        "selected_pairs": len(observed_ids),
        "selected_candidate_members": len(finalist_candidates),
        "selected_pair_ids": observed_ids,
        "selected_economic_mechanism_unique": int(
            finalist_pairs["economic_mechanism_id"].astype(str).nunique()
        ),
        "economic_gate_columns": list(GATES),
        "blocked_rows_backfilled": 0,
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    receipt["receipt_payload_sha256"] = _payload_sha256(receipt)
    _write_json(receipt_path, receipt)
    return {
        "receipt_path": str(receipt_path),
        "receipt_file_sha256": _sha256(receipt_path),
        "receipt_payload_sha256": receipt["receipt_payload_sha256"],
        "status": receipt["status"],
        "selected_pairs": len(observed_ids),
        "selected_pair_ids": observed_ids,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--finalist-root", type=Path, required=True)
    parser.add_argument("--receipt-path", type=Path, required=True)
    args = parser.parse_args()
    result = verify_decoder_v2_finalists(
        finalist_root=args.finalist_root, receipt_path=args.receipt_path
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
