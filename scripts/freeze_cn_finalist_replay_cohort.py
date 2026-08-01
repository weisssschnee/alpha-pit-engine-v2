"""Freeze the deterministic 64-pair train-replay input from a closed review pool.

This is a train-only down-selection.  It never reads financial data and it does
not authorize validation, promotion, or a successor search.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
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


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _verify_payload_hash(
    payload: Mapping[str, Any], *, field: str, label: str
) -> None:
    expected = str(payload.get(field) or "")
    candidate = dict(payload)
    candidate.pop(field, None)
    observed = _payload_sha256(candidate)
    if not expected or observed != expected:
        raise RuntimeError(
            f"{label} self-hash mismatch: expected={expected} "
            f"observed={observed}"
        )


def _verify_artifacts(payload: Mapping[str, Any], *, root: Path) -> None:
    for artifact in payload.get("artifacts") or []:
        path = root / str(artifact["path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"parent artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"parent artifact hash mismatch: {path}")


def _rank(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "pair_id",
        "train_stability_score",
        "train_stability_floor",
        "train_stability_median",
        "search_score",
        "economic_mechanism_id",
        "portfolio_exposure_family_id",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise RuntimeError(f"parent review pair columns missing: {missing}")
    return frame.sort_values(
        [
            "train_stability_score",
            "train_stability_floor",
            "train_stability_median",
            "search_score",
            "pair_id",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)


def freeze_replay_cohort(
    *,
    parent_review_root: Path,
    output_root: Path,
    funnel_contract_path: Path,
    pair_count: int = 64,
) -> dict[str, Any]:
    parent_review_root = Path(parent_review_root).resolve()
    output_root = Path(output_root).resolve()
    funnel_contract_path = Path(funnel_contract_path).resolve()
    if output_root.exists():
        raise FileExistsError(output_root)
    if pair_count <= 0:
        raise ValueError("pair_count must be positive")

    parent_manifest_path = parent_review_root / "keep_review_manifest.json"
    parent_manifest = _read_json(parent_manifest_path)
    if str(parent_manifest.get("status") or "") != (
        "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("parent review pool is not immutable")
    _verify_payload_hash(
        parent_manifest,
        field="manifest_payload_sha256",
        label="parent review manifest",
    )
    _verify_artifacts(parent_manifest, root=parent_review_root)

    funnel_contract = _read_json(funnel_contract_path)
    _verify_payload_hash(
        funnel_contract,
        field="contract_payload_sha256",
        label="finalist funnel contract",
    )
    if str(funnel_contract.get("status") or "") != (
        "FROZEN_USER_AUTHORIZED_TRAIN_ONLY_FUNNEL"
    ):
        raise RuntimeError("finalist funnel contract is not frozen")
    replay_contract = dict(funnel_contract.get("replay_stage") or {})
    if int(replay_contract.get("input_pairs") or 0) != pair_count:
        raise RuntimeError("requested replay pair count differs from contract")

    parent_contract = _read_json(
        parent_review_root / "keep_review_contract.json"
    )
    _verify_payload_hash(
        parent_contract,
        field="contract_payload_sha256",
        label="parent review contract",
    )
    if str(parent_contract.get("selection_mode") or "") != (
        "finalist_funnel_v1"
    ):
        raise RuntimeError("parent review pool is not finalist_funnel_v1")

    parent_pairs = pd.read_parquet(
        parent_review_root / "keep_review_pairs.parquet"
    ).where(pd.notna, None)
    parent_candidates = pd.read_parquet(
        parent_review_root / "keep_review_candidates.parquet"
    ).where(pd.notna, None)
    expected_parent_pairs = int(
        (funnel_contract.get("review_pool") or {}).get("target_pairs") or 0
    )
    if len(parent_pairs) != expected_parent_pairs:
        raise RuntimeError("parent review pool pair count drift")
    if len(parent_candidates) != expected_parent_pairs * 2:
        raise RuntimeError("parent review pool member count drift")
    if parent_pairs["pair_id"].astype(str).nunique() != len(parent_pairs):
        raise RuntimeError("parent review pool pair IDs are not unique")

    ranked = _rank(parent_pairs)
    exposure_cap = math.ceil(pair_count * 0.25)
    selected_indexes: list[int] = []
    selection_reason: dict[str, str] = {}
    seen_mechanisms: set[str] = set()
    exposure_counts: Counter[str] = Counter()
    for index, row in ranked.iterrows():
        pair_id = str(row["pair_id"])
        mechanism_id = str(row["economic_mechanism_id"])
        exposure_id = str(row["portfolio_exposure_family_id"])
        if mechanism_id in seen_mechanisms:
            selection_reason[pair_id] = "MECHANISM_DUPLICATE"
            continue
        if exposure_counts[exposure_id] >= exposure_cap:
            selection_reason[pair_id] = "PORTFOLIO_EXPOSURE_FAMILY_CAP"
            continue
        if len(selected_indexes) >= pair_count:
            selection_reason[pair_id] = "OUTSIDE_REPLAY_INPUT_SIZE"
            continue
        selected_indexes.append(index)
        seen_mechanisms.add(mechanism_id)
        exposure_counts[exposure_id] += 1
        selection_reason[pair_id] = "SELECTED_FOR_STRICT_TRAIN_REPLAY"

    if len(selected_indexes) != pair_count:
        raise RuntimeError(
            f"unable to freeze {pair_count} replay pairs from parent pool"
        )
    selected = ranked.loc[selected_indexes].copy().reset_index(drop=True)
    selected_ids = selected["pair_id"].astype(str).tolist()
    if selected["economic_mechanism_id"].astype(str).nunique() != pair_count:
        raise RuntimeError("selected replay mechanisms are not unique")
    selected_exposure_counts = Counter(
        selected["portfolio_exposure_family_id"].astype(str)
    )
    if max(selected_exposure_counts.values()) > exposure_cap:
        raise RuntimeError("selected replay exposure cap exceeded")

    rank_by_pair = {
        pair_id: rank for rank, pair_id in enumerate(selected_ids, start=1)
    }
    review = ranked.copy()
    review["replay_input_outcome"] = review["pair_id"].astype(str).map(
        selection_reason
    )
    review["replay_input_rank"] = review["pair_id"].astype(str).map(
        rank_by_pair
    )
    review["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    review["financial_result_recomputed"] = False
    review["validation_reads"] = 0
    review["holdout_reads"] = 0
    review["forward_2026_reads"] = 0
    review["promotion_eligible"] = False

    selected["parent_keep_review_rank"] = selected.get("keep_review_rank")
    selected["keep_review_rank"] = range(1, pair_count + 1)
    selected["replay_input_outcome"] = "SELECTED_FOR_STRICT_TRAIN_REPLAY"
    selected["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    selected["financial_result_recomputed"] = False
    selected["validation_reads"] = 0
    selected["holdout_reads"] = 0
    selected["forward_2026_reads"] = 0
    selected["promotion_eligible"] = False

    selected_candidates = parent_candidates[
        parent_candidates["pair_id"].astype(str).isin(selected_ids)
    ].copy()
    selected_candidates["parent_keep_review_rank"] = selected_candidates.get(
        "keep_review_rank"
    )
    selected_candidates["keep_review_rank"] = selected_candidates[
        "pair_id"
    ].astype(str).map(rank_by_pair)
    selected_candidates["replay_input_outcome"] = (
        "SELECTED_FOR_STRICT_TRAIN_REPLAY"
    )
    selected_candidates["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    selected_candidates["financial_result_recomputed"] = False
    selected_candidates["validation_reads"] = 0
    selected_candidates["holdout_reads"] = 0
    selected_candidates["forward_2026_reads"] = 0
    selected_candidates["promotion_eligible"] = False
    selected_candidates = selected_candidates.sort_values(
        ["keep_review_rank", "pair_member_role"], kind="mergesort"
    )
    if len(selected_candidates) != pair_count * 2:
        raise RuntimeError("selected replay candidate member count drift")

    output_root.mkdir(parents=True)
    contract = {
        "schema_version": "cn_finalist_replay_input_freeze_v1",
        "status": "FROZEN_DEVELOPMENT_REPLAY_INPUT_ONLY",
        "parent_review_root": str(parent_review_root),
        "parent_manifest_sha256": _sha256(parent_manifest_path),
        "parent_selection_payload_sha256": str(
            parent_manifest["selection_payload_sha256"]
        ),
        "funnel_contract": str(funnel_contract_path),
        "funnel_contract_sha256": _sha256(funnel_contract_path),
        "cohort_pairs": pair_count,
        "cohort_candidate_members": pair_count * 2,
        "ranking": (
            "train_stability_score desc, train_stability_floor desc, "
            "train_stability_median desc, search_score desc, pair_id asc"
        ),
        "economic_mechanism_policy": "UNIQUE",
        "portfolio_exposure_family_max_share": 0.25,
        "portfolio_exposure_family_cap": exposure_cap,
        "interstage_filtering_before_replay": "FORBIDDEN",
        "authority_boundary": {
            "evidence_scope": "DEVELOPMENT_TRAIN_ONLY",
            "financial_result_recomputed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "optimizer_feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
            "automatic_report_only_oos": "FORBIDDEN",
        },
    }
    contract["contract_payload_sha256"] = _payload_sha256(contract)
    contract_path = _write_json(
        output_root / "keep_review_contract.json", contract
    )
    review_path = output_root / "productive_review_ledger.parquet"
    pair_path = output_root / "keep_review_pairs.parquet"
    candidate_path = output_root / "keep_review_candidates.parquet"
    review.to_parquet(review_path, index=False)
    selected.to_parquet(pair_path, index=False)
    selected_candidates.to_parquet(candidate_path, index=False)

    summary = {
        "schema_version": "cn_finalist_replay_input_freeze_v1",
        "status": "FROZEN_DEVELOPMENT_REPLAY_INPUT_ONLY",
        "parent_review_pairs": len(parent_pairs),
        "selected_pairs": pair_count,
        "selected_candidate_members": pair_count * 2,
        "selected_economic_mechanism_unique": int(
            selected["economic_mechanism_id"].nunique()
        ),
        "selected_portfolio_exposure_family_counts": dict(
            sorted(selected_exposure_counts.items())
        ),
        "portfolio_exposure_family_cap": exposure_cap,
        "selected_pair_ids": selected_ids,
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    summary["selection_payload_sha256"] = _payload_sha256(
        {
            "pair_ids": selected_ids,
            "contract_payload_sha256": contract["contract_payload_sha256"],
            "parent_selection_payload_sha256": str(
                parent_manifest["selection_payload_sha256"]
            ),
        }
    )
    summary_path = _write_json(
        output_root / "keep_review_summary.json", summary
    )
    artifacts = [
        _artifact(path, root=output_root)
        for path in (
            contract_path,
            review_path,
            pair_path,
            candidate_path,
            summary_path,
        )
    ]
    manifest = {
        "schema_version": "cn_finalist_replay_input_freeze_v1",
        "status": "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "parent_review_root": str(parent_review_root),
        "parent_manifest_sha256": _sha256(parent_manifest_path),
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "artifacts": artifacts,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "financial_result_recomputed": False,
        "promotion_eligible": False,
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    manifest_path = _write_json(
        output_root / "keep_review_manifest.json", manifest
    )
    return {
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "selected_pairs": pair_count,
        "selected_candidate_members": pair_count * 2,
        "selected_economic_mechanism_unique": pair_count,
        "selected_portfolio_exposure_family_counts": dict(
            sorted(selected_exposure_counts.items())
        ),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-review-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--funnel-contract", type=Path, required=True)
    parser.add_argument("--pair-count", type=int, default=64)
    args = parser.parse_args()
    result = freeze_replay_cohort(
        parent_review_root=args.parent_review_root,
        output_root=args.output_root,
        funnel_contract_path=args.funnel_contract,
        pair_count=args.pair_count,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
