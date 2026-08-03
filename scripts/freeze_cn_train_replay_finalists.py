"""Freeze train-only finalists from an immutable strict A-share replay.

The freeze consumes only the closed replay result and its already-frozen input.
It never evaluates candidates, reads validation/holdout/2026, promotes alpha, or
backfills replay-blocked pairs when executable-positive supply is below target.
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
from our_system_phase2.services.finalist_economic_admission import (
    POLICY_ID,
    strict_replay_blockers,
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _verify_self_hash(
    payload: Mapping[str, Any], *, field: str, label: str
) -> None:
    expected = str(payload.get(field) or "")
    candidate = dict(payload)
    candidate.pop(field, None)
    observed = _payload_sha256(candidate)
    if not expected or observed != expected:
        raise RuntimeError(
            f"{label} self-hash mismatch: expected={expected} observed={observed}"
        )


def _verify_relative_artifacts(
    payload: Mapping[str, Any], *, root: Path, label: str
) -> None:
    for artifact in payload.get("artifacts") or []:
        path = root / str(artifact["path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"{label} artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"{label} artifact hash mismatch: {path}")


def _verify_absolute_artifacts(
    payload: Mapping[str, Any], *, label: str
) -> None:
    for artifact in payload.get("artifacts") or []:
        path = Path(str(artifact["path"]))
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"{label} artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"{label} artifact hash mismatch: {path}")


def _rank_eligible(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "pair_id",
        "economic_mechanism_id",
        "a_share_replay_status",
        "primary_a_share_executable_net_reward",
        "a_share_executable_net_increment",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise RuntimeError(f"finalist source columns missing: {missing}")
    eligible_mask = frame.apply(
        lambda row: not strict_replay_blockers(row), axis=1
    )
    eligible = frame[eligible_mask].copy()
    ranking_columns = [
        "primary_a_share_executable_net_reward",
        "a_share_executable_net_increment",
    ]
    ascending = [False, False]
    for column in (
        "train_stability_score",
        "train_stability_floor",
        "train_stability_median",
        "search_score",
    ):
        if column in eligible.columns:
            ranking_columns.append(column)
            ascending.append(False)
    ranking_columns.append("pair_id")
    ascending.append(True)
    return eligible.sort_values(
        ranking_columns, ascending=ascending, kind="mergesort"
    ).reset_index(drop=True)


def freeze_train_replay_finalists(
    *,
    replay_root: Path,
    replay_input_root: Path,
    funnel_contract_path: Path,
    output_root: Path,
    generator_repo_sha: str,
) -> dict[str, Any]:
    replay_root = Path(replay_root).resolve()
    replay_input_root = Path(replay_input_root).resolve()
    funnel_contract_path = Path(funnel_contract_path).resolve()
    output_root = Path(output_root).resolve()
    if output_root.exists():
        raise FileExistsError(output_root)
    if len(generator_repo_sha) != 40:
        raise ValueError("generator_repo_sha must be a 40-character Git SHA")

    funnel_contract = _read_json(funnel_contract_path)
    _verify_self_hash(
        funnel_contract,
        field="contract_payload_sha256",
        label="finalist funnel contract",
    )
    if str(funnel_contract.get("status") or "") != (
        "FROZEN_USER_AUTHORIZED_TRAIN_ONLY_FUNNEL"
    ):
        raise RuntimeError("finalist funnel contract is not frozen")
    freeze_policy = dict(funnel_contract.get("finalist_freeze") or {})
    minimum = int(freeze_policy.get("target_pairs_minimum") or 0)
    maximum = int(freeze_policy.get("target_pairs_maximum") or 0)
    if minimum <= 0 or maximum < minimum:
        raise RuntimeError("invalid finalist target range")
    if str(freeze_policy.get("insufficient_supply_action") or "") != (
        "FREEZE_ACTUAL_SMALLER_COUNT_AND_REPORT_DO_NOT_BACKFILL_WITH_BLOCKED_ROWS"
    ):
        raise RuntimeError("unsupported insufficient finalist supply policy")

    input_manifest_path = replay_input_root / "keep_review_manifest.json"
    input_manifest = _read_json(input_manifest_path)
    _verify_self_hash(
        input_manifest,
        field="manifest_payload_sha256",
        label="replay input manifest",
    )
    _verify_relative_artifacts(
        input_manifest, root=replay_input_root, label="replay input"
    )
    if str(input_manifest.get("status") or "") != (
        "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("replay input is not immutable")

    train_closure_path = replay_root.parent / "TRAIN_REPLAY_ONLY_COMPLETE.json"
    train_closure = _read_json(train_closure_path)
    if str(train_closure.get("status") or "") != "TRAIN_REPLAY_ONLY_CLOSED":
        raise RuntimeError("train replay-only root is not closed")
    if any(
        int(train_closure.get(field) or 0) != 0
        for field in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise RuntimeError("sealed reads detected in train replay closure")

    replay_closure_path = replay_root / "REPLAY_COMPLETE.json"
    replay_closure = _read_json(replay_closure_path)
    _verify_self_hash(
        replay_closure,
        field="manifest_body_sha256",
        label="strict replay closure",
    )
    _verify_absolute_artifacts(replay_closure, label="strict replay")
    if _sha256(replay_closure_path) != str(
        train_closure.get("replay_closure_sha256") or ""
    ):
        raise RuntimeError("train root replay closure binding mismatch")
    if str(replay_closure.get("status") or "") != (
        "A_SHARE_REPLAY_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("strict replay is not immutable")

    input_pairs = pd.read_parquet(
        replay_input_root / "keep_review_pairs.parquet"
    ).where(pd.notna, None)
    input_candidates = pd.read_parquet(
        replay_input_root / "keep_review_candidates.parquet"
    ).where(pd.notna, None)
    replay_pairs = pd.read_parquet(
        replay_root / "pair_replay_results.parquet"
    ).where(pd.notna, None)
    replay_candidates = pd.read_parquet(
        replay_root / "candidate_replay_results.parquet"
    ).where(pd.notna, None)

    input_pair_ids = input_pairs["pair_id"].astype(str).tolist()
    replay_pair_ids = replay_pairs["pair_id"].astype(str).tolist()
    if replay_pair_ids != input_pair_ids:
        raise RuntimeError("replay pair identity/order differs from frozen input")
    input_candidate_ids = input_candidates["candidate_id"].astype(str).tolist()
    replay_candidate_ids = replay_candidates["candidate_id"].astype(str).tolist()
    if len(set(input_candidate_ids)) != len(input_candidate_ids):
        raise RuntimeError("duplicate replay-input candidate IDs")
    if set(replay_candidate_ids) != set(input_candidate_ids):
        raise RuntimeError(
            "replay candidate identities differ from frozen input"
        )
    if len(set(replay_pair_ids)) != len(replay_pair_ids):
        raise RuntimeError("duplicate replay pair IDs")
    if len(set(replay_candidate_ids)) != len(replay_candidate_ids):
        raise RuntimeError("duplicate replay candidate IDs")

    source = input_pairs.merge(
        replay_pairs,
        on="pair_id",
        how="left",
        sort=False,
        validate="one_to_one",
        suffixes=("", "_replay"),
    )
    if source["pair_id"].astype(str).tolist() != input_pair_ids:
        raise RuntimeError("finalist source merge reordered pairs")
    eligible = _rank_eligible(source)
    selected_rows: list[int] = []
    seen_mechanisms: set[str] = set()
    for index, row in eligible.iterrows():
        mechanism_id = str(row["economic_mechanism_id"])
        if mechanism_id in seen_mechanisms:
            continue
        if len(selected_rows) >= maximum:
            break
        selected_rows.append(index)
        seen_mechanisms.add(mechanism_id)

    selected = eligible.loc[selected_rows].copy().reset_index(drop=True)
    selected_ids = selected["pair_id"].astype(str).tolist()
    if len(selected) > maximum:
        raise RuntimeError("finalist maximum exceeded")
    if selected["economic_mechanism_id"].astype(str).nunique() != len(selected):
        raise RuntimeError("finalist economic mechanisms are not unique")
    if not (selected["a_share_executable_net_increment"].astype(float) > 0).all():
        raise RuntimeError("nonpositive executable increment entered finalists")
    if not (
        selected["primary_a_share_executable_net_reward"].astype(float) > 0
    ).all():
        raise RuntimeError("nonpositive primary reward entered finalists")
    if not (
        selected["a_share_replay_status"].astype(str)
        == "PAIR_REPLAY_COMPLETE"
    ).all():
        raise RuntimeError("replay-blocked pair entered finalists")

    finalist_rank = {pair_id: rank for rank, pair_id in enumerate(selected_ids, 1)}
    selected["finalist_rank"] = selected["pair_id"].astype(str).map(finalist_rank)
    selected["finalist_outcome"] = "FROZEN_TRAIN_ONLY_FINALIST"
    selected["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    selected["validation_reads"] = 0
    selected["holdout_reads"] = 0
    selected["forward_2026_reads"] = 0
    selected["promotion_eligible"] = False

    selected_candidates = input_candidates[
        input_candidates["pair_id"].astype(str).isin(selected_ids)
    ].copy()
    selected_candidates["finalist_rank"] = selected_candidates[
        "pair_id"
    ].astype(str).map(finalist_rank)
    selected_candidates["finalist_outcome"] = "FROZEN_TRAIN_ONLY_FINALIST"
    selected_candidates["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    selected_candidates["validation_reads"] = 0
    selected_candidates["holdout_reads"] = 0
    selected_candidates["forward_2026_reads"] = 0
    selected_candidates["promotion_eligible"] = False
    if len(selected_candidates) != len(selected) * 2:
        raise RuntimeError("finalist candidate member count drift")

    review = source.copy()
    review["economic_admission_blockers"] = review.apply(
        lambda row: "|".join(strict_replay_blockers(row)), axis=1
    )
    review["finalist_outcome"] = "NOT_ELIGIBLE_OR_OUTSIDE_MAXIMUM"
    review.loc[
        review["a_share_replay_status"].astype(str) != "PAIR_REPLAY_COMPLETE",
        "finalist_outcome",
    ] = "REPLAY_BLOCKED_NO_BACKFILL"
    review.loc[
        (review["a_share_replay_status"].astype(str) == "PAIR_REPLAY_COMPLETE")
        & (
            review["primary_a_share_executable_net_reward"].astype(float)
            <= 0
        ),
        "finalist_outcome",
    ] = "NONPOSITIVE_PRIMARY_ABSOLUTE_REWARD"
    review.loc[
        (review["a_share_replay_status"].astype(str) == "PAIR_REPLAY_COMPLETE")
        & (
            review["primary_a_share_executable_net_reward"].astype(float)
            > 0
        )
        & (review["a_share_executable_net_increment"].astype(float) <= 0),
        "finalist_outcome",
    ] = "NONPOSITIVE_EXECUTABLE_INCREMENT"
    review.loc[
        review["pair_id"].astype(str).isin(selected_ids), "finalist_outcome"
    ] = "FROZEN_TRAIN_ONLY_FINALIST"
    review["finalist_rank"] = review["pair_id"].astype(str).map(finalist_rank)
    review["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    review["validation_reads"] = 0
    review["holdout_reads"] = 0
    review["forward_2026_reads"] = 0
    review["promotion_eligible"] = False

    actual_count = len(selected)
    status = (
        "TRAIN_ONLY_FINALISTS_CLOSED_TARGET_RANGE"
        if actual_count >= minimum
        else "TRAIN_ONLY_FINALISTS_CLOSED_ACTUAL_SMALLER_NO_BACKFILL"
    )
    exposure_counts = Counter(
        selected["portfolio_exposure_family_id"].astype(str)
    )
    output_root.mkdir(parents=True)
    contract = {
        "schema_version": "cn_train_replay_finalist_freeze_v1",
        "status": status,
        "generator_repo_sha": generator_repo_sha,
        "funnel_contract": str(funnel_contract_path),
        "funnel_contract_sha256": _sha256(funnel_contract_path),
        "replay_input_root": str(replay_input_root),
        "replay_input_manifest_sha256": _sha256(input_manifest_path),
        "strict_replay_root": str(replay_root),
        "strict_replay_closure_sha256": _sha256(replay_closure_path),
        "target_pairs_minimum": minimum,
        "target_pairs_maximum": maximum,
        "actual_pairs": actual_count,
        "eligibility": {
            "policy_id": POLICY_ID,
            "a_share_replay_status": "PAIR_REPLAY_COMPLETE",
            "primary_a_share_executable_net_reward": ">0",
            "a_share_executable_net_increment": ">0",
            "economic_mechanism_policy": "UNIQUE",
            "ranking": (
                "primary absolute reward desc, executable increment desc, "
                "train stability desc when present, "
                "search_score desc when present, pair_id asc"
            ),
            "blocked_backfill": "FORBIDDEN",
        },
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
            "successor_search": "FORBIDDEN",
        },
    }
    contract["contract_payload_sha256"] = _payload_sha256(contract)
    contract_path = _write_json(output_root / "finalist_contract.json", contract)

    review_path = output_root / "finalist_review_ledger.parquet"
    pair_path = output_root / "finalist_pairs.parquet"
    candidate_path = output_root / "finalist_candidates.parquet"
    review.to_parquet(review_path, index=False)
    selected.to_parquet(pair_path, index=False)
    selected_candidates.to_parquet(candidate_path, index=False)

    selection_payload = {
        "pair_ids": selected_ids,
        "economic_mechanism_ids": selected[
            "economic_mechanism_id"
        ].astype(str).tolist(),
        "strict_replay_closure_sha256": _sha256(replay_closure_path),
        "contract_payload_sha256": contract["contract_payload_sha256"],
    }
    summary = {
        "schema_version": "cn_train_replay_finalist_freeze_v1",
        "status": status,
        "eligible_replay_complete_positive_pairs": len(eligible),
        "selected_pairs": actual_count,
        "selected_candidate_members": actual_count * 2,
        "selected_pair_ids": selected_ids,
        "selected_economic_mechanism_unique": int(
            selected["economic_mechanism_id"].nunique()
        ),
        "selected_portfolio_exposure_family_counts": dict(
            sorted(exposure_counts.items())
        ),
        "insufficient_supply": actual_count < minimum,
        "blocked_rows_backfilled": 0,
        "selection_payload_sha256": _payload_sha256(selection_payload),
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    summary_path = _write_json(output_root / "finalist_summary.json", summary)

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
        "schema_version": "cn_train_replay_finalist_freeze_v1",
        "status": "TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "generator_repo_sha": generator_repo_sha,
        "strict_replay_closure_sha256": _sha256(replay_closure_path),
        "replay_input_manifest_sha256": _sha256(input_manifest_path),
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "selected_pairs": actual_count,
        "selected_candidate_members": actual_count * 2,
        "artifacts": artifacts,
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    manifest_path = _write_json(output_root / "finalist_manifest.json", manifest)
    return {
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "status": status,
        "eligible_pairs": len(eligible),
        "selected_pairs": actual_count,
        "selected_pair_ids": selected_ids,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-root", type=Path, required=True)
    parser.add_argument("--replay-input-root", type=Path, required=True)
    parser.add_argument("--funnel-contract", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--generator-repo-sha", required=True)
    args = parser.parse_args()
    result = freeze_train_replay_finalists(
        replay_root=args.replay_root,
        replay_input_root=args.replay_input_root,
        funnel_contract_path=args.funnel_contract,
        output_root=args.output_root,
        generator_repo_sha=args.generator_repo_sha,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
