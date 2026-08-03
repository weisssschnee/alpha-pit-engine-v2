"""Freeze train-only finalists from immutable final-close MTM evidence.

This is an alternate admission path for a separately authorized report-only
OOS run.  It never claims strict flat-book executability, writes search state,
reads sealed periods, or backfills economically ineligible rows.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from our_system_phase2.services.finalist_economic_admission import (
    MAXIMUM_TERMINAL_HOLDINGS_WEIGHT,
    POLICY_ID,
    mark_to_market_blockers,
)
from scripts.freeze_cn_productive_keep_review_cohort import (
    _artifact,
    _payload_sha256,
    _sha256,
    _write_json,
)


TARGET_PAIRS_MINIMUM = 24
TARGET_PAIRS_MAXIMUM = 32


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _verify_self_hash(
    payload: Mapping[str, Any], *, field: str, label: str
) -> None:
    expected = str(payload.get(field) or "")
    candidate = dict(payload)
    candidate.pop(field, None)
    observed = _payload_sha256(candidate)
    if not expected or expected != observed:
        raise RuntimeError(
            f"{label} self-hash mismatch: expected={expected} observed={observed}"
        )


def _verify_absolute_artifacts(
    payload: Mapping[str, Any], *, label: str
) -> None:
    for artifact in payload.get("artifacts") or []:
        path = Path(str(artifact["path"])).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"{label} artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"{label} artifact hash mismatch: {path}")


def _resolve_frozen_artifact(
    *, root: Path, artifact: Mapping[str, Any], label: str
) -> Path:
    path = (root / str(artifact["path"])).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size != int(artifact["bytes"]):
        raise RuntimeError(f"{label} artifact size mismatch: {path}")
    if _sha256(path) != str(artifact["sha256"]):
        raise RuntimeError(f"{label} artifact hash mismatch: {path}")
    return path


def _bound_freeze(closure: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    matches = [
        Path(str(artifact["path"])).resolve()
        for artifact in closure.get("artifacts") or []
        if Path(str(artifact["path"])).name
        == "finalist_replay_then_oos_freeze.json"
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one frozen cohort artifact, observed={matches}"
        )
    freeze_path = matches[0]
    freeze = _read_json(freeze_path)
    _verify_self_hash(
        freeze, field="manifest_body_sha256", label="frozen cohort"
    )
    if str(freeze.get("status") or "") != "FROZEN_UNCHANGED_FIXED_COHORT":
        raise RuntimeError("frozen cohort status drift")
    return freeze_path, freeze


def _rank_eligible(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "pair_id",
        "economic_mechanism_id",
        "pair_mark_to_market_status",
        "primary_mark_to_market_net_reward",
        "mark_to_market_net_increment",
        "primary_ending_holdings_weight",
        "control_ending_holdings_weight",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise RuntimeError(f"MTM finalist source columns missing: {missing}")
    eligible = frame[
        frame.apply(lambda row: not mark_to_market_blockers(row), axis=1)
    ].copy()
    ranking_columns = [
        "primary_mark_to_market_net_reward",
        "mark_to_market_net_increment",
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


def freeze_mark_to_market_finalists(
    *,
    mark_to_market_root: Path,
    output_root: Path,
    generator_repo_sha: str,
) -> dict[str, Any]:
    mark_to_market_root = Path(mark_to_market_root).resolve()
    output_root = Path(output_root).resolve()
    if output_root.exists():
        raise FileExistsError(output_root)
    if len(generator_repo_sha) != 40:
        raise ValueError("generator_repo_sha must be a 40-character Git SHA")

    closure_path = mark_to_market_root / "MARK_TO_MARKET_REPLAY_COMPLETE.json"
    closure = _read_json(closure_path)
    _verify_self_hash(
        closure, field="manifest_body_sha256", label="MTM replay closure"
    )
    _verify_absolute_artifacts(closure, label="MTM replay closure")
    if str(closure.get("status") or "") != (
        "FINAL_CLOSE_MARK_TO_MARKET_REPLAY_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY"
    ):
        raise RuntimeError("MTM replay is not immutable")
    if any(
        int(closure.get(field) or 0) != 0
        for field in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise RuntimeError("sealed reads detected in MTM closure")
    if not bool(closure.get("no_fabricated_terminal_sale")) or bool(
        closure.get("terminal_sale_fee_applied")
    ):
        raise RuntimeError("MTM ending-book evidence drift")

    freeze_path, freeze = _bound_freeze(closure)
    frozen_root = freeze_path.parents[1]
    pair_input_path = _resolve_frozen_artifact(
        root=frozen_root,
        artifact=freeze["pair_artifact"],
        label="frozen pair input",
    )
    candidate_input_path = _resolve_frozen_artifact(
        root=frozen_root,
        artifact=freeze["candidate_artifact"],
        label="frozen candidate input",
    )
    mtm_pair_path = mark_to_market_root / "pair_mark_to_market_results.parquet"
    mtm_candidate_path = (
        mark_to_market_root / "candidate_mark_to_market_results.parquet"
    )
    source_pairs = pd.read_parquet(pair_input_path).where(pd.notna, None)
    source_candidates = pd.read_parquet(candidate_input_path).where(
        pd.notna, None
    )
    mtm_pairs = pd.read_parquet(mtm_pair_path).where(pd.notna, None)
    mtm_candidates = pd.read_parquet(mtm_candidate_path).where(pd.notna, None)
    pair_ids = [str(value) for value in freeze["pair_ids"]]
    candidate_ids = [str(value) for value in freeze["candidate_ids"]]
    if source_pairs["pair_id"].astype(str).tolist() != pair_ids:
        raise RuntimeError("frozen pair input order drift")
    if mtm_pairs["pair_id"].astype(str).tolist() != pair_ids:
        raise RuntimeError("MTM pair identity/order drift")
    if source_candidates["candidate_id"].astype(str).tolist() != candidate_ids:
        raise RuntimeError("frozen candidate input order drift")
    if mtm_candidates["candidate_id"].astype(str).tolist() != candidate_ids:
        raise RuntimeError("MTM candidate identity/order drift")

    source = source_pairs.merge(
        mtm_pairs,
        on="pair_id",
        how="left",
        sort=False,
        validate="one_to_one",
        suffixes=("", "_mtm"),
    )
    if source["pair_id"].astype(str).tolist() != pair_ids:
        raise RuntimeError("MTM finalist source merge reordered pairs")
    source["economic_admission_blockers"] = source.apply(
        lambda row: "|".join(mark_to_market_blockers(row)), axis=1
    )
    eligible = _rank_eligible(source)
    selected_rows: list[int] = []
    seen_mechanisms: set[str] = set()
    for index, row in eligible.iterrows():
        mechanism_id = str(row["economic_mechanism_id"])
        if mechanism_id in seen_mechanisms:
            continue
        if len(selected_rows) >= TARGET_PAIRS_MAXIMUM:
            break
        selected_rows.append(index)
        seen_mechanisms.add(mechanism_id)
    selected = eligible.loc[selected_rows].copy().reset_index(drop=True)
    selected_ids = selected["pair_id"].astype(str).tolist()
    rank = {pair_id: index for index, pair_id in enumerate(selected_ids, 1)}
    selected["finalist_rank"] = selected["pair_id"].astype(str).map(rank)
    selected["finalist_outcome"] = "FROZEN_MTM_TRAIN_ONLY_FINALIST"
    selected["strict_execution_ready"] = False
    selected["promotion_eligible"] = False

    selected_candidates = source_candidates[
        source_candidates["pair_id"].astype(str).isin(selected_ids)
    ].copy()
    selected_candidates["finalist_rank"] = selected_candidates[
        "pair_id"
    ].astype(str).map(rank)
    selected_candidates["finalist_outcome"] = (
        "FROZEN_MTM_TRAIN_ONLY_FINALIST"
    )
    selected_candidates["strict_execution_ready"] = False
    selected_candidates["promotion_eligible"] = False
    if len(selected_candidates) != len(selected) * 2:
        raise RuntimeError("MTM finalist candidate member count drift")

    review = source.copy()
    review["finalist_outcome"] = "NOT_ELIGIBLE_OR_OUTSIDE_MAXIMUM"
    review.loc[
        review["economic_admission_blockers"].astype(str) != "",
        "finalist_outcome",
    ] = "ECONOMIC_ADMISSION_BLOCKED_NO_BACKFILL"
    review.loc[
        review["pair_id"].astype(str).isin(selected_ids), "finalist_outcome"
    ] = "FROZEN_MTM_TRAIN_ONLY_FINALIST"
    review["finalist_rank"] = review["pair_id"].astype(str).map(rank)
    review["strict_execution_ready"] = False
    review["promotion_eligible"] = False

    output_root.mkdir(parents=True)
    status = (
        "MTM_TRAIN_ONLY_FINALISTS_CLOSED_TARGET_RANGE"
        if len(selected) >= TARGET_PAIRS_MINIMUM
        else "MTM_TRAIN_ONLY_FINALISTS_CLOSED_ACTUAL_SMALLER_NO_BACKFILL"
    )
    contract = {
        "schema_version": "cn_mtm_train_only_finalist_freeze_v1",
        "status": status,
        "generator_repo_sha": generator_repo_sha,
        "mark_to_market_root": str(mark_to_market_root),
        "mark_to_market_closure_sha256": _sha256(closure_path),
        "frozen_cohort_sha256": _sha256(freeze_path),
        "target_pairs_minimum": TARGET_PAIRS_MINIMUM,
        "target_pairs_maximum": TARGET_PAIRS_MAXIMUM,
        "actual_pairs": len(selected),
        "eligibility": {
            "policy_id": POLICY_ID,
            "pair_mark_to_market_status": "PAIR_MARK_TO_MARKET_COMPLETE",
            "primary_mark_to_market_net_reward": ">0",
            "mark_to_market_net_increment": ">0",
            "primary_ending_holdings_weight": (
                f"<={MAXIMUM_TERMINAL_HOLDINGS_WEIGHT}"
            ),
            "control_ending_holdings_weight": (
                f"<={MAXIMUM_TERMINAL_HOLDINGS_WEIGHT}"
            ),
            "economic_mechanism_policy": "UNIQUE",
            "blocked_backfill": "FORBIDDEN",
        },
        "authority_boundary": {
            "evidence_scope": "DEVELOPMENT_TRAIN_ONLY_MTM",
            "strict_execution_ready": False,
            "report_only_oos_requires_separate_authorization": True,
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
        "mark_to_market_closure_sha256": _sha256(closure_path),
        "contract_payload_sha256": contract["contract_payload_sha256"],
    }
    exposure_counts = Counter(
        selected["portfolio_exposure_family_id"].astype(str)
    )
    summary = {
        "schema_version": "cn_mtm_train_only_finalist_freeze_v1",
        "status": status,
        "eligible_pairs": len(eligible),
        "selected_pairs": len(selected),
        "selected_candidate_members": len(selected) * 2,
        "selected_pair_ids": selected_ids,
        "selected_economic_mechanism_unique": int(
            selected["economic_mechanism_id"].nunique()
        ),
        "selected_portfolio_exposure_family_counts": dict(
            sorted(exposure_counts.items())
        ),
        "insufficient_supply": len(selected) < TARGET_PAIRS_MINIMUM,
        "blocked_rows_backfilled": 0,
        "selection_payload_sha256": _payload_sha256(selection_payload),
        "strict_execution_ready": False,
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
        "schema_version": "cn_mtm_train_only_finalist_freeze_v1",
        "status": "MTM_TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "generator_repo_sha": generator_repo_sha,
        "mark_to_market_closure_sha256": _sha256(closure_path),
        "frozen_cohort_sha256": _sha256(freeze_path),
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "selected_pairs": len(selected),
        "selected_candidate_members": len(selected) * 2,
        "artifacts": artifacts,
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "strict_execution_ready": False,
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
        "selected_pairs": len(selected),
        "selected_pair_ids": selected_ids,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mark-to-market-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--generator-repo-sha", required=True)
    args = parser.parse_args()
    result = freeze_mark_to_market_finalists(
        mark_to_market_root=args.mark_to_market_root,
        output_root=args.output_root,
        generator_repo_sha=args.generator_repo_sha,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
