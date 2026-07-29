from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


COHORT_PAIRS = 64
HIGHER_IS_BETTER = (
    "search_score",
    "matched_net_increment",
    "train_worst_horizon_day_sortino",
    "train_day_mcmc_p25",
    "train_regime_stability_score",
    "train_rank_ic_hit_rate",
)
LOWER_IS_BETTER = (
    "train_mean_one_way_turnover",
    "train_horizon_sortino_stdev",
    "matched_trading_cost_difference",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _recompute_ranking(frame: pd.DataFrame) -> pd.DataFrame:
    ranked = frame[frame["train_stability_screen_pass"] == True].copy()  # noqa: E712
    for column in HIGHER_IS_BETTER:
        ranked[f"{column}_verify_percentile"] = ranked[column].rank(
            method="average",
            ascending=True,
            pct=True,
        )
    for column in LOWER_IS_BETTER:
        ranked[f"{column}_verify_percentile"] = ranked[column].rank(
            method="average",
            ascending=False,
            pct=True,
        )
    columns = [
        f"{column}_verify_percentile"
        for column in (*HIGHER_IS_BETTER, *LOWER_IS_BETTER)
    ]
    ranked["verify_floor"] = ranked[columns].min(axis=1)
    ranked["verify_median"] = ranked[columns].median(axis=1)
    ranked["verify_score"] = (
        0.65 * ranked["verify_floor"] + 0.35 * ranked["verify_median"]
    )
    return ranked.sort_values(
        [
            "verify_score",
            "verify_floor",
            "verify_median",
            "search_score",
            "pair_id",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)


def _recompute_selection(ranked: pd.DataFrame) -> list[str]:
    selected: list[str] = []
    selected_set: set[str] = set()
    route_counts: Counter[str] = Counter()
    structural_counts: Counter[str] = Counter()
    signal_counts: Counter[str] = Counter()

    def add(row: Mapping[str, Any]) -> bool:
        pair_id = str(row["pair_id"])
        route_id = str(row["route_id"])
        structural_id = str(row["structural_family_id"])
        signal_id = str(row["signal_cluster_id"])
        if pair_id in selected_set:
            return False
        if route_counts[route_id] >= 48:
            return False
        if structural_counts[structural_id] >= 32:
            return False
        if signal_counts[signal_id] >= 48:
            return False
        selected.append(pair_id)
        selected_set.add(pair_id)
        route_counts[route_id] += 1
        structural_counts[structural_id] += 1
        signal_counts[signal_id] += 1
        return True

    rows = ranked.to_dict(orient="records")
    for column in ("structural_family_id", "signal_cluster_id"):
        anchored: set[str] = set()
        for row in rows:
            group_id = str(row[column])
            if group_id in anchored:
                continue
            if add(row):
                anchored.add(group_id)
    for row in rows:
        if len(selected) == COHORT_PAIRS:
            break
        add(row)
    if len(selected) != COHORT_PAIRS:
        raise RuntimeError("independent cap-constrained supply below 64")
    return selected


def verify(*, campaign_root: Path, selection_root: Path) -> dict[str, Any]:
    campaign_root = campaign_root.resolve()
    selection_root = selection_root.resolve()
    manifest_path = selection_root / "keep_review_manifest.json"
    manifest = _read_json(manifest_path)
    claimed_manifest_payload = manifest.pop("manifest_payload_sha256")
    actual_manifest_payload = _payload_sha256(manifest)
    if claimed_manifest_payload != actual_manifest_payload:
        raise RuntimeError("manifest payload self-hash mismatch")
    manifest["manifest_payload_sha256"] = claimed_manifest_payload
    if manifest.get("status") != "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE":
        raise RuntimeError("selection manifest is not closed immutable")

    declared_paths: set[str] = set()
    for artifact in manifest["artifacts"]:
        relative = str(artifact["path"])
        path = selection_root / relative
        if not path.is_file():
            raise RuntimeError(f"missing declared artifact: {relative}")
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"artifact byte mismatch: {relative}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"artifact hash mismatch: {relative}")
        declared_paths.add(relative)
    expected_paths = {
        "keep_review_contract.json",
        "productive_review_ledger.parquet",
        "keep_review_pairs.parquet",
        "keep_review_candidates.parquet",
        "keep_review_summary.json",
    }
    if declared_paths != expected_paths:
        raise RuntimeError("declared artifact set mismatch")

    contract = _read_json(selection_root / "keep_review_contract.json")
    claimed_contract_payload = contract.pop("contract_payload_sha256")
    actual_contract_payload = _payload_sha256(contract)
    if claimed_contract_payload != actual_contract_payload:
        raise RuntimeError("contract payload self-hash mismatch")
    contract["contract_payload_sha256"] = claimed_contract_payload
    if contract.get("status") != "FROZEN_DEVELOPMENT_KEEP_REVIEW_ONLY":
        raise RuntimeError("contract status drift")
    boundary = contract["authority_boundary"]
    expected_zero_or_false = {
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "finalist_execution_eligible": False,
        "validation_eligible": False,
        "promotion_eligible": False,
        "successor_search_authorized": False,
    }
    for key, expected in expected_zero_or_false.items():
        if boundary.get(key) != expected:
            raise RuntimeError(f"authority boundary drift: {key}")

    source_artifact_count = 0
    for artifact in contract["source_artifacts"]:
        path = campaign_root / str(artifact["path"])
        if not path.is_file():
            raise RuntimeError(f"missing source artifact: {path}")
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"source bytes changed: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"source hash changed: {path}")
        source_artifact_count += 1

    review = pd.read_parquet(
        selection_root / "productive_review_ledger.parquet"
    )
    pairs = pd.read_parquet(selection_root / "keep_review_pairs.parquet")
    candidates = pd.read_parquet(
        selection_root / "keep_review_candidates.parquet"
    )
    summary = _read_json(selection_root / "keep_review_summary.json")
    if len(review) != 2676 or review["pair_id"].nunique() != 2676:
        raise RuntimeError("review ledger productive coverage drift")
    if (
        review["portfolio_behavior_family_id"].nunique() != 2676
        or review["portfolio_behavior_signature_id"].nunique() != 2676
    ):
        raise RuntimeError("behavior deduplication proof drift")
    if len(pairs) != 64 or pairs["pair_id"].nunique() != 64:
        raise RuntimeError("selected pair count drift")
    if (
        len(candidates) != 128
        or candidates["pair_id"].nunique() != 64
        or not (candidates.groupby("pair_id").size() == 2).all()
    ):
        raise RuntimeError("selected pair-member coverage drift")
    if pairs["primary_exact_identity"].nunique() != 64:
        raise RuntimeError("selected primary exact duplicate")
    if pairs["portfolio_behavior_family_id"].nunique() != 64:
        raise RuntimeError("selected behavior-family duplicate")
    if pairs["portfolio_behavior_signature_id"].nunique() != 64:
        raise RuntimeError("selected behavior-signature duplicate")
    if set(pairs["review_outcome"]) != {"ALLOW_KEEP_REVIEW"}:
        raise RuntimeError("selected review outcome drift")
    if not pairs["train_stability_screen_pass"].all():
        raise RuntimeError("screen-failing pair selected")
    for column in (
        "financial_result_recomputed",
        "finalist_execution_eligible",
        "validation_eligible",
        "promotion_eligible",
    ):
        if pairs[column].astype(bool).any():
            raise RuntimeError(f"selected authority overclaim: {column}")
    for column in ("validation_reads", "holdout_reads", "forward_2026_reads"):
        if int(pd.to_numeric(pairs[column]).sum()) != 0:
            raise RuntimeError(f"selected sealed reads nonzero: {column}")

    route_counts = {
        str(key): int(value)
        for key, value in pairs["route_id"].value_counts().items()
    }
    structural_counts = {
        str(key): int(value)
        for key, value in pairs[
            "structural_family_id"
        ].value_counts().items()
    }
    signal_counts = {
        str(key): int(value)
        for key, value in pairs["signal_cluster_id"].value_counts().items()
    }
    if max(route_counts.values()) > 48:
        raise RuntimeError("route concentration cap violated")
    if max(structural_counts.values()) > 32:
        raise RuntimeError("structural concentration cap violated")
    if max(signal_counts.values()) > 48:
        raise RuntimeError("signal concentration cap violated")

    ranked = _recompute_ranking(review)
    recomputed_selection = _recompute_selection(ranked)
    recorded_selection = (
        pairs.sort_values("keep_review_rank", kind="mergesort")["pair_id"]
        .astype(str)
        .tolist()
    )
    if recomputed_selection != recorded_selection:
        raise RuntimeError("independent ranking/selection mismatch")
    recomputed_selection_payload = _payload_sha256(
        {
            "pair_ids": recorded_selection,
            "contract_payload_sha256": claimed_contract_payload,
        }
    )
    if (
        recomputed_selection_payload
        != summary["selection_payload_sha256"]
        or recomputed_selection_payload
        != manifest["selection_payload_sha256"]
    ):
        raise RuntimeError("selection payload hash mismatch")

    screen_hold_reasons = {
        str(key): int(value)
        for key, value in review.loc[
            review["train_stability_screen_pass"] == False,  # noqa: E712
            "train_stability_screen_reason",
        ].value_counts().items()
    }
    return {
        "status": "INDEPENDENT_VERIFICATION_PASS",
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": claimed_manifest_payload,
        "contract_payload_sha256": claimed_contract_payload,
        "selection_payload_sha256": recomputed_selection_payload,
        "declared_artifacts_verified": len(declared_paths),
        "source_artifacts_verified_unchanged": source_artifact_count,
        "review_pairs": len(review),
        "screen_pass": int(review["train_stability_screen_pass"].sum()),
        "screen_hold": int((~review["train_stability_screen_pass"]).sum()),
        "screen_hold_reasons": screen_hold_reasons,
        "selected_pairs": len(pairs),
        "selected_candidate_members": len(candidates),
        "selected_route_counts": route_counts,
        "selected_structural_family_counts": structural_counts,
        "selected_signal_cluster_counts": signal_counts,
        "selected_search_score": {
            "minimum": float(pairs["search_score"].min()),
            "median": float(pairs["search_score"].median()),
            "maximum": float(pairs["search_score"].max()),
        },
        "selected_stability_score": {
            "minimum": float(pairs["train_stability_score"].min()),
            "median": float(pairs["train_stability_score"].median()),
            "maximum": float(pairs["train_stability_score"].max()),
        },
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "finalist_execution_eligible": False,
        "promotion_eligible": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--selection-root", type=Path, required=True)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    result = verify(
        campaign_root=args.campaign_root,
        selection_root=args.selection_root,
    )
    rendered = json.dumps(
        result,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
    )
    if args.receipt:
        args.receipt.resolve().parent.mkdir(parents=True, exist_ok=True)
        args.receipt.resolve().write_text(
            json.dumps(
                result,
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
