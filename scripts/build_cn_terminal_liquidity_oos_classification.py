"""Classify OOS-positive pairs that strict A-share replay could not flatten.

This is an artifact-only diagnostic.  It joins immutable replay, finalist and
report-only OOS outputs; it never opens train/validation/holdout market data and
never writes optimizer, scheduler, archive or promotion state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


ALLOWED_BLOCKERS = {"FINAL_SESSION_UNLIQUIDATED_HOLDINGS"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(root)).replace("\\", "/"),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _remaining_holdings(payload: Any) -> list[str]:
    row = json.loads(str(payload or "{}"))
    return sorted({str(code) for code in row.get("remaining_holdings") or []})


def classify(
    *,
    candidate_replay_results: Path,
    pair_replay_results: Path,
    oos_pair_results: Path,
    finalist_pairs: Path,
    output_root: Path,
) -> dict[str, Any]:
    inputs = tuple(
        Path(path).resolve()
        for path in (
            candidate_replay_results,
            pair_replay_results,
            oos_pair_results,
            finalist_pairs,
        )
    )
    if any(not path.is_file() for path in inputs):
        missing = [str(path) for path in inputs if not path.is_file()]
        raise FileNotFoundError(",".join(missing))
    output_root = Path(output_root).resolve()
    if output_root.exists():
        raise FileExistsError(output_root)
    output_root.mkdir(parents=True)

    candidates = pd.read_parquet(inputs[0])
    replay_pairs = pd.read_parquet(inputs[1])
    oos_pairs = pd.read_parquet(inputs[2])
    finalists = pd.read_parquet(inputs[3])

    expected_pair_ids = finalists["pair_id"].astype(str).tolist()
    if len(expected_pair_ids) != 32 or len(set(expected_pair_ids)) != 32:
        raise RuntimeError("fixed finalist pair identity/count drift")
    for label, frame in (("replay", replay_pairs), ("oos", oos_pairs)):
        observed = frame["pair_id"].astype(str).tolist()
        if observed != expected_pair_ids:
            raise RuntimeError(f"{label} pair identity/order drift")

    expected_candidates: list[str] = []
    for row in finalists.itertuples(index=False):
        expected_candidates.extend(
            [str(row.primary_candidate_id), str(row.control_candidate_id)]
        )
    observed_candidates = candidates["candidate_id"].astype(str).tolist()
    if observed_candidates != expected_candidates:
        raise RuntimeError("candidate replay identity/order drift")
    if not candidates["candidate_replay_status"].eq(
        "CANDIDATE_REPLAY_BLOCKED"
    ).all():
        raise RuntimeError("classification expects the all-blocked strict cohort")
    blockers = set(candidates["blocker_code"].dropna().astype(str))
    if not blockers or not blockers.issubset(ALLOWED_BLOCKERS):
        raise RuntimeError(f"unexpected replay blockers: {sorted(blockers)}")
    if not replay_pairs["a_share_replay_status"].eq(
        "PAIR_REPLAY_BLOCKED"
    ).all():
        raise RuntimeError("strict replay pair status drift")
    if not oos_pairs["validation_pair_status"].eq("PAIR_EVALUATED").all():
        raise RuntimeError("OOS cohort is not fully evaluated")
    if oos_pairs["interstage_filter_applied"].astype(bool).any():
        raise RuntimeError("interstage filtering detected")

    candidate_payload = {
        str(row.candidate_id): _remaining_holdings(row.result_payload_json)
        for row in candidates.itertuples(index=False)
    }
    finalist_by_pair = finalists.set_index("pair_id", drop=False)
    positive = oos_pairs[oos_pairs["oos_positive_transfer"].astype(bool)].copy()
    rows: list[dict[str, Any]] = []
    for oos in positive.itertuples(index=False):
        pair = finalist_by_pair.loc[str(oos.pair_id)]
        primary_holdings = candidate_payload[str(oos.primary_candidate_id)]
        control_holdings = candidate_payload[str(oos.control_candidate_id)]
        union = sorted(set(primary_holdings) | set(control_holdings))
        shared = sorted(set(primary_holdings) & set(control_holdings))
        rows.append(
            {
                "pair_id": str(oos.pair_id),
                "route_id": str(oos.route_id),
                "primary_candidate_id": str(oos.primary_candidate_id),
                "control_candidate_id": str(oos.control_candidate_id),
                "economic_mechanism_id": str(pair.economic_mechanism_id),
                "portfolio_exposure_family_id": str(
                    pair.portfolio_exposure_family_id
                ),
                "skeleton_id": str(pair.skeleton_id),
                "financial_hypothesis": str(pair.financial_hypothesis),
                "source_checkpoint": str(pair.source_checkpoint),
                "source_backend": str(pair.source_backend),
                "train_search_score": float(pair.search_score),
                "train_mean_one_way_turnover": float(
                    pair.train_mean_one_way_turnover
                ),
                "train_regime_positive_share": float(
                    pair.train_regime_positive_share
                ),
                "train_regime_worst_day_sortino": float(
                    pair.train_regime_worst_day_sortino
                ),
                "validation_search_score": float(oos.validation_search_score),
                "validation_mean_one_way_turnover": float(
                    oos.validation_mean_one_way_turnover
                ),
                "validation_regime_positive_share": float(
                    oos.validation_regime_positive_share
                ),
                "validation_regime_worst_day_sortino": float(
                    oos.validation_regime_worst_day_sortino
                ),
                "validation_worst_horizon_day_sortino": float(
                    oos.validation_worst_horizon_day_sortino
                ),
                "primary_remaining_holdings": json.dumps(
                    primary_holdings, ensure_ascii=False
                ),
                "control_remaining_holdings": json.dumps(
                    control_holdings, ensure_ascii=False
                ),
                "remaining_holdings_union": json.dumps(union, ensure_ascii=False),
                "remaining_holdings_shared": json.dumps(shared, ensure_ascii=False),
                "remaining_holdings_union_count": len(union),
                "remaining_holdings_shared_count": len(shared),
                "strict_replay_status": str(oos.a_share_replay_status),
                "classification_role": (
                    "REPORT_ONLY_DIAGNOSTIC_NOT_EXECUTABLE_OR_PROMOTION"
                ),
            }
        )

    classified = pd.DataFrame(rows)
    if len(classified) != 21:
        raise RuntimeError(
            f"expected 21 OOS-positive blocked pairs, observed {len(classified)}"
        )
    if classified["economic_mechanism_id"].eq("").any():
        raise RuntimeError("economic mechanism binding is incomplete")

    pair_output = output_root / "positive_oos_terminal_liquidity_pairs.parquet"
    classified.to_parquet(pair_output, index=False)
    holding_counts: dict[str, int] = {}
    for encoded in classified["remaining_holdings_union"]:
        for code in json.loads(encoded):
            holding_counts[code] = holding_counts.get(code, 0) + 1

    summary = {
        "schema_version": "cn_terminal_liquidity_oos_classification_v1",
        "status": "REPORT_ARTIFACT_CLASSIFICATION_COMPLETE",
        "fixed_pair_count": len(expected_pair_ids),
        "strict_replay_blocked_pair_count": int(
            replay_pairs["a_share_replay_status"].eq("PAIR_REPLAY_BLOCKED").sum()
        ),
        "oos_positive_blocked_pair_count": len(classified),
        "oos_nonpositive_blocked_pair_count": len(expected_pair_ids) - len(classified),
        "unique_economic_mechanism_count": int(
            classified["economic_mechanism_id"].nunique()
        ),
        "unique_exposure_family_count": int(
            classified["portfolio_exposure_family_id"].nunique()
        ),
        "route_counts": {
            str(key): int(value)
            for key, value in classified["route_id"].value_counts().items()
        },
        "terminal_holding_pair_counts": dict(
            sorted(holding_counts.items(), key=lambda item: (-item[1], item[0]))
        ),
        "validation_search_score_median": float(
            classified["validation_search_score"].median()
        ),
        "validation_search_score_p10": float(
            classified["validation_search_score"].quantile(0.1)
        ),
        "validation_turnover_median": float(
            classified["validation_mean_one_way_turnover"].median()
        ),
        "validation_regime_positive_share_median": float(
            classified["validation_regime_positive_share"].median()
        ),
        "validation_regime_worst_day_sortino_median": float(
            classified["validation_regime_worst_day_sortino"].median()
        ),
        "new_financial_data_reads": 0,
        "new_validation_data_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "source_artifacts": [
            {"path": str(path), "bytes": path.stat().st_size, "sha256": _sha256(path)}
            for path in inputs
        ],
    }
    summary_path = output_root / "terminal_liquidity_oos_classification.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    closure = {
        "schema_version": "cn_terminal_liquidity_oos_classification_closure_v1",
        "status": "CLASSIFICATION_CLOSED_IMMUTABLE",
        "pair_count": len(classified),
        "classification_scope": "EXISTING_REPLAY_AND_REPORT_ONLY_OOS_ARTIFACTS",
        "new_financial_data_reads": 0,
        "new_validation_data_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "oos_feedback_to_search": False,
        "promotion": "FORBIDDEN",
        "artifacts": [
            _artifact(pair_output, root=output_root),
            _artifact(summary_path, root=output_root),
        ],
    }
    closure["manifest_body_sha256"] = _stable_hash(closure)
    closure_path = output_root / "CLASSIFICATION_COMPLETE.json"
    closure_path.write_text(
        json.dumps(closure, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        **summary,
        "closure": str(closure_path),
        "closure_sha256": _sha256(closure_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-replay-results", type=Path, required=True)
    parser.add_argument("--pair-replay-results", type=Path, required=True)
    parser.add_argument("--oos-pair-results", type=Path, required=True)
    parser.add_argument("--finalist-pairs", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            classify(
                candidate_replay_results=args.candidate_replay_results,
                pair_replay_results=args.pair_replay_results,
                oos_pair_results=args.oos_pair_results,
                finalist_pairs=args.finalist_pairs,
                output_root=args.output_root,
            ),
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
