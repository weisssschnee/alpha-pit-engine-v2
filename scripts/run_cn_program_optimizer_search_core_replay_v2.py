"""Walk-forward offline replay for Program search-core policies.

Each fold trains only on earlier spent-development cohorts and re-ranks the next
already-spent cohort. This is ranking evidence, not a counterfactual financial
claim: no candidate is re-evaluated and no validation/OOS artifact is opened.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from our_system_phase2.services.program_search_trained_optimizer_v2 import (
    LambdaMARTRankerV2,
    StructuredExtraTreesReplayV1,
    StructuredMultiHeadSearchV2,
    xgboost_available,
    zscore,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

COHORT_ORDER = (
    "TOURNAMENT_V1",
    "SUCCESSOR_D1",
    "D1_FRESH",
    "D1_CONTINUATION_B",
    "D1_CONTINUATION_C",
)
BUDGETS = (24, 48, 96, 168)
TEMPLATES = tuple(x for x in TEMPLATE_ORDER if x != "BASE")


def _read_dataset(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    body = dict(payload)
    claimed = str(body.pop("dataset_payload_sha256", ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError("spent dataset self-hash drift")
    if (
        payload.get("status") != "FROZEN_SPENT_DEVELOPMENT_DATASET_READY"
        or int(payload.get("post_C_prior_exact_count") or 0) != 1310
        or int(payload.get("validation_reads") or 0) != 0
        or int(payload.get("holdout_reads") or 0) != 0
        or int(payload.get("forward_2026_reads") or 0) != 0
    ):
        raise RuntimeError("spent dataset replay boundary drift")
    return payload


def _hash_scores(rows: Sequence[Mapping[str, Any]], seed: int) -> np.ndarray:
    return np.asarray([
        -int(stable_hash({"seed": int(seed), "exact_identity": row["exact_identity"]})[:15], 16)
        for row in rows
    ], dtype=float)


def _historical_scores(rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
    return np.asarray([-float(row["source_order"]) for row in rows], dtype=float)


def _oracle_scores(rows: Sequence[Mapping[str, Any]]) -> np.ndarray:
    output = []
    for row in rows:
        productive = float(bool(row["productive"]))
        admitted = float(bool(row["admitted"]))
        ret = max(0.0, float(row.get("matched_cumulative_net_return_increment") or 0.0))
        reward = max(0.0, float(row.get("matched_net_reward_increment") or 0.0))
        output.append(100.0 * productive + 10.0 * admitted + math.sqrt(ret * reward))
    return np.asarray(output, dtype=float)


def _balanced_order(rows: Sequence[Mapping[str, Any]], scores: Sequence[float]) -> list[int]:
    by_template: dict[str, list[int]] = {template: [] for template in TEMPLATES}
    for index, row in enumerate(rows):
        template = str(row["template_id"])
        if template not in by_template:
            raise RuntimeError(f"unexpected replay template: {template}")
        by_template[template].append(index)
    for template in TEMPLATES:
        by_template[template].sort(
            key=lambda index: (-float(scores[index]), str(rows[index]["exact_identity"]))
        )
    output: list[int] = []
    cursor = {template: 0 for template in TEMPLATES}
    while len(output) < len(rows):
        progressed = False
        for template in TEMPLATES:
            pos = cursor[template]
            if pos < len(by_template[template]):
                output.append(by_template[template][pos])
                cursor[template] = pos + 1
                progressed = True
        if not progressed:
            break
    if len(output) != len(rows) or len(set(output)) != len(rows):
        raise RuntimeError("balanced replay ranking coverage drift")
    return output


def _ranking_metrics(
    rows: Sequence[Mapping[str, Any]],
    order: Sequence[int],
    *,
    train_behavior_pairs: set[str],
) -> dict[str, Any]:
    productive = np.asarray([bool(row["productive"]) for row in rows], dtype=bool)
    admitted = np.asarray([bool(row["admitted"]) for row in rows], dtype=bool)
    metrics: dict[str, Any] = {}
    for requested in BUDGETS:
        k = min(int(requested), len(order))
        selected = list(order[:k])
        productive_count = int(productive[selected].sum())
        admitted_count = int(admitted[selected].sum())
        behavior = {
            str(rows[index].get("behavior_pair_identity"))
            for index in selected
            if rows[index].get("behavior_pair_identity")
            and str(rows[index].get("behavior_pair_identity")) not in train_behavior_pairs
        }
        base_groups = {str(rows[index]["base_group_id"]) for index in selected}
        joint = 0.0
        for index in selected:
            ret = max(0.0, float(rows[index].get("matched_cumulative_net_return_increment") or 0.0))
            reward = max(0.0, float(rows[index].get("matched_net_reward_increment") or 0.0))
            if bool(rows[index]["productive"]):
                joint += math.sqrt(ret * reward)
        metrics[str(requested)] = {
            "effective_k": k,
            "productive": productive_count,
            "productive_precision": productive_count / k if k else 0.0,
            "admitted": admitted_count,
            "admission_rate": admitted_count / k if k else 0.0,
            "new_behavior_pair_count": len(behavior),
            "new_behavior_pair_rate": len(behavior) / k if k else 0.0,
            "distinct_base_group_count": len(base_groups),
            "positive_joint_economic_utility": joint,
        }
    return metrics


def _rank_quality(rows: Sequence[Mapping[str, Any]], scores: Sequence[float]) -> dict[str, Any]:
    from sklearn.metrics import average_precision_score, roc_auc_score

    labels = np.asarray([bool(row["productive"]) for row in rows], dtype=int)
    score = np.asarray(scores, dtype=float)
    if len(set(labels.tolist())) < 2:
        return {"productive_auc": None, "productive_average_precision": None}
    return {
        "productive_auc": float(roc_auc_score(labels, score)),
        "productive_average_precision": float(average_precision_score(labels, score)),
    }


def _score_policies(
    train_rows: Sequence[Mapping[str, Any]],
    test_rows: Sequence[Mapping[str, Any]],
    *, seed: int,
) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    scores: dict[str, np.ndarray] = {
        "UNIFORM_HASH": _hash_scores(test_rows, seed),
        "HISTORICAL_EXECUTION_ORDER": _historical_scores(test_rows),
        "ORACLE_UPPER_BOUND": _oracle_scores(test_rows),
    }
    metadata: dict[str, Any] = {}

    extra_v1 = StructuredExtraTreesReplayV1(seed=seed, beta=1.0).fit(train_rows)
    scores["STRUCTURED_EXTRATREES_V1"] = np.asarray(
        [row["score"] for row in extra_v1.score_rows(test_rows)], dtype=float
    )

    multi = StructuredMultiHeadSearchV2(seed=seed, beta=0.75).fit(train_rows)
    multi_rows = multi.score_rows(test_rows)
    scores["TRAINED_MULTIHEAD_V2"] = np.asarray(
        [row["score"] for row in multi_rows], dtype=float
    )
    metadata["TRAINED_MULTIHEAD_V2"] = {
        "return_scale": multi.return_scale,
        "reward_scale": multi.reward_scale,
        "beta": multi.beta,
    }

    metadata["xgboost_available"] = xgboost_available()
    if xgboost_available():
        try:
            ranker = LambdaMARTRankerV2(seed=seed).fit(train_rows)
            rank_scores = ranker.score_rows(test_rows)
            scores["LAMBDAMART_V2"] = rank_scores
            scores["HYBRID_LAMBDAMART_MULTIHEAD_V2"] = (
                zscore(rank_scores) + zscore(scores["TRAINED_MULTIHEAD_V2"])
            )
            metadata["lambdamart_status"] = "AVAILABLE_AND_FIT"
        except Exception as exc:
            metadata["lambdamart_status"] = "FIT_FAILED"
            metadata["lambdamart_error"] = f"{type(exc).__name__}:{exc}"
    else:
        metadata["lambdamart_status"] = "XGBOOST_UNAVAILABLE"
    return scores, metadata


def run_replay(*, dataset_path: Path, output_path: Path, seed: int) -> dict[str, Any]:
    dataset = _read_dataset(dataset_path.resolve())
    rows = list(dataset["rows"])
    fold_rows: list[dict[str, Any]] = []
    for test_index in range(1, len(COHORT_ORDER)):
        train = [row for row in rows if int(row["cohort_index"]) < test_index]
        test = [row for row in rows if int(row["cohort_index"]) == test_index]
        expected_test = COHORT_ORDER[test_index]
        if not train or not test or {str(row["source_cohort"]) for row in test} != {expected_test}:
            raise RuntimeError(f"walk-forward cohort drift: {expected_test}")
        scores, model_metadata = _score_policies(train, test, seed=seed + test_index)
        train_behavior = {
            str(row["behavior_pair_identity"])
            for row in train if row.get("behavior_pair_identity")
        }
        policies = {}
        for policy, values in scores.items():
            order = _balanced_order(test, values)
            policies[policy] = {
                "rank_quality": _rank_quality(test, values),
                "budgets": _ranking_metrics(test, order, train_behavior_pairs=train_behavior),
                "top_exact_identities": [
                    str(test[index]["exact_identity"])
                    for index in order[: min(24, len(order))]
                ],
            }
        fold_rows.append({
            "fold_id": f"TRAIN_BEFORE_{expected_test}_TEST_{expected_test}",
            "test_cohort": expected_test,
            "train_count": len(train),
            "test_count": len(test),
            "train_productive": sum(bool(row["productive"]) for row in train),
            "test_productive": sum(bool(row["productive"]) for row in test),
            "model_metadata": model_metadata,
            "policies": policies,
        })

    policy_names = sorted({name for fold in fold_rows for name in fold["policies"]})
    aggregate: dict[str, Any] = {}
    for policy in policy_names:
        available_folds = [fold for fold in fold_rows if policy in fold["policies"]]
        by_budget = {}
        for budget in BUDGETS:
            blocks = [fold["policies"][policy]["budgets"][str(budget)] for fold in available_folds]
            by_budget[str(budget)] = {
                "fold_count": len(blocks),
                "productive": sum(int(block["productive"]) for block in blocks),
                "effective_k": sum(int(block["effective_k"]) for block in blocks),
                "productive_precision": (
                    sum(int(block["productive"]) for block in blocks)
                    / max(1, sum(int(block["effective_k"]) for block in blocks))
                ),
                "new_behavior_pair_count": sum(int(block["new_behavior_pair_count"]) for block in blocks),
                "positive_joint_economic_utility": sum(
                    float(block["positive_joint_economic_utility"]) for block in blocks
                ),
            }
        aucs = [
            fold["policies"][policy]["rank_quality"]["productive_auc"]
            for fold in available_folds
            if fold["policies"][policy]["rank_quality"]["productive_auc"] is not None
        ]
        aggregate[policy] = {
            "available_fold_count": len(available_folds),
            "mean_productive_auc": float(np.mean(aucs)) if aucs else None,
            "budgets": by_budget,
        }

    trained_candidates = [
        name for name in (
            "TRAINED_MULTIHEAD_V2",
            "LAMBDAMART_V2",
            "HYBRID_LAMBDAMART_MULTIHEAD_V2",
            "STRUCTURED_EXTRATREES_V1",
        )
        if name in aggregate and aggregate[name]["available_fold_count"] == len(fold_rows)
    ]
    def replay_key(name: str) -> tuple[float, float, float, str]:
        return (
            float(aggregate[name]["budgets"]["24"]["productive_precision"]),
            float(aggregate[name]["budgets"]["48"]["productive_precision"]),
            float(aggregate[name]["mean_productive_auc"] or 0.0),
            name,
        )
    replay_leader = max(trained_candidates, key=replay_key) if trained_candidates else None
    payload: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_search_core_replay_v2",
        "status": "OFFLINE_WALK_FORWARD_SEARCH_CORE_REPLAY_COMPLETE",
        "evidence_role": "DEVELOPMENT_ONLY_RERANKING_EVIDENCE",
        "dataset_file": str(dataset_path.resolve()),
        "dataset_payload_sha256": dataset["dataset_payload_sha256"],
        "cohort_order": list(COHORT_ORDER),
        "folds": fold_rows,
        "aggregate": aggregate,
        "trained_policy_replay_leader": replay_leader,
        "trained_policy_replay_leader_is_fresh_search_authority": False,
        "counterfactual_candidate_selection_claim_authorized": False,
        "financial_evaluation_performed_by_replay": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_challenge_reads": 0,
        "forward_B_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    payload["replay_payload_sha256"] = stable_hash(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=8261701)
    args = parser.parse_args(argv)
    payload = run_replay(dataset_path=args.dataset, output_path=args.output, seed=args.seed)
    print(json.dumps({
        "status": payload["status"],
        "trained_policy_replay_leader": payload["trained_policy_replay_leader"],
        "replay_payload_sha256": payload["replay_payload_sha256"],
        "policies": sorted(payload["aggregate"]),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
