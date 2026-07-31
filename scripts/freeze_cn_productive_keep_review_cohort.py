from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from our_system_phase2.services.execution_clock_capability import (
    assess_candidate_field_capability,
    load_execution_capability_manifest,
)


COHORT_PAIRS = 64
PRODUCTIVE_DECISION = "TRAIN_REWARD_FOLLOWUP_READY"
REVIEW_OUTCOME_ALLOW = "ALLOW_KEEP_REVIEW"
REVIEW_OUTCOME_HOLD = "HOLD_RESEARCH"
REVIEW_OUTCOME_REJECT_DUPLICATE = "REJECT_DUPLICATE"
SCHEMA_VERSION = "cn_productive_keep_review_freeze_v2"

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
REQUIRED_FINITE_METRICS = (
    "search_score",
    "primary_composite_reward",
    "matched_train_increment",
    "matched_gross_increment",
    "matched_net_increment",
    "matched_trading_cost_difference",
    "train_day_sortino",
    "train_worst_horizon_day_sortino",
    "train_median_horizon_day_sortino",
    "train_horizon_sortino_stdev",
    "train_day_mcmc_p25",
    "train_day_mcmc_prob_gt_0",
    "train_regime_positive_share",
    "train_regime_stability_score",
    "train_rank_ic_mean",
    "train_rank_ic_hit_rate",
    "train_mean_one_way_turnover",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _source_artifact(path: Path, *, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def _count(frame: pd.DataFrame, column: str) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in frame[column].fillna("").astype(str).value_counts().items()
    }


def _source_paths(root: Path) -> list[Path]:
    paths = [
        root / "run_manifest.json",
        root / "final_decision.json",
        root / "train_complete_manifest.json",
        root / "frozen_contract.json",
        root / "candidate_ledger.parquet",
        root / "observation_ledger.parquet",
    ]
    checkpoints = sorted((root / "checkpoints").glob("checkpoint_*"))
    if len(checkpoints) != 8:
        raise RuntimeError(f"expected 8 immutable checkpoints, found {len(checkpoints)}")
    for checkpoint in checkpoints:
        paths.extend(
            [
                checkpoint / "batch_manifest.json",
                checkpoint / "full_behavior.parquet",
            ]
        )
        result_paths = sorted(
            (checkpoint / "phase3cm").glob(
                "*/CN_STREAMING_BACKEND_RESULT.json"
            )
        )
        if len(result_paths) != 2:
            raise RuntimeError(
                f"expected two Phase3CM results under {checkpoint}"
            )
        paths.extend(result_paths)
    missing = [path for path in paths if not path.is_file()]
    if missing:
        raise RuntimeError(
            "missing immutable source artifacts: "
            + ", ".join(str(path) for path in missing)
        )
    return paths


def _load_excluded_cohort_pairs(
    roots: tuple[Path, ...],
) -> tuple[set[str], list[dict[str, Any]]]:
    excluded: set[str] = set()
    bindings: list[dict[str, Any]] = []
    for raw_root in roots:
        root = Path(raw_root).resolve()
        manifest_path = root / "keep_review_manifest.json"
        if not manifest_path.is_file():
            raise RuntimeError(
                f"excluded cohort manifest is missing: {manifest_path}"
            )
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8-sig")
        )
        claimed = str(manifest.get("manifest_payload_sha256") or "")
        body = dict(manifest)
        body.pop("manifest_payload_sha256", None)
        if claimed != _payload_sha256(body):
            raise RuntimeError(
                f"excluded cohort manifest self-hash drift: {manifest_path}"
            )
        if (
            str(manifest.get("status") or "")
            != "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE"
        ):
            raise RuntimeError(
                f"excluded cohort is not immutable: {manifest_path}"
            )
        for artifact in manifest.get("artifacts") or []:
            path = root / str(artifact.get("path") or "")
            if not path.is_file():
                raise RuntimeError(
                    f"excluded cohort artifact missing: {path}"
                )
            if int(artifact.get("bytes") or -1) != path.stat().st_size:
                raise RuntimeError(
                    f"excluded cohort artifact size drift: {path}"
                )
            if str(artifact.get("sha256") or "") != _sha256(path):
                raise RuntimeError(
                    f"excluded cohort artifact hash drift: {path}"
                )
        pair_path = root / "keep_review_pairs.parquet"
        pair_ids = set(
            pd.read_parquet(pair_path, columns=["pair_id"])[
                "pair_id"
            ].astype(str)
        )
        if not pair_ids:
            raise RuntimeError(f"excluded cohort has no pairs: {root}")
        overlap = excluded & pair_ids
        if overlap:
            raise RuntimeError(
                "excluded cohort pair overlap: "
                + ",".join(sorted(overlap)[:5])
            )
        excluded.update(pair_ids)
        bindings.append(
            {
                "root": str(root),
                "manifest": str(manifest_path),
                "manifest_file_sha256": _sha256(manifest_path),
                "manifest_payload_sha256": claimed,
                "selection_payload_sha256": str(
                    manifest.get("selection_payload_sha256") or ""
                ),
                "excluded_pair_count": len(pair_ids),
            }
        )
    return excluded, bindings


def _productive_observations(observations: pd.DataFrame) -> pd.DataFrame:
    return observations[
        (observations["pair_evaluation_status"] == "PAIR_EVALUATED")
        & (_numeric(observations, "search_score") > 0.0)
        & (_numeric(observations, "matched_train_increment") > 0.0)
        & (
            observations["primary_standalone_train_reward_decision"]
            == PRODUCTIVE_DECISION
        )
    ].copy()


def _load_train_reward_rows(
    *,
    root: Path,
    productive: pd.DataFrame,
) -> pd.DataFrame:
    pair_by_primary_candidate = {
        str(row["primary_candidate_id"]): str(row["pair_id"])
        for row in productive[
            ["pair_id", "primary_candidate_id"]
        ].to_dict(orient="records")
    }
    rewards: list[dict[str, Any]] = []
    for result_path in sorted(
        (root / "checkpoints").glob(
            "checkpoint_*/phase3cm/*/CN_STREAMING_BACKEND_RESULT.json"
        )
    ):
        result = json.loads(result_path.read_text(encoding="utf-8-sig"))
        access_contract = {
            "evaluation_role": result.get("evaluation_role"),
            "evaluation_scope": result.get("evaluation_scope"),
            "validation_reads": result.get("validation_reads"),
            "holdout_reads": result.get("holdout_reads"),
            "forward_2026_reads": result.get("forward_2026_reads"),
            "validation_usage": result.get("validation_usage"),
            "holdout_usage": result.get("holdout_usage"),
            "promotion": result.get("promotion"),
        }
        expected = {
            "evaluation_role": "train",
            "evaluation_scope": "full_coordinate_development",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "validation_usage": "not_accessed",
            "holdout_usage": "not_accessed",
            "promotion": "FORBIDDEN",
        }
        if access_contract != expected:
            raise RuntimeError(
                f"sealed-access or role drift in {result_path}: "
                f"{access_contract}"
            )
        result_sha256 = _sha256(result_path)
        for reward in result.get("candidate_rewards", []):
            candidate_id = str(reward.get("candidate_id") or "")
            if candidate_id in pair_by_primary_candidate:
                row = dict(reward)
                row["pair_id"] = pair_by_primary_candidate[candidate_id]
                row["source_checkpoint"] = result_path.parents[2].name
                row["source_backend"] = result_path.parent.name
                row["source_result_sha256"] = result_sha256
                rewards.append(row)
    reward_frame = pd.DataFrame(rewards)
    if len(reward_frame) != len(productive):
        raise RuntimeError(
            "productive-to-primary-reward coverage drift: "
            f"{len(productive)} productive versus {len(reward_frame)} rewards"
        )
    if reward_frame["pair_id"].duplicated().any():
        raise RuntimeError("duplicate productive primary reward rows")
    return reward_frame


def _load_behavior_rows(root: Path, productive_pairs: set[str]) -> pd.DataFrame:
    columns = [
        "pair_id",
        "route_id",
        "portfolio_behavior_family_id",
        "portfolio_behavior_signature_id",
        "structural_family_id",
        "signal_cluster_id",
    ]
    frame = pd.concat(
        [
            pd.read_parquet(
                checkpoint / "full_behavior.parquet",
                columns=columns,
            )
            for checkpoint in sorted(
                (root / "checkpoints").glob("checkpoint_*")
            )
        ],
        ignore_index=True,
    )
    frame = frame[
        frame["pair_id"].astype(str).isin(productive_pairs)
    ].drop_duplicates("pair_id")
    if len(frame) != len(productive_pairs):
        raise RuntimeError("productive-to-full-behavior coverage drift")
    for column in columns[1:]:
        if (frame[column].fillna("").astype(str) == "").any():
            raise RuntimeError(f"blank productive behavior identity: {column}")
    return frame


def _rank_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    ranked = frame.copy()
    for column in HIGHER_IS_BETTER:
        ranked[f"{column}_percentile"] = ranked[column].rank(
            method="average",
            ascending=True,
            pct=True,
        )
    for column in LOWER_IS_BETTER:
        ranked[f"{column}_percentile"] = ranked[column].rank(
            method="average",
            ascending=False,
            pct=True,
        )
    percentile_columns = [
        f"{column}_percentile"
        for column in (*HIGHER_IS_BETTER, *LOWER_IS_BETTER)
    ]
    ranked["train_stability_floor"] = ranked[percentile_columns].min(axis=1)
    ranked["train_stability_median"] = ranked[percentile_columns].median(
        axis=1
    )
    ranked["train_stability_score"] = (
        0.65 * ranked["train_stability_floor"]
        + 0.35 * ranked["train_stability_median"]
    )
    return ranked.sort_values(
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


def _deduplicate_behavior_candidates(
    ranked: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Keep the highest-ranked pair for each behavior family/signature."""

    kept_indexes: list[int] = []
    rejected: dict[str, str] = {}
    seen_families: set[str] = set()
    seen_signatures: set[str] = set()
    for index, row in ranked.iterrows():
        pair_id = str(row["pair_id"])
        family_id = str(row["portfolio_behavior_family_id"])
        signature_id = str(row["portfolio_behavior_signature_id"])
        if family_id in seen_families:
            rejected[pair_id] = "BEHAVIOR_FAMILY_DUPLICATE_LOWER_RANK"
            continue
        if signature_id in seen_signatures:
            rejected[pair_id] = "BEHAVIOR_SIGNATURE_DUPLICATE_LOWER_RANK"
            continue
        kept_indexes.append(index)
        seen_families.add(family_id)
        seen_signatures.add(signature_id)
    return ranked.loc[kept_indexes].reset_index(drop=True), rejected


def _select_with_caps(
    ranked: pd.DataFrame,
    *,
    cohort_pairs: int = COHORT_PAIRS,
) -> list[str]:
    route_cap = math.ceil(cohort_pairs * 0.75)
    structural_cap = math.ceil(cohort_pairs * 0.50)
    signal_cap = math.ceil(cohort_pairs * 0.75)
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
        if route_counts[route_id] >= route_cap:
            return False
        if structural_counts[structural_id] >= structural_cap:
            return False
        if signal_counts[signal_id] >= signal_cap:
            return False
        selected.append(pair_id)
        selected_set.add(pair_id)
        route_counts[route_id] += 1
        structural_counts[structural_id] += 1
        signal_counts[signal_id] += 1
        return True

    records = ranked.to_dict(orient="records")
    for group_column in ("structural_family_id", "signal_cluster_id"):
        seen: set[str] = set()
        for row in records:
            if len(selected) >= cohort_pairs:
                break
            group_id = str(row[group_column])
            if group_id in seen:
                continue
            if add(row):
                seen.add(group_id)
    for row in records:
        if len(selected) >= cohort_pairs:
            break
        add(row)
    if len(selected) != cohort_pairs:
        raise RuntimeError(
            f"cap-constrained keep-review supply {len(selected)} below "
            f"{cohort_pairs}"
        )
    return selected


def _screen_reason(row: Mapping[str, Any]) -> str:
    missing = [
        metric
        for metric in REQUIRED_FINITE_METRICS
        if not math.isfinite(float(row.get(metric, float("nan"))))
    ]
    if missing:
        return "MISSING_FINITE_TRAIN_METRICS:" + ",".join(missing)
    if float(row["matched_gross_increment"]) <= 0.0:
        return "NONPOSITIVE_MATCHED_GROSS_INCREMENT"
    if float(row["matched_net_increment"]) <= 0.0:
        return "NONPOSITIVE_MATCHED_NET_INCREMENT"
    if float(row["train_worst_horizon_day_sortino"]) <= 0.0:
        return "NONPOSITIVE_WORST_HORIZON_DAY_SORTINO"
    if float(row["train_day_mcmc_p25"]) <= 0.0:
        return "NONPOSITIVE_TRAIN_DAY_MCMC_P25"
    if float(row["train_regime_positive_share"]) < 0.5:
        return "TRAIN_REGIME_POSITIVE_SHARE_BELOW_HALF"
    return ""


def _validate_source_closure(root: Path) -> int:
    run_manifest = json.loads(
        (root / "run_manifest.json").read_text(encoding="utf-8-sig")
    )
    final_decision = json.loads(
        (root / "final_decision.json").read_text(encoding="utf-8-sig")
    )
    train_manifest = json.loads(
        (root / "train_complete_manifest.json").read_text(
            encoding="utf-8-sig"
        )
    )
    if run_manifest.get("status") != "CAMPAIGN_CLOSED":
        raise RuntimeError("source run manifest is not CAMPAIGN_CLOSED")
    if final_decision.get("status") != "CAMPAIGN_CLOSED":
        raise RuntimeError("source final decision is not CAMPAIGN_CLOSED")
    if train_manifest.get("status") not in {
        "HYBRID_BOUNDED_LARGE_TRANCHE_COMPLETE",
        "WINNER_GUIDED_LARGE_SEARCH_COMPLETE",
    }:
        raise RuntimeError("source train manifest is not closed")
    productive_candidates = int(
        final_decision.get("productive_candidates") or -1
    )
    if productive_candidates <= 0:
        raise RuntimeError("source productive count drift")
    for payload in (run_manifest, final_decision):
        if int(payload.get("holdout_reads") or 0) != 0:
            raise RuntimeError("source holdout reads are nonzero")
        if int(payload.get("forward_2026_reads") or 0) != 0:
            raise RuntimeError("source 2026 reads are nonzero")
    return productive_candidates


def freeze_cohort(
    *,
    campaign_root: Path,
    output_root: Path,
    cohort_pairs: int = COHORT_PAIRS,
    excluded_routes: tuple[str, ...] = (),
    excluded_cohort_roots: tuple[Path, ...] = (),
    execution_capability_manifest: Path | None = None,
) -> dict[str, Any]:
    campaign_root = campaign_root.resolve()
    output_root = output_root.resolve()
    if output_root.exists() and any(output_root.iterdir()):
        raise RuntimeError(f"output root is not empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)

    if cohort_pairs <= 0:
        raise ValueError("cohort_pairs must be positive")
    source_productive_pairs = _validate_source_closure(campaign_root)
    source_paths = _source_paths(campaign_root)
    source_artifacts = [
        _source_artifact(path, root=campaign_root) for path in source_paths
    ]
    source_hashes_before = {
        artifact["path"]: artifact["sha256"] for artifact in source_artifacts
    }

    observations = pd.read_parquet(
        campaign_root / "observation_ledger.parquet"
    )
    productive = _productive_observations(observations)
    if len(productive) != source_productive_pairs:
        raise RuntimeError(
            f"expected {source_productive_pairs} productive observations, "
            f"found {len(productive)}"
        )
    if productive["pair_id"].duplicated().any():
        raise RuntimeError("productive pair-id duplicate")
    productive_pairs = set(productive["pair_id"].astype(str))
    excluded_pair_ids, excluded_cohort_bindings = (
        _load_excluded_cohort_pairs(excluded_cohort_roots)
    )
    outside_source = excluded_pair_ids - productive_pairs
    if outside_source:
        raise RuntimeError(
            "excluded cohort contains pairs outside source campaign: "
            + ",".join(sorted(outside_source)[:5])
        )

    behavior = _load_behavior_rows(campaign_root, productive_pairs)
    rewards = _load_train_reward_rows(
        root=campaign_root,
        productive=productive,
    )
    candidates = pd.read_parquet(campaign_root / "candidate_ledger.parquet")
    productive_candidates = candidates[
        candidates["pair_id"].astype(str).isin(productive_pairs)
    ].copy()
    member_counts = productive_candidates.groupby("pair_id").size()
    if (
        len(member_counts) != source_productive_pairs
        or not (member_counts == 2).all()
    ):
        raise RuntimeError("productive pair candidate-member coverage drift")
    primary_candidates = productive_candidates[
        productive_candidates["pair_member_role"] == "PRIMARY"
    ].copy()
    if len(primary_candidates) != source_productive_pairs:
        raise RuntimeError("productive primary candidate coverage drift")
    if primary_candidates["exact_identity"].duplicated().any():
        raise RuntimeError("productive primary exact-identity duplicate")

    capability_payload = None
    capability_path = None
    if execution_capability_manifest is not None:
        capability_path = execution_capability_manifest.resolve()
        capability_payload = load_execution_capability_manifest(capability_path)
    pair_capability_rows = []
    for pair_id, members in productive_candidates.groupby("pair_id", sort=False):
        if capability_payload is None:
            assessment = {
                "execution_clock": "NOT_BOUND",
                "required_field_ids": [],
                "missing_field_ids": [],
                "unsupported_field_ids": [],
                "incompatibility_reasons": {},
                "compatible": True,
            }
        else:
            assessment = assess_candidate_field_capability(
                tuple(members["canonical_expression"].astype(str)),
                capability_payload,
            )
        pair_capability_rows.append(
            {
                "pair_id": str(pair_id),
                "execution_clock": assessment["execution_clock"],
                "execution_clock_compatible": bool(assessment["compatible"]),
                "execution_required_field_ids": json.dumps(
                    assessment["required_field_ids"], sort_keys=True
                ),
                "execution_missing_field_ids": json.dumps(
                    assessment["missing_field_ids"], sort_keys=True
                ),
                "execution_unsupported_field_ids": json.dumps(
                    assessment["unsupported_field_ids"], sort_keys=True
                ),
                "execution_incompatibility_reasons": json.dumps(
                    assessment["incompatibility_reasons"], sort_keys=True
                ),
            }
        )
    pair_capability = pd.DataFrame(pair_capability_rows)

    identity_columns = [
        "route_id",
        "structural_family_id",
        "signal_cluster_id",
        "portfolio_behavior_family_id",
        "portfolio_behavior_signature_id",
    ]
    identity_check = productive[
        ["pair_id", *identity_columns]
    ].merge(
        behavior,
        on="pair_id",
        how="inner",
        validate="one_to_one",
        suffixes=("_observation", "_behavior"),
    )
    if len(identity_check) != source_productive_pairs:
        raise RuntimeError("productive behavior identity coverage drift")
    for column in (
        "route_id",
        "portfolio_behavior_family_id",
        "portfolio_behavior_signature_id",
    ):
        if not (
            identity_check[f"{column}_observation"].astype(str)
            == identity_check[f"{column}_behavior"].astype(str)
        ).all():
            raise RuntimeError(
                f"productive behavior identity mismatch: {column}"
            )
    for column in ("structural_family_id", "signal_cluster_id"):
        observation_values = identity_check[
            f"{column}_observation"
        ].fillna("").astype(str)
        behavior_values = identity_check[
            f"{column}_behavior"
        ].fillna("").astype(str)
        if not (
            (observation_values == "") | (observation_values == behavior_values)
        ).all():
            raise RuntimeError(
                f"productive legacy identity conflicts with behavior "
                f"authority: {column}"
            )

    review = (
        productive.drop(columns=identity_columns).merge(
            behavior,
            on="pair_id",
            how="inner",
            validate="one_to_one",
        )
        .merge(
            rewards[
                [
                    "pair_id",
                    "source_checkpoint",
                    "source_backend",
                    "source_result_sha256",
                    "train_day_sortino",
                    "train_worst_horizon_day_sortino",
                    "train_median_horizon_day_sortino",
                    "train_horizon_sortino_stdev",
                    "train_day_mcmc_p25",
                    "train_day_mcmc_prob_gt_0",
                    "train_regime_count",
                    "train_regime_positive_share",
                    "train_regime_median_day_sortino",
                    "train_regime_worst_day_sortino",
                    "train_regime_stability_score",
                    "train_rank_ic_mean",
                    "train_rank_ic_hit_rate",
                    "train_rank_ic_loss",
                    "train_mean_one_way_turnover",
                ]
            ],
            on="pair_id",
            how="inner",
            validate="one_to_one",
        )
        .merge(
            primary_candidates[
                [
                    "pair_id",
                    "candidate_id",
                    "exact_identity",
                    "canonical_expression",
                    "operator_family",
                    "skeleton_id",
                    "source_field_ids",
                    "operator_paths",
                ]
            ].rename(
                columns={
                    "candidate_id": "primary_candidate_id_from_ledger",
                    "exact_identity": "primary_exact_identity",
                }
            ),
            on="pair_id",
            how="inner",
            validate="one_to_one",
        )
        .merge(
            pair_capability,
            on="pair_id",
            how="inner",
            validate="one_to_one",
        )
    )
    if len(review) != source_productive_pairs:
        raise RuntimeError("productive review evidence join drift")
    if not (
        review["primary_candidate_id"].astype(str)
        == review["primary_candidate_id_from_ledger"].astype(str)
    ).all():
        raise RuntimeError("primary candidate identity join drift")

    for column in REQUIRED_FINITE_METRICS:
        review[column] = pd.to_numeric(review[column], errors="coerce")
    excluded_route_set = {
        str(route_id) for route_id in excluded_routes if str(route_id)
    }
    review["train_stability_screen_reason"] = [
        (
            "PRIOR_REVIEW_COHORT_PAIR"
            if str(row["pair_id"]) in excluded_pair_ids
            else (
                "EXECUTION_CLOCK_FIELD_INCOMPATIBLE:"
                + str(row["execution_incompatibility_reasons"])
                if not bool(row["execution_clock_compatible"])
                else (
                    "EXECUTION_CLOCK_ROUTE_EXCLUDED"
                    if str(row["route_id"]) in excluded_route_set
                    else _screen_reason(row)
                )
            )
        )
        for row in review.to_dict(orient="records")
    ]
    review["train_stability_screen_pass"] = (
        review["train_stability_screen_reason"] == ""
    )
    screen_ranked = _rank_candidates(
        review[review["train_stability_screen_pass"]].copy()
    )
    eligible, duplicate_reasons = _deduplicate_behavior_candidates(
        screen_ranked
    )
    selected_pairs = _select_with_caps(
        eligible,
        cohort_pairs=cohort_pairs,
    )
    selected_set = set(selected_pairs)
    rank_by_pair = {
        pair_id: rank
        for rank, pair_id in enumerate(selected_pairs, start=1)
    }
    ranked_metrics = screen_ranked.set_index("pair_id")[
        [
            *[
                f"{column}_percentile"
                for column in (*HIGHER_IS_BETTER, *LOWER_IS_BETTER)
            ],
            "train_stability_floor",
            "train_stability_median",
            "train_stability_score",
        ]
    ]
    review = review.merge(
        ranked_metrics,
        left_on="pair_id",
        right_index=True,
        how="left",
    )
    review["behavior_dedup_reason"] = [
        duplicate_reasons.get(str(pair_id), "")
        for pair_id in review["pair_id"]
    ]
    review["behavior_dedup_pass"] = (
        review["train_stability_screen_pass"]
        & (review["behavior_dedup_reason"] == "")
    )
    review["review_outcome"] = [
        (
            REVIEW_OUTCOME_ALLOW
            if str(pair_id) in selected_set
            else (
                REVIEW_OUTCOME_REJECT_DUPLICATE
                if str(pair_id) in duplicate_reasons
                else REVIEW_OUTCOME_HOLD
            )
        )
        for pair_id in review["pair_id"]
    ]
    review["review_reason"] = [
        (
            f"FROZEN_{cohort_pairs}_PAIR_TRAIN_STABILITY_AND_DIVERSITY_COHORT"
            if str(row["pair_id"]) in selected_set
            else (
                str(row["behavior_dedup_reason"])
                or str(row["train_stability_screen_reason"])
                or "VALID_BUT_OUTSIDE_FROZEN_COHORT"
            )
        )
        for row in review.to_dict(orient="records")
    ]
    review["keep_review_rank"] = [
        rank_by_pair.get(str(pair_id))
        for pair_id in review["pair_id"]
    ]
    review["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    review["financial_result_recomputed"] = False
    review["validation_reads"] = 0
    review["holdout_reads"] = 0
    review["forward_2026_reads"] = 0
    review["finalist_execution_eligible"] = False
    review["validation_eligible"] = False
    review["promotion_eligible"] = False

    selected_review = review[
        review["pair_id"].astype(str).isin(selected_set)
    ].copy()
    selected_review = selected_review.sort_values(
        "keep_review_rank", kind="mergesort"
    )
    if len(selected_review) != cohort_pairs:
        raise RuntimeError("selected keep-review cohort size drift")
    selected_candidates = productive_candidates[
        productive_candidates["pair_id"].astype(str).isin(selected_set)
    ].copy()
    selected_candidates["keep_review_rank"] = selected_candidates[
        "pair_id"
    ].astype(str).map(rank_by_pair)
    selected_candidates["review_outcome"] = REVIEW_OUTCOME_ALLOW
    selected_candidates["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    selected_candidates["finalist_execution_eligible"] = False
    selected_candidates["validation_eligible"] = False
    selected_candidates["promotion_eligible"] = False
    selected_candidates = selected_candidates.sort_values(
        ["keep_review_rank", "pair_member_role"], kind="mergesort"
    )
    if len(selected_candidates) != cohort_pairs * 2:
        raise RuntimeError("selected candidate member count drift")

    contract = {
        "schema_version": SCHEMA_VERSION,
        "status": "FROZEN_DEVELOPMENT_KEEP_REVIEW_ONLY",
        "source_campaign_root": str(campaign_root),
        "source_campaign_status": "CAMPAIGN_CLOSED",
        "source_productive_pairs": source_productive_pairs,
        "cohort_pairs": cohort_pairs,
        "cohort_candidate_members": cohort_pairs * 2,
        "excluded_routes": sorted(excluded_route_set),
        "excluded_prior_cohorts": excluded_cohort_bindings,
        "excluded_prior_pair_count": len(excluded_pair_ids),
        "execution_capability_manifest": (
            {
                "path": str(capability_path),
                "sha256": _sha256(capability_path),
                "execution_clock": str(
                    capability_payload.get("execution_clock") or ""
                ),
                "selection_granularity": "CANDIDATE_EXPRESSION_FIELDS",
            }
            if capability_path is not None and capability_payload is not None
            else None
        ),
        "productive_definition": {
            "pair_evaluation_status": "PAIR_EVALUATED",
            "search_score": ">0",
            "matched_train_increment": ">0",
            "primary_standalone_train_reward_decision": PRODUCTIVE_DECISION,
        },
        "train_stability_screen": {
            "required_finite_metrics": list(REQUIRED_FINITE_METRICS),
            "matched_gross_increment": ">0",
            "matched_net_increment": ">0",
            "train_worst_horizon_day_sortino": ">0",
            "train_day_mcmc_p25": ">0",
            "train_regime_positive_share": ">=0.5",
        },
        "ranking": {
            "higher_is_better": list(HIGHER_IS_BETTER),
            "lower_is_better": list(LOWER_IS_BETTER),
            "percentile_scope": "ALL_SCREEN_PASS_PRODUCTIVE_PAIRS",
            "train_stability_floor": "minimum percentile across 9 metrics",
            "train_stability_median": "median percentile across 9 metrics",
            "train_stability_score": (
                "0.65*train_stability_floor+"
                "0.35*train_stability_median"
            ),
            "tie_break": (
                "score desc,floor desc,median desc,search_score desc,"
                "pair_id asc"
            ),
        },
        "behavior_deduplication": {
            "scope": "TRAIN_STABILITY_SCREEN_PASS_PRODUCTIVE_PAIRS",
            "family_policy": (
                "KEEP_HIGHEST_RANKED_PAIR_PER_BEHAVIOR_FAMILY"
            ),
            "signature_policy": (
                "KEEP_HIGHEST_RANKED_PAIR_PER_BEHAVIOR_SIGNATURE"
            ),
            "lower_ranked_outcome": REVIEW_OUTCOME_REJECT_DUPLICATE,
            "ranking_precedes_deduplication": True,
        },
        "diversity_selection": {
            "anchors": [
                "best screen-pass pair per structural_family_id",
                "best screen-pass pair per signal_cluster_id",
            ],
            "route_max_share": 0.75,
            "structural_family_max_share": 0.50,
            "signal_cluster_max_share": 0.75,
            "caps_are_selection_contract": True,
            "caps_are_not_runtime_stop_gates": True,
        },
        "authority_boundary": {
            "review_outcome": REVIEW_OUTCOME_ALLOW,
            "evidence_scope": "DEVELOPMENT_TRAIN_ONLY",
            "financial_result_recomputed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "optimizer_feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "finalist_execution_eligible": False,
            "validation_eligible": False,
            "promotion_eligible": False,
            "successor_search_authorized": False,
        },
        "source_artifacts": source_artifacts,
    }
    contract["contract_payload_sha256"] = _payload_sha256(contract)
    contract_path = _write_json(
        output_root / "keep_review_contract.json", contract
    )
    review_path = output_root / "productive_review_ledger.parquet"
    review.to_parquet(review_path, index=False)
    pair_path = output_root / "keep_review_pairs.parquet"
    selected_review.to_parquet(pair_path, index=False)
    candidate_path = output_root / "keep_review_candidates.parquet"
    selected_candidates.to_parquet(candidate_path, index=False)

    selected_route_counts = _count(selected_review, "route_id")
    selected_structural_counts = _count(
        selected_review, "structural_family_id"
    )
    selected_signal_counts = _count(selected_review, "signal_cluster_id")
    summary = {
        "schema_version": SCHEMA_VERSION,
        "status": "FROZEN_DEVELOPMENT_KEEP_REVIEW_ONLY",
        "source_productive_pairs": len(review),
        "behavior_family_unique": int(
            review["portfolio_behavior_family_id"].nunique()
        ),
        "behavior_signature_unique": int(
            review["portfolio_behavior_signature_id"].nunique()
        ),
        "behavior_dedup_pass": int(review["behavior_dedup_pass"].sum()),
        "behavior_duplicate_rejected": len(duplicate_reasons),
        "behavior_duplicate_reasons": {
            str(key): int(value)
            for key, value in Counter(duplicate_reasons.values()).items()
        },
        "train_stability_screen_pass": int(
            review["train_stability_screen_pass"].sum()
        ),
        "train_stability_screen_hold": int(
            (~review["train_stability_screen_pass"]).sum()
        ),
        "excluded_prior_pair_count": len(excluded_pair_ids),
        "selected_pairs": len(selected_review),
        "selected_candidate_members": len(selected_candidates),
        "selected_route_counts": selected_route_counts,
        "selected_structural_family_counts": selected_structural_counts,
        "selected_signal_cluster_counts": selected_signal_counts,
        "selected_operator_family_counts": _count(
            selected_review, "operator_family"
        ),
        "selected_checkpoint_counts": _count(
            selected_review, "source_checkpoint"
        ),
        "selected_search_score": {
            "minimum": float(selected_review["search_score"].min()),
            "median": float(selected_review["search_score"].median()),
            "maximum": float(selected_review["search_score"].max()),
        },
        "selected_train_stability_score": {
            "minimum": float(
                selected_review["train_stability_score"].min()
            ),
            "median": float(
                selected_review["train_stability_score"].median()
            ),
            "maximum": float(
                selected_review["train_stability_score"].max()
            ),
        },
        "selected_exact_identity_unique": int(
            selected_review["primary_exact_identity"].nunique()
        ),
        "selected_behavior_family_unique": int(
            selected_review["portfolio_behavior_family_id"].nunique()
        ),
        "selected_behavior_signature_unique": int(
            selected_review["portfolio_behavior_signature_id"].nunique()
        ),
        "review_outcome": REVIEW_OUTCOME_ALLOW,
        "evidence_scope": "DEVELOPMENT_TRAIN_ONLY",
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "finalist_execution_eligible": False,
        "validation_eligible": False,
        "promotion_eligible": False,
        "successor_search_authorized": False,
        "non_claims": [
            "This is not A-share execution or economic evidence.",
            "This is not validation, holdout or promotion evidence.",
            "The cohort does not authorize financial replay or a successor search.",
        ],
    }
    summary["selection_payload_sha256"] = _payload_sha256(
        {
            "pair_ids": selected_pairs,
            "contract_payload_sha256": contract[
                "contract_payload_sha256"
            ],
        }
    )
    summary_path = _write_json(
        output_root / "keep_review_summary.json", summary
    )

    source_hashes_after = {
        artifact["path"]: _sha256(campaign_root / artifact["path"])
        for artifact in source_artifacts
    }
    if source_hashes_after != source_hashes_before:
        raise RuntimeError("immutable source artifact changed during selection")

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
        "schema_version": SCHEMA_VERSION,
        "status": "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_campaign_root": str(campaign_root),
        "source_hashes_unchanged": True,
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "artifacts": artifacts,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "financial_result_recomputed": False,
        "finalist_execution_eligible": False,
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
        "selected_pairs": len(selected_review),
        "selected_candidate_members": len(selected_candidates),
        "selected_route_counts": selected_route_counts,
        "selected_structural_family_counts": selected_structural_counts,
        "selected_signal_cluster_counts": selected_signal_counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--cohort-pairs",
        type=int,
        default=COHORT_PAIRS,
    )
    parser.add_argument(
        "--exclude-route",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--exclude-cohort-root",
        type=Path,
        action="append",
        default=[],
        help=(
            "Previously closed keep-review cohort whose pair IDs may not be "
            "selected again. May be repeated."
        ),
    )
    parser.add_argument(
        "--execution-capability-manifest",
        type=Path,
        required=True,
        help=(
            "Closed zero-financial candidate-level execution-clock "
            "capability authority. Future cohort freezes may not bypass it."
        ),
    )
    args = parser.parse_args()
    result = freeze_cohort(
        campaign_root=args.campaign_root,
        output_root=args.output_root,
        cohort_pairs=args.cohort_pairs,
        excluded_routes=tuple(args.exclude_route),
        excluded_cohort_roots=tuple(args.exclude_cohort_root),
        execution_capability_manifest=args.execution_capability_manifest,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
