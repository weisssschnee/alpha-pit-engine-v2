from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import numpy as np

from our_system_phase2.services.execution_clock_capability import (
    load_execution_capability_manifest,
)


SUPPORTED_SCHEMA_VERSIONS = {
    "cn_productive_keep_review_freeze_v1",
    "cn_productive_keep_review_freeze_v2",
    "cn_productive_keep_review_freeze_v3",
}
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


def _deduplicate_behavior_candidates(
    ranked: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, str]]:
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


def _normalized_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    parsed = value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            parsed = [stripped]
    if isinstance(parsed, np.ndarray):
        parsed = parsed.tolist()
    if isinstance(parsed, (list, tuple, set)):
        return sorted({str(item) for item in parsed if str(item)})
    return [str(parsed)]


def _identity(prefix: str, payload: Mapping[str, Any]) -> str:
    return f"{prefix}.{_payload_sha256(payload)[:32]}"


def _finite_or_none(value: Any) -> float | None:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    return output if math.isfinite(output) else None


def _recompute_finalist_identities(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    mechanism_ids: list[str] = []
    exposure_ids: list[str] = []
    support_deciles: list[int] = []
    turnover_deciles: list[int] = []
    for row in output.to_dict(orient="records"):
        mechanism_payload = {
            "route_id": str(row.get("route_id") or ""),
            "financial_hypothesis": str(
                row.get("financial_hypothesis") or ""
            ),
            "operator_family": str(row.get("operator_family") or ""),
            "skeleton_id": str(row.get("skeleton_id") or ""),
            "source_field_ids": _normalized_string_list(
                row.get("source_field_ids")
            ),
            "operator_paths": _normalized_string_list(
                row.get("operator_paths")
            ),
            "event_state_family": str(
                row.get("event_state_family") or ""
            ),
            "primitive_family": str(row.get("primitive_family") or ""),
            "horizon_bucket": str(row.get("horizon_bucket") or ""),
        }
        support = _finite_or_none(row.get("primary_support_rate"))
        turnover = _finite_or_none(row.get("primary_mean_turnover"))
        support_decile = int(round(support * 10.0)) if support is not None else -1
        turnover_decile = (
            int(round(turnover * 10.0)) if turnover is not None else -1
        )
        exposure_payload = {
            "event_state_family": str(
                row.get("event_state_family") or ""
            ),
            "horizon_bucket": str(row.get("horizon_bucket") or ""),
            "primary_support_rate_decile": support_decile,
            "primary_behavior_turnover_decile": turnover_decile,
        }
        mechanism_ids.append(
            _identity("cn.economic_mechanism", mechanism_payload)
        )
        exposure_ids.append(
            _identity("cn.portfolio_exposure_family", exposure_payload)
        )
        support_deciles.append(support_decile)
        turnover_deciles.append(turnover_decile)
    output["verify_economic_mechanism_id"] = mechanism_ids
    output["verify_portfolio_exposure_family_id"] = exposure_ids
    output["verify_primary_support_rate_decile"] = support_deciles
    output["verify_primary_behavior_turnover_decile"] = turnover_deciles
    return output


def _deduplicate_mechanism_candidates(
    ranked: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, str]]:
    kept_indexes: list[int] = []
    rejected: dict[str, str] = {}
    seen: set[str] = set()
    for index, row in ranked.iterrows():
        pair_id = str(row["pair_id"])
        mechanism_id = str(row["economic_mechanism_id"])
        if mechanism_id in seen:
            rejected[pair_id] = "ECONOMIC_MECHANISM_DUPLICATE_LOWER_RANK"
            continue
        kept_indexes.append(index)
        seen.add(mechanism_id)
    return ranked.loc[kept_indexes].reset_index(drop=True), rejected


def _recompute_selection(
    ranked: pd.DataFrame,
    *,
    cohort_pairs: int,
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

    rows = ranked.to_dict(orient="records")
    for column in ("structural_family_id", "signal_cluster_id"):
        anchored: set[str] = set()
        for row in rows:
            if len(selected) >= cohort_pairs:
                break
            group_id = str(row[column])
            if group_id in anchored:
                continue
            if add(row):
                anchored.add(group_id)
    for row in rows:
        if len(selected) >= cohort_pairs:
            break
        add(row)
    if len(selected) != cohort_pairs:
        raise RuntimeError(
            "independent cap-constrained supply below "
            f"{cohort_pairs}"
        )
    return selected


def _recompute_finalist_selection(
    ranked: pd.DataFrame,
    *,
    cohort_pairs: int,
) -> tuple[list[str], dict[str, Any]]:
    exposure_cap = math.ceil(cohort_pairs * 0.25)
    route_group_count = int(ranked["route_id"].nunique())
    structural_group_count = int(
        ranked["structural_family_id"].nunique()
    )
    signal_group_count = int(ranked["signal_cluster_id"].nunique())
    route_cap = (
        math.ceil(cohort_pairs * 0.75) if route_group_count >= 2 else None
    )
    signal_cap = (
        math.ceil(cohort_pairs * 0.75) if signal_group_count >= 2 else None
    )
    structural_cap = (
        math.ceil(cohort_pairs * 0.60)
        if structural_group_count >= 2
        else None
    )
    selected: list[str] = []
    selected_set: set[str] = set()
    route_counts: Counter[str] = Counter()
    structural_counts: Counter[str] = Counter()
    signal_counts: Counter[str] = Counter()
    exposure_counts: Counter[str] = Counter()

    def add(row: Mapping[str, Any]) -> bool:
        pair_id = str(row["pair_id"])
        route_id = str(row["route_id"])
        structural_id = str(row["structural_family_id"])
        signal_id = str(row["signal_cluster_id"])
        exposure_id = str(row["portfolio_exposure_family_id"])
        if pair_id in selected_set:
            return False
        if route_cap is not None and route_counts[route_id] >= route_cap:
            return False
        if (
            structural_cap is not None
            and structural_counts[structural_id] >= structural_cap
        ):
            return False
        if signal_cap is not None and signal_counts[signal_id] >= signal_cap:
            return False
        if exposure_counts[exposure_id] >= exposure_cap:
            return False
        selected.append(pair_id)
        selected_set.add(pair_id)
        route_counts[route_id] += 1
        structural_counts[structural_id] += 1
        signal_counts[signal_id] += 1
        exposure_counts[exposure_id] += 1
        return True

    rows = ranked.to_dict(orient="records")
    anchor_columns = [
        "portfolio_exposure_family_id",
        "structural_family_id",
    ]
    if route_group_count >= 2:
        anchor_columns.append("route_id")
    if signal_group_count >= 2:
        anchor_columns.append("signal_cluster_id")
    for column in anchor_columns:
        anchored: set[str] = set()
        for row in rows:
            if len(selected) >= cohort_pairs:
                break
            group_id = str(row[column])
            if group_id in anchored:
                continue
            if add(row):
                anchored.add(group_id)
    for row in rows:
        if len(selected) >= cohort_pairs:
            break
        add(row)
    if len(selected) != cohort_pairs:
        raise RuntimeError(
            "independent finalist-funnel constrained supply below "
            f"{cohort_pairs}"
        )
    return selected, {
        "route_group_count": route_group_count,
        "structural_group_count": structural_group_count,
        "signal_group_count": signal_group_count,
        "route_cap": route_cap,
        "structural_cap": structural_cap,
        "signal_cap": signal_cap,
        "exposure_cap": exposure_cap,
    }


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
    schema_version = str(manifest.get("schema_version") or "")
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise RuntimeError(f"unsupported selection schema: {schema_version}")

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
    if str(contract.get("schema_version") or "") != schema_version:
        raise RuntimeError("contract/manifest schema drift")
    expected_productive = int(contract.get("source_productive_pairs") or 0)
    cohort_pairs = int(contract.get("cohort_pairs") or 0)
    if expected_productive <= 0 or cohort_pairs <= 0:
        raise RuntimeError("invalid contract pair counts")
    expected_members = cohort_pairs * 2
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

    excluded_pair_ids: set[str] = set()
    for binding in contract.get("excluded_prior_cohorts") or []:
        prior_root = Path(str(binding.get("root") or "")).resolve()
        prior_manifest_path = Path(
            str(binding.get("manifest") or "")
        ).resolve()
        if prior_manifest_path != prior_root / "keep_review_manifest.json":
            raise RuntimeError("excluded cohort manifest path drift")
        if (
            not prior_manifest_path.is_file()
            or _sha256(prior_manifest_path)
            != str(binding.get("manifest_file_sha256") or "")
        ):
            raise RuntimeError("excluded cohort manifest file hash drift")
        prior_manifest = _read_json(prior_manifest_path)
        prior_claimed = str(
            prior_manifest.pop("manifest_payload_sha256", "")
        )
        if (
            prior_claimed
            != str(binding.get("manifest_payload_sha256") or "")
            or prior_claimed != _payload_sha256(prior_manifest)
        ):
            raise RuntimeError("excluded cohort manifest payload drift")
        prior_pairs = set(
            pd.read_parquet(
                prior_root / "keep_review_pairs.parquet",
                columns=["pair_id"],
            )["pair_id"].astype(str)
        )
        if len(prior_pairs) != int(binding.get("excluded_pair_count") or 0):
            raise RuntimeError("excluded cohort pair count drift")
        overlap = excluded_pair_ids & prior_pairs
        if overlap:
            raise RuntimeError("excluded cohort bindings overlap")
        excluded_pair_ids.update(prior_pairs)
    if len(excluded_pair_ids) != int(
        contract.get("excluded_prior_pair_count") or 0
    ):
        raise RuntimeError("excluded prior pair total drift")

    capability_binding = contract.get("execution_capability_manifest")
    if capability_binding:
        capability_path = Path(
            str(capability_binding.get("path") or "")
        ).resolve()
        if (
            not capability_path.is_file()
            or _sha256(capability_path)
            != str(capability_binding.get("sha256") or "")
        ):
            raise RuntimeError("execution capability manifest file drift")
        load_execution_capability_manifest(capability_path)
    if schema_version == "cn_productive_keep_review_freeze_v3":
        if contract.get("selection_mode") != "finalist_funnel_v1":
            raise RuntimeError("finalist funnel selection mode drift")
        funnel_binding = contract.get("funnel_contract") or {}
        funnel_path = Path(str(funnel_binding.get("path") or "")).resolve()
        if (
            not funnel_path.is_file()
            or _sha256(funnel_path)
            != str(funnel_binding.get("file_sha256") or "")
        ):
            raise RuntimeError("finalist funnel contract file drift")
        funnel_payload = _read_json(funnel_path)
        funnel_claimed = str(
            funnel_payload.pop("contract_payload_sha256", "")
        )
        if (
            funnel_claimed != str(funnel_binding.get("payload_sha256") or "")
            or funnel_claimed != _payload_sha256(funnel_payload)
        ):
            raise RuntimeError("finalist funnel contract payload drift")
        if int(
            (funnel_payload.get("review_pool") or {}).get("target_pairs")
            or 0
        ) != cohort_pairs:
            raise RuntimeError("finalist funnel target drift")

    review = pd.read_parquet(
        selection_root / "productive_review_ledger.parquet"
    )
    pairs = pd.read_parquet(selection_root / "keep_review_pairs.parquet")
    candidates = pd.read_parquet(
        selection_root / "keep_review_candidates.parquet"
    )
    summary = _read_json(selection_root / "keep_review_summary.json")
    if (
        len(review) != expected_productive
        or review["pair_id"].nunique() != expected_productive
    ):
        raise RuntimeError("review ledger productive coverage drift")
    if len(pairs) != cohort_pairs or pairs["pair_id"].nunique() != cohort_pairs:
        raise RuntimeError("selected pair count drift")
    if (
        len(candidates) != expected_members
        or candidates["pair_id"].nunique() != cohort_pairs
        or not (candidates.groupby("pair_id").size() == 2).all()
    ):
        raise RuntimeError("selected pair-member coverage drift")
    if pairs["primary_exact_identity"].nunique() != cohort_pairs:
        raise RuntimeError("selected primary exact duplicate")
    if pairs["portfolio_behavior_family_id"].nunique() != cohort_pairs:
        raise RuntimeError("selected behavior-family duplicate")
    if pairs["portfolio_behavior_signature_id"].nunique() != cohort_pairs:
        raise RuntimeError("selected behavior-signature duplicate")
    if set(pairs["review_outcome"]) != {"ALLOW_KEEP_REVIEW"}:
        raise RuntimeError("selected review outcome drift")
    if set(pairs["pair_id"].astype(str)) & excluded_pair_ids:
        raise RuntimeError("prior review pair selected again")
    if capability_binding and not pairs[
        "execution_clock_compatible"
    ].astype(bool).all():
        raise RuntimeError("execution-incompatible pair selected")
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
    if schema_version == "cn_productive_keep_review_freeze_v3":
        route_cap = (
            math.ceil(cohort_pairs * 0.75)
            if len(route_counts) >= 2
            else None
        )
        structural_cap = (
            math.ceil(cohort_pairs * 0.60)
            if len(structural_counts) >= 2
            else None
        )
        signal_cap = (
            math.ceil(cohort_pairs * 0.75)
            if len(signal_counts) >= 2
            else None
        )
        exposure_counts = {
            str(key): int(value)
            for key, value in pairs[
                "portfolio_exposure_family_id"
            ].value_counts().items()
        }
        exposure_cap = math.ceil(cohort_pairs * 0.25)
        if route_cap is not None and max(route_counts.values()) > route_cap:
            raise RuntimeError("route concentration cap violated")
        if (
            structural_cap is not None
            and max(structural_counts.values()) > structural_cap
        ):
            raise RuntimeError("structural concentration cap violated")
        if signal_cap is not None and max(signal_counts.values()) > signal_cap:
            raise RuntimeError("signal concentration cap violated")
        if max(exposure_counts.values()) > exposure_cap:
            raise RuntimeError("portfolio exposure concentration cap violated")
        if pairs["economic_mechanism_id"].nunique() != cohort_pairs:
            raise RuntimeError("selected economic mechanism duplicate")
    else:
        exposure_counts = {}
        route_cap = math.ceil(cohort_pairs * 0.75)
        structural_cap = math.ceil(cohort_pairs * 0.50)
        signal_cap = math.ceil(cohort_pairs * 0.75)
        if max(route_counts.values()) > route_cap:
            raise RuntimeError("route concentration cap violated")
        if max(structural_counts.values()) > structural_cap:
            raise RuntimeError("structural concentration cap violated")
        if max(signal_counts.values()) > signal_cap:
            raise RuntimeError("signal concentration cap violated")

    ranked = _recompute_ranking(review)
    if schema_version in {
        "cn_productive_keep_review_freeze_v2",
        "cn_productive_keep_review_freeze_v3",
    }:
        ranked, duplicate_reasons = _deduplicate_behavior_candidates(ranked)
        behavior_duplicate_mask = (
            review["behavior_dedup_reason"].astype(str) != ""
            if schema_version == "cn_productive_keep_review_freeze_v3"
            else review["review_outcome"] == "REJECT_DUPLICATE"
        )
        recorded_duplicate_reasons = {
            str(row["pair_id"]): str(row["behavior_dedup_reason"])
            for row in review.loc[
                behavior_duplicate_mask,
                ["pair_id", "behavior_dedup_reason"],
            ].to_dict(orient="records")
        }
        if duplicate_reasons != recorded_duplicate_reasons:
            raise RuntimeError("independent behavior deduplication mismatch")
        if int(summary.get("behavior_duplicate_rejected") or 0) != len(
            duplicate_reasons
        ):
            raise RuntimeError("behavior duplicate summary count drift")
    else:
        duplicate_reasons = {}
        if (
            review["portfolio_behavior_family_id"].nunique()
            != expected_productive
            or review["portfolio_behavior_signature_id"].nunique()
            != expected_productive
        ):
            raise RuntimeError("legacy behavior deduplication proof drift")
    mechanism_duplicate_reasons: dict[str, str] = {}
    if schema_version == "cn_productive_keep_review_freeze_v3":
        recomputed_identities = _recompute_finalist_identities(review)
        identity_checks = {
            "economic_mechanism_id": "verify_economic_mechanism_id",
            "portfolio_exposure_family_id": (
                "verify_portfolio_exposure_family_id"
            ),
            "primary_support_rate_decile": (
                "verify_primary_support_rate_decile"
            ),
            "primary_behavior_turnover_decile": (
                "verify_primary_behavior_turnover_decile"
            ),
        }
        for recorded, recomputed in identity_checks.items():
            if not (
                review[recorded].astype(str)
                == recomputed_identities[recomputed].astype(str)
            ).all():
                raise RuntimeError(
                    f"independent finalist identity mismatch: {recorded}"
                )
        ranked, mechanism_duplicate_reasons = (
            _deduplicate_mechanism_candidates(ranked)
        )
        recorded_mechanism_reasons = {
            str(row["pair_id"]): str(row["mechanism_dedup_reason"])
            for row in review.loc[
                review["mechanism_dedup_reason"].astype(str) != "",
                ["pair_id", "mechanism_dedup_reason"],
            ].to_dict(orient="records")
        }
        if mechanism_duplicate_reasons != recorded_mechanism_reasons:
            raise RuntimeError("independent mechanism deduplication mismatch")
        recomputed_selection, recomputed_caps = (
            _recompute_finalist_selection(
                ranked,
                cohort_pairs=cohort_pairs,
            )
        )
        if recomputed_caps != contract["diversity_selection"][
            "resolved_caps"
        ]:
            raise RuntimeError("resolved finalist diversity cap drift")
    else:
        recomputed_selection = _recompute_selection(
            ranked,
            cohort_pairs=cohort_pairs,
        )
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
    result = {
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
        "behavior_duplicate_rejected": len(duplicate_reasons),
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
    if schema_version == "cn_productive_keep_review_freeze_v3":
        result.update(
            {
                "mechanism_duplicate_rejected": len(
                    mechanism_duplicate_reasons
                ),
                "selected_economic_mechanism_unique": int(
                    pairs["economic_mechanism_id"].nunique()
                ),
                "selected_portfolio_exposure_family_counts": (
                    exposure_counts
                ),
                "resolved_diversity_caps": recomputed_caps,
            }
        )
    return result


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
