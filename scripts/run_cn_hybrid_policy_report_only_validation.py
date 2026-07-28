from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _context_and_binding,
    _run_phase3cm,
    _sha256,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
ARMS = ("HYBRID_TPE_AVAILABILITY", "AVAILABILITY_AWARE_UNIFORM")
BACKENDS = ("active_bar", "stock_session")
ROUTE_QUOTAS_PER_ARM = {
    "SLOW_TEMPORAL_CHANGE": 54,
    "SLOW_CROSS_SECTIONAL_LEVEL": 32,
    "MARKET_REGIME_CONDITION": 16,
    "FIRSTN_PATH": 13,
    "DISCLOSURE_EVENT": 13,
}
SOURCE_REQUIRED_ARTIFACTS = {
    "candidate_ledger.parquet",
    "observation_ledger.parquet",
    "behavior_archive.parquet",
    "policy_productivity_ledger.parquet",
    "search_policy_decision.json",
    "train_complete_manifest.json",
}


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)
    return destination


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    source = Path(path).resolve()
    return {
        "path": str(source.relative_to(root.resolve())).replace("\\", "/"),
        "sha256": _sha256(source),
        "bytes": source.stat().st_size,
    }


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _load_authorization(path: Path) -> dict[str, Any]:
    authorization = _read_json(path)
    required = {
        "status": "ACTIVE_REPORT_ONLY_VALIDATION_AUTHORIZATION",
        "accepted_development_policy": "HYBRID_TPE_AVAILABILITY",
        "policy_selection_reopened": False,
        "evaluation_role": "validation",
        "validation_usage": "report_only",
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    drift = [
        key
        for key, expected in required.items()
        if authorization.get(key) != expected
    ]
    if drift:
        raise RuntimeError("validation authorization drift: " + ",".join(drift))
    if dict(authorization.get("route_quotas_per_arm") or {}) != ROUTE_QUOTAS_PER_ARM:
        raise RuntimeError("validation route quotas drift")
    if dict(authorization.get("arm_pair_counts") or {}) != {
        arm: 128 for arm in ARMS
    }:
        raise RuntimeError("validation arm counts drift")
    return authorization


def _verify_source_campaign(
    campaign_root: Path,
    *,
    authorization: Mapping[str, Any],
) -> dict[str, str]:
    root = Path(campaign_root).resolve()
    run_manifest_path = root / "run_manifest.json"
    train_manifest_path = root / "train_complete_manifest.json"
    decision_path = root / "search_policy_decision.json"
    run_manifest = _read_json(run_manifest_path)
    train_manifest = _read_json(train_manifest_path)
    decision = _read_json(decision_path)
    if str(run_manifest.get("status") or "") != "CAMPAIGN_CLOSED":
        raise RuntimeError("source Medium is not closed")
    if str(train_manifest.get("status") or "") != "PRODUCTIVITY_MEDIUM_COMPLETE":
        raise RuntimeError("source Medium train closure drift")
    if (
        str(decision.get("selected_search_policy") or "")
        != "HYBRID_TPE_AVAILABILITY"
    ):
        raise RuntimeError("accepted Hybrid decision drift")
    if str(train_manifest.get("promotion") or "") != "FORBIDDEN":
        raise RuntimeError("source Medium promotion boundary drift")

    declared = {
        str(row["path"]).replace("\\", "/"): str(row["sha256"])
        for row in run_manifest.get("artifacts") or ()
    }
    if not SOURCE_REQUIRED_ARTIFACTS.issubset(declared):
        raise RuntimeError("source Medium required artifact set incomplete")
    observed: dict[str, str] = {
        "run_manifest.json": _sha256(run_manifest_path)
    }
    for relative, expected_hash in declared.items():
        path = root / relative
        if not path.is_file():
            raise FileNotFoundError(f"source Medium artifact missing: {path}")
        observed[relative] = _sha256(path)
        if observed[relative] != expected_hash:
            raise RuntimeError(f"source Medium artifact hash drift: {relative}")

    expected = dict(authorization["source_campaign_artifact_sha256"])
    for relative, expected_hash in expected.items():
        if observed.get(relative) != str(expected_hash):
            raise RuntimeError(f"authorization source hash drift: {relative}")
    return observed


def _finite(value: Any) -> float | None:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    return output if math.isfinite(output) else None


def _selection_payload(
    *,
    policy_rows: pd.DataFrame,
    observation_rows: pd.DataFrame,
) -> list[dict[str, Any]]:
    policy = policy_rows.loc[policy_rows["pair_evaluated"].eq(True)].copy()
    observations = observation_rows.loc[
        observation_rows["pair_evaluation_status"].astype(str).eq("PAIR_EVALUATED"),
        [
            "pair_id",
            "primary_composite_reward",
            "matched_train_increment",
            "primary_standalone_train_reward_decision",
        ],
    ].copy()
    if observations["pair_id"].astype(str).duplicated().any():
        raise RuntimeError("source evaluated observation pair IDs are not unique")
    merged = policy.merge(
        observations,
        on="pair_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_observation"),
    )
    for column in (
        "search_score",
        "primary_composite_reward",
        "matched_train_increment_observation",
    ):
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
        if not merged[column].map(math.isfinite).all():
            raise RuntimeError(f"non-finite train ranking metric: {column}")
    decision_column = (
        "primary_standalone_train_reward_decision_observation"
        if "primary_standalone_train_reward_decision_observation" in merged
        else "primary_standalone_train_reward_decision"
    )

    selected: list[dict[str, Any]] = []
    for arm in ARMS:
        for route_id, quota in ROUTE_QUOTAS_PER_ARM.items():
            route = merged.loc[
                merged["intention_to_treat_arm"].astype(str).eq(arm)
                & merged["route_id"].astype(str).eq(route_id)
            ].sort_values(
                [
                    "search_score",
                    "primary_composite_reward",
                    "matched_train_increment_observation",
                    "pair_id",
                ],
                ascending=[False, False, False, True],
                kind="mergesort",
            )
            if len(route) < quota:
                raise RuntimeError(
                    f"validation route supply below quota: {arm}/{route_id}"
                )
            for rank, row in enumerate(
                route.head(quota).to_dict(orient="records"),
                start=1,
            ):
                selected.append(
                    {
                        "intention_to_treat_arm": arm,
                        "route_id": route_id,
                        "route_rank": rank,
                        "pair_id": str(row["pair_id"]),
                        "proposal_id": str(row["proposal_id"]),
                        "checkpoint": str(row["checkpoint"]),
                        "search_score": float(row["search_score"]),
                        "primary_composite_reward": float(
                            row["primary_composite_reward"]
                        ),
                        "matched_train_increment": float(
                            row["matched_train_increment_observation"]
                        ),
                        "primary_standalone_train_reward_decision": str(
                            row[decision_column]
                        ),
                        "productive_candidate": bool(
                            row["productive_candidate"]
                        ),
                        "portfolio_behavior_family_id": str(
                            row["portfolio_behavior_family_id"]
                        ),
                    }
                )
    if len(selected) != 256 or len({row["pair_id"] for row in selected}) != 256:
        raise RuntimeError("validation selection identity/count drift")
    return selected


def _freeze_candidates(
    *,
    candidate_ledger: pd.DataFrame,
    selection: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    frame = candidate_ledger.where(pd.notna(candidate_ledger), None)
    candidate_rows: list[dict[str, Any]] = []
    frozen_pairs: list[dict[str, Any]] = []
    for selected in selection:
        pair_id = str(selected["pair_id"])
        pair = frame.loc[frame["pair_id"].astype(str).eq(pair_id)]
        if len(pair) != 2:
            raise RuntimeError(f"validation pair membership drift: {pair_id}")
        by_role = {
            str(row["pair_member_role"]): row
            for row in pair.to_dict(orient="records")
        }
        if set(by_role) != {"PRIMARY", "CONTROL"}:
            raise RuntimeError(f"validation pair role drift: {pair_id}")
        primary, control = by_role["PRIMARY"], by_role["CONTROL"]
        for row in (primary, control):
            if str(row["route_id"]) != str(selected["route_id"]):
                raise RuntimeError(f"validation pair route drift: {pair_id}")
            if str(row["intention_to_treat_arm"]) != str(
                selected["intention_to_treat_arm"]
            ):
                raise RuntimeError(f"validation pair arm drift: {pair_id}")
        candidate_rows.extend((primary, control))
        frozen_pairs.append(
            {
                **dict(selected),
                "candidate_id": str(primary["candidate_id"]),
                "control_candidate_id": str(control["candidate_id"]),
                "primary_exact_identity": str(primary["exact_identity"]),
                "control_exact_identity": str(control["exact_identity"]),
                "primary_expression": str(primary["expression"]),
                "control_expression": str(control["expression"]),
            }
        )
    return candidate_rows, frozen_pairs


def prepare(
    *,
    campaign_root: Path,
    authorization_path: Path,
    output_root: Path,
) -> dict[str, Any]:
    authorization = _load_authorization(authorization_path)
    train_hashes = _verify_source_campaign(
        campaign_root,
        authorization=authorization,
    )
    root = Path(campaign_root).resolve()
    policy = pd.read_parquet(root / "policy_productivity_ledger.parquet")
    observations = pd.read_parquet(root / "observation_ledger.parquet")
    candidates = pd.read_parquet(root / "candidate_ledger.parquet")
    selection = _selection_payload(
        policy_rows=policy,
        observation_rows=observations,
    )
    selection_hash = _stable_hash(selection)
    if selection_hash != str(authorization["expected_selection_payload_sha256"]):
        raise RuntimeError("validation selection payload hash drift")
    candidate_rows, frozen_pairs = _freeze_candidates(
        candidate_ledger=candidates,
        selection=selection,
    )
    destination = Path(output_root).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    selection_path = destination / "validation_pair_selection.parquet"
    pd.DataFrame(selection).to_parquet(selection_path, index=False)
    candidate_path = destination / "validation_candidate_members.parquet"
    pd.DataFrame(candidate_rows).to_parquet(candidate_path, index=False)
    tables: dict[str, Path] = {}
    for backend, route_ids in {
        "active_bar": {"FIRSTN_PATH", "MARKET_REGIME_CONDITION"},
        "stock_session": {
            "SLOW_TEMPORAL_CHANGE",
            "SLOW_CROSS_SECTIONAL_LEVEL",
            "DISCLOSURE_EVENT",
        },
    }.items():
        table_path = destination / f"validation_{backend}_candidates.csv"
        pd.DataFrame(
            [
                row
                for row in candidate_rows
                if str(row["route_id"]) in route_ids
            ]
        ).to_csv(table_path, index=False)
        tables[backend] = table_path
    freeze = {
        "schema_version": "cn_hybrid_policy_report_only_validation_freeze_v1",
        "status": "FROZEN_BEFORE_VALIDATION_ACCESS",
        "accepted_development_policy": "HYBRID_TPE_AVAILABILITY",
        "policy_selection_reopened": False,
        "source_campaign_root": str(root),
        "authorization_path": str(Path(authorization_path).resolve()),
        "authorization_sha256": _sha256(Path(authorization_path).resolve()),
        "source_train_artifact_hashes": train_hashes,
        "selection_rule": authorization["selection_rule"],
        "route_quotas_per_arm": ROUTE_QUOTAS_PER_ARM,
        "arm_pair_counts": {arm: 128 for arm in ARMS},
        "frozen_pair_count": 256,
        "frozen_candidate_member_count": 512,
        "selection_payload_sha256": selection_hash,
        "selection_artifact": _artifact(selection_path, root=destination),
        "candidate_member_artifact": _artifact(candidate_path, root=destination),
        "backend_candidate_tables": {
            backend: _artifact(path, root=destination)
            for backend, path in tables.items()
        },
        "candidates": frozen_pairs,
        "validation_contract": {
            "evaluation_role": "validation",
            "usage": "report_only",
            "feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
    }
    freeze["manifest_body_sha256"] = _stable_hash(freeze)
    freeze_path = _write_json(destination / "validation_candidate_freeze.json", freeze)
    return {
        "status": "VALIDATION_COHORT_PREPARED_ZERO_SEALED_READS",
        "freeze_manifest_path": str(freeze_path),
        "freeze_manifest_sha256": _sha256(freeze_path),
        "selection_payload_sha256": selection_hash,
        "pair_count": 256,
        "candidate_member_count": 512,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _read_report_only_sidecar_manifest(
    root: Path,
    *,
    filename: str,
    split_manifest_hash: str,
) -> dict[str, Any]:
    path = Path(root) / filename
    payload = _read_json(path)
    expected_status = (
        "TIME_MAJOR_LAYOUT_PARITY_PASS"
        if filename == "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
        else "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY"
    )
    required = {
        "status": expected_status,
        "evaluation_role": "validation",
        "data_role": "validation_report_only",
        "split_manifest_hash": split_manifest_hash,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    if any(payload.get(key) != value for key, value in required.items()):
        raise RuntimeError(f"validation sidecar contract drift: {path}")
    if int(payload.get("eligible_validation_date_count") or 0) <= 0:
        raise RuntimeError(f"validation sidecar has no validation dates: {path}")
    if int(payload.get("validation_reads") or 0) <= 0:
        raise RuntimeError(f"validation sidecar has no validation reads: {path}")
    return payload


def _stats(values: Iterable[Any]) -> dict[str, Any]:
    series = pd.Series(
        [value for value in (_finite(raw) for raw in values) if value is not None],
        dtype=float,
    )
    if series.empty:
        return {
            "count": 0,
            "median": None,
            "p10": None,
            "mean": None,
            "positive_share": None,
        }
    return {
        "count": int(len(series)),
        "median": float(series.median()),
        "p10": float(series.quantile(0.1)),
        "mean": float(series.mean()),
        "positive_share": float((series > 0).mean()),
    }


def _spearman(frame: pd.DataFrame) -> float | None:
    complete = frame.dropna()
    if len(complete) < 3:
        return None
    value = _finite(complete.corr(method="spearman").iloc[0, 1])
    return value


def _validation_report(
    *,
    freeze: Mapping[str, Any],
    results: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rewards = {
        str(row["candidate_id"]): dict(row)
        for result in results
        for row in result.get("candidate_rewards") or ()
    }
    pairs = {
        str(row["pair_id"]): dict(row)
        for result in results
        for row in result.get("pair_results") or ()
    }
    rows: list[dict[str, Any]] = []
    for selected in freeze["candidates"]:
        pair_id = str(selected["pair_id"])
        primary = rewards[str(selected["candidate_id"])]
        pair = pairs[pair_id]
        primary_metric = _finite(primary.get("validation_report_metric"))
        pair_metric = _finite(pair.get("pair_validation_report_metric"))
        validation_score = (
            min(primary_metric, pair_metric)
            if primary_metric is not None and pair_metric is not None
            else None
        )
        rows.append(
            {
                **dict(selected),
                "validation_pair_status": str(
                    pair.get("pair_evaluation_status") or ""
                ),
                "validation_pair_blockers": str(
                    pair.get("pair_evaluation_blockers") or ""
                ),
                "validation_pair_support_count": int(
                    pair.get("pair_support_count") or 0
                ),
                "primary_validation_report_metric": primary_metric,
                "control_validation_report_metric": _finite(
                    pair.get("control_validation_report_metric")
                ),
                "pair_validation_report_metric": pair_metric,
                "validation_search_score": validation_score,
                "oos_positive_transfer": bool(
                    str(pair.get("pair_evaluation_status") or "")
                    == "PAIR_EVALUATED"
                    and validation_score is not None
                    and validation_score > 0
                ),
                "validation_day_sortino": _finite(
                    primary.get("validation_day_sortino")
                ),
                "validation_worst_horizon_day_sortino": _finite(
                    primary.get("train_worst_horizon_day_sortino")
                ),
                "validation_mean_one_way_turnover": _finite(
                    primary.get("train_mean_one_way_turnover")
                ),
                "validation_regime_positive_share": _finite(
                    primary.get("train_regime_positive_share")
                ),
                "validation_regime_worst_day_sortino": _finite(
                    primary.get("train_regime_worst_day_sortino")
                ),
            }
        )

    frame = pd.DataFrame(rows)
    summaries: list[dict[str, Any]] = []
    for keys, group in frame.groupby(
        ["intention_to_treat_arm", "route_id"],
        sort=True,
    ):
        arm, route_id = keys
        evaluated = group["validation_pair_status"].eq("PAIR_EVALUATED")
        summaries.append(
            {
                "intention_to_treat_arm": str(arm),
                "route_id": str(route_id),
                "selected_pairs": int(len(group)),
                "validation_evaluated_pairs": int(evaluated.sum()),
                "validation_blocked_pairs": int((~evaluated).sum()),
                "train_productive_pairs": int(
                    group["productive_candidate"].sum()
                ),
                "oos_positive_transfer_pairs": int(
                    group["oos_positive_transfer"].sum()
                ),
                "oos_positive_transfer_per_selected": float(
                    group["oos_positive_transfer"].mean()
                ),
                "validation_search_score": _stats(
                    group["validation_search_score"]
                ),
                "primary_validation_report_metric": _stats(
                    group["primary_validation_report_metric"]
                ),
                "pair_validation_report_metric": _stats(
                    group["pair_validation_report_metric"]
                ),
                "train_validation_spearman": _spearman(
                    group[["search_score", "validation_search_score"]]
                ),
            }
        )
    summary = {
        "schema_version": "cn_hybrid_policy_report_only_validation_report_v1",
        "status": "REPORT_ONLY_VALIDATION_COMPLETE",
        "accepted_development_policy": "HYBRID_TPE_AVAILABILITY",
        "policy_selection_reopened": False,
        "evaluation_role": "validation",
        "validation_usage": "report_only",
        "selected_pair_count": len(rows),
        "validation_evaluated_pair_count": int(
            frame["validation_pair_status"].eq("PAIR_EVALUATED").sum()
        ),
        "validation_blocked_pair_count": int(
            (~frame["validation_pair_status"].eq("PAIR_EVALUATED")).sum()
        ),
        "route_arm_summary": summaries,
        "arm_summary": [
            {
                "intention_to_treat_arm": str(arm),
                "selected_pairs": int(len(group)),
                "validation_evaluated_pairs": int(
                    group["validation_pair_status"]
                    .eq("PAIR_EVALUATED")
                    .sum()
                ),
                "train_productive_pairs": int(
                    group["productive_candidate"].sum()
                ),
                "oos_positive_transfer_pairs": int(
                    group["oos_positive_transfer"].sum()
                ),
                "oos_positive_transfer_per_selected": float(
                    group["oos_positive_transfer"].mean()
                ),
                "validation_search_score": _stats(
                    group["validation_search_score"]
                ),
                "train_validation_spearman": _spearman(
                    group[["search_score", "validation_search_score"]]
                ),
            }
            for arm, group in frame.groupby(
                "intention_to_treat_arm",
                sort=True,
            )
        ],
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    return rows, summary


def execute(
    *,
    campaign_root: Path,
    authorization_path: Path,
    freeze_path: Path,
    registry_path: Path,
    split_manifest_path: Path,
    active_field_root: Path,
    active_label_root: Path,
    session_field_root: Path,
    session_label_root: Path,
    output_root: Path,
    active_threads: int,
    session_threads: int,
) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"validation is authorized only on {AUTHORIZED_HOST}")
    authorization = _load_authorization(authorization_path)
    train_hashes_before = _verify_source_campaign(
        campaign_root,
        authorization=authorization,
    )
    freeze = _read_json(freeze_path)
    if str(freeze.get("status") or "") != "FROZEN_BEFORE_VALIDATION_ACCESS":
        raise RuntimeError("validation cohort is not frozen")
    if str(freeze.get("selection_payload_sha256") or "") != str(
        authorization["expected_selection_payload_sha256"]
    ):
        raise RuntimeError("validation freeze selection hash drift")
    if int(freeze.get("frozen_pair_count") or 0) != 256:
        raise RuntimeError("validation freeze pair count drift")

    split = FixedSplitAuthority.read(Path(split_manifest_path).resolve())
    if split.manifest_hash != str(authorization["split_manifest_sha256"]):
        raise RuntimeError("validation split authority drift")
    field_roots = {
        "active_bar": Path(active_field_root).resolve(),
        "stock_session": Path(session_field_root).resolve(),
    }
    label_roots = {
        "active_bar": Path(active_label_root).resolve(),
        "stock_session": Path(session_label_root).resolve(),
    }
    sidecar_manifest_paths: list[Path] = []
    for backend in BACKENDS:
        for root, filename in (
            (
                field_roots[backend],
                "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json",
            ),
            (
                label_roots[backend],
                "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json",
            ),
        ):
            _read_report_only_sidecar_manifest(
                root,
                filename=filename,
                split_manifest_hash=split.manifest_hash,
            )
            sidecar_manifest_paths.append(root / filename)

    candidate_frame = pd.read_parquet(
        Path(freeze_path).resolve().parent
        / freeze["candidate_member_artifact"]["path"]
    ).where(pd.notna, None)
    candidates = candidate_frame.to_dict(orient="records")
    registry = UnifiedCapabilityRegistry.read(Path(registry_path).resolve())
    destination = Path(output_root).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    sidecar_binding_hash = _stable_hash(
        [
            {"path": str(path), "sha256": _sha256(path)}
            for path in sidecar_manifest_paths
        ]
    )
    binding_path, table_paths = _context_and_binding(
        batch_root=destination,
        candidates=candidates,
        registry=registry,
        split=split,
        data_release_hash=sidecar_binding_hash,
        evaluation_role="validation",
    )
    access_receipts = _run_phase3cm(
        batch_id="hybrid_policy_report_only_validation_256",
        batch_root=destination,
        binding_path=binding_path,
        table_paths=table_paths,
        split_manifest=Path(split_manifest_path).resolve(),
        field_roots=field_roots,
        label_roots=label_roots,
        compute_threads={
            "active_bar": int(active_threads),
            "stock_session": int(session_threads),
        },
        evaluation_role="validation",
    )
    results: list[dict[str, Any]] = []
    for backend in BACKENDS:
        result_path = (
            destination
            / "phase3cm_validation"
            / backend
            / "CN_STREAMING_BACKEND_RESULT.json"
        )
        if not result_path.is_file():
            raise RuntimeError(f"validation backend result missing: {backend}")
        result = _read_json(result_path)
        required = {
            "evaluation_role": "validation",
            "validation_usage": "report_only",
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
        }
        if any(result.get(key) != value for key, value in required.items()):
            raise RuntimeError(f"validation backend boundary drift: {backend}")
        if int(result.get("validation_reads") or 0) <= 0:
            raise RuntimeError(f"validation backend has no validation reads: {backend}")
        if str(result.get("portfolio_mode") or "") != "long_only_top":
            raise RuntimeError(f"validation backend portfolio drift: {backend}")
        if float(result.get("cost_bps") or 0.0) != 5.0:
            raise RuntimeError(f"validation backend cost drift: {backend}")
        if [int(value) for value in result.get("horizons") or ()] != [1, 5, 15, 30]:
            raise RuntimeError(f"validation backend horizon drift: {backend}")
        results.append(result)

    train_hashes_after = _verify_source_campaign(
        campaign_root,
        authorization=authorization,
    )
    if train_hashes_after != train_hashes_before:
        raise RuntimeError("validation mutated protected Medium artifacts")
    rows, report = _validation_report(freeze=freeze, results=results)
    report["validation_reads"] = sum(
        int(result.get("validation_reads") or 0) for result in results
    )
    report["source_train_artifacts_unchanged"] = True
    report["source_train_artifact_hashes"] = train_hashes_after
    report["sidecar_binding_hash"] = sidecar_binding_hash
    report["freeze_manifest_sha256"] = _sha256(Path(freeze_path).resolve())
    rows_path = destination / "validation_pair_results.parquet"
    pd.DataFrame(rows).to_parquet(rows_path, index=False)
    report_path = _write_json(destination / "validation_report.json", report)
    declared_paths = [
        Path(freeze_path).resolve(),
        binding_path,
        rows_path,
        report_path,
        *sidecar_manifest_paths,
        *[
            destination
            / "phase3cm_validation"
            / backend
            / "CN_STREAMING_BACKEND_RESULT.json"
            for backend in BACKENDS
        ],
        *[
            destination
            / "phase3cm_validation"
            / backend
            / "ITERATIVE_ACCESS_RECEIPT.json"
            for backend in BACKENDS
        ],
    ]
    closure = {
        "schema_version": "cn_hybrid_policy_report_only_validation_closure_v1",
        "status": "VALIDATION_COMPLETE_IMMUTABLE_REPORT_ONLY",
        "accepted_development_policy": "HYBRID_TPE_AVAILABILITY",
        "policy_selection_reopened": False,
        "source_campaign_root": str(Path(campaign_root).resolve()),
        "source_train_artifacts_unchanged": True,
        "source_train_artifact_hashes": train_hashes_after,
        "selection_payload_sha256": freeze["selection_payload_sha256"],
        "selected_pair_count": 256,
        "validation_reads": report["validation_reads"],
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "access_receipts": access_receipts,
        "artifacts": [
            {
                "path": str(path),
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for path in declared_paths
        ],
    }
    closure["manifest_body_sha256"] = _stable_hash(closure)
    closure_path = _write_json(destination / "VALIDATION_COMPLETE.json", closure)
    return {
        **closure,
        "closure_path": str(closure_path),
        "closure_sha256": _sha256(closure_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--campaign-root", type=Path, required=True)
    prepare_parser.add_argument("--authorization", type=Path, required=True)
    prepare_parser.add_argument("--output-root", type=Path, required=True)

    execute_parser = subparsers.add_parser("execute")
    execute_parser.add_argument("--campaign-root", type=Path, required=True)
    execute_parser.add_argument("--authorization", type=Path, required=True)
    execute_parser.add_argument("--freeze-manifest", type=Path, required=True)
    execute_parser.add_argument("--registry", type=Path, required=True)
    execute_parser.add_argument("--split-manifest", type=Path, required=True)
    execute_parser.add_argument("--active-field-root", type=Path, required=True)
    execute_parser.add_argument("--active-label-root", type=Path, required=True)
    execute_parser.add_argument("--session-field-root", type=Path, required=True)
    execute_parser.add_argument("--session-label-root", type=Path, required=True)
    execute_parser.add_argument("--output-root", type=Path, required=True)
    execute_parser.add_argument("--active-threads", type=int, default=32)
    execute_parser.add_argument("--session-threads", type=int, default=32)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare(
            campaign_root=args.campaign_root,
            authorization_path=args.authorization,
            output_root=args.output_root,
        )
    else:
        result = execute(
            campaign_root=args.campaign_root,
            authorization_path=args.authorization,
            freeze_path=args.freeze_manifest,
            registry_path=args.registry,
            split_manifest_path=args.split_manifest,
            active_field_root=args.active_field_root,
            active_label_root=args.active_label_root,
            session_field_root=args.session_field_root,
            session_label_root=args.session_label_root,
            output_root=args.output_root,
            active_threads=args.active_threads,
            session_threads=args.session_threads,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
