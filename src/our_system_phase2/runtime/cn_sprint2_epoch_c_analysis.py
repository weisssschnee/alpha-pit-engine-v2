"""Joint analysis and frozen union reranking for Sprint-2 development Epoch-C."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from our_system_phase2.runtime.cn_b1s_development_canary import (
    _atomic_csv,
    _json_hash,
    _select_seed_set,
    _sha256,
    build_admissions,
)
from our_system_phase2.runtime.cn_sprint2_repair_canary_analysis import (
    _artifact_root,
    _seed_report,
    _set_overlap,
    _shared_stability,
)
from our_system_phase2.services.atomic_checkpoint import atomic_write_json


ANALYSIS_VERSION = "cn_sprint2_epoch_c_analysis_v1"
SEEDS = ("seed_a", "seed_b", "seed_c")


def _as_bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes"}


def _triple_overlap(sets: Mapping[str, set[str]]) -> dict[str, Any]:
    union = set().union(*sets.values())
    intersection = set.intersection(*sets.values()) if sets else set()
    pairwise = {
        f"{left}__{right}": _set_overlap(sets[left], sets[right])
        for index, left in enumerate(SEEDS)
        for right in SEEDS[index + 1 :]
    }
    return {
        "per_seed_count": {seed: len(sets[seed]) for seed in SEEDS},
        "union_count": len(union),
        "three_way_intersection_count": len(intersection),
        "three_way_jaccard": len(intersection) / max(1, len(union)),
        "pairwise": pairwise,
    }


def _proposal_union_metrics(
    proposals: Mapping[str, pd.DataFrame], contract: Mapping[str, Any]
) -> dict[str, Any]:
    expressions = {
        seed: set(proposals[seed]["expression"].fillna("").astype(str)) for seed in SEEDS
    }
    exact_identities = {
        seed: {
            value for value in proposals[seed]["exact_identity"].fillna("").astype(str) if value
        }
        for seed in SEEDS
    }
    backbone = {
        seed: set(
            proposals[seed]
            .loc[proposals[seed]["proposal_stage"].ne("adaptive"), "exact_identity"]
            .fillna("")
            .astype(str)
        )
        for seed in SEEDS
    }
    expansion = {
        seed: set(
            proposals[seed]
            .loc[proposals[seed]["proposal_stage"].eq("adaptive"), "exact_identity"]
            .fillna("")
            .astype(str)
        )
        for seed in SEEDS
    }
    legal_exact: dict[str, set[str]] = {}
    for seed in SEEDS:
        frame = proposals[seed]
        mask = frame["legal"].map(_as_bool)
        legal_exact[seed] = {
            value for value in frame.loc[mask, "exact_identity"].fillna("").astype(str) if value
        }
    expression_overlap = _triple_overlap(expressions)
    exact_overlap = _triple_overlap(exact_identities)
    backbone_overlap = _triple_overlap(backbone)
    expansion_overlap = _triple_overlap(expansion)
    legal_overlap = _triple_overlap(legal_exact)
    expected_unique = int(contract["epoch_budget"]["unique_proposal_total"])
    expected_backbone = int(contract["epoch_budget"]["shared_deterministic_backbone"])
    expected_expansion = int(contract["epoch_budget"]["seed_specific_expansion_per_seed"])
    return {
        "execution_row_count": sum(len(frame) for frame in proposals.values()),
        "unique_exact_identity_union_count": exact_overlap["union_count"],
        "expected_unique_proposal_count": expected_unique,
        "unique_proposal_contract_met": exact_overlap["union_count"] == expected_unique,
        "exact_identity_overlap": exact_overlap,
        "raw_expression_overlap_diagnostic": expression_overlap,
        "expression_overlap": expression_overlap,
        "shared_backbone": {
            **backbone_overlap,
            "expected_count": expected_backbone,
            "contract_met": (
                backbone_overlap["three_way_intersection_count"] == expected_backbone
                and all(count == expected_backbone for count in backbone_overlap["per_seed_count"].values())
            ),
        },
        "seed_specific_expansion": {
            **expansion_overlap,
            "expected_per_seed": expected_expansion,
            "contract_met": (
                expansion_overlap["union_count"] == expected_expansion * len(SEEDS)
                and expansion_overlap["three_way_intersection_count"] == 0
                and all(count == expected_expansion for count in expansion_overlap["per_seed_count"].values())
            ),
        },
        "legal_exact_identity_overlap": legal_overlap,
        "typed_gate_rejection_count": sum(
            len(proposals[seed]) - int(proposals[seed]["legal"].map(_as_bool).sum())
            for seed in SEEDS
        ),
    }


def _epoch_c_shared_stability(left: pd.DataFrame, right: pd.DataFrame) -> dict[str, Any]:
    # Repair-CANARY used only fixed/control stages. Epoch-C also contains the
    # frozen LLM proposal/repair rows in its shared deterministic backbone.
    normalized = []
    for source in (left, right):
        frame = source.copy()
        frame.loc[frame["proposal_stage"].ne("adaptive"), "proposal_stage"] = "fixed"
        normalized.append(frame)
    return _shared_stability(normalized[0], normalized[1])


def _align_clusters_and_union(
    proposals: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    frames: dict[str, pd.DataFrame] = {}
    for seed in SEEDS:
        frame = proposals[seed].copy()
        for column in (
            "parent_id", "family_id", "semantic_bucket", "canonical_identity",
            "exact_identity", "candidate_id", "lane_id", "proposal_stage", "expression",
        ):
            frame[column] = frame[column].fillna("").astype(str)
        for column in ("development_eligible", "strict_priority_eligible", "legal", "materialized"):
            frame[column] = frame[column].map(_as_bool)
        frame = frame[frame["legal"] & frame["exact_identity"].ne("")].copy()
        frame["source_seed"] = seed
        frames[seed] = frame

    reference = frames["seed_a"]
    next_cluster = int(pd.to_numeric(reference["signal_cluster_id"], errors="coerce").max()) + 1
    for seed in ("seed_b", "seed_c"):
        source = frames[seed]
        shared = (
            reference[["exact_identity", "signal_cluster_id"]]
            .drop_duplicates("exact_identity")
            .merge(
                source[["exact_identity", "signal_cluster_id"]].drop_duplicates("exact_identity"),
                on="exact_identity", suffixes=("_reference", "_source"), validate="one_to_one",
            )
        )
        source_to_reference = (
            shared.groupby("signal_cluster_id_source")["signal_cluster_id_reference"]
            .agg(lambda values: int(pd.Series(values).mode().iloc[0]))
            .to_dict()
        )
        source_clusters = sorted(
            set(pd.to_numeric(source["signal_cluster_id"], errors="coerce").fillna(0).astype(int))
        )
        fallback: dict[int, int] = {}
        for cluster in source_clusters:
            if cluster not in source_to_reference:
                fallback[cluster] = next_cluster
                next_cluster += 1
        source["signal_cluster_id"] = [
            int(source_to_reference.get(int(value), fallback.get(int(value), 0)))
            for value in pd.to_numeric(source["signal_cluster_id"], errors="coerce").fillna(0)
        ]

    combined = pd.concat([frames[seed] for seed in SEEDS], ignore_index=True, sort=False)
    return (
        combined.sort_values(
            ["strict_priority_score", "proxy_reward", "source_seed", "candidate_id"],
            ascending=[False, False, True, True], kind="mergesort",
        )
        .drop_duplicates("exact_identity", keep="first")
        .reset_index(drop=True)
    )


def _union_rerank(
    proposals: Mapping[str, pd.DataFrame],
    contract: Mapping[str, Any],
    output_root: Path,
) -> dict[str, Any]:
    union = _align_clusters_and_union(proposals)
    admissions = build_admissions(union.to_dict("records"), contract)
    output_root.mkdir(parents=True, exist_ok=True)
    _atomic_csv(union, output_root / "epoch_c_union_legal_exact_deduplicated.csv")
    strategies = (
        "current_scalar", "strict_priority", "stratified_diversity",
        "quality_diversity_hybrid",
    )
    for strategy in strategies:
        _atomic_csv(
            pd.DataFrame(admissions[strategy]),
            output_root / f"epoch_c_union_admission_{strategy}.csv",
        )
    return {
        "legal_exact_union_count": len(union),
        "aligned_signal_cluster_count": int(union["signal_cluster_id"].nunique()),
        "admission_counts": {strategy: len(admissions[strategy]) for strategy in strategies},
        "rerank_contract": "same_frozen_selector_and_admission_contract_as_epoch_c",
        "promotion_made": False,
    }


def _lane_economic_summary(evidence: pd.DataFrame, lane: str) -> dict[str, Any]:
    source = evidence[evidence["lane_id"].eq(lane)]
    return {
        "strict_candidate_count": len(source),
        "strict_hit_rate": float(source["strict_priority_hit"].mean()) if len(source) else 0.0,
        "mean_h5_cost_adjusted_abs_ic": float(source["cost_adjusted_abs_ic"].mean()) if len(source) else 0.0,
        "mean_strict_min_lcb": float(source["strict_min_lcb"].mean()) if len(source) else 0.0,
        "mean_benchmark_increment": float(source["strict_benchmark_increment"].mean()) if len(source) else 0.0,
        "positive_benchmark_increment_share": (
            float(source["strict_benchmark_increment"].gt(0).mean()) if len(source) else 0.0
        ),
        "signal_cluster_count": int(source["signal_cluster_id"].nunique()),
    }


def _state_economic_increment(
    reports: Mapping[str, Mapping[str, Any]], evidence: Mapping[str, pd.DataFrame]
) -> dict[str, Any]:
    per_seed: dict[str, Any] = {}
    positive = 0
    for seed in SEEDS:
        state = _lane_economic_summary(evidence[seed], "state_transition")
        static = _lane_economic_summary(evidence[seed], "static_cross_sectional")
        delta = {
            "mean_h5_cost_adjusted_abs_ic": (
                state["mean_h5_cost_adjusted_abs_ic"] - static["mean_h5_cost_adjusted_abs_ic"]
            ),
            "mean_strict_min_lcb": state["mean_strict_min_lcb"] - static["mean_strict_min_lcb"],
            "mean_benchmark_increment": (
                state["mean_benchmark_increment"] - static["mean_benchmark_increment"]
            ),
        }
        seed_positive = bool(
            int(reports[seed]["new_clusters"]["state_transition"]["new_vs_static_count"]) > 0
            and state["strict_candidate_count"] > 0
            and state["mean_benchmark_increment"] > 0
            and state["mean_strict_min_lcb"] > 0
            and delta["mean_benchmark_increment"] > 0
            and delta["mean_strict_min_lcb"] > 0
        )
        positive += int(seed_positive)
        per_seed[seed] = {"state": state, "static": static, "state_minus_static": delta, "pass": seed_positive}
    return {
        "per_seed": per_seed,
        "positive_seed_count": positive,
        "gate_passed": positive >= 2,
        "definition": "new_state_clusters_plus_positive_absolute_and_matched_static_increment",
    }


def _strict_union_pack(
    evidence: Mapping[str, pd.DataFrame], output_root: Path
) -> tuple[dict[str, Any], pd.DataFrame]:
    frames = []
    for seed in SEEDS:
        frame = evidence[seed].copy()
        frame["source_seed"] = seed
        frames.append(frame)
    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined["exact_identity"] = combined["exact_identity"].fillna("").astype(str)
    combined = combined[combined["exact_identity"].ne("")].copy()
    pack = (
        combined.sort_values(
            [
                "strict_priority_hit", "strict_stable", "strict_min_lcb",
                "cost_adjusted_abs_ic", "strict_priority_score", "source_seed", "candidate_id",
            ],
            ascending=[False, False, False, False, False, True, True],
            kind="mergesort",
        )
        .drop_duplicates("exact_identity", keep="first")
        .reset_index(drop=True)
    )
    pack["pack_role"] = "EPOCH_C_RESEARCH_ONLY_NOT_PROMOTED"
    pack["forward_2026_allowed"] = False
    output_root.mkdir(parents=True, exist_ok=True)
    _atomic_csv(pack, output_root / "epoch_c_strict_union_pack.research_only.csv")

    def top_share(column: str, *, exclude_blank: bool = False) -> float:
        values = pack[column].fillna("").astype(str)
        if exclude_blank:
            values = values[values.ne("")]
            return float(values.value_counts().iloc[0] / max(1, len(pack))) if len(values) else 0.0
        return float(values.value_counts(normalize=True).iloc[0]) if len(values) else 0.0

    diversity = {
        "lane_top_share": top_share("lane_id"),
        "primitive_top_share": top_share("primitive_family"),
        "family_top_share": top_share("family_id"),
        "parent_top_share": top_share("parent_id", exclude_blank=True),
        "root_candidate_share": float(pack["parent_id"].fillna("").astype(str).eq("").mean()) if len(pack) else 0.0,
        "signal_cluster_top_share": top_share("signal_cluster_id"),
    }
    diversity["not_single_source_dominated"] = bool(
        diversity["lane_top_share"] <= 0.50
        and diversity["primitive_top_share"] <= 0.35
        and diversity["family_top_share"] <= 0.25
        and diversity["parent_top_share"] <= 0.10
    )
    report = {
        "input_strict_rows": len(combined),
        "exact_deduplicated_count": len(pack),
        "strict_hit_rate": float(pack["strict_priority_hit"].mean()) if len(pack) else 0.0,
        "mean_benchmark_increment": float(pack["strict_benchmark_increment"].mean()) if len(pack) else 0.0,
        "positive_benchmark_increment_share": float(pack["strict_benchmark_increment"].gt(0).mean()) if len(pack) else 0.0,
        "mean_h5_cost_adjusted_abs_ic": float(pack["cost_adjusted_abs_ic"].mean()) if len(pack) else 0.0,
        "diversity": diversity,
        "promotion_made": False,
    }
    return report, pack


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    for seed in SEEDS:
        parser.add_argument(f"--{seed.replace('_', '-')}-root", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--expected-repo-sha", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    started = datetime.now(timezone.utc)
    seed_roots = {
        seed: _artifact_root(getattr(args, f"{seed}_root")) for seed in SEEDS
    }
    reports: dict[str, Any] = {}
    proposals: dict[str, pd.DataFrame] = {}
    evidence: dict[str, pd.DataFrame] = {}
    for seed in SEEDS:
        reports[seed], proposals[seed], evidence[seed] = _seed_report(seed, seed_roots[seed])

    contract_payload = json.loads(args.contract.read_text(encoding="utf-8-sig"))
    selected_contract = _select_seed_set(contract_payload, "seed_a")
    proposal_union = _proposal_union_metrics(proposals, contract_payload)
    pairwise_shared = {
        f"{left}__{right}": _epoch_c_shared_stability(proposals[left], proposals[right])
        for index, left in enumerate(SEEDS)
        for right in SEEDS[index + 1 :]
    }
    shared_stability_pass = all(
        row["exact_identity_complete"]
        and row["development_proxy_rank_correlation"] >= 0.999
        and row["strict_priority_rank_correlation"] >= 0.999
        and row["behaviour_cluster_ari"] >= 0.99
        for row in pairwise_shared.values()
    )
    union_rerank = _union_rerank(proposals, selected_contract, args.output_root / "union_rerank")

    pack_sets = {seed: set(evidence[seed]["exact_identity"].astype(str)) for seed in SEEDS}
    mechanism_sets = {seed: set(evidence[seed]["hypothesis_arm"].astype(str)) for seed in SEEDS}
    primitive_sets = {seed: set(evidence[seed]["primitive_family"].astype(str)) for seed in SEEDS}
    pack_overlap = _triple_overlap(pack_sets)
    mechanism_overlap = _triple_overlap(mechanism_sets)
    primitive_overlap = _triple_overlap(primitive_sets)
    state_increment = _state_economic_increment(reports, evidence)
    strict_union, strict_pack = _strict_union_pack(evidence, args.output_root)

    zero_access = all(report["zero_forbidden_access"] for report in reports.values())
    strict_total = sum(int(report["strict_count"]) for report in reports.values())
    expected_strict = int(contract_payload["epoch_budget"]["strict_eval_total"])
    selector_reduction = all(
        int(report["strict_priority_eligible_count"]) <= 0.25 * int(report["development_eligible_count"])
        for report in reports.values()
    )
    selector_realized_wins = sum(
        report["selector_realized_strict"]["strict_priority"]["precision"]
        > report["selector_realized_strict"]["current_scalar"]["precision"]
        for report in reports.values()
    )
    rx_strict_wins = sum(report["rx_ucb_strict"]["strict_outperformance_gate"] for report in reports.values())
    temporal_pass = all(
        int(report["new_clusters"]["temporal_program"]["new_vs_static_count"]) > 0
        for report in reports.values()
    )
    event_failed = all(
        int(report["new_clusters"]["event_conditioned"]["new_vs_static_count"]) == 0
        for report in reports.values()
    )
    event_decision = "EVENT_GENERATOR_NOT_OPERATIONAL" if event_failed else "EVENT_GENERATOR_OPERATIONAL"
    event_budget_stopped = all(
        int(report["new_clusters"]["event_conditioned"]["cluster_count"]) == 0
        for report in reports.values()
    )
    mechanism_repeatability = min(
        row["jaccard"] for row in mechanism_overlap["pairwise"].values()
    ) >= 0.50
    primitive_repeatability = min(
        row["jaccard"] for row in primitive_overlap["pairwise"].values()
    ) >= 0.50
    benchmark_pass = bool(
        strict_union["mean_benchmark_increment"] > 0
        and strict_union["positive_benchmark_increment_share"] > 0.50
    )
    success_gates = {
        "offline_selector_oof_lift": bool(
            contract_payload["strict_priority_contract"]["offline_evidence_gate_passed"]
        ),
        "development_to_strict_priority_reduction": selector_reduction,
        "rx_ucb_strict_outperformance_at_least_two_seeds": rx_strict_wins >= 2,
        "temporal_new_clusters": temporal_pass,
        "event_operational_or_explicitly_denied_and_budget_stopped": (
            not event_failed or event_budget_stopped
        ),
        "state_non_degenerate_new_clusters": all(
            int(report["new_clusters"]["state_transition"]["new_vs_static_count"]) > 0
            for report in reports.values()
        ),
        "shared_backbone_stability": shared_stability_pass,
        "union_mechanism_repeatability": mechanism_repeatability and primitive_repeatability,
        "strict_increment_over_simple_benchmark": benchmark_pass,
        "final_pack_not_single_source_dominated": strict_union["diversity"]["not_single_source_dominated"],
    }
    passed_count = sum(success_gates.values())
    access_safety_pass = zero_access
    contract_fidelity_pass = bool(
        proposal_union["unique_proposal_contract_met"]
        and proposal_union["shared_backbone"]["contract_met"]
        and proposal_union["seed_specific_expansion"]["contract_met"]
        and strict_total == expected_strict
    )
    safety_pass = bool(access_safety_pass and contract_fidelity_pass)
    candidate_pack_ready = bool(
        safety_pass
        and passed_count >= 8
        and success_gates["offline_selector_oof_lift"]
        and success_gates["rx_ucb_strict_outperformance_at_least_two_seeds"]
        and success_gates["shared_backbone_stability"]
        and success_gates["strict_increment_over_simple_benchmark"]
    )
    if candidate_pack_ready:
        authorized_review_pack = strict_pack.copy()
        authorized_review_pack["pack_role"] = "FROZEN_FOR_SEPARATE_FORWARD_AUTHORIZATION_REVIEW"
        _atomic_csv(
            authorized_review_pack,
            args.output_root / "forward_authorization_candidate_pack.frozen.csv",
        )

    # Epoch-D requires a single, identified sample-insufficiency defect. Three
    # complete seeds do not create such a defect by themselves.
    epoch_d_reason = (
        "SELECTOR_LIFT_NOT_STABLE"
        if selector_realized_wins < 2
        else "RX_UCB_DID_NOT_OUTPERFORM_IN_TWO_SEEDS"
        if rx_strict_wins < 2
        else "NO_EVENT_OR_STATE_INDEPENDENT_ECONOMIC_INCREMENT"
        if not state_increment["gate_passed"]
        else "NO_SINGLE_SAMPLE_INSUFFICIENCY_DEFECT_AFTER_COMPLETE_THREE_SEED_EPOCH_C"
    )
    epoch_d = {
        "authorized": False,
        "reason": epoch_d_reason,
        "selector_lift_stable": selector_realized_wins >= 2,
        "rx_ucb_outperformance": rx_strict_wins >= 2,
        "state_independent_economic_increment": state_increment["gate_passed"],
    }
    if not access_safety_pass:
        final_status = "CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_FAILED"
        recommendation = "CONTINUE_SEARCH_SELECTION_AND_GENERATOR_RESEARCH"
    elif candidate_pack_ready:
        final_status = "CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_COMPLETED"
        recommendation = "PREPARE_CANDIDATE_PACK_FOR_FORWARD_AUTHORIZATION"
    elif passed_count >= 6:
        final_status = "CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_PARTIALLY_COMPLETED"
        recommendation = "CONTINUE_SEARCH_SELECTION_AND_GENERATOR_RESEARCH"
    else:
        final_status = "CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_FAILED"
        recommendation = "PIVOT_TO_NEW_CN_DATA_OR_MECHANISM"

    report = {
        "analysis_version": ANALYSIS_VERSION,
        "repo_sha": args.expected_repo_sha,
        "started_at": started.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "contract_canonical_sha256": _json_hash(contract_payload),
        "seed_reports": reports,
        "proposal_union": proposal_union,
        "strict_budget": {"actual": strict_total, "expected": expected_strict, "met": strict_total == expected_strict},
        "shared_backbone_pairwise": pairwise_shared,
        "shared_backbone_stability_passed": shared_stability_pass,
        "strict_pack_overlap": pack_overlap,
        "economic_hypothesis_overlap": mechanism_overlap,
        "primitive_family_overlap": primitive_overlap,
        "union_rerank": union_rerank,
        "strict_union_pack": strict_union,
        "selector_realized_win_seed_count_vs_scalar": selector_realized_wins,
        "rx_ucb_strict_win_seed_count": rx_strict_wins,
        "state_independent_economic_increment": state_increment,
        "event_decision": event_decision,
        "event_budget_stopped": event_budget_stopped,
        "success_gates": success_gates,
        "success_gate_pass_count": passed_count,
        "access_safety_pass": access_safety_pass,
        "contract_fidelity_pass": contract_fidelity_pass,
        "safety_pass": safety_pass,
        "candidate_pack_ready_for_separate_forward_authorization": candidate_pack_ready,
        "epoch_d": epoch_d,
        "final_status": final_status,
        "recommendation": recommendation,
        "boundaries": {
            "forward_2026_sealed": True,
            "candidate_promotion_made": False,
            "cross_sprint_adaptive_memory_written": False,
        },
    }
    args.output_root.mkdir(parents=True, exist_ok=True)
    report_path = args.output_root / "epoch_c_joint_analysis.json"
    atomic_write_json(report_path, report)
    outputs = [report_path, *sorted(path for path in args.output_root.rglob("*") if path.is_file())]
    unique_outputs = list(dict.fromkeys(outputs))
    manifest = {
        "manifest_version": "cn_sprint2_epoch_c_analysis_manifest_v1",
        "status": "COMPLETED",
        "experiment_identity": {
            "repo_sha": args.expected_repo_sha,
            "analyzer_code_sha256": _sha256(Path(__file__)),
            "contract_canonical_sha256": _json_hash(contract_payload),
            "seed_roots": {seed: str(seed_roots[seed]) for seed in SEEDS},
        },
        "outputs": [
            {"path": str(path), "size": path.stat().st_size, "sha256": _sha256(path)}
            for path in unique_outputs
        ],
        "result": {
            "final_status": final_status,
            "recommendation": recommendation,
            "candidate_pack_ready_for_separate_forward_authorization": candidate_pack_ready,
            "epoch_d_authorized": False,
        },
        "reproducibility": "YES_FOR_FROZEN_INPUTS",
        "continuation": recommendation,
        "failure": None,
    }
    atomic_write_json(args.output_root / "run_manifest.json", manifest)
    print(json.dumps(manifest["result"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
