from __future__ import annotations

from hashlib import sha1
from pathlib import Path
from statistics import mean
from typing import Any

from our_system_phase2.domain.models import CandidateRecord, make_candidate_id, utc_now_iso
from our_system_phase2.services.artifact_schema import read_json_artifact, write_json_artifact
from our_system_phase2.services.real_market_data import dataset_role_for_path
from our_system_phase2.services.variation import (
    canonicalize_expression_light,
    expression_complexity,
    extract_structural_skeleton,
)


SEARCH_MEMORY_SCHEMA_VERSION = "phase2-v2_1-search-memory-v1"


def _record_dataset_role(record: dict[str, Any]) -> str | None:
    role = record.get("real_replay_dataset_role") or record.get("dataset_role")
    return str(role) if role else None


def _role_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        role = _record_dataset_role(record) or "unscoped"
        counts[role] = counts.get(role, 0) + 1
    return counts


def _digest(value: str) -> str:
    return sha1(value.encode("utf-8")).hexdigest()[:16]


def expression_memory_key(expression: str) -> str:
    return make_candidate_id(canonicalize_expression_light(expression))


def skeleton_memory_key(expression: str) -> str:
    return f"skeleton-{_digest(extract_structural_skeleton(expression))}"


def production_rule_key(
    *,
    source_mode: str | None,
    frontier_lane: str | None,
    generation_context: dict[str, Any] | None = None,
) -> str:
    context = generation_context or {}
    detail = (
        context.get("phase2_native_ast_kind")
        or context.get("coverage_refresh_source")
        or context.get("bridge_source")
        or context.get("source")
        or "__direct__"
    )
    return "::".join(
        [
            str(frontier_lane or "__missing_lane__"),
            str(source_mode or "__missing_source__"),
            str(detail),
        ]
    )


def candidate_reward_proxy(record: CandidateRecord) -> dict[str, Any]:
    """Local search-training reward proxy; it is not a tradable edge claim."""

    complexity = expression_complexity(record.expression)
    complexity_penalty = min(0.35, complexity["char_count"] / 4000.0 + complexity["operator_count"] / 800.0)
    real_feedback_score = float(record.metadata.get("real_replay_feedback_score", 0.0) or 0.0)
    retained_bonus = 0.20 if record.retained else -0.10
    novelty_bonus = 0.08 if record.novel_structure else -0.03
    full_eval_bonus = 0.06 if record.metadata.get("full_evaluation_reached") else 0.0
    reward = (
        retained_bonus
        + novelty_bonus
        + full_eval_bonus
        + (0.32 * record.ic_max)
        + (0.20 * record.oos_stability)
        + (0.14 * record.ic_positive_coverage)
        + min(0.18, real_feedback_score)
        - complexity_penalty
    )
    return {
        "reward": round(reward, 6),
        "scope": "local_generator_policy_training_proxy_not_archive_retention",
        "components": {
            "retained_bonus": round(retained_bonus, 6),
            "novelty_bonus": round(novelty_bonus, 6),
            "full_eval_bonus": round(full_eval_bonus, 6),
            "ic_max_component": round(0.32 * record.ic_max, 6),
            "oos_stability_component": round(0.20 * record.oos_stability, 6),
            "coverage_component": round(0.14 * record.ic_positive_coverage, 6),
            "real_replay_feedback_component": round(min(0.18, real_feedback_score), 6),
            "complexity_penalty": round(complexity_penalty, 6),
        },
        "competitor_inspired_upgrade_notes": [
            "AlphaGPT-style policy reward remains separate from archive retention",
            "CFG-style structural production credit is tracked by production_rule_key",
            "MAP-Elites-style behavior coverage is rewarded through novelty and archive cell statistics",
        ],
    }


def _float_value(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def replay_reward_proxy(replay_item: dict[str, Any]) -> dict[str, Any]:
    """Replay-aware transparent reward component for generator policy learning."""

    long_return = _float_value(replay_item.get("mean_window_long_return"))
    long_sortino = _float_value(replay_item.get("mean_window_long_sortino"))
    rank_ic = _float_value(replay_item.get("mean_window_rank_ic"))
    decision = str(replay_item.get("auto_long_only_decision") or "")
    decision_component = {
        "LONG_ONLY_REVIEW": 0.14,
        "WATCHLIST_LONG_ONLY": 0.08,
        "HOLD_WEAK_LONG_ONLY": 0.0,
        "REJECT_NO_LONG_ONLY_METRICS": -0.10,
        "REJECT_NON_POSITIVE_LONG_RETURN": -0.12,
    }.get(decision, -0.02)
    smoke_flags = list(replay_item.get("smoke_flags", []) or [])
    tradability_available = bool(replay_item.get("tradability_filter_available", False))
    dataset_role = str(replay_item.get("dataset_role") or "unknown_panel")
    tradability_component = 0.02 if tradability_available else -0.03
    long_return_component = max(-0.18, min(0.18, long_return * 40.0))
    long_sortino_component = max(-0.16, min(0.24, long_sortino * 0.08))
    rank_ic_component = max(-0.08, min(0.08, rank_ic * 4.0))
    smoke_penalty = min(0.12, 0.04 * len(smoke_flags))
    reward = (
        decision_component
        + long_return_component
        + long_sortino_component
        + rank_ic_component
        + tradability_component
        - smoke_penalty
    )
    return {
        "reward": round(reward, 6),
        "scope": "real_replay_sortino_long_only_component_for_generator_policy_training",
        "components": {
            "decision_component": round(decision_component, 6),
            "long_return_component": round(long_return_component, 6),
            "long_sortino_component": round(long_sortino_component, 6),
            "rank_ic_component": round(rank_ic_component, 6),
            "tradability_component": round(tradability_component, 6),
            "smoke_penalty": round(smoke_penalty, 6),
        },
        "metrics": {
            "auto_long_only_decision": decision,
            "mean_window_long_return": round(long_return, 6),
            "mean_window_long_sortino": round(long_sortino, 6),
            "mean_window_rank_ic": round(rank_ic, 6),
            "window_count": replay_item.get("window_count"),
            "execution_lag_days": replay_item.get("execution_lag_days"),
            "signal_clock": replay_item.get("signal_clock"),
            "tradability_filter_available": tradability_available,
            "dataset_role": dataset_role,
            "smoke_flags": smoke_flags[:8],
        },
    }


class LocalSearchMemory:
    def __init__(
        self,
        *,
        inherited_paths: list[str] | None = None,
        expression_keys: set[str] | None = None,
        skeleton_keys: set[str] | None = None,
        records: list[dict[str, Any]] | None = None,
        duplicate_skip_events: list[dict[str, Any]] | None = None,
        replay_enrichment_paths: list[str] | None = None,
        dataset_role_filter_report: dict[str, Any] | None = None,
    ) -> None:
        self.inherited_paths = inherited_paths or []
        self.expression_keys = expression_keys or set()
        self.skeleton_keys = skeleton_keys or set()
        self.records = records or []
        self.duplicate_skip_events = duplicate_skip_events or []
        self.replay_enrichment_paths = replay_enrichment_paths or []
        self.dataset_role_filter_report = dataset_role_filter_report or {
            "active": False,
            "reason": "not_requested",
        }

    @classmethod
    def from_previous_run(
        cls,
        previous_run_root: str | Path | None,
        *,
        expected_dataset_role: str | None = None,
    ) -> "LocalSearchMemory":
        if previous_run_root is None:
            return cls()
        root = Path(previous_run_root)
        memory_path = root / "search_memory.json"
        if memory_path.exists():
            payload = read_json_artifact(memory_path)
            memory = cls(
                inherited_paths=[str(memory_path), *list(payload.get("inherited_paths", []))],
                expression_keys=set(payload.get("expression_keys", [])),
                skeleton_keys=set(payload.get("skeleton_keys", [])),
                records=list(payload.get("records", [])),
                duplicate_skip_events=list(payload.get("duplicate_skip_events", [])),
                replay_enrichment_paths=list(payload.get("replay_enrichment_paths", [])),
            )
            memory.enrich_from_auto_long_only_replay(root / "auto_long_only_replay_report.json")
            memory.apply_dataset_role_filter(expected_dataset_role)
            return memory
        ledger_path = root / "candidate_ledger.json"
        if not ledger_path.exists():
            return cls()
        if expected_dataset_role is not None:
            return cls(
                inherited_paths=[str(ledger_path)],
                dataset_role_filter_report={
                    "active": True,
                    "expected_dataset_role": expected_dataset_role,
                    "source_path": str(ledger_path),
                    "reason": "unscoped_ledger_quarantined_for_dataset_role_strict_memory",
                    "quarantined_unscoped_ledger": True,
                },
            )
        payload = read_json_artifact(ledger_path)
        memory = cls(inherited_paths=[str(ledger_path)])
        for item in payload.get("records", []):
            expression = str(item.get("expression", ""))
            if not expression:
                continue
            memory.expression_keys.add(expression_memory_key(expression))
            memory.skeleton_keys.add(skeleton_memory_key(expression))
            memory.records.append(
                {
                    "candidate_id": item.get("candidate_id") or expression_memory_key(expression),
                    "expression_key": expression_memory_key(expression),
                    "skeleton_key": skeleton_memory_key(expression),
                    "source_mode": item.get("source_mode"),
                    "frontier_lane": item.get("frontier_lane"),
                    "retained": bool(item.get("retained", False)),
                    "reward_proxy": None,
                    "inherited_from_ledger": str(ledger_path),
                }
            )
        memory.enrich_from_auto_long_only_replay(root / "auto_long_only_replay_report.json")
        return memory

    def apply_dataset_role_filter(self, expected_dataset_role: str | None) -> dict[str, Any]:
        if expected_dataset_role is None:
            self.dataset_role_filter_report = {
                "active": False,
                "reason": "not_requested",
            }
            return self.dataset_role_filter_report
        before_record_count = len(self.records)
        before_expression_key_count = len(self.expression_keys)
        before_skeleton_key_count = len(self.skeleton_keys)
        role_counts = _role_counts(self.records)
        kept_records = [
            record
            for record in self.records
            if _record_dataset_role(record) == expected_dataset_role
        ]
        self.records = kept_records
        self.expression_keys = {
            str(record["expression_key"])
            for record in kept_records
            if record.get("expression_key")
        }
        self.skeleton_keys = {
            str(record["skeleton_key"])
            for record in kept_records
            if record.get("skeleton_key")
        }
        self.duplicate_skip_events = []
        self.replay_enrichment_paths = []
        self.dataset_role_filter_report = {
            "active": True,
            "expected_dataset_role": expected_dataset_role,
            "role_counts_before": role_counts,
            "before_record_count": before_record_count,
            "after_record_count": len(self.records),
            "quarantined_record_count": before_record_count - len(self.records),
            "before_expression_key_count": before_expression_key_count,
            "after_expression_key_count": len(self.expression_keys),
            "quarantined_expression_key_count": before_expression_key_count - len(self.expression_keys),
            "before_skeleton_key_count": before_skeleton_key_count,
            "after_skeleton_key_count": len(self.skeleton_keys),
            "quarantined_skeleton_key_count": before_skeleton_key_count - len(self.skeleton_keys),
            "unscoped_records_are_not_inherited": True,
            "reason": "strict_same_dataset_role_space_memory",
        }
        return self.dataset_role_filter_report

    def register_seed_records(self, records: list[CandidateRecord]) -> None:
        for record in records:
            self.expression_keys.add(expression_memory_key(record.expression))
            self.skeleton_keys.add(skeleton_memory_key(record.expression))

    def has_seen_expression(self, expression: str) -> bool:
        return expression_memory_key(expression) in self.expression_keys

    def record_duplicate_skip(
        self,
        *,
        expression: str,
        run_id: str,
        round_index: int,
        lane: str,
        source_mode: str,
        reason: str = "local_search_memory_duplicate_expression",
    ) -> None:
        self.duplicate_skip_events.append(
            {
                "run_id": run_id,
                "round_index": round_index,
                "frontier_lane": lane,
                "source_mode": source_mode,
                "candidate_id": expression_memory_key(expression),
                "expression_key": expression_memory_key(expression),
                "skeleton_key": skeleton_memory_key(expression),
                "reason": reason,
            }
        )

    def record_evaluation(
        self,
        *,
        record: CandidateRecord,
        run_id: str,
        generation_context: dict[str, Any] | None = None,
    ) -> None:
        expression_key = expression_memory_key(record.expression)
        skeleton_key = skeleton_memory_key(record.expression)
        self.expression_keys.add(expression_key)
        self.skeleton_keys.add(skeleton_key)
        reward = candidate_reward_proxy(record)
        self.records.append(
            {
                "run_id": run_id,
                "candidate_id": record.candidate_id,
                "expression_key": expression_key,
                "skeleton_key": skeleton_key,
                "production_rule_key": production_rule_key(
                    source_mode=record.source_mode,
                    frontier_lane=record.frontier_lane,
                    generation_context=generation_context,
                ),
                "source_mode": record.source_mode,
                "frontier_lane": record.frontier_lane,
                "archive_cell": record.archive_cell,
                "retained": record.retained,
                "label": record.label,
                "ic_max": record.ic_max,
                "oos_stability": record.oos_stability,
                "ic_positive_coverage": record.ic_positive_coverage,
                "novel_structure": record.novel_structure,
                "round_index": record.round_index,
                "complexity": expression_complexity(record.expression),
                "reward_proxy": reward,
                "created_at": utc_now_iso(),
            }
        )

    def enrich_from_auto_long_only_replay(self, replay_report_path: str | Path) -> dict[str, Any]:
        path = Path(replay_report_path)
        if not path.exists():
            return {
                "active": False,
                "reason": "auto_long_only_replay_report_not_found",
                "path": str(path),
            }
        if str(path) in self.replay_enrichment_paths:
            return {
                "active": False,
                "reason": "auto_long_only_replay_report_already_ingested",
                "path": str(path),
            }
        report = read_json_artifact(path)
        evaluations = list(report.get("validation", {}).get("evaluations", []))
        report_dataset_path = report.get("dataset_path")
        report_dataset_role = str(report.get("dataset_role") or dataset_role_for_path(report_dataset_path))
        for item in evaluations:
            item.setdefault("dataset_role", report_dataset_role)
        by_candidate_id = {str(item.get("candidate_id")): item for item in evaluations if item.get("candidate_id")}
        by_expression_key = {
            expression_memory_key(str(item.get("expression", ""))): item
            for item in evaluations
            if item.get("expression")
        }
        enriched_count = 0
        for record in self.records:
            replay_item = by_candidate_id.get(str(record.get("candidate_id"))) or by_expression_key.get(
                str(record.get("expression_key"))
            )
            if replay_item is None:
                continue
            replay_reward = replay_reward_proxy(replay_item)
            base_reward = record.get("reward_proxy")
            if isinstance(base_reward, dict):
                combined = round(float(base_reward.get("reward", 0.0)) + replay_reward["reward"], 6)
                base_reward["real_replay_reward_proxy"] = replay_reward
                base_reward["reward_after_real_replay"] = combined
            else:
                record["reward_proxy"] = {
                    "reward": replay_reward["reward"],
                    "scope": "replay_only_inherited_record_reward_proxy",
                    "real_replay_reward_proxy": replay_reward,
                    "reward_after_real_replay": replay_reward["reward"],
                }
            record["real_replay_enriched"] = True
            record["real_replay_report_path"] = str(path)
            record["real_replay_dataset_path"] = report_dataset_path
            record["real_replay_dataset_role"] = report_dataset_role
            enriched_count += 1
        self.replay_enrichment_paths.append(str(path))
        return {
            "active": True,
            "path": str(path),
            "evaluation_count": len(evaluations),
            "enriched_count": enriched_count,
            "summary": report.get("summary", {}),
            "dataset_path": report_dataset_path,
            "dataset_role": report_dataset_role,
        }

    def report(self, *, run_id: str) -> dict[str, Any]:
        production_stats: dict[str, dict[str, Any]] = {}
        for item in self.records:
            key = str(item.get("production_rule_key") or "__inherited__")
            current = production_stats.setdefault(
                key,
                {
                    "count": 0,
                    "retained_count": 0,
                    "reward_values": [],
                    "frontier_lanes": set(),
                    "source_modes": set(),
                },
            )
            current["count"] += 1
            current["retained_count"] += 1 if item.get("retained") else 0
            reward = item.get("reward_proxy")
            if isinstance(reward, dict):
                current["reward_values"].append(float(reward.get("reward_after_real_replay", reward.get("reward", 0.0))))
            if item.get("frontier_lane"):
                current["frontier_lanes"].add(str(item["frontier_lane"]))
            if item.get("source_mode"):
                current["source_modes"].add(str(item["source_mode"]))

        compact_stats = {}
        for key, item in production_stats.items():
            rewards = item["reward_values"]
            compact_stats[key] = {
                "count": item["count"],
                "retained_count": item["retained_count"],
                "retained_yield": round(item["retained_count"] / max(1, item["count"]), 6),
                "mean_reward_proxy": round(mean(rewards), 6) if rewards else None,
                "frontier_lanes": sorted(item["frontier_lanes"]),
                "source_modes": sorted(item["source_modes"]),
            }

        return {
            "run_id": run_id,
            "created_at": utc_now_iso(),
            "schema_version": SEARCH_MEMORY_SCHEMA_VERSION,
            "scope": "local_space_search_memory_for_duplicate_avoidance_and_policy_learning",
            "market_portability": "per_run_chain_memory_no_global_market_lock",
            "does_not_change_archive_retention": True,
            "expression_key_policy": "canonicalized_expression_sha1_candidate_id",
            "skeleton_key_policy": "canonicalized_structural_skeleton_sha1",
            "reward_policy": "local_generator_training_proxy_not_tradable_edge_claim",
            "real_replay_reward_policy": "transparent_sortino_long_only_component_no_reward_model_training",
            "dataset_role_filter": self.dataset_role_filter_report,
            "inherited_paths": self.inherited_paths,
            "replay_enrichment_paths": self.replay_enrichment_paths,
            "expression_key_count": len(self.expression_keys),
            "skeleton_key_count": len(self.skeleton_keys),
            "record_count": len(self.records),
            "duplicate_skip_count": len(self.duplicate_skip_events),
            "duplicate_skip_events": self.duplicate_skip_events[-200:],
            "production_rule_stats": compact_stats,
            "records": self.records[-2000:],
            "expression_keys": sorted(self.expression_keys),
            "skeleton_keys": sorted(self.skeleton_keys),
        }


def enrich_search_memory_with_auto_long_only_replay(run_root: str | Path) -> dict[str, Any]:
    root = Path(run_root)
    memory_path = root / "search_memory.json"
    if not memory_path.exists():
        return {
            "active": False,
            "reason": "search_memory_not_found",
            "path": str(memory_path),
        }
    payload = read_json_artifact(memory_path)
    memory = LocalSearchMemory(
        inherited_paths=list(payload.get("inherited_paths", [])),
        expression_keys=set(payload.get("expression_keys", [])),
        skeleton_keys=set(payload.get("skeleton_keys", [])),
        records=list(payload.get("records", [])),
        duplicate_skip_events=list(payload.get("duplicate_skip_events", [])),
        replay_enrichment_paths=list(payload.get("replay_enrichment_paths", [])),
    )
    enrichment = memory.enrich_from_auto_long_only_replay(root / "auto_long_only_replay_report.json")
    if enrichment.get("active"):
        write_json_artifact(
            memory_path,
            memory.report(run_id=str(payload.get("run_id") or root.name)),
            schema_version=SEARCH_MEMORY_SCHEMA_VERSION,
        )
    return enrichment
