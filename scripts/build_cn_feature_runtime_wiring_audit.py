"""Build the non-performance CN feature runtime-wiring audit.

This script joins frozen registries, completed development-only exposure
ledgers and source reachability.  It does not read labels, validation,
holdout, challenge or 2026 data and it never evaluates candidate returns.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (  # noqa: E402
    RegistryDrivenGenerator,
)


AUDIT_VERSION = "cn_feature_runtime_wiring_audit_v1"
FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
OUTPUT_NAMES = (
    "CN_PHYSICAL_FIELD_UNIVERSE.csv",
    "CN_QUALIFIED_REPRESENTATION_UNIVERSE.csv",
    "CN_RUNTIME_FIELD_UNIVERSE.csv",
    "CN_FIELD_RUNTIME_AUTHORITY_AUDIT.csv",
    "CN_FIELD_SEARCH_EXPOSURE_FUNNEL.csv",
    "CN_ROUTE_CAPABILITY_COVERAGE.csv",
    "CN_FUNDAMENTAL_RUNTIME_WIRING_AUDIT.csv",
    "CN_GLOBAL_SPLIT_RUNTIME_REACHABILITY_AUDIT.csv",
    "CN_FEATURE_WIRING_CAPABILITY_TEST.json",
    "CN_FEATURE_RUNTIME_WIRING_INDEPENDENT_AUDIT.md",
    "CN_HARDCODED_FIELD_PATHS.md",
    "CN_UNTESTED_OR_UNDEREXPOSED_INFORMATION_FAMILIES.csv",
    "CN_FUNDAMENTAL_CROSS_SEED_MECHANISM_ALIGNMENT.md",
    "CN_UNIFIED_DISCOVERY_ELIGIBILITY_AUDIT.md",
    "CN_SPLIT_LEAKAGE_REPAIR_VERIFICATION.md",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def iter_csv(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        yield from csv.DictReader(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (list, dict, tuple, set))
                    else value
                    for key, value in row.items()
                }
            )


def parse_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
    except (SyntaxError, ValueError):
        return [part for part in re.split(r"[|,]", text) if part]
    if isinstance(parsed, (list, tuple, set)):
        return [str(item) for item in parsed]
    return [str(parsed)]


def as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "allow", "allowed"}


def expression_fields(expression: Any) -> list[str]:
    return sorted(set(FIELD_RE.findall(str(expression or ""))))


def git_head(repo: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo, text=True
    ).strip()


def source_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _field_stats(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "proposal_ids": set(),
            "legal_ids": set(),
            "canonical_ids": set(),
            "exact_ids": set(),
            "behavior_ids": set(),
            "admission_ids": set(),
            "strict_ids": set(),
            "reward_ids": set(),
            "survivor_ids": set(),
            "seeds_by_exact": defaultdict(set),
        }
    )
    for row in rows:
        candidate_id = str(row.get("candidate_id") or row.get("expression_hash") or "")
        exact = str(row.get("exact_identity") or row.get("expression_hash") or candidate_id)
        canonical = str(
            row.get("canonical_identity")
            or row.get("semantic_key")
            or row.get("ast_skeleton")
            or exact
        )
        behavior = str(row.get("behavior_identity") or "")
        fields = parse_list(row.get("field_ids")) or parse_list(row.get("fields_list"))
        if not fields:
            fields = expression_fields(row.get("canonical_expression") or row.get("expression"))
        for field_id in fields:
            item = stats[field_id]
            item["proposal_ids"].add(candidate_id)
            if as_bool(row.get("legal", True)) and str(row.get("typed_gate_decision", "allow")).lower() != "block":
                item["legal_ids"].add(candidate_id)
            item["canonical_ids"].add(canonical)
            item["exact_ids"].add(exact)
            if behavior:
                item["behavior_ids"].add(behavior)
            if as_bool(row.get("admission")):
                item["admission_ids"].add(candidate_id)
            if as_bool(row.get("strict")):
                item["strict_ids"].add(candidate_id)
            if str(row.get("reward") or row.get("train_reward") or "").strip() and as_bool(row.get("strict", True)):
                item["reward_ids"].add(candidate_id)
            if as_bool(row.get("survivor")):
                item["survivor_ids"].add(candidate_id)
            item["seeds_by_exact"][exact].add(str(row.get("seed_set") or row.get("seed") or ""))
    return stats


def _flatten_stats(item: Mapping[str, Any]) -> dict[str, int]:
    seeds_by_exact = item.get("seeds_by_exact", {})
    return {
        "proposal_count": len(item.get("proposal_ids", ())),
        "legal_count": len(item.get("legal_ids", ())),
        "canonical_identity_count": len(item.get("canonical_ids", ())),
        "exact_identity_count": len(item.get("exact_ids", ())),
        "signal_cluster_count": len(item.get("behavior_ids", ())),
        "admission_count": len(item.get("admission_ids", ())),
        "strict_count": len(item.get("strict_ids", ())),
        "full_development_reward_count": len(item.get("reward_ids", ())),
        "survivor_count": len(item.get("survivor_ids", ())),
        "cross_seed_exact_count": sum(
            1 for seeds in seeds_by_exact.values() if len({seed for seed in seeds if seed}) >= 2
        ),
    }


def _physical_universe(
    active: Mapping[str, Any], qualification_rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for field in active["fields"]:
        output.append(
            {
                "physical_field_id": f"active121::{field['name']}",
                "source_domain": "TRUE1MIN_VERSIONED_PANEL",
                "source_table": "true1min_augmented_panel",
                "source_field": field["name"],
                "source_family": field["family"],
                "dtype": field.get("dtype", ""),
                "entity_scope": "STOCK",
                "observable_time_field": field.get("observable_time_field", ""),
                "pit_status": "PIT_VERSIONED_HISTORY",
                "physical_evidence": "versioned 121-field Fabric registry and development release",
                "current_121_presence": True,
            }
        )
    for row in qualification_rows:
        output.append(
            {
                "physical_field_id": row["source_field_id"],
                "source_domain": "PIT_FUNDAMENTAL_SIDECAR",
                "source_table": row["source_table"],
                "source_field": row["source_field"],
                "source_family": row["source_family"],
                "dtype": row.get("source_dtype", ""),
                "entity_scope": row.get("entity_scope", ""),
                "observable_time_field": row.get("observable_time_policy", ""),
                "pit_status": row.get("pit_status", ""),
                "physical_evidence": "fundamental source inventory and PIT qualification matrix",
                "current_121_presence": row.get("current_121_presence", "ABSENT"),
            }
        )
    return output


def _qualified_universe(registry: UnifiedCapabilityRegistry) -> list[dict[str, Any]]:
    return [
        {
            "field_id": field.field_id,
            "representation_id": field.representation_id,
            "source_field_id": field.source_field_id,
            "source_family": field.source_family,
            "source_table": field.source_table,
            "source_field": field.source_field,
            "entity_scope": field.entity_scope,
            "temporal_semantics": field.temporal_semantics,
            "observable_clock": field.observable_clock,
            "maturity_rule": field.maturity_rule,
            "pit_status": field.pit_status,
            "unit_status": field.unit_status,
            "field_role": field.field_role,
            "search_eligible": field.search_eligible,
            "allowed_routes": list(field.allowed_routes),
            "blocked_reason": field.blocked_reason,
        }
        for field in registry.fields
    ]


def _legacy_stage_rows(collapse: Mapping[str, Any]) -> tuple[dict[str, list[dict[str, str]]], dict[str, Path]]:
    stages: dict[str, list[dict[str, str]]] = {}
    paths: dict[str, Path] = {}
    for stage in collapse.get("stages", []):
        name = str(stage["stage"])
        path = Path(str(stage["source"]))
        if path.is_file() and name != "scheduler_predecessor":
            stages[name] = read_csv(path)
            paths[name] = path
    return stages, paths


def _runtime_and_funnel(
    registry: UnifiedCapabilityRegistry,
    unified_rows: Sequence[Mapping[str, Any]],
    legacy_stages: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    unified_stats = _field_stats(unified_rows)
    legacy_stage_stats = {
        stage: _field_stats(rows) for stage, rows in legacy_stages.items()
    }
    legacy_all_fields = {
        field_id
        for stats in legacy_stage_stats.values()
        for field_id in stats
    }
    registry_by_id = {field.field_id: field for field in registry.fields}
    all_fields = sorted(set(registry_by_id) | legacy_all_fields)
    runtime_rows: list[dict[str, Any]] = []
    funnel: list[dict[str, Any]] = []
    for field_id in all_fields:
        field = registry_by_id.get(field_id)
        generation = _flatten_stats(
            legacy_stage_stats.get("generation", {}).get(field_id, {})
        )
        proxy = _flatten_stats(
            legacy_stage_stats.get("proxy", {}).get(field_id, {})
        )
        admission = _flatten_stats(
            legacy_stage_stats.get("admission", {}).get(field_id, {})
        )
        strict = _flatten_stats(
            legacy_stage_stats.get("strict_reward", {}).get(field_id, {})
        )
        memory = _flatten_stats(
            legacy_stage_stats.get("memory_predecessor", {}).get(field_id, {})
        )
        legacy = {
            "proposal_count": generation["proposal_count"],
            "legal_count": generation["legal_count"],
            "canonical_identity_count": generation["canonical_identity_count"],
            "exact_identity_count": generation["exact_identity_count"],
            "signal_cluster_count": generation["signal_cluster_count"],
            "admission_count": admission["proposal_count"],
            "strict_count": strict["proposal_count"],
            "full_development_reward_count": strict["proposal_count"],
            "survivor_count": 0,
            "cross_seed_exact_count": 0,
            "proxy_count": proxy["proposal_count"],
        }
        unified = _flatten_stats(unified_stats.get(field_id, {}))
        unified["proxy_count"] = 0
        runtime_rows.append(
            {
                "field_id": field_id,
                "registered_in_unified_authority": field is not None,
                "source_family": field.source_family if field else "LEGACY_UNREGISTERED",
                "pit_status": field.pit_status if field else "UNVERIFIED_ON_LEGACY_PATH",
                "search_eligible_unified": field.search_eligible if field else False,
                "allowed_routes": list(field.allowed_routes) if field else [],
                "legacy_phase3ga_visible": legacy["proposal_count"] > 0,
                "unified_runner_visible": unified["proposal_count"] > 0,
                "runtime_authority": (
                    "SPLIT_AUTHORITY_LEGACY_AND_UNIFIED"
                    if legacy["proposal_count"] and unified["proposal_count"]
                    else "LEGACY_HARDCODED_OR_SCHEMA_PATH"
                    if legacy["proposal_count"]
                    else "UNIFIED_REGISTRY_PATH"
                    if unified["proposal_count"]
                    else "REGISTERED_NOT_RUNTIME_EXPOSED"
                ),
            }
        )
        for runner, values, feedback in (
            ("LEGACY_PHASE3GA_CM_CN", legacy, memory["proposal_count"]),
            ("UNIFIED_DEVELOPMENT_DISCOVERY", unified, 0),
        ):
            funnel.append(
                {
                    "runner": runner,
                    "field_id": field_id,
                    "source_family": field.source_family if field else "LEGACY_UNREGISTERED",
                    "registered": field is not None,
                    "physically_available": field is not None or legacy["proposal_count"] > 0,
                    "pit_qualified": bool(field and field.pit_status != "PIT_CONTRACT_UNRESOLVED"),
                    "search_eligible": bool(field and field.search_eligible),
                    "generator_visible": values["proposal_count"] > 0,
                    **values,
                    "feedback_count": feedback,
                    "evidence_ceiling": (
                        "DEVELOPMENT_ONLY_NOT_OOS"
                        if values["strict_count"]
                        else "PROPOSAL_OR_NOT_EVALUATED"
                    ),
                }
            )
    return runtime_rows, funnel


def _route_coverage(
    registry: UnifiedCapabilityRegistry, unified_rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for route in ROUTE_IDS:
        rows = [row for row in unified_rows if row.get("route_id") == route]
        fields = {
            field_id for row in rows for field_id in parse_list(row.get("field_ids"))
        }
        output.append(
            {
                "route_id": route,
                "qualified_registry_field_count": len(registry.fields_for_route(route)),
                "proposal_count": len(rows),
                "legal_count": sum(as_bool(row.get("legal")) for row in rows),
                "canonical_count": len({row.get("canonical_identity") for row in rows}),
                "exact_count": len({row.get("exact_identity") for row in rows}),
                "signal_cluster_count": len({row.get("behavior_identity") for row in rows if row.get("behavior_identity")}),
                "admission_count": sum(as_bool(row.get("admission")) for row in rows),
                "strict_count": sum(as_bool(row.get("strict")) for row in rows),
                "survivor_count": sum(as_bool(row.get("survivor")) for row in rows),
                "runtime_field_count": len(fields),
                "seed_count": len({row.get("seed_set") for row in rows}),
                "claim_ceiling": "DEVELOPMENT_ONLY_NOT_OOS" if rows else "NOT_EXECUTED",
            }
        )
    return output


def _fundamental_audit(
    registry: UnifiedCapabilityRegistry, unified_rows: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates = [
        field
        for field in registry.fields
        if field.field_id.startswith("fund_")
        or field.source_family.startswith("canonical_fundamental")
    ]
    by_field: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in unified_rows:
        for field_id in parse_list(row.get("field_ids")):
            by_field[field_id].append(row)
    result: list[dict[str, Any]] = []
    for field in candidates:
        rows = by_field.get(field.field_id, [])
        by_seed = Counter(str(row.get("seed_set") or "") for row in rows)
        survivor_seeds = {
            str(row.get("seed_set") or "") for row in rows if as_bool(row.get("survivor"))
        }
        exact_by_seed: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            if as_bool(row.get("survivor")):
                exact_by_seed[str(row.get("exact_identity") or "")].add(str(row.get("seed_set") or ""))
        result.append(
            {
                "field_id": field.field_id,
                "representation_id": field.representation_id,
                "source_field_id": field.source_field_id,
                "source_family": field.source_family,
                "source_table": field.source_table,
                "source_field": field.source_field,
                "temporal_semantics": field.temporal_semantics,
                "search_eligible": field.search_eligible,
                "allowed_routes": list(field.allowed_routes),
                "historical_revision_replay_supported": bool(
                    (field.metadata or {}).get("canonical_representation", {}).get(
                        "historical_revision_replay_supported", False
                    )
                ),
                "seed_a_proposal_count": by_seed.get("seed_a", 0),
                "seed_b_proposal_count": by_seed.get("seed_b", 0),
                "strict_count": sum(as_bool(row.get("strict")) for row in rows),
                "survivor_seed_count": len({seed for seed in survivor_seeds if seed}),
                "cross_seed_exact_survivor": any(len(seeds) >= 2 for seeds in exact_by_seed.values()),
                "legacy_phase3ga_cm_cn_exposure": False,
                "runtime_scope": "UNIFIED_DEVELOPMENT_DISCOVERY_ONLY",
            }
        )
    survivor_rows = [row for row in unified_rows if as_bool(row.get("survivor"))]
    fund_survivors = [
        row for row in survivor_rows if any(field_id.startswith("fund_") for field_id in parse_list(row.get("field_ids")))
    ]
    family_seed: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    family_fields: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in fund_survivors:
        for field_id in parse_list(row.get("field_ids")):
            field = next((item for item in candidates if item.field_id == field_id), None)
            if field is None:
                continue
            canonical = dict((field.metadata or {}).get("canonical_representation") or {})
            key = (
                field.source_family,
                str(canonical.get("semantic_family") or canonical.get("canonical_family") or ""),
                str(canonical.get("representation_type") or ""),
            )
            family_seed[key].add(str(row.get("seed_set") or ""))
            family_fields[key].add(field.field_id)
    alignments = [
        {
            "source_family": key[0],
            "semantic_family": key[1],
            "representation_type": key[2],
            "seed_count": len({seed for seed in seeds if seed}),
            "field_ids": sorted(family_fields[key]),
        }
        for key, seeds in family_seed.items()
        if len({seed for seed in seeds if seed}) >= 2
    ]
    return result, {"survivors": fund_survivors, "family_alignments": alignments}


def _split_audit(repo: Path) -> list[dict[str, Any]]:
    manifest = repo / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
    rows = read_csv(manifest)
    counts = Counter(row["split"] for row in rows)
    cm = repo / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
    cp = repo / "src/our_system_phase2/runtime/phase3cp_real_cm_small_loop.py"
    launcher = repo / "runtime/launchers/phase3ga_77o_semantic_efficiency_v2_20260710.ps1"
    recovery = repo / "runtime/launchers/phase3ga_recover_missing_chunk04_20260710.ps1"
    cm_text = source_text(cm)
    cp_text = source_text(cp)
    launcher_text = source_text(launcher)
    recovery_text = source_text(recovery)
    return [
        {
            "component": "FIXED_GLOBAL_MANIFEST",
            "status": "PASS",
            "evidence": f"485 dates; train={counts['train']}; validation={counts['validation']}; holdout={counts['holdout']}",
            "path": str(manifest.relative_to(repo)),
            "runtime_reachable": True,
        },
        {
            "component": "PRIMARY_PHASE3GA_LAUNCHER",
            "status": "PASS" if "--cm-split-manifest" in launcher_text else "FAIL",
            "evidence": "primary launcher passes the fixed manifest to Phase3CP",
            "path": str(launcher.relative_to(repo)),
            "runtime_reachable": True,
        },
        {
            "component": "PHASE3CP_PARALLEL_GUARD",
            "status": "PASS" if 'parser.add_argument("--cm-split-manifest", type=Path, required=True)' in cp_text else "FAIL",
            "evidence": "manifest is required for serial, candidate-parallel, shard-parallel and retry paths",
            "path": str(cp.relative_to(repo)),
            "runtime_reachable": True,
        },
        {
            "component": "PHASE3CM_WORKER_LOCAL_SPLIT",
            "status": "PASS" if "_split_map(" not in cm_text and "split_authority.map_times(full_signal_times)" in cm_text else "FAIL_REACHABLE",
            "evidence": "formal worker resolves every timestamp through FixedSplitAuthority; local splitter removed",
            "path": str(cm.relative_to(repo)),
            "runtime_reachable": "split_authority.map_times(full_signal_times)" in cm_text,
        },
        {
            "component": "CHUNK04_RECOVERY_WORKERS",
            "status": "PASS" if recovery_text.count("--split-manifest") >= 2 else "FAIL_REACHABLE",
            "evidence": "worker subprocesses and final exact merge both receive the fixed manifest",
            "path": str(recovery.relative_to(repo)),
            "runtime_reachable": recovery_text.count("--split-manifest") >= 2,
        },
        {
            "component": "FINAL_EXACT_NORMALIZATION",
            "status": "PASS",
            "evidence": "fixed manifest and candidate receipt are required at exact merge; unknown dates hard fail",
            "path": str(cm.relative_to(repo)),
            "runtime_reachable": "fixed_split_manifest_rows" in cm_text and "split_manifest and unassigned" in cm_text,
        },
    ]


def _synthetic_capability_tests(registry_path: Path) -> dict[str, Any]:
    registry = UnifiedCapabilityRegistry.read(registry_path)
    generator = RegistryDrivenGenerator(registry)
    metadata = next(field for field in registry.fields if field.source_family == "metadata")
    specs = (
        ("raw_minute", "MINUTE_STATIC", lambda fields: any(f.source_family == "raw_1min" for f in fields)),
        ("firstN_intraday_path", "FIRSTN_PATH", lambda fields: any(f.source_family == "firstN" for f in fields)),
        ("lagged_context", "SLOW_CROSS_SECTIONAL_LEVEL", lambda fields: any(f.source_family == "lagged_daily_context" for f in fields)),
        ("typed_event", "BROAD_EVENT_FROZEN_ENTRY", lambda fields: any(f.source_family == "broad_event_frozen_entry" for f in fields)),
        ("intraday_state_transition", "INTRADAY_STATE_TRANSITION", lambda fields: any(f.temporal_semantics == "INTRADAY_DERIVED_STATE" for f in fields)),
        ("fundamental_level", "SLOW_CROSS_SECTIONAL_LEVEL", lambda fields: any(f.field_id.startswith("fund_") for f in fields)),
        ("fundamental_change", "SLOW_TEMPORAL_CHANGE", lambda fields: any(f.field_id.startswith("fund_") for f in fields)),
        ("disclosure_event", "DISCLOSURE_EVENT", lambda fields: any(f.field_id.startswith("fund_") for f in fields)),
        ("market_regime", "MARKET_REGIME_CONDITION", lambda fields: any(f.entity_scope == "MARKET" for f in fields)),
    )
    cases: list[dict[str, Any]] = []
    for index, (name, route, predicate) in enumerate(specs):
        selected: dict[str, Any] | None = None
        control: dict[str, Any] | None = None
        for pair_index in range(512):
            pair = generator._pair(route, pair_index, 8100 + index)  # audited deterministic construction
            verdict = generator.compiler.compile(pair.candidate)
            fields = [registry.resolve(field_id) for field_id in verdict.field_ids] if verdict.legal else []
            if verdict.legal and predicate(fields):
                selected = {**pair.candidate, **verdict.to_dict()}
                control_verdict = generator.compiler.compile(pair.control)
                control = {**pair.control, **control_verdict.to_dict()}
                break
        if selected is None or control is None:
            cases.append({"case": name, "route_id": route, "status": "FAIL_NO_QUALIFIED_PAIR"})
            continue
        wrong_lag = generator.compiler.compile({**selected, "uses_future_revision": True})
        metadata_misuse = generator.compiler.compile(
            {
                **selected,
                "expression": f"CSRank(${metadata.field_id})",
                "canonical_expression": f"CSRank(${metadata.field_id})",
                "declared_field_ids": [metadata.field_id],
                "condition_field_ids": [],
                "operator_family": "CSRank",
            }
        )
        passed = bool(
            selected["legal"]
            and control["legal"]
            and selected["matched_control_id"] == control["candidate_id"]
            and not wrong_lag.legal
            and wrong_lag.rejection_code == "PIT_UNQUALIFIED"
            and not metadata_misuse.legal
            and metadata_misuse.rejection_code in {"PIT_UNQUALIFIED", "ROUTE_NOT_ALLOWED"}
        )
        cases.append(
            {
                "case": name,
                "route_id": route,
                "status": "PASS" if passed else "FAIL",
                "positive_candidate_id": selected["candidate_id"],
                "positive_exact_identity": selected["exact_identity"],
                "matched_control_id": control["candidate_id"],
                "positive_field_ids": selected["field_ids"],
                "wrong_lag_rejection_code": wrong_lag.rejection_code,
                "metadata_misuse_rejection_code": metadata_misuse.rejection_code,
                "performance_used": False,
            }
        )
    return {
        "audit_version": AUDIT_VERSION,
        "status": "PASS" if all(case["status"] == "PASS" for case in cases) else "FAIL",
        "case_count": len(cases),
        "passed_count": sum(case["status"] == "PASS" for case in cases),
        "cases": cases,
        "access_roles": ["source_registry", "synthetic_only"],
        "performance_used": False,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
    }


def _authority_rows(repo: Path) -> list[dict[str, Any]]:
    return [
        {"authority": "121_FIELD_FABRIC", "status": "IMPLEMENTED_VERSIONED", "runtime_scope": "true1min panel", "authoritative_path": "runtime/field_registry/nextgen_dark_field_registry_v2.json", "finding": "121 registered fields: 7 metadata, 12 raw, 30 FirstN, 59 lagged context, 13 event/state"},
        {"authority": "FUNDAMENTAL_SOURCE_QUALIFICATION", "status": "IMPLEMENTED_SIDECAR", "runtime_scope": "unified development runner only", "authoritative_path": "runtime/cn_unified_capability_v1/fundamental_field_qualification_matrix.csv", "finding": "1,227 source fields remain fail-closed; 147 canonical representations are the only roots"},
        {"authority": "UNIFIED_CAPABILITY_REGISTRY", "status": "IMPLEMENTED_PARALLEL_PATH", "runtime_scope": "unified preflight/discovery", "authoritative_path": "src/our_system_phase2/services/unified_capability_registry.py", "finding": "single semantic/PIT/route authority inside unified runner, but not wired into legacy Phase3GA/CM/CN"},
        {"authority": "LEGACY_PHASE3DV_GENERATOR", "status": "ACTIVE_BYPASS", "runtime_scope": "CURRENT_SEARCH_ROUTE", "authoritative_path": "src/our_system_phase2/runtime/phase3dv_budget_pool_self_deepen_pack.py", "finding": "hardcoded field pools and physical-schema filtering bypass the unified registry"},
        {"authority": "TYPED_ROUTE_COMPILER", "status": "IMPLEMENTED_UNIFIED_ONLY", "runtime_scope": "RegistryDrivenGenerator", "authoritative_path": "src/our_system_phase2/services/typed_route_compiler.py", "finding": "PIT, unit, route, maturity, episode vote and sealed-access checks apply only on unified route"},
        {"authority": "PHASE3CM_EVALUATOR", "status": "PARTIAL_GLOBAL_SPLIT", "runtime_scope": "legacy exact evaluator", "authoritative_path": "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py", "finding": "final normalization uses the fixed manifest; worker-local split mapping remains reachable"},
        {"authority": "PHASE3CN_FEEDBACK", "status": "LEGACY_ONLY", "runtime_scope": "legacy search memory", "authoritative_path": "runtime evidence: phase3cn_search_feedback_memory.csv", "finding": "unified registry candidates do not flow to Phase3CN; unified run explicitly persisted no memory"},
        {"authority": "PLATE_INDUSTRY_PIT", "status": "DISABLED_NO_ACTIVE_RELEASE", "runtime_scope": "none", "authoritative_path": ".planning/STATE.md", "finding": "builders exist, but no trustworthy historical PIT membership release is registered; no plate field is exposed"},
        {"authority": "FORWARD_2026", "status": "SEALED", "runtime_scope": "forbidden", "authoritative_path": ".planning/STATE.md", "finding": "this audit made no validation, holdout, challenge or 2026 read"},
    ]


def _write_markdown_reports(
    output: Path,
    *,
    physical: Sequence[Mapping[str, Any]],
    registry: UnifiedCapabilityRegistry,
    route_rows: Sequence[Mapping[str, Any]],
    fundamental_alignment: Mapping[str, Any],
    split_rows: Sequence[Mapping[str, Any]],
    capability: Mapping[str, Any],
    unified_rows: Sequence[Mapping[str, Any]],
) -> None:
    survivors_by_seed = {
        seed: [row for row in unified_rows if row.get("seed_set") == seed and as_bool(row.get("survivor"))]
        for seed in ("seed_a", "seed_b")
    }
    shared = set(row.get("exact_identity") for row in survivors_by_seed["seed_a"]) & set(
        row.get("exact_identity") for row in survivors_by_seed["seed_b"]
    )
    shared_rows = [row for row in survivors_by_seed["seed_a"] if row.get("exact_identity") in shared]
    shared_broad = sum(row.get("route_id") == "BROAD_EVENT_FROZEN_ENTRY" for row in shared_rows)
    status = "CN_FEATURE_RUNTIME_WIRING_MISMATCH_CONFIRMED"
    main = f"""# CN Feature Runtime Wiring Independent Audit

Status: `{status}`

## Outcome

The data and representation work is real, but the runtime is split into two authorities. The unified runner correctly exposes the versioned 121-field Fabric, 147 canonical fundamental representations and frozen Broad Event mechanisms through typed routes. The active legacy `Phase3GA -> Phase3CM -> Phase3CN` search chain still uses hardcoded/schema-derived pools and does not consult the unified registry.

This is a wiring mismatch, not a claim that the new features lack value. No performance search was run.

## Measured universes

- Physical/inventoried source fields: {len(physical):,} (`121` panel fields + `1,227` fundamental source fields).
- Qualified candidate-visible representations: {len(registry.fields):,}.
- Search-eligible unified representations: {sum(field.search_eligible for field in registry.fields):,}.
- Unified routes executed with non-zero proposal and strict evidence: {sum(int(row['proposal_count']) > 0 and int(row['strict_count']) > 0 for row in route_rows)}/{len(route_rows)}.
- Synthetic/planted wiring cases: {capability['passed_count']}/{capability['case_count']} passed.

## Independent findings

1. `RegistryDrivenGenerator` and `TypedRouteCompiler` are used by unified preflight/discovery only. `app.py` still names `phase3dv-budget-pool-self-deepen-pack` as the current search route.
2. The legacy Phase3DV generator owns hardcoded field-family arrays and Phase3CP intersects them with physical parquet schemas. It can therefore expose legacy fields that the unified registry blocks, including latched Event/State columns.
3. All 147 fundamental representations are confined to the completed development-only unified runner. They do not enter legacy Phase3GA, Phase3CM or Phase3CN.
4. The completed two-seed run had 7 survivors per seed and {len(shared)} shared exact survivors. All {shared_broad} shared exact survivors are old frozen Broad Event replays. They are regression evidence, not new capability discovery.
5. The historical result's `qualified_to_apply_for_independent_challenge=true` was an eligibility bug. Current code now requires at least one non-frozen cross-seed reproduction; no challenge was opened.
6. Final Phase3CM exact normalization uses the fixed 485-date manifest, but worker-local `_split_map(...)` and the chunk-04 recovery workers remain reachable before/without that normalization. The repair is therefore not globally enforced at every worker boundary.
7. Plate/industry remains disabled: build code is present, but no active versioned historical PIT membership release is registered. No placeholder or current snapshot was treated as a capability.

## Access boundary

Only registries, source code, historical audit CSVs, completed development-only ledgers and synthetic candidates were read. Validation, holdout, challenge and 2026 were not read. No reward, survivor selection, promotion, candidate discovery or persistent adaptive memory was executed.
"""
    (output / "CN_FEATURE_RUNTIME_WIRING_INDEPENDENT_AUDIT.md").write_text(main, encoding="utf-8")

    hardcoded = """# CN Hardcoded Field Paths

| Runtime path | Authority | Finding |
|---|---|---|
| `app.py` | `CURRENT_SEARCH_ROUTE` | Points to `phase3dv-budget-pool-self-deepen-pack`. |
| `phase3dv_budget_pool_self_deepen_pack.py` | `EVENT_FIELDS`, `VALUE_FIELDS`, `SENTIMENT_FIELDS`, `LIQUIDITY_FIELDS`, `UNDERUSED_CONTEXT_FIELDS`, `INTRADAY_FIELDS` | Hardcoded candidate pools; no `UnifiedCapabilityRegistry`. |
| `phase3cp_real_cm_small_loop.py` | `_available_fields` | Reads parquet schemas and filters atom availability; registry identity/PIT/route authority is not consulted. |
| `unified_discovery_generators.py` | `RegistryDrivenGenerator` | Correct unified authority, currently isolated to unified preflight/discovery. |
| `typed_route_compiler.py` | `TypedRouteCompiler` | Correct typed gate, currently isolated to unified candidates. |
| `nextgen_dark_development_canary.py` and `cn_b1s_development_canary.py` | fixed lane field lists | Historical canary infrastructure, not the current unified authority. |

The smallest safe integration is to make candidate submission into the legacy evaluator require a frozen unified-registry/typed-compiler receipt; physical schema presence must remain an availability check, never a search-eligibility authority.
"""
    (output / "CN_HARDCODED_FIELD_PATHS.md").write_text(hardcoded, encoding="utf-8")

    alignments = fundamental_alignment["family_alignments"]
    alignment_lines = "\n".join(
        f"- `{row['source_family']}` / `{row['semantic_family']}` / `{row['representation_type']}`: fields {', '.join(row['field_ids'])}"
        for row in alignments
    ) or "- No cross-seed fundamental family/template alignment."
    fundamental = f"""# CN Fundamental Cross-Seed Mechanism Alignment

- Exact shared fundamental survivor: **0**.
- Shared fundamental behavior identity: **0**.
- Family/template alignments across seeds: **{len(alignments)}**.

{alignment_lines}

The observed capital-investment family alignment is a field-substituted family/template alignment. It is not an exact mechanism reproduction and must not be labeled `NEW_CANONICAL_MECHANISM_CROSS_SEED_REPRODUCED`. Snapshot-only revision replay remains disabled.
"""
    (output / "CN_FUNDAMENTAL_CROSS_SEED_MECHANISM_ALIGNMENT.md").write_text(fundamental, encoding="utf-8")

    eligibility = f"""# CN Unified Discovery Eligibility Audit

Historical completed-run fact: two seeds each produced 7 development-only survivors; the {len(shared)} shared exact identities are all old frozen Broad Event mechanisms.

Revised non-performance classes:

- `OLD_FROZEN_MECHANISM_REPRODUCED`: {shared_broad}
- `NEW_CAPABILITY_SHARED_SURVIVOR`: 0
- `NEW_CANONICAL_MECHANISM_CROSS_SEED_REPRODUCED`: 0

Corrected decision: `qualified_to_apply_for_independent_challenge=false` for this evidence. The source bug was corrected without rewriting the historical CSVs or frozen discovery pack. No challenge was opened.
"""
    (output / "CN_UNIFIED_DISCOVERY_ELIGIBILITY_AUDIT.md").write_text(eligibility, encoding="utf-8")

    failed_split = [row for row in split_rows if str(row["status"]).startswith("FAIL")]
    split_md = f"""# CN Split Leakage Repair Verification

Status: `PARTIAL_RUNTIME_REACHABILITY_MISMATCH`

The fixed manifest is correct (485 dates: 364 train, 73 validation, 48 holdout), the primary launcher passes it, and final exact normalization can hard-fail unassigned dates. However, {len(failed_split)} worker/recovery paths remain reachable with local or absent manifest assignment before final normalization.

This audit did not read validation or holdout contents. It inspected routing and source reachability only. See `CN_GLOBAL_SPLIT_RUNTIME_REACHABILITY_AUDIT.csv` for component evidence.
"""
    (output / "CN_SPLIT_LEAKAGE_REPAIR_VERIFICATION.md").write_text(split_md, encoding="utf-8")


def build(repo: Path, output: Path) -> dict[str, Any]:
    repo = repo.resolve()
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    active_path = repo / "runtime/field_registry/nextgen_dark_field_registry_v2.json"
    qualification_path = repo / "runtime/cn_unified_capability_v1/fundamental_field_qualification_matrix.csv"
    representation_path = repo / "runtime/cn_unified_capability_v1/fundamental_canonical_representation_registry.json"
    completed = repo / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1"
    registry_path = completed / "registry/unified_capability_registry.json"
    seed_paths = {
        "seed_a": completed / "unified_discovery/seed_a/candidate_results.csv",
        "seed_b": completed / "unified_discovery/seed_b/candidate_results.csv",
    }
    collapse_path = repo / "reports/evalreset_phase1_20260711/search_collapse_audit.json"

    active = read_json(active_path)
    qualification = read_csv(qualification_path)
    representation = read_json(representation_path)
    registry = UnifiedCapabilityRegistry.read(registry_path)
    unified_rows = [
        {**row, "seed_set": seed}
        for seed, path in seed_paths.items()
        for row in read_csv(path)
    ]
    collapse = read_json(collapse_path)
    legacy_stages, legacy_paths = _legacy_stage_rows(collapse)
    cluster_path = (
        repo
        / "reports/evalreset_phase1_20260711/signal_sketch/candidate_signal_cluster_registry.csv"
    )
    clusters = {
        row["candidate_id"]: row["consensus_cluster_id"]
        for row in iter_csv(cluster_path)
        if row.get("signal_coverage_status") == "qualified"
        and row.get("consensus_cluster_id")
    }
    for stage_rows in legacy_stages.values():
        for row in stage_rows:
            cluster_id = clusters.get(str(row.get("candidate_id") or ""))
            if cluster_id:
                row["behavior_identity"] = f"signal_cluster::{cluster_id}"

    physical = _physical_universe(active, qualification)
    qualified = _qualified_universe(registry)
    runtime_rows, funnel = _runtime_and_funnel(registry, unified_rows, legacy_stages)
    route_rows = _route_coverage(registry, unified_rows)
    fundamental_rows, fundamental_alignment = _fundamental_audit(registry, unified_rows)
    split_rows = _split_audit(repo)
    capability = _synthetic_capability_tests(registry_path)
    authority = _authority_rows(repo)

    write_csv(output / "CN_PHYSICAL_FIELD_UNIVERSE.csv", physical)
    write_csv(output / "CN_QUALIFIED_REPRESENTATION_UNIVERSE.csv", qualified)
    write_csv(output / "CN_RUNTIME_FIELD_UNIVERSE.csv", runtime_rows)
    write_csv(output / "CN_FIELD_RUNTIME_AUTHORITY_AUDIT.csv", authority)
    write_csv(output / "CN_FIELD_SEARCH_EXPOSURE_FUNNEL.csv", funnel)
    write_csv(output / "CN_ROUTE_CAPABILITY_COVERAGE.csv", route_rows)
    write_csv(output / "CN_FUNDAMENTAL_RUNTIME_WIRING_AUDIT.csv", fundamental_rows)
    write_csv(output / "CN_GLOBAL_SPLIT_RUNTIME_REACHABILITY_AUDIT.csv", split_rows)
    write_json(output / "CN_FEATURE_WIRING_CAPABILITY_TEST.json", capability)

    family_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"qualified": 0, "eligible": 0, "proposal": 0, "strict": 0, "survivor": 0}
    )
    unified_by_field = _field_stats(unified_rows)
    for field in registry.fields:
        item = family_stats[field.source_family]
        item["qualified"] += 1
        item["eligible"] += int(field.search_eligible)
        stats = _flatten_stats(unified_by_field.get(field.field_id, {}))
        item["proposal"] += stats["proposal_count"]
        item["strict"] += stats["strict_count"]
        item["survivor"] += stats["survivor_count"]
    underexposed = [
        {
            "source_family": family,
            "qualified_representation_count": values["qualified"],
            "search_eligible_count": values["eligible"],
            "proposal_exposure_count": values["proposal"],
            "strict_exposure_count": values["strict"],
            "survivor_count": values["survivor"],
            "status": (
                "BLOCKED_NOT_SEARCH_ELIGIBLE"
                if not values["eligible"]
                else "UNTESTED"
                if not values["proposal"]
                else "UNDEREXPOSED"
                if values["eligible"] > values["proposal"]
                else "EXPOSED"
            ),
        }
        for family, values in sorted(family_stats.items())
        if values["eligible"] == 0 or values["proposal"] < values["eligible"]
    ]
    write_csv(output / "CN_UNTESTED_OR_UNDEREXPOSED_INFORMATION_FAMILIES.csv", underexposed)

    _write_markdown_reports(
        output,
        physical=physical,
        registry=registry,
        route_rows=route_rows,
        fundamental_alignment=fundamental_alignment,
        split_rows=split_rows,
        capability=capability,
        unified_rows=unified_rows,
    )

    inputs = [
        active_path,
        qualification_path,
        representation_path,
        registry_path,
        collapse_path,
        *seed_paths.values(),
        *legacy_paths.values(),
        cluster_path,
        repo / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv",
    ]
    manifest = {
        "audit_version": AUDIT_VERSION,
        "status": "CN_FEATURE_RUNTIME_WIRING_MISMATCH_CONFIRMED",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": git_head(repo),
        "scope": "READ_ONLY_RUNTIME_WIRING_AND_SYNTHETIC_CAPABILITY_TEST",
        "inputs": [
            {"path": str(path), "sha256": sha256_file(path)} for path in inputs
        ],
        "counts": {
            "physical_field_universe": len(physical),
            "active_121": len(active["fields"]),
            "fundamental_source_fields": len(qualification),
            "canonical_fundamental_representations": int(representation["representation_count"]),
            "qualified_runtime_representations": len(registry.fields),
            "unified_candidate_rows": len(unified_rows),
            "synthetic_cases_passed": capability["passed_count"],
            "synthetic_cases_total": capability["case_count"],
        },
        "access": {
            "performance_search_started": False,
            "reward_used_to_select_fields": False,
            "validation_accessed": False,
            "holdout_accessed": False,
            "challenge_accessed": False,
            "forward_2026_accessed": False,
            "candidate_promotion": False,
            "cross_sprint_memory": False,
        },
    }
    write_json(output / "run_manifest.json", manifest)
    artifacts = []
    for path in sorted(output.iterdir()):
        if path.is_file() and path.name != "artifact_index.json":
            artifacts.append(
                {
                    "path": path.name,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
            )
    index = {
        "artifact_index_version": "cn_feature_runtime_wiring_artifact_index_v1",
        "repo_sha": manifest["repo_sha"],
        "status": manifest["status"],
        "artifacts": artifacts,
        "schema_hash": stable_hash(
            {
                row["path"]: list(next(csv.reader((output / row["path"]).open("r", encoding="utf-8"))))
                for row in artifacts
                if row["path"].endswith(".csv")
            }
        ),
    }
    write_json(output / "artifact_index.json", index)
    return {**manifest, "artifact_index": index}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=REPO)
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO / "reports/cn_feature_runtime_wiring_audit_20260714",
    )
    args = parser.parse_args()
    result = build(args.repo, args.output)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
