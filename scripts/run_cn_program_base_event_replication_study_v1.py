from __future__ import annotations

import argparse, gc, hashlib, json, time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Mapping, Sequence

import psutil

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large
from scripts import run_cn_program_optimizer_tournament_v1 as tournament
from scripts import run_cn_program_primitive_main_production_v1 as base
from scripts import run_cn_program_primitive_market_successor_v3 as v3
from scripts.run_cn_program_optimizer_large_fresh_v3 import ARMS, SEEDS, EVOLUTION_CONFIG
from our_system_phase2.services.program_optimizer_large_fresh_v3 import LargeFreshProgramBanditV3
from our_system_phase2.services.program_search_optimizer_v1 import normalized_program_gene_identity_v1
from our_system_phase2.services.program_search_primitive_credit_v1 import primitive_program_metadata_v1
from our_system_phase2.services.unified_capability_registry import stable_hash

CAMPAIGN_ID = "CN_PROGRAM_BASE_EVENT_REPLICATION_STUDY_V1"
CAMPAIGN_PROFILE = "cn_program_base_event_replication_study_v1"
STATUS_COMPLETE = "BASE_EVENT_REPLICATION_STUDY_COMPLETE"
PASS_STATUS = "BASE_EVENT_REPLICATION_STUDY_PROSPECTIVE_PASS"
FAIL_STATUS = "BASE_EVENT_REPLICATION_STUDY_PROSPECTIVE_FAIL"
CLOSURE_NAME = "CN_PROGRAM_BASE_EVENT_REPLICATION_STUDY_V1_COMPLETE.json"
PLAN_RELATIVE_PATH = Path("runtime/run_plans/cn_program_base_event_replication_study_plan.json")
CHECKPOINT_SIZE = 24
TEMPLATE = "BASE_EVENT"
P = "PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1"
U = "UNIFORM_CONTROL"
E = "CATALOG_TYPED_EVOLUTION_PROGRAM_V2"
REPLICATES = ("CONFIRM_A", "CONFIRM_B")


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bound_payload(repo_root: Path, binding: Mapping[str, Any], field: str, label: str) -> dict[str, Any]:
    path = (repo_root / Path(str(binding["relative_path"]))).resolve()
    if not path.is_relative_to(repo_root.resolve()) or not path.is_file():
        raise RuntimeError(f"{label}_FILE_MISSING")
    if _sha256_file(path) != str(binding["file_sha256"]):
        raise RuntimeError(f"{label}_FILE_DRIFT")
    row = _read(path)
    body = dict(row)
    claim = str(body.pop(field, ""))
    if claim != str(binding["payload_sha256"]) or stable_hash(body) != claim:
        raise RuntimeError(f"{label}_PAYLOAD_DRIFT")
    return row


def verify_plan(path: Path) -> dict[str, Any]:
    plan = _read(path.resolve())
    body = dict(plan)
    claim = str(body.pop("plan_payload_sha256", ""))
    if not claim or stable_hash(body) != claim:
        raise RuntimeError("BASE_EVENT_REPLICATION_PLAN_SELF_HASH_DRIFT")
    if plan.get("schema_version") != "cn_program_base_event_replication_study_plan_v1" or plan.get("status") != "BASE_EVENT_REPLICATION_STUDY_PLAN_FROZEN_NOT_RUN" or plan.get("candidate_evaluation_executed") is not False:
        raise RuntimeError("BASE_EVENT_REPLICATION_PLAN_STATUS_DRIFT")
    if plan.get("anchor_v3_used_in_prospective_gate") is not False or plan.get("focus_template") != TEMPLATE:
        raise RuntimeError("BASE_EVENT_REPLICATION_ANCHOR_OR_TEMPLATE_DRIFT")
    budget = dict(plan["budget"])
    if budget != {"hard_cap_logical_records": 240, "checkpoint_count": 10, "checkpoint_size": 24, "replicate_count": 2, "records_per_replicate": 120, "primitive_records": 144, "uniform_records": 48, "typed_evolution_records": 48}:
        raise RuntimeError("BASE_EVENT_REPLICATION_BUDGET_DRIFT")
    expected_orders = {
        "CONFIRM_A": [U, P, P, E, P],
        "CONFIRM_B": [E, P, P, P, U],
    }
    reps = {str(row["replicate_id"]): dict(row) for row in plan["replicates"]}
    if set(reps) != set(REPLICATES):
        raise RuntimeError("BASE_EVENT_REPLICATION_REPLICATE_COVERAGE_DRIFT")
    if reps["CONFIRM_A"]["pool_id"] != "REPL_1" or int(reps["CONFIRM_A"]["raw_offset"]) != 3584 or list(reps["CONFIRM_A"]["arm_order"]) != expected_orders["CONFIRM_A"]:
        raise RuntimeError("BASE_EVENT_REPLICATION_CONFIRM_A_DRIFT")
    if reps["CONFIRM_B"]["pool_id"] != "REPL_2" or int(reps["CONFIRM_B"]["raw_offset"]) != 5120 or list(reps["CONFIRM_B"]["arm_order"]) != expected_orders["CONFIRM_B"]:
        raise RuntimeError("BASE_EVENT_REPLICATION_CONFIRM_B_DRIFT")
    schedule = [dict(row) for row in plan["schedule"]]
    if len(schedule) != 10 or [int(x["checkpoint"]) for x in schedule] != list(range(10)) or [int(x["start_ordinal"]) for x in schedule] != [24 * i for i in range(10)]:
        raise RuntimeError("BASE_EVENT_REPLICATION_SCHEDULE_GEOMETRY_DRIFT")
    for rid in REPLICATES:
        rows = [row for row in schedule if row["replicate_id"] == rid]
        if [row["arm"] for row in rows] != expected_orders[rid] or any(row["template"] != TEMPLATE for row in rows):
            raise RuntimeError("BASE_EVENT_REPLICATION_SCHEDULE_ORDER_DRIFT")
    expected_gate = {
        "aggregate_primitive_productive_rate_min": 0.45,
        "aggregate_primitive_uplift_stable_2of3_rate_min": 0.35,
        "aggregate_primitive_to_best_control_productive_ratio_min": 1.20,
        "aggregate_primitive_minus_best_control_productive_rate_min": 0.08,
        "aggregate_primitive_to_best_control_uplift_stable_ratio_min": 1.40,
        "aggregate_primitive_minus_best_control_uplift_stable_rate_min": 0.10,
        "each_replicate_primitive_productive_rate_min": 0.30,
        "each_replicate_primitive_uplift_stable_2of3_rate_min": 0.25,
        "replicate_directional_policy_win_count_min": 2,
        "behavior_pair_rate_min": 0.75,
        "effective_spent_overlap_count_required": 0,
        "restricted_reads_required_zero": True,
    }
    if dict(plan["prospective_gate"]) != expected_gate:
        raise RuntimeError("BASE_EVENT_REPLICATION_GATE_DRIFT")
    authority = dict(plan["search_authority"])
    if any(int(authority.get(key) or 0) != 0 for key in ("production_results_in_stats", "v1_successor_results_in_stats", "v2_successor_results_in_stats", "v3_successor_results_in_stats")):
        raise RuntimeError("BASE_EVENT_REPLICATION_STATS_IMPORT_COUNT_DRIFT")
    if any(authority.get(key) is not False for key in ("production_feedback_imported_into_primitive_stats", "v1_successor_feedback_imported_into_primitive_stats", "v2_successor_feedback_imported_into_primitive_stats", "v3_successor_feedback_imported_into_primitive_stats", "diversity_contract_changed")) or int(authority.get("max_variants_per_base_per_template") or 0) != 4:
        raise RuntimeError("BASE_EVENT_REPLICATION_SEARCH_AUTHORITY_DRIFT")
    resource = dict(plan["resource_contract"])
    if resource.get("profile") != "SEARCH_DUAL_24" or int(resource.get("primary_executor_workers") or 0) != 24 or int(resource.get("fallback_executor_workers") or 0) != 16 or resource.get("evaluator_pool_lifetime") != "PERSISTENT_RUN_SCOPE" or float(resource.get("minimum_records_per_hour_after_first_checkpoint") or 0) < 650.0 or int(resource.get("minimum_free_memory_bytes") or 0) < 24 * 1024**3 or int(resource.get("throughput_enforcement_after_warm_checkpoints") or 0) != 2 or float(resource.get("wall_clock_budget_minutes") or 0) != 40.0 or resource.get("persistent_pool_record_hash_parity_required") is not True:
        raise RuntimeError("BASE_EVENT_REPLICATION_RESOURCE_CONTRACT_DRIFT")
    if any(int(v) != 0 for v in dict(plan["restricted_reads"]).values()) or plan.get("oos_authority") != "NONE" or plan.get("promotion_authorized") is not False or plan.get("automatic_successor_authorized") is not False:
        raise RuntimeError("BASE_EVENT_REPLICATION_AUTHORITY_BOUNDARY_DRIFT")
    root = Path(__file__).resolve().parents[1]
    pool = _bound_payload(root, plan["pool_review"], "review_payload_sha256", "BASE_EVENT_REPLICATION_POOL_REVIEW")
    if pool.get("status") != "ZERO_FINANCIAL_BASE_EVENT_REPLICATION_POOLS_INSUFFICIENT_UNDER_FULL_POOL_RESERVATION" or int(pool.get("effective_spent_exact_count") or 0) != 8654 or pool.get("candidate_evaluation_executed") is not False or pool.get("financial_labels_read") is not False:
        raise RuntimeError("BASE_EVENT_REPLICATION_POOL_REVIEW_CONTRACT_DRIFT")
    accepted = {str(x["replicate_id"]): dict(x) for x in pool["replicate_pools"]}
    if set(accepted) != {"REPL_1", "REPL_2"} or any(int(x["max4_capacity"]) < 120 for x in accepted.values()):
        raise RuntimeError("BASE_EVENT_REPLICATION_POOL_CAPACITY_DRIFT")
    raw_sets = [{str(x["exact_identity"]) for x in accepted[key]["entries"]} for key in ("REPL_1", "REPL_2")]
    norm_sets = [{str(x["normalized_exact_identity"]) for x in accepted[key]["entries"]} for key in ("REPL_1", "REPL_2")]
    if raw_sets[0] & raw_sets[1] or norm_sets[0] & norm_sets[1]:
        raise RuntimeError("BASE_EVENT_REPLICATION_POOL_DISJOINTNESS_DRIFT")
    review = _bound_payload(root, plan["study_review"], "review_payload_sha256", "BASE_EVENT_REPLICATION_STUDY_REVIEW")
    if review.get("status") != "BASE_EVENT_REPLICATION_STUDY_REVIEW_FROZEN" or review.get("new_candidate_evaluation_executed") is not False:
        raise RuntimeError("BASE_EVENT_REPLICATION_STUDY_REVIEW_DRIFT")
    anchor = _bound_payload(root, plan["source_v3_terminal_audit"], "audit_payload_sha256", "BASE_EVENT_REPLICATION_V3_AUDIT")
    outcome = _bound_payload(root, plan["source_v3_postrun_outcome"], "outcome_payload_sha256", "BASE_EVENT_REPLICATION_V3_OUTCOME")
    if anchor.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT" or outcome.get("post_batch_recommendation") != "REDIRECT" or outcome.get("redirect_target") != "BASE_EVENT_ONLY_SYSTEM_SCALE_DEVELOPMENT_SEARCH_REVIEW":
        raise RuntimeError("BASE_EVENT_REPLICATION_V3_LINEAGE_DRIFT")
    accel = _bound_payload(root, plan["acceleration_accuracy_audit"], "audit_payload_sha256", "BASE_EVENT_REPLICATION_ACCEL_AUDIT")
    if accel.get("status") != "PASS_SUCCESSOR_REAUTH_ELIGIBLE":
        raise RuntimeError("BASE_EVENT_REPLICATION_ACCEL_AUDIT_DRIFT")
    return plan


def _load_authority(args: argparse.Namespace, *, authorization: Mapping[str, Any], repo_sha: str) -> dict[str, Any]:
    return base._load_authority(args, authorization=authorization, repo_sha=repo_sha)


def _load_pools(plan: Mapping[str, Any], authority: Mapping[str, Any], repo_root: Path) -> dict[str, dict[str, Any]]:
    pool_review = _read(repo_root / Path(str(plan["pool_review"]["relative_path"])))
    selected = {"CONFIRM_A": "REPL_1", "CONFIRM_B": "REPL_2"}
    source = {str(x["replicate_id"]): dict(x) for x in pool_review["replicate_pools"]}
    result: dict[str, dict[str, Any]] = {}
    global_raw: set[str] = set()
    global_norm: set[str] = set()
    for rid in REPLICATES:
        pool_id = selected[rid]
        block = source[pool_id]
        catalog = {t: [] for t in large.ENHANCED_TEMPLATES}
        source_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for row in block["entries"]:
            reservoir = {
                "schema_version": "cn_joint_program_phase_c_reservoir_record_v0",
                "template_id": TEMPLATE,
                "components": dict(row["components"]),
                "combination_policy": dict(row["combination_policy"]),
                "raw_combination_sha256": str(row["raw_combination_sha256"]),
                "reservoir_record_sha256": str(row["reservoir_record_sha256"]),
                "semantic_compile_required_at_selection": True,
                "semantic_noop_does_not_count_toward_quota": True,
                "financial_evaluation_executed": False,
            }
            entry = engine._catalog_entry(reservoir, components_by_id=authority["components_by_id"], adapter=authority["adapter"], compiler=authority["compiler"])
            if entry.get("status") != "EXECUTABLE" or stable_hash(dict(entry["program_genes"])) != str(row["exact_identity"]):
                raise RuntimeError("BASE_EVENT_REPLICATION_POOL_REBUILD_DRIFT")
            catalog[TEMPLATE].append(entry)
            source_rows.append((entry, dict(row)))
        entries = tournament._program_entries(catalog)
        slots = tuple(entries[0].genes)
        catalog_by_exact: dict[str, Any] = {}
        physical_by_normalized: dict[str, str] = {}
        for entry, row in source_rows:
            norm = normalized_program_gene_identity_v1(dict(entry["program_genes"]), ordered_slots=slots)
            raw = stable_hash(dict(entry["program_genes"]))
            if norm != str(row["normalized_exact_identity"]) or norm in catalog_by_exact or raw != str(row["exact_identity"]):
                raise RuntimeError("BASE_EVENT_REPLICATION_POOL_IDENTITY_DRIFT")
            catalog_by_exact[norm] = entry
            physical_by_normalized[norm] = raw
        if set(catalog_by_exact) != {entry.exact_identity for entry in entries}:
            raise RuntimeError("BASE_EVENT_REPLICATION_POOL_NORMALIZED_COVERAGE_DRIFT")
        metadata = primitive_program_metadata_v1(entries=entries, catalog_by_exact=catalog_by_exact)
        raw_set = set(physical_by_normalized.values())
        norm_set = set(physical_by_normalized)
        if raw_set & global_raw or norm_set & global_norm:
            raise RuntimeError("BASE_EVENT_REPLICATION_CROSS_POOL_IDENTITY_DRIFT")
        global_raw.update(raw_set); global_norm.update(norm_set)
        result[rid] = {"pool_id": pool_id, "catalog": catalog, "entries": entries, "catalog_by_exact": catalog_by_exact, "physical_by_normalized": physical_by_normalized, "metadata": metadata, "fresh_unique_count": len(entries), "max4_capacity": int(block["max4_capacity"]), "fresh_exact_identities_sha256": str(block["fresh_exact_identities_sha256"])}
    return result


def _combined_fresh(pools: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    catalog = {t: [] for t in large.ENHANCED_TEMPLATES}
    for rid in REPLICATES:
        catalog[TEMPLATE].extend(list(pools[rid]["catalog"][TEMPLATE]))
    return {"catalog": catalog}


def _bandit(plan: Mapping[str, Any], fresh: Mapping[str, Any], repo_root: Path) -> LargeFreshProgramBanditV3:
    authority = dict(plan["search_authority"])
    stats_path = repo_root / Path(str(authority["primitive_stats_relative_path"]))
    stats = _read(stats_path)
    body = dict(stats); claim = str(body.pop("stats_payload_sha256", ""))
    if claim != str(authority["primitive_stats_payload_sha256"]) or stable_hash(body) != claim:
        raise RuntimeError("BASE_EVENT_REPLICATION_PRIMITIVE_STATS_DRIFT")
    entries = tuple(fresh["entries"])
    primitive_config = {"metadata_by_exact_identity": fresh["metadata"], "primitive_stats": stats, "primitive_stats_payload_sha256": claim}
    return LargeFreshProgramBanditV3(campaign_id=CAMPAIGN_ID, entries_by_arm={arm: entries for arm in ARMS}, seeds=SEEDS, primitive_config=primitive_config, evolution_config=EVOLUTION_CONFIG)


def _new_replicate_state(plan: Mapping[str, Any], fresh: Mapping[str, Any], repo_root: Path) -> tuple[LargeFreshProgramBanditV3, dict[str, Any]]:
    return _bandit(plan, fresh, repo_root), engine._selection_state()


def _ask_rows(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    out = []
    for local in range(CHECKPOINT_SIZE):
        ask = {
            "schema_version": "cn_program_base_event_replication_study_ask_v1",
            "checkpoint_ordinal": int(row["checkpoint"]),
            "replicate_id": str(row["replicate_id"]),
            "pool_id": str(row["pool_id"]),
            "optimizer_arm": str(row["arm"]),
            "generation_arm": str(row["arm"]),
            "template_id": TEMPLATE,
            "template_record_ordinal": int(row["template_start_ordinal"]) + local,
            "main_record_ordinal": int(row["start_ordinal"]) + local,
            "campaign_profile": CAMPAIGN_PROFILE,
            "allocation_role": str(row["role"]),
            "absolute_admission_head_eligible": True,
            "conditional_uplift_head_eligible": True,
            "matched_control_contract_id": "CANDIDATE_PROGRAM_V1_FULL_VS_BASE_MATCHED_CONTROL",
            "program_level_credit_only": True,
            "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
        }
        ask["ask_record_sha256"] = stable_hash(ask)
        out.append(ask)
    return out


def _productive_rows(feedback: Sequence[Mapping[str, Any]], schedules: Sequence[Mapping[str, Any]], replicate_id: str, pool_id: str) -> list[dict[str, Any]]:
    by = {int(s["main_record_ordinal"]): s for s in schedules}
    out = []
    for row in feedback:
        if not large._productive_feedback(row):
            continue
        schedule = by[int(row["main_record_ordinal"])]
        out.append({"main_record_ordinal": int(row["main_record_ordinal"]), "replicate_id": replicate_id, "pool_id": pool_id, "template_id": TEMPLATE, "generation_arm": str(row["generation_arm"]), "fresh_physical_exact_identity": str(schedule["fresh_physical_exact_identity"]), "normalized_search_exact_identity": str(schedule["optimizer_ask"]["exact_identity"]), "program_id": str(schedule["primary_program"]["program_id"]), "absolute_admission": dict(row["absolute_admission"]), "enhancer_credit": dict(row["enhancer_credit"])})
    return out


def _ratio(a: float, b: float) -> float:
    return a / b if b > 0 else (999.0 if a > 0 else 0.0)


def _gate(plan: Mapping[str, Any], feedback: Sequence[Mapping[str, Any]], records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    schedule = list(plan["schedule"])
    replicate_by_ordinal: dict[int, str] = {}
    for row in schedule:
        for ordinal in range(int(row["start_ordinal"]), int(row["start_ordinal"]) + 24):
            replicate_by_ordinal[ordinal] = str(row["replicate_id"])
    rows = [dict(x) for x in feedback]
    per_replicate: dict[str, Any] = {}
    directional_wins = 0
    for rid in REPLICATES:
        subset = [x for x in rows if replicate_by_ordinal[int(x["main_record_ordinal"])] == rid]
        p = large._metric_block([x for x in subset if str(x.get("generation_arm")) == P])
        u = large._metric_block([x for x in subset if str(x.get("generation_arm")) == U])
        e = large._metric_block([x for x in subset if str(x.get("generation_arm")) == E])
        ps = v3._stable_block([x for x in subset if str(x.get("generation_arm")) == P])
        us = v3._stable_block([x for x in subset if str(x.get("generation_arm")) == U])
        es = v3._stable_block([x for x in subset if str(x.get("generation_arm")) == E])
        best_prod = max(float(u["productive_efficiency"]), float(e["productive_efficiency"]))
        best_stable = max(float(us["uplift_stable_2of3_rate"]), float(es["uplift_stable_2of3_rate"]))
        win = float(p["productive_efficiency"]) > best_prod and float(ps["uplift_stable_2of3_rate"]) > best_stable
        directional_wins += int(win)
        per_replicate[rid] = {"primitive": p, "uniform": u, "typed_evolution": e, "primitive_stable": ps, "uniform_stable": us, "typed_evolution_stable": es, "best_control_productive_rate": best_prod, "best_control_stable_rate": best_stable, "directional_policy_win": win}
    p_rows = [x for x in rows if str(x.get("generation_arm")) == P]
    u_rows = [x for x in rows if str(x.get("generation_arm")) == U]
    e_rows = [x for x in rows if str(x.get("generation_arm")) == E]
    pm, um, em = large._metric_block(p_rows), large._metric_block(u_rows), large._metric_block(e_rows)
    ps, us, es = v3._stable_block(p_rows), v3._stable_block(u_rows), v3._stable_block(e_rows)
    p_prod = float(pm["productive_efficiency"]); p_stable = float(ps["uplift_stable_2of3_rate"])
    best_prod = max(float(um["productive_efficiency"]), float(em["productive_efficiency"]))
    best_stable = max(float(us["uplift_stable_2of3_rate"]), float(es["uplift_stable_2of3_rate"]))
    behavior_pairs = v3._behavior_count(records); behavior_rate = behavior_pairs / len(records) if records else 0.0
    gate = dict(plan["prospective_gate"])
    checks = {
        "aggregate_primitive_productive_rate": p_prod >= float(gate["aggregate_primitive_productive_rate_min"]),
        "aggregate_primitive_uplift_stable_2of3_rate": p_stable >= float(gate["aggregate_primitive_uplift_stable_2of3_rate_min"]),
        "aggregate_primitive_to_best_control_productive_ratio": _ratio(p_prod, best_prod) >= float(gate["aggregate_primitive_to_best_control_productive_ratio_min"]),
        "aggregate_primitive_minus_best_control_productive_rate": p_prod - best_prod >= float(gate["aggregate_primitive_minus_best_control_productive_rate_min"]),
        "aggregate_primitive_to_best_control_uplift_stable_ratio": _ratio(p_stable, best_stable) >= float(gate["aggregate_primitive_to_best_control_uplift_stable_ratio_min"]),
        "aggregate_primitive_minus_best_control_uplift_stable_rate": p_stable - best_stable >= float(gate["aggregate_primitive_minus_best_control_uplift_stable_rate_min"]),
        "each_replicate_primitive_productive_rate": all(float(per_replicate[rid]["primitive"]["productive_efficiency"]) >= float(gate["each_replicate_primitive_productive_rate_min"]) for rid in REPLICATES),
        "each_replicate_primitive_uplift_stable": all(float(per_replicate[rid]["primitive_stable"]["uplift_stable_2of3_rate"]) >= float(gate["each_replicate_primitive_uplift_stable_2of3_rate_min"]) for rid in REPLICATES),
        "replicate_directional_policy_win_count": directional_wins >= int(gate["replicate_directional_policy_win_count_min"]),
        "behavior_pair_rate": behavior_rate >= float(gate["behavior_pair_rate_min"]),
    }
    return {"status": PASS_STATUS if all(checks.values()) else FAIL_STATUS, "checks": checks, "aggregate": {"primitive": pm, "uniform": um, "typed_evolution": em, "primitive_stable": ps, "uniform_stable": us, "typed_evolution_stable": es, "best_control_productive_rate": best_prod, "best_control_stable_rate": best_stable, "productive_ratio_vs_best_control": _ratio(p_prod, best_prod), "productive_delta_vs_best_control": p_prod - best_prod, "stable_ratio_vs_best_control": _ratio(p_stable, best_stable), "stable_delta_vs_best_control": p_stable - best_stable}, "per_replicate": per_replicate, "replicate_directional_policy_win_count": directional_wins, "behavior_pair_count": behavior_pairs, "behavior_pair_rate": behavior_rate, "total": large._metric_block(rows), "anchor_v3_used_in_prospective_gate": False}


def prefinancial_rehearsal(args: argparse.Namespace, *, authorization: Mapping[str, Any], repo_sha: str) -> dict[str, Any]:
    plan = verify_plan(args.replication_plan)
    root = Path(__file__).resolve().parents[1]
    authority = _load_authority(args, authorization=authorization, repo_sha=repo_sha)
    pools = _load_pools(plan, authority, root)
    fields = base._full_field_union(_combined_fresh(pools), authority)
    selected_raw: set[str] = set(); selected_norm: set[str] = set(); counts = Counter()
    current = None; bandit = None; state = None
    for row in plan["schedule"]:
        rid = str(row["replicate_id"])
        if rid != current:
            bandit, state = _new_replicate_state(plan, pools[rid], root)
            current = rid
        assert bandit is not None and state is not None
        asks = _ask_rows(row)
        schedules, _ = tournament._select_checkpoint(asks, catalog=pools[rid]["catalog"], bandit=bandit, state=state, components_by_id=authority["components_by_id"], adapter=authority["adapter"], compiler=authority["compiler"], prior_exact_identities=())
        norms = [str(s["optimizer_ask"]["exact_identity"]) for s in schedules]
        raws = [str(pools[rid]["physical_by_normalized"][x]) for x in norms]
        if len(schedules) != 24 or len(set(norms)) != 24 or len(set(raws)) != 24 or set(norms) & selected_norm or set(raws) & selected_raw:
            raise RuntimeError("BASE_EVENT_REPLICATION_PREFLIGHT_SELECTION_DRIFT")
        selected_norm.update(norms); selected_raw.update(raws); counts[rid] += 24
        expected = [dict(s["optimizer_ask"]) for s in schedules]
        bandit.commit_ask(arm=str(row["arm"]), checkpoint_id=str(schedules[0]["optimizer_ask"]["checkpoint_id"]), count=len(schedules), required_program_template_id=TEMPLATE, eligible_exact_identities=list(schedules[0]["optimizer_eligible_exact_identities"]), batch_group_constraint=dict(schedules[0]["optimizer_batch_group_constraint"]), expected_asks=expected)
        bandit.discard_nonlearning_pending(arm=str(row["arm"]), expected_asks=expected)
    if counts != Counter({"CONFIRM_A": 120, "CONFIRM_B": 120}) or len(selected_raw) != 240 or len(selected_norm) != 240:
        raise RuntimeError("BASE_EVENT_REPLICATION_PREFLIGHT_CARDINALITY_DRIFT")
    return {"status": "ZERO_FINANCIAL_BASE_EVENT_REPLICATION_STUDY_PREFLIGHT_READY", "preview_selected_count": 240, "preview_unique_normalized_count": 240, "per_replicate_selected": dict(counts), "field_column_count": len(fields), "field_columns": list(fields), "field_columns_sha256": stable_hash(list(fields)), "candidate_evaluation_executed": False, "anchor_v3_used_in_prospective_gate": False, "production_feedback_imported_into_primitive_stats": False, "v1_successor_feedback_imported_into_primitive_stats": False, "v2_successor_feedback_imported_into_primitive_stats": False, "v3_successor_feedback_imported_into_primitive_stats": False, "diversity_contract_changed": False, "validation_reads": 0, "holdout_reads": 0, "historical_2023_reads": 0, "forward_b_reads": 0, "forward_2026_reads": 0}


def run(args: argparse.Namespace, *, admission: Mapping[str, Any], authorization: Mapping[str, Any]) -> dict[str, Any]:
    repo_sha = str(admission["repo_sha"])
    repo_root = Path(__file__).resolve().parents[1]
    plan = verify_plan(args.replication_plan)
    authority = _load_authority(args, authorization=authorization, repo_sha=repo_sha)
    pools = _load_pools(plan, authority, repo_root)
    root = args.output_root.resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir() or {p.name for p in root.iterdir()} != {".project_control_execution"}:
        raise RuntimeError("BASE_EVENT_REPLICATION_ADMITTED_ROOT_NOT_CLEAN")
    fields = base._full_field_union(_combined_fresh(pools), authority)
    input_binding = engine._self_hashed({"schema_version": "cn_program_base_event_replication_study_input_binding_v1", "repo_sha": repo_sha, "authorization_payload_sha256": authorization["authorization_payload_sha256"], "plan_payload_sha256": plan["plan_payload_sha256"], "pool_review_payload_sha256": plan["pool_review"]["payload_sha256"], "study_review_payload_sha256": plan["study_review"]["payload_sha256"], "source_v3_terminal_audit_payload_sha256": plan["source_v3_terminal_audit"]["payload_sha256"], "source_v3_postrun_outcome_payload_sha256": plan["source_v3_postrun_outcome"]["payload_sha256"], "effective_spent_exact_count": 8654, "pool_exact_identity_hashes": {rid: pools[rid]["fresh_exact_identities_sha256"] for rid in REPLICATES}, "primitive_stats_payload_sha256": plan["search_authority"]["primitive_stats_payload_sha256"], "v3_anchor_used_in_prospective_gate": False, "field_columns_sha256": stable_hash(list(fields)), "evaluation_data_role": "DEVELOPMENT_ONLY", "evaluator_pool_lifetime": "PERSISTENT_RUN_SCOPE", "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0}}, "input_binding_sha256")
    large._write_json(root / "input_binding.json", input_binding)
    input_hash = str(input_binding["input_binding_sha256"])
    workers = None; canary_decision: dict[str, Any] | None = None
    for worker_count in (24, 16):
        try:
            receipt = base._resource_canary(authority, input_hash, worker_count, fields)
            workers = worker_count
            canary_decision = {"status": "PASS", "selected_executor_workers": worker_count, "attempts": [receipt], "fallback_applied": worker_count != 24, "field_columns": list(fields), "field_columns_sha256": stable_hash(list(fields))}
            break
        except Exception as exc:
            canary_decision = {"status": "FAIL", "error": f"{type(exc).__name__}:{exc}"}
    if workers is None:
        raise RuntimeError(f"BASE_EVENT_REPLICATION_RESOURCE_CANARY_FAILED:{canary_decision}")
    large._write_json(root / "resource_canary.json", canary_decision)
    started = time.perf_counter(); previous = "GENESIS"; current = None; bandit = None; state = None
    selected_raw: set[str] = set(); selected_norm: set[str] = set(); all_feedback: list[dict[str, Any]] = []; all_records: list[dict[str, Any]] = []; productive: list[dict[str, Any]] = []; evaluator_telemetry: list[dict[str, Any]] = []; replicate_counts = Counter(); final_states: dict[str, str] = {}
    before_children = {child.pid for child in psutil.Process().children(recursive=True)}
    rc = dict(plan["resource_contract"]); throughput_min = float(rc["minimum_records_per_hour_after_first_checkpoint"]); wall_budget = float(rc["wall_clock_budget_minutes"]) * 60.0; warm_enforce = int(rc["throughput_enforcement_after_warm_checkpoints"])
    executor_options = v3._persistent_executor_options(authority, input_hash, workers, fields)
    with ProcessPoolExecutor(**executor_options) as evaluator:
        for row in plan["schedule"]:
            cp = int(row["checkpoint"]); rid = str(row["replicate_id"]); pool_id = str(row["pool_id"]); arm = str(row["arm"])
            if rid != current:
                if current is not None and replicate_counts[current] != 120:
                    raise RuntimeError("BASE_EVENT_REPLICATION_BOUNDARY_CARDINALITY_DRIFT")
                bandit, state = _new_replicate_state(plan, pools[rid], repo_root)
                current = rid
                large._write_json(root / f"replicate_{rid}_optimizer_state_genesis.json", bandit.snapshot())
                large._write_json(root / f"replicate_{rid}_selection_state_genesis.json", large._selection_state_record(state))
            assert bandit is not None and state is not None
            inflight = root / f"checkpoint_{cp:04d}.inflight"; closed = root / f"checkpoint_{cp:04d}"; inflight.mkdir(parents=False, exist_ok=False)
            binding = engine._self_hashed({"schema_version": "cn_program_base_event_replication_checkpoint_binding_v1", "checkpoint": cp, "replicate_id": rid, "pool_id": pool_id, "replicate_checkpoint": int(row["replicate_checkpoint"]), "arm": arm, "template": TEMPLATE, "anchor_v3_used_in_prospective_gate": False}, "replicate_binding_payload_sha256")
            large._write_json(inflight / "replicate_binding.json", binding)
            asks = _ask_rows(row); large._write_jsonl(inflight / "logical_asks.jsonl", asks); large._write_json(inflight / "optimizer_state_before.json", bandit.snapshot()); large._write_json(inflight / "selection_state_before.json", large._selection_state_record(state))
            schedules, decisions = tournament._select_checkpoint(asks, catalog=pools[rid]["catalog"], bandit=bandit, state=state, components_by_id=authority["components_by_id"], adapter=authority["adapter"], compiler=authority["compiler"], prior_exact_identities=())
            norms = [str(s["optimizer_ask"]["exact_identity"]) for s in schedules]; raws = [str(pools[rid]["physical_by_normalized"][x]) for x in norms]
            if len(schedules) != 24 or len(set(norms)) != 24 or len(set(raws)) != 24 or set(norms) & selected_norm or set(raws) & selected_raw:
                raise RuntimeError("BASE_EVENT_REPLICATION_FRESH_SELECTION_DRIFT")
            selected_norm.update(norms); selected_raw.update(raws); replicate_counts[rid] += 24
            for schedule, raw in zip(schedules, raws, strict=True):
                schedule["fresh_physical_exact_identity"] = raw; schedule["allocation_role"] = str(row["role"]); schedule["replicate_id"] = rid; schedule["pool_id"] = pool_id; schedule["schedule_record_sha256"] = stable_hash({k: v for k, v in schedule.items() if k != "schedule_record_sha256"})
            large._write_jsonl(inflight / "selected_schedule.jsonl", schedules); large._write_jsonl(inflight / "selection_ledger.jsonl", decisions)
            records, telemetry = v3._evaluate_schedules_persistent(schedules, record_root=inflight / "records", executor=evaluator, input_hash=input_hash, checkpoint=cp)
            telemetry = dict(telemetry); telemetry["schema_version"] = "cn_program_base_event_replication_study_checkpoint_evaluator_telemetry_v1"; telemetry["replicate_id"] = rid; telemetry["pool_id"] = pool_id
            evaluator_telemetry.append(telemetry); large._write_json(root / "evaluator_telemetry.json", {"schema_version": "cn_program_base_event_replication_study_evaluator_telemetry_v1", "executor_lifetime": "PERSISTENT_RUN_SCOPE", "selected_executor_workers": workers, "field_column_count": len(fields), "checkpoints": evaluator_telemetry})
            for record in records:
                if any(int(record.get(key) or 0) != 0 for key in ("validation_reads", "holdout_reads", "historical_2023_reads", "forward_b_reads", "forward_2026_reads")):
                    raise RuntimeError("BASE_EVENT_REPLICATION_RESTRICTED_READ_DRIFT")
            feedback = tournament._feedback_update(records, schedules, bandit=bandit, behavior_counts=Counter())
            large._write_jsonl(inflight / "feedback.jsonl", feedback); large._write_json(inflight / "optimizer_state_after.json", bandit.snapshot()); large._write_json(inflight / "selection_state_after.json", large._selection_state_record(state)); previous = large._close_checkpoint(inflight=inflight, closed=closed, previous_manifest_sha256=previous, macro_index=0 if rid == "CONFIRM_A" else 1, template_id=TEMPLATE, optimizer_arm=arm)
            final_states[rid] = str(bandit.snapshot()["bandit_state_sha256"]); productive.extend(_productive_rows(feedback, schedules, rid, pool_id)); all_feedback.extend(feedback); all_records.extend(records)
            warm = evaluator_telemetry[1:]; warm_records = sum(int(x["records"]) for x in warm); warm_wall = sum(float(x["wall_seconds"]) for x in warm); warm_rate = warm_records / max(warm_wall / 3600.0, 1e-12) if warm else None; elapsed = float(time.perf_counter() - started); remaining = 240 - len(all_feedback); remaining_cp = max(0, 10 - (cp + 1)); projected = elapsed + remaining / max((warm_rate or 1.0) / 3600.0, 1e-12) + remaining_cp * 7.5 if warm_rate else None; throughput_status = "WARMUP" if len(warm) < warm_enforce else ("PASS" if warm_rate >= throughput_min and projected <= wall_budget else "FAIL")
            large._write_json(root / "progress_metrics.json", {"closed_checkpoints": cp + 1, "evaluated": len(all_feedback), "productive": large._metric_block(all_feedback)["productive"], "uplift_stable_2of3": sum(v3._uplift_stable_2of3(x) for x in all_feedback), "behavior_pair_count": v3._behavior_count(all_records), "last_replicate_id": rid, "last_checkpoint_wall_seconds": telemetry["wall_seconds"], "warm_records_per_hour": warm_rate, "projected_total_wall_seconds": projected, "throughput_contract_status": throughput_status})
            if throughput_status == "FAIL":
                raise RuntimeError(f"BASE_EVENT_REPLICATION_THROUGHPUT_CONTRACT_FAILED:rate={warm_rate}:projected={projected}:budget={wall_budget}")
    gc.collect(); engine._require_runtime_resource_safety(engine._runtime_resource_snapshot()); orphans = engine._new_child_process_ids(before_children)
    if orphans:
        raise RuntimeError("BASE_EVENT_REPLICATION_PERSISTENT_POOL_LEFT_ORPHANS:" + ",".join(map(str, orphans)))
    if len(all_feedback) != 240 or len(selected_raw) != 240 or len(selected_norm) != 240 or replicate_counts != Counter({"CONFIRM_A": 120, "CONFIRM_B": 120}):
        raise RuntimeError("BASE_EVENT_REPLICATION_TERMINAL_CARDINALITY_DRIFT")
    large._write_jsonl(root / "productive_discoveries.jsonl", productive)
    gate = _gate(plan, all_feedback, all_records); mean_cpu = sum(float(x["host_cpu_mean_percent"]) for x in evaluator_telemetry) / len(evaluator_telemetry); mean_wall = sum(float(x["wall_seconds"]) for x in evaluator_telemetry) / len(evaluator_telemetry); warm = evaluator_telemetry[1:]; warm_records = sum(int(x["records"]) for x in warm); warm_wall = sum(float(x["wall_seconds"]) for x in warm); warm_rate = warm_records / max(warm_wall / 3600.0, 1e-12)
    closure = engine._self_hashed({"schema_version": "cn_program_base_event_replication_study_complete_v1", "status": STATUS_COMPLETE, "prospective_gate_status": gate["status"], "campaign_id": CAMPAIGN_ID, "campaign_profile": CAMPAIGN_PROFILE, "repo_sha": repo_sha, "authorization_payload_sha256": authorization["authorization_payload_sha256"], "plan_payload_sha256": plan["plan_payload_sha256"], "pool_review_payload_sha256": plan["pool_review"]["payload_sha256"], "study_review_payload_sha256": plan["study_review"]["payload_sha256"], "source_v3_terminal_audit_payload_sha256": plan["source_v3_terminal_audit"]["payload_sha256"], "source_v3_postrun_outcome_payload_sha256": plan["source_v3_postrun_outcome"]["payload_sha256"], "anchor_v3_used_in_prospective_gate": False, "input_binding_sha256": input_hash, "logical_records": 240, "unique_normalized_exact_count": 240, "unique_fresh_physical_exact_count": 240, "effective_spent_exact_count_before_study": 8654, "effective_spent_overlap_count": 0, "closed_checkpoints": 10, "per_replicate_records": dict(replicate_counts), "productive_discovery_count": len(productive), "total_metrics": large._metric_block(all_feedback), "prospective_gate": gate, "replicate_final_state_sha256": final_states, "behavior_pair_count": v3._behavior_count(all_records), "uplift_stable_2of3_count": sum(v3._uplift_stable_2of3(x) for x in all_feedback), "wall_seconds": time.perf_counter() - started, "evaluator_acceleration": {"executor_lifetime": "PERSISTENT_RUN_SCOPE", "selected_executor_workers": workers, "field_column_count": len(fields), "mean_checkpoint_wall_seconds": mean_wall, "mean_host_cpu_percent": mean_cpu, "checkpoint_telemetry_count": len(evaluator_telemetry), "warm_records_per_hour": warm_rate, "throughput_contract_status": "PASS", "wall_clock_budget_seconds": wall_budget}, "production_feedback_imported_into_primitive_stats": False, "v1_successor_feedback_imported_into_primitive_stats": False, "v2_successor_feedback_imported_into_primitive_stats": False, "v3_successor_feedback_imported_into_primitive_stats": False, "diversity_contract_changed": False, "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0}, "validation_feedback_used": False, "oos_authority": "NONE", "promotion_authorized": False, "automatic_successor_authorized": False}, "closure_payload_sha256")
    large._write_json(root / CLOSURE_NAME, closure)
    return closure


__all__ = ["verify_plan", "prefinancial_rehearsal", "run", "_gate", "_load_pools", "_new_replicate_state"]
