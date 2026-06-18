"""Apply the opt-in Phase3AA G2 selector to an enriched shared pool."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from our_system_phase2.domain.models import utc_now_iso
from our_system_phase2.services.artifact_schema import write_json_artifact
from our_system_phase2.services.phase3e_selectors import (
    Phase3ERegistryContext,
    select_phase3e_queue,
    strip_forbidden_replay_label_rows,
    write_selector_artifacts,
)
from our_system_phase2.services.phase3g_signal_vector_store import Phase3GSignalVectorStore
from our_system_phase2.services.phase3g_signal_vector_store import DEFAULT_PHASE3G_VECTOR_METADATA
from our_system_phase2.services.phase3g_signal_vector_store import DEFAULT_PHASE3G_VECTOR_NPZ
from our_system_phase2.services.stock_pit_phase3_repair import PHASE3H_CUMULATIVE_BASELINE_PATH
from our_system_phase2.services.typed_primitive_gate import gate_g2_input_rows
from our_system_phase2.runtime.phase3aa_enrich_shared_candidate_pool import (
    PHASE3AA_ABLATION_ARM,
    PHASE3AA_EVENT_BUCKET,
    PHASE3AA_FUNDAMENTAL_BUCKET,
    PHASE3AA_RESEARCH_BUCKET,
)


PHASE3AA_SELECTOR_PROFILE = "signal_vector_diversified_source_priority_proxy"
PHASE3AA_APPLY_VERSION = "phase3aa-mature-g2-source-priority-selector-v1-2026-05-29"


def _normalize_budget(budgets: dict[str, int], total: int) -> dict[str, int]:
    total = max(1, int(total))
    out = {key: max(0, int(value)) for key, value in budgets.items()}
    overflow = sum(out.values()) - total
    reduce_order = [
        "replay_aware_residual",
        "formula_gen_v2_repair_expansion",
        "agnostic_freeform_ast",
        "ast_failure_aware_repair",
        "r0_cem_led",
        PHASE3AA_RESEARCH_BUCKET,
        PHASE3AA_FUNDAMENTAL_BUCKET,
        PHASE3AA_EVENT_BUCKET,
    ]
    for key in reduce_order:
        if overflow <= 0:
            break
        take = min(out.get(key, 0), overflow)
        out[key] = out.get(key, 0) - take
        overflow -= take
    deficit = total - sum(out.values())
    if deficit > 0:
        out["r0_cem_led"] = out.get("r0_cem_led", 0) + deficit
    return out


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _budget(total: int, event_share: float, research_share: float, fundamental_share: float = 0.0) -> dict[str, int]:
    total = max(1, int(total))
    event = int(round(total * max(0.0, min(0.60, float(event_share)))))
    research = int(round(total * max(0.0, min(0.35, float(research_share)))))
    fundamental = int(round(total * max(0.0, min(0.25, float(fundamental_share)))))
    if event + research + fundamental > total:
        overflow = event + research + fundamental - total
        research_reduction = min(research, overflow)
        research = max(0, research - research_reduction)
        overflow -= research_reduction
        fundamental = max(0, fundamental - overflow)
    remaining = max(0, total - event - research - fundamental)
    r0 = int(round(remaining * 0.43))
    repair = int(round(remaining * 0.21))
    agnostic = int(round(remaining * 0.20))
    repair_expansion = int(round(remaining * 0.13))
    residual = max(0, remaining - r0 - repair - agnostic - repair_expansion)
    return _normalize_budget(
        {
        "r0_cem_led": r0,
        "ast_failure_aware_repair": repair,
        "replay_aware_residual": residual,
        "novelty_diagnostic": 0,
        "formula_gen_v2_defined": 0,
        "agnostic_freeform_ast": agnostic,
        "formula_gen_v2_repair_expansion": repair_expansion,
        PHASE3AA_EVENT_BUCKET: event,
        PHASE3AA_FUNDAMENTAL_BUCKET: fundamental,
        PHASE3AA_RESEARCH_BUCKET: research,
        },
        total,
    )


def _source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        source = str(row.get("source_lane") or row.get("source_generator") or row.get("feature_adapter") or "unknown")
        counts[source] = counts.get(source, 0) + 1
    return dict(sorted(counts.items()))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if out == out else default


def _prefilter_pool(rows: list[dict[str, Any]], *, pool_cap: int) -> list[dict[str, Any]]:
    if pool_cap <= 0 or len(rows) <= pool_cap:
        return rows
    event_rows = [row for row in rows if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_EVENT_BUCKET]
    fundamental_rows = [row for row in rows if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_FUNDAMENTAL_BUCKET]
    research_rows = [row for row in rows if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_RESEARCH_BUCKET]
    other_rows = [
        row
        for row in rows
        if str(row.get("phase3_budget_bucket") or "") not in {PHASE3AA_EVENT_BUCKET, PHASE3AA_FUNDAMENTAL_BUCKET, PHASE3AA_RESEARCH_BUCKET}
    ]
    event_cap = min(len(event_rows), max(16, int(round(pool_cap * 0.30))))
    fundamental_cap = min(len(fundamental_rows), max(8, int(round(pool_cap * 0.12))))
    research_cap = min(len(research_rows), max(12, int(round(pool_cap * 0.18))))
    other_cap = max(0, pool_cap - event_cap - fundamental_cap - research_cap)

    def key(row: dict[str, Any]) -> tuple[float, float, str]:
        priority = _safe_float(row.get("pool_priority_score"), 1.0)
        quality = _safe_float(row.get("cost_adjusted_proxy"), _safe_float(row.get("fast_reward"), 0.0))
        return (priority, quality, str(row.get("candidate_id") or row.get("expression") or ""))

    event_selected = sorted(event_rows, key=key, reverse=True)[:event_cap]
    fundamental_selected = sorted(fundamental_rows, key=key, reverse=True)[:fundamental_cap]
    research_selected = sorted(research_rows, key=key, reverse=True)[:research_cap]
    other_selected = sorted(other_rows, key=key, reverse=True)[:other_cap]
    return event_selected + fundamental_selected + research_selected + other_selected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--dataset-path", type=Path, default=None)
    parser.add_argument("--signal-sample-size", type=int, default=5000)
    parser.add_argument("--signal-warmup-days", type=int, default=90)
    parser.add_argument("--signal-recent-quarter-window-count", type=int, default=1)
    parser.add_argument("--signal-vector-npz", type=Path, default=DEFAULT_PHASE3G_VECTOR_NPZ)
    parser.add_argument("--signal-vector-metadata", type=Path, default=DEFAULT_PHASE3G_VECTOR_METADATA)
    parser.add_argument("--signal-runtime-cache-dir", type=Path, default=Path("runtime/phase3g_signal_vectors/runtime_eval_cache"))
    parser.add_argument("--total-budget", type=int, default=64)
    parser.add_argument("--event-share", type=float, default=0.25)
    parser.add_argument("--research-share", type=float, default=0.16)
    parser.add_argument("--fundamental-share", type=float, default=0.0)
    parser.add_argument("--pool-cap", type=int, default=160)
    parser.add_argument("--seed", default=None)
    args = parser.parse_args()

    pool = _read_json(args.pool)
    dataset_path = str(args.dataset_path or pool.get("dataset_path") or "")
    output_root = args.output_root / "aa"
    output_root.mkdir(parents=True, exist_ok=True)
    total_budget = max(1, int(args.total_budget or pool.get("strict_audit_budget") or 64))
    budgets = _budget(total_budget, args.event_share, args.research_share, args.fundamental_share)

    candidate_pool_raw = strip_forbidden_replay_label_rows(list(pool.get("candidate_pool") or []))
    g2_allowed_pool, g2_rejected_pool = gate_g2_input_rows(
        candidate_pool_raw,
        entry_lineage="phase3aa_apply_mature_g2_selector",
        materialization_stage="g2_selector_input",
        candidate_role="mature_g2_candidate",
    )
    _write_csv(output_root / "phase3ce1_g2_input_gate_rejects.csv", g2_rejected_pool)
    write_json_artifact(
        output_root / "phase3ce1_g2_input_gate_summary.json",
        {
            "input_candidate_pool_count": len(candidate_pool_raw),
            "g2_input_allowed_count": len(g2_allowed_pool),
            "g2_input_rejected_count": len(g2_rejected_pool),
            "rejected_decision_counts": _source_counts(
                [{"source_lane": row.get("typed_gate_decision") or "unknown"} for row in g2_rejected_pool]
            ),
            "rejected_reason_counts": _source_counts(
                [{"source_lane": row.get("typed_gate_reason") or "unknown"} for row in g2_rejected_pool]
            ),
        },
    )
    candidate_pool = _prefilter_pool(g2_allowed_pool, pool_cap=int(args.pool_cap))
    default_selected = strip_forbidden_replay_label_rows(list(pool.get("default_selected") or []))
    context = Phase3ERegistryContext.from_path(PHASE3H_CUMULATIVE_BASELINE_PATH)
    signal_store = Phase3GSignalVectorStore(
        vector_npz=args.signal_vector_npz,
        metadata_path=args.signal_vector_metadata,
        dataset_path=dataset_path,
        sample_size=max(1, int(args.signal_sample_size)),
        recent_warmup_days=max(1, int(args.signal_warmup_days)),
        recent_quarter_window_count=max(1, int(args.signal_recent_quarter_window_count)),
        runtime_cache_dir=args.signal_runtime_cache_dir,
    )
    selected, audit_rows, preflight = select_phase3e_queue(
        candidate_pool,
        budgets=budgets,
        selector_profile=PHASE3AA_SELECTOR_PROFILE,
        context=context,
        seed=str(args.seed or pool.get("seed") or "phase3aa"),
        default_selected=default_selected,
        total_budget=total_budget,
        signal_vector_store=signal_store,
    )
    write_selector_artifacts(output_root, audit_rows=audit_rows, preflight=preflight, selector_profile=PHASE3AA_SELECTOR_PROFILE)
    design = {
        "description": "Phase3AA mature-chain event-derived factor search: shared pool + event factor injection + source-priority G2 selector.",
        "phase3e_generation_profile": "G2_phase3i_primary_plus_event_derived_feature_layer",
        "phase3e_selector_profile": PHASE3AA_SELECTOR_PROFILE,
        "phase3e_cumulative_baseline_path": str(PHASE3H_CUMULATIVE_BASELINE_PATH),
        "phase3_metadata_policy": "DUAL_BASELINE_ACCEPTED",
        "phase3_discovery_baseline_count": 149,
        "phase3_selector_vector_baseline_count": 137,
            "event_bucket": PHASE3AA_EVENT_BUCKET,
            "research_bucket": PHASE3AA_RESEARCH_BUCKET,
            "source_priority": "opt_in_selector_bonus_and_audit_fields",
        "shared_candidate_pool_source": pool.get("source_ablation_arm"),
        "pool_enrichment": pool.get("phase3aa_enrichment") or {},
    }
    write_json_artifact(
        output_root / "phase3_strict_selection_inputs.json",
        {
            "selected": selected,
            "budgets": budgets,
            "ablation_arm": PHASE3AA_ABLATION_ARM,
            "ablation_design": design,
            "phase3e_selector_audit_count": len(audit_rows),
            "phase3e_selector_preflight": preflight,
        },
    )
    report = {
        "phase3_version": PHASE3AA_APPLY_VERSION,
        "created_at": utc_now_iso(),
        "experiment_id": f"phase3aa_mature_g2_event_selector_{pool.get('seed') or args.seed or 'seed'}",
        "status": "selection_only",
        "dataset_path": dataset_path,
        "dataset_role": pool.get("dataset_role"),
        "output_root": str(output_root),
        "ablation_arm": PHASE3AA_ABLATION_ARM,
        "ablation_design": design,
        "parameters": {
            "candidate_pool_count": len(candidate_pool),
            "candidate_pool_count_before_prefilter": len(g2_allowed_pool),
            "candidate_pool_count_before_g2_input_gate": len(candidate_pool_raw),
            "g2_input_gate_rejected_count": len(g2_rejected_pool),
            "pool_cap": int(args.pool_cap),
            "default_selected_count": len(default_selected),
            "selected_count": len(selected),
            "strict_audit_budget": total_budget,
            "budgets": budgets,
            "selector_baseline_path": str(PHASE3H_CUMULATIVE_BASELINE_PATH),
            "signal_sample_size": int(args.signal_sample_size),
            "signal_warmup_days": int(args.signal_warmup_days),
            "signal_recent_quarter_window_count": int(args.signal_recent_quarter_window_count),
            "signal_vector_npz": str(args.signal_vector_npz),
            "signal_vector_metadata": str(args.signal_vector_metadata),
            "research_share": float(args.research_share),
            "fundamental_share": float(args.fundamental_share),
            "signal_runtime_cache_dir": str(args.signal_runtime_cache_dir),
        },
        "selector_checks": {
            "candidate_source_counts": _source_counts(candidate_pool),
            "selected_source_counts": _source_counts(selected),
            "event_candidates_in_pool": sum(1 for row in candidate_pool if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_EVENT_BUCKET),
            "event_candidates_selected": sum(1 for row in selected if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_EVENT_BUCKET),
            "fundamental_candidates_in_pool": sum(1 for row in candidate_pool if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_FUNDAMENTAL_BUCKET),
            "fundamental_candidates_selected": sum(1 for row in selected if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_FUNDAMENTAL_BUCKET),
            "research_candidates_in_pool": sum(1 for row in candidate_pool if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_RESEARCH_BUCKET),
            "research_candidates_selected": sum(1 for row in selected if str(row.get("phase3_budget_bucket") or "") == PHASE3AA_RESEARCH_BUCKET),
            "forbidden_label_guard": preflight.get("replay_label_leakage_guard"),
            "signal_vector_proxy_requirement_pass": preflight.get("signal_vector_proxy_requirement_pass"),
        },
    }
    write_json_artifact(output_root / "phase3_selection_only_report.json", report)
    print(json.dumps(report["selector_checks"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
