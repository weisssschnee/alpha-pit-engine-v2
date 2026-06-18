"""Inject event-derived factor candidates into a mature shared candidate pool.

This is the Phase3AA bridge that was missing from the first launcher:

    mature shared pool -> event-derived factor candidates -> G2 selector

It does not replace X0/R3 or promote event diagnostics. It only makes the
event-derived feature layer visible to the frozen-selection path.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from our_system_phase2.domain.models import utc_now_iso
from our_system_phase2.services.artifact_schema import write_json_artifact
from our_system_phase2.services.candidate_pool_priority import enrich_candidate_pool_priority
from our_system_phase2.services.search_memory import LocalSearchMemory, expression_memory_key
from our_system_phase2.runtime.phase3r_limit_motif_pack_diagnostic import _candidate_rows as limit_candidate_rows
from our_system_phase2.runtime.phase3z46_eventalpha_canary import _generate_candidates as z46_candidate_rows


PHASE3AA_EVENT_BUCKET = "event_derived_feature_layer"
PHASE3AA_FUNDAMENTAL_BUCKET = "fundamental_pit_feature_layer"
PHASE3AA_RESEARCH_BUCKET = "research_factor_feature_layer"
PHASE3AA_ABLATION_ARM = "Phase3AA_G2_event_source_priority"
PHASE3AA_ENRICH_VERSION = "phase3aa-shared-pool-event-injection-v1-2026-05-29"
DEFAULT_CN_FACTOR_PACK = Path("runtime/factor_packs/cn_event_factor_candidate_pack_v1_20260531.json")
FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
FORBIDDEN_FORMULA_FIELD_RE = re.compile(r"^(?:label_|next_|future_|forward_return|meta_)", re.IGNORECASE)


def _formula_fields(expression: str) -> list[str]:
    return sorted(set(FIELD_RE.findall(expression or "")))


def _unsafe_formula_fields(expression: str) -> list[str]:
    fields = _formula_fields(expression)
    return [field for field in fields if FORBIDDEN_FORMULA_FIELD_RE.match(field) or field.lower().endswith("_source")]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _copy_event_row(row: dict[str, Any], *, index: int) -> dict[str, Any]:
    item = enrich_candidate_pool_priority(dict(row))
    item["candidate_id"] = f"phase3aa_event_{index:04d}_{item.get('candidate_id') or 'candidate'}"
    if item.get("contains_fundamental_field") and not item.get("contains_new_event_adapter_field") and not item.get("contains_flow_liquidity_field"):
        bucket = PHASE3AA_FUNDAMENTAL_BUCKET
    elif item.get("contains_flow_liquidity_field") and not item.get("contains_new_event_adapter_field"):
        bucket = PHASE3AA_RESEARCH_BUCKET
    else:
        bucket = PHASE3AA_EVENT_BUCKET
    item["ablation_arm"] = PHASE3AA_ABLATION_ARM
    item["phase3_budget_bucket"] = bucket
    item["proof_variant"] = bucket
    item["source_profile"] = "phase3aa_research_feature_injection"
    item["source_lane"] = item.get("source_lane") or bucket
    item["source_generator"] = item.get("source_generator") or "phase3aa_research_feature_pool_injection"
    item["source_credit_policy"] = "phase3aa_opt_in_selector_source_priority"
    item["official_book_eligible"] = False
    item["phase3aa_injection_version"] = PHASE3AA_ENRICH_VERSION
    item["phase3aa_factor_candidate"] = True
    return item


def _memory_objects(memory_roots: list[Path], *, dataset_role: str | None) -> list[LocalSearchMemory]:
    memories: list[LocalSearchMemory] = []
    for root in _expand_memory_roots(memory_roots):
        if root:
            memories.append(LocalSearchMemory.from_previous_run(root, expected_dataset_role=dataset_role))
    return memories


def _expand_memory_roots(memory_roots: list[Path]) -> list[Path]:
    expanded: list[Path] = []
    seen: set[str] = set()
    for root in memory_roots:
        if not root:
            continue
        candidates: list[Path]
        if (root / "search_memory.json").exists() or (root / "candidate_ledger.json").exists():
            candidates = [root]
        elif (root / "previous").is_dir():
            candidates = sorted(path for path in (root / "previous").iterdir() if path.is_dir())
        else:
            candidates = [root]
        for candidate in candidates:
            key = str(candidate.resolve()) if candidate.exists() else str(candidate)
            if key in seen:
                continue
            seen.add(key)
            expanded.append(candidate)
    return expanded


def _seen_by_memory(expression: str, memories: list[LocalSearchMemory]) -> bool:
    return any(memory.has_seen_expression(expression) for memory in memories)


def _factor_pack_rows(factor_pack_paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in factor_pack_paths:
        if not path or not path.exists():
            continue
        payload = _read_json(path)
        for raw in list(payload.get("candidate_rows") or []):
            if not raw.get("expression"):
                continue
            unsafe_fields = _unsafe_formula_fields(str(raw.get("expression") or ""))
            if unsafe_fields:
                continue
            item = dict(raw)
            item["source_lane"] = item.get("source_lane") or "event_derived_feature_layer"
            item["source_generator"] = item.get("source_generator") or "cn_field_factor_pack"
            item["source_factor_pack"] = str(path)
            item["factor_pack_id"] = item.get("factor_pack_id") or payload.get("factor_pack_id")
            item["official_book_eligible"] = False
            item["phase3aa_factor_candidate"] = True
            item["phase3aa_injection_version"] = PHASE3AA_ENRICH_VERSION
            rows.append(enrich_candidate_pool_priority(item))
    return rows


def _event_rows(
    *,
    max_per_role: int,
    include_gate_candidates: bool,
    include_fundamental_candidates: bool,
    include_research_factor_candidates: bool,
    factor_pack_only: bool,
    factor_pack_paths: list[Path],
) -> list[dict[str, Any]]:
    rows = []
    if not factor_pack_only:
        rows.extend(limit_candidate_rows(max_per_role=max_per_role))
        rows.extend(z46_candidate_rows())
    rows.extend(_factor_pack_rows(factor_pack_paths))
    if not include_gate_candidates:
        rows = [row for row in rows if str(row.get("diagnostic_role") or "") != "r3_secondary_gate"]
    rows = [
        row
        for row in rows
        if row.get("expression")
        and (
            bool(row.get("contains_new_event_adapter_field"))
            or (include_fundamental_candidates and bool(row.get("contains_fundamental_field")))
            or (include_research_factor_candidates and bool(row.get("phase3aa_factor_candidate")))
            or (include_research_factor_candidates and bool(row.get("contains_flow_liquidity_field")))
        )
    ]
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = expression_memory_key(str(row.get("expression") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def enrich_pool(
    pool: dict[str, Any],
    *,
    max_event_rows: int,
    max_per_role: int,
    memory_roots: list[Path],
    include_gate_candidates: bool = False,
    include_fundamental_candidates: bool = False,
    include_research_factor_candidates: bool = False,
    factor_pack_only: bool = False,
    factor_pack_paths: list[Path] | None = None,
) -> dict[str, Any]:
    dataset_role = str(pool.get("dataset_role") or "") or None
    expanded_memory_roots = _expand_memory_roots(memory_roots)
    memories = _memory_objects(expanded_memory_roots, dataset_role=dataset_role)
    candidate_pool = [dict(row) for row in list(pool.get("candidate_pool") or [])]
    existing_expr_keys = {expression_memory_key(str(row.get("expression") or "")) for row in candidate_pool if row.get("expression")}

    added: list[dict[str, Any]] = []
    duplicate_existing = 0
    duplicate_memory = 0
    factor_pack_paths = list(factor_pack_paths or [])
    pre_dedup_source_rows = _event_rows(
        max_per_role=max_per_role,
        include_gate_candidates=include_gate_candidates,
        include_fundamental_candidates=include_fundamental_candidates,
        include_research_factor_candidates=include_research_factor_candidates,
        factor_pack_only=factor_pack_only,
        factor_pack_paths=factor_pack_paths,
    )
    for raw in pre_dedup_source_rows:
        expression = str(raw.get("expression") or "")
        expr_key = expression_memory_key(expression)
        if expr_key in existing_expr_keys:
            duplicate_existing += 1
            continue
        if _seen_by_memory(expression, memories):
            duplicate_memory += 1
            continue
        added.append(_copy_event_row(raw, index=len(added) + 1))
        existing_expr_keys.add(expr_key)
        if len(added) >= max_event_rows:
            break

    out = dict(pool)
    out["candidate_pool"] = candidate_pool + added
    out["phase3aa_enrichment"] = {
        "version": PHASE3AA_ENRICH_VERSION,
        "ablation_arm": PHASE3AA_ABLATION_ARM,
        "event_bucket": PHASE3AA_EVENT_BUCKET,
        "event_rows_added": len(added),
        "duplicate_existing_skipped": duplicate_existing,
        "duplicate_memory_skipped": duplicate_memory,
        "input_candidate_pool_count": len(candidate_pool),
        "output_candidate_pool_count": len(out["candidate_pool"]),
        "memory_roots": [str(path) for path in memory_roots],
        "expanded_memory_root_count": len(expanded_memory_roots),
        "expanded_memory_root_sample": [str(path) for path in expanded_memory_roots[:12]],
        "include_gate_candidates": bool(include_gate_candidates),
        "include_fundamental_candidates": bool(include_fundamental_candidates),
        "include_research_factor_candidates": bool(include_research_factor_candidates),
        "factor_pack_only": bool(factor_pack_only),
        "factor_pack_paths": [str(path) for path in factor_pack_paths],
        "pre_dedup_event_source_rows": len(pre_dedup_source_rows),
        "factor_pack_source_rows": len(_factor_pack_rows(factor_pack_paths)),
        "scope": "event-derived factor candidates injected into shared pre-replay pool",
    }
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-pool", type=Path, required=True)
    parser.add_argument("--output-pool", type=Path, required=True)
    parser.add_argument("--max-event-rows", type=int, default=256)
    parser.add_argument("--max-per-role", type=int, default=96)
    parser.add_argument("--memory-root", type=Path, action="append", default=[])
    parser.add_argument("--include-gate-candidates", action="store_true")
    parser.add_argument("--include-fundamental-candidates", action="store_true")
    parser.add_argument("--include-research-factor-candidates", action="store_true")
    parser.add_argument("--factor-pack-only", action="store_true")
    parser.add_argument("--factor-pack", type=Path, action="append", default=[])
    parser.add_argument("--use-default-cn-factor-pack", action="store_true")
    args = parser.parse_args()
    factor_packs = list(args.factor_pack or [])
    if args.use_default_cn_factor_pack and DEFAULT_CN_FACTOR_PACK.exists():
        factor_packs.append(DEFAULT_CN_FACTOR_PACK)

    pool = _read_json(args.input_pool)
    enriched = enrich_pool(
        pool,
        max_event_rows=max(1, int(args.max_event_rows)),
        max_per_role=max(1, int(args.max_per_role)),
        memory_roots=list(args.memory_root or []),
        include_gate_candidates=bool(args.include_gate_candidates),
        include_fundamental_candidates=bool(args.include_fundamental_candidates),
        include_research_factor_candidates=bool(args.include_research_factor_candidates),
        factor_pack_only=bool(args.factor_pack_only),
        factor_pack_paths=factor_packs,
    )
    enriched["created_at"] = utc_now_iso()
    enriched["source_pool"] = str(args.input_pool)
    args.output_pool.parent.mkdir(parents=True, exist_ok=True)
    write_json_artifact(args.output_pool, enriched)
    print(json.dumps(enriched["phase3aa_enrichment"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
