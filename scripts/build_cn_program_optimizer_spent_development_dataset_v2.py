"""Build canonical spent-development Program evidence without financial reads.

The dataset is the five immutable development generations that comprise the
post-C 1310-exact prior set. Closed result artifacts are joined to the frozen
Program structural-gene catalog. No validation, holdout or forward data is read.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as shared
from our_system_phase2.runtime.cn_program_optimizer_d1_development_v1 import (
    SOURCE_COMPONENT_POOL_SHA256,
    SOURCE_FREEZE_ROOT,
    SOURCE_RAW_RESERVOIR_SHA256,
    SOURCE_REGISTRY_PATH,
    SOURCE_REGISTRY_SHA256,
)
from our_system_phase2.services.program_search_optimizer_v1 import normalized_program_gene_identity_v1
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry, stable_hash

EXPECTED_COHORT_COUNTS = {
    "TOURNAMENT_V1": 504,
    "SUCCESSOR_D1": 386,
    "D1_FRESH": 140,
    "D1_CONTINUATION_B": 140,
    "D1_CONTINUATION_C": 140,
}
EXPECTED_TOTAL = 1310
PRIOR_FREEZE_RELATIVE = Path(
    "runtime/run_plans/cn_program_optimizer_d1_continuation_C_postrun_prior_exact_freeze_20260817.json"
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _float(value: Any) -> float | None:
    if value is None:
        return None
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite spent-development metric")
    return parsed


def _behavior_pair(record: Mapping[str, Any] | None) -> str | None:
    if not isinstance(record, Mapping):
        return None
    primary = record.get("primary")
    control = record.get("base_control")
    if not isinstance(primary, Mapping) or not isinstance(control, Mapping):
        return None
    left = str(primary.get("behavior_identity") or "")
    right = str(control.get("behavior_identity") or "")
    if not left or not right:
        return None
    return stable_hash({"primary": left, "base_control": right})[:24]


def _economic_row(
    *, exact: str, cohort: str, cohort_index: int, source_order: int,
    source_wave: int | None, template_id: str, selection_kind: str,
    admission: Mapping[str, Any], uplift: Mapping[str, Any] | None,
    record: Mapping[str, Any] | None, source_record_sha256: str,
) -> dict[str, Any]:
    admitted = bool(admission.get("admitted"))
    if admitted != (uplift is not None):
        raise RuntimeError(f"spent dual-head domain drift: {exact}")
    credit = dict((uplift or {}).get("program_credit") or {})
    matched_return = _float(credit.get("matched_cumulative_net_return_increment"))
    matched_reward = _float(credit.get("matched_net_reward_increment"))
    if admitted and (matched_return is None or matched_reward is None):
        raise RuntimeError(f"spent admitted uplift missing: {exact}")
    windows = tuple(float(x) for x in credit.get("window_return_increments") or ())
    productive = bool(
        admitted and matched_return is not None and matched_reward is not None
        and matched_return > 0.0 and matched_reward > 0.0
    )
    return {
        "exact_identity": exact,
        "source_cohort": cohort,
        "cohort_index": int(cohort_index),
        "source_order": int(source_order),
        "source_wave": source_wave,
        "template_id": str(template_id),
        "selection_kind": str(selection_kind),
        "admitted": admitted,
        "productive": productive,
        "relevance_grade": 2 if productive else 1 if admitted else 0,
        "matched_cumulative_net_return_increment": matched_return,
        "matched_net_reward_increment": matched_reward,
        "window_return_increments": list(windows),
        "lower_tail_window_return_increment": _float(credit.get("lower_tail_window_return_increment")),
        "robust_median_window_return_increment": _float(credit.get("robust_median_window_return_increment")),
        "cross_window_matched_consistency": _float(credit.get("cross_window_matched_consistency")),
        "behavior_pair_identity": _behavior_pair(record),
        "source_record_sha256": str(source_record_sha256),
        "admission_policy_id": str(admission.get("policy_id") or ""),
    }


def _record_by_hash(record_root: Path) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    if not record_root.is_dir():
        return output
    for path in sorted(record_root.glob("record_*.json")):
        row = _read_json(path)
        identity = str(row.get("record_payload_sha256") or "")
        if not identity or identity in output:
            raise RuntimeError(f"record hash cardinality drift: {path}")
        output[identity] = row
    return output


def _read_one_existing(root: Path, names: Sequence[str]) -> Path:
    found = [root / name for name in names if (root / name).is_file()]
    if len(found) != 1:
        raise RuntimeError(f"artifact cardinality drift in {root}: {found}")
    return found[0]


def _collect_wave_run(run_root: Path, cohort: str, cohort_index: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    successor_seen: dict[str, dict[str, str]] = {}
    wave_roots = [p for p in sorted(run_root.glob("wave_*")) if p.is_dir() and not p.name.endswith(".inflight")]
    for wave_root in wave_roots:
        try:
            wave = int(wave_root.name.split("_")[-1])
        except ValueError:
            continue
        schedules_path = _read_one_existing(wave_root, ("physical_schedules.jsonl", "wave_physical_schedules.jsonl"))
        results_path = _read_one_existing(wave_root, ("physical_results.jsonl", "wave_physical_results.jsonl"))
        schedules = _read_jsonl(schedules_path)
        results = _read_jsonl(results_path)
        by_exact: dict[str, dict[str, Any]] = {}
        for schedule in schedules:
            exact = str(schedule.get("d1_exact_identity") or schedule.get("successor_exact_identity") or "")
            if not exact:
                raise RuntimeError(f"wave schedule missing exact: {schedules_path}")
            if exact in by_exact:
                raise RuntimeError(f"duplicate wave schedule exact: {exact}")
            by_exact[exact] = schedule
        result_by_exact = {str(row.get("exact_identity") or ""): row for row in results}
        if any(not exact for exact in result_by_exact) or len(result_by_exact) != len(results):
            raise RuntimeError(f"wave result exact cardinality drift: {results_path}")
        records = _record_by_hash(wave_root / "records")

        if cohort == "SUCCESSOR_D1":
            asks_path = wave_root / "logical_asks.jsonl"
            if not asks_path.is_file():
                raise RuntimeError(f"successor logical asks missing: {wave_root}")
            asks = _read_jsonl(asks_path)
            asks_by_exact: dict[str, list[dict[str, Any]]] = {}
            ask_order: dict[str, int] = {}
            for ordinal, ask in enumerate(asks):
                exact = str(ask.get("exact_identity") or "")
                if not exact:
                    raise RuntimeError(f"successor logical exact missing: {asks_path}")
                asks_by_exact.setdefault(exact, []).append(ask)
                ask_order.setdefault(exact, ordinal)
            if set(asks_by_exact) != set(result_by_exact):
                raise RuntimeError(f"successor logical/result exact drift: {wave_root}")
            if not set(by_exact).issubset(result_by_exact):
                raise RuntimeError(f"successor schedule outside result set: {wave_root}")

            for exact in sorted(result_by_exact, key=lambda identity: ask_order[identity]):
                result = result_by_exact[exact]
                schedule = by_exact.get(exact)
                if schedule is None:
                    if not bool(result.get("cache_hit")):
                        raise RuntimeError(f"successor unscheduled result is not cache hit: {exact}")
                    prior = successor_seen.get(exact)
                    if prior is None:
                        raise RuntimeError(f"successor cache hit lacks prior physical provenance: {exact}")
                    if (
                        str(result.get("physical_result_hash") or "") != prior["physical_result_hash"]
                        or str(result.get("source_record_sha256") or "") != prior["source_record_sha256"]
                    ):
                        raise RuntimeError(f"successor cache-hit provenance drift: {exact}")
                    continue
                if bool(result.get("cache_hit")):
                    raise RuntimeError(f"successor scheduled result unexpectedly marked cache hit: {exact}")
                if exact in successor_seen:
                    raise RuntimeError(f"successor repeated physical schedule without cache semantics: {exact}")
                logical_asks = asks_by_exact[exact]
                logical_templates = {
                    str(ask.get("template_id") or "") for ask in logical_asks
                }
                schedule_template = str(schedule.get("template_id") or "")
                if logical_templates != {schedule_template}:
                    raise RuntimeError(f"successor logical/schedule template drift: {exact}")
                logical_policies = tuple(
                    dict.fromkeys(
                        str(ask.get("policy") or ask.get("selection_kind") or "")
                        for ask in logical_asks
                        if str(ask.get("policy") or ask.get("selection_kind") or "")
                    )
                )
                schedule_policies = tuple(
                    map(str, schedule.get("successor_logical_policies") or ())
                )
                if schedule_policies and set(schedule_policies) != set(logical_policies):
                    raise RuntimeError(f"successor logical/schedule policy drift: {exact}")
                source_sha = str(result.get("source_record_sha256") or "")
                record = records.get(source_sha)
                if record is None:
                    raise RuntimeError(f"successor physical record missing: {exact}")
                selection_kind = "+".join(schedule_policies or logical_policies)
                rows.append(_economic_row(
                    exact=exact,
                    cohort=cohort,
                    cohort_index=cohort_index,
                    source_order=wave * 1000 + ask_order[exact],
                    source_wave=wave,
                    template_id=str(schedule.get("template_id") or ""),
                    selection_kind=selection_kind,
                    admission=dict(result.get("admission") or {}),
                    uplift=(dict(result["uplift"]) if isinstance(result.get("uplift"), Mapping) else None),
                    record=record,
                    source_record_sha256=source_sha,
                ))
                successor_seen[exact] = {
                    "physical_result_hash": str(result.get("physical_result_hash") or ""),
                    "source_record_sha256": source_sha,
                }
            evidence.append({"path": str(asks_path), "sha256": _sha256(asks_path)})
        else:
            if set(by_exact) != set(result_by_exact):
                raise RuntimeError(f"wave schedule/result exact drift: {wave_root}")
            for exact, schedule in sorted(by_exact.items(), key=lambda item: int(item[1].get("main_record_ordinal") or 0)):
                result = result_by_exact[exact]
                source_sha = str(result.get("source_record_sha256") or "")
                record = records.get(source_sha)
                if record is None:
                    raise RuntimeError(f"wave physical record missing: {exact}")
                selection_kind = str(
                    schedule.get("d1_selection_kind")
                    or "+".join(map(str, schedule.get("successor_logical_policies") or ()))
                    or dict(schedule.get("proposal_receipt") or {}).get("generation_arm")
                    or ""
                )
                rows.append(_economic_row(
                    exact=exact,
                    cohort=cohort,
                    cohort_index=cohort_index,
                    source_order=wave * 1000 + int(schedule.get("main_record_ordinal") or 0),
                    source_wave=wave,
                    template_id=str(schedule.get("template_id") or ""),
                    selection_kind=selection_kind,
                    admission=dict(result.get("admission") or {}),
                    uplift=(dict(result["uplift"]) if isinstance(result.get("uplift"), Mapping) else None),
                    record=record,
                    source_record_sha256=source_sha,
                ))
        evidence.extend([
            {"path": str(schedules_path), "sha256": _sha256(schedules_path)},
            {"path": str(results_path), "sha256": _sha256(results_path)},
        ])
    return rows, evidence


def _collect_tournament(run_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    for phase_index, phase_name in enumerate(("stage01_run", "stage02_run")):
        phase_root = run_root / phase_name
        schedules_path = phase_root / "phase_c_selected_schedule.jsonl"
        feedback_path = phase_root / "phase_c_bandit_feedback_ledger.jsonl"
        records_path = phase_root / "phase_c_record_results.jsonl"
        schedules = _read_jsonl(schedules_path)
        feedback = _read_jsonl(feedback_path)
        records = _read_jsonl(records_path)
        by_ordinal = {int(x["main_record_ordinal"]): x for x in schedules}
        feedback_by_ordinal = {int(x["main_record_ordinal"]): x for x in feedback}
        record_by_ordinal = {int(x["main_record_ordinal"]): x for x in records}
        if set(by_ordinal) != set(feedback_by_ordinal) or set(by_ordinal) != set(record_by_ordinal):
            raise RuntimeError(f"tournament phase coverage drift: {phase_root}")
        for ordinal, schedule in sorted(by_ordinal.items()):
            if str(schedule.get("template_id") or "") == "BASE":
                continue
            optimizer_ask = dict(schedule.get("optimizer_ask") or {})
            exact = str(optimizer_ask.get("exact_identity") or "")
            if not exact:
                raise RuntimeError("tournament schedule missing optimizer exact")
            fb = feedback_by_ordinal[ordinal]
            rows.append(_economic_row(
                exact=exact,
                cohort="TOURNAMENT_V1",
                cohort_index=0,
                source_order=phase_index * 1_000_000 + ordinal,
                source_wave=int(schedule.get("checkpoint_ordinal") or 0),
                template_id=str(schedule.get("template_id") or ""),
                selection_kind=str(schedule.get("generation_arm") or ""),
                admission=dict(fb.get("absolute_admission") or {}),
                uplift=(dict(fb["enhancer_credit"]) if isinstance(fb.get("enhancer_credit"), Mapping) else None),
                record=record_by_ordinal[ordinal],
                source_record_sha256=str(fb.get("record_payload_sha256") or ""),
            ))
        evidence.extend([
            {"path": str(schedules_path), "sha256": _sha256(schedules_path)},
            {"path": str(feedback_path), "sha256": _sha256(feedback_path)},
            {"path": str(records_path), "sha256": _sha256(records_path)},
        ])
    return rows, evidence


def _catalog_metadata() -> tuple[dict[str, dict[str, Any]], dict[str, str], list[dict[str, Any]]]:
    freeze_root = Path(SOURCE_FREEZE_ROOT)
    reservoir_path = freeze_root / "phase_c_raw_program_reservoir.jsonl"
    component_path = freeze_root / "phase_c_session_executable_component_pool.jsonl"
    registry_path = Path(SOURCE_REGISTRY_PATH)
    if _sha256(reservoir_path) != SOURCE_RAW_RESERVOIR_SHA256:
        raise RuntimeError("spent dataset raw reservoir drift")
    if _sha256(component_path) != SOURCE_COMPONENT_POOL_SHA256:
        raise RuntimeError("spent dataset component pool drift")
    if _sha256(registry_path) != SOURCE_REGISTRY_SHA256:
        raise RuntimeError("spent dataset registry drift")
    reservoir = engine._read_jsonl(reservoir_path)
    component_rows = engine._read_jsonl(component_path)
    registry = UnifiedCapabilityRegistry.read(registry_path)
    catalog, _ = engine._build_catalog(
        reservoir=reservoir,
        component_rows=component_rows,
        registry=registry,
    )
    entries = shared._program_entries(catalog)
    genes_by_exact = {entry.exact_identity: dict(entry.genes) for entry in entries}
    slots = tuple(dict(entries[0].genes))
    group_by_exact: dict[str, str] = {}
    for template in sorted(catalog):
        for source in catalog[template]:
            exact = normalized_program_gene_identity_v1(dict(source["program_genes"]), ordered_slots=slots)
            group_by_exact[exact] = str(source["base_component_id"])
    if set(group_by_exact) != set(genes_by_exact):
        raise RuntimeError("spent dataset catalog exact coverage drift")
    evidence = [
        {"path": str(reservoir_path), "sha256": _sha256(reservoir_path)},
        {"path": str(component_path), "sha256": _sha256(component_path)},
        {"path": str(registry_path), "sha256": _sha256(registry_path)},
    ]
    return genes_by_exact, group_by_exact, evidence


def _discover_roots(repo_root: Path) -> dict[str, Path]:
    plans = repo_root / "runtime" / "run_plans"
    tournament = _read_json(plans / "cn_program_optimizer_tournament_batch_feasibility_retry_outcome_20260815.json")
    roots: dict[str, Path] = {
        "TOURNAMENT_V1": Path(str(tournament["execution_output_root"])),
    }
    member_files = (
        plans / "cn_program_optimizer_d1_validation_candidate_members_20260816.jsonl",
        plans / "cn_program_optimizer_d1_transfer_B_validation_freeze_20260817" / "validation_candidate_members.jsonl",
        plans / "cn_program_optimizer_d1_transfer_C_validation_freeze_20260817_4c5404b" / "validation_candidate_members.jsonl",
    )
    for member_file in member_files:
        for row in _read_jsonl(member_file):
            cohort = str(row.get("source_cohort") or "")
            root = str(row.get("source_root") or "")
            if cohort in {"SUCCESSOR_D1", "D1_FRESH", "D1_CONTINUATION_B", "D1_CONTINUATION_C"}:
                if cohort in roots and str(roots[cohort]) != root:
                    raise RuntimeError(f"spent source root drift: {cohort}")
                roots[cohort] = Path(root)
    expected = set(EXPECTED_COHORT_COUNTS)
    if set(roots) != expected:
        raise RuntimeError(f"spent source-root coverage drift: {sorted(roots)}")
    for cohort, root in roots.items():
        if not root.is_dir():
            raise FileNotFoundError(f"spent source root missing {cohort}: {root}")
    return roots


def build_dataset(*, repo_root: Path, output: Path) -> dict[str, Any]:
    repo = repo_root.resolve()
    prior = _read_json(repo / PRIOR_FREEZE_RELATIVE)
    prior_sha = _verify_self_hash(prior, "freeze_payload_sha256", "post-C prior freeze")
    prior_ids = list(map(str, prior.get("combined_prior_exact_identities") or ()))
    if len(prior_ids) != EXPECTED_TOTAL or len(set(prior_ids)) != EXPECTED_TOTAL:
        raise RuntimeError("spent prior population drift")
    roots = _discover_roots(repo)
    genes_by_exact, group_by_exact, catalog_evidence = _catalog_metadata()

    all_rows: list[dict[str, Any]] = []
    evidence = list(catalog_evidence)
    tournament_rows, tournament_evidence = _collect_tournament(roots["TOURNAMENT_V1"])
    all_rows.extend(tournament_rows)
    evidence.extend(tournament_evidence)
    for cohort_index, cohort in enumerate(
        ("SUCCESSOR_D1", "D1_FRESH", "D1_CONTINUATION_B", "D1_CONTINUATION_C"),
        start=1,
    ):
        rows, source_evidence = _collect_wave_run(roots[cohort], cohort, cohort_index)
        all_rows.extend(rows)
        evidence.extend(source_evidence)

    counts: dict[str, int] = {}
    for cohort in EXPECTED_COHORT_COUNTS:
        counts[cohort] = sum(str(row["source_cohort"]) == cohort for row in all_rows)
    if counts != EXPECTED_COHORT_COUNTS:
        raise RuntimeError(f"spent cohort count drift: {counts}")
    exacts = [str(row["exact_identity"]) for row in all_rows]
    if len(exacts) != EXPECTED_TOTAL or len(set(exacts)) != EXPECTED_TOTAL:
        raise RuntimeError("spent exact cardinality drift")
    if set(exacts) != set(prior_ids):
        raise RuntimeError("spent dataset does not equal post-C prior set")

    for row in all_rows:
        exact = str(row["exact_identity"])
        genes = genes_by_exact.get(exact)
        if genes is None:
            raise RuntimeError(f"spent exact missing structural genes: {exact}")
        if str(genes.get("program_template_id") or "") != str(row["template_id"]):
            raise RuntimeError(f"spent template/gene drift: {exact}")
        row["structural_genes"] = genes
        row["base_group_id"] = group_by_exact[exact]
        row["query_group"] = f"{row['source_cohort']}::{row['template_id']}"

    all_rows.sort(key=lambda row: (int(row["cohort_index"]), int(row["source_order"]), str(row["exact_identity"])))
    cohort_metrics = {}
    for cohort in EXPECTED_COHORT_COUNTS:
        subset = [row for row in all_rows if row["source_cohort"] == cohort]
        cohort_metrics[cohort] = {
            "evaluated": len(subset),
            "admitted": sum(bool(row["admitted"]) for row in subset),
            "productive": sum(bool(row["productive"]) for row in subset),
            "behavior_pair_observed": sum(bool(row.get("behavior_pair_identity")) for row in subset),
        }
    payload: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_spent_development_dataset_v2",
        "status": "FROZEN_SPENT_DEVELOPMENT_DATASET_READY",
        "dataset_role": "DEVELOPMENT_ONLY_OFFLINE_SEARCH_POLICY_REPLAY",
        "post_C_prior_exact_count": EXPECTED_TOTAL,
        "post_C_prior_exact_identities_sha256": stable_hash(sorted(prior_ids)),
        "post_C_prior_freeze_payload_sha256": prior_sha,
        "cohort_order": list(EXPECTED_COHORT_COUNTS),
        "cohort_counts": counts,
        "cohort_metrics": cohort_metrics,
        "rows": all_rows,
        "rows_sha256": stable_hash(all_rows),
        "source_roots": {key: str(value) for key, value in roots.items()},
        "source_artifacts": evidence,
        "financial_evaluation_performed_by_builder": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_challenge_reads": 0,
        "forward_B_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "promotion_authorized": False,
    }
    payload["dataset_payload_sha256"] = stable_hash(payload)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = build_dataset(repo_root=args.repo_root, output=args.output)
    print(json.dumps({
        "status": payload["status"],
        "rows": len(payload["rows"]),
        "cohort_counts": payload["cohort_counts"],
        "rows_sha256": payload["rows_sha256"],
        "dataset_payload_sha256": payload["dataset_payload_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
