from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import time
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as shared
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from our_system_phase2.runtime.cn_program_optimizer_d1_development_v1 import (
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    FROZEN_BASE_PROGRAM_COUNT,
    FROZEN_ENHANCED_PROGRAM_COUNT,
    FROZEN_PROGRAM_SPACE_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    PRIOR_EXACT_COUNT,
    PRIOR_EXACT_IDENTITIES_SHA256,
    PRIOR_FREEZE_PAYLOAD_SHA256,
    REMAINING_PROSPECTIVE_ENHANCED,
    SEEDS,
    SURROGATE_CONFIG,
    TPE_CONFIG,
)
from our_system_phase2.services.program_optimizer_d1_cohort_v1 import (
    D1_LOGICAL_RECORDS,
    D1_POLICY_ID,
    D1_SELECTION_SURROGATE,
    D1_SELECTION_TPE,
    D1_SELECTION_UNIFORM,
    D1_SELECTOR_COUNTS,
    ProgramOptimizerD1CohortV1,
)
from our_system_phase2.services.program_optimizer_successor_benchmark_v1 import (
    BOOTSTRAP_WAVES,
    TOTAL_WAVES,
    UNIFORM_FLOOR_WAVES,
    PhysicalProgramResultV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HYBRID_TPE_PROGRAM,
    STRUCTURED_SURROGATE_PROGRAM,
    UNIFORM_CONTROL,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import (
    MATCHED_CONTROL_CONTRACT_ID,
    ProgramUpliftCredit,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


STATUS = "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE"
CLOSURE_NAME = "CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE.json"
ENHANCED_TEMPLATES = tuple(template for template in TEMPLATE_ORDER if template != "BASE")


def _load_authority(
    args: argparse.Namespace,
    *,
    authorization: Mapping[str, Any],
    repo_sha: str,
) -> dict[str, Any]:
    campaign_id = str(authorization.get("campaign_id") or CAMPAIGN_ID)
    campaign_profile = str(authorization.get("campaign_profile") or CAMPAIGN_PROFILE)
    program_space = dict(authorization.get("program_space") or {})
    prior_count = int(program_space.get("prior_exact_count") or PRIOR_EXACT_COUNT)
    prior_ident_sha = str(
        program_space.get("prior_exact_identities_sha256")
        or PRIOR_EXACT_IDENTITIES_SHA256
    )
    prior_freeze_payload_sha = str(
        program_space.get("prior_freeze_payload_sha256")
        or PRIOR_FREEZE_PAYLOAD_SHA256
    )
    return shared._load_authority(
        args,
        authorization=authorization,
        repo_sha=repo_sha,
        campaign_id=campaign_id,
        campaign_profile=campaign_profile,
        prior_freeze_payload_sha256=prior_freeze_payload_sha,
        prior_exact_count=prior_count,
        prior_exact_identities_sha256=prior_ident_sha,
        prior_identity_field="combined_prior_exact_identities",
        input_binding_schema_version="cn_program_optimizer_d1_input_binding_v1",
    )


def _cohort(authority: Mapping[str, Any]) -> ProgramOptimizerD1CohortV1:
    return ProgramOptimizerD1CohortV1(
        entries=authority["entries"],
        template_ids=ENHANCED_TEMPLATES,
        group_by_exact_identity=authority["group_by_exact"],
        uniform_seed=int(SEEDS["UNIFORM"]),
        tpe_seed=int(SEEDS["TPE_CONTROL"]),
        surrogate_seed=int(SEEDS["TPE_TO_SURROGATE"]),
        tpe_config=TPE_CONFIG,
        surrogate_config=SURROGATE_CONFIG,
        prior_exact_identities=authority["prior_ids"],
        minimum_distinct_groups=16,
        maximum_per_group=4,
    )


def _logical_row(row: Any) -> dict[str, Any]:
    return {
        "policy_id": D1_POLICY_ID,
        "wave_index": row.wave_index,
        "template_id": row.template_id,
        "exact_identity": row.exact_identity,
        "logical_proposal_id": row.logical_proposal_id,
        "optimizer_proposal_id": row.optimizer_proposal_id,
        "selection_kind": row.selection_kind,
        "optimizer_ask": copy.deepcopy(row.optimizer_ask),
    }


def _generation_arm(selection_kind: str) -> str:
    mapping = {
        D1_SELECTION_UNIFORM: UNIFORM_CONTROL,
        D1_SELECTION_TPE: HYBRID_TPE_PROGRAM,
        D1_SELECTION_SURROGATE: STRUCTURED_SURROGATE_PROGRAM,
    }
    try:
        return mapping[str(selection_kind)]
    except KeyError as exc:
        raise RuntimeError("D1_UNKNOWN_SELECTION_KIND") from exc


def _build_physical_schedules(
    *,
    prepared: Any,
    authority: Mapping[str, Any],
    start_ordinal: int,
) -> list[dict[str, Any]]:
    schedules = []
    by_exact = {row.exact_identity: row for row in prepared.asks}
    for offset, exact in enumerate(prepared.physical_exact_identities):
        logical = by_exact[exact]
        template_id = str(logical.template_id)
        ask_body = {
            "schema_version": "cn_program_optimizer_d1_physical_ask_v1",
            "main_record_ordinal": int(start_ordinal + offset),
            "checkpoint_ordinal": int(prepared.wave_index),
            "template_id": template_id,
            "template_record_ordinal": int(prepared.wave_index),
            "generation_arm": _generation_arm(logical.selection_kind),
            "matched_control_contract_id": MATCHED_CONTROL_CONTRACT_ID,
            "canary_profile": CAMPAIGN_PROFILE,
            "absolute_admission_head_eligible": True,
            "conditional_uplift_head_eligible": True,
        }
        ask = {**ask_body, "ask_record_sha256": stable_hash(ask_body)}
        decision = engine._self_hashed(
            {
                "schema_version": "cn_program_optimizer_d1_physical_selection_v1",
                "policy_id": D1_POLICY_ID,
                "wave_index": int(prepared.wave_index),
                "main_record_ordinal": int(start_ordinal + offset),
                "template_id": template_id,
                "selection_kind": str(logical.selection_kind),
                "exact_identity": exact,
                "logical_proposal_id": str(logical.logical_proposal_id),
                "adaptive_template_credit_used": False,
            },
            "selection_decision_sha256",
        )
        schedule = engine._schedule_record(
            ask,
            dict(authority["catalog_by_exact"][exact]),
            decision,
            components_by_id=authority["components_by_id"],
            adapter=authority["adapter"],
            compiler=authority["compiler"],
        )
        schedule.update(
            {
                "d1_policy_id": D1_POLICY_ID,
                "d1_wave_index": int(prepared.wave_index),
                "d1_exact_identity": exact,
                "d1_selection_kind": str(logical.selection_kind),
                "d1_logical_proposal_id": str(logical.logical_proposal_id),
                # Reuse the already-audited PhysicalProgramResultV1 extractor.
                "successor_exact_identity": exact,
            }
        )
        schedule["schedule_record_sha256"] = stable_hash(
            {
                key: value
                for key, value in schedule.items()
                if key != "schedule_record_sha256"
            }
        )
        schedules.append(schedule)
    return schedules


def _physical_result_record(
    result: PhysicalProgramResultV1,
    *,
    source_record_sha256: str,
    wave_index: int,
) -> dict[str, Any]:
    return {
        "schema_version": "cn_program_optimizer_d1_physical_result_receipt_v1",
        "policy_id": D1_POLICY_ID,
        "wave_index": int(wave_index),
        "exact_identity": result.exact_identity,
        "physical_result_hash": result.physical_result_hash,
        "admission": result.admission.to_record(),
        "uplift": None if result.uplift is None else result.uplift.to_record(),
        "source_record_sha256": str(source_record_sha256),
    }


def _close_wave(
    *,
    inflight: Path,
    closed: Path,
    previous_manifest_sha256: str,
    wave_index: int,
) -> Path:
    artifacts = [
        shared._artifact(path, inflight)
        for path in sorted(inflight.rglob("*"))
        if path.is_file() and path.name != "wave_manifest.json"
    ]
    manifest = engine._self_hashed(
        {
            "schema_version": "cn_program_optimizer_d1_wave_manifest_v1",
            "status": "D1_DEVELOPMENT_WAVE_CLOSED_IMMUTABLE",
            "policy_id": D1_POLICY_ID,
            "wave_index": int(wave_index),
            "previous_wave_manifest_sha256": str(previous_manifest_sha256),
            "artifacts": artifacts,
        },
        "manifest_payload_sha256",
    )
    engine._write_json(inflight / "wave_manifest.json", manifest)
    if closed.exists():
        raise RuntimeError("D1_CLOSED_WAVE_ALREADY_EXISTS")
    inflight.replace(closed)
    return closed / "wave_manifest.json"


def _metrics(cohort: ProgramOptimizerD1CohortV1) -> dict[str, Any]:
    rows = cohort.completed_rows()
    post = [row for row in rows if int(row["wave_index"]) >= BOOTSTRAP_WAVES]
    optimized = [
        row
        for row in rows
        if str(row["selection_kind"]) == D1_SELECTION_SURROGATE
    ]
    if len(rows) != 140 or len(post) != 112 or len(optimized) != 84:
        raise RuntimeError("D1_FINAL_METRIC_DENOMINATOR_DRIFT")
    return {
        "policy_id": D1_POLICY_ID,
        "TOTAL_POLICY_EFFICIENCY": shared._metric_block(rows),
        "POST_BOOTSTRAP_EFFICIENCY": shared._metric_block(post),
        "SURROGATE_OPTIMIZED_EFFICIENCY": shared._metric_block(optimized),
        "selector_counts": cohort.selector_counts(),
        "per_template": {
            template: shared._metric_block(
                [row for row in rows if str(row["template_id"]) == template]
            )
            for template in ENHANCED_TEMPLATES
        },
        "per_wave": {
            str(wave): shared._metric_block(
                [row for row in rows if int(row["wave_index"]) == wave]
            )
            for wave in range(TOTAL_WAVES)
        },
    }


def _synthetic_result(exact_identity: str) -> PhysicalProgramResultV1:
    admitted = int(stable_hash({"d1_prefinancial": exact_identity})[-1], 16) % 4 != 0
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"prefinancial-{exact_identity}",
        pair_id=f"pair-{exact_identity}",
        program_id=f"program-{exact_identity}",
        control_program_id=f"control-{exact_identity}",
        admitted=admitted,
        failure_reasons=() if admitted else ("PREFINANCIAL_SYNTHETIC_NOT_ADMITTED",),
        metrics={"prefinancial_synthetic": True},
    )
    uplift = (
        ProgramUpliftCredit(
            record_payload_sha256=admission.record_payload_sha256,
            pair_id=admission.pair_id,
            program_id=admission.program_id,
            control_program_id=admission.control_program_id,
            program_credit={
                "matched_cumulative_net_return_increment": 0.1,
                "matched_net_reward_increment": 0.2,
            },
        )
        if admitted
        else None
    )
    return PhysicalProgramResultV1.create(
        exact_identity=exact_identity,
        admission=admission,
        uplift=uplift,
    )


def _run_prefinancial(
    args: argparse.Namespace,
    *,
    authorization: Mapping[str, Any],
    repo_sha: str,
) -> dict[str, Any]:
    authority = _load_authority(args, authorization=authorization, repo_sha=repo_sha)
    program_space = dict(authorization.get("program_space") or {})
    remaining_prospective = int(
        program_space.get("remaining_prospective_enhanced_exact_count")
        or REMAINING_PROSPECTIVE_ENHANCED
    )
    cohort = _cohort(authority)
    selected: list[str] = []
    arms: dict[str, int] = {}
    for wave in range(TOTAL_WAVES):
        prepared = cohort.prepare_wave()
        if len(prepared.asks) != 7:
            raise RuntimeError("D1_PREFINANCIAL_WAVE_CARDINALITY_DRIFT")
        if set(prepared.physical_exact_identities).intersection(authority["prior_ids"]):
            raise RuntimeError("D1_PREFINANCIAL_PRIOR_EXACT_REUSE")
        schedules = _build_physical_schedules(
            prepared=prepared,
            authority=authority,
            start_ordinal=wave * 7,
        )
        if len(schedules) != 7:
            raise RuntimeError("D1_PREFINANCIAL_SCHEDULE_COUNT_DRIFT")
        for schedule in schedules:
            arm = str(dict(schedule["proposal_receipt"])["generation_arm"])
            arms[arm] = arms.get(arm, 0) + 1
        selected.extend(prepared.physical_exact_identities)
        cohort.commit_wave(
            prepared,
            {
                exact: _synthetic_result(exact)
                for exact in prepared.physical_exact_identities
            },
        )
    cohort.verify_terminal_shape()
    if len(selected) != D1_LOGICAL_RECORDS or len(set(selected)) != D1_LOGICAL_RECORDS:
        raise RuntimeError("D1_PREFINANCIAL_EXACT_CARDINALITY_DRIFT")
    expected_arms = {
        UNIFORM_CONTROL: D1_SELECTOR_COUNTS[D1_SELECTION_UNIFORM],
        HYBRID_TPE_PROGRAM: D1_SELECTOR_COUNTS[D1_SELECTION_TPE],
        STRUCTURED_SURROGATE_PROGRAM: D1_SELECTOR_COUNTS[D1_SELECTION_SURROGATE],
    }
    if arms != expected_arms:
        raise RuntimeError("D1_PREFINANCIAL_PROVENANCE_ARM_DRIFT")
    return {
        "status": "D1_PREFINANCIAL_READY",
        "program_space_count": len(authority["entries"]),
        "prior_exact_count": len(authority["prior_ids"]),
        "prospective_enhanced_available_count": remaining_prospective,
        "logical_records": len(selected),
        "unique_selected_exact_count": len(set(selected)),
        "selector_counts": cohort.selector_counts(),
        "physical_generation_arm_counts": arms,
        "real_space_20_wave_synthetic_rehearsal": True,
        "financial_evaluation_executed": False,
        "restricted_reads": 0,
    }


def run_authorized_d1_cohort(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    repo_sha = str(admission["repo_sha"])
    if bool(getattr(args, "prefinancial_only", False)):
        raise RuntimeError("D1_PREFINANCIAL_ONLY_FORBIDDEN_AFTER_ADMISSION")
    authority = _load_authority(args, authorization=authorization, repo_sha=repo_sha)
    program_space = dict(authorization.get("program_space") or {})
    campaign_id = str(authorization.get("campaign_id") or CAMPAIGN_ID)
    campaign_profile = str(authorization.get("campaign_profile") or CAMPAIGN_PROFILE)
    prior_exact_count = int(program_space.get("prior_exact_count") or PRIOR_EXACT_COUNT)
    prior_exact_identities_sha256 = str(
        program_space.get("prior_exact_identities_sha256")
        or PRIOR_EXACT_IDENTITIES_SHA256
    )
    prior_freeze_payload_sha256 = str(
        program_space.get("prior_freeze_payload_sha256")
        or PRIOR_FREEZE_PAYLOAD_SHA256
    )
    remaining_prospective = int(
        program_space.get("remaining_prospective_enhanced_exact_count")
        or REMAINING_PROSPECTIVE_ENHANCED
    )
    root = args.output_root.resolve()
    if not root.is_dir() or not (root / ".project_control_execution").is_dir():
        raise RuntimeError("D1_ADMITTED_OUTPUT_ROOT_MISSING")
    unexpected = {
        path.name for path in root.iterdir()
        if path.name != ".project_control_execution"
    }
    if unexpected:
        raise RuntimeError("D1_ADMITTED_OUTPUT_ROOT_NOT_CLEAN")

    engine._write_json(root / "input_binding.json", authority["input_binding"])
    input_hash = str(authority["input_binding"]["input_binding_sha256"])
    cohort = _cohort(authority)
    engine._write_json(root / "cohort_state_genesis.json", cohort.snapshot())
    engine._write_json(
        root / "prior_exact_binding.json",
        {
            "schema_version": "cn_program_optimizer_d1_prior_binding_v1",
            "prior_exact_count": prior_exact_count,
            "prior_exact_identities_sha256": prior_exact_identities_sha256,
            "prior_freeze_payload_sha256": prior_freeze_payload_sha256,
            "prior_results_imported_as_optimizer_feedback": False,
        },
    )
    engine._write_json(
        root / "policy_binding.json",
        {
            "schema_version": "cn_program_optimizer_d1_policy_binding_v1",
            "policy_id": D1_POLICY_ID,
            "selector_counts": D1_SELECTOR_COUNTS,
            "logical_records": D1_LOGICAL_RECORDS,
            "template_routing": False,
            "dynamic_handoff": False,
            "dynamic_budget_reallocation": False,
        },
    )

    previous_manifest_sha = "GENESIS"
    wave_manifests: list[str] = []
    next_ordinal = 0
    wall_start = time.perf_counter()
    for wave in range(TOTAL_WAVES):
        state_before = cohort.snapshot()
        prepared = cohort.prepare_wave()
        inflight = root / f"wave_{wave:03d}.inflight"
        closed = root / f"wave_{wave:03d}"
        if inflight.exists() or closed.exists():
            raise RuntimeError("D1_WAVE_PATH_NOT_FRESH")
        inflight.mkdir(parents=False, exist_ok=False)
        engine._write_json(inflight / "cohort_state_before.json", state_before)
        engine._write_jsonl(
            inflight / "logical_asks.jsonl",
            [_logical_row(row) for row in prepared.asks],
        )
        engine._write_json(
            inflight / "physical_union.json",
            {
                "schema_version": "cn_program_optimizer_d1_physical_union_v1",
                "policy_id": D1_POLICY_ID,
                "wave_index": wave,
                "logical_count": len(prepared.asks),
                "physical_exact_identities": list(prepared.physical_exact_identities),
                "selection_frozen_before_evaluation": True,
            },
        )
        schedules = _build_physical_schedules(
            prepared=prepared,
            authority=authority,
            start_ordinal=next_ordinal,
        )
        next_ordinal += len(schedules)
        engine._write_jsonl(inflight / "physical_schedules.jsonl", schedules)
        records = shared._evaluate_schedules(
            schedules,
            record_root=inflight / "records",
            authority=authority,
            input_hash=input_hash,
            executor_workers=int(args.executor_workers),
        )
        schedule_by_ordinal = {
            int(row["main_record_ordinal"]): row for row in schedules
        }
        physical_results: dict[str, PhysicalProgramResultV1] = {}
        physical_rows = []
        for record in records:
            schedule = schedule_by_ordinal[int(record["main_record_ordinal"])]
            result = shared._physical_result(record, schedule)
            if result.exact_identity in physical_results:
                raise RuntimeError("D1_PHYSICAL_RESULT_DUPLICATE")
            physical_results[result.exact_identity] = result
            physical_rows.append(
                _physical_result_record(
                    result,
                    source_record_sha256=str(record["record_payload_sha256"]),
                    wave_index=wave,
                )
            )
        engine._write_jsonl(inflight / "physical_results.jsonl", physical_rows)
        commit = cohort.commit_wave(prepared, physical_results)
        engine._write_json(inflight / "wave_commit_receipt.json", commit)
        engine._write_json(inflight / "cohort_state_after.json", cohort.snapshot())
        manifest_path = _close_wave(
            inflight=inflight,
            closed=closed,
            previous_manifest_sha256=previous_manifest_sha,
            wave_index=wave,
        )
        previous_manifest_sha = engine._sha256(manifest_path)
        wave_manifests.append(previous_manifest_sha)

    cohort.verify_terminal_shape()
    final_state = cohort.snapshot()
    metrics = _metrics(cohort)
    engine._write_json(root / "cohort_state_final.json", final_state)
    engine._write_json(root / "policy_metrics.json", metrics)
    wall_seconds = float(time.perf_counter() - wall_start)
    closure = engine._self_hashed(
        {
            "schema_version": "cn_program_optimizer_d1_development_complete_v1",
            "status": STATUS,
            "campaign_id": campaign_id,
            "campaign_profile": campaign_profile,
            "policy_id": D1_POLICY_ID,
            "repo_sha": repo_sha,
            "authorization_payload_sha256": str(
                authorization["authorization_payload_sha256"]
            ),
            "input_binding_sha256": input_hash,
            "program_space_count": FROZEN_PROGRAM_SPACE_COUNT,
            "program_space_sha256": FROZEN_PROGRAM_SPACE_SHA256,
            "prior_exact_count": prior_exact_count,
            "prior_exact_identities_sha256": prior_exact_identities_sha256,
            "remaining_prospective_enhanced_before_run": remaining_prospective,
            "logical_records": D1_LOGICAL_RECORDS,
            "physical_evaluation_calls": D1_LOGICAL_RECORDS,
            "closed_waves": TOTAL_WAVES,
            "selector_counts": cohort.selector_counts(),
            "policy_metrics": metrics,
            "wave_manifest_sha256": wave_manifests,
            "wall_seconds": wall_seconds,
            "resource_final": engine._runtime_resource_snapshot(),
            "restricted_reads": {
                "validation": 0,
                "holdout": 0,
                "historical_2023": 0,
                "forward_b": 0,
                "forward_2026": 0,
            },
            "oos_authority": "NONE",
            "promotion_authorized": False,
            "automatic_successor_authorized": False,
        },
        "closure_payload_sha256",
    )
    engine._write_json(root / CLOSURE_NAME, closure)
    return closure


def _prefinancial_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--executor-workers", type=int, default=8)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--prefinancial-only", action="store_true", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _prefinancial_parser().parse_args(argv)
    from our_system_phase2.runtime.cn_program_optimizer_d1_development_v1 import (
        verify_authorization,
    )

    authorization = verify_authorization(args.campaign_authorization)
    result = _run_prefinancial(
        args,
        authorization=authorization,
        repo_sha=str(args.repo_sha),
    )
    root = args.output_root.resolve()
    if root.exists():
        raise FileExistsError(root)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
