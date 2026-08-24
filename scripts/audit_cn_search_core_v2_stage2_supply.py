"""Zero-financial supply audit for mature Search Core V2 Stage-2 continuation."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from scripts import run_cn_search_core_v2_stage2_v1 as stage2
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as stage_d_runtime
from our_system_phase2.runtime import cn_search_core_v2_stage2_v1 as runtime
from our_system_phase2.services.program_search_state_jump_adapter_v2 import StateJumpProgramSearchAdapterV2
from our_system_phase2.services.unified_capability_registry import stable_hash

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PER_TEMPLATE = 72


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _generated_supply_rows(
    state: StateJumpProgramSearchAdapterV2,
    *, authority: Mapping[str, Any], template: str, checkpoint_ordinal: int, global_start: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    asks = state.ask(
        checkpoint_id=f"SEARCH_CORE_V2_STAGE2_SUPPLY_{template}",
        count=PER_TEMPLATE,
        required_program_template_id=template,
        eligible_exact_identities=None,
        batch_group_constraint=None,
    )
    if len(asks) != PER_TEMPLATE:
        raise RuntimeError("STAGE2_SUPPLY_ASK_UNDERFILL")
    schedules: list[dict[str, Any]] = []
    for offset, optimizer_ask in enumerate(asks):
        generated = state.generated_for_proposal(str(optimizer_ask["proposal_id"]))
        reservoir = stage1._generated_reservoir(generated)
        entry = engine._catalog_entry(
            reservoir,
            components_by_id=authority["components_by_id"],
            adapter=authority["adapter"],
            compiler=authority["compiler"],
        )
        if entry.get("status") != "EXECUTABLE":
            raise RuntimeError("STAGE2_SUPPLY_GENERATED_NOT_EXECUTABLE")
        observed_genes = dict(entry["program_genes"])
        asked_genes = dict(optimizer_ask["program_genes"])
        if observed_genes != asked_genes:
            raise RuntimeError("STAGE2_SUPPLY_PROGRAM_GENE_DRIFT")
        exact = stage1.normalized_program_gene_identity_v1(
            observed_genes, ordered_slots=state.ordered_gene_slots
        )
        if exact != str(optimizer_ask["exact_identity"]):
            raise RuntimeError("STAGE2_SUPPLY_NORMALIZED_EXACT_DRIFT")
        ask_record = stage1._ask_record(
            global_ordinal=global_start + offset,
            checkpoint_ordinal=checkpoint_ordinal,
            template_id=template,
            template_ordinal=offset,
            arm=stage1.SEMANTIC_STATE_JUMP_GENERATOR_V2,
        )
        decision = engine._self_hashed(
            {
                "schema_version": "cn_search_core_v2_stage2_supply_selection_v1",
                "selection_mode": "ZERO_FINANCIAL_MATURE_STATE_SUPPLY_PROBE_72",
                "exact_identity": exact,
                "optimizer_proposal_id": str(optimizer_ask["proposal_id"]),
                "generator_summary": generated.summary(),
            },
            "selection_decision_sha256",
        )
        schedule = engine._schedule_record(
            ask_record,
            entry,
            decision,
            components_by_id=authority["components_by_id"],
            adapter=authority["adapter"],
            compiler=authority["compiler"],
        )
        schedule.update({
            "optimizer_ask": dict(optimizer_ask),
            "search_core_v2_arm": stage1.SEMANTIC_STATE_JUMP_GENERATOR_V2,
            "search_core_exact_identity": exact,
            "successor_exact_identity": exact,
            "generator_summary": generated.summary(),
            "optimizer_selection_used": True,
            "online_feedback_used": True,
            "search_core_stage": "STAGE2_SUPPLY_AUDIT",
        })
        schedule["schedule_record_sha256"] = stable_hash(
            {key: value for key, value in schedule.items() if key != "schedule_record_sha256"}
        )
        schedules.append(schedule)
    return schedules, asks


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo = PROJECT_ROOT.resolve()
    source_auth = stage_d_runtime.verify_authorization(
        args.source_stage_d_authorization.resolve(), repo_root=repo
    )
    pre = stage2.verify_prefreeze(args.stage2_prefreeze.resolve())
    provisional = {
        "authorization_payload_sha256": stable_hash({
            "role": "SEARCH_CORE_V2_STAGE2_ZERO_FINANCIAL_SUPPLY",
            "prefreeze": pre["prefreeze_payload_sha256"],
        }),
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "source_prior_exact": dict(source_auth["source_prior_exact"]),
    }
    args.executor_workers = runtime.PRIMARY_EXECUTOR_WORKERS
    authority = stage1._load_authority(
        args, authorization=provisional, repo_sha=str(args.repo_sha)
    )
    snapshot_path = repo / Path(str(pre["arm_b_state_jump"]["source_snapshot_relative_path"]))
    if engine._sha256(snapshot_path) != str(pre["arm_b_state_jump"]["source_snapshot_file_sha256"]):
        raise RuntimeError("STAGE2_SUPPLY_SNAPSHOT_FILE_DRIFT")
    snapshot = _read(snapshot_path)
    if str(snapshot.get("snapshot_hash") or "") != str(pre["arm_b_state_jump"]["source_snapshot_payload_sha256"]):
        raise RuntimeError("STAGE2_SUPPLY_SNAPSHOT_PAYLOAD_DRIFT")

    schedules: list[dict[str, Any]] = []
    operations: Counter[str] = Counter()
    by_template: dict[str, int] = {}
    global_ordinal = 0
    # No synthetic tell(): each template independently probes 72 fresh Programs
    # from the exact mature Stage-1.5 snapshot.
    for checkpoint_ordinal, template in enumerate(stage1.TEMPLATES):
        state = StateJumpProgramSearchAdapterV2.restore(
            snapshot=snapshot,
            adapter=authority["adapter"],
            compiler=authority["compiler"],
            components_by_role=stage1._components_by_role(authority),
            seed=0,
        )
        rows, asks = _generated_supply_rows(
            state,
            authority=authority,
            template=template,
            checkpoint_ordinal=checkpoint_ordinal,
            global_start=global_ordinal,
        )
        global_ordinal += len(rows)
        schedules.extend(rows)
        by_template[template] = len(rows)
        operations.update(
            str(
                dict(dict(ask.get("acquisition") or {}).get("generator_summary") or {}).get("operation")
                or "UNKNOWN"
            )
            for ask in asks
        )

    exacts = [str(schedule["search_core_exact_identity"]) for schedule in schedules]
    arm_a_exacts = set(map(str, pre["arm_a_primitive"]["selected_exact_identities"]))
    if len(exacts) != 504 or len(set(exacts)) != 504:
        raise RuntimeError("STAGE2_SUPPLY_GENERATED_EXACT_DRIFT")
    overlap = len(set(exacts) & arm_a_exacts)
    if overlap:
        raise RuntimeError("STAGE2_SUPPLY_ARM_OVERLAP")
    if any(count != PER_TEMPLATE for count in by_template.values()) or len(by_template) != 7:
        raise RuntimeError("STAGE2_SUPPLY_TEMPLATE_UNDERFILL")
    if "UNKNOWN" in operations:
        raise RuntimeError("STAGE2_SUPPLY_OPERATION_RECEIPT_DRIFT")
    fields = tuple(sorted(map(str, engine._checkpoint_field_columns(schedules))))
    if not fields:
        raise RuntimeError("STAGE2_SUPPLY_FIELD_SURFACE_EMPTY")

    payload = {
        "schema_version": "cn_search_core_v2_stage2_mature_supply_audit_v1",
        "status": "PASS_ZERO_FINANCIAL_MATURE_STATE_JUMP_STAGE2_SUPPLY_AUDIT",
        "repo_sha": str(args.repo_sha),
        "prefreeze_payload_sha256": str(pre["prefreeze_payload_sha256"]),
        "source_snapshot_payload_sha256": str(snapshot["snapshot_hash"]),
        "generated_total": len(exacts),
        "unique_exact_count": len(set(exacts)),
        "generated_exact_identities_sha256": stable_hash(sorted(exacts)),
        "arm_a_overlap_count": overlap,
        "per_template_generated": dict(sorted(by_template.items())),
        "operation_counts": dict(sorted(operations.items())),
        "resource_field_surface": {
            "field_column_count": len(fields),
            "field_columns": list(fields),
            "field_columns_sha256": stable_hash(list(fields)),
        },
        "candidate_evaluation_executed": False,
        "financial_sidecar_read": False,
        "restricted_reads": {
            "validation": 0, "holdout": 0, "historical_2023": 0,
            "forward_b": 0, "forward_2026": 0,
        },
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-stage-d-authorization", type=Path, required=True)
    parser.add_argument("--stage2-prefreeze", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = run(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    engine._write_json(args.output, payload)
    print(json.dumps({
        "status": payload["status"],
        "generated_total": payload["generated_total"],
        "payload": payload["audit_payload_sha256"],
        "output": str(args.output.resolve()),
    }, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())