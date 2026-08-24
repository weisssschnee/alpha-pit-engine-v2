"""Zero-financial checkpoint-local supply audit for Search Core V2 Stage-2."""
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
SOURCE_STAGE1_STATE = Path("runtime/run_plans/cn_search_core_v2_stage1_final_optimizer_state_47bc94f_20260824.json")
BASE_EVENT_ROBUSTNESS_BATCH = 12


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_snapshot(payload: Mapping[str, Any], label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop("snapshot_hash", ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} snapshot self-hash drift")
    return claimed


def _restore(
    snapshot: Mapping[str, Any], *, authority: Mapping[str, Any]
) -> StateJumpProgramSearchAdapterV2:
    return StateJumpProgramSearchAdapterV2.restore(
        snapshot=snapshot,
        adapter=authority["adapter"],
        compiler=authority["compiler"],
        components_by_role=stage1._components_by_role(authority),
        seed=0,
    )


def _probe_batch(
    snapshot: Mapping[str, Any],
    *,
    authority: Mapping[str, Any],
    template: str,
    batch_size: int,
    checkpoint_id: str,
    checkpoint_ordinal: int,
    global_start: int,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    state = _restore(snapshot, authority=authority)
    schedules = stage2._generated_schedules_batch(
        state,
        authority=authority,
        template_id=template,
        checkpoint_id=checkpoint_id,
        checkpoint_ordinal=checkpoint_ordinal,
        global_start=global_start,
        template_start=0,
        batch_size=batch_size,
    )
    operations: Counter[str] = Counter(
        str(dict(row["generator_summary"])["operation"]) for row in schedules
    )
    return schedules, operations


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo = PROJECT_ROOT.resolve()
    source_auth = stage_d_runtime.verify_authorization(
        args.source_stage_d_authorization.resolve(), repo_root=repo
    )
    prefreeze = stage2.verify_prefreeze(args.stage2_prefreeze.resolve())
    provisional = {
        "authorization_payload_sha256": stable_hash(
            {
                "role": "SEARCH_CORE_V2_STAGE2_ZERO_FINANCIAL_CHECKPOINT_SUPPLY",
                "prefreeze": prefreeze["prefreeze_payload_sha256"],
            }
        ),
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "source_prior_exact": dict(source_auth["source_prior_exact"]),
    }
    args.executor_workers = runtime.PRIMARY_EXECUTOR_WORKERS
    authority = stage1._load_authority(
        args,
        authorization=provisional,
        repo_sha=str(args.repo_sha),
    )

    source_snapshot_path = repo / Path(
        str(prefreeze["arm_b_state_jump"]["source_snapshot_relative_path"])
    )
    if engine._sha256(source_snapshot_path) != str(
        prefreeze["arm_b_state_jump"]["source_snapshot_file_sha256"]
    ):
        raise RuntimeError("STAGE2_SUPPLY_SOURCE_SNAPSHOT_FILE_DRIFT")
    source_snapshot = _read(source_snapshot_path)
    source_snapshot_hash = _verify_snapshot(source_snapshot, "Stage-2 source")
    if source_snapshot_hash != str(
        prefreeze["arm_b_state_jump"]["source_snapshot_payload_sha256"]
    ):
        raise RuntimeError("STAGE2_SUPPLY_SOURCE_SNAPSHOT_PAYLOAD_DRIFT")

    batch_map = {
        str(key): int(value)
        for key, value in dict(prefreeze["stage2"]["template_batch_size"]).items()
    }
    expected_batch_map = {
        template: (12 if template == "BASE_EVENT" else 24)
        for template in stage1.TEMPLATES
    }
    if batch_map != expected_batch_map:
        raise RuntimeError("STAGE2_SUPPLY_TEMPLATE_BATCH_CONTRACT_DRIFT")

    schedules: list[dict[str, Any]] = []
    operation_counts: Counter[str] = Counter()
    per_template_generated: dict[str, int] = {}
    global_ordinal = 0
    for checkpoint_ordinal, template in enumerate(stage1.TEMPLATES):
        batch_size = batch_map[template]
        rows, operations = _probe_batch(
            source_snapshot,
            authority=authority,
            template=template,
            batch_size=batch_size,
            checkpoint_id=f"SEARCH_CORE_V2_STAGE2_SUPPLY_SOURCE_{template}",
            checkpoint_ordinal=checkpoint_ordinal,
            global_start=global_ordinal,
        )
        global_ordinal += len(rows)
        schedules.extend(rows)
        operation_counts.update(operations)
        per_template_generated[template] = len(rows)

    expected_probe_total = sum(batch_map.values())
    exacts = [str(schedule["search_core_exact_identity"]) for schedule in schedules]
    arm_a_exacts = set(
        map(str, prefreeze["arm_a_primitive"]["selected_exact_identities"])
    )
    if (
        expected_probe_total != 156
        or len(exacts) != expected_probe_total
        or len(set(exacts)) != expected_probe_total
    ):
        raise RuntimeError("STAGE2_SUPPLY_SOURCE_PROBE_EXACT_DRIFT")
    arm_a_overlap = len(set(exacts) & arm_a_exacts)
    if arm_a_overlap:
        raise RuntimeError("STAGE2_SUPPLY_SOURCE_PROBE_ARM_OVERLAP")
    if per_template_generated != batch_map:
        raise RuntimeError("STAGE2_SUPPLY_SOURCE_PROBE_TEMPLATE_DRIFT")
    if "UNKNOWN" in operation_counts:
        raise RuntimeError("STAGE2_SUPPLY_OPERATION_RECEIPT_DRIFT")

    # Robustness evidence for the only template whose mature ask-24 supply was
    # state-dependent.  No synthetic tell is introduced: every state is restored
    # independently and probed for the frozen BASE_EVENT microbatch of 12.
    stage15_root = args.stage15_run_root.resolve()
    historical_paths: list[tuple[str, Path]] = [
        ("stage1_final", repo / SOURCE_STAGE1_STATE),
    ]
    historical_paths.extend(
        (
            f"stage15_checkpoint_{checkpoint:04d}",
            stage15_root / f"checkpoint_{checkpoint:04d}" / "optimizer_state_after.json",
        )
        for checkpoint in (1, 3, 5, 7, 9, 11, 13)
    )
    robustness: dict[str, Any] = {}
    for ordinal, (label, path) in enumerate(historical_paths):
        if not path.is_file():
            raise FileNotFoundError(path)
        snapshot = _read(path)
        snapshot_hash = _verify_snapshot(snapshot, label)
        rows, operations = _probe_batch(
            snapshot,
            authority=authority,
            template="BASE_EVENT",
            batch_size=BASE_EVENT_ROBUSTNESS_BATCH,
            checkpoint_id=f"SEARCH_CORE_V2_STAGE2_ROBUSTNESS_{label}",
            checkpoint_ordinal=100 + ordinal,
            global_start=10000 + ordinal * BASE_EVENT_ROBUSTNESS_BATCH,
        )
        robustness[label] = {
            "path": str(path),
            "file_sha256": engine._sha256(path),
            "snapshot_payload_sha256": snapshot_hash,
            "history_count": len(list(snapshot.get("history") or ())),
            "generated_exact_count": len(
                list(snapshot.get("generated_exact_identities") or ())
            ),
            "requested": BASE_EVENT_ROBUSTNESS_BATCH,
            "generated": len(rows),
            "unique_exact_count": len(
                {str(row["search_core_exact_identity"]) for row in rows}
            ),
            "operation_counts": dict(sorted(operations.items())),
            "status": "PASS",
        }
        if (
            robustness[label]["generated"] != BASE_EVENT_ROBUSTNESS_BATCH
            or robustness[label]["unique_exact_count"]
            != BASE_EVENT_ROBUSTNESS_BATCH
        ):
            raise RuntimeError("STAGE2_BASE_EVENT_MICROBATCH_ROBUSTNESS_DRIFT")

    fields = tuple(sorted(map(str, engine._checkpoint_field_columns(schedules))))
    if not fields:
        raise RuntimeError("STAGE2_SUPPLY_FIELD_SURFACE_EMPTY")

    payload = {
        "schema_version": "cn_search_core_v2_stage2_checkpoint_supply_audit_v1",
        "status": "PASS_ZERO_FINANCIAL_MATURE_STATE_JUMP_STAGE2_CHECKPOINT_SUPPLY_AUDIT",
        "repo_sha": str(args.repo_sha),
        "prefreeze_payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
        "source_snapshot_payload_sha256": source_snapshot_hash,
        "supply_scope": "CHECKPOINT_LOCAL_FROM_EXACT_SOURCE_PLUS_HISTORICAL_MATURE_STATE_ROBUSTNESS",
        "template_batch_size": dict(batch_map),
        "generated_total": len(exacts),
        "unique_exact_count": len(set(exacts)),
        "generated_exact_identities_sha256": stable_hash(sorted(exacts)),
        "arm_a_overlap_count": arm_a_overlap,
        "per_template_generated": dict(sorted(per_template_generated.items())),
        "operation_counts": dict(sorted(operation_counts.items())),
        "base_event_microbatch_robustness": robustness,
        "base_event_microbatch_robustness_state_count": len(robustness),
        "base_event_microbatch_robustness_batch_size": BASE_EVENT_ROBUSTNESS_BATCH,
        "synthetic_tell_used": False,
        "future_checkpoint_supply_fail_closed": True,
        "resource_field_surface": {
            "field_column_count": len(fields),
            "field_columns": list(fields),
            "field_columns_sha256": stable_hash(list(fields)),
        },
        "candidate_evaluation_executed": False,
        "financial_sidecar_read": False,
        "restricted_reads": {
            "validation": 0,
            "holdout": 0,
            "historical_2023": 0,
            "forward_b": 0,
            "forward_2026": 0,
        },
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-stage-d-authorization", type=Path, required=True)
    parser.add_argument("--stage2-prefreeze", type=Path, required=True)
    parser.add_argument("--stage15-run-root", type=Path, required=True)
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
    print(
        json.dumps(
            {
                "status": payload["status"],
                "generated_total": payload["generated_total"],
                "robustness_states": payload[
                    "base_event_microbatch_robustness_state_count"
                ],
                "payload": payload["audit_payload_sha256"],
                "output": str(args.output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())