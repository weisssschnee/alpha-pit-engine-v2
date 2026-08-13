"""Freeze builder for the staged Program optimizer tournament.

Stage 0/1 reuses the accepted Search V2 prefinancial materialization.  Stage 2
is derived mechanically after the frozen racing boundary and carries forward
only state created inside the same tournament campaign.
"""

from __future__ import annotations

import json
from pathlib import Path
import shutil
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as phase_c
from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    FROZEN_PROGRAM_SPACE_ENTRY_COUNT,
    FROZEN_PROGRAM_SPACE_SHA256,
    FROZEN_PROGRAM_SPACE_SOURCE_SHA256,
    RECORDS_PER_CHECKPOINT,
    SEEDS,
    SURROGATE_CONFIG,
    TPE_CONFIG,
    build_maximum_ask_plan_v1,
)
from our_system_phase2.services.program_optimizer_tournament_v1 import (
    ProgramOptimizerTournamentV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    program_availability_entries_v1,
)
from our_system_phase2.services.search_v2_canary_freeze import (
    build_prefinancial_freeze_v1 as build_search_v2_freeze,
    verify_prefinancial_freeze_v1 as verify_search_v2_freeze,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


FREEZE_CLOSURE_NAME = "PROGRAM_OPTIMIZER_TOURNAMENT_PREFINANCIAL_FREEZE_COMPLETE.json"
FREEZE_SCHEMA = "cn_program_optimizer_tournament_prefinancial_freeze_v1"
FREEZE_STATUS = "PROGRAM_OPTIMIZER_TOURNAMENT_PHASE_FROZEN"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    return phase_c._write_json(path, payload)


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    return phase_c._write_jsonl(path, rows)


def _self_hashed(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    return phase_c._self_hashed(payload, field)


def _program_entries(
    *, reservoir_path: Path, component_path: Path, registry_path: Path
) -> tuple[Any, ...]:
    registry = UnifiedCapabilityRegistry.read(registry_path)
    catalog, _ = phase_c._build_catalog(
        reservoir=phase_c._read_jsonl(reservoir_path),
        component_rows=phase_c._read_jsonl(component_path),
        registry=registry,
    )
    by_identity = {
        stable_hash(dict(entry["program_genes"])): {
            "genes": dict(entry["program_genes"])
        }
        for template_id in sorted(catalog)
        for entry in catalog[template_id]
    }
    return program_availability_entries_v1(list(by_identity.values()))


def _optimizer_config() -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        {
            key: value
            for key, value in TPE_CONFIG.items()
            if key in {"n_startup_trials", "n_ei_candidates"}
        },
        {
            key: value
            for key, value in SURROGATE_CONFIG.items()
            if key
            in {
                "cold_start_asks",
                "candidate_pool_size",
                "n_estimators",
                "min_samples_leaf",
                "exploration_beta",
            }
        },
    )


def _renumber(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for ordinal, source in enumerate(rows):
        row = dict(source)
        row.pop("ask_record_sha256", None)
        row["main_record_ordinal"] = ordinal
        row["checkpoint_ordinal"] = ordinal // RECORDS_PER_CHECKPOINT
        row["generation_arm"] = str(row["optimizer_arm"])
        row["canary_profile"] = CAMPAIGN_PROFILE
        row["ask_record_sha256"] = stable_hash(row)
        output.append(row)
    if len(output) % RECORDS_PER_CHECKPOINT:
        raise ValueError("PROGRAM_TOURNAMENT_PHASE_NOT_CHECKPOINT_ALIGNED")
    return output


def stage01_asks_v1() -> list[dict[str, Any]]:
    return _renumber(
        [row for row in build_maximum_ask_plan_v1() if int(row["stage"]) <= 1]
    )


def stage2_asks_v1(active_arms: Sequence[str]) -> list[dict[str, Any]]:
    active = frozenset(map(str, active_arms))
    if not active or not active.issubset(SEEDS):
        raise ValueError("PROGRAM_TOURNAMENT_STAGE2_ACTIVE_ARM_DRIFT")
    return _renumber(
        [
            row
            for row in build_maximum_ask_plan_v1()
            if int(row["stage"]) == 2 and str(row["optimizer_arm"]) in active
        ]
    )


def _phase_freeze(
    *,
    output_root: Path,
    source_freeze_root: Path,
    asks: Sequence[Mapping[str, Any]],
    initial_state: Mapping[str, Any],
    program_space_hash: str,
    tournament_phase: str,
    active_arms: Sequence[str],
    repo_sha: str,
    prior_program_exact_identities: Sequence[str] = (),
) -> dict[str, Any]:
    root = output_root.resolve()
    if root.exists():
        raise FileExistsError(f"Program tournament freeze root exists: {root}")
    root.mkdir(parents=True)
    source = source_freeze_root.resolve()
    source_contract = _read_json(source / "phase_c_run_contract.json")
    copied = []
    for name in (
        "phase_c_raw_program_reservoir.jsonl",
        "phase_c_session_executable_component_pool.jsonl",
    ):
        target = root / name
        shutil.copy2(source / name, target)
        copied.append(target)
    ask_path = _write_jsonl(root / "phase_c_ask_plan.jsonl", asks)
    state_path = _write_json(root / "initial_bandit_state.json", initial_state)
    prior_identity_path = _write_json(
        root / "prior_program_exact_identities.json",
        _self_hashed(
            {
                "schema_version": "cn_program_tournament_prior_exact_v1",
                "exact_identities": sorted(
                    set(map(str, prior_program_exact_identities))
                ),
            },
            "prior_exact_payload_sha256",
        ),
    )
    template_counts = {
        template_id: sum(str(row["template_id"]) == template_id for row in asks)
        for template_id in {str(row["template_id"]) for row in asks}
    }
    enhanced_counts = [
        count for template_id, count in template_counts.items() if template_id != "BASE"
    ]
    minimum_base_identities = min(16, min(enhanced_counts))
    contract_body = {
        **{
            key: value
            for key, value in source_contract.items()
            if key != "run_contract_sha256"
        },
        "schema_version": "cn_program_optimizer_tournament_run_contract_v1",
        "status": "PROGRAM_OPTIMIZER_TOURNAMENT_INPUTS_FROZEN",
        "repo_sha": str(repo_sha),
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "tournament_phase": str(tournament_phase),
        "active_optimizer_arms": list(map(str, active_arms)),
        "main_record_count": len(asks),
        "checkpoint_count": len(asks) // RECORDS_PER_CHECKPOINT,
        "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
        "ask_plan_file_sha256": phase_c._sha256(ask_path),
        "ask_plan_sha256": stable_hash(list(asks)),
        "raw_program_reservoir_file_sha256": phase_c._sha256(copied[0]),
        "session_executable_component_pool_file_sha256": phase_c._sha256(
            copied[1]
        ),
        "initial_bandit_state_file_sha256": phase_c._sha256(state_path),
        "initial_bandit_state_payload_sha256": str(
            initial_state["bandit_state_sha256"]
        ),
        "prior_program_exact_identities_file_sha256": phase_c._sha256(
            prior_identity_path
        ),
        "prior_program_exact_identity_count": len(
            set(map(str, prior_program_exact_identities))
        ),
        "program_space_hash": str(program_space_hash),
        "program_space_same_for_all_arms": True,
        "template_quotas": dict(sorted(template_counts.items())),
        "arm_quotas": dict(
            sorted(
                {
                    arm: sum(str(row["optimizer_arm"]) == arm for row in asks)
                    for arm in {str(row["optimizer_arm"]) for row in asks}
                }.items()
            )
        ),
        "adaptive_budget_reallocation": str(tournament_phase) == "STAGE_2",
        "minimum_base_identities_per_template": minimum_base_identities,
        "development_observation_count": int(initial_state["observations"]),
        "same_campaign_optimizer_state_carried_forward": (
            str(tournament_phase) == "STAGE_2"
        ),
        "development_financial_observations_imported": False,
        "serialized_optimizer_state_imported": False,
        "candidate_results_imported": False,
        "fixed_arm_floors": True,
        "program_level_credit_only": True,
        "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
        "scalar_absolute_plus_uplift_reward": None,
        "financial_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    contract = _self_hashed(contract_body, "run_contract_sha256")
    contract_path = _write_json(root / "phase_c_run_contract.json", contract)
    artifacts = [
        *copied,
        ask_path,
        state_path,
        prior_identity_path,
        contract_path,
    ]
    manifest = _self_hashed(
        {
            "schema_version": "cn_program_optimizer_tournament_freeze_artifacts_v1",
            "artifacts": [phase_c._artifact(path, root=root) for path in artifacts],
        },
        "artifact_manifest_sha256",
    )
    manifest_path = _write_json(root / "ARTIFACT_MANIFEST.json", manifest)
    closure = _self_hashed(
        {
            "schema_version": FREEZE_SCHEMA,
            "status": FREEZE_STATUS,
            "repo_sha": str(repo_sha),
            "output_root": str(root),
            "campaign_id": CAMPAIGN_ID,
            "campaign_profile": CAMPAIGN_PROFILE,
            "tournament_phase": str(tournament_phase),
            "active_optimizer_arms": list(map(str, active_arms)),
            "main_record_count": len(asks),
            "checkpoint_count": len(asks) // RECORDS_PER_CHECKPOINT,
            "program_space_hash": str(program_space_hash),
            "artifact_manifest": phase_c._artifact(manifest_path, root=root),
            "financial_evaluation_executed": False,
            "market_price_rows_read": 0,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "tournament_run": False,
        },
        "closure_sha256",
    )
    return _read_json(_write_json(root / FREEZE_CLOSURE_NAME, closure))


def build_stage01_freeze_v1(
    *,
    output_root: Path,
    phase_b_freeze_root: Path,
    phase_b_result_root: Path,
    phase_b_outcome_path: Path,
    registry_path: Path,
    accepted_field_manifest_path: Path,
    information_metrics_path: Path,
    bar_source_root: Path,
    node_resource_capacity_path: Path,
    repo_sha: str,
    search_v2_authorization: Mapping[str, Any],
) -> dict[str, Any]:
    root = output_root.resolve()
    source = root.parent / "source_search_v2_prefinancial_freeze"
    build_search_v2_freeze(
        output_root=source,
        phase_b_freeze_root=phase_b_freeze_root,
        phase_b_result_root=phase_b_result_root,
        phase_b_outcome_path=phase_b_outcome_path,
        registry_path=registry_path,
        accepted_field_manifest_path=accepted_field_manifest_path,
        information_metrics_path=information_metrics_path,
        bar_source_root=bar_source_root,
        node_resource_capacity_path=node_resource_capacity_path,
        repo_sha=repo_sha,
        authorization=search_v2_authorization,
    )
    verify_search_v2_freeze(source)
    source_hashes = {
        "raw_program_reservoir": phase_c._sha256(
            source / "phase_c_raw_program_reservoir.jsonl"
        ),
        "session_executable_component_pool": phase_c._sha256(
            source / "phase_c_session_executable_component_pool.jsonl"
        ),
        "unified_capability_registry": phase_c._sha256(registry_path),
    }
    if source_hashes != FROZEN_PROGRAM_SPACE_SOURCE_SHA256:
        raise RuntimeError("PROGRAM_TOURNAMENT_FROZEN_SPACE_SOURCE_DRIFT")
    entries = _program_entries(
        reservoir_path=source / "phase_c_raw_program_reservoir.jsonl",
        component_path=source / "phase_c_session_executable_component_pool.jsonl",
        registry_path=registry_path,
    )
    realized_space_hash = stable_hash([entry.to_dict() for entry in entries])
    if (
        len(entries) != FROZEN_PROGRAM_SPACE_ENTRY_COUNT
        or realized_space_hash != FROZEN_PROGRAM_SPACE_SHA256
    ):
        raise RuntimeError("PROGRAM_TOURNAMENT_FROZEN_SPACE_IDENTITY_DRIFT")
    tpe_config, surrogate_config = _optimizer_config()
    state = ProgramOptimizerTournamentV1.fresh(
        campaign_id=CAMPAIGN_ID,
        entries_by_arm={arm: entries for arm in SEEDS},
        seeds=SEEDS,
        tpe_config=tpe_config,
        surrogate_config=surrogate_config,
    ).snapshot()
    if str(state["program_space_hash"]) != FROZEN_PROGRAM_SPACE_SHA256:
        raise RuntimeError("PROGRAM_TOURNAMENT_OPTIMIZER_SPACE_HASH_DRIFT")
    return _phase_freeze(
        output_root=root,
        source_freeze_root=source,
        asks=stage01_asks_v1(),
        initial_state=state,
        program_space_hash=str(state["program_space_hash"]),
        tournament_phase="STAGE_0_1",
        active_arms=tuple(SEEDS),
        repo_sha=repo_sha,
    )


def build_stage2_freeze_v1(
    *,
    output_root: Path,
    stage01_freeze_root: Path,
    final_stage01_state_path: Path,
    stage01_schedule_path: Path,
    active_arms: Sequence[str],
    repo_sha: str,
) -> dict[str, Any]:
    state = _read_json(final_stage01_state_path.resolve())
    schedule = phase_c._read_jsonl(stage01_schedule_path.resolve())
    prior_exact = [
        str(dict(row.get("optimizer_ask") or {})["exact_identity"])
        for row in schedule
    ]
    if len(prior_exact) != len(set(prior_exact)):
        raise ValueError("PROGRAM_TOURNAMENT_STAGE01_EXACT_DUPLICATE")
    return _phase_freeze(
        output_root=output_root,
        source_freeze_root=stage01_freeze_root,
        asks=stage2_asks_v1(active_arms),
        initial_state=state,
        program_space_hash=str(state["program_space_hash"]),
        tournament_phase="STAGE_2",
        active_arms=active_arms,
        repo_sha=repo_sha,
        prior_program_exact_identities=prior_exact,
    )


def verify_phase_freeze_v1(root: Path) -> dict[str, Any]:
    resolved = root.resolve()
    closure = _read_json(resolved / FREEZE_CLOSURE_NAME)
    body = dict(closure)
    expected = str(body.pop("closure_sha256", ""))
    if not expected or stable_hash(body) != expected:
        raise ValueError("Program tournament freeze closure self-hash drift")
    if closure.get("status") != FREEZE_STATUS or bool(
        closure.get("financial_evaluation_executed")
    ):
        raise ValueError("Program tournament freeze status drift")
    manifest_path = resolved / str(closure["artifact_manifest"]["relative_path"])
    manifest = _read_json(manifest_path)
    manifest_body = dict(manifest)
    manifest_hash = str(manifest_body.pop("artifact_manifest_sha256", ""))
    if not manifest_hash or stable_hash(manifest_body) != manifest_hash:
        raise ValueError("Program tournament freeze manifest self-hash drift")
    for artifact in manifest["artifacts"]:
        path = resolved / str(artifact["relative_path"])
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact["size_bytes"])
            or phase_c._sha256(path) != str(artifact["sha256"])
        ):
            raise ValueError(f"Program tournament freeze artifact drift: {path}")
    contract = _read_json(resolved / "phase_c_run_contract.json")
    contract_body = dict(contract)
    contract_hash = str(contract_body.pop("run_contract_sha256", ""))
    asks = phase_c._read_jsonl(resolved / "phase_c_ask_plan.jsonl")
    state = _read_json(resolved / "initial_bandit_state.json")
    prior_exact = _read_json(resolved / "prior_program_exact_identities.json")
    prior_body = dict(prior_exact)
    prior_hash = str(prior_body.pop("prior_exact_payload_sha256", ""))
    if (
        not contract_hash
        or stable_hash(contract_body) != contract_hash
        or stable_hash(list(asks)) != str(contract["ask_plan_sha256"])
        or len(asks) != int(contract["main_record_count"])
        or str(state["program_space_hash"]) != str(contract["program_space_hash"])
        or not prior_hash
        or stable_hash(prior_body) != prior_hash
        or len(prior_exact["exact_identities"])
        != int(contract["prior_program_exact_identity_count"])
        or len(prior_exact["exact_identities"])
        != len(set(prior_exact["exact_identities"]))
        or any(
            int(closure.get(key, 0))
            for key in (
                "market_price_rows_read",
                "validation_reads",
                "holdout_reads",
                "historical_2023_reads",
                "forward_b_reads",
                "forward_2026_reads",
            )
        )
    ):
        raise ValueError("Program tournament freeze contract drift")
    return closure
