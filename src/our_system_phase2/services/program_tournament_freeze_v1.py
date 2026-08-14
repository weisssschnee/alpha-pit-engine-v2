"""Freeze builder for the staged Program optimizer tournament.

Stage 0/1 reuses the accepted Search V2 prefinancial materialization.  Stage 2
is derived mechanically after the frozen racing boundary and carries forward
only state created inside the same tournament campaign.
"""

from __future__ import annotations

import json
import os
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
    authorization_payload_v1,
    build_maximum_ask_plan_v1,
)
from our_system_phase2.services.program_optimizer_tournament_v1 import (
    ProgramOptimizerTournamentV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    program_availability_entries_v1,
)
from our_system_phase2.services.search_v2_canary_freeze import (
    verify_prefinancial_freeze_v1 as verify_search_v2_freeze,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


FREEZE_CLOSURE_NAME = "PROGRAM_OPTIMIZER_TOURNAMENT_PREFINANCIAL_FREEZE_COMPLETE.json"
FREEZE_SCHEMA = "cn_program_optimizer_tournament_prefinancial_freeze_v1"
FREEZE_STATUS = "PROGRAM_OPTIMIZER_TOURNAMENT_PHASE_FROZEN"
SOURCE_BINDING_SCHEMA = "cn_program_optimizer_tournament_source_binding_v1"
SOURCE_BINDING_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_tournament_source_binding_v1.json"
)
SOURCE_FREEZE_ROOT = Path(
    r"D:\ChengboRemote\runtime\cn_search_engine_v2_canary_replacement_20260812_1b91c88\prefinancial_freeze"
)
SOURCE_FREEZE_CLOSURE_NAME = (
    "SEARCH_ENGINE_V2_CANARY_PREFINANCIAL_FREEZE_COMPLETE.json"
)
SOURCE_FREEZE_CLOSURE_FILE_SHA256 = (
    "ba4a5423dc73a4e73a0782c40b64efbbce3fa8e5d78a6a6464f7343568dfa83d"
)
SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256 = (
    "94c28e5d1117904e80c255b5ef01b8cbf8f5d3e4681fc5910e4129ee1b7c9c44"
)
SOURCE_FREEZE_MANIFEST_FILE_SHA256 = (
    "7c42148769203d9ca0fc4c2fc4a4404a12eb489b9aaa8da53dc49e5d341876da"
)
SOURCE_FREEZE_MANIFEST_PAYLOAD_SHA256 = (
    "ccf9efdbc36b040b9fa24555471a2d2c9d614e3c8bf217b076a75eac883e8bf1"
)
SOURCE_REGISTRY_PATH = Path(
    r"D:\ChengboRemote\workspace\alpha_pit_search_v2_1b91c88_20260812\runtime\field_registry\cn_unified_capability_registry_v3_20260717\unified_capability_registry.json"
)
SOURCE_REGISTRY_REPOSITORY_RELATIVE_PATH = Path(
    "runtime/field_registry/cn_unified_capability_registry_v3_20260717/"
    "unified_capability_registry.json"
)
SOURCE_REGISTRY_SHA256 = (
    "449fea36daaba8e501bd03d052497b881ac03c601cee701f3ebfe069c7ae61d7"
)
SOURCE_NODE_CAPACITY_PATH = Path(
    r"D:\ChengboRemote\workspace\alpha_pit_search_v2_1b91c88_20260812\runtime\run_plans\cn_alpha_node_resource_profiles_v1.json"
)
SOURCE_NODE_CAPACITY_SHA256 = (
    "48abcebe9324fdfdb4923b5de5b1e85fff28ca310e80ff90f22b0c403bb308a5"
)
SOURCE_ACCEPTED_FIELD_MANIFEST_PATH = Path(
    r"D:\ChengboRemote\runtime\cn_program_materialization_preflight_v1_information_qualified_20260808_3fec6ad\session_time_major_train_v1\CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
)
SOURCE_ACCEPTED_FIELD_MANIFEST_SHA256 = (
    "0f433704de8cb8818308d0fed0f81b80ae4d9ac666828c5db5992096b8bbfceb"
)
SOURCE_PHASE_B_FREEZE_ROOT = Path(
    r"D:\ChengboRemote\runtime\cn_joint_program_rolling_search_v0_phase_b_freeze_materialization_closed_20260808_3fec6ad"
)
SOURCE_PHASE_B_RESULT_ROOT = Path(
    r"D:\ChengboRemote\runtime\cn_joint_program_rolling_search_v0_phase_b_blocker_safe_recovery_20260809_bd771fd_10w"
)
SOURCE_PHASE_B_CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_B_COMPLETE.json"
SOURCE_PHASE_B_CLOSURE_FILE_SHA256 = (
    "14623ebc131b84660bd131b11268e40433ea1f219d1ed77caff2eb6ab147106a"
)
SOURCE_PHASE_B_CLOSURE_PAYLOAD_SHA256 = (
    "0765c684c3f67ab592eb1006dbd1a5dd6e96dad7825fef251ac0d4f6ee36f3cf"
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    return phase_c._write_json(path, payload)


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    return phase_c._write_jsonl(path, rows)


def _self_hashed(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    return phase_c._self_hashed(payload, field)


def _same_path(actual: str | Path, expected: str | Path) -> bool:
    return os.path.normcase(os.path.normpath(str(actual))) == os.path.normcase(
        os.path.normpath(str(expected))
    )


def _verify_bound_file(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file() or phase_c._sha256(path) != str(expected_sha256):
        raise ValueError(f"Program tournament source binding {label} drift")


def _artifact_path(binding: Mapping[str, Any]) -> str:
    value = str(binding.get("relative_path") or binding.get("path") or "")
    if not value:
        raise ValueError("Program tournament artifact path drift")
    return value


def _artifact_size(binding: Mapping[str, Any]) -> int:
    value = binding.get("size_bytes")
    if value is None:
        value = binding.get("bytes")
    if value is None or int(value) < 0:
        raise ValueError("Program tournament artifact size drift")
    return int(value)


def verify_source_binding_v1(
    binding_path: Path, *, repository_root: Path | None = None
) -> dict[str, Any]:
    payload = _read_json(binding_path.resolve())
    body = dict(payload)
    expected = str(body.pop("source_binding_payload_sha256", ""))
    if not expected or stable_hash(body) != expected:
        raise ValueError("Program tournament source binding self-hash drift")
    if payload.get("schema_version") != SOURCE_BINDING_SCHEMA:
        raise ValueError("Program tournament source binding schema drift")

    source = dict(payload["source_search_v2_prefinancial_freeze"])
    registry = dict(payload["registry_authority"])
    capacity = dict(payload["node_resource_capacity_authority"])
    accepted = dict(payload["accepted_field_manifest"])
    phase_b = dict(payload["phase_b_binding"])
    program_space = dict(payload["program_space"])
    exact_values = (
        (source["root"], SOURCE_FREEZE_ROOT, "freeze root"),
        (
            source["closure"]["relative_path"],
            SOURCE_FREEZE_CLOSURE_NAME,
            "closure path",
        ),
        (
            source["closure"]["file_sha256"],
            SOURCE_FREEZE_CLOSURE_FILE_SHA256,
            "closure file SHA",
        ),
        (
            source["closure"]["payload_sha256"],
            SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256,
            "closure payload SHA",
        ),
        (
            source["artifact_manifest"]["file_sha256"],
            SOURCE_FREEZE_MANIFEST_FILE_SHA256,
            "artifact manifest file SHA",
        ),
        (
            source["artifact_manifest"]["payload_sha256"],
            SOURCE_FREEZE_MANIFEST_PAYLOAD_SHA256,
            "artifact manifest payload SHA",
        ),
        (registry["source_path"], SOURCE_REGISTRY_PATH, "registry source path"),
        (
            registry["repository_relative_path"],
            SOURCE_REGISTRY_REPOSITORY_RELATIVE_PATH,
            "registry repository path",
        ),
        (registry["sha256"], SOURCE_REGISTRY_SHA256, "registry SHA"),
        (
            capacity["source_path"],
            SOURCE_NODE_CAPACITY_PATH,
            "node-capacity source path",
        ),
        (capacity["sha256"], SOURCE_NODE_CAPACITY_SHA256, "node-capacity SHA"),
        (
            accepted["path"],
            SOURCE_ACCEPTED_FIELD_MANIFEST_PATH,
            "accepted manifest path",
        ),
        (
            accepted["sha256"],
            SOURCE_ACCEPTED_FIELD_MANIFEST_SHA256,
            "accepted manifest SHA",
        ),
        (phase_b["freeze_root"], SOURCE_PHASE_B_FREEZE_ROOT, "Phase B freeze root"),
        (phase_b["result_root"], SOURCE_PHASE_B_RESULT_ROOT, "Phase B result root"),
        (
            phase_b["result_closure"]["relative_path"],
            SOURCE_PHASE_B_CLOSURE_NAME,
            "Phase B closure path",
        ),
        (
            phase_b["result_closure"]["file_sha256"],
            SOURCE_PHASE_B_CLOSURE_FILE_SHA256,
            "Phase B closure file SHA",
        ),
        (
            phase_b["result_closure"]["payload_sha256"],
            SOURCE_PHASE_B_CLOSURE_PAYLOAD_SHA256,
            "Phase B closure payload SHA",
        ),
        (
            program_space["entry_count"],
            FROZEN_PROGRAM_SPACE_ENTRY_COUNT,
            "Program-space count",
        ),
        (
            program_space["sha256"],
            FROZEN_PROGRAM_SPACE_SHA256,
            "Program-space SHA",
        ),
        (
            payload["maximum_ask_plan_sha256"],
            stable_hash(list(build_maximum_ask_plan_v1())),
            "maximum ask-plan SHA",
        ),
        (
            payload["tournament_authorization_payload_sha256"],
            authorization_payload_v1()["authorization_payload_sha256"],
            "Tournament authorization payload SHA",
        ),
    )
    for actual, expected_value, label in exact_values:
        if isinstance(expected_value, Path):
            matches = _same_path(actual, expected_value)
        else:
            matches = actual == expected_value
        if not matches:
            raise ValueError(f"Program tournament source binding {label} drift")
    if (
        int(source["required_physical_leaf_count"]) != 84
        or int(source["available_after_materialization_count"]) != 84
        or int(source["unresolved_required_field_count"]) != 0
        or bool(source["financial_evaluation_executed"])
        or any(
            int(payload["restricted_reads"][key])
            for key in (
                "validation",
                "holdout",
                "historical_2023",
                "forward_b",
                "forward_2026",
            )
        )
    ):
        raise ValueError("Program tournament source binding access boundary drift")

    source_root = Path(str(source["root"]))
    closure_path = source_root / str(source["closure"]["relative_path"])
    manifest_path = source_root / "ARTIFACT_MANIFEST.json"
    registry_source = Path(str(registry["source_path"]))
    capacity_source = Path(str(capacity["source_path"]))
    accepted_path = Path(str(accepted["path"]))
    phase_b_freeze_root = Path(str(phase_b["freeze_root"]))
    phase_b_result_root = Path(str(phase_b["result_root"]))
    phase_b_closure_path = phase_b_result_root / str(
        phase_b["result_closure"]["relative_path"]
    )
    if not phase_b_freeze_root.is_dir() or not phase_b_result_root.is_dir():
        raise ValueError("Program tournament source binding Phase B root drift")
    _verify_bound_file(
        closure_path, SOURCE_FREEZE_CLOSURE_FILE_SHA256, "closure file"
    )
    _verify_bound_file(
        manifest_path, SOURCE_FREEZE_MANIFEST_FILE_SHA256, "artifact manifest"
    )
    _verify_bound_file(registry_source, SOURCE_REGISTRY_SHA256, "registry")
    _verify_bound_file(capacity_source, SOURCE_NODE_CAPACITY_SHA256, "node capacity")
    _verify_bound_file(
        accepted_path, SOURCE_ACCEPTED_FIELD_MANIFEST_SHA256, "accepted manifest"
    )
    _verify_bound_file(
        phase_b_closure_path,
        SOURCE_PHASE_B_CLOSURE_FILE_SHA256,
        "Phase B closure",
    )
    phase_b_closure = _read_json(phase_b_closure_path)
    if (
        phase_b_closure.get("closure_payload_sha256")
        != SOURCE_PHASE_B_CLOSURE_PAYLOAD_SHA256
    ):
        raise ValueError("Program tournament source binding Phase B payload drift")

    verified_source = verify_search_v2_freeze(source_root)
    if (
        verified_source.get("closure_sha256")
        != SOURCE_FREEZE_CLOSURE_PAYLOAD_SHA256
        or int(verified_source.get("required_physical_leaf_count", -1)) != 84
        or int(verified_source.get("available_after_materialization_count", -1))
        != 84
        or int(verified_source.get("unresolved_required_field_count", -1)) != 0
    ):
        raise ValueError("Program tournament verified Search V2 freeze drift")
    source_contract = _read_json(source_root / "phase_c_run_contract.json")
    contract_values = (
        (source_contract["registry_path"], SOURCE_REGISTRY_PATH),
        (source_contract["registry_file_sha256"], SOURCE_REGISTRY_SHA256),
        (
            source_contract["node_resource_capacity_path"],
            SOURCE_NODE_CAPACITY_PATH,
        ),
        (
            source_contract["node_resource_capacity_file_sha256"],
            SOURCE_NODE_CAPACITY_SHA256,
        ),
        (
            source_contract["source_accepted_field_manifest_path"],
            SOURCE_ACCEPTED_FIELD_MANIFEST_PATH,
        ),
        (
            source_contract["source_accepted_field_manifest_file_sha256"],
            SOURCE_ACCEPTED_FIELD_MANIFEST_SHA256,
        ),
        (source_contract["phase_b_freeze_root"], SOURCE_PHASE_B_FREEZE_ROOT),
        (source_contract["phase_b_result_root"], SOURCE_PHASE_B_RESULT_ROOT),
    )
    if any(
        not (_same_path(actual, expected_value) if isinstance(expected_value, Path) else actual == expected_value)
        for actual, expected_value in contract_values
    ):
        raise ValueError("Program tournament Search V2 run-contract source drift")

    source_hashes = {
        "raw_program_reservoir": phase_c._sha256(
            source_root / "phase_c_raw_program_reservoir.jsonl"
        ),
        "session_executable_component_pool": phase_c._sha256(
            source_root / "phase_c_session_executable_component_pool.jsonl"
        ),
        "unified_capability_registry": phase_c._sha256(registry_source),
    }
    if source_hashes != FROZEN_PROGRAM_SPACE_SOURCE_SHA256 or source_hashes != dict(
        program_space["source_sha256"]
    ):
        raise ValueError("Program tournament frozen-space source drift")
    repo_root = (repository_root or Path(__file__).resolve().parents[3]).resolve()
    repository_registry = repo_root / str(registry["repository_relative_path"])
    _verify_bound_file(
        repository_registry, SOURCE_REGISTRY_SHA256, "repository registry"
    )
    entries = _program_entries(
        reservoir_path=source_root / "phase_c_raw_program_reservoir.jsonl",
        component_path=source_root / "phase_c_session_executable_component_pool.jsonl",
        registry_path=registry_source,
    )
    realized_space_hash = stable_hash([entry.to_dict() for entry in entries])
    if (
        len(entries) != FROZEN_PROGRAM_SPACE_ENTRY_COUNT
        or realized_space_hash != FROZEN_PROGRAM_SPACE_SHA256
    ):
        raise ValueError("Program tournament frozen-space identity drift")
    return {
        "payload": payload,
        "source_freeze_root": source_root,
        "registry_path": registry_source,
        "node_resource_capacity_path": capacity_source,
        "accepted_field_manifest_path": accepted_path,
        "program_entries": entries,
        "program_space_sha256": realized_space_hash,
    }


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
    template_counter: dict[str, int] = {}
    for ordinal, source in enumerate(rows):
        row = dict(source)
        row.pop("ask_record_sha256", None)
        template_id = str(row["template_id"])
        row["main_record_ordinal"] = ordinal
        row["checkpoint_ordinal"] = ordinal // RECORDS_PER_CHECKPOINT
        row["template_record_ordinal"] = template_counter.get(template_id, 0)
        template_counter[template_id] = row["template_record_ordinal"] + 1
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
    source_binding_path: Path,
    repo_sha: str,
) -> dict[str, Any]:
    root = output_root.resolve()
    binding = verify_source_binding_v1(source_binding_path)
    source = Path(binding["source_freeze_root"])
    entries = tuple(binding["program_entries"])
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
    manifest_path = resolved / _artifact_path(closure["artifact_manifest"])
    manifest = _read_json(manifest_path)
    manifest_body = dict(manifest)
    manifest_hash = str(manifest_body.pop("artifact_manifest_sha256", ""))
    if not manifest_hash or stable_hash(manifest_body) != manifest_hash:
        raise ValueError("Program tournament freeze manifest self-hash drift")
    for artifact in manifest["artifacts"]:
        path = resolved / _artifact_path(artifact)
        if (
            not path.is_file()
            or path.stat().st_size != _artifact_size(artifact)
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
