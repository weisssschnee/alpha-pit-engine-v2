from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import asdict
import gc
import json
from pathlib import Path
import platform
import statistics
import time
from typing import Any, Mapping, MutableMapping, Sequence

import numpy as np
import psutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_b_v0 as phase_b
from scripts import run_cn_portfolio_decoder_v2 as decoder_v2
from our_system_phase2.runtime.cn_iterative_search_v1 import _artifact, _batch_manifest
from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import TEMPLATE_ORDER
from our_system_phase2.runtime.cn_joint_program_phase_c_v0 import (
    CHECKPOINT_COUNT,
    ENHANCED_TEMPLATE_ORDER,
    EXPECTED_RECORDS,
    FREEZE_CLOSURE_NAME,
    PHASE_C_BATCH_ID,
    RECORDS_PER_CHECKPOINT,
    _feedback_quality_inputs,
    verify_phase_c_prefinancial_freeze_v0,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    PROGRAM_TEMPLATE_COMPONENTS,
    CandidateProgramProposalAdapterV0,
    ProgramProposalReceiptV0,
    ProgramSourceComponentV0,
)
from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_v1 import (
    ProgramCompilerV1,
    legacy_candidate_program_v1,
)
from our_system_phase2.services.program_factorized_bandit_v0 import (
    ProgramFactorizedBanditV0,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    program_structural_genes_v1,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
STATUS = "CN_JOINT_PROGRAM_PHASE_C_COMPLETE"
CLOSURE_NAME = "CN_JOINT_PROGRAM_PHASE_C_COMPLETE.json"
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
MINIMUM_COMMIT_HEADROOM_BYTES = 24 * 1024**3
PROJECTED_WORKER_COMMIT_BUDGET_BYTES = 8 * 1024**3
CHECKPOINT_WORKER_CHOICES = (4, 6, 8)
MANDATORY_CHECKPOINT_FIELD_COLUMNS = ("trade_time", "code", "close")
MAX_VARIANTS_PER_BASE_PER_TEMPLATE = 4
MIN_BASE_IDENTITIES_PER_TEMPLATE = 16
CATALOG_MIN_RECORDS_PER_TEMPLATE = 64
PAIR_REPLAY_COMPLETE = phase_b.PAIR_REPLAY_COMPLETE
PAIR_REPLAY_BLOCKED = phase_b.PAIR_REPLAY_BLOCKED
CHECKPOINT_RECOVERY_EXECUTOR_MODE = "RECOVERY_ISOLATED_FIRST_CHECKPOINT"
CHECKPOINT_ADAPTIVE_EXECUTOR_MODE = "NORMAL_ADAPTIVE"
CHECKPOINT_POST_RECOVERY_EXECUTOR_MODE = (
    "NORMAL_ADAPTIVE_AFTER_RECOVERY_ISOLATION"
)
CHECKPOINT_RECOVERY_EXECUTOR_LIFECYCLE = (
    "FIRST_RECOVERED_CHECKPOINT_ISOLATED_THEN_NORMAL_ADAPTIVE"
)
CHECKPOINT_RECOVERY_SCOPE = "PHASE_C_CHECKPOINT_RECOVERY"
LEGACY_CHECKPOINT_RECOVERY_SCOPE = (
    "PHASE_C_CHECKPOINT_RECOVERY_AFTER_RESOURCE_FAILURE"
)
RESTRICTED_READ_KEYS = (
    "validation_reads",
    "holdout_reads",
    "historical_2023_reads",
    "forward_b_reads",
    "forward_2026_reads",
)


def _read_json(path: Path) -> dict[str, Any]:
    return phase_b._read_json(path)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return phase_b._read_jsonl(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    return phase_b._write_json(path, payload)


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        "".join(
            json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def _self_hashed(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    return phase_b._self_hashed(payload, field)


def _sha256(path: Path) -> str:
    return phase_b._sha256(path)


def _resolve_checkpoint_recovery_incident_boundary(
    payload: Mapping[str, Any],
) -> tuple[str, int]:
    root_values = [
        str(payload[key])
        for key in ("output_root", "root")
        if payload.get(key) not in (None, "")
    ]
    normalized_roots = {
        value.replace("/", "\\").lower() for value in root_values
    }
    count_values: list[int] = []
    for key in ("closed_checkpoint_count", "accepted_checkpoint_count"):
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            raise RuntimeError("Phase C checkpoint recovery incident boundary drift")
        count_values.append(value)
    if (
        not root_values
        or len(normalized_roots) != 1
        or not count_values
        or len(set(count_values)) != 1
    ):
        raise RuntimeError("Phase C checkpoint recovery incident boundary drift")
    return root_values[0], count_values[0]


def _verify_payload_hash(
    payload: Mapping[str, Any], field: str, label: str
) -> str:
    body = dict(payload)
    declared = str(body.pop(field, ""))
    if not declared or declared != stable_hash(body):
        raise RuntimeError(f"{label} self-hash drift")
    return declared


def _same_path(left: str | Path, right: str | Path) -> bool:
    return str(Path(left).resolve()).replace("/", "\\").lower() == str(
        Path(right).resolve()
    ).replace("/", "\\").lower()


def _require_zero_restricted_reads(
    payload: Mapping[str, Any], label: str
) -> None:
    if any(int(payload.get(key, 0)) for key in RESTRICTED_READ_KEYS):
        raise RuntimeError(f"{label} restricted-read drift")


def _checkpoint_recovery_history(
    root: Path,
) -> list[tuple[Path, dict[str, Any]]]:
    legacy_path = root / "checkpoint_recovery_binding.json"
    attempts_root = root / "checkpoint_recovery_attempts"
    attempt_paths = (
        sorted(attempts_root.glob("*.json")) if attempts_root.is_dir() else []
    )
    if not legacy_path.is_file():
        if attempt_paths:
            raise RuntimeError("Phase C checkpoint recovery history has no genesis")
        return []

    legacy = _read_json(legacy_path)
    _verify_payload_hash(
        legacy, "recovery_binding_sha256", "Phase C checkpoint recovery genesis"
    )
    history = [(legacy_path.resolve(), legacy)]
    remaining: dict[Path, dict[str, Any]] = {}
    for path in attempt_paths:
        payload = _read_json(path)
        payload_hash = _verify_payload_hash(
            payload,
            "recovery_binding_sha256",
            "Phase C checkpoint recovery continuation",
        )
        if path.stem != payload_hash:
            raise RuntimeError("Phase C checkpoint recovery continuation name drift")
        remaining[path.resolve()] = payload

    while remaining:
        previous_path, previous = history[-1]
        candidates = [
            (path, payload)
            for path, payload in remaining.items()
            if _same_path(
                str(payload.get("previous_recovery_binding") or ""),
                previous_path,
            )
            and str(payload.get("previous_recovery_binding_file_sha256") or "")
            == _sha256(previous_path)
            and str(
                payload.get("previous_recovery_binding_payload_sha256") or ""
            )
            == str(previous.get("recovery_binding_sha256") or "")
        ]
        if len(candidates) != 1:
            raise RuntimeError("Phase C checkpoint recovery history is not linear")
        path, payload = candidates[0]
        history.append((path, payload))
        remaining.pop(path)
    return history


def _verify_checkpoint_quarantine(
    path: Path,
    *,
    root: Path,
    checkpoint_number: int,
) -> tuple[dict[str, Any], str]:
    payload = _read_json(path)
    payload_hash = _verify_payload_hash(
        payload, "quarantine_payload_sha256", "Phase C checkpoint quarantine"
    )
    artifacts = list(payload.get("artifacts") or ())
    if (
        str(payload.get("status") or "") != "PASS"
        or not _same_path(str(payload.get("accepted_root") or ""), root)
        or int(payload.get("checkpoint_number") or 0) != checkpoint_number
        or int(payload.get("record_count") or 0) != RECORDS_PER_CHECKPOINT
        or len(artifacts) != RECORDS_PER_CHECKPOINT
        or not bool(payload.get("formal_inflight_empty"))
        or bool(payload.get("financial_results_reusable"))
        or bool(payload.get("incomplete_results_reused"))
        or int(payload.get("optimizer_tell_count") or 0) != 0
    ):
        raise RuntimeError("Phase C checkpoint quarantine contract drift")
    _require_zero_restricted_reads(payload, "Phase C checkpoint quarantine")
    expected_ordinals = set(
        range(
            (checkpoint_number - 1) * RECORDS_PER_CHECKPOINT,
            checkpoint_number * RECORDS_PER_CHECKPOINT,
        )
    )
    observed_ordinals: set[int] = set()
    quarantine_paths: set[Path] = set()
    for artifact in artifacts:
        ordinal = int(artifact.get("main_record_ordinal", -1))
        source_path = Path(str(artifact.get("source_path") or "")).resolve()
        quarantine_path = Path(str(artifact.get("quarantine_path") or "")).resolve()
        expected_hash = str(artifact.get("file_sha256") or "")
        if (
            ordinal in observed_ordinals
            or quarantine_path in quarantine_paths
            or not source_path.is_relative_to(
                (root / "inflight" / f"checkpoint_{checkpoint_number:03d}").resolve()
            )
            or quarantine_path.is_relative_to(root.resolve())
            or not quarantine_path.is_file()
            or not expected_hash
            or _sha256(quarantine_path) != expected_hash
        ):
            raise RuntimeError("Phase C checkpoint quarantine artifact drift")
        observed_ordinals.add(ordinal)
        quarantine_paths.add(quarantine_path)
    if observed_ordinals != expected_ordinals:
        raise RuntimeError("Phase C checkpoint quarantine ordinal drift")
    return payload, payload_hash


def _runtime_resource_snapshot() -> dict[str, int]:
    available, parent_rss, tree_rss = phase_b._resource_snapshot()
    swap = psutil.swap_memory()
    if platform.system() == "Windows":
        import ctypes
        from ctypes import wintypes

        class PerformanceInformation(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("CommitTotal", ctypes.c_size_t),
                ("CommitLimit", ctypes.c_size_t),
                ("CommitPeak", ctypes.c_size_t),
                ("PhysicalTotal", ctypes.c_size_t),
                ("PhysicalAvailable", ctypes.c_size_t),
                ("SystemCache", ctypes.c_size_t),
                ("KernelTotal", ctypes.c_size_t),
                ("KernelPaged", ctypes.c_size_t),
                ("KernelNonpaged", ctypes.c_size_t),
                ("PageSize", ctypes.c_size_t),
                ("HandleCount", wintypes.DWORD),
                ("ProcessCount", wintypes.DWORD),
                ("ThreadCount", wintypes.DWORD),
            ]

        info = PerformanceInformation()
        info.cb = ctypes.sizeof(info)
        if not ctypes.windll.psapi.GetPerformanceInfo(
            ctypes.byref(info), info.cb
        ):
            raise ctypes.WinError()
        committed = int(info.CommitTotal * info.PageSize)
        commit_limit = int(info.CommitLimit * info.PageSize)
        available = int(info.PhysicalAvailable * info.PageSize)
    else:
        virtual = psutil.virtual_memory()
        committed = int((virtual.total - virtual.available) + swap.used)
        commit_limit = int(virtual.total + swap.total)
    return {
        "available_physical_bytes": int(available),
        "committed_bytes": committed,
        "commit_limit_bytes": commit_limit,
        "commit_headroom_bytes": max(0, commit_limit - committed),
        "pagefile_total_bytes": int(swap.total),
        "pagefile_used_bytes": int(swap.used),
        "pagefile_pages_in_bytes": int(swap.sin),
        "pagefile_pages_out_bytes": int(swap.sout),
        "parent_rss_bytes": int(parent_rss),
        "process_tree_rss_bytes": int(tree_rss),
    }


def _require_runtime_resource_safety(snapshot: Mapping[str, int]) -> None:
    if int(snapshot["commit_headroom_bytes"]) < MINIMUM_COMMIT_HEADROOM_BYTES:
        raise RuntimeError("Phase C runtime commit-headroom gate failed")


def _checkpoint_field_columns(
    schedules: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    fields = set(MANDATORY_CHECKPOINT_FIELD_COLUMNS)
    for schedule in schedules:
        for member in ("primary_compiled", "control_compiled"):
            compiled = schedule.get(member)
            if not isinstance(compiled, Mapping):
                raise RuntimeError("Phase C checkpoint compiled field binding missing")
            leaves = compiled.get("physical_leaf_ids")
            if not isinstance(leaves, (list, tuple)):
                raise RuntimeError("Phase C checkpoint physical leaves missing")
            fields.update(map(str, leaves))
    if any(not field for field in fields):
        raise RuntimeError("Phase C checkpoint physical leaf identity drift")
    return tuple(sorted(fields))


def _effective_checkpoint_workers(
    *,
    worker_cap: int,
    schedule_count: int,
    commit_headroom_bytes: int,
) -> int:
    if worker_cap not in CHECKPOINT_WORKER_CHOICES:
        raise RuntimeError("Phase C checkpoint worker cap drift")
    usable_headroom = max(
        0, int(commit_headroom_bytes) - MINIMUM_COMMIT_HEADROOM_BYTES
    )
    memory_cap = max(1, usable_headroom // PROJECTED_WORKER_COMMIT_BUDGET_BYTES)
    eligible = [
        choice
        for choice in CHECKPOINT_WORKER_CHOICES
        if choice <= worker_cap and choice <= schedule_count and choice <= memory_cap
    ]
    return max(eligible) if eligible else min(4, worker_cap, schedule_count)


def _checkpoint_executor_plan(
    *,
    checkpoint_index: int,
    recovery_start_checkpoint_index: int,
    checkpoint_recovery_mode: bool,
    worker_cap: int,
    schedule_count: int,
    commit_headroom_bytes: int,
) -> dict[str, Any]:
    if (
        checkpoint_recovery_mode
        and checkpoint_index == recovery_start_checkpoint_index
    ):
        return {
            "executor_mode": CHECKPOINT_RECOVERY_EXECUTOR_MODE,
            "effective_checkpoint_workers": 1,
            "max_tasks_per_child": 1,
            "checkpoint_recovery_provenance": True,
        }
    return {
        "executor_mode": (
            CHECKPOINT_POST_RECOVERY_EXECUTOR_MODE
            if checkpoint_recovery_mode
            else CHECKPOINT_ADAPTIVE_EXECUTOR_MODE
        ),
        "effective_checkpoint_workers": _effective_checkpoint_workers(
            worker_cap=worker_cap,
            schedule_count=schedule_count,
            commit_headroom_bytes=commit_headroom_bytes,
        ),
        "max_tasks_per_child": None,
        "checkpoint_recovery_provenance": checkpoint_recovery_mode,
    }


def _new_child_process_ids(before: set[int]) -> list[int]:
    current = psutil.Process()
    output: list[int] = []
    for child in current.children(recursive=True):
        try:
            command = " ".join(child.cmdline()).lower()
            if (
                child.pid not in before
                and child.is_running()
                and "multiprocessing.resource_tracker" not in command
            ):
                output.append(int(child.pid))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return sorted(output)


def _component_from_row(row: Mapping[str, Any]) -> ProgramSourceComponentV0:
    component = ProgramSourceComponentV0(
        role=str(row["role"]),
        primary=dict(row["primary"]),
        control=dict(row["control"]),
        proposal_id=str(row["proposal_id"]),
        trial_number=(
            int(row["trial_number"]) if row.get("trial_number") is not None else None
        ),
        sampling_phase=str(row["sampling_phase"]),
    )
    if (
        component.component_id != str(row["component_id"])
        or component.route_id != str(row["route_id"])
        or component.generation_receipt_hash
        != str(row["generation_receipt_hash"])
        or component.credit_eligible != bool(row["credit_eligible"])
    ):
        raise RuntimeError("Phase C executable component identity drift")
    return component


def _components_for_reservoir(
    reservoir: Mapping[str, Any],
    *,
    components_by_id: Mapping[str, ProgramSourceComponentV0],
) -> dict[str, ProgramSourceComponentV0]:
    selected: dict[str, ProgramSourceComponentV0] = {}
    for role in PROGRAM_TEMPLATE_COMPONENTS[str(reservoir["template_id"])]:
        binding = dict(reservoir["components"][role])
        component = components_by_id[str(binding["component_id"])]
        if (
            component.role != role
            or component.route_id != str(binding["route_id"])
            or component.proposal_id != str(binding["proposal_id"])
            or component.generation_receipt_hash
            != str(binding["generation_receipt_hash"])
        ):
            raise RuntimeError("Phase C reservoir/component binding drift")
        selected[role] = component
    return selected


def _catalog_entry(
    reservoir: Mapping[str, Any],
    *,
    components_by_id: Mapping[str, ProgramSourceComponentV0],
    adapter: CandidateProgramProposalAdapterV0,
    compiler: ProgramCompilerV1,
) -> dict[str, Any]:
    template_id = str(reservoir["template_id"])
    components = _components_for_reservoir(
        reservoir, components_by_id=components_by_id
    )
    program = adapter.compose(
        template_id,
        base_component=components["base"],
        temporal_component=components.get("temporal"),
        market_component=components.get("market"),
        event_component=components.get("event"),
        combination_policy=dict(reservoir["combination_policy"]),
    )
    if template_id == "BASE":
        control = legacy_candidate_program_v1(
            components["base"].control,
            portfolio_contract=adapter.portfolio_contract,
        )
    else:
        control = construct_matched_control_program_v1(program).control
        if program.semantic_program_hash == control.semantic_program_hash:
            return {
                "status": "SEMANTIC_NOOP_REJECTED",
                "template_id": template_id,
                "reservoir_record_sha256": str(
                    reservoir["reservoir_record_sha256"]
                ),
            }
    compiled_program = compiler.compile(program)
    compiler.compile(control)
    receipt = adapter.build_receipt(
        program_template_id=template_id,
        program=program,
        components=tuple(
            components[role] for role in PROGRAM_TEMPLATE_COMPONENTS[template_id]
        ),
        combination_policy=dict(reservoir["combination_policy"]),
        batch_id=PHASE_C_BATCH_ID,
        ask_ordinal=0,
        generation_arm="UNIFORM_FRESH",
    )
    return {
        "status": "EXECUTABLE",
        "template_id": template_id,
        "reservoir": dict(reservoir),
        "score_receipt": receipt.to_record(),
        "program_id": program.program_id,
        "base_component_id": components["base"].component_id,
        "component_ids": tuple(
            sorted(
                str(component.primary["candidate_id"])
                for component in components.values()
            )
        ),
        "combination_id": stable_hash(dict(reservoir["combination_policy"])),
        "program_genes": program_structural_genes_v1(
            program_template_id=template_id,
            components=components,
            combination_policy=dict(reservoir["combination_policy"]),
            program=program,
            compiled=compiled_program,
        ),
    }


def _build_catalog(
    *,
    reservoir: Sequence[Mapping[str, Any]],
    component_rows: Sequence[Mapping[str, Any]],
    registry: UnifiedCapabilityRegistry,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    components_by_id = {
        component.component_id: component
        for component in (_component_from_row(row) for row in component_rows)
    }
    if len(components_by_id) != len(component_rows):
        raise RuntimeError("Phase C component pool contains duplicate identities")
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    by_template: dict[str, list[dict[str, Any]]] = {
        template_id: [] for template_id in TEMPLATE_ORDER
    }
    rejected: list[dict[str, Any]] = []
    for row in reservoir:
        entry = _catalog_entry(
            row,
            components_by_id=components_by_id,
            adapter=adapter,
            compiler=compiler,
        )
        if entry["status"] == "EXECUTABLE":
            by_template[str(entry["template_id"])].append(entry)
        else:
            rejected.append(entry)
    for template_id, entries in by_template.items():
        quota = CATALOG_MIN_RECORDS_PER_TEMPLATE
        base_ids = {str(entry["base_component_id"]) for entry in entries}
        if len(entries) < quota or len(base_ids) < MIN_BASE_IDENTITIES_PER_TEMPLATE:
            raise RuntimeError(
                f"Phase C executable catalog underfilled: {template_id} "
                f"records={len(entries)} bases={len(base_ids)}"
            )
    report = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_catalog_preflight_v0",
            "status": "PHASE_C_CATALOG_EXECUTABLE",
            "template_executable_counts": {
                template_id: len(entries)
                for template_id, entries in by_template.items()
            },
            "template_base_identity_counts": {
                template_id: len(
                    {str(entry["base_component_id"]) for entry in entries}
                )
                for template_id, entries in by_template.items()
            },
            "semantic_noop_rejections": rejected,
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "catalog_preflight_sha256",
    )
    return by_template, report


def _selection_state() -> dict[str, Any]:
    return {
        "program_ids": set(),
        "reservoir_ids": set(),
        "base_counts": Counter(),
        "component_ids": set(),
        "combination_ids": set(),
    }


def _choose_entry(
    ask: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
    *,
    bandit: ProgramFactorizedBanditV0,
    state: MutableMapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    template_id = str(ask["template_id"])
    arm = str(ask["generation_arm"])
    candidates = [
        dict(entry)
        for entry in entries
        if str(entry["reservoir"]["reservoir_record_sha256"])
        not in state["reservoir_ids"]
        and (
            template_id == "BASE"
            or str(entry["program_id"]) not in state["program_ids"]
        )
        and int(state["base_counts"][(template_id, str(entry["base_component_id"]))])
        < MAX_VARIANTS_PER_BASE_PER_TEMPLATE
    ]
    require_new_base = (
        template_id != "BASE"
        and int(ask["template_record_ordinal"]) < MIN_BASE_IDENTITIES_PER_TEMPLATE
    )
    if require_new_base:
        candidates = [
            entry
            for entry in candidates
            if int(
                state["base_counts"][(template_id, str(entry["base_component_id"]))]
            )
            == 0
        ]
    if not candidates:
        raise RuntimeError(f"Phase C selection supply exhausted: {template_id}/{arm}")

    scored: list[tuple[tuple[Any, ...], dict[str, Any], dict[str, Any]]] = []
    for entry in candidates:
        receipt = ProgramProposalReceiptV0.from_record(entry["score_receipt"])
        factorized = bandit.score_receipt(receipt)
        unseen_components = sum(
            component_id not in state["component_ids"]
            for component_id in entry["component_ids"]
        )
        unseen_combination = int(
            str(entry["combination_id"]) not in state["combination_ids"]
        )
        base_count = int(
            state["base_counts"][(template_id, str(entry["base_component_id"]))]
        )
        reservoir_ordinal = int(entry["reservoir"]["template_reservoir_ordinal"])
        if arm == "FACTORIZED_EXPLOIT":
            key = (
                -int(bool(factorized["factorized_signal_available"])),
                -float(factorized["factorized_score"]),
                -int(factorized["eligible_factor_count"]),
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        elif arm == "NOVELTY_RESERVE":
            key = (
                -unseen_components,
                -unseen_combination,
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        elif arm == "UNIFORM_FRESH":
            key = (
                base_count,
                reservoir_ordinal,
                str(entry["reservoir"]["raw_combination_sha256"]),
            )
        else:
            raise RuntimeError(f"unknown Phase C generation arm: {arm}")
        metrics = {
            "factorized_score": float(factorized["factorized_score"]),
            "factorized_signal_available": bool(
                factorized["factorized_signal_available"]
            ),
            "eligible_factor_count": int(factorized["eligible_factor_count"]),
            "unseen_component_count": int(unseen_components),
            "unseen_combination": bool(unseen_combination),
            "prior_base_variant_count": base_count,
        }
        scored.append((key, entry, metrics))
    _, selected, metrics = min(scored, key=lambda item: item[0])
    state["program_ids"].add(str(selected["program_id"]))
    state["reservoir_ids"].add(
        str(selected["reservoir"]["reservoir_record_sha256"])
    )
    state["base_counts"][(template_id, str(selected["base_component_id"]))] += 1
    state["component_ids"].update(selected["component_ids"])
    state["combination_ids"].add(str(selected["combination_id"]))
    decision = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_selection_decision_v0",
            "main_record_ordinal": int(ask["main_record_ordinal"]),
            "template_id": template_id,
            "generation_arm": arm,
            "ask_record_sha256": str(ask["ask_record_sha256"]),
            "reservoir_record_sha256": str(
                selected["reservoir"]["reservoir_record_sha256"]
            ),
            "raw_combination_sha256": str(
                selected["reservoir"]["raw_combination_sha256"]
            ),
            "program_id": str(selected["program_id"]),
            "base_component_id": str(selected["base_component_id"]),
            "selection_metrics": metrics,
            "bandit_state_before_selection_sha256": str(
                bandit.snapshot()["bandit_state_sha256"]
            ),
            "adaptive_template_credit_used": bool(
                arm == "FACTORIZED_EXPLOIT"
                and metrics["factorized_signal_available"]
            ),
        },
        "selection_decision_sha256",
    )
    return selected, decision


def _schedule_record(
    ask: Mapping[str, Any],
    entry: Mapping[str, Any],
    decision: Mapping[str, Any],
    *,
    components_by_id: Mapping[str, ProgramSourceComponentV0],
    adapter: CandidateProgramProposalAdapterV0,
    compiler: ProgramCompilerV1,
) -> dict[str, Any]:
    reservoir = dict(entry["reservoir"])
    template_id = str(ask["template_id"])
    components = _components_for_reservoir(
        reservoir, components_by_id=components_by_id
    )
    program = adapter.compose(
        template_id,
        base_component=components["base"],
        temporal_component=components.get("temporal"),
        market_component=components.get("market"),
        event_component=components.get("event"),
        combination_policy=dict(reservoir["combination_policy"]),
    )
    receipt = adapter.build_receipt(
        program_template_id=template_id,
        program=program,
        components=tuple(
            components[role] for role in PROGRAM_TEMPLATE_COMPONENTS[template_id]
        ),
        combination_policy=dict(reservoir["combination_policy"]),
        batch_id=PHASE_C_BATCH_ID,
        ask_ordinal=int(ask["main_record_ordinal"]),
        generation_arm=str(ask["generation_arm"]),
    )
    record: dict[str, Any] = {
        "schema_version": "cn_joint_program_phase_c_schedule_record_v0",
        "main_record_ordinal": int(ask["main_record_ordinal"]),
        "checkpoint_ordinal": int(ask["checkpoint_ordinal"]),
        "template_id": template_id,
        "template_record_ordinal": int(ask["template_record_ordinal"]),
        "generation_arm": str(ask["generation_arm"]),
        "adaptive_template_credit_used": bool(
            decision["adaptive_template_credit_used"]
        ),
        "ask_record_sha256": str(ask["ask_record_sha256"]),
        "reservoir_record_sha256": str(reservoir["reservoir_record_sha256"]),
        "raw_combination_sha256": str(reservoir["raw_combination_sha256"]),
        "selection_decision_sha256": str(decision["selection_decision_sha256"]),
        "combination_policy": dict(reservoir["combination_policy"]),
        "components": dict(reservoir["components"]),
        "proposal_receipt": receipt.to_record(),
        "primary_program": program.to_record(),
        "primary_compiled": compiler.compile(program).to_record(),
        "semantic_noop": False,
    }
    for search_control_key in (
        "matched_control_contract_id",
        "canary_profile",
        "absolute_admission_head_eligible",
        "conditional_uplift_head_eligible",
    ):
        if search_control_key in ask:
            record[search_control_key] = ask[search_control_key]
    if template_id == "BASE":
        legacy_control = legacy_candidate_program_v1(
            components["base"].control,
            portfolio_contract=adapter.portfolio_contract,
        )
        record.update(
            {
                "record_kind": "BASE_WRAPPER_PARITY",
                "legacy_primary_candidate": dict(components["base"].primary),
                "legacy_control_candidate": dict(components["base"].control),
                "control_program": legacy_control.to_record(),
                "control_compiled": compiler.compile(legacy_control).to_record(),
                "pair_id": str(components["base"].primary["pair_id"]),
            }
        )
    else:
        matched = construct_matched_control_program_v1(program)
        if matched.primary.semantic_program_hash == matched.control.semantic_program_hash:
            raise RuntimeError("semantic no-op escaped Phase C catalog preflight")
        record.update(
            {
                "record_kind": "ENHANCED_FULL_BASE_PAIR",
                "control_program": matched.control.to_record(),
                "control_compiled": compiler.compile(matched.control).to_record(),
                "pair_id": matched.pair_id,
                "diagnostic_only": matched.diagnostic_only,
            }
        )
    record["schedule_record_sha256"] = stable_hash(record)
    return record


def _select_checkpoint(
    asks: Sequence[Mapping[str, Any]],
    *,
    catalog: Mapping[str, Sequence[Mapping[str, Any]]],
    bandit: ProgramFactorizedBanditV0,
    state: MutableMapping[str, Any],
    components_by_id: Mapping[str, ProgramSourceComponentV0],
    adapter: CandidateProgramProposalAdapterV0,
    compiler: ProgramCompilerV1,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    schedules: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    for ask in asks:
        entry, decision = _choose_entry(
            ask,
            catalog[str(ask["template_id"])],
            bandit=bandit,
            state=state,
        )
        schedules.append(
            _schedule_record(
                ask,
                entry,
                decision,
                components_by_id=components_by_id,
                adapter=adapter,
                compiler=compiler,
            )
        )
        decisions.append(decision)
    return schedules, decisions


def _initialize_worker(*args: Any) -> None:
    phase_b._initialize_worker(*args)


def _evaluate_record(record: Mapping[str, Any], target_path: str) -> dict[str, Any]:
    target = Path(target_path)
    temporary = target.with_suffix(".phase_b.tmp.json")
    try:
        payload = phase_b._evaluate_record(record, str(temporary))
        body = dict(payload)
        body.pop("record_payload_sha256", None)
        body.update(
            {
                "schema_version": "cn_joint_program_phase_c_record_v0",
                "status": "JOINT_PROGRAM_PHASE_C_RECORD_CLOSED_IMMUTABLE",
                "generation_arm": str(record["generation_arm"]),
                "adaptive_template_credit_used": bool(
                    record["adaptive_template_credit_used"]
                ),
                "ask_record_sha256": str(record["ask_record_sha256"]),
                "reservoir_record_sha256": str(
                    record["reservoir_record_sha256"]
                ),
                "raw_combination_sha256": str(
                    record["raw_combination_sha256"]
                ),
                "selection_decision_sha256": str(
                    record["selection_decision_sha256"]
                ),
                "campaign_local_bandit_feedback_eligible": bool(
                    str(record["template_id"]) != "BASE"
                ),
                **{
                    key: record[key]
                    for key in (
                        "matched_control_contract_id",
                        "canary_profile",
                        "absolute_admission_head_eligible",
                        "conditional_uplift_head_eligible",
                    )
                    if key in record
                },
                "formal_optimizer_feedback_write": "FORBIDDEN",
            }
        )
        closed = _self_hashed(body, "record_payload_sha256")
        _write_json(target, closed)
        return closed
    finally:
        if temporary.exists():
            temporary.unlink()
        gc.collect()


def _verify_record(
    path: Path,
    *,
    expected_input_hash: str,
    schedule: Mapping[str, Any],
) -> dict[str, Any]:
    row = _read_json(path)
    body = dict(row)
    expected = str(body.pop("record_payload_sha256", ""))
    if expected != stable_hash(body):
        raise RuntimeError(f"Phase C record self-hash drift: {path}")
    if (
        str(row.get("status") or "")
        != "JOINT_PROGRAM_PHASE_C_RECORD_CLOSED_IMMUTABLE"
        or str(row.get("input_binding_sha256") or "") != expected_input_hash
        or int(row.get("main_record_ordinal", -1))
        != int(schedule["main_record_ordinal"])
        or str(row.get("schedule_record_sha256") or "")
        != str(schedule["schedule_record_sha256"])
        or str(row.get("generation_arm") or "")
        != str(schedule["generation_arm"])
        or str(row.get("selection_decision_sha256") or "")
        != str(schedule["selection_decision_sha256"])
    ):
        raise RuntimeError(f"Phase C record identity drift: {path}")
    return row


def _feedback_update(
    records: Sequence[Mapping[str, Any]],
    schedules: Sequence[Mapping[str, Any]],
    *,
    bandit: ProgramFactorizedBanditV0,
    behavior_counts: Counter[str],
) -> list[dict[str, Any]]:
    schedules_by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in schedules
    }
    ledger: list[dict[str, Any]] = []
    for row in sorted(records, key=lambda item: int(item["main_record_ordinal"])):
        ordinal = int(row["main_record_ordinal"])
        schedule = schedules_by_ordinal[ordinal]
        behavior_identity = str(
            (row.get("primary") or {}).get("behavior_identity") or ""
        )
        reason = "UPDATED"
        updated = True
        if str(row["template_id"]) == "BASE":
            reason = "BASE_PARITY_NOT_REWARD_ELIGIBLE"
            updated = False
        elif str(row.get("replay_status") or "") != PAIR_REPLAY_COMPLETE:
            reason = "REPLAY_BLOCKED_NO_UPDATE"
            updated = False
        elif row.get("blockers"):
            reason = "BLOCKER_NO_UPDATE"
            updated = False
        elif row.get("matched_net_reward_increment") is None:
            reason = "NONFINITE_INCREMENT_NO_UPDATE"
            updated = False
        before = str(bandit.snapshot()["bandit_state_sha256"])
        if updated:
            receipt = ProgramProposalReceiptV0.from_record(
                dict(schedule["proposal_receipt"])
            )
            _, turnover_excess, concentration = _feedback_quality_inputs(row)
            bandit.observe(
                receipt,
                matched_increment=float(row["matched_net_reward_increment"]),
                behavior_cluster_repeat_count=behavior_counts[behavior_identity],
                turnover_excess_ratio=turnover_excess,
                single_window_concentration=concentration,
            )
        if behavior_identity:
            behavior_counts[behavior_identity] += 1
        ledger.append(
            _self_hashed(
                {
                    "schema_version": "cn_joint_program_phase_c_feedback_v0",
                    "main_record_ordinal": ordinal,
                    "template_id": str(row["template_id"]),
                    "generation_arm": str(row["generation_arm"]),
                    "record_payload_sha256": str(row["record_payload_sha256"]),
                    "proposal_receipt_sha256": str(
                        schedule["proposal_receipt"]["proposal_receipt_sha256"]
                    ),
                    "bandit_update_applied": updated,
                    "reason": reason,
                    "bandit_state_before_sha256": before,
                    "bandit_state_after_sha256": str(
                        bandit.snapshot()["bandit_state_sha256"]
                    ),
                    "validation_feedback_used": False,
                    "cross_campaign_state_imported": False,
                },
                "feedback_record_sha256",
            )
        )
    return ledger


def _checkpoint_artifacts(checkpoint_root: Path) -> None:
    manifest = _read_json(checkpoint_root / "batch_manifest.json")
    body = dict(manifest)
    expected = str(body.pop("manifest_payload_hash", ""))
    if expected != stable_hash(body):
        raise RuntimeError("Phase C checkpoint manifest self-hash drift")
    for artifact in manifest.get("artifacts") or ():
        path = (checkpoint_root / str(artifact["path"])).resolve()
        if not path.is_relative_to(checkpoint_root.resolve()):
            raise RuntimeError("Phase C checkpoint artifact escape")
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact["bytes"])
            or _sha256(path) != str(artifact["sha256"])
        ):
            raise RuntimeError(f"Phase C checkpoint artifact drift: {path}")


def _verify_checkpoint(
    checkpoint_root: Path,
    *,
    checkpoint_id: str,
    previous_manifest: Path | None,
    input_hash: str,
    expected_schedules: Sequence[Mapping[str, Any]],
    expected_decisions: Sequence[Mapping[str, Any]],
    bandit: ProgramFactorizedBanditV0,
    behavior_counts: Counter[str],
) -> tuple[Path, list[dict[str, Any]], ProgramFactorizedBanditV0]:
    _checkpoint_artifacts(checkpoint_root)
    manifest_path = checkpoint_root / "batch_manifest.json"
    manifest = _read_json(manifest_path)
    expected_prior = _sha256(previous_manifest) if previous_manifest else "GENESIS"
    if (
        str(manifest.get("status") or "") != "BATCH_CLOSED_IMMUTABLE"
        or str(manifest.get("batch_id") or "") != checkpoint_id
        or str((manifest.get("input_hashes") or {}).get("prior_checkpoint_manifest"))
        != expected_prior
        or str((manifest.get("input_hashes") or {}).get("phase_c_input_binding"))
        != input_hash
    ):
        raise RuntimeError(f"Phase C checkpoint chain drift: {checkpoint_id}")
    schedules = _read_jsonl(checkpoint_root / "selected_schedule.jsonl")
    decisions = _read_jsonl(checkpoint_root / "selection_ledger.jsonl")
    if schedules != [dict(row) for row in expected_schedules]:
        raise RuntimeError(f"Phase C checkpoint schedule replay drift: {checkpoint_id}")
    if decisions != [dict(row) for row in expected_decisions]:
        raise RuntimeError(f"Phase C checkpoint selection replay drift: {checkpoint_id}")
    before = _read_json(checkpoint_root / "bandit_state_before.json")
    if before != bandit.snapshot():
        raise RuntimeError(f"Phase C checkpoint bandit-before drift: {checkpoint_id}")
    ProgramFactorizedBanditV0.restore(before)
    by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in schedules
    }
    rows = [
        _verify_record(
            path,
            expected_input_hash=input_hash,
            schedule=by_ordinal[int(path.stem.split("_")[-1])],
        )
        for path in sorted((checkpoint_root / "records").glob("record_*.json"))
    ]
    if len(rows) != RECORDS_PER_CHECKPOINT:
        raise RuntimeError(f"Phase C checkpoint record count drift: {checkpoint_id}")
    replay_bandit = ProgramFactorizedBanditV0.restore(before)
    expected_feedback = _feedback_update(
        rows,
        schedules,
        bandit=replay_bandit,
        behavior_counts=behavior_counts,
    )
    feedback = _read_jsonl(checkpoint_root / "bandit_feedback_ledger.jsonl")
    if feedback != expected_feedback:
        raise RuntimeError(f"Phase C checkpoint feedback replay drift: {checkpoint_id}")
    after = _read_json(checkpoint_root / "bandit_state_after.json")
    if after != replay_bandit.snapshot():
        raise RuntimeError(f"Phase C checkpoint bandit-after drift: {checkpoint_id}")
    ProgramFactorizedBanditV0.restore(after)
    return manifest_path, rows, replay_bandit


def _close_checkpoint(
    inflight_root: Path,
    final_root: Path,
    *,
    checkpoint_id: str,
    previous_manifest: Path | None,
    input_hash: str,
    freeze_closure_sha256: str,
    ask_plan_sha256: str,
    reservoir_sha256: str,
    schedules: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    bandit_before: Mapping[str, Any],
    bandit_after: Mapping[str, Any],
    feedback: Sequence[Mapping[str, Any]],
    telemetry: Mapping[str, Any],
) -> Path:
    record_root = inflight_root / "records"
    schedule_by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in schedules
    }
    records = [
        _verify_record(
            record_root / f"record_{int(row['main_record_ordinal']):04d}.json",
            expected_input_hash=input_hash,
            schedule=row,
        )
        for row in schedules
    ]
    schedule_path = _write_jsonl(
        inflight_root / "selected_schedule.jsonl", schedules
    )
    decision_path = _write_jsonl(
        inflight_root / "selection_ledger.jsonl", decisions
    )
    before_path = _write_json(
        inflight_root / "bandit_state_before.json", bandit_before
    )
    feedback_path = _write_jsonl(
        inflight_root / "bandit_feedback_ledger.jsonl", feedback
    )
    after_path = _write_json(
        inflight_root / "bandit_state_after.json", bandit_after
    )
    summary = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_checkpoint_summary_v0",
            "status": "JOINT_PROGRAM_PHASE_C_CHECKPOINT_COMPLETE",
            "checkpoint_id": checkpoint_id,
            "record_count": len(records),
            "template_id": str(records[0]["template_id"]),
            "generation_arm_counts": dict(
                sorted(Counter(str(row["generation_arm"]) for row in records).items())
            ),
            "productive_count": sum(bool(row["productive"]) for row in records),
            "replay_complete_count": sum(
                phase_b._record_replay_complete(row) for row in records
            ),
            "replay_blocked_count": sum(
                not phase_b._record_replay_complete(row) for row in records
            ),
            "bandit_update_count": sum(
                bool(row["bandit_update_applied"]) for row in feedback
            ),
            "bandit_state_before_sha256": str(
                bandit_before["bandit_state_sha256"]
            ),
            "bandit_state_after_sha256": str(
                bandit_after["bandit_state_sha256"]
            ),
            **dict(telemetry),
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "summary_payload_sha256",
    )
    summary_path = _write_json(inflight_root / "checkpoint_summary.json", summary)
    record_paths = sorted(record_root.glob("record_*.json"))
    manifest_path = _batch_manifest(
        batch_root=inflight_root,
        batch_id=checkpoint_id,
        input_hashes={
            "phase_c_input_binding": input_hash,
            "phase_c_prefinancial_closure": freeze_closure_sha256,
            "phase_c_ask_plan": ask_plan_sha256,
            "phase_c_raw_reservoir": reservoir_sha256,
            "bandit_state_before": str(bandit_before["bandit_state_sha256"]),
            "prior_checkpoint_manifest": (
                _sha256(previous_manifest) if previous_manifest else "GENESIS"
            ),
        },
        paths=[
            *record_paths,
            schedule_path,
            decision_path,
            before_path,
            feedback_path,
            after_path,
            summary_path,
        ],
        access_receipts=[],
        evaluation_name=(
            "Phase C factorized joint candidate-program development continuous-book evaluation"
        ),
    )
    if final_root.exists():
        raise RuntimeError(f"closed Phase C checkpoint already exists: {final_root}")
    inflight_root.replace(final_root)
    return final_root / manifest_path.name


def _template_summary(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = phase_b._template_summary(records)
    for row in output:
        template_records = [
            item
            for item in records
            if str(item["template_id"]) == str(row["template_id"])
        ]
        row["generation_arm_counts"] = dict(
            sorted(
                Counter(
                    str(item["generation_arm"]) for item in template_records
                ).items()
            )
        )
        row["adaptive_template_credit_used"] = sum(
            bool(item["adaptive_template_credit_used"])
            for item in template_records
        )
    return output


def _verify_run_contract(
    contract: Mapping[str, Any],
    *,
    executor_workers: int,
) -> None:
    expected = {
        "main_record_count": EXPECTED_RECORDS,
        "checkpoint_count": CHECKPOINT_COUNT,
        "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
        "executor_backend": "PROCESS_POOL",
        "executor_workers": executor_workers,
        "executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE",
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "portfolio_decoder_id": "TOPK_10_EQUAL",
        "initial_uniform_baseline_per_enhanced_template": 16,
        "maximum_variants_per_base_per_template": MAX_VARIANTS_PER_BASE_PER_TEMPLATE,
        "minimum_base_identities_per_template": MIN_BASE_IDENTITIES_PER_TEMPLATE,
        "no_early_template_cancellation": True,
        "cross_campaign_optimizer_state_import": False,
        "route_local_tpe_authority_unchanged": True,
    }
    drift = [key for key, value in expected.items() if contract.get(key) != value]
    if drift:
        raise RuntimeError("Phase C frozen run contract drift: " + ",".join(drift))


def _verify_base_parity_root_gate(
    records: Sequence[Mapping[str, Any]],
) -> tuple[int, int]:
    parity_pass = 0
    replay_blocked = 0
    for row in records:
        if str(row["record_kind"]) != "BASE_WRAPPER_PARITY":
            continue
        replay_status = str(row["replay_status"])
        parity = row.get("base_wrapper_parity")
        if replay_status == PAIR_REPLAY_COMPLETE:
            if not isinstance(parity, Mapping) or str(parity.get("status")) != "PASS":
                raise RuntimeError("Phase C BASE parity root gate failed")
            parity_pass += 1
            continue
        if replay_status == PAIR_REPLAY_BLOCKED:
            blocker = row.get("replay_blocker")
            if (
                parity is not None
                or not isinstance(blocker, Mapping)
                or bool(blocker.get("economic_claim_authorized"))
                or bool(blocker.get("promotion_authorized"))
            ):
                raise RuntimeError("Phase C blocked BASE root gate failed")
            replay_blocked += 1
            continue
        raise RuntimeError("Phase C BASE replay status root gate failed")
    return parity_pass, replay_blocked


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"Phase C financial comparison is authorized only on {AUTHORIZED_HOST}"
        )
    root = args.output_root.resolve()
    recovery_from_sha = str(
        getattr(args, "root_finalization_recovery_from_repo_sha", "") or ""
    )
    recovery_incident_arg = getattr(args, "root_finalization_incident", None)
    recovery_deployment_arg = getattr(
        args, "root_finalization_deployment_manifest", None
    )
    recovery_mode = bool(recovery_from_sha)
    if recovery_mode != bool(recovery_incident_arg) or recovery_mode != bool(
        recovery_deployment_arg
    ):
        raise RuntimeError("Phase C root-finalization recovery binding is incomplete")
    checkpoint_recovery_from_sha = str(
        getattr(args, "checkpoint_recovery_from_repo_sha", "") or ""
    )
    checkpoint_recovery_incident_arg = getattr(
        args, "checkpoint_recovery_incident", None
    )
    checkpoint_recovery_diagnostic_audit_arg = getattr(
        args, "checkpoint_recovery_diagnostic_audit", None
    )
    checkpoint_recovery_deployment_arg = getattr(
        args, "checkpoint_recovery_deployment_manifest", None
    )
    checkpoint_recovery_mode = bool(checkpoint_recovery_from_sha)
    if len(
        {
            bool(checkpoint_recovery_from_sha),
            bool(checkpoint_recovery_incident_arg),
            bool(checkpoint_recovery_diagnostic_audit_arg),
            bool(checkpoint_recovery_deployment_arg),
        }
    ) != 1:
        raise RuntimeError("Phase C checkpoint recovery binding is incomplete")
    if recovery_mode and checkpoint_recovery_mode:
        raise RuntimeError("Phase C recovery modes are mutually exclusive")
    checkpoint_builder_sha = (
        recovery_from_sha
        if recovery_mode
        else (
            checkpoint_recovery_from_sha
            if checkpoint_recovery_mode
            else str(args.builder_commit_sha)
        )
    )
    freeze_root = args.phase_c_freeze_root.resolve()
    freeze = verify_phase_c_prefinancial_freeze_v0(freeze_root)
    contract = _read_json(freeze_root / "phase_c_run_contract.json")
    _verify_run_contract(contract, executor_workers=int(args.executor_workers))
    ask_path = freeze_root / "phase_c_ask_plan.jsonl"
    reservoir_path = freeze_root / "phase_c_raw_program_reservoir.jsonl"
    component_path = freeze_root / "phase_c_session_executable_component_pool.jsonl"
    initial_bandit_path = freeze_root / "initial_bandit_state.json"
    asks = _read_jsonl(ask_path)
    reservoir = _read_jsonl(reservoir_path)
    component_rows = _read_jsonl(component_path)
    if (
        len(asks) != EXPECTED_RECORDS
        or [int(row["main_record_ordinal"]) for row in asks]
        != list(range(EXPECTED_RECORDS))
    ):
        raise RuntimeError("Phase C ask plan ordinal drift")

    execution_contract_path = args.execution_contract.resolve()
    train_field_root = args.train_field_root.resolve()
    train_price_root = args.train_price_root.resolve()
    registry_path = args.registry.resolve()
    capacity_path = args.node_resource_capacity.resolve()
    for path in (execution_contract_path, registry_path, capacity_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    for path in (train_field_root, train_price_root):
        if not path.is_dir():
            raise FileNotFoundError(path)
    if _sha256(registry_path) != str(contract["registry_file_sha256"]):
        raise RuntimeError("Phase C registry file hash drift")

    phase_b_result_root = Path(str(contract["phase_b_result_root"])).resolve()
    phase_b_input = _read_json(phase_b_result_root / "input_binding.json")
    phase_b_input_body = dict(phase_b_input)
    phase_b_input_hash = str(phase_b_input_body.pop("input_binding_sha256", ""))
    if phase_b_input_hash != stable_hash(phase_b_input_body):
        raise RuntimeError("accepted Phase B input binding self-hash drift")
    if _sha256(execution_contract_path) != str(
        phase_b_input["execution_contract_sha256"]
    ):
        raise RuntimeError("Phase C execution contract is not the accepted Phase B authority")
    if str(train_price_root).replace("/", "\\").lower() != str(
        phase_b_input["train_price_root"]
    ).replace("/", "\\").lower():
        raise RuntimeError("Phase C train-price root is not the accepted Phase B authority")
    accepted_field_manifest = Path(
        str(contract["accepted_field_manifest_path"])
    ).resolve()
    if train_field_root != accepted_field_manifest.parent:
        raise RuntimeError("Phase C train-field root is not the frozen materialization root")

    execution_contract = _read_json(execution_contract_path)
    decoder_v2.base._verify_payload_hash(
        execution_contract,
        field="contract_payload_sha256",
        label="joint-program Phase C execution contract",
    )
    field_manifest_file_sha256 = str(
        contract["accepted_field_manifest_file_sha256"]
    )
    field_manifest_payload_sha256 = str(
        contract["accepted_field_manifest_payload_sha256"]
    )
    field_manifest, field_manifest_path = phase_b._validate_phase_b_materialized_sidecar(
        train_field_root,
        split_manifest_sha256=str(execution_contract["split_manifest_sha256"]),
        verify_shards=True,
        expected_manifest_file_sha256=field_manifest_file_sha256,
        expected_manifest_payload_sha256=field_manifest_payload_sha256,
    )
    price_manifest, price_manifest_path = phase_b._validate_phase_b_execution_price_sidecar(
        train_price_root,
        split_manifest_sha256=str(execution_contract["split_manifest_sha256"]),
        expected_manifest_file_sha256=str(
            phase_b_input["train_price_manifest_sha256"]
        ),
    )
    if int(field_manifest["sidecar_rows"]) != int(price_manifest["sidecar_rows"]):
        raise RuntimeError("Phase C feature/price row-count drift")

    capacity = _read_json(capacity_path)
    capacity_body = dict(capacity)
    capacity_expected = str(capacity_body.pop("capacity_manifest_sha256", ""))
    if capacity_expected != stable_hash(capacity_body):
        raise RuntimeError("node resource capacity self-hash drift")
    expected_capacity_file_sha256 = str(
        contract.get("node_resource_capacity_file_sha256")
        or phase_b_input["node_resource_capacity_sha256"]
    )
    if _sha256(capacity_path) != expected_capacity_file_sha256:
        raise RuntimeError("Phase C node resource authority differs from frozen contract")
    resource_profile = str(contract.get("resource_profile") or "VALIDATION_EXCLUSIVE_32")
    profile = dict(capacity.get("profiles", {}).get(resource_profile) or {})
    if (
        int(profile.get("cpu_threads") or 0) != 32
        or int(profile.get("minimum_free_memory_bytes") or 0)
        != MINIMUM_FREE_MEMORY_BYTES
        or int(args.executor_workers) != 10
        or int(getattr(args, "checkpoint_worker_cap", 8))
        not in CHECKPOINT_WORKER_CHOICES
        or int(getattr(args, "checkpoint_worker_cap", 8))
        > int(args.executor_workers)
    ):
        raise RuntimeError("Phase C process resource contract drift")
    for candidate_root in (train_field_root, train_price_root):
        normalized = str(candidate_root).replace("/", "\\").lower()
        if any(
            token in normalized
            for token in (
                "validation",
                "holdout",
                "historical_challenge_2023",
                "forward_b",
                "forward_2026",
            )
        ):
            raise RuntimeError("Phase C input root is not development-only")

    registry = UnifiedCapabilityRegistry.read(registry_path)
    catalog, catalog_report = _build_catalog(
        reservoir=reservoir,
        component_rows=component_rows,
        registry=registry,
    )
    components_by_id = {
        component.component_id: component
        for component in (_component_from_row(row) for row in component_rows)
    }
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    initial_bandit = _read_json(initial_bandit_path)
    ProgramFactorizedBanditV0.restore(initial_bandit)
    windows = tuple(
        dict(row)
        for row in _read_json(
            Path(str(contract["phase_b_freeze_root"])) / "phase_b_run_contract.json"
        )["development_subwindows"]
    )

    input_binding = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_input_binding_v0",
            "status": "JOINT_PROGRAM_PHASE_C_INPUTS_BOUND",
            "runner_repo_sha": checkpoint_builder_sha,
            "freeze_repo_sha": str(freeze["repo_sha"]),
            "phase_c_prefinancial_closure_file_sha256": _sha256(
                freeze_root / FREEZE_CLOSURE_NAME
            ),
            "phase_c_prefinancial_closure_payload_sha256": str(
                freeze["closure_sha256"]
            ),
            "phase_c_run_contract_file_sha256": _sha256(
                freeze_root / "phase_c_run_contract.json"
            ),
            "phase_c_run_contract_payload_sha256": str(
                contract["run_contract_sha256"]
            ),
            "ask_plan_file_sha256": _sha256(ask_path),
            "raw_reservoir_file_sha256": _sha256(reservoir_path),
            "component_pool_file_sha256": _sha256(component_path),
            "initial_bandit_state_file_sha256": _sha256(initial_bandit_path),
            "initial_bandit_state_payload_sha256": str(
                initial_bandit["bandit_state_sha256"]
            ),
            "phase_b_input_binding_sha256": phase_b_input_hash,
            "execution_contract_sha256": _sha256(execution_contract_path),
            "train_field_root": str(train_field_root),
            "train_field_manifest_sha256": _sha256(field_manifest_path),
            "train_field_manifest_payload_sha256": str(
                field_manifest["manifest_hash"]
            ),
            "train_price_root": str(train_price_root),
            "train_price_manifest_sha256": _sha256(price_manifest_path),
            "train_price_manifest_payload_sha256": stable_hash(price_manifest),
            "registry_sha256": _sha256(registry_path),
            "node_resource_capacity_sha256": _sha256(capacity_path),
            "decoder_policy": {
                **asdict(phase_b.DECODER),
                "payload_sha256": phase_b.DECODER.payload_sha256,
            },
            "record_count": EXPECTED_RECORDS,
            "checkpoint_count": CHECKPOINT_COUNT,
            "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
            "executor_backend": "PROCESS_POOL",
            "executor_workers": int(args.executor_workers),
            "executor_lifecycle": "CHECKPOINT_SCOPED_RECYCLE",
            "maximum_inflight_records": RECORDS_PER_CHECKPOINT,
            "native_threads_per_worker": 1,
            "campaign_local_bandit_feedback": True,
            "formal_optimizer_authority_write": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
        },
        "input_binding_sha256",
    )
    input_hash = str(input_binding["input_binding_sha256"])
    recovery_binding_path: Path | None = None
    recovery_binding: dict[str, Any] | None = None
    if recovery_mode:
        if len(recovery_from_sha) != 40:
            raise RuntimeError("Phase C checkpoint-builder SHA is not full length")
        if not root.is_dir() or (root / CLOSURE_NAME).exists():
            raise RuntimeError("Phase C root-finalization recovery root is not eligible")
        checkpoints = root / "checkpoints"
        if any(
            not (
                checkpoints
                / f"checkpoint_{checkpoint_index + 1:03d}"
                / "batch_manifest.json"
            ).is_file()
            for checkpoint_index in range(CHECKPOINT_COUNT)
        ):
            raise RuntimeError(
                "Phase C root-finalization recovery requires all immutable checkpoints"
            )
        inflight = root / "inflight"
        if inflight.exists() and any(inflight.rglob("*.json")):
            raise RuntimeError("Phase C root-finalization recovery found inflight results")
        recovery_incident = Path(recovery_incident_arg).resolve()
        incident_payload = _read_json(recovery_incident)
        incident_body = dict(incident_payload)
        incident_hash = str(incident_body.pop("incident_payload_sha256", ""))
        if (
            incident_hash != stable_hash(incident_body)
            or str(incident_payload.get("output_root", "")).replace("/", "\\").lower()
            != str(root).replace("/", "\\").lower()
            or int(incident_payload.get("closed_checkpoint_count", 0))
            != CHECKPOINT_COUNT
            or int(incident_payload.get("closed_record_count", 0))
            != EXPECTED_RECORDS
        ):
            raise RuntimeError("Phase C root-finalization incident binding drift")
        recovery_deployment = Path(recovery_deployment_arg).resolve()
        deployment_payload = _read_json(recovery_deployment)
        if (
            str(deployment_payload.get("repo_sha", "")) != str(args.builder_commit_sha)
            or str(deployment_payload.get("remote_workspace", "")).replace(
                "/", "\\"
            ).lower()
            != str(PROJECT_ROOT).replace("/", "\\").lower()
        ):
            raise RuntimeError("Phase C root-finalizer deployment binding drift")
        recovery_binding = _self_hashed(
            {
                "schema_version": "cn_joint_program_phase_c_root_finalization_recovery_v0",
                "status": "ROOT_FINALIZATION_RECOVERY_BOUND",
                "checkpoint_builder_repo_sha": checkpoint_builder_sha,
                "root_finalizer_repo_sha": str(args.builder_commit_sha),
                "deployment_manifest": str(recovery_deployment),
                "deployment_manifest_file_sha256": _sha256(recovery_deployment),
                "incident": str(recovery_incident),
                "incident_file_sha256": _sha256(recovery_incident),
                "incident_payload_sha256": incident_hash,
                "closed_checkpoint_count": CHECKPOINT_COUNT,
                "closed_record_count": EXPECTED_RECORDS,
                "financial_evaluation_executed": False,
                "incomplete_results_reused": False,
                "validation_reads": 0,
                "holdout_reads": 0,
                "historical_2023_reads": 0,
                "forward_b_reads": 0,
                "forward_2026_reads": 0,
            },
            "recovery_binding_sha256",
        )
        recovery_binding_path = (
            root
            / "root_finalization_recovery_attempts"
            / f"{recovery_binding['recovery_binding_sha256']}.json"
        )
        if recovery_binding_path.is_file():
            if _read_json(recovery_binding_path) != recovery_binding:
                raise RuntimeError("Phase C root-finalization recovery receipt drift")
    if root.exists():
        existing = root / "input_binding.json"
        if existing.is_file():
            if _read_json(existing) != input_binding:
                raise RuntimeError("Phase C same-root recovery input binding drift")
        else:
            allowed_bootstrap = {
                "deployment_binding.json",
                "joint_program_phase_c.stdout.log",
                "joint_program_phase_c.stderr.log",
                "resource_leases",
            }
            unexpected = sorted(
                path.name for path in root.iterdir() if path.name not in allowed_bootstrap
            )
            if unexpected:
                raise RuntimeError(
                    f"Phase C bootstrap root contains unexpected files: {unexpected}"
                )
            _write_json(existing, input_binding)
    else:
        root.mkdir(parents=True)
        _write_json(root / "input_binding.json", input_binding)
    if recovery_binding_path is not None and not recovery_binding_path.is_file():
        assert recovery_binding is not None
        _write_json(recovery_binding_path, recovery_binding)
    catalog_path = root / "catalog_preflight.json"
    if catalog_path.is_file():
        if _read_json(catalog_path) != catalog_report:
            raise RuntimeError("Phase C catalog preflight recovery drift")
    else:
        _write_json(catalog_path, catalog_report)

    inflight_root = root / "inflight"
    if inflight_root.exists() and any(inflight_root.iterdir()):
        raise RuntimeError(
            "Phase C contains incomplete checkpoint results; preserve them before recovery"
        )
    checkpoints_root = root / "checkpoints"
    checkpoints_root.mkdir(exist_ok=True)
    selection_state = _selection_state()
    behavior_counts: Counter[str] = Counter()
    bandit = ProgramFactorizedBanditV0.restore(initial_bandit)
    previous_manifest: Path | None = None
    closed_checkpoints = 0
    records: list[dict[str, Any]] = []
    for checkpoint_index in range(CHECKPOINT_COUNT):
        checkpoint_id = f"checkpoint_{checkpoint_index + 1:03d}"
        checkpoint_root = checkpoints_root / checkpoint_id
        if not checkpoint_root.exists():
            break
        checkpoint_asks = asks[
            checkpoint_index * RECORDS_PER_CHECKPOINT :
            (checkpoint_index + 1) * RECORDS_PER_CHECKPOINT
        ]
        expected_schedules, expected_decisions = _select_checkpoint(
            checkpoint_asks,
            catalog=catalog,
            bandit=bandit,
            state=selection_state,
            components_by_id=components_by_id,
            adapter=adapter,
            compiler=compiler,
        )
        previous_manifest, checkpoint_rows, bandit = _verify_checkpoint(
            checkpoint_root,
            checkpoint_id=checkpoint_id,
            previous_manifest=previous_manifest,
            input_hash=input_hash,
            expected_schedules=expected_schedules,
            expected_decisions=expected_decisions,
            bandit=bandit,
            behavior_counts=behavior_counts,
        )
        records.extend(checkpoint_rows)
        closed_checkpoints += 1
    if any(
        (checkpoints_root / f"checkpoint_{index + 1:03d}").exists()
        for index in range(closed_checkpoints + 1, CHECKPOINT_COUNT)
    ):
        raise RuntimeError("Phase C checkpoint chain contains a gap")

    checkpoint_recovery_binding_path: Path | None = None
    checkpoint_recovery_binding: dict[str, Any] | None = None
    checkpoint_recovery_history_paths: list[Path] = []
    closed_checkpoints_at_recovery_start = closed_checkpoints
    if checkpoint_recovery_mode:
        if len(checkpoint_recovery_from_sha) != 40:
            raise RuntimeError("Phase C checkpoint-builder SHA is not full length")
        if not root.is_dir() or (root / CLOSURE_NAME).exists():
            raise RuntimeError("Phase C checkpoint recovery root is not eligible")
        if closed_checkpoints < 1 or closed_checkpoints >= CHECKPOINT_COUNT:
            raise RuntimeError("Phase C checkpoint recovery boundary is not partial")
        if inflight_root.exists() and any(inflight_root.iterdir()):
            raise RuntimeError("Phase C checkpoint recovery found inflight results")

        checkpoint_recovery_incident = Path(
            checkpoint_recovery_incident_arg
        ).resolve()
        recovery_history = _checkpoint_recovery_history(root)
        incident_payload = _read_json(checkpoint_recovery_incident)
        incident_hash = _verify_payload_hash(
            incident_payload,
            "incident_payload_sha256",
            "Phase C checkpoint recovery incident",
        )
        incident_root, incident_checkpoint_count = (
            _resolve_checkpoint_recovery_incident_boundary(incident_payload)
        )
        recovery_scope = str(incident_payload.get("recovery_scope") or "")
        failure_classification = str(
            incident_payload.get("failure_classification") or ""
        )
        if (
            recovery_scope
            not in {CHECKPOINT_RECOVERY_SCOPE, LEGACY_CHECKPOINT_RECOVERY_SCOPE}
            or not _same_path(incident_root, root)
            or incident_checkpoint_count != closed_checkpoints
            or str(incident_payload.get("checkpoint_builder_repo_sha") or "")
            != checkpoint_recovery_from_sha
            or not bool(incident_payload.get("checkpoint_recomputation_authorized"))
            or int(incident_payload.get("closed_record_count") or 0)
            != closed_checkpoints * RECORDS_PER_CHECKPOINT
            or int(incident_payload.get("first_recovered_checkpoint") or 0)
            != closed_checkpoints + 1
            or bool(incident_payload.get("incomplete_results_reused"))
        ):
            raise RuntimeError("Phase C checkpoint recovery incident binding drift")
        _require_zero_restricted_reads(
            incident_payload, "Phase C checkpoint recovery incident"
        )
        if recovery_scope == CHECKPOINT_RECOVERY_SCOPE and (
            not failure_classification
            or str(
                incident_payload.get("continuation_implementation_repo_sha") or ""
            )
            != str(args.builder_commit_sha)
            or bool(incident_payload.get("financial_results_reusable"))
            or int(incident_payload.get("optimizer_tell_count") or 0) != 0
        ):
            raise RuntimeError("Phase C checkpoint recovery incident contract drift")

        if previous_manifest is None:
            raise RuntimeError("Phase C checkpoint recovery has no closed manifest")
        previous_manifest_payload = _read_json(previous_manifest)
        previous_manifest_hash = _verify_payload_hash(
            previous_manifest_payload,
            "manifest_payload_hash",
            "Phase C last closed checkpoint manifest",
        )
        if recovery_scope == CHECKPOINT_RECOVERY_SCOPE and (
            not _same_path(
                str(incident_payload.get("last_closed_checkpoint_manifest") or ""),
                previous_manifest,
            )
            or str(
                incident_payload.get("last_closed_checkpoint_manifest_file_sha256")
                or ""
            )
            != _sha256(previous_manifest)
            or str(
                incident_payload.get(
                    "last_closed_checkpoint_manifest_payload_sha256"
                )
                or ""
            )
            != previous_manifest_hash
        ):
            raise RuntimeError("Phase C last closed checkpoint binding drift")

        diagnostic_audit = Path(
            checkpoint_recovery_diagnostic_audit_arg
        ).resolve()
        diagnostic_payload = _read_json(diagnostic_audit)
        diagnostic_hash = _verify_payload_hash(
            diagnostic_payload,
            "audit_payload_sha256",
            "Phase C checkpoint recovery diagnostic",
        )
        expected_failure_classification = (
            failure_classification
            if recovery_scope == CHECKPOINT_RECOVERY_SCOPE
            else "PARALLEL_PROCESS_LIFECYCLE_OR_NATIVE_CONCURRENCY"
        )
        if (
            str(diagnostic_payload.get("status", "")) != "PASS"
            or str(diagnostic_payload.get("classification", ""))
            != expected_failure_classification
            or not _same_path(
                str(diagnostic_payload.get("accepted_root", "")), root
            )
            or str(diagnostic_payload.get("runner_repo_sha", ""))
            != checkpoint_recovery_from_sha
            or int(diagnostic_payload.get("record_count", 0)) != RECORDS_PER_CHECKPOINT
            or bool(diagnostic_payload.get("diagnostic_financial_results_reusable"))
            or bool(diagnostic_payload.get("financial_results_reused"))
        ):
            raise RuntimeError("Phase C checkpoint diagnostic binding drift")
        _require_zero_restricted_reads(
            diagnostic_payload, "Phase C checkpoint recovery diagnostic"
        )
        if recovery_scope == CHECKPOINT_RECOVERY_SCOPE and (
            int(diagnostic_payload.get("closed_checkpoint_count") or 0)
            != closed_checkpoints
            or int(diagnostic_payload.get("closed_record_count") or 0)
            != closed_checkpoints * RECORDS_PER_CHECKPOINT
            or int(diagnostic_payload.get("first_recovered_checkpoint") or 0)
            != closed_checkpoints + 1
            or str(
                diagnostic_payload.get("continuation_implementation_repo_sha")
                or ""
            )
            != str(args.builder_commit_sha)
            or bool(diagnostic_payload.get("financial_results_reusable"))
            or bool(diagnostic_payload.get("incomplete_results_reused"))
            or int(diagnostic_payload.get("optimizer_tell_count") or 0) != 0
        ):
            raise RuntimeError("Phase C checkpoint diagnostic boundary drift")

        checkpoint_recovery_deployment = Path(
            checkpoint_recovery_deployment_arg
        ).resolve()
        deployment_payload = _read_json(checkpoint_recovery_deployment)
        if (
            str(deployment_payload.get("repo_sha", ""))
            != str(args.builder_commit_sha)
            or str(deployment_payload.get("remote_workspace", "")).replace(
                "/", "\\"
            ).lower()
            != str(PROJECT_ROOT).replace("/", "\\").lower()
        ):
            raise RuntimeError("Phase C checkpoint recovery deployment binding drift")

        previous_recovery_path: Path | None = None
        previous_recovery_payload: dict[str, Any] | None = None
        quarantine_path: Path | None = None
        quarantine_hash = ""
        if recovery_scope == CHECKPOINT_RECOVERY_SCOPE:
            if not recovery_history:
                raise RuntimeError(
                    "Phase C generic checkpoint continuation requires prior recovery"
                )
            previous_recovery_path, previous_recovery_payload = recovery_history[-1]
            quarantine_path = Path(
                str(incident_payload.get("quarantine_manifest") or "")
            ).resolve()
            _, quarantine_hash = _verify_checkpoint_quarantine(
                quarantine_path,
                root=root,
                checkpoint_number=closed_checkpoints + 1,
            )
            exact_incident_bindings = (
                _same_path(
                    str(incident_payload.get("diagnostic_audit") or ""),
                    diagnostic_audit,
                )
                and str(
                    incident_payload.get("diagnostic_audit_file_sha256") or ""
                )
                == _sha256(diagnostic_audit)
                and str(
                    incident_payload.get("diagnostic_audit_payload_sha256") or ""
                )
                == diagnostic_hash
                and _same_path(
                    str(incident_payload.get("deployment_manifest") or ""),
                    checkpoint_recovery_deployment,
                )
                and str(
                    incident_payload.get("deployment_manifest_file_sha256") or ""
                )
                == _sha256(checkpoint_recovery_deployment)
                and _same_path(
                    str(incident_payload.get("previous_recovery_binding") or ""),
                    previous_recovery_path,
                )
                and str(
                    incident_payload.get(
                        "previous_recovery_binding_file_sha256"
                    )
                    or ""
                )
                == _sha256(previous_recovery_path)
                and str(
                    incident_payload.get(
                        "previous_recovery_binding_payload_sha256"
                    )
                    or ""
                )
                == str(previous_recovery_payload["recovery_binding_sha256"])
                and _same_path(
                    str(incident_payload.get("quarantine_manifest") or ""),
                    quarantine_path,
                )
                and str(
                    incident_payload.get("quarantine_manifest_file_sha256") or ""
                )
                == _sha256(quarantine_path)
                and str(
                    incident_payload.get("quarantine_manifest_payload_sha256")
                    or ""
                )
                == quarantine_hash
            )
            if not exact_incident_bindings:
                raise RuntimeError("Phase C checkpoint incident artifact binding drift")

        checkpoint_recovery_body = {
                "schema_version": "cn_joint_program_phase_c_checkpoint_recovery_v0",
                "status": "CHECKPOINT_RECOVERY_BOUND",
                "recovery_scope": recovery_scope,
                "failure_classification": expected_failure_classification,
                "checkpoint_builder_repo_sha": checkpoint_recovery_from_sha,
                "checkpoint_recovery_repo_sha": str(args.builder_commit_sha),
                "deployment_manifest": str(checkpoint_recovery_deployment),
                "deployment_manifest_file_sha256": _sha256(
                    checkpoint_recovery_deployment
                ),
                "incident": str(checkpoint_recovery_incident),
                "incident_file_sha256": _sha256(checkpoint_recovery_incident),
                "incident_payload_sha256": incident_hash,
                "diagnostic_audit": str(diagnostic_audit),
                "diagnostic_audit_file_sha256": _sha256(diagnostic_audit),
                "diagnostic_audit_payload_sha256": diagnostic_hash,
                "last_closed_checkpoint_manifest": str(previous_manifest),
                "last_closed_checkpoint_manifest_file_sha256": _sha256(
                    previous_manifest
                ),
                "last_closed_checkpoint_manifest_payload_sha256": (
                    previous_manifest_hash
                ),
                "closed_checkpoint_count": closed_checkpoints,
                "closed_record_count": closed_checkpoints * RECORDS_PER_CHECKPOINT,
                "first_recovered_checkpoint": closed_checkpoints + 1,
                "first_isolated_recovery_checkpoint": closed_checkpoints + 1,
                "isolated_recovery_checkpoint_count": 1,
                "recovery_isolation_scope": "FIRST_RECOVERED_CHECKPOINT_ONLY",
                "adaptive_continuation_enabled": True,
                "executor_mode": CHECKPOINT_RECOVERY_EXECUTOR_MODE,
                "executor_worker_authority": int(args.executor_workers),
                "effective_concurrent_workers": 1,
                "max_tasks_per_child": 1,
                "financial_results_reused": False,
                "diagnostic_financial_results_reused": False,
                "incomplete_results_reused": False,
                "validation_reads": 0,
                "holdout_reads": 0,
                "historical_2023_reads": 0,
                "forward_b_reads": 0,
                "forward_2026_reads": 0,
            }
        if previous_recovery_path is not None and previous_recovery_payload is not None:
            checkpoint_recovery_body.update(
                {
                    "previous_recovery_binding": str(previous_recovery_path),
                    "previous_recovery_binding_file_sha256": _sha256(
                        previous_recovery_path
                    ),
                    "previous_recovery_binding_payload_sha256": str(
                        previous_recovery_payload["recovery_binding_sha256"]
                    ),
                    "quarantine_manifest": str(quarantine_path),
                    "quarantine_manifest_file_sha256": _sha256(quarantine_path),
                    "quarantine_manifest_payload_sha256": quarantine_hash,
                }
            )
        checkpoint_recovery_binding = _self_hashed(
            checkpoint_recovery_body,
            "recovery_binding_sha256",
        )
        checkpoint_recovery_binding_path = (
            root
            / "checkpoint_recovery_attempts"
            / f"{checkpoint_recovery_binding['recovery_binding_sha256']}.json"
            if recovery_history
            else root / "checkpoint_recovery_binding.json"
        )
        if checkpoint_recovery_binding_path.is_file():
            if _read_json(checkpoint_recovery_binding_path) != checkpoint_recovery_binding:
                raise RuntimeError("Phase C checkpoint recovery receipt drift")
        else:
            _write_json(
                checkpoint_recovery_binding_path, checkpoint_recovery_binding
            )
        checkpoint_recovery_history_paths = [
            path for path, _ in _checkpoint_recovery_history(root)
        ]

    if closed_checkpoints < CHECKPOINT_COUNT:
        inflight_root.mkdir(parents=True, exist_ok=True)
        for checkpoint_index in range(closed_checkpoints, CHECKPOINT_COUNT):
            checkpoint_id = f"checkpoint_{checkpoint_index + 1:03d}"
            checkpoint_asks = asks[
                checkpoint_index * RECORDS_PER_CHECKPOINT :
                (checkpoint_index + 1) * RECORDS_PER_CHECKPOINT
            ]
            bandit_before = bandit.snapshot()
            checkpoint_schedules, checkpoint_decisions = _select_checkpoint(
                checkpoint_asks,
                catalog=catalog,
                bandit=bandit,
                state=selection_state,
                components_by_id=components_by_id,
                adapter=adapter,
                compiler=compiler,
            )
            checkpoint_inflight = inflight_root / checkpoint_id
            record_root = checkpoint_inflight / "records"
            record_root.mkdir(parents=True, exist_ok=True)
            ordinals = {
                int(row["main_record_ordinal"]) for row in checkpoint_schedules
            }
            completed: set[int] = set()
            futures = {}
            checkpoint_field_columns = _checkpoint_field_columns(checkpoint_schedules)
            resource_before = _runtime_resource_snapshot()
            _require_runtime_resource_safety(resource_before)
            executor_plan = _checkpoint_executor_plan(
                checkpoint_index=checkpoint_index,
                recovery_start_checkpoint_index=(
                    closed_checkpoints_at_recovery_start
                ),
                checkpoint_recovery_mode=checkpoint_recovery_mode,
                worker_cap=int(getattr(args, "checkpoint_worker_cap", 8)),
                schedule_count=len(checkpoint_schedules),
                commit_headroom_bytes=int(resource_before["commit_headroom_bytes"]),
            )
            effective_workers = int(
                executor_plan["effective_checkpoint_workers"]
            )
            minimum_observed_free = int(
                resource_before["available_physical_bytes"]
            )
            minimum_commit_headroom = int(
                resource_before["commit_headroom_bytes"]
            )
            maximum_committed = int(resource_before["committed_bytes"])
            maximum_pagefile_used = int(resource_before["pagefile_used_bytes"])
            maximum_parent_rss = int(resource_before["parent_rss_bytes"])
            maximum_tree_rss = int(resource_before["process_tree_rss_bytes"])
            children_before = {
                child.pid
                for child in psutil.Process().children(recursive=True)
            }
            cpu_samples: list[float] = []
            psutil.cpu_percent(interval=None)
            started = time.perf_counter()
            executor_options: dict[str, Any] = {
                "max_workers": min(int(args.executor_workers), effective_workers),
                "initializer": _initialize_worker,
                "initargs": (
                    str(execution_contract_path),
                    str(train_field_root),
                    str(train_price_root),
                    price_manifest,
                    str(price_manifest_path),
                    str(registry_path),
                    input_hash,
                    windows,
                    field_manifest_file_sha256,
                    field_manifest_payload_sha256,
                    checkpoint_field_columns,
                ),
            }
            if executor_plan["max_tasks_per_child"] is not None:
                executor_options.update(
                    {"max_workers": 1, "max_tasks_per_child": 1}
                )
            with ProcessPoolExecutor(**executor_options) as executor:
                for schedule in checkpoint_schedules:
                    ordinal = int(schedule["main_record_ordinal"])
                    target = record_root / f"record_{ordinal:04d}.json"
                    futures[
                        executor.submit(_evaluate_record, schedule, str(target))
                    ] = ordinal
                pending = set(futures)
                while pending:
                    done, pending = wait(
                        pending, timeout=2.0, return_when=FIRST_COMPLETED
                    )
                    for future in done:
                        payload = future.result()
                        completed.add(int(payload["main_record_ordinal"]))
                    resource_sample = _runtime_resource_snapshot()
                    _require_runtime_resource_safety(resource_sample)
                    minimum_observed_free = min(
                        minimum_observed_free,
                        int(resource_sample["available_physical_bytes"]),
                    )
                    minimum_commit_headroom = min(
                        minimum_commit_headroom,
                        int(resource_sample["commit_headroom_bytes"]),
                    )
                    maximum_committed = max(
                        maximum_committed,
                        int(resource_sample["committed_bytes"]),
                    )
                    maximum_pagefile_used = max(
                        maximum_pagefile_used,
                        int(resource_sample["pagefile_used_bytes"]),
                    )
                    maximum_parent_rss = max(
                        maximum_parent_rss,
                        int(resource_sample["parent_rss_bytes"]),
                    )
                    maximum_tree_rss = max(
                        maximum_tree_rss,
                        int(resource_sample["process_tree_rss_bytes"]),
                    )
                    cpu_samples.append(float(psutil.cpu_percent(interval=None)))
            gc.collect()
            resource_after = _runtime_resource_snapshot()
            _require_runtime_resource_safety(resource_after)
            orphan_worker_pids = _new_child_process_ids(children_before)
            if orphan_worker_pids:
                raise RuntimeError(
                    "Phase C checkpoint left orphan worker processes: "
                    + ",".join(map(str, orphan_worker_pids))
                )
            wall_seconds = float(time.perf_counter() - started)
            minimum_observed_free = min(
                minimum_observed_free,
                int(resource_after["available_physical_bytes"]),
            )
            minimum_commit_headroom = min(
                minimum_commit_headroom,
                int(resource_after["commit_headroom_bytes"]),
            )
            maximum_committed = max(
                maximum_committed,
                int(resource_after["committed_bytes"]),
            )
            maximum_pagefile_used = max(
                maximum_pagefile_used,
                int(resource_after["pagefile_used_bytes"]),
            )
            maximum_parent_rss = max(
                maximum_parent_rss,
                int(resource_after["parent_rss_bytes"]),
            )
            maximum_tree_rss = max(
                maximum_tree_rss,
                int(resource_after["process_tree_rss_bytes"]),
            )
            cpu_samples.append(float(psutil.cpu_percent(interval=None)))
            if completed != ordinals:
                raise RuntimeError(
                    f"Phase C checkpoint completion drift: {checkpoint_id}"
                )
            schedule_by_ordinal = {
                int(row["main_record_ordinal"]): row for row in checkpoint_schedules
            }
            checkpoint_records = [
                _verify_record(
                    path,
                    expected_input_hash=input_hash,
                    schedule=schedule_by_ordinal[int(path.stem.split("_")[-1])],
                )
                for path in sorted(record_root.glob("record_*.json"))
            ]
            feedback = _feedback_update(
                checkpoint_records,
                checkpoint_schedules,
                bandit=bandit,
                behavior_counts=behavior_counts,
            )
            previous_manifest = _close_checkpoint(
                checkpoint_inflight,
                checkpoints_root / checkpoint_id,
                checkpoint_id=checkpoint_id,
                previous_manifest=previous_manifest,
                input_hash=input_hash,
                freeze_closure_sha256=_sha256(freeze_root / FREEZE_CLOSURE_NAME),
                ask_plan_sha256=_sha256(ask_path),
                reservoir_sha256=_sha256(reservoir_path),
                schedules=checkpoint_schedules,
                decisions=checkpoint_decisions,
                bandit_before=bandit_before,
                bandit_after=bandit.snapshot(),
                feedback=feedback,
                telemetry={
                    "wall_seconds": wall_seconds,
                    "minimum_checkpoint_boundary_free_memory_bytes": int(
                        resource_after["available_physical_bytes"]
                    ),
                    "minimum_observed_in_pool_free_memory_bytes": minimum_observed_free,
                    "minimum_commit_headroom_bytes": minimum_commit_headroom,
                    "maximum_committed_bytes": maximum_committed,
                    "commit_limit_bytes": int(resource_after["commit_limit_bytes"]),
                    "maximum_pagefile_used_bytes": maximum_pagefile_used,
                    "pagefile_total_bytes": int(resource_after["pagefile_total_bytes"]),
                    "pagefile_pages_in_delta_bytes": max(
                        0,
                        int(resource_after["pagefile_pages_in_bytes"])
                        - int(resource_before["pagefile_pages_in_bytes"]),
                    ),
                    "pagefile_pages_out_delta_bytes": max(
                        0,
                        int(resource_after["pagefile_pages_out_bytes"])
                        - int(resource_before["pagefile_pages_out_bytes"]),
                    ),
                    "post_pool_available_physical_bytes": int(
                        resource_after["available_physical_bytes"]
                    ),
                    "post_pool_commit_headroom_bytes": int(
                        resource_after["commit_headroom_bytes"]
                    ),
                    "orphan_worker_pids": orphan_worker_pids,
                    "executor_mode": str(executor_plan["executor_mode"]),
                    "effective_checkpoint_workers": effective_workers,
                    "max_tasks_per_child": executor_plan[
                        "max_tasks_per_child"
                    ],
                    "checkpoint_recovery_provenance": bool(
                        executor_plan["checkpoint_recovery_provenance"]
                    ),
                    "checkpoint_field_column_count": len(checkpoint_field_columns),
                    "checkpoint_field_columns_sha256": stable_hash(
                        list(checkpoint_field_columns)
                    ),
                    "maximum_parent_rss_bytes": maximum_parent_rss,
                    "maximum_process_tree_rss_bytes": maximum_tree_rss,
                    "mean_host_cpu_percent": (
                        float(np.mean(cpu_samples)) if cpu_samples else 0.0
                    ),
                },
            )
        if inflight_root.exists() and not any(inflight_root.iterdir()):
            inflight_root.rmdir()

    previous_manifest = None
    bandit = ProgramFactorizedBanditV0.restore(initial_bandit)
    behavior_counts = Counter()
    selection_state = _selection_state()
    records = []
    schedules: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    feedback_rows: list[dict[str, Any]] = []
    checkpoint_manifests: list[Path] = []
    checkpoint_summaries: list[dict[str, Any]] = []
    for checkpoint_index in range(CHECKPOINT_COUNT):
        checkpoint_id = f"checkpoint_{checkpoint_index + 1:03d}"
        checkpoint_root = checkpoints_root / checkpoint_id
        checkpoint_asks = asks[
            checkpoint_index * RECORDS_PER_CHECKPOINT :
            (checkpoint_index + 1) * RECORDS_PER_CHECKPOINT
        ]
        expected_schedules, expected_decisions = _select_checkpoint(
            checkpoint_asks,
            catalog=catalog,
            bandit=bandit,
            state=selection_state,
            components_by_id=components_by_id,
            adapter=adapter,
            compiler=compiler,
        )
        previous_manifest, checkpoint_records, bandit = _verify_checkpoint(
            checkpoint_root,
            checkpoint_id=checkpoint_id,
            previous_manifest=previous_manifest,
            input_hash=input_hash,
            expected_schedules=expected_schedules,
            expected_decisions=expected_decisions,
            bandit=bandit,
            behavior_counts=behavior_counts,
        )
        checkpoint_manifests.append(previous_manifest)
        records.extend(checkpoint_records)
        schedules.extend(expected_schedules)
        decisions.extend(expected_decisions)
        feedback_rows.extend(
            _read_jsonl(checkpoint_root / "bandit_feedback_ledger.jsonl")
        )
        checkpoint_summaries.append(
            _read_json(checkpoint_root / "checkpoint_summary.json")
        )
    records.sort(key=lambda row: int(row["main_record_ordinal"]))
    schedules.sort(key=lambda row: int(row["main_record_ordinal"]))
    if len(records) != EXPECTED_RECORDS or len(schedules) != EXPECTED_RECORDS:
        raise RuntimeError("Phase C root result count drift")
    base_parity_pass, base_parity_blocked = _verify_base_parity_root_gate(records)
    for template_id in ENHANCED_TEMPLATE_ORDER:
        template_schedules = [
            row for row in schedules if str(row["template_id"]) == template_id
        ]
        counts = Counter(
            str(row["components"]["base"]["component_id"])
            for row in template_schedules
        )
        if (
            len(counts) < MIN_BASE_IDENTITIES_PER_TEMPLATE
            or max(counts.values()) > MAX_VARIANTS_PER_BASE_PER_TEMPLATE
        ):
            raise RuntimeError(f"Phase C base-diversity gate failed: {template_id}")

    result_path = _write_jsonl(root / "phase_c_record_results.jsonl", records)
    schedule_path = _write_jsonl(root / "phase_c_selected_schedule.jsonl", schedules)
    decision_path = _write_jsonl(root / "phase_c_selection_ledger.jsonl", decisions)
    feedback_path = _write_jsonl(
        root / "phase_c_bandit_feedback_ledger.jsonl", feedback_rows
    )
    final_bandit_path = _write_json(root / "final_bandit_state.json", bandit.snapshot())
    template_path = _write_json(
        root / "template_productivity.json",
        _self_hashed(
            {
                "schema_version": "cn_joint_program_phase_c_template_productivity_v0",
                "status": "PHASE_C_TEMPLATE_PRODUCTIVITY_REPORTED",
                "templates": _template_summary(records),
            },
            "template_productivity_payload_sha256",
        ),
    )
    blockers = Counter(
        blocker for row in records for blocker in row.get("blockers") or ()
    )
    blocker_path = _write_json(
        root / "blocker_taxonomy.json",
        _self_hashed(
            {
                "schema_version": "cn_joint_program_phase_c_blockers_v0",
                "status": "PHASE_C_BLOCKERS_REPORTED",
                "counts": dict(sorted(blockers.items())),
            },
            "blocker_taxonomy_payload_sha256",
        ),
    )
    wall_seconds = sum(float(row["wall_seconds"]) for row in checkpoint_summaries)
    minimum_boundary_free = min(
        int(row["minimum_checkpoint_boundary_free_memory_bytes"])
        for row in checkpoint_summaries
    )
    minimum_observed_free = min(
        int(row["minimum_observed_in_pool_free_memory_bytes"])
        for row in checkpoint_summaries
    )
    maximum_parent_rss = max(
        int(row["maximum_parent_rss_bytes"]) for row in checkpoint_summaries
    )
    maximum_tree_rss = max(
        int(row["maximum_process_tree_rss_bytes"]) for row in checkpoint_summaries
    )
    cpu_mean = float(
        np.mean([float(row["mean_host_cpu_percent"]) for row in checkpoint_summaries])
    )
    commit_headrooms = [
        int(row["minimum_commit_headroom_bytes"])
        for row in checkpoint_summaries
        if row.get("minimum_commit_headroom_bytes") is not None
    ]
    maximum_committed_values = [
        int(row["maximum_committed_bytes"])
        for row in checkpoint_summaries
        if row.get("maximum_committed_bytes") is not None
    ]
    orphan_worker_pids = sorted(
        {
            int(pid)
            for row in checkpoint_summaries
            for pid in row.get("orphan_worker_pids") or ()
        }
    )
    commit_gate_passed = (
        bool(commit_headrooms)
        and min(commit_headrooms) >= MINIMUM_COMMIT_HEADROOM_BYTES
        and not orphan_worker_pids
    )
    legacy_physical_gate_passed = (
        not commit_headrooms and minimum_boundary_free >= MINIMUM_FREE_MEMORY_BYTES
    )
    logical_cpu = int(psutil.cpu_count(logical=True) or 1)
    resource = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_resource_summary_v0",
            "status": (
                "PASS"
                if commit_gate_passed or legacy_physical_gate_passed
                else "FAIL"
            ),
            "runtime_safety_gate": (
                "COMMIT_HEADROOM_AND_POST_POOL_PROCESS_CLEANLINESS"
                if commit_headrooms
                else "LEGACY_PHYSICAL_FREE_MEMORY"
            ),
            "wall_seconds": wall_seconds,
            "main_records_per_hour": EXPECTED_RECORDS / max(wall_seconds / 3600.0, 1e-12),
            "minimum_free_memory_bytes": minimum_boundary_free,
            "minimum_observed_free_memory_bytes": minimum_observed_free,
            "minimum_commit_headroom_bytes": (
                min(commit_headrooms) if commit_headrooms else None
            ),
            "minimum_required_commit_headroom_bytes": (
                MINIMUM_COMMIT_HEADROOM_BYTES if commit_headrooms else None
            ),
            "maximum_committed_bytes": (
                max(maximum_committed_values) if maximum_committed_values else None
            ),
            "maximum_pagefile_used_bytes": max(
                (
                    int(row.get("maximum_pagefile_used_bytes") or 0)
                    for row in checkpoint_summaries
                ),
                default=0,
            ),
            "pagefile_pages_in_delta_bytes": sum(
                int(row.get("pagefile_pages_in_delta_bytes") or 0)
                for row in checkpoint_summaries
            ),
            "pagefile_pages_out_delta_bytes": sum(
                int(row.get("pagefile_pages_out_delta_bytes") or 0)
                for row in checkpoint_summaries
            ),
            "orphan_worker_pids": orphan_worker_pids,
            "maximum_parent_rss_bytes": maximum_parent_rss,
            "maximum_process_tree_rss_bytes": maximum_tree_rss,
            "mean_host_cpu_percent": cpu_mean,
            "mean_effective_cores": cpu_mean * logical_cpu / 100.0,
            "logical_cpu_count": logical_cpu,
            "executor_workers": int(args.executor_workers),
            "checkpoint_worker_cap": int(
                getattr(args, "checkpoint_worker_cap", 8)
            ),
            "maximum_effective_workers_per_checkpoint": max(
                (
                    int(row.get("effective_checkpoint_workers") or 0)
                    for row in checkpoint_summaries
                ),
                default=(
                    1
                    if checkpoint_recovery_mode
                    else min(int(args.executor_workers), RECORDS_PER_CHECKPOINT)
                ),
            ),
            "executor_lifecycle": (
                CHECKPOINT_RECOVERY_EXECUTOR_LIFECYCLE
                if checkpoint_recovery_mode
                else "CHECKPOINT_SCOPED_RECYCLE"
            ),
            "first_isolated_recovery_checkpoint": (
                int(checkpoint_recovery_binding["first_recovered_checkpoint"])
                if checkpoint_recovery_binding is not None
                else None
            ),
            "isolated_recovery_checkpoint_count": (
                1 if checkpoint_recovery_mode else 0
            ),
            "adaptive_continuation_enabled": checkpoint_recovery_mode,
            "maximum_inflight_records": RECORDS_PER_CHECKPOINT,
            "native_threads_per_worker": 1,
        },
        "resource_summary_payload_sha256",
    )
    if str(resource["status"]) != "PASS":
        raise RuntimeError("Phase C resource summary failed")
    resource_path = _write_json(root / "resource_summary.json", resource)
    access_path = _write_json(
        root / "access_ledger.json",
        _self_hashed(
            {
                "schema_version": "cn_joint_program_phase_c_access_ledger_v0",
                "financial_role": "DEVELOPMENT_ONLY",
                "market_price_rows_read": "POSITIVE_DEVELOPMENT_ONLY",
                "validation_reads": 0,
                "holdout_reads": 0,
                "historical_2023_reads": 0,
                "forward_b_reads": 0,
                "forward_2026_reads": 0,
                "campaign_local_bandit_feedback_updates": sum(
                    bool(row["bandit_update_applied"]) for row in feedback_rows
                ),
                "formal_optimizer_feedback_write": "FORBIDDEN",
                "formal_scheduler_write": "FORBIDDEN",
                "archive_write": "FORBIDDEN",
                "promotion": "FORBIDDEN",
            },
            "access_ledger_sha256",
        ),
    )
    root_artifacts = [
        root / "input_binding.json",
        catalog_path,
        result_path,
        schedule_path,
        decision_path,
        feedback_path,
        final_bandit_path,
        template_path,
        blocker_path,
        resource_path,
        access_path,
        *([recovery_binding_path] if recovery_binding_path is not None else []),
        *checkpoint_recovery_history_paths,
        *checkpoint_manifests,
    ]
    legacy_recovery_binding = root / "root_finalization_recovery_binding.json"
    if recovery_binding_path is not None and legacy_recovery_binding.is_file():
        root_artifacts.append(legacy_recovery_binding)
    manifest = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_artifact_manifest_v0",
            "artifacts": [_artifact(path, root=root) for path in root_artifacts],
        },
        "artifact_manifest_sha256",
    )
    manifest_path = _write_json(root / "ARTIFACT_MANIFEST.json", manifest)
    closure = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_c_closure_v0",
            "status": STATUS,
            "output_root": str(root),
            "runner_repo_sha": checkpoint_builder_sha,
            "root_finalizer_repo_sha": str(args.builder_commit_sha),
            "root_finalization_recovery": recovery_mode,
            "checkpoint_recovery": checkpoint_recovery_mode,
            "checkpoint_recovery_executor_mode": (
                CHECKPOINT_RECOVERY_EXECUTOR_LIFECYCLE
                if checkpoint_recovery_mode
                else None
            ),
            "checkpoint_recovery_first_isolated_executor_mode": (
                CHECKPOINT_RECOVERY_EXECUTOR_MODE
                if checkpoint_recovery_mode
                else None
            ),
            "checkpoint_recovery_from_checkpoint": (
                int(checkpoint_recovery_binding["first_recovered_checkpoint"])
                if checkpoint_recovery_binding is not None
                else None
            ),
            "checkpoint_recovery_binding": (
                str(checkpoint_recovery_binding_path)
                if checkpoint_recovery_binding_path is not None
                else None
            ),
            "checkpoint_recovery_binding_sha256": (
                str(checkpoint_recovery_binding["recovery_binding_sha256"])
                if checkpoint_recovery_binding is not None
                else None
            ),
            "checkpoint_recovery_history_count": len(
                checkpoint_recovery_history_paths
            ),
            "checkpoint_recovery_isolated_checkpoint_count": (
                1 if checkpoint_recovery_mode else 0
            ),
            "checkpoint_recovery_adaptive_continuation_enabled": (
                checkpoint_recovery_mode
            ),
            "phase_c_input_binding_sha256": input_hash,
            "record_count": len(records),
            "checkpoint_count": len(checkpoint_manifests),
            "base_parity_pass": base_parity_pass,
            "base_parity_blocked": base_parity_blocked,
            "enhanced_replay_complete": sum(
                str(row["record_kind"]) == "ENHANCED_FULL_BASE_PAIR"
                and phase_b._record_replay_complete(row)
                for row in records
            ),
            "replay_blocked_count": sum(
                not phase_b._record_replay_complete(row) for row in records
            ),
            "productive_count": sum(bool(row["productive"]) for row in records),
            "behavior_unique": len(
                {
                    str(row["primary"]["behavior_identity"])
                    for row in records
                    if phase_b._record_replay_complete(row)
                }
            ),
            "semantic_noop_count": sum(bool(row["semantic_noop"]) for row in records),
            "bandit_initial_observations": int(initial_bandit["observations"]),
            "bandit_final_observations": int(bandit.snapshot()["observations"]),
            "bandit_update_count": sum(
                bool(row["bandit_update_applied"]) for row in feedback_rows
            ),
            "generation_arm_counts": dict(
                sorted(Counter(str(row["generation_arm"]) for row in records).items())
            ),
            "artifact_manifest": _artifact(manifest_path, root=root),
            "artifact_count": len(manifest["artifacts"]),
            "minimum_free_memory_bytes": minimum_boundary_free,
            "minimum_observed_free_memory_bytes": minimum_observed_free,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "automatic_phase_d_launch": False,
            "promotion": "FORBIDDEN",
        },
        "closure_payload_sha256",
    )
    return _read_json(_write_json(root / CLOSURE_NAME, closure))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase-c-freeze-root", required=True, type=Path)
    parser.add_argument("--execution-contract", required=True, type=Path)
    parser.add_argument("--train-field-root", required=True, type=Path)
    parser.add_argument("--train-price-root", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--node-resource-capacity", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--builder-commit-sha", required=True)
    parser.add_argument("--root-finalization-recovery-from-repo-sha")
    parser.add_argument("--root-finalization-incident", type=Path)
    parser.add_argument("--root-finalization-deployment-manifest", type=Path)
    parser.add_argument("--checkpoint-recovery-from-repo-sha")
    parser.add_argument("--checkpoint-recovery-incident", type=Path)
    parser.add_argument("--checkpoint-recovery-diagnostic-audit", type=Path)
    parser.add_argument("--checkpoint-recovery-deployment-manifest", type=Path)
    parser.add_argument("--executor-workers", type=int, default=10)
    parser.add_argument(
        "--checkpoint-worker-cap",
        type=int,
        choices=CHECKPOINT_WORKER_CHOICES,
        default=8,
    )
    args = parser.parse_args(argv)
    if len(str(args.builder_commit_sha)) != 40:
        parser.error("builder-commit-sha must be a full Git SHA")
    closure = run(args)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
