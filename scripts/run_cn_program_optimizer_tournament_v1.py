"""Thin Program-optimizer tournament adapter over the accepted Phase C engine.

All economic evaluation stays in ``run_cn_joint_program_phase_c_v0``.  This
module replaces only proposal selection and whole-Program feedback through the
shared ``ProgramSearchOptimizerAdapter`` contract.
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_cn_joint_program_phase_c_v0 as engine
from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    BATCH_ID,
    CAMPAIGN_ID,
    CAMPAIGN_PROFILE,
    RECORDS_PER_CHECKPOINT,
    SEEDS,
    SURROGATE_CONFIG,
    TPE_CONFIG,
    freeze_racing_decision_v1,
    verify_racing_decision_v1,
)
from our_system_phase2.services.program_optimizer_tournament_v1 import (
    ProgramOptimizerTournamentV1,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    PROGRAM_OPTIMIZER_ARMS,
    ProgramOptimizerObservationV1,
    UNIFORM_CONTROL,
    normalized_program_gene_identity_v1,
    program_availability_entries_v1,
    verify_program_batch_group_selection_v1,
)
from our_system_phase2.services.search_v2_admission import (
    AbsoluteEconomicAdmission,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    conditional_uplift_credit,
)
from our_system_phase2.services.program_tournament_freeze_v1 import (
    FREEZE_CLOSURE_NAME,
    build_stage01_freeze_v1,
    build_stage2_freeze_v1,
    verify_phase_freeze_v1,
    verify_source_binding_v1,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


STATUS = "CN_PROGRAM_OPTIMIZER_TOURNAMENT_COMPLETE"
CLOSURE_NAME = "CN_PROGRAM_OPTIMIZER_TOURNAMENT_COMPLETE.json"
_PRIOR_PROGRAM_EXACT_IDENTITIES: frozenset[str] = frozenset()


def _program_entries(
    catalog: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[Any, ...]:
    by_identity = {
        stable_hash(dict(entry["program_genes"])): {
            "genes": dict(entry["program_genes"])
        }
        for template_id in sorted(catalog)
        for entry in catalog[template_id]
    }
    return program_availability_entries_v1(list(by_identity.values()))


def _tournament_from_catalog(
    catalog: Mapping[str, Sequence[Mapping[str, Any]]],
) -> ProgramOptimizerTournamentV1:
    entries = _program_entries(catalog)
    return ProgramOptimizerTournamentV1.fresh(
        campaign_id=CAMPAIGN_ID,
        entries_by_arm={arm: entries for arm in SEEDS},
        seeds=SEEDS,
        tpe_config={
            key: value
            for key, value in TPE_CONFIG.items()
            if key in {"n_startup_trials", "n_ei_candidates"}
        },
        surrogate_config={
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


class ProgramOptimizerTournamentState:
    """Engine restore hook bound to the currently verified Program catalog."""

    _entries_by_arm: Mapping[str, Sequence[Any]] | None = None
    _initial_snapshot: Mapping[str, Any] | None = None

    @classmethod
    def bind_entries(cls, entries: Sequence[Any]) -> None:
        cls._entries_by_arm = {arm: tuple(entries) for arm in SEEDS}
        cls._initial_snapshot = ProgramOptimizerTournamentV1.fresh(
            campaign_id=CAMPAIGN_ID,
            entries_by_arm=cls._entries_by_arm,
            seeds=SEEDS,
            tpe_config={
                key: value
                for key, value in TPE_CONFIG.items()
                if key in {"n_startup_trials", "n_ei_candidates"}
            },
            surrogate_config={
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
        ).snapshot()

    @classmethod
    def restore(
        cls, snapshot: Mapping[str, Any], **_: Any
    ) -> ProgramOptimizerTournamentV1:
        if cls._entries_by_arm is None:
            raise RuntimeError("PROGRAM_TOURNAMENT_SPACE_NOT_BOUND")
        source = (
            dict(cls._initial_snapshot)
            if str(snapshot.get("schema_version") or "")
            != "cn_program_optimizer_tournament_state_v1"
            else dict(snapshot)
        )
        return ProgramOptimizerTournamentV1.restore(
            source,
            entries_by_arm=cls._entries_by_arm,
            expected_campaign_id=CAMPAIGN_ID,
        )


def _build_catalog(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    catalog, report = engine._vnext_original_build_catalog(*args, **kwargs)
    entries = _program_entries(catalog)
    ProgramOptimizerTournamentState.bind_entries(entries)
    report = dict(report)
    report["program_space_hash"] = stable_hash(
        [entry.to_dict() for entry in entries]
    )
    report["program_space_record_count"] = len(entries)
    report["program_space_same_for_all_arms"] = True
    return catalog, report


def _eligible_entries(
    asks: Sequence[Mapping[str, Any]],
    *,
    catalog: Mapping[str, Sequence[Mapping[str, Any]]],
    state: MutableMapping[str, Any],
    program_gene_slots: Sequence[str],
    prior_exact_identities: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    template_id = str(asks[0]["template_id"])
    prior = (
        _PRIOR_PROGRAM_EXACT_IDENTITIES
        if prior_exact_identities is None
        else frozenset(map(str, prior_exact_identities))
    )
    candidates = [
        dict(entry)
        for entry in catalog[template_id]
        if normalized_program_gene_identity_v1(
            dict(entry["program_genes"]), ordered_slots=program_gene_slots
        )
        not in prior
        if str(entry["reservoir"]["reservoir_record_sha256"])
        not in state["reservoir_ids"]
        and (
            template_id == "BASE"
            or str(entry["program_id"]) not in state["program_ids"]
        )
    ]
    if not candidates:
        raise RuntimeError(
            f"Program tournament selection supply exhausted: {template_id}"
        )
    eligible = [
        normalized_program_gene_identity_v1(
            dict(entry["program_genes"]), ordered_slots=program_gene_slots
        )
        for entry in candidates
    ]
    return candidates, eligible


def _batch_group_constraint(
    *,
    template_id: str,
    candidates: Sequence[Mapping[str, Any]],
    state: Mapping[str, Any],
    program_gene_slots: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema_version": "cn_program_batch_group_constraint_v1",
        "group_by_exact_identity": {
            normalized_program_gene_identity_v1(
                dict(entry["program_genes"]), ordered_slots=program_gene_slots
            ): str(entry["base_component_id"])
            for entry in candidates
        },
        "current_group_counts": {
            str(base_id): int(count)
            for (candidate_template, base_id), count in state["base_counts"].items()
            if str(candidate_template) == template_id and int(count) > 0
        },
        "minimum_distinct_groups": (
            0 if template_id == "BASE" else engine.MIN_BASE_IDENTITIES_PER_TEMPLATE
        ),
        "maximum_per_group": engine.MAX_VARIANTS_PER_BASE_PER_TEMPLATE,
    }


def _select_checkpoint(
    asks: Sequence[Mapping[str, Any]],
    *,
    catalog: Mapping[str, Sequence[Mapping[str, Any]]],
    bandit: ProgramOptimizerTournamentV1,
    state: MutableMapping[str, Any],
    components_by_id: Mapping[str, Any],
    adapter: Any,
    compiler: Any,
    prior_exact_identities: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    arms = {str(row["generation_arm"]) for row in asks}
    templates = {str(row["template_id"]) for row in asks}
    if len(arms) != 1 or len(templates) != 1:
        raise RuntimeError("PROGRAM_TOURNAMENT_CHECKPOINT_STRATUM_DRIFT")
    arm = next(iter(arms))
    template_id = next(iter(templates))
    program_gene_slots = tuple(bandit.entries_by_arm[arm][0].genes)
    candidates, eligible = _eligible_entries(
        asks,
        catalog=catalog,
        state=state,
        program_gene_slots=program_gene_slots,
        prior_exact_identities=prior_exact_identities,
    )
    constraint = _batch_group_constraint(
        template_id=template_id,
        candidates=candidates,
        state=state,
        program_gene_slots=program_gene_slots,
    )
    checkpoint_id = f"checkpoint_{int(asks[0]['checkpoint_ordinal']) + 1:03d}"
    optimizer_asks = bandit.ask(
        arm=arm,
        checkpoint_id=checkpoint_id,
        count=len(asks),
        required_program_template_id=template_id,
        eligible_exact_identities=eligible,
        batch_group_constraint=constraint,
    )
    if len(optimizer_asks) != len(asks):
        raise RuntimeError("PROGRAM_TOURNAMENT_CHECKPOINT_SUPPLY_EXHAUSTED")
    by_exact = {
        normalized_program_gene_identity_v1(
            dict(entry["program_genes"]), ordered_slots=program_gene_slots
        ): entry
        for entry in candidates
    }
    selected_batch = [
        dict(by_exact[str(optimizer_ask["exact_identity"])])
        for optimizer_ask in optimizer_asks
    ]
    feasibility_receipt = verify_program_batch_group_selection_v1(
        [str(row["exact_identity"]) for row in optimizer_asks],
        constraint=constraint,
    )
    for selected in selected_batch:
        state["program_ids"].add(str(selected["program_id"]))
        state["reservoir_ids"].add(
            str(selected["reservoir"]["reservoir_record_sha256"])
        )
        state["base_counts"][(template_id, str(selected["base_component_id"]))] += 1
        state["component_ids"].update(selected["component_ids"])
        state["combination_ids"].add(str(selected["combination_id"]))
    actual_group_counts = {
        str(base_id): int(count)
        for (candidate_template, base_id), count in state["base_counts"].items()
        if str(candidate_template) == template_id and int(count) > 0
    }
    if actual_group_counts != feasibility_receipt["group_counts_after"]:
        raise RuntimeError("PROGRAM_TOURNAMENT_BATCH_GROUP_STATE_DRIFT")
    schedules: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    for index, (ask, optimizer_ask, selected) in enumerate(
        zip(asks, optimizer_asks, selected_batch, strict=True)
    ):
        decision = engine._self_hashed(
            {
                "schema_version": "cn_program_optimizer_tournament_selection_v1",
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
                "optimizer_ask": dict(optimizer_ask),
                "batch_group_feasibility_receipt": (
                    feasibility_receipt if index == 0 else {}
                ),
                "bandit_state_before_selection_sha256": str(
                    bandit.snapshot()["bandit_state_sha256"]
                ),
                "adaptive_template_credit_used": False,
                "program_level_credit_only": True,
                "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
            },
            "selection_decision_sha256",
        )
        schedule = engine._schedule_record(
            ask,
            selected,
            decision,
            components_by_id=components_by_id,
            adapter=adapter,
            compiler=compiler,
        )
        schedule["optimizer_ask"] = dict(optimizer_ask)
        schedule["optimizer_eligible_exact_identities"] = (
            list(eligible) if index == 0 else []
        )
        schedule["optimizer_batch_group_constraint"] = (
            constraint if index == 0 else {}
        )
        schedule["schedule_record_sha256"] = stable_hash(
            {key: value for key, value in schedule.items() if key != "schedule_record_sha256"}
        )
        schedules.append(schedule)
        decisions.append(decision)
    return schedules, decisions


def _feedback_update(
    records: Sequence[Mapping[str, Any]],
    schedules: Sequence[Mapping[str, Any]],
    *,
    bandit: ProgramOptimizerTournamentV1,
    behavior_counts: Counter[str],
) -> list[dict[str, Any]]:
    schedules_by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in schedules
    }
    arms = {str(row["generation_arm"]) for row in schedules}
    if len(arms) != 1:
        raise RuntimeError("PROGRAM_TOURNAMENT_FEEDBACK_ARM_DRIFT")
    arm = next(iter(arms))
    template_ids = {str(row["template_id"]) for row in schedules}
    is_base_parity = template_ids == {"BASE"}
    observations: list[ProgramOptimizerObservationV1] = []
    rows: list[dict[str, Any]] = []
    before = str(bandit.snapshot()["bandit_state_sha256"])
    for record in sorted(records, key=lambda row: int(row["main_record_ordinal"])):
        ordinal = int(record["main_record_ordinal"])
        schedule = schedules_by_ordinal[ordinal]
        if str(record["template_id"]) == "BASE":
            rows.append(
                {
                    "main_record_ordinal": ordinal,
                    "template_id": "BASE",
                    "generation_arm": arm,
                    "record_payload_sha256": str(record["record_payload_sha256"]),
                    "bandit_update_applied": False,
                    "reason": "BASE_PARITY_NOT_PROGRAM_OPTIMIZER_ELIGIBLE",
                }
            )
            continue
        admission = AbsoluteEconomicAdmission.evaluate(
            record,
            expected_pair_id=str(schedule["pair_id"]),
            expected_program_id=str(schedule["primary_program"]["program_id"]),
            expected_control_program_id=str(schedule["control_program"]["program_id"]),
        )
        credit = conditional_uplift_credit(record, admission)
        optimizer_ask = dict(schedule["optimizer_ask"])
        observations.append(
            ProgramOptimizerObservationV1(
                proposal_id=str(optimizer_ask["proposal_id"]),
                exact_identity=str(optimizer_ask["exact_identity"]),
                admission=admission,
                uplift=credit,
            )
        )
        rows.append(
            {
                "main_record_ordinal": ordinal,
                "template_id": str(record["template_id"]),
                "generation_arm": arm,
                "record_payload_sha256": str(record["record_payload_sha256"]),
                "bandit_update_applied": True,
                "reason": "WHOLE_PROGRAM_DUAL_HEAD_OBSERVED",
                "optimizer_proposal_id": str(optimizer_ask["proposal_id"]),
                "optimizer_trial_number": optimizer_ask["trial_number"],
                "absolute_admission": admission.to_record(),
                "enhancer_credit": credit.to_record() if credit else None,
            }
        )
    if observations:
        checkpoint_id = str(schedules[0]["optimizer_ask"]["checkpoint_id"])
        template_id = str(schedules[0]["template_id"])
        eligible = list(schedules[0]["optimizer_eligible_exact_identities"])
        batch_group_constraint = dict(
            schedules[0]["optimizer_batch_group_constraint"]
        )
        bandit.commit_ask(
            arm=arm,
            checkpoint_id=checkpoint_id,
            count=len(observations),
            required_program_template_id=template_id,
            eligible_exact_identities=eligible,
            batch_group_constraint=batch_group_constraint,
            expected_asks=[row["optimizer_ask"] for row in schedules],
        )
        tell_receipt = bandit.tell(arm=arm, observations=observations)
    elif is_base_parity:
        checkpoint_id = str(schedules[0]["optimizer_ask"]["checkpoint_id"])
        bandit.commit_ask(
            arm=arm,
            checkpoint_id=checkpoint_id,
            count=len(schedules),
            required_program_template_id="BASE",
            eligible_exact_identities=[
                *schedules[0]["optimizer_eligible_exact_identities"]
            ],
            batch_group_constraint=dict(
                schedules[0]["optimizer_batch_group_constraint"]
            ),
            expected_asks=[row["optimizer_ask"] for row in schedules],
        )
        tell_receipt = bandit.discard_nonlearning_pending(
            arm=arm,
            expected_asks=[row["optimizer_ask"] for row in schedules],
        )
    else:
        tell_receipt = None
    after = str(bandit.snapshot()["bandit_state_sha256"])
    return [
        engine._self_hashed(
            {
                "schema_version": "cn_program_optimizer_tournament_feedback_v1",
                **row,
                "tell_receipt": tell_receipt,
                "program_level_credit_only": True,
                "component_attribution": "COMPONENT_ATTRIBUTION_UNIDENTIFIED",
                "failed_candidate_negative_enhancer_reward": False,
                "bandit_state_before_sha256": (
                    before if index == 0 else after
                ),
                "bandit_state_after_sha256": after,
                "validation_feedback_used": False,
                "cross_campaign_state_imported": False,
            },
            "feedback_record_sha256",
        )
        for index, row in enumerate(rows)
    ]


def _configure_engine() -> None:
    if not hasattr(engine, "_vnext_original_build_catalog"):
        engine._vnext_original_build_catalog = engine._build_catalog
    engine.PHASE_C_BATCH_ID = BATCH_ID
    engine.RECORDS_PER_CHECKPOINT = RECORDS_PER_CHECKPOINT
    engine.FREEZE_CLOSURE_NAME = FREEZE_CLOSURE_NAME
    engine.ProgramFactorizedBanditV0 = ProgramOptimizerTournamentState
    engine._build_catalog = _build_catalog
    engine._select_checkpoint = _select_checkpoint
    engine._feedback_update = _feedback_update
    engine._verify_checkpoint = _verify_checkpoint
    engine.STATUS = STATUS
    engine.CLOSURE_NAME = CLOSURE_NAME


def _verify_checkpoint(
    checkpoint_root: Path,
    *,
    checkpoint_id: str,
    previous_manifest: Path | None,
    input_hash: str,
    expected_schedules: Sequence[Mapping[str, Any]],
    expected_decisions: Sequence[Mapping[str, Any]],
    bandit: ProgramOptimizerTournamentV1,
    behavior_counts: Counter[str],
) -> tuple[Path, list[dict[str, Any]], ProgramOptimizerTournamentV1]:
    engine._checkpoint_artifacts(checkpoint_root)
    manifest_path = checkpoint_root / "batch_manifest.json"
    manifest = engine._read_json(manifest_path)
    expected_prior = engine._sha256(previous_manifest) if previous_manifest else "GENESIS"
    if (
        str(manifest.get("status") or "") != "BATCH_CLOSED_IMMUTABLE"
        or str(manifest.get("batch_id") or "") != checkpoint_id
        or str((manifest.get("input_hashes") or {}).get("prior_checkpoint_manifest"))
        != expected_prior
        or str((manifest.get("input_hashes") or {}).get("phase_c_input_binding"))
        != input_hash
    ):
        raise RuntimeError(f"Program tournament checkpoint chain drift: {checkpoint_id}")
    schedules = engine._read_jsonl(checkpoint_root / "selected_schedule.jsonl")
    decisions = engine._read_jsonl(checkpoint_root / "selection_ledger.jsonl")
    if schedules != [dict(row) for row in expected_schedules] or decisions != [
        dict(row) for row in expected_decisions
    ]:
        raise RuntimeError(f"Program tournament checkpoint replay drift: {checkpoint_id}")
    before = engine._read_json(checkpoint_root / "bandit_state_before.json")
    if before != bandit.snapshot():
        raise RuntimeError(f"Program tournament checkpoint before drift: {checkpoint_id}")
    by_ordinal = {
        int(row["main_record_ordinal"]): dict(row) for row in schedules
    }
    rows = [
        engine._verify_record(
            path,
            expected_input_hash=input_hash,
            schedule=by_ordinal[int(path.stem.split("_")[-1])],
        )
        for path in sorted((checkpoint_root / "records").glob("record_*.json"))
    ]
    if len(rows) != RECORDS_PER_CHECKPOINT:
        raise RuntimeError(f"Program tournament checkpoint count drift: {checkpoint_id}")
    replay_bandit = ProgramOptimizerTournamentState.restore(before)
    expected_feedback = _feedback_update(
        rows,
        schedules,
        bandit=replay_bandit,
        behavior_counts=behavior_counts,
    )
    feedback = engine._read_jsonl(checkpoint_root / "bandit_feedback_ledger.jsonl")
    if feedback != expected_feedback:
        raise RuntimeError(f"Program tournament checkpoint feedback drift: {checkpoint_id}")
    after = engine._read_json(checkpoint_root / "bandit_state_after.json")
    if after != replay_bandit.snapshot():
        raise RuntimeError(f"Program tournament checkpoint after drift: {checkpoint_id}")
    return manifest_path, rows, replay_bandit


def _verify_run_contract(
    contract: Mapping[str, Any], *, executor_workers: int
) -> None:
    expected = {
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "records_per_checkpoint": RECORDS_PER_CHECKPOINT,
        "executor_backend": "PROCESS_POOL",
        "executor_workers": int(executor_workers),
        "scalar_absolute_plus_uplift_reward": None,
        "component_credit_authoritative": False,
        "serialized_optimizer_state_imported": False,
        "development_financial_observations_imported": False,
        "candidate_results_imported": False,
        "financial_evaluation_executed": False,
    }
    drift = [key for key, value in expected.items() if contract.get(key) != value]
    if drift:
        raise RuntimeError(
            "Program tournament frozen run contract drift: " + ",".join(drift)
        )
    if int(contract.get("development_observation_count", -1)) < 0:
        raise RuntimeError("Program tournament observation provenance drift")


def _verify_freeze(root: Path) -> dict[str, Any]:
    global _PRIOR_PROGRAM_EXACT_IDENTITIES
    closure = verify_phase_freeze_v1(root)
    contract = engine._read_json(root / "phase_c_run_contract.json")
    prior = engine._read_json(root / "prior_program_exact_identities.json")
    _PRIOR_PROGRAM_EXACT_IDENTITIES = frozenset(
        map(str, prior["exact_identities"])
    )
    engine.EXPECTED_RECORDS = int(contract["main_record_count"])
    engine.CHECKPOINT_COUNT = int(contract["checkpoint_count"])
    engine.MIN_BASE_IDENTITIES_PER_TEMPLATE = int(
        contract["minimum_base_identities_per_template"]
    )
    engine.MAX_VARIANTS_PER_BASE_PER_TEMPLATE = int(
        contract["maximum_variants_per_base_per_template"]
    )
    return closure


def _run_phase(
    args: argparse.Namespace,
    *,
    freeze_root: Path,
    run_root: Path,
    repo_sha: str,
    checkpoint_recovery: bool,
) -> dict[str, Any]:
    _verify_freeze(freeze_root)
    run_args = argparse.Namespace(**vars(args))
    run_args.phase_c_freeze_root = freeze_root
    run_args.output_root = run_root
    run_args.builder_commit_sha = repo_sha
    run_args.train_field_root = Path(
        str(engine._read_json(freeze_root / "phase_c_run_contract.json")[
            "accepted_field_manifest_path"
        ])
    ).resolve().parent
    for name in (
        "root_finalization_recovery_from_repo_sha",
        "root_finalization_incident",
        "root_finalization_deployment_manifest",
    ):
        setattr(run_args, name, None)
    if not checkpoint_recovery:
        for name in (
            "checkpoint_recovery_from_repo_sha",
            "checkpoint_recovery_incident",
            "checkpoint_recovery_diagnostic_audit",
            "checkpoint_recovery_deployment_manifest",
        ):
            setattr(run_args, name, None)
    engine.verify_phase_c_prefinancial_freeze_v0 = _verify_freeze
    engine._verify_run_contract = _verify_run_contract
    return engine.run(run_args)


def run_authorized_tournament(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    _configure_engine()
    admitted_root = args.output_root.resolve()
    freeze_root = admitted_root / "prefinancial_freeze_stage01"
    run_root = admitted_root / "stage01_run"
    source_binding = verify_source_binding_v1(args.source_binding)
    args.registry = Path(source_binding["registry_path"])
    args.node_resource_capacity = Path(
        source_binding["node_resource_capacity_path"]
    )
    if freeze_root.exists():
        _verify_freeze(freeze_root)
    else:
        build_stage01_freeze_v1(
            output_root=freeze_root,
            source_binding_path=args.source_binding,
            repo_sha=str(admission["repo_sha"]),
        )
        _verify_freeze(freeze_root)
    repo_sha = str(admission["repo_sha"])
    stage01_closure = _run_phase(
        args,
        freeze_root=freeze_root,
        run_root=run_root,
        repo_sha=repo_sha,
        checkpoint_recovery=True,
    )
    feedback_rows = engine._read_jsonl(
        run_root / "phase_c_bandit_feedback_ledger.jsonl"
    )
    racing_path = admitted_root / "stage01_racing_decision.json"
    computed_racing = freeze_racing_decision_v1(feedback_rows)
    if racing_path.exists():
        racing = verify_racing_decision_v1(engine._read_json(racing_path))
        if racing != computed_racing:
            raise RuntimeError("PROGRAM_TOURNAMENT_RACING_DECISION_DRIFT")
    else:
        racing = engine._read_json(
            engine._write_json(racing_path, computed_racing)
        )
    active_arms = tuple(map(str, racing["stage2_active_arms"]))
    if UNIFORM_CONTROL not in active_arms or any(
        arm not in PROGRAM_OPTIMIZER_ARMS for arm in active_arms
    ):
        raise RuntimeError("PROGRAM_TOURNAMENT_RACING_ACTIVE_ARM_DRIFT")

    stage2_freeze = admitted_root / "prefinancial_freeze_stage02"
    stage2_run = admitted_root / "stage02_run"
    if stage2_freeze.exists():
        _verify_freeze(stage2_freeze)
    else:
        build_stage2_freeze_v1(
            output_root=stage2_freeze,
            stage01_freeze_root=freeze_root,
            final_stage01_state_path=run_root / "final_bandit_state.json",
            stage01_schedule_path=run_root / "phase_c_selected_schedule.jsonl",
            active_arms=active_arms,
            repo_sha=repo_sha,
        )
    stage2_closure = _run_phase(
        args,
        freeze_root=stage2_freeze,
        run_root=stage2_run,
        repo_sha=repo_sha,
        checkpoint_recovery=False,
    )
    body = {
        "schema_version": "cn_program_optimizer_tournament_closure_v1",
        "status": STATUS,
        "campaign_id": CAMPAIGN_ID,
        "campaign_profile": CAMPAIGN_PROFILE,
        "repo_sha": repo_sha,
        "stage01_closure_sha256": str(stage01_closure["closure_payload_sha256"]),
        "racing_decision_sha256": str(racing["racing_decision_sha256"]),
        "stage2_active_arms": list(active_arms),
        "stage2_closure_sha256": str(stage2_closure["closure_payload_sha256"]),
        "automatic_stage2_launch": True,
        "same_project_control_admission": True,
        "manual_post_result_reallocation_allowed": False,
        "automatic_successor_authorized": False,
        "promotion_authorized": False,
    }
    return engine._read_json(engine._write_json(
        admitted_root / CLOSURE_NAME,
        engine._self_hashed(body, "closure_payload_sha256"),
    ))


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    raise PermissionError(
        "Direct Program tournament invocation is forbidden; use app.py and Project Control"
    )


if __name__ == "__main__":
    raise SystemExit(main())
