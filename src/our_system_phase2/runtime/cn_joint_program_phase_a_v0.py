"""Zero-financial Phase A for CN joint-program rolling search V0."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    PROGRAM_TEMPLATE_COMPONENTS,
    CandidateProgramProposalAdapterV0,
    ProgramProposalReceiptV0,
    ProgramSourceComponentV0,
    source_sampling_phase_v0,
)
from our_system_phase2.services.candidate_program_v1 import (
    CandidateProgramSpecV1,
    ProgramCompilerV1,
    legacy_candidate_program_v1,
)
from our_system_phase2.services.compositional_grammar import (
    CompositionalGrammarV2,
)
from our_system_phase2.services.program_factorized_bandit_v0 import (
    ProgramFactorizedBanditV0,
    generation_arm_v0,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


PHASE_A_STATUS = "CN_JOINT_PROGRAM_ROLLING_SEARCH_V0_PHASE_A_COMPLETE"
PHASE_A_AUDIT_STATUS = "CN_JOINT_PROGRAM_PHASE_A_INDEPENDENT_AUDIT_PASS"
PHASE_A_SEED = 1729
COMPONENT_ROUTES = {
    "base": "SLOW_CROSS_SECTIONAL_LEVEL",
    "temporal": "SLOW_TEMPORAL_CHANGE",
    "market": "MARKET_REGIME_CONDITION",
    "event": "DISCLOSURE_EVENT",
}
SEALED_READ_KEYS = (
    "financial_reads",
    "market_reads",
    "label_reads",
    "validation_reads",
    "holdout_reads",
    "historical_2023_reads",
    "forward_b_reads",
    "forward_2026_reads",
)


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _self_hashed(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    body = dict(payload)
    body.pop(key, None)
    body[key] = stable_hash(body)
    return body


def _component_record(component: ProgramSourceComponentV0) -> dict[str, Any]:
    return {
        "role": component.role,
        "route_id": component.route_id,
        "component_id": component.component_id,
        "primary": dict(component.primary),
        "control": dict(component.control),
        "proposal_id": component.proposal_id,
        "trial_number": component.trial_number,
        "sampling_phase": component.sampling_phase,
        "generation_receipt_hash": component.generation_receipt_hash,
        "credit_eligible": component.credit_eligible,
    }


def _component_from_record(record: Mapping[str, Any]) -> ProgramSourceComponentV0:
    component = ProgramSourceComponentV0(
        role=str(record.get("role") or ""),
        primary=dict(record.get("primary") or {}),
        control=dict(record.get("control") or {}),
        proposal_id=str(record.get("proposal_id") or ""),
        trial_number=(
            int(record["trial_number"])
            if record.get("trial_number") is not None
            else None
        ),
        sampling_phase=str(record.get("sampling_phase") or ""),
    )
    if component.component_id != str(record.get("component_id") or ""):
        raise ValueError("phase A component identity replay drift")
    if component.generation_receipt_hash != str(
        record.get("generation_receipt_hash") or ""
    ):
        raise ValueError("phase A component receipt replay drift")
    if component.credit_eligible != bool(record.get("credit_eligible")):
        raise ValueError("phase A component credit boundary drift")
    return component


def _generate_components(
    registry: UnifiedCapabilityRegistry, root_contract_path: Path
) -> dict[str, ProgramSourceComponentV0]:
    allowlists = json.loads(root_contract_path.read_text(encoding="utf-8"))[
        "route_root_allowlists"
    ]
    grammar = CompositionalGrammarV2(
        registry, route_root_allowlist=allowlists
    )
    components = {}
    for ordinal, (role, route_id) in enumerate(COMPONENT_ROUTES.items()):
        pair = grammar.propose(
            route_id, attempt_index=ordinal, seed=PHASE_A_SEED
        )
        components[role] = ProgramSourceComponentV0(
            role=role,
            primary=dict(pair.primary),
            control=dict(pair.control),
            proposal_id=f"phase-a-fixed-{role}-{ordinal:03d}",
            trial_number=None,
            sampling_phase="AVAILABILITY_FIXED",
        )
    return components


def _template_components(
    components: Mapping[str, ProgramSourceComponentV0], template_id: str
) -> list[ProgramSourceComponentV0]:
    return [
        components[role] for role in PROGRAM_TEMPLATE_COMPONENTS[template_id]
    ]


def _compose(
    adapter: CandidateProgramProposalAdapterV0,
    components: Mapping[str, ProgramSourceComponentV0],
    template_id: str,
) -> CandidateProgramSpecV1:
    roles = PROGRAM_TEMPLATE_COMPONENTS[template_id]
    return adapter.compose(
        template_id,
        components["base"],
        **{
            f"{role}_component": components[role]
            for role in roles
            if role != "base"
        },
    )


def build_phase_a_v0(
    *,
    output_root: Path,
    registry_path: Path,
    root_contract_path: Path,
    repo_sha: str,
) -> dict[str, Any]:
    output_root = Path(output_root).resolve()
    if output_root.exists() and any(output_root.iterdir()):
        raise FileExistsError(f"Phase A output root is not empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    registry = UnifiedCapabilityRegistry.read(Path(registry_path).resolve())
    components = _generate_components(
        registry, Path(root_contract_path).resolve()
    )
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    artifact_paths: list[Path] = []

    component_manifest = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_a_component_pool_v0",
            "seed": PHASE_A_SEED,
            "component_count": len(components),
            "components": [
                _component_record(components[role])
                for role in COMPONENT_ROUTES
            ],
            "performance_data_accessed": False,
            "pool_feedback_allowed": False,
        },
        "component_pool_sha256",
    )
    component_path = _write_json(
        output_root / "component_pool.json", component_manifest
    )
    artifact_paths.append(component_path)

    transcript = []
    program_rows = []
    control_rows = []
    base_parity_pass = False
    for ordinal, template_id in enumerate(PROGRAM_TEMPLATE_COMPONENTS):
        program = _compose(adapter, components, template_id)
        compiled = compiler.compile(program)
        receipt = adapter.build_receipt(
            program_template_id=template_id,
            program=program,
            components=_template_components(components, template_id),
            combination_policy=None,
            batch_id="phase_a_batch_001",
            ask_ordinal=ordinal,
            generation_arm=generation_arm_v0(ordinal),
        )
        program_record = _self_hashed(
            {
                "template_id": template_id,
                "template_order": ordinal,
                "program": program.to_record(),
                "compiled": compiled.to_record(),
                "proposal_receipt": receipt.to_record(),
            },
            "program_record_sha256",
        )
        program_path = _write_json(
            output_root / "programs" / f"{ordinal:02d}_{template_id}.json",
            program_record,
        )
        artifact_paths.append(program_path)
        program_rows.append(
            {
                "template_id": template_id,
                "program_id": program.program_id,
                "semantic_program_hash": program.semantic_program_hash,
                "proposal_receipt_sha256": receipt.to_record()[
                    "proposal_receipt_sha256"
                ],
                "compiled_program_hash": compiled.to_record()[
                    "compiled_program_hash"
                ],
            }
        )
        transcript.append(
            {
                "ask_ordinal": ordinal,
                "template_id": template_id,
                "program_id": program.program_id,
                "proposal_receipt_sha256": receipt.to_record()[
                    "proposal_receipt_sha256"
                ],
            }
        )
        if template_id == "BASE":
            legacy = legacy_candidate_program_v1(
                components["base"].primary,
                portfolio_contract=adapter.portfolio_contract,
            )
            legacy_compiled = compiler.compile(legacy).to_record()
            base_parity_pass = (
                program.to_record() == legacy.to_record()
                and compiled.to_record() == legacy_compiled
            )
            continue
        pair = construct_matched_control_program_v1(program)
        compiled_control = compiler.compile(pair.control)
        if pair.primary.semantic_program_hash == pair.control.semantic_program_hash:
            raise ValueError("enhanced Phase A control did not change semantics")
        if set(compiled.component_clock_requirements) != set(
            compiled_control.component_clock_requirements
        ):
            raise ValueError("enhanced Phase A control changed support clocks")
        control_record = _self_hashed(
            {
                "template_id": template_id,
                "pair_id": pair.pair_id,
                "primary_program_id": pair.primary_program_id,
                "control_program_id": pair.control_program_id,
                "primary_semantic_program_hash": pair.primary.semantic_program_hash,
                "control_semantic_program_hash": pair.control.semantic_program_hash,
                "primary_component_clocks": compiled.component_clock_requirements,
                "control_component_clocks": (
                    compiled_control.component_clock_requirements
                ),
                "control": pair.control.to_record(),
                "compiled_control": compiled_control.to_record(),
            },
            "control_record_sha256",
        )
        control_path = _write_json(
            output_root / "controls" / f"{ordinal:02d}_{template_id}.json",
            control_record,
        )
        artifact_paths.append(control_path)
        control_rows.append(
            {
                "template_id": template_id,
                "pair_id": pair.pair_id,
                "control_record_sha256": control_record[
                    "control_record_sha256"
                ],
            }
        )

    second_transcript = []
    for ordinal, template_id in enumerate(PROGRAM_TEMPLATE_COMPONENTS):
        program = _compose(adapter, components, template_id)
        receipt = adapter.build_receipt(
            program_template_id=template_id,
            program=program,
            components=_template_components(components, template_id),
            combination_policy=None,
            batch_id="phase_a_batch_001",
            ask_ordinal=ordinal,
            generation_arm=generation_arm_v0(ordinal),
        )
        second_transcript.append(
            {
                "ask_ordinal": ordinal,
                "template_id": template_id,
                "program_id": program.program_id,
                "proposal_receipt_sha256": receipt.to_record()[
                    "proposal_receipt_sha256"
                ],
            }
        )
    transcript_replay_pass = transcript == second_transcript

    forgery_checks = []
    base = components["base"]
    for check_id, kwargs in (
        (
            "FORGED_CONTROL_ID",
            {"control": {**base.control, "candidate_id": "forged-control"}},
        ),
        (
            "FORGED_PAIR_ID",
            {"control": {**base.control, "pair_id": "forged-pair"}},
        ),
        (
            "FORGED_PRIMARY_EXACT_IDENTITY",
            {"primary": {**base.primary, "exact_identity": "forged-exact"}},
        ),
        ):
        try:
            forged_component = ProgramSourceComponentV0(
                role=base.role,
                primary=kwargs.get("primary", base.primary),
                control=kwargs.get("control", base.control),
                proposal_id="forgery-check",
                trial_number=0,
                sampling_phase="STARTUP_RANDOM",
            )
            forged_program = adapter.compose(
                "BASE", forged_component
            )
            compiler.compile(forged_program)
        except Exception as exc:
            forgery_checks.append(
                {"check_id": check_id, "rejected": True, "error": type(exc).__name__}
            )
        else:
            forgery_checks.append({"check_id": check_id, "rejected": False})
    forgery_path = _write_json(
        output_root / "forgery_checks.json",
        _self_hashed(
            {
                "schema_version": "cn_joint_program_forgery_checks_v0",
                "checks": forgery_checks,
                "all_rejected": all(row["rejected"] for row in forgery_checks),
            },
            "forgery_checks_sha256",
        ),
    )
    artifact_paths.append(forgery_path)

    sampling_phase_rows = [
        {
            "optimizer_ask_kind": "AVAILABILITY_FIXED_ENQUEUED",
            "trial_number": 999,
            "n_startup_trials": 512,
            "source_sampling_phase": source_sampling_phase_v0(
                optimizer_ask_kind="AVAILABILITY_FIXED_ENQUEUED",
                trial_number=999,
                n_startup_trials=512,
            ),
        },
        {
            "optimizer_ask_kind": "TPE_NATIVE_DRAW",
            "trial_number": 511,
            "n_startup_trials": 512,
            "source_sampling_phase": source_sampling_phase_v0(
                optimizer_ask_kind="TPE_NATIVE_DRAW",
                trial_number=511,
                n_startup_trials=512,
            ),
        },
        {
            "optimizer_ask_kind": "TPE_NATIVE_DRAW",
            "trial_number": 512,
            "n_startup_trials": 512,
            "source_sampling_phase": source_sampling_phase_v0(
                optimizer_ask_kind="TPE_NATIVE_DRAW",
                trial_number=512,
                n_startup_trials=512,
            ),
        },
    ]
    sampling_path = _write_json(
        output_root / "sampling_phase_checks.json",
        _self_hashed(
            {
                "schema_version": "cn_source_sampling_phase_checks_v0",
                "rows": sampling_phase_rows,
                "startup_threshold_changed": False,
            },
            "sampling_phase_checks_sha256",
        ),
    )
    artifact_paths.append(sampling_path)

    bandit_template = "BASE_TEMPORAL_MARKET_EVENT"
    bandit_program = _compose(adapter, components, bandit_template)
    bandit_receipt = adapter.build_receipt(
        program_template_id=bandit_template,
        program=bandit_program,
        components=_template_components(components, bandit_template),
        combination_policy=None,
        batch_id="phase_a_bandit_replay",
        ask_ordinal=0,
        generation_arm="UNIFORM_FRESH",
    )
    bandit = ProgramFactorizedBanditV0("phase-a-campaign")
    for index in range(8):
        bandit.observe(
            bandit_receipt,
            matched_increment=(0.01 * (index + 1) if index != 6 else -0.08),
            behavior_cluster_repeat_count=index % 2,
            turnover_excess_ratio=0.1 * (index % 3),
            single_window_concentration=0.25 if index == 7 else 0.0,
        )
    bandit_snapshot = bandit.snapshot()
    restored_snapshot = ProgramFactorizedBanditV0.restore(
        bandit_snapshot
    ).snapshot()
    bandit_path = _write_json(
        output_root / "bandit_replay.json",
        _self_hashed(
            {
                "schema_version": "cn_joint_program_bandit_replay_v0",
                "snapshot": bandit_snapshot,
                "exact_restore": restored_snapshot == bandit_snapshot,
                "arm_counts_first_10": {
                    arm: sum(generation_arm_v0(index) == arm for index in range(10))
                    for arm in (
                        "UNIFORM_FRESH",
                        "FACTORIZED_EXPLOIT",
                        "NOVELTY_RESERVE",
                    )
                },
            },
            "bandit_replay_sha256",
        ),
    )
    artifact_paths.append(bandit_path)

    access_ledger = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_a_access_ledger_v0",
            "authorized_metadata_reads": [
                str(Path(registry_path).resolve()),
                str(Path(root_contract_path).resolve()),
            ],
            **{key: 0 for key in SEALED_READ_KEYS},
            "optimizer_writes": 0,
            "formal_scheduler_writes": 0,
            "archive_writes": 0,
            "promotion_writes": 0,
            "financial_evaluation_executed": False,
        },
        "access_ledger_sha256",
    )
    access_path = _write_json(output_root / "access_ledger.json", access_ledger)
    artifact_paths.append(access_path)

    gates = {
        "ZERO_FINANCIAL_READS": all(access_ledger[key] == 0 for key in SEALED_READ_KEYS),
        "ALL_EIGHT_TEMPLATES_REACHABLE": len(program_rows) == 8,
        "BASE_PARITY_FIXTURE_PASS": base_parity_pass,
        "CONTROL_FIXTURE_PASS": len(control_rows) == 7,
        "FORGERY_REJECTION_PASS": all(row["rejected"] for row in forgery_checks),
        "SAMPLING_PHASE_PASS": [
            row["source_sampling_phase"] for row in sampling_phase_rows
        ]
        == ["AVAILABILITY_FIXED", "STARTUP_RANDOM", "TPE_GUIDED"],
        "BANDIT_REPLAY_PASS": restored_snapshot == bandit_snapshot,
        "DETERMINISTIC_TRANSCRIPT_PASS": transcript_replay_pass,
    }
    summary = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_a_summary_v0",
            "status": PHASE_A_STATUS if all(gates.values()) else "PHASE_A_FAILED",
            "repo_sha": str(repo_sha),
            "registry_hash": registry.registry_hash,
            "component_pool_sha256": component_manifest["component_pool_sha256"],
            "template_count": len(program_rows),
            "enhanced_control_count": len(control_rows),
            "program_rows": program_rows,
            "control_rows": control_rows,
            "proposal_transcript": transcript,
            "gates": gates,
            "performance_claim_allowed": False,
            "financial_phase_authorized": False,
        },
        "summary_sha256",
    )
    if not all(gates.values()):
        raise RuntimeError(f"joint program Phase A gates failed: {gates}")
    summary_path = _write_json(output_root / "summary.json", summary)
    artifact_paths.append(summary_path)

    artifacts = [
        {
            "relative_path": str(path.relative_to(output_root)).replace("\\", "/"),
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }
        for path in sorted(artifact_paths)
    ]
    manifest = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_a_artifact_manifest_v0",
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
        },
        "artifact_manifest_sha256",
    )
    manifest_path = _write_json(output_root / "ARTIFACT_MANIFEST.json", manifest)
    closure = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_a_closure_v0",
            "status": PHASE_A_STATUS,
            "repo_sha": str(repo_sha),
            "summary_file_sha256": _sha256(summary_path),
            "summary_sha256": summary["summary_sha256"],
            "artifact_manifest_file_sha256": _sha256(manifest_path),
            "artifact_manifest_sha256": manifest["artifact_manifest_sha256"],
            "artifact_count": len(artifacts),
            "financial_evaluation_executed": False,
        },
        "closure_sha256",
    )
    _write_json(output_root / "CN_JOINT_PROGRAM_PHASE_A_COMPLETE.json", closure)
    return closure


def verify_phase_a_v0(
    *,
    phase_a_root: Path,
    registry_path: Path,
    root_contract_path: Path,
    audit_output: Path | None = None,
) -> dict[str, Any]:
    root = Path(phase_a_root).resolve()
    closure_path = root / "CN_JOINT_PROGRAM_PHASE_A_COMPLETE.json"
    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    if closure.get("closure_sha256") != stable_hash(
        {key: value for key, value in closure.items() if key != "closure_sha256"}
    ):
        raise ValueError("Phase A closure self-hash mismatch")
    manifest_path = root / "ARTIFACT_MANIFEST.json"
    if _sha256(manifest_path) != str(
        closure.get("artifact_manifest_file_sha256") or ""
    ):
        raise ValueError("Phase A manifest file hash mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("artifact_manifest_sha256") != stable_hash(
        {
            key: value
            for key, value in manifest.items()
            if key != "artifact_manifest_sha256"
        }
    ):
        raise ValueError("Phase A manifest self-hash mismatch")
    for artifact in manifest["artifacts"]:
        path = (root / str(artifact["relative_path"])).resolve()
        if not path.is_relative_to(root) or _sha256(path) != artifact["sha256"]:
            raise ValueError("Phase A declared artifact mismatch")

    registry = UnifiedCapabilityRegistry.read(Path(registry_path).resolve())
    adapter = CandidateProgramProposalAdapterV0(registry)
    compiler = ProgramCompilerV1(registry)
    pool = json.loads((root / "component_pool.json").read_text(encoding="utf-8"))
    if pool.get("component_pool_sha256") != stable_hash(
        {key: value for key, value in pool.items() if key != "component_pool_sha256"}
    ):
        raise ValueError("Phase A component pool self-hash mismatch")
    components = {
        str(row["role"]): _component_from_record(row)
        for row in pool["components"]
    }
    regenerated = _generate_components(registry, Path(root_contract_path).resolve())
    if [_component_record(components[role]) for role in COMPONENT_ROUTES] != [
        _component_record(regenerated[role]) for role in COMPONENT_ROUTES
    ]:
        raise ValueError("Phase A component pool deterministic replay drift")

    for ordinal, template_id in enumerate(PROGRAM_TEMPLATE_COMPONENTS):
        record = json.loads(
            (
                root / "programs" / f"{ordinal:02d}_{template_id}.json"
            ).read_text(encoding="utf-8")
        )
        if record.get("program_record_sha256") != stable_hash(
            {
                key: value
                for key, value in record.items()
                if key != "program_record_sha256"
            }
        ):
            raise ValueError("Phase A program record self-hash mismatch")
        program = CandidateProgramSpecV1.from_record(record["program"])
        receipt = ProgramProposalReceiptV0.from_record(
            record["proposal_receipt"]
        )
        expected = _compose(adapter, components, template_id)
        if program.to_record() != expected.to_record():
            raise ValueError("Phase A program deterministic replay drift")
        if compiler.compile(program).to_record() != record["compiled"]:
            raise ValueError("Phase A compiled program replay drift")
        if receipt.semantic_program_hash != program.semantic_program_hash:
            raise ValueError("Phase A proposal/semantic identity drift")
        if template_id != "BASE":
            pair = construct_matched_control_program_v1(program)
            compiler.compile(pair.control)

    summary_path = root / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if _sha256(summary_path) != str(closure.get("summary_file_sha256") or ""):
        raise ValueError("Phase A summary file hash mismatch")
    if summary.get("summary_sha256") != stable_hash(
        {key: value for key, value in summary.items() if key != "summary_sha256"}
    ):
        raise ValueError("Phase A summary self-hash mismatch")
    if not all(dict(summary.get("gates") or {}).values()):
        raise ValueError("Phase A summary contains a failed gate")
    access = json.loads((root / "access_ledger.json").read_text(encoding="utf-8"))
    if any(int(access.get(key) or 0) != 0 for key in SEALED_READ_KEYS):
        raise ValueError("Phase A accessed financial or sealed data")
    bandit_record = json.loads(
        (root / "bandit_replay.json").read_text(encoding="utf-8")
    )
    if ProgramFactorizedBanditV0.restore(
        bandit_record["snapshot"]
    ).snapshot() != bandit_record["snapshot"]:
        raise ValueError("Phase A bandit independent replay drift")

    audit = _self_hashed(
        {
            "schema_version": "cn_joint_program_phase_a_independent_audit_v0",
            "status": PHASE_A_AUDIT_STATUS,
            "phase_a_root": str(root),
            "closure_file_sha256": _sha256(closure_path),
            "closure_sha256": closure["closure_sha256"],
            "artifact_count_verified": len(manifest["artifacts"]),
            "template_count_verified": len(PROGRAM_TEMPLATE_COMPONENTS),
            "enhanced_control_count_verified": 7,
            "component_count_verified": len(components),
            "zero_financial_and_sealed_reads": True,
            "all_eight_templates_replayed": True,
            "bandit_exact_restore": True,
            "independent_review_clean": True,
        },
        "audit_sha256",
    )
    if audit_output is not None:
        _write_json(Path(audit_output).resolve(), audit)
    return audit


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--output-root", type=Path, required=True)
    build.add_argument("--registry", type=Path, required=True)
    build.add_argument("--root-contract", type=Path, required=True)
    build.add_argument("--repo-sha", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--phase-a-root", type=Path, required=True)
    verify.add_argument("--registry", type=Path, required=True)
    verify.add_argument("--root-contract", type=Path, required=True)
    verify.add_argument("--audit-output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "build":
        result = build_phase_a_v0(
            output_root=args.output_root,
            registry_path=args.registry,
            root_contract_path=args.root_contract,
            repo_sha=args.repo_sha,
        )
    else:
        result = verify_phase_a_v0(
            phase_a_root=args.phase_a_root,
            registry_path=args.registry,
            root_contract_path=args.root_contract,
            audit_output=args.audit_output,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
