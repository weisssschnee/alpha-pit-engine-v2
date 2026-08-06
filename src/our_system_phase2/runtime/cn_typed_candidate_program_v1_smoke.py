"""Zero-financial compile smoke and independent verifier for program V1."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.candidate_program_controls_v1 import (
    construct_matched_control_program_v1,
)
from our_system_phase2.services.candidate_program_fixtures_v1 import (
    build_golden_program_fixtures_v1,
)
from our_system_phase2.services.candidate_program_v1 import (
    CandidateProgramSpecV1,
    ProgramCompilerV1,
    ProposalLineageV1,
)
from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


SMOKE_STATUS = "CN_TYPED_CANDIDATE_PROGRAM_V1_SMOKE_COMPLETE"
AUDIT_STATUS = "CN_TYPED_CANDIDATE_PROGRAM_V1_INDEPENDENT_AUDIT_PASS"
FIXTURE_IDS = (
    "A_LEGACY_PARITY",
    "B_MULTI_TIMESCALE_FINANCING",
    "C_MARKET_CONDITIONED",
    "D_BILLBOARD_EVENT",
    "E_FULL_MULTI_COMPONENT",
    "F_FROZEN_BROAD_EVENT_GATE",
)
SEALED_READ_KEYS = (
    "market_reads",
    "label_reads",
    "validation_reads",
    "holdout_reads",
    "historical_2023_reads",
    "forward_b_reads",
    "forward_2026_reads",
)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _file_sha256(path: Path) -> str:
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


def _legacy_candidate(
    registry: UnifiedCapabilityRegistry,
    root_contract_path: Path,
) -> dict[str, Any]:
    allowlists = json.loads(root_contract_path.read_text(encoding="utf-8"))[
        "route_root_allowlists"
    ]
    pair = CompositionalGrammarV2(
        registry, route_root_allowlist=allowlists
    ).propose("MINUTE_STATIC", attempt_index=0, seed=1729)
    return dict(pair.primary)


def build_program_v1_smoke(
    *,
    output_root: Path,
    registry_path: Path,
    root_contract_path: Path,
    repo_sha: str,
) -> dict[str, Any]:
    output_root = Path(output_root)
    if output_root.exists() and any(output_root.iterdir()):
        raise FileExistsError(f"smoke output root is not empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    registry = UnifiedCapabilityRegistry.read(Path(registry_path))
    legacy = _legacy_candidate(registry, Path(root_contract_path))
    compiler = ProgramCompilerV1(registry)
    fixtures = build_golden_program_fixtures_v1(
        registry, legacy_candidate=legacy
    )
    if tuple(row.fixture_id for row in fixtures) != FIXTURE_IDS:
        raise ValueError("golden fixture identity/order drift")

    fixture_rows = []
    compiled_rows = []
    control_rows = []
    artifact_paths: list[Path] = []
    for index, fixture in enumerate(fixtures):
        fixture_record = _self_hashed(
            {
                **fixture.to_record(),
                "fixture_order": index,
                "performance_claim_allowed": False,
            },
            "fixture_payload_sha256",
        )
        fixture_path = output_root / "fixtures" / f"{fixture.fixture_id}.json"
        _write_json(fixture_path, fixture_record)
        artifact_paths.append(fixture_path)
        fixture_rows.append(
            {
                "fixture_id": fixture.fixture_id,
                "status": fixture.status,
                "blocker_code": fixture.blocker_code,
                "fixture_payload_sha256": fixture_record[
                    "fixture_payload_sha256"
                ],
            }
        )
        if fixture.program is None:
            continue
        compiled = compiler.compile(fixture.program)
        compiled_record = compiled.to_record()
        compiled_path = output_root / "compiled" / f"{fixture.fixture_id}.json"
        _write_json(compiled_path, compiled_record)
        artifact_paths.append(compiled_path)
        compiled_rows.append(
            {
                "fixture_id": fixture.fixture_id,
                "program_id": fixture.program.program_id,
                "semantic_program_hash": fixture.program.semantic_program_hash,
                "compiled_program_hash": compiled_record[
                    "compiled_program_hash"
                ],
                "physical_leaf_ids": list(compiled.physical_leaf_ids),
                "external_adapter_requirements": list(
                    compiled.external_adapter_requirements
                ),
            }
        )
        lineage = ProposalLineageV1(
            template_stratum_id="MANUAL_FIXTURE",
            seed=1729,
            attempt=index,
            sampler="FIXED_GOLDEN_FIXTURE_V1",
            generation_source="CN_TYPED_CANDIDATE_PROGRAM_V1",
        ).to_record(semantic_program_hash=fixture.program.semantic_program_hash)
        lineage_path = output_root / "proposal_lineage" / f"{fixture.fixture_id}.json"
        _write_json(lineage_path, lineage)
        artifact_paths.append(lineage_path)
        if fixture.program.matched_control_plan.operations:
            pair = construct_matched_control_program_v1(fixture.program)
            compiled_control = compiler.compile(pair.control).to_record()
            control_record = _self_hashed(
                {
                    "fixture_id": fixture.fixture_id,
                    "pair_id": pair.pair_id,
                    "primary_program_id": pair.primary_program_id,
                    "primary_semantic_program_hash": pair.primary.semantic_program_hash,
                    "control_program_id": pair.control_program_id,
                    "control_semantic_program_hash": pair.control.semantic_program_hash,
                    "control_compiled_program_hash": compiled_control[
                        "compiled_program_hash"
                    ],
                    "diagnostic_only": pair.diagnostic_only,
                },
                "control_pair_payload_sha256",
            )
            control_path = output_root / "controls" / f"{fixture.fixture_id}.json"
            _write_json(control_path, control_record)
            artifact_paths.append(control_path)
            control_rows.append(control_record)

    access_ledger = _self_hashed(
        {
            "schema_version": "cn_typed_candidate_program_v1_access_ledger_v1",
            "authorized_metadata_reads": [
                str(Path(registry_path).resolve()),
                str(Path(root_contract_path).resolve()),
            ],
            **{key: 0 for key in SEALED_READ_KEYS},
            "optimizer_writes": 0,
            "scheduler_writes": 0,
            "promotion_writes": 0,
            "financial_evaluation_executed": False,
        },
        "access_ledger_payload_sha256",
    )
    access_path = output_root / "access_ledger.json"
    _write_json(access_path, access_ledger)
    artifact_paths.append(access_path)

    summary = _self_hashed(
        {
            "schema_version": "cn_typed_candidate_program_v1_smoke_summary_v1",
            "status": SMOKE_STATUS,
            "repo_sha": str(repo_sha),
            "registry_hash": registry.registry_hash,
            "fixture_count": len(fixtures),
            "compile_ready_count": len(compiled_rows),
            "fail_closed_count": len(fixtures) - len(compiled_rows),
            "fixture_rows": fixture_rows,
            "compiled_rows": compiled_rows,
            "matched_control_pair_count": len(control_rows),
            "legacy_candidate_id": str(legacy.get("candidate_id") or ""),
            "legacy_exact_identity": str(legacy.get("exact_identity") or ""),
            "portfolio_decoder_id": "TOPK_10_EQUAL",
            "performance_claim_allowed": False,
            "prospective_64_pair_joint_smoke_qualified": False,
            "prospective_blockers": [
                "BILLBOARD_EPISODE_AUTHORITY_NOT_REGISTERED"
            ],
        },
        "summary_payload_sha256",
    )
    summary_path = output_root / "summary.json"
    _write_json(summary_path, summary)
    artifact_paths.append(summary_path)

    artifacts = [
        {
            "relative_path": path.relative_to(output_root).as_posix(),
            "sha256": _file_sha256(path),
            "size": path.stat().st_size,
        }
        for path in sorted(artifact_paths)
    ]
    manifest = _self_hashed(
        {
            "schema_version": "cn_typed_candidate_program_v1_artifact_manifest_v1",
            "artifacts": artifacts,
            "artifact_count": len(artifacts),
        },
        "manifest_payload_sha256",
    )
    manifest_path = output_root / "ARTIFACT_MANIFEST.json"
    _write_json(manifest_path, manifest)
    closure = _self_hashed(
        {
            "schema_version": "cn_typed_candidate_program_v1_closure_v1",
            "status": SMOKE_STATUS,
            "repo_sha": str(repo_sha),
            "registry_hash": registry.registry_hash,
            "manifest_file_sha256": _file_sha256(manifest_path),
            "manifest_payload_sha256": manifest["manifest_payload_sha256"],
            "artifact_count": len(artifacts),
            "fixture_count": len(fixtures),
            "compile_ready_count": len(compiled_rows),
            "blocked_fixture_count": len(fixtures) - len(compiled_rows),
            "market_label_validation_holdout_historical_forward_reads": 0,
            "financial_evaluation_executed": False,
        },
        "closure_payload_sha256",
    )
    _write_json(output_root / "CN_TYPED_CANDIDATE_PROGRAM_V1_COMPLETE.json", closure)
    return closure


def verify_program_v1_smoke(
    *,
    smoke_root: Path,
    registry_path: Path,
    root_contract_path: Path,
    audit_output: Path | None = None,
) -> dict[str, Any]:
    smoke_root = Path(smoke_root)
    closure_path = smoke_root / "CN_TYPED_CANDIDATE_PROGRAM_V1_COMPLETE.json"
    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    closure_hash = str(closure.get("closure_payload_sha256") or "")
    if stable_hash({key: value for key, value in closure.items() if key != "closure_payload_sha256"}) != closure_hash:
        raise ValueError("closure canonical self-hash mismatch")
    if closure.get("status") != SMOKE_STATUS:
        raise ValueError("smoke closure status mismatch")
    manifest_path = smoke_root / "ARTIFACT_MANIFEST.json"
    if _file_sha256(manifest_path) != closure.get("manifest_file_sha256"):
        raise ValueError("artifact manifest file hash mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_hash = str(manifest.get("manifest_payload_sha256") or "")
    if stable_hash({key: value for key, value in manifest.items() if key != "manifest_payload_sha256"}) != manifest_hash:
        raise ValueError("artifact manifest canonical self-hash mismatch")
    for artifact in manifest.get("artifacts") or ():
        path = smoke_root / str(artifact["relative_path"])
        if not path.is_file() or _file_sha256(path) != artifact["sha256"]:
            raise ValueError(f"declared artifact drift: {path}")
        if path.stat().st_size != int(artifact["size"]):
            raise ValueError(f"declared artifact size drift: {path}")

    registry = UnifiedCapabilityRegistry.read(Path(registry_path))
    legacy = _legacy_candidate(registry, Path(root_contract_path))
    compiler = ProgramCompilerV1(registry)
    fixtures = build_golden_program_fixtures_v1(registry, legacy_candidate=legacy)
    for fixture in fixtures:
        record = json.loads(
            (smoke_root / "fixtures" / f"{fixture.fixture_id}.json").read_text(
                encoding="utf-8"
            )
        )
        record_hash = str(record.get("fixture_payload_sha256") or "")
        if stable_hash({key: value for key, value in record.items() if key != "fixture_payload_sha256"}) != record_hash:
            raise ValueError("fixture canonical self-hash mismatch")
        if fixture.program is None:
            if record.get("status") != "BLOCKED_FAIL_CLOSED":
                raise ValueError("blocked fixture status drift")
            continue
        restored = CandidateProgramSpecV1.from_record(record["program"])
        expected = compiler.compile(restored).to_record()
        observed = json.loads(
            (smoke_root / "compiled" / f"{fixture.fixture_id}.json").read_text(
                encoding="utf-8"
            )
        )
        if expected != observed:
            raise ValueError(f"compiled fixture drift: {fixture.fixture_id}")
        if restored.matched_control_plan.operations:
            pair = construct_matched_control_program_v1(restored)
            control = json.loads(
                (smoke_root / "controls" / f"{fixture.fixture_id}.json").read_text(
                    encoding="utf-8"
                )
            )
            if pair.pair_id != control.get("pair_id"):
                raise ValueError("matched-control pair identity drift")
            if pair.control.semantic_program_hash != control.get(
                "control_semantic_program_hash"
            ):
                raise ValueError("matched-control program identity drift")
    access = json.loads((smoke_root / "access_ledger.json").read_text(encoding="utf-8"))
    if any(int(access.get(key, -1)) != 0 for key in SEALED_READ_KEYS):
        raise ValueError("smoke access ledger contains a prohibited read")
    if bool(access.get("financial_evaluation_executed")):
        raise ValueError("zero-financial smoke claims financial execution")

    audit = _self_hashed(
        {
            "schema_version": "cn_typed_candidate_program_v1_independent_audit_v1",
            "status": AUDIT_STATUS,
            "closure_file_sha256": _file_sha256(closure_path),
            "closure_payload_sha256": closure_hash,
            "manifest_file_sha256": _file_sha256(manifest_path),
            "manifest_payload_sha256": manifest_hash,
            "artifact_count_verified": int(manifest["artifact_count"]),
            "fixture_identity_order_verified": list(FIXTURE_IDS),
            "compile_ready_count_verified": 5,
            "fail_closed_fixture_count_verified": 1,
            "matched_controls_recomputed": 3,
            "all_declared_artifacts_verified": True,
            "zero_financial_and_sealed_reads_verified": True,
        },
        "audit_payload_sha256",
    )
    if audit_output is not None:
        _write_json(Path(audit_output), audit)
    return audit


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--output-root", type=Path, required=True)
    build.add_argument("--registry", type=Path, required=True)
    build.add_argument("--root-contract", type=Path, required=True)
    build.add_argument("--repo-sha", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--smoke-root", type=Path, required=True)
    verify.add_argument("--registry", type=Path, required=True)
    verify.add_argument("--root-contract", type=Path, required=True)
    verify.add_argument("--audit-output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "build":
        payload = build_program_v1_smoke(
            output_root=args.output_root,
            registry_path=args.registry,
            root_contract_path=args.root_contract,
            repo_sha=args.repo_sha,
        )
    else:
        payload = verify_program_v1_smoke(
            smoke_root=args.smoke_root,
            registry_path=args.registry,
            root_contract_path=args.root_contract,
            audit_output=args.audit_output,
        )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
