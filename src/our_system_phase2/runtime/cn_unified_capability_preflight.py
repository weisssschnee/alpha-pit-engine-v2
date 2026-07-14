"""Run the automatic unified capability preflight and dry exposure proof."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services import development_only_data_access
from our_system_phase2.services.development_only_data_access import (
    read_development_panel,
    validate_development_release,
)
from our_system_phase2.services.search_exposure_ledger import SearchExposureLedger
from our_system_phase2.services.typed_route_compiler import TypedRouteCompiler
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


PREFLIGHT_VERSION = "cn_unified_discovery_capability_preflight_v2_executable"


def _sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _assert_hash(path: Path, expected: str, label: str) -> None:
    observed = _sha256(path)
    if observed != str(expected):
        raise ValueError(f"{label} hash mismatch: {observed} != {expected}")


def _join_unresolved(path: Path) -> int:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return sum(row["join_status"] != "EXACT_ACTIVE121_JOIN" for row in csv.DictReader(handle))


def _negative_checks(
    compiler: TypedRouteCompiler,
    generator: RegistryDrivenGenerator,
    contract: Mapping[str, Any],
) -> dict[str, bool]:
    seed_name = sorted(contract["seed_sets"])[0]
    seeds = contract["seed_sets"][seed_name]
    valid_regime = generator.generate_route(
        "MARKET_REGIME_CONDITION", proposal_budget=2, seed=int(seeds["MARKET_REGIME_CONDITION"])
    )[0]
    market_id = valid_regime["condition_field_ids"][0]
    valid_state = generator.generate_route(
        "INTRADAY_STATE_TRANSITION", proposal_budget=2, seed=int(seeds["INTRADAY_STATE_TRANSITION"])
    )[0]
    valid_event = generator.generate_route(
        "DISCLOSURE_EVENT", proposal_budget=2, seed=int(seeds["DISCLOSURE_EVENT"])
    )[0]
    invalid_cases = {
        "market_field_direct_csrank": (
            {**valid_regime, "expression": f"CSRank(${market_id})"},
            "MARKET_FIELD_DIRECT_CSRANK",
        ),
        "intrabar_order_fabrication": (
            {**valid_state, "requires_intrabar_order": True},
            "INTRABAR_ORDER_FABRICATED",
        ),
        "future_revision": (
            {**valid_event, "uses_future_revision": True},
            "PIT_UNQUALIFIED",
        ),
        "pre_maturity_event": (
            {**valid_event, "maturity_contract_registered": False},
            "POST_WINDOW_NOT_MATURE",
        ),
        "repeated_episode_vote": (
            {**valid_event, "vote_policy": "MINUTE_ROWS_REPEAT_VOTE"},
            "EPISODE_REPEAT_VOTE",
        ),
        "missing_matched_control": (
            {**valid_event, "matched_control_id": ""},
            "CONTROL_CONTRACT_MISSING",
        ),
        "unresolved_field_identity": (
            {**valid_state, "expression": "CSRank($unknown_unified_field)", "declared_field_ids": []},
            "UNKNOWN_FIELD_ID",
        ),
        "sealed_data_access": (
            {**valid_state, "access_roles": ["development", "forward"]},
            "SEALED_DATA_ACCESS",
        ),
    }
    output = {}
    for name, (candidate, expected) in invalid_cases.items():
        verdict = compiler.compile(candidate)
        output[name] = not verdict.legal and verdict.rejection_code == expected
    # Latched observations fail before compilation because the registry does
    # not grant them the lifecycle route.  This is stronger than relying on a
    # lexical operator check at runtime.
    output["latched_observation_not_lifecycle_routable"] = not any(
        "LATCHED" in row.temporal_semantics
        and "INTRADAY_STATE_TRANSITION" in row.allowed_routes
        for row in compiler.registry.fields
    )
    return output


def run_preflight(
    *,
    repo_root: Path,
    registry_path: Path,
    contract_path: Path,
    external_preflight_path: Path,
    external_join_path: Path,
    panel_root: Path,
    release_manifest_path: Path,
    split_manifest_path: Path,
    output_root: Path,
    repo_sha: str,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    contract = _json(contract_path)
    external = _json(external_preflight_path)
    registry = UnifiedCapabilityRegistry.read(registry_path)
    compiler = TypedRouteCompiler(registry)
    generator = RegistryDrivenGenerator(registry)
    contract_hash = stable_hash(contract)
    gates: dict[str, dict[str, Any]] = {}

    g0_errors: list[str] = []
    if registry.registry_hash != contract["registry_hash"]:
        g0_errors.append("registry hash does not match frozen contract")
    unresolved_join = _join_unresolved(external_join_path)
    if unresolved_join:
        g0_errors.append(f"{unresolved_join} external aliases remain unresolved")
    if any(not row.source_field_id.startswith("cn.sf.") for row in registry.fields if row.search_eligible):
        g0_errors.append("candidate-visible source identity is unresolved")
    gates["G0_AUTHORITY_AND_IDENTITY"] = {"passed": not g0_errors, "errors": g0_errors}

    g1_errors: list[str] = []
    if any(row.search_eligible and row.pit_status == "PIT_CONTRACT_UNRESOLVED" for row in registry.fields):
        g1_errors.append("PIT unresolved field is candidate visible")
    if any(row.search_eligible and row.unit_status == "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED" for row in registry.fields):
        g1_errors.append("unit-unqualified field is candidate visible")
    if any(row.source_table == "zygc_em" and row.search_eligible for row in registry.fields):
        g1_errors.append("zygc_em escaped isolation")
    if contract["maximum_observable_time"] != "2025-07-07T15:00:00":
        g1_errors.append("fundamental release maximum observable time drift")
    gates["G1_SEMANTICS_AND_PIT"] = {"passed": not g1_errors, "errors": g1_errors}

    dry_rows: dict[str, list[dict[str, Any]]] = {}
    g2_errors: list[str] = []
    for seed_name in sorted(contract["seed_sets"]):
        rows = generator.dry_generate(contract["capability_canary"], seed_name=seed_name)
        dry_rows[seed_name] = rows
        if not all(bool(row["legal"]) for row in rows):
            g2_errors.append(f"illegal dry candidate in {seed_name}")
        state_rows = [row for row in rows if row["route_id"] == "INTRADAY_STATE_TRANSITION" and not row["is_matched_control"]]
        if not state_rows or not all(row.get("claimed_state_field_id") and row.get("state_source_expression") in row["canonical_expression"] for row in state_rows):
            g2_errors.append(f"state representation not consumed in {seed_name}")
    negative = _negative_checks(compiler, generator, contract["capability_canary"])
    if not all(negative.values()):
        g2_errors.extend(name for name, passed in negative.items() if not passed)
    gates["G2_TYPED_WIRING"] = {"passed": not g2_errors, "errors": g2_errors, "negative_checks": negative}

    g3_errors: list[str] = []
    for mode in ("capability_canary", "unified_discovery"):
        mode_contract = contract[mode]
        for route_id in ROUTE_IDS:
            budget = mode_contract["route_budgets"][route_id]
            if not all(int(budget[key]) > 0 for key in ("proposal", "admission", "strict")):
                g3_errors.append(f"{mode}:{route_id} has zero budget")
    dry_summaries: dict[str, Any] = {}
    for seed_name, rows in dry_rows.items():
        ledger = SearchExposureLedger(
            registry=registry,
            run_id=f"{contract['experiment_id']}_dry_{seed_name}",
            repo_sha=repo_sha,
            contract_hash=contract_hash,
            data_release_hash=contract["data_release"]["release_hash"],
            route_budgets=contract["capability_canary"]["route_budgets"],
        )
        ledger.ingest(rows)
        dry_summaries[seed_name] = ledger.write(output_root / "dry_generation" / seed_name)
    gates["G3_EXPOSURE_AND_BUDGET"] = {"passed": not g3_errors, "errors": g3_errors, "dry_summaries": dry_summaries}

    g4_errors: list[str] = []
    for seed_name, rows in dry_rows.items():
        ids = {row["candidate_id"] for row in rows}
        if any(row["matched_control_id"] not in ids for row in rows):
            g4_errors.append(f"matched control missing in {seed_name}")
        if any(row["vote_policy"] not in {"ONE_SUPPORT_UNIT_ONE_VOTE", "CONTROL_NO_SEPARATE_VOTE"} for row in rows):
            g4_errors.append(f"support-unit vote contract invalid in {seed_name}")
        for row in rows:
            expected = registry.route_contracts[row["route_id"]]["support_unit"]
            if row["support_unit"] != expected:
                g4_errors.append(f"support unit drift: {row['candidate_id']}")
    gates["G4_MATCHED_CONTROLS"] = {"passed": not g4_errors, "errors": g4_errors}

    g5_errors: list[str] = []
    if repo_sha != contract["repo_sha"]:
        g5_errors.append("repo SHA does not match frozen contract")
    _assert_hash(release_manifest_path, contract["data_release"]["manifest_file_sha256"], "release manifest")
    _assert_hash(split_manifest_path, contract["data_release"]["split_manifest_sha256"], "split manifest")
    _assert_hash(registry_path, contract["input_hashes"]["registry_file_sha256"], "registry file")
    code_paths = {
        "compiler": repo_root / "src/our_system_phase2/services/typed_route_compiler.py",
        "generator": repo_root / "src/our_system_phase2/services/unified_discovery_generators.py",
        "ledger": repo_root / "src/our_system_phase2/services/search_exposure_ledger.py",
    }
    for label, path in code_paths.items():
        _assert_hash(path, contract["code_hashes"][label], label)
    release = validate_development_release(
        panel_root,
        release_manifest_path,
        split_manifest_path,
        expected_release_hash=contract["data_release"]["release_hash"],
    )
    access_path = output_root / "development_only_access_smoke.json"
    _, _ = read_development_panel(
        release,
        trade_date=contract["preflight_smoke"]["trade_date"],
        row_group_index=int(contract["preflight_smoke"]["row_group_index"]),
        columns=["trade_time", "code"],
        read_ledger_path=access_path,
        loader_sha=_sha256(Path(development_only_data_access.__file__).resolve()),
    )
    access = _json(access_path)
    forbidden = sum(int(access.get(key) or 0) for key in (
        "forbidden_file_open_count", "forbidden_row_group_read_count", "validation_rows_read",
        "holdout_rows_read", "forward_rows_read",
    ))
    if forbidden:
        g5_errors.append(f"forbidden data access count={forbidden}")
    if len(release.files) != 16:
        g5_errors.append(f"physical development release has {len(release.files)} files, expected 16")
    gates["G5_ACCESS_AND_REPRODUCIBILITY"] = {
        "passed": not g5_errors,
        "errors": g5_errors,
        "physical_file_count": len(release.files),
        "forbidden_read_count": forbidden,
        "release_hash": release.release_hash,
    }

    passed = all(gate["passed"] for gate in gates.values())
    summary = {
        "preflight_version": PREFLIGHT_VERSION,
        "external_contract_version": external["contract_version"],
        "status": "READY_FOR_FULL_DEVELOPMENT_CAPABILITY_CANARY" if passed else "CN_UNIFIED_CAPABILITY_PREFLIGHT_FAILED",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": repo_sha,
        "contract_hash": contract_hash,
        "registry_hash": registry.registry_hash,
        "gates": gates,
        "all_gates_passed": passed,
        "forward_2026_sealed": True,
        "validation_accessed": False,
        "holdout_accessed": False,
        "candidate_promotion": False,
        "cross_sprint_memory": False,
    }
    (output_root / "preflight.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--external-preflight", type=Path, required=True)
    parser.add_argument("--external-join", type=Path, required=True)
    parser.add_argument("--panel-root", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    args = parser.parse_args(argv)
    result = run_preflight(
        repo_root=args.repo,
        registry_path=args.registry,
        contract_path=args.contract,
        external_preflight_path=args.external_preflight,
        external_join_path=args.external_join,
        panel_root=args.panel_root,
        release_manifest_path=args.release_manifest,
        split_manifest_path=args.split_manifest,
        output_root=args.output_root,
        repo_sha=args.repo_sha,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["all_gates_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
