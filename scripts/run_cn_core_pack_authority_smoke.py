"""Tiny 77o integration smoke for the active Core-Pack discovery authority."""

from __future__ import annotations

import argparse
import json
import platform
from pathlib import Path

import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    AUTHORIZED_HOST,
    _context_and_binding,
    _clock_for_route,
    _outcome_rows,
    _probe_pack,
    _run_automatic_validation_after_train,
    _run_phase3cm,
    _sha256,
    _train_dates,
    _write_csv,
    _write_json,
    _write_parquet,
)
from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    _add_resolved_behavior_rows,
    _admit_behavior_unique,
    _load_historical_dedupe,
    materialized_schema_binding,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.portfolio_behavior_archive import PortfolioBehaviorArchive
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    RegistryDrivenGenerator,
    load_development_discovery_root_authority,
)


SMOKE_ROUTES = (
    "MINUTE_STATIC",
    "FIRSTN_PATH",
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
    "DISCLOSURE_EVENT",
    "MARKET_REGIME_CONDITION",
    "INTRADAY_STATE_TRANSITION",
)
PAIR_BUDGET_PER_ROUTE = 4
SUPPLY_PROBE_PAIRS_PER_ROUTE = 12
SESSION_COMPUTE_THREADS = 2


def _restore_candidate_rows(path: Path) -> list[dict]:
    rows = pd.read_parquet(path).fillna("").to_dict(orient="records")
    for row in rows:
        for key in (
            "declared_field_ids",
            "condition_field_ids",
            "access_roles",
            "subtree_hashes",
        ):
            value = row.get(key)
            if isinstance(value, str) and value.startswith("["):
                row[key] = json.loads(value)
    return rows


def _load_authority(args: argparse.Namespace, registry: UnifiedCapabilityRegistry) -> dict:
    authority = load_development_discovery_root_authority(
        args.discovery_contract.resolve(), registry=registry
    )
    authorization = json.loads(
        args.discovery_authorization.resolve().read_text(encoding="utf-8")
    )
    if not bool(authorization.get("execution_authorized")):
        raise RuntimeError("DEVELOPMENT_DISCOVERY_EXECUTION_NOT_AUTHORIZED")
    if authorization.get("root_contract_hash") != authority["contract_hash"]:
        raise RuntimeError("DEVELOPMENT_DISCOVERY_AUTHORIZATION_HASH_MISMATCH")
    if int(authorization.get("initial_integration_smoke_max_pairs") or 0) < (
        len(SMOKE_ROUTES) * PAIR_BUDGET_PER_ROUTE
    ):
        raise RuntimeError("SMOKE_PAIR_BUDGET_EXCEEDS_AUTHORIZATION")
    return authority


def prepare(args: argparse.Namespace) -> dict:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"77o only: {AUTHORIZED_HOST}")
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    authority = _load_authority(args, registry)
    historical_exact, historical_behavior, archive_snapshot = _load_historical_dedupe(
        candidate_archive_path=args.historical_candidate_archive.resolve(),
        behavior_archive_path=args.historical_behavior_archive.resolve(),
    )
    _, schema_by_backend = materialized_schema_binding(
        field_roots={
            "active_bar": args.train_active_field_root.resolve(),
            "stock_session": args.train_session_field_root.resolve(),
        },
        registry=registry,
    )
    generator = RegistryDrivenGenerator(
        registry,
        constructor_profile=COMPOSITIONAL_V2_PROFILE,
        enforce_route_compatibility=True,
        route_root_allowlist=authority["route_root_allowlists"],
    )
    generated = []
    supply_probe_rows = []
    funnels = []
    for ordinal, route_id in enumerate(SMOKE_ROUTES):
        rows, funnel = generator.generate_route_attempts(
            route_id,
            scheduled_pairs=SUPPLY_PROBE_PAIRS_PER_ROUTE,
            seed=int(args.seed + ordinal * 1009),
            attempt_limit=1024,
            existing_exact_identities=set(historical_exact),
            available_field_ids=schema_by_backend[_clock_for_route(route_id)],
        )
        supply_probe_rows.extend(rows)
        generated.extend(rows[: 2 * PAIR_BUDGET_PER_ROUTE])
        funnel["execution_pair_budget"] = PAIR_BUDGET_PER_ROUTE
        funnel["supply_probe_pair_target"] = SUPPLY_PROBE_PAIRS_PER_ROUTE
        funnels.append(funnel)
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    probe_rows, probe_audit = _probe_pack(
        candidate_rows=generated,
        field_roots={
            "active_bar": args.train_active_field_root.resolve(),
            "stock_session": args.train_session_field_root.resolve(),
        },
        train_dates=_train_dates(split),
        coordinate_binding=authority["contract_hash"],
        batch_id="core_pack_authority_smoke",
        compute_threads={
            "active_bar": int(args.compute_threads),
            "stock_session": SESSION_COMPUTE_THREADS,
        },
    )
    admitted, decisions = _admit_behavior_unique(
        candidate_rows=generated,
        probe_rows=probe_rows,
        historical_archive=historical_behavior,
    )
    if len(admitted) // 2 < 14:
        raise RuntimeError(
            f"SMOKE_BEHAVIOR_UNIQUE_UNDERFILL: {len(admitted) // 2} < 14"
        )
    paths = {
        "candidate_attempts": _write_csv(output_root / "candidate_attempts.csv", generated),
        "supply_probe_candidates": _write_csv(
            output_root / "supply_probe_candidates.csv", supply_probe_rows
        ),
        "behavior_probe": _write_parquet(output_root / "behavior_probe.parquet", probe_rows),
        "admission_decisions": _write_parquet(output_root / "admission_decisions.parquet", decisions),
        "admitted_candidates": _write_csv(output_root / "admitted_candidates.csv", admitted),
        "admitted_candidates_parquet": _write_parquet(
            output_root / "admitted_candidates.parquet", admitted
        ),
        "route_funnel": _write_parquet(output_root / "route_funnel.parquet", funnels),
        "probe_audit": _write_json(output_root / "probe_audit.json", probe_audit),
        "archive_snapshot": _write_json(output_root / "historical_archive_snapshot.json", archive_snapshot),
    }
    result = {
        "status": "SMOKE_PREPARED",
        "scheduled_pairs": len(SMOKE_ROUTES) * PAIR_BUDGET_PER_ROUTE,
        "generated_pairs": len(generated) // 2,
        "supply_probe_target_pairs_per_route": SUPPLY_PROBE_PAIRS_PER_ROUTE,
        "supply_probe_generated_pairs": len(supply_probe_rows) // 2,
        "admitted_pairs": len(admitted) // 2,
        "root_contract_hash": authority["contract_hash"],
        "root_scope_authority": "FROZEN_DEVELOPMENT_DISCOVERY_CONTRACT",
        "schema_role": "EXECUTION_COMPATIBILITY_ONLY",
        "paths": {key: str(path) for key, path in paths.items()},
        "validation_trigger": "AUTOMATIC_AFTER_TRAIN_COMPLETE",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    _write_json(output_root / "prepare_manifest.json", result)
    return result


def execute(args: argparse.Namespace) -> dict:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"77o only: {AUTHORIZED_HOST}")
    output_root = args.output_root.resolve()
    prepare_manifest = json.loads(
        (output_root / "prepare_manifest.json").read_text(encoding="utf-8")
    )
    if prepare_manifest.get("status") != "SMOKE_PREPARED":
        raise RuntimeError("SMOKE_NOT_PREPARED")
    candidates = _restore_candidate_rows(output_root / "admitted_candidates.parquet")
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    _load_authority(args, registry)
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    binding_path, table_paths = _context_and_binding(
        batch_root=output_root / "train",
        candidates=candidates,
        registry=registry,
        split=split,
        data_release_hash=_sha256(args.train_sidecar_closure.resolve()),
        evaluation_role="train",
    )
    train_receipts = _run_phase3cm(
        batch_id="core_pack_authority_smoke_train",
        batch_root=output_root / "train",
        binding_path=binding_path,
        table_paths=table_paths,
        split_manifest=args.split_manifest.resolve(),
        field_roots={"active_bar": args.train_active_field_root.resolve(), "stock_session": args.train_session_field_root.resolve()},
        label_roots={"active_bar": args.train_active_label_root.resolve(), "stock_session": args.train_session_label_root.resolve()},
        compute_threads={
            "active_bar": int(args.compute_threads),
            "stock_session": SESSION_COMPUTE_THREADS,
        },
        evaluation_role="train",
    )
    outcomes, full_behavior = _outcome_rows(output_root / "train")
    outcome_path = _write_parquet(output_root / "train_outcomes.parquet", outcomes)
    full_behavior_path = _write_parquet(output_root / "train_full_behavior.parquet", full_behavior)
    train_archive = PortfolioBehaviorArchive()
    _add_resolved_behavior_rows(train_archive, full_behavior)
    train_archive_path = output_root / "train_behavior_archive.parquet"
    train_archive.write_parquet(train_archive_path)
    train_manifest_path = _write_json(
        output_root / "train_complete_manifest.json",
        {
            "status": "TRAIN_COMPLETE",
            "pair_count": len(outcomes),
            "candidate_pack": str(output_root / "admitted_candidates.csv"),
            "train_receipts": train_receipts,
            "validation_trigger": "AUTOMATIC",
            "promotion": "FORBIDDEN",
        },
    )
    validation_receipt = _run_automatic_validation_after_train(
        train_manifest_path=train_manifest_path,
        protected_train_artifacts=(
            output_root / "admitted_candidates.csv",
            output_root / "admitted_candidates.parquet",
            outcome_path,
            full_behavior_path,
            train_archive_path,
        ),
        validation_root=output_root / "post_train_validation",
        candidates=candidates,
        registry=registry,
        split=split,
        validation_data_release_hash=_sha256(args.validation_sidecar_closure.resolve()),
        split_manifest=args.split_manifest.resolve(),
        validation_field_roots={
            "active_bar": args.validation_active_field_root.resolve(),
            "stock_session": args.validation_session_field_root.resolve(),
        },
        validation_label_roots={
            "active_bar": args.validation_active_label_root.resolve(),
            "stock_session": args.validation_session_label_root.resolve(),
        },
        compute_threads={"active_bar": int(args.compute_threads), "stock_session": 1},
    )
    result = {
        "status": "SMOKE_COMPLETE",
        "train_pairs": len(outcomes),
        "validation_status": validation_receipt["status"],
        "validation_usage": "report_only",
        "validation_feedback": "FORBIDDEN",
        "train_artifacts_immutable": validation_receipt["train_artifacts_immutable"],
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    _write_json(output_root / "final_decision.json", result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "execute"))
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--discovery-contract", type=Path, required=True)
    parser.add_argument("--discovery-authorization", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--historical-candidate-archive", type=Path, required=True)
    parser.add_argument("--historical-behavior-archive", type=Path, required=True)
    parser.add_argument("--train-sidecar-closure", type=Path, required=True)
    parser.add_argument("--train-active-field-root", type=Path, required=True)
    parser.add_argument("--train-active-label-root", type=Path, required=True)
    parser.add_argument("--train-session-field-root", type=Path, required=True)
    parser.add_argument("--train-session-label-root", type=Path, required=True)
    parser.add_argument("--validation-sidecar-closure", type=Path)
    parser.add_argument("--validation-active-field-root", type=Path)
    parser.add_argument("--validation-active-label-root", type=Path)
    parser.add_argument("--validation-session-field-root", type=Path)
    parser.add_argument("--validation-session-label-root", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=2026072201)
    parser.add_argument("--compute-threads", type=int, default=11)
    args = parser.parse_args(argv)
    if args.mode == "execute" and not all(
        (
            args.validation_sidecar_closure,
            args.validation_active_field_root,
            args.validation_active_label_root,
            args.validation_session_field_root,
            args.validation_session_label_root,
        )
    ):
        parser.error("execute requires validation sidecar closure and active/session roots")
    result = prepare(args) if args.mode == "prepare" else execute(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
